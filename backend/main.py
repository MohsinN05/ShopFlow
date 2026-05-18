from fastapi import FastAPI, Request, WebSocket, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from aiokafka import AIOKafkaConsumer

from sqlalchemy import text
from database import engine

from kafka import KafkaProducer
from datetime import datetime

import os
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# =========================
# App Initialization
# =========================
app = FastAPI()

TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

templates = Jinja2Templates(directory=TEMPLATES_DIR)

if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# =========================
# Kafka Producer
# =========================
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# =========================
# Root
# =========================
@app.get("/")
def home():
    return {"message": "Recommendation API running"}


# =========================
# SEARCH API
# =========================
@app.get("/search")
def search(query: str, min_price: float = 0, max_price: float = 1e9):
    sql = text("""
        SELECT
            product_id,
            name,
            category,
            price_usd,
            CASE
                WHEN name ILIKE :q THEN 3
                WHEN category ILIKE :q THEN 2
                ELSE 1
            END AS score
        FROM products
        WHERE
            (name ILIKE :q OR category ILIKE :q)
            AND price_usd BETWEEN :min_price AND :max_price
        ORDER BY score DESC, price_usd ASC
        LIMIT 20
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {
            "q": f"%{query}%",
            "min_price": min_price,
            "max_price": max_price
        })
        products = [
            {
                "product_id": r.product_id,
                "name":       r.name,
                "category":   r.category,
                "price":      float(r.price_usd),
                "score":      float(r.score)
            }
            for r in result
        ]
    return {
        "query": query,
        "filters": {"min_price": min_price, "max_price": max_price},
        "results": products
    }


# =========================
# DASHBOARD
# =========================
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request, "user_id": 1})


# =========================
# RECOMMENDATIONS — top overall
# IMPORTANT: must be defined before /recommend/{user_id}
# =========================
@app.get("/recommend/top")
def recommend_top(limit: int = 20):
    query = text("""
        SELECT
            ur.product_id,
            p.name,
            p.category,
            SUM(ur.popularity_score) AS popularity_score
        FROM user_recommendations ur
        JOIN products p ON ur.product_id = p.product_id
        GROUP BY ur.product_id, p.name, p.category
        ORDER BY popularity_score DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        return [
            {
                "user_id":    None,
                "product_id": r.product_id,
                "name":       r.name,
                "category":   r.category,
                "score":      float(r.popularity_score),
                "rank":       i + 1
            }
            for i, r in enumerate(result)
        ]


# =========================
# RECOMMENDATIONS — per user
# =========================
@app.get("/recommend/{user_id}")
def recommend(user_id: int):
    query = text("""
        SELECT
            ur.user_id,
            ur.product_id,
            p.name,
            p.category,
            ur.popularity_score,
            ur.rank
        FROM user_recommendations ur
        JOIN products p ON ur.product_id = p.product_id
        WHERE ur.user_id = :user_id
        ORDER BY ur.rank ASC
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"user_id": user_id})
            return [
                {
                    "user_id":    r.user_id,
                    "product_id": r.product_id,
                    "name":       r.name,
                    "category":   r.category,
                    "score":      float(r.popularity_score),
                    "rank":       r.rank
                }
                for r in result
            ]
    except Exception as e:
        print("DB ERROR:", e)
        raise HTTPException(status_code=500, detail="Recommendation query failed")


# =========================
# RECENT EVENTS (REST fallback)
# =========================
@app.get("/recent-events")
def recent_events(limit: int = 30):
    query = text("""
        SELECT user_id, product_id, event_type, timestamp
        FROM user_click_logs
        ORDER BY timestamp DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        return [
            {
                "user_id":    r.user_id,
                "product_id": r.product_id,
                "event_type": r.event_type,
                "timestamp":  r.timestamp.isoformat()
            }
            for r in result
        ]


# =========================
# LOG CLICK EVENT (Kafka + DB)
# =========================
@app.post("/log-click")
def log_click(user_id: int, product_id: int, event_type: str = "page_view"):
    event = {
        "user_id":    user_id,
        "product_id": product_id,
        "event_type": event_type,
        "timestamp":  datetime.utcnow().isoformat()
    }

    producer.send("click_events", event)

    score_map = {"page_view": 1, "add_to_cart": 5, "checkout": 7, "purchase": 10}
    score_increment = score_map.get(event_type, 1)

    insert_sql = text("""
        INSERT INTO user_click_logs (user_id, product_id, event_type, timestamp)
        VALUES (:user_id, :product_id, :event_type, :ts)
    """)
    update_sql = text("""
        INSERT INTO user_recommendations (user_id, product_id, popularity_score, rank)
        VALUES (:user_id, :product_id, :score, 999)
        ON CONFLICT (user_id, product_id)
        DO UPDATE SET
            popularity_score = user_recommendations.popularity_score + :score
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, {
            "user_id":    user_id,
            "product_id": product_id,
            "event_type": event_type,
            "ts":         datetime.utcnow()
        })
        conn.execute(update_sql, {
            "user_id":    user_id,
            "product_id": product_id,
            "score":      score_increment
        })

    return {"status": "logged", "kafka": True}


# =========================
# WEBSOCKET — live event stream
# Streams directly from Kafka, sends batches as array to frontend
# =========================
@app.websocket("/ws/events")
async def events_socket(websocket: WebSocket):
    await websocket.accept()

    consumer = AIOKafkaConsumer(
        "click_events",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        group_id=None  # no group_id = always gets latest, no offset tracking
    )

    await consumer.start()
    batch = []

    try:
        async for msg in consumer:
            batch.append(msg.value)

            # Send in batches of 10 or flush every message — send as array
            if len(batch) >= 10:
                await websocket.send_json(batch)
                batch = []
            else:
                # Send immediately so UI feels live
                await websocket.send_json(batch)

    except Exception as e:
        print(f"WebSocket closed: {e}")
    finally:
        await consumer.stop()