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

app = FastAPI()

TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR    = os.path.join(BASE_DIR, "static")

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
# SEARCH
# =========================
@app.get("/search")
def search(query: str, min_price: float = 0, max_price: float = 1e9):
    sql = text("""
        SELECT
            product_id, name, category, price_usd,
            CASE
                WHEN name ILIKE :q THEN 3
                WHEN category ILIKE :q THEN 2
                ELSE 1
            END AS score
        FROM products
        WHERE (name ILIKE :q OR category ILIKE :q)
          AND price_usd BETWEEN :min_price AND :max_price
        ORDER BY score DESC, price_usd ASC
        LIMIT 20
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"q": f"%{query}%", "min_price": min_price, "max_price": max_price})
        products = [
            {"product_id": r.product_id, "name": r.name, "category": r.category,
             "price": float(r.price_usd), "score": float(r.score)}
            for r in result
        ]
    return {"query": query, "filters": {"min_price": min_price, "max_price": max_price}, "results": products}

# =========================
# DASHBOARD
# =========================
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request, "user_id": 1})

# =========================
# RECOMMENDATIONS — top overall
# =========================
@app.get("/recommend/top")
def recommend_top(limit: int = 200):
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
            {"user_id": None, "product_id": r.product_id, "name": r.name,
             "category": r.category, "score": float(r.popularity_score), "rank": i + 1}
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
            SUM(ur.popularity_score)                                    AS popularity_score,
            ROW_NUMBER() OVER (ORDER BY SUM(ur.popularity_score) DESC)  AS rank
        FROM user_recommendations ur
        JOIN products p ON ur.product_id = p.product_id
        WHERE ur.user_id = :user_id
        GROUP BY ur.user_id, ur.product_id, p.name, p.category
        ORDER BY rank ASC
        LIMIT 20
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"user_id": user_id})
            return [
                {"user_id": r.user_id, "product_id": r.product_id, "name": r.name,
                 "category": r.category, "score": float(r.popularity_score), "rank": r.rank}
                for r in result
            ]
    except Exception as e:
        print("DB ERROR:", e)
        raise HTTPException(status_code=500, detail="Recommendation query failed")

# =========================
# RECENT EVENTS
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
            {"user_id": r.user_id, "product_id": r.product_id,
             "event_type": r.event_type, "timestamp": r.timestamp.isoformat()}
            for r in result
        ]

# =========================
# LOG CLICK EVENT
# =========================
@app.post("/log-click")
def log_click(user_id: int, product_id: int, event_type: str = "page_view"):
    event = {
        "user_id": user_id, "product_id": product_id,
        "event_type": event_type, "timestamp": datetime.utcnow().isoformat()
    }
    producer.send("click_events", event)

    score_map = {"page_view": 1, "add_to_cart": 5, "checkout": 7, "purchase": 10}
    score_increment = score_map.get(event_type, 1)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO user_click_logs (user_id, product_id, event_type, timestamp)
            VALUES (:user_id, :product_id, :event_type, :ts)
        """), {"user_id": user_id, "product_id": product_id,
               "event_type": event_type, "ts": datetime.utcnow()})

        conn.execute(text("""
            INSERT INTO user_recommendations (user_id, product_id, popularity_score, rank)
            VALUES (:user_id, :product_id, :score, 999)
            ON CONFLICT (user_id, product_id)
            DO UPDATE SET popularity_score = user_recommendations.popularity_score + :score
        """), {"user_id": user_id, "product_id": product_id, "score": score_increment})

    return {"status": "logged", "kafka": True}

# =========================
# DAILY STATS — based on latest date in streamed data
# =========================
@app.get("/stats/daily")
def daily_stats():
    query = text("""
        WITH latest AS (
            SELECT DATE(MAX(timestamp::timestamptz AT TIME ZONE 'UTC')) AS latest_date
            FROM user_click_logs
        )
        SELECT
            COUNT(*)                                                    AS total_events,
            COUNT(DISTINCT user_id)                                     AS active_users,
            COUNT(*) FILTER (WHERE event_type = 'purchase')             AS purchases,
            ROUND(AVG(CASE event_type
                WHEN 'page_view'   THEN 1
                WHEN 'add_to_cart' THEN 5
                WHEN 'checkout'    THEN 7
                WHEN 'purchase'    THEN 10
                ELSE 1 END)::numeric, 2)                                AS avg_score,
            latest.latest_date                                          AS data_date
        FROM user_click_logs, latest
        WHERE DATE(timestamp::timestamptz AT TIME ZONE 'UTC') = latest.latest_date
        GROUP BY latest.latest_date
    """)
    with engine.connect() as conn:
        r = conn.execute(query).fetchone()
        if not r:
            return {"total_events": 0, "active_users": 0, "purchases": 0,
                    "avg_score": 0.0, "data_date": None}
        return {
            "total_events": r.total_events,
            "active_users": r.active_users,
            "purchases":    r.purchases,
            "avg_score":    float(r.avg_score or 0),
            "data_date":    str(r.data_date)
        }

# =========================
# SALES TREND FILTERS — from actual streamed data
# =========================
@app.get("/stats/sales-trend/filters")
def sales_trend_filters():
    query = text("""
        SELECT
            EXTRACT(YEAR  FROM timestamp::timestamptz AT TIME ZONE 'UTC')::int AS year,
            EXTRACT(MONTH FROM timestamp::timestamptz AT TIME ZONE 'UTC')::int AS month,
            EXTRACT(DAY   FROM timestamp::timestamptz AT TIME ZONE 'UTC')::int AS day
        FROM user_click_logs
        WHERE event_type = 'purchase'
          AND amount_usd IS NOT NULL
        GROUP BY year, month, day
        ORDER BY year, month, day
    """)
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    years  = sorted(set(r.year  for r in rows))
    months = sorted(set(r.month for r in rows))
    days   = sorted(set(r.day   for r in rows))
    return {"years": years, "months": months, "days": days}

# =========================
# SALES TREND — session-based, respects granularity scope
# granularity=hour  → shows 24 hours of selected day
# granularity=day   → shows days of selected month
# granularity=month → shows months of selected year
# granularity=year  → shows all years
# =========================
@app.get("/stats/sales-trend")
def sales_trend(
    granularity: str = "month",
    year:  int = None,
    month: int = None,
    day:   int = None
):
    filters = ["event_type = 'purchase'", "amount_usd IS NOT NULL"]
    params  = {}

    # Scope filters based on granularity
    if granularity == "hour":
        # Show 24 hours — requires a specific day
        if year:
            filters.append("EXTRACT(YEAR  FROM timestamp::timestamptz AT TIME ZONE 'UTC') = :year")
            params["year"] = year
        if month:
            filters.append("EXTRACT(MONTH FROM timestamp::timestamptz AT TIME ZONE 'UTC') = :month")
            params["month"] = month
        if day:
            filters.append("EXTRACT(DAY   FROM timestamp::timestamptz AT TIME ZONE 'UTC') = :day")
            params["day"] = day

    elif granularity == "day":
        # Show days of a specific month
        if year:
            filters.append("EXTRACT(YEAR  FROM timestamp::timestamptz AT TIME ZONE 'UTC') = :year")
            params["year"] = year
        if month:
            filters.append("EXTRACT(MONTH FROM timestamp::timestamptz AT TIME ZONE 'UTC') = :month")
            params["month"] = month

    elif granularity == "month":
        # Show months of a specific year
        if year:
            filters.append("EXTRACT(YEAR  FROM timestamp::timestamptz AT TIME ZONE 'UTC') = :year")
            params["year"] = year

    # granularity == "year" — no filters, show everything

    where = " AND ".join(filters)
    trunc  = {"hour": "hour", "day": "day", "month": "month", "year": "year"}.get(granularity, "month")

    query = text(f"""
        WITH session_sales AS (
            SELECT
                session_id,
                MAX(timestamp::timestamptz AT TIME ZONE 'UTC') AS sale_time,
                MAX(amount_usd)                                AS amount_usd
            FROM user_click_logs
            WHERE {where}
            GROUP BY session_id
        )
        SELECT
            DATE_TRUNC('{trunc}', sale_time)   AS period,
            COUNT(*)                           AS order_count,
            ROUND(SUM(amount_usd)::numeric, 2) AS revenue
        FROM session_sales
        GROUP BY period
        ORDER BY period ASC
    """)

    with engine.connect() as conn:
        result = conn.execute(query, params)
        return [
            {"period": r.period.isoformat(), "order_count": r.order_count,
             "revenue": float(r.revenue or 0)}
            for r in result
        ]

# =========================
# TOP CATEGORIES — dynamic from streamed events
# =========================
@app.get("/stats/top-categories")
def top_categories():
    query = text("""
        SELECT
            p.category,
            COUNT(DISTINCT ucl.session_id)                              AS total_sessions,
            COUNT(*)                                                    AS total_events,
            COALESCE(ROUND(SUM(
                CASE WHEN ucl.event_type = 'purchase'
                THEN ucl.amount_usd ELSE 0 END
            )::numeric, 2), 0)                                          AS total_revenue
        FROM user_click_logs ucl
        JOIN products p ON ucl.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        return [
            {"category": r.category, "total_sessions": r.total_sessions,
             "total_events": r.total_events, "total_revenue": float(r.total_revenue or 0)}
            for r in result
        ]

# =========================
# ACTIVE USERS BY COUNTRY — current day only
# =========================
@app.get("/stats/active-countries")
def active_countries():
    query = text("""
        WITH latest AS (
            SELECT DATE(MAX(timestamp::timestamptz AT TIME ZONE 'UTC')) AS latest_date
            FROM user_click_logs
        )
        SELECT
            s.country,
            COUNT(DISTINCT ucl.user_id) AS active_users
        FROM user_click_logs ucl
        JOIN sessions s ON ucl.session_id = s.session_id, latest
        WHERE DATE(ucl.timestamp::timestamptz AT TIME ZONE 'UTC') = latest.latest_date
        GROUP BY s.country
        ORDER BY active_users DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        return [
            {"country": r.country, "active_users": r.active_users}
            for r in result
        ]

# =========================
# TOP 3 USERS — from streamed events
# =========================
@app.get("/stats/top-users")
def top_users():
    query = text("""
        SELECT
            ucl.user_id,
            c.name,
            c.country,
            SUM(CASE ucl.event_type
                WHEN 'page_view'   THEN 1
                WHEN 'add_to_cart' THEN 5
                WHEN 'checkout'    THEN 7
                WHEN 'purchase'    THEN 10
                ELSE 1 END)        AS total_score
        FROM user_click_logs ucl
        JOIN customers c ON ucl.user_id = c.customer_id
        GROUP BY ucl.user_id, c.name, c.country
        ORDER BY total_score DESC
        LIMIT 3
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        return [
            {"user_id": r.user_id, "name": r.name,
             "country": r.country, "total_score": float(r.total_score)}
            for r in result
        ]

# =========================
# DEVICE & SOURCE BREAKDOWN
# =========================
@app.get("/stats/devices")
def device_stats():
    query = text("""
        WITH latest AS (
            SELECT DATE(MAX(timestamp::timestamptz AT TIME ZONE 'UTC')) AS latest_date
            FROM user_click_logs
        )
        SELECT
            s.device,
            s.source,
            COUNT(DISTINCT ucl.user_id)  AS users,
            COUNT(*)                     AS events
        FROM user_click_logs ucl
        JOIN sessions s ON ucl.session_id = s.session_id, latest
        WHERE DATE(ucl.timestamp::timestamptz AT TIME ZONE 'UTC') = latest.latest_date
        GROUP BY s.device, s.source
        ORDER BY events DESC
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()

    # Aggregate by device
    devices = {}
    sources = {}
    for r in rows:
        devices[r.device] = devices.get(r.device, 0) + r.events
        sources[r.source] = sources.get(r.source, 0) + r.events

    return {
        "by_device": [{"name": k, "value": v} for k, v in sorted(devices.items(), key=lambda x: -x[1])],
        "by_source": [{"name": k, "value": v} for k, v in sorted(sources.items(), key=lambda x: -x[1])]
    }

# =========================
# WEBSOCKET — live event stream
# =========================
@app.websocket("/ws/events")
async def events_socket(websocket: WebSocket):
    await websocket.accept()

    consumer = AIOKafkaConsumer(
        "click_events",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        group_id=None
    )

    await consumer.start()
    batch = []

    try:
        async for msg in consumer:
            batch.append(msg.value)
            if len(batch) >= 10:
                await websocket.send_json(batch)
                batch = []
            else:
                await websocket.send_json(batch)
    except Exception as e:
        print(f"WebSocket closed: {e}")
    finally:
        await consumer.stop()