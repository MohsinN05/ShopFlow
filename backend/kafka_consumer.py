"""
kafka_consumer.py
Consumes events from Kafka and writes to:
  - sessions table (upsert — one row per session)
  - user_click_logs table (one row per event)
"""

import json
from kafka import KafkaConsumer
from sqlalchemy import text
from database import engine
from datetime import datetime

consumer = KafkaConsumer(
    "click_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="click-log-writer"
)

print("=== Kafka Consumer: writing click_events → sessions + user_click_logs ===")

insert_session_sql = text("""
    INSERT INTO sessions (session_id, customer_id, start_time, device, source, country)
    VALUES (:session_id, :customer_id, :start_time, :device, :source, :country)
    ON CONFLICT (session_id) DO NOTHING
""")

insert_event_sql = text("""
    INSERT INTO user_click_logs
        (user_id, product_id, event_type, timestamp, session_id, qty, cart_size, amount_usd, discount_pct)
    VALUES
        (:user_id, :product_id, :event_type, :ts, :session_id, :qty, :cart_size, :amount_usd, :discount_pct)
""")

for msg in consumer:
    ev = msg.value
    try:
        with engine.begin() as conn:

            # 1. upsert session
            conn.execute(insert_session_sql, {
                "session_id":  ev["session_id"],
                "customer_id": ev["user_id"],
                "start_time":  ev.get("start_time") or datetime.utcnow().isoformat(),
                "device":      ev.get("device", ""),
                "source":      ev.get("source", ""),
                "country":     ev.get("country", "")
            })

            # 2. insert event
            conn.execute(insert_event_sql, {
                "user_id":     ev["user_id"],
                "product_id":  ev["product_id"],
                "event_type":  ev["event_type"],
                "ts":          ev.get("timestamp", datetime.utcnow().isoformat()),
                "session_id":  ev["session_id"],
                "qty":         ev.get("qty"),
                "cart_size":   ev.get("cart_size"),
                "amount_usd":  ev.get("amount_usd"),
                "discount_pct":ev.get("discount_pct")
            })

        print(f"  logged: user_{ev['user_id']} → product_{ev['product_id']} "
              f"| {ev['event_type']} | {ev.get('country', '?')} | {ev.get('device', '?')}")

    except Exception as e:
        print(f"  error: {e}")