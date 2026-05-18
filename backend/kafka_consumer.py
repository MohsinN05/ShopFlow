"""
kafka_consumer.py
Consumes events from Kafka and writes them to user_click_logs table.
Run this alongside kafka_producer.py so the event stream shows in the GUI.
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

print("=== Kafka Consumer: writing click_events → user_click_logs ===")

insert_sql = text("""
    INSERT INTO user_click_logs (user_id, product_id, event_type, timestamp)
    VALUES (:user_id, :product_id, :event_type, :ts)
""")

for msg in consumer:
    ev = msg.value
    try:
        with engine.begin() as conn:
            conn.execute(insert_sql, {
                "user_id":    ev["user_id"],
                "product_id": ev["product_id"],
                "event_type": ev["event_type"],
                "ts":         ev.get("timestamp", datetime.utcnow().isoformat())
            })
        print(f"  logged: user_{ev['user_id']} → product_{ev['product_id']} | {ev['event_type']}")
    except Exception as e:
        print(f"  error: {e}")