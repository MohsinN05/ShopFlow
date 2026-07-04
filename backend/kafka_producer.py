"""
kafka_producer.py
Replays events_new.csv + sessions.csv through Kafka topic 'click_events'.

Usage:
    python kafka_producer.py              # 1 event/sec
    python kafka_producer.py --speed 5   # 5 events/sec
    python kafka_producer.py --speed 0   # as fast as possible
"""

import json
import time
import argparse
import pandas as pd
from pathlib import Path
from kafka import KafkaProducer
from datetime import datetime

DATA_DIR = Path(__file__).resolve().parent.parent / "ecommerce_data"

SCORE_MAP = {
    "page_view":   1,
    "add_to_cart": 5,
    "checkout":    7,
    "purchase":    10,
}

def load_events():
    events   = pd.read_csv(DATA_DIR / "events_new.csv")
    sessions = pd.read_csv(DATA_DIR / "sessions.csv")

    events.columns   = events.columns.str.lower().str.strip()
    sessions.columns = sessions.columns.str.lower().str.strip()

    # Join events with full session info
    merged = events.merge(sessions, on="session_id", how="left")

    # Drop rows without product_id or customer_id
    merged = merged.dropna(subset=["product_id", "customer_id"])
    merged["product_id"]  = merged["product_id"].astype(int)
    merged["customer_id"] = merged["customer_id"].astype(int)
    merged["session_id"]  = merged["session_id"].astype(int)

    # Fill NaN with None for optional fields
    for col in ["qty", "cart_size", "amount_usd", "discount_pct"]:
        if col in merged.columns:
            merged[col] = merged[col].where(merged[col].notna(), None)

    merged = merged.sort_values("timestamp").reset_index(drop=True)
    print(f"  loaded {len(merged)} events to replay")
    return merged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=float, default=1.0,
                        help="events per second (0 = no delay)")
    parser.add_argument("--topic", default="click_events",
                        help="Kafka topic name")
    args = parser.parse_args()

    print(f"\n=== Kafka Producer: replaying events → topic '{args.topic}' ===")
    print(f"    speed: {args.speed} events/sec\n")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )

    df = load_events()
    delay = 1.0 / args.speed if args.speed > 0 else 0

    for i, row in df.iterrows():
        event_type = str(row.get("event_type", "page_view")).strip()

        message = {
            # event fields
            "user_id":     int(row["customer_id"]),
            "product_id":  int(row["product_id"]),
            "event_type":  event_type,
            "score":       SCORE_MAP.get(event_type, 1),
            "timestamp":   str(row.get("timestamp", datetime.utcnow().isoformat())),
            "qty":         float(row["qty"])         if pd.notna(row.get("qty"))         else None,
            "cart_size":   float(row["cart_size"])   if pd.notna(row.get("cart_size"))   else None,
            "amount_usd":  float(row["amount_usd"])  if pd.notna(row.get("amount_usd"))  else None,
            "discount_pct":float(row["discount_pct"])if pd.notna(row.get("discount_pct"))else None,
            # session fields
            "session_id":  int(row["session_id"]),
            "device":      str(row.get("device", "")),
            "source":      str(row.get("source", "")),
            "country":     str(row.get("country", "")),
            "start_time":  str(row.get("start_time", "")),
        }

        producer.send(args.topic, message)

        print(f"  [{i+1}/{len(df)}] user_{message['user_id']} → "
              f"product_{message['product_id']} | {event_type} | "
              f"country={message['country']} | score={message['score']}")

        if delay:
            time.sleep(delay)

    producer.flush()
    print("\n=== Replay complete ===")


if __name__ == "__main__":
    main()