"""
kafka_producer.py
Replays events.csv through Kafka topic 'click_events'.
Replaces the fake JS simulation with real historical data.

Usage:
    python kafka_producer.py              # replay at 1 event/sec
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
    events   = pd.read_csv(DATA_DIR / "events.csv")
    sessions = pd.read_csv(DATA_DIR / "sessions.csv")

    events.columns   = events.columns.str.lower().str.strip()
    sessions.columns = sessions.columns.str.lower().str.strip()

    # Join to get customer_id from session
    merged = events.merge(sessions[["session_id", "customer_id"]], on="session_id", how="left")

    # Drop rows without product_id or customer_id
    merged = merged.dropna(subset=["product_id", "customer_id"])
    merged["product_id"]   = merged["product_id"].astype(int)
    merged["customer_id"]  = merged["customer_id"].astype(int)

    # Sort by timestamp so replay is chronological
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

    print(f"\n=== Kafka Producer: replaying events.csv → topic '{args.topic}' ===")
    print(f"    speed: {args.speed} events/sec\n")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    df = load_events()
    delay = 1.0 / args.speed if args.speed > 0 else 0

    for i, row in df.iterrows():
        event_type = str(row.get("event_type", "page_view")).strip()

        message = {
            "user_id":    int(row["customer_id"]),
            "product_id": int(row["product_id"]),
            "event_type": event_type,
            "score":      SCORE_MAP.get(event_type, 1),
            "timestamp":  str(row.get("timestamp", datetime.utcnow().isoformat())),
            "session_id": int(row["session_id"]),
        }

        producer.send(args.topic, message)

        print(f"  [{i+1}/{len(df)}] user_{message['user_id']} → "
              f"product_{message['product_id']} | {event_type} | score={message['score']}")

        if delay:
            time.sleep(delay)

    producer.flush()
    print("\n=== Replay complete ===")


if __name__ == "__main__":
    main()
