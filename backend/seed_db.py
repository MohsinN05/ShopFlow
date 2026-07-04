"""
seed_db.py
Run once to load static CSV data into PostgreSQL.
Usage: python seed_db.py
"""

import pandas as pd
from sqlalchemy import text
from database import engine
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "ecommerce_data"

def load(filename):
    path = DATA_DIR / filename
    df = pd.read_csv(path)
    print(f"  loaded {filename}: {len(df)} rows")
    return df

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id      INT PRIMARY KEY,
    name             TEXT,
    email            TEXT,
    country          TEXT,
    age              INT,
    signup_date      TEXT,
    marketing_opt_in BOOLEAN
);

CREATE TABLE IF NOT EXISTS products (
    product_id  INT PRIMARY KEY,
    category    TEXT,
    name        TEXT,
    price_usd   NUMERIC,
    cost_usd    NUMERIC,
    margin_usd  NUMERIC
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        INT PRIMARY KEY,
    customer_id     INT,
    order_time      TIMESTAMPTZ,
    payment_method  TEXT,
    discount_pct    NUMERIC,
    subtotal_usd    NUMERIC,
    total_usd       NUMERIC,
    country         TEXT,
    device          TEXT,
    source          TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id        INT,
    product_id      INT,
    unit_price_usd  NUMERIC,
    quantity        INT,
    line_total_usd  NUMERIC
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id    INT PRIMARY KEY,
    order_id     INT,
    product_id   INT,
    rating       INT,
    review_text  TEXT,
    review_time  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id  BIGINT PRIMARY KEY,
    customer_id INT,
    start_time  TIMESTAMPTZ,
    device      TEXT,
    source      TEXT,
    country     TEXT
);

CREATE TABLE IF NOT EXISTS user_click_logs (
    id           SERIAL PRIMARY KEY,
    user_id      INT,
    product_id   INT,
    event_type   TEXT,
    timestamp    TIMESTAMPTZ,
    session_id   BIGINT,
    qty          NUMERIC,
    cart_size    NUMERIC,
    amount_usd   NUMERIC,
    discount_pct NUMERIC
);

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id          INT,
    product_id       INT,
    popularity_score NUMERIC DEFAULT 0,
    rank             INT DEFAULT 999,
    PRIMARY KEY (user_id, product_id)
);
"""

def main():
    print("\n=== Seeding PostgreSQL with static data ===\n")

    customers   = load("customers.csv")
    products    = load("products.csv")
    orders      = load("orders.csv")
    order_items = load("order_items.csv")
    reviews     = load("reviews.csv")

    for df in [customers, products, orders, order_items, reviews]:
        df.columns = df.columns.str.lower().str.strip()

    with engine.begin() as conn:
        print("-- Creating tables...")
        for stmt in CREATE_TABLES.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

        print("\n-- Loading static tables...")
        customers.to_sql("customers",     conn, if_exists="replace", index=False)
        print("  customers done")
        products.to_sql("products",       conn, if_exists="replace", index=False)
        print("  products done")
        orders.to_sql("orders",           conn, if_exists="replace", index=False)
        print("  orders done")
        order_items.to_sql("order_items", conn, if_exists="replace", index=False)
        print("  order_items done")
        reviews.to_sql("reviews",         conn, if_exists="replace", index=False)
        print("  reviews done")

        print("\n-- Clearing real-time tables for fresh start...")
        conn.execute(text("TRUNCATE TABLE sessions RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE user_click_logs RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE user_recommendations RESTART IDENTITY CASCADE"))
        print("  sessions, user_click_logs, user_recommendations cleared")

    print("\n=== Done! Run kafka_producer.py to start streaming. ===\n")

if __name__ == "__main__":
    main()