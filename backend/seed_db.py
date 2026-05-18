"""
seed_db.py
Run once to load all CSV data into PostgreSQL.
Usage: python seed_db.py
"""

import pandas as pd
from sqlalchemy import text
from database import engine
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "ecommerce_data"

# ── helpers ──────────────────────────────────────────────────────────────────

def load(filename):
    path = DATA_DIR / filename
    df = pd.read_csv(path)
    print(f"  loaded {filename}: {len(df)} rows")
    return df

def run_sql(sql, conn):
    conn.execute(text(sql))

# ── 1. create tables ──────────────────────────────────────────────────────────

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id   INT PRIMARY KEY,
    name          TEXT,
    email         TEXT,
    country       TEXT,
    age           INT,
    signup_date   TEXT,
    marketing_opt_in BOOLEAN
);

CREATE TABLE IF NOT EXISTS products (
    product_id    INT PRIMARY KEY,
    category      TEXT,
    name          TEXT,
    price_usd     NUMERIC,
    cost_usd      NUMERIC,
    margin_usd    NUMERIC
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        INT PRIMARY KEY,
    customer_id     INT,
    order_time      TIMESTAMP,
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

CREATE TABLE IF NOT EXISTS sessions (
    session_id  INT PRIMARY KEY,
    customer_id INT,
    start_time  TIMESTAMP,
    device      TEXT,
    source      TEXT,
    country     TEXT
);

CREATE TABLE IF NOT EXISTS events (
    event_id     BIGINT PRIMARY KEY,
    session_id   INT,
    timestamp    TIMESTAMP,
    event_type   TEXT,
    product_id   INT,
    qty          NUMERIC,
    cart_size    NUMERIC,
    payment      TEXT,
    discount_pct NUMERIC,
    amount_usd   NUMERIC
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id    INT PRIMARY KEY,
    order_id     INT,
    product_id   INT,
    rating       INT,
    review_text  TEXT,
    review_time  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_click_logs (
    id           SERIAL PRIMARY KEY,
    user_id      INT,
    product_id   INT,
    event_type   TEXT,
    timestamp    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id          INT,
    product_id       INT,
    popularity_score NUMERIC DEFAULT 0,
    rank             INT DEFAULT 999,
    PRIMARY KEY (user_id, product_id)
);
"""

# ── 2. seed from events + sessions → user_recommendations ────────────────────

SCORE_MAP = {
    "page_view":    1,
    "add_to_cart":  5,
    "purchase":     10,
    "checkout":     7,
}

def seed_recommendations(conn):
    """
    Join events → sessions to get customer_id, compute scores,
    insert into user_recommendations.
    """
    print("  computing recommendation scores from events + sessions...")

    # Step 1: truncate and drop any old version, recreate with proper PK
    conn.execute(text("DROP TABLE IF EXISTS user_recommendations"))
    conn.execute(text("""
        CREATE TABLE user_recommendations (
            user_id          INT,
            product_id       INT,
            popularity_score NUMERIC DEFAULT 0,
            rank             INT DEFAULT 999,
            PRIMARY KEY (user_id, product_id)
        )
    """))

    # Step 2: compute and insert scores cleanly (no conflict possible on fresh table)
    conn.execute(text("""
        INSERT INTO user_recommendations (user_id, product_id, popularity_score, rank)
        SELECT
            user_id,
            product_id,
            popularity_score,
            ROW_NUMBER() OVER (
                PARTITION BY user_id
                ORDER BY popularity_score DESC
            ) AS rank
        FROM (
            SELECT
                s.customer_id AS user_id,
                e.product_id,
                SUM(
                    CASE e.event_type
                        WHEN 'page_view'   THEN 1
                        WHEN 'add_to_cart' THEN 5
                        WHEN 'checkout'    THEN 7
                        WHEN 'purchase'    THEN 10
                        ELSE 1
                    END
                ) AS popularity_score
            FROM events e
            JOIN sessions s ON e.session_id = s.session_id
            WHERE e.product_id IS NOT NULL
            GROUP BY s.customer_id, e.product_id
        ) scored
    """))

    print("  user_recommendations seeded.")


# ── 3. main ───────────────────────────────────────────────────────────────────

def main():
    print("\n=== Seeding PostgreSQL from CSVs ===\n")

    customers   = load("customers.csv")
    products    = load("products.csv")
    orders      = load("orders.csv")
    order_items = load("order_items.csv")
    sessions    = load("sessions.csv")
    events      = load("events.csv")
    reviews     = load("reviews.csv")

    # Normalise column names
    customers.columns   = customers.columns.str.lower().str.strip()
    products.columns    = products.columns.str.lower().str.strip()
    orders.columns      = orders.columns.str.lower().str.strip()
    order_items.columns = order_items.columns.str.lower().str.strip()
    sessions.columns    = sessions.columns.str.lower().str.strip()
    events.columns      = events.columns.str.lower().str.strip()
    reviews.columns     = reviews.columns.str.lower().str.strip()

    with engine.begin() as conn:
        print("\n-- Creating tables...")
        for statement in CREATE_TABLES.strip().split(";"):
            s = statement.strip()
            if s:
                conn.execute(text(s))

        print("\n-- Loading tables...")
        customers.to_sql("customers",   conn, if_exists="replace", index=False)
        print("  customers done")
        products.to_sql("products",     conn, if_exists="replace", index=False)
        print("  products done")
        orders.to_sql("orders",         conn, if_exists="replace", index=False)
        print("  orders done")
        order_items.to_sql("order_items", conn, if_exists="replace", index=False)
        print("  order_items done")
        sessions.to_sql("sessions",     conn, if_exists="replace", index=False)
        print("  sessions done")
        events.to_sql("events",         conn, if_exists="replace", index=False)
        print("  events done")
        reviews.to_sql("reviews",       conn, if_exists="replace", index=False)
        print("  reviews done")

        print("\n-- Building user_recommendations...")
        seed_recommendations(conn)

    print("\n=== Done! PostgreSQL is seeded. ===\n")


if __name__ == "__main__":
    main()
