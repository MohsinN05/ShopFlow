# ShopFlow — Real-Time E-Commerce Recommendation Engine

> A full-stack, event-driven recommendation system that tracks user behavior across 760K+ interactions, dynamically updating personalized product rankings based on purchase intent signals, with results streamed live to an analytics dashboard.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [First-Time Setup](#first-time-setup)
- [Running the Project](#running-the-project)
- [Dashboard Features](#dashboard-features)
- [API Reference](#api-reference)
- [Recommendation Model](#recommendation-model)
- [Troubleshooting](#troubleshooting)

---

## Project Overview

ShopFlow is a real-time behavioral recommendation engine built on actual e-commerce data. Events stream through Apache Kafka, scores are computed in real time by Apache Spark Structured Streaming, and results are delivered instantly to a React dashboard via WebSocket.

The system reflects recommendation changes within seconds of user activity — behaving like a production-grade recommendation pipeline rather than a static ML project.

---

## Architecture

```
events_new.csv + sessions.csv
        │
        ▼
kafka_producer.py ──────────────► Kafka Topic: click_events
                                        │
                        ┌───────────────┴───────────────┐
                        ▼                               ▼
              kafka_consumer.py              realtime_recommender.py
                        │                    (Spark Structured Streaming)
                        ▼                               ▼
               user_click_logs                user_recommendations
               sessions (PostgreSQL)           (PostgreSQL)
                        │                               │
                        └───────────────┬───────────────┘
                                        ▼
                              FastAPI Backend
                          (REST APIs + WebSocket)
                                        │
                                        ▼
                               React Frontend
                          (Live Dashboard at :3000)
```

### Data Flow

1. `kafka_producer.py` reads `events_new.csv` + `sessions.csv`, joins them to attach `customer_id` to each event, and replays them chronologically into the `click_events` Kafka topic.
2. `kafka_consumer.py` reads from Kafka and writes each event to `user_click_logs` and each session to the `sessions` table in PostgreSQL.
3. `realtime_recommender.py` (Spark) reads from the same Kafka topic, applies weighted scoring, and upserts aggregated scores into `user_recommendations`.
4. FastAPI serves the frontend via REST endpoints and a WebSocket that streams events directly from Kafka.
5. React polls recommendation endpoints every 3 seconds and receives live events via WebSocket.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event Streaming | Apache Kafka 3.4.2 (KRaft mode) |
| Stream Processing | Apache Spark 4.1.1 Structured Streaming |
| Backend API | FastAPI + SQLAlchemy + aiokafka |
| Database | PostgreSQL |
| Frontend | React 18 + Vite + Recharts |
| Data Processing | pandas |
| Language | Python 3.13 / JavaScript (ES2022) |

---

## Dataset

Seven CSV files located in `ecommerce_data/`:

| File | Rows | Description |
|---|---|---|
| `customers.csv` | 20,000 | User profiles — name, email, country, age |
| `products.csv` | 1,197 | Product catalog — name, category, price, margin |
| `orders.csv` | 33,580 | Completed purchases with payment and device info |
| `order_items.csv` | 59,163 | Line items per order |
| `events_new.csv` | 760,958 | User interactions — page views, add-to-cart, checkout, purchase |
| `sessions.csv` | 120,000 | Browsing sessions with device, source, country |
| `reviews.csv` | 10,780 | Product ratings and review text |

**Static data** (customers, products, orders, order_items, reviews) is loaded once via `seed_db.py`.  
**Real-time data** (events, sessions) streams through Kafka and is written to PostgreSQL as it arrives.

---

## Project Structure

```
ecommerce_pipeline/
├── ecommerce_data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── events_new.csv
│   ├── sessions.csv
│   └── reviews.csv
│
├── backend/
│   ├── .env                     # Your local DB credentials (never commit this)
│   ├── .gitignore               # Excludes .env from version control
│   ├── main.py                  # FastAPI app — all REST + WebSocket endpoints
│   ├── database.py              # SQLAlchemy engine — reads credentials from .env
│   ├── seed_db.py               # One-time static data loader
│   ├── kafka_producer.py        # Replays events_new.csv into Kafka
│   ├── kafka_consumer.py        # Writes Kafka events → PostgreSQL
│   └── realtime_recommender.py  # Spark Structured Streaming job
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx
│   │   ├── index.css
│   │   ├── api.js
│   │   └── components/
│   │       ├── EventStream.jsx
│   │       ├── RecommendationPanel.jsx
│   │       ├── TopProducts.jsx
│   │       ├── SalesTrend.jsx
│   │       ├── CategorySales.jsx
│   │       ├── TopUsers.jsx
│   │       ├── DeviceStats.jsx
│   │       └── CountryMap.jsx
│   ├── package.json
│   └── vite.config.js
│
└── README.md
```

---

## Environment Configuration

Credentials are stored in a `.env` file inside the `backend/` folder and are never committed to version control.

### 1. Create the file

```bash
cd ~/ecommerce_pipeline/backend
touch .env
```

### 2. Add your credentials

```env
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_db
```

### 3. Add to .gitignore

```bash
echo ".env" >> ~/ecommerce_pipeline/backend/.gitignore
```

### 4. Install python-dotenv

```bash
source ../venv/bin/activate
pip install python-dotenv
```

> Anyone cloning this project on a new machine must create their own `.env` with their local PostgreSQL credentials before running anything. The app will not start without it.

---

## Prerequisites

Make sure these are installed inside WSL (Debian/Ubuntu):

- Python 3.13 with `pip`
- Node.js (via `nvm`) — **must be WSL-native, not Windows**
- Java 11+ (required for Kafka and Spark)
- Apache Kafka 3.4.2 at `~/kafka`
- Apache Spark 4.1.1 with `spark-submit` on PATH
- PostgreSQL running as a service
- A Python virtual environment at `ecommerce_pipeline/venv`

### Python dependencies

```bash
cd ~/ecommerce_pipeline
source venv/bin/activate
pip install fastapi uvicorn[standard] sqlalchemy psycopg2-binary \
            kafka-python aiokafka pandas pyspark
```

### Node dependencies

```bash
cd ~/ecommerce_pipeline/frontend
npm install
```

> **Important:** always run `npm` from inside WSL terminal. Never run it from Windows PowerShell or CMD — it will fail with UNC path errors.

---

## First-Time Setup

These steps are run **once only** when setting up the project from scratch.

### 1. Drop and recreate the database (if needed)

Connect to PostgreSQL and run:

```sql
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    )
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;
```

### 2. Seed static data

```bash
cd ~/ecommerce_pipeline/backend
source ../venv/bin/activate
python seed_db.py
```

This loads customers, products, orders, order_items, and reviews into PostgreSQL. It also clears the `sessions`, `user_click_logs`, and `user_recommendations` tables for a fresh start.

Expected output:
```
=== Seeding PostgreSQL with static data ===
  loaded customers.csv: 20000 rows
  loaded products.csv: 1197 rows
  loaded orders.csv: 33580 rows
  loaded order_items.csv: 59163 rows
  loaded reviews.csv: 10780 rows
-- Creating tables...
-- Loading static tables...
-- Clearing real-time tables for fresh start...
=== Done! Run kafka_producer.py to start streaming. ===
```

### 3. Format Kafka storage (first time only)

```bash
cd ~/kafka
bin/kafka-storage.sh format --standalone -t $(bin/kafka-storage.sh random-uuid) -c config/server.properties
```

> Only run this once. Running it again will wipe all Kafka topics and offsets.

---

## Running the Project

Open **5 separate WSL terminals** and run each command in order. Wait for each service to be ready before starting the next.

---

### Terminal 1 — Kafka

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

Wait until you see:
```
Kafka Server started
```

---

### Terminal 2 — FastAPI Backend

```bash
cd ~/ecommerce_pipeline/backend
source ../venv/bin/activate
uvicorn main:app --reload --port 8000
```

Wait until you see:
```
Application startup complete.
```

Verify it's running:
```bash
curl http://localhost:8000/
# → {"message":"Recommendation API running"}
```

---

### Terminal 3 — Spark Recommender

```bash
cd ~/ecommerce_pipeline/backend
source ../venv/bin/activate
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 realtime_recommender.py
```

Wait until you see:
```
Streaming query started
```

> Spark will download the Kafka connector on first run — this may take a few minutes.

---

### Terminal 4 — Kafka Producer

```bash
cd ~/ecommerce_pipeline/backend
source ../venv/bin/activate
python kafka_producer.py --speed 5
```

This replays `events_new.csv` at 5 events per second into the `click_events` Kafka topic.

Speed options:
```bash
python kafka_producer.py              # 1 event/sec (default)
python kafka_producer.py --speed 5   # 5 events/sec
python kafka_producer.py --speed 20  # 20 events/sec
python kafka_producer.py --speed 0   # as fast as possible
```

---

### Terminal 5 — Kafka Consumer

```bash
cd ~/ecommerce_pipeline/backend
source ../venv/bin/activate
python kafka_consumer.py
```

This reads events from Kafka and writes them to `user_click_logs` and `sessions` in PostgreSQL — powering the KPIs, event stream, country map, and device stats on the dashboard.

---

### Terminal 6 — React Frontend

```bash
cd ~/ecommerce_pipeline/frontend
npm run dev
```

Open your browser at:
```
http://localhost:3000
```

---

## Dashboard Features

| Panel | Description | Data Source |
|---|---|---|
| **KPI Strip** | Total events, active users, purchases, avg score for the current data day | `user_click_logs` |
| **Live Event Stream** | Real-time feed of user interactions via WebSocket | Kafka → WebSocket |
| **Sales Trend** | Line chart of revenue + order count, filterable by granularity (hour/day/month/year) and date range | `user_click_logs` (session-deduplicated) |
| **Top Products** | Bar chart of most interacted products by weighted score | Live event stream |
| **Top Users** | Top 3 users by total interaction score with real names | `user_click_logs` + `customers` |
| **Recommendations** | Personalized ranked product recommendations per user (search by user ID 1–20000) | `user_recommendations` |
| **Sales by Category** | Revenue breakdown by product category from streamed events | `user_click_logs` + `products` |
| **Traffic by Device** | Donut chart of mobile/desktop/tablet split | `sessions` |
| **Traffic by Source** | Donut chart of organic/paid/email/direct split | `sessions` |
| **Active Users by Country** | World map with color intensity showing user distribution for current day | `sessions` |

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Health check |
| GET | `/search?query=&min_price=&max_price=` | Product search |
| GET | `/recommend/top` | Top products across all users |
| GET | `/recommend/{user_id}` | Personalized recommendations for a user |
| GET | `/recent-events` | Last 30 events from click logs |
| POST | `/log-click?user_id=&product_id=&event_type=` | Log a manual event |
| GET | `/stats/daily` | KPI stats for current data day |
| GET | `/stats/sales-trend` | Sales over time (session-deduplicated) |
| GET | `/stats/sales-trend/filters` | Available years/months/days from data |
| GET | `/stats/top-categories` | Revenue by product category |
| GET | `/stats/active-countries` | Active users by country (current day) |
| GET | `/stats/top-users` | Top 3 users by interaction score |
| GET | `/stats/devices` | Device and source breakdown |
| WS | `/ws/events` | WebSocket — live event stream from Kafka |

---

## Recommendation Model

ShopFlow uses a **weighted implicit feedback scoring model** — purchase intent is inferred from user behavior rather than explicit ratings.

### Scoring weights

| Event Type | Score | Reasoning |
|---|---|---|
| `page_view` | 1 | Low intent — passive browsing |
| `add_to_cart` | 5 | Medium intent — actively considering |
| `checkout` | 7 | High intent — about to purchase |
| `purchase` | 10 | Strongest signal — confirmed buy |

### How it works

1. Each event from Kafka is scored based on its type.
2. Spark aggregates scores per `(user_id, product_id)` pair using `SUM`.
3. Products are ranked per user by descending score.
4. The frontend polls `/recommend/{user_id}` every 3 seconds to reflect updates.

A user who viewed product A three times and added it to cart once gets:
```
score(A) = (3 × 1) + (1 × 5) = 8
```

If they then purchase it:
```
score(A) = 8 + 10 = 18  →  moves to top of recommendations
```

---

## Troubleshooting

### npm fails with UNC path errors
You are running npm from Windows. Open a WSL terminal and run from there.

### Kafka fails to start — `NoSuchFileException: config/kraft/server.properties`
Your Kafka uses `config/server.properties`, not `config/kraft/server.properties`. Use:
```bash
bin/kafka-server-start.sh config/server.properties
```

### Kafka format error — `controller.quorum.voters is not set`
Use the `--standalone` flag:
```bash
bin/kafka-storage.sh format --standalone -t $(bin/kafka-storage.sh random-uuid) -c config/server.properties
```

### Dashboard shows "disconnected"
FastAPI is missing the `uvicorn[standard]` package. Run:
```bash
pip install 'uvicorn[standard]'
```

### KPIs stuck at 0
`kafka_consumer.py` is not running. Start it in a new terminal. Then restart `kafka_producer.py` to stream fresh events.

### psql peer authentication error
Use `-h localhost` to force TCP connection:
```bash
psql -U postgres -d ecommerce_db -h localhost
```

### Recommendations not updating
Check that `realtime_recommender.py` is running and that the Kafka topic name in all files is `click_events`.

### Sales trend shows nothing
Events need `event_type = 'purchase'` and a non-null `amount_usd` to appear in the sales trend. Run the producer long enough for purchase events to stream through.