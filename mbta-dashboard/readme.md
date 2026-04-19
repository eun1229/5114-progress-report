# MBTA Bus Performance Dashboard

**CSE 5114 Data Manipulation and Management at Scale**  
**Washington University in St. Louis**  
**Group 1: Grace Lee, Mijung Jung, Duy Huynh**

---

## Overview

A real-time and historical performance dashboard for MBTA bus routes in Boston, MA.
Built to support transparency and accountability efforts at the MBTA — notably,
the MBTA's own public dashboard has no metrics for bus routes specifically.

---

## Pipeline Architecture

```
MBTA GTFS-RT API (every 60 seconds)
         ↓
    AWS Lambda
    (fetches protobuf .pb files)
         ↓
    AWS S3
    (raw protobuf snapshots, partitioned by date/hour)
         ↓
    Apache Spark
    (decodes protobuf → structured rows)
         ↓
    Snowflake — LEMMING_DB
    ├── FINAL_PROJECT_RAW    ← decoded snapshots
    ├── FINAL_PROJECT_STATIC ← GTFS schedule (dim tables)
    ├── FINAL_PROJECT_FACT   ← cleaned, joined events
    └── FINAL_PROJECT_MART   ← aggregated metrics for dashboard
         ↓
    Streamlit Dashboard
    (queries MART tables + live MBTA API)
```

---

## Dashboard Features

### Live right now tab
- Queries the MBTA GTFS-RT API **directly** — data is seconds old
- Live map of every active bus in Boston
- Active buses by route
- Current occupancy status breakdown
- Active service alerts

### Occupancy by route tab
- Average occupancy % per route from Snowflake MART tables
- Stacked bar chart showing empty / few seats / standing / full breakdown
- Sortable detail table of most crowded routes

### Occupancy by hour tab
- Line chart showing when buses are most crowded throughout the day
- AM peak (7–9am) and PM peak (4–7pm) highlighted
- Heatmap: which routes are crowded at which hours

### Stop events tab
- Total stop events, unique trips, unique vehicles
- Daily event volume chart
- Top 15 busiest stops and routes

---

## File Structure

```
mbta-dashboard/
├── dashboard.py          ← main Streamlit app
├── live_tab.py           ← live MBTA API functions (vehicle positions, alerts)
├── snowflake_queries.py  ← Snowflake connection and all SQL queries
├── utils.py              ← shared helper functions (formatting, filters)
├── README.md             ← this file
└── .streamlit/
    └── secrets.toml      ← credentials (not committed to git)
```

---

## Setup

### Prerequisites
```bash
pip install streamlit plotly snowflake-connector-python pandas cryptography gtfs-realtime-bindings requests
```

### Credentials
Create `.streamlit/secrets.toml`:
```toml
SF_USER             = "your_snowflake_username"
SF_ACCOUNT          = "UNB02139"
SF_WAREHOUSE        = "LEMMING_WH"
SF_PRIVATE_KEY_PATH = "/path/to/rsa_key.p8"
```

### Run
```bash
cd mbta-dashboard
streamlit run dashboard.py
```

Open `http://localhost:8501` in your browser.

---

## Data Sources

- **Live data:** [MBTA GTFS-RT API](https://www.mbta.com/developers/v3-api) — vehicle positions, trip updates, alerts (public, no API key required)
- **Static schedule:** [MBTA GTFS](https://www.mbta.com/developers/gtfs) — routes, stops, trips, stop times
- **Collection period:** March 2026 – present

---

## Key Technical Decisions

**Why Lambda instead of Kafka for ingestion?**  
For a semester project collecting data every 60 seconds, Lambda + EventBridge provides sufficient freshness with minimal operational overhead. A production system handling millions of concurrent users would use Kafka for its buffering and replay capabilities.

**Why direct API for the live tab?**  
Querying Snowflake for live data would introduce 1–24 hour latency depending on when the Spark job last ran. Querying the MBTA GTFS-RT API directly gives us data that is seconds old with no additional infrastructure.

**Why Parquet in S3?**  
Columnar format allows Spark to read only the columns it needs (predicate pushdown), significantly reducing I/O when processing large date ranges.

**Why RSA key-pair auth for Snowflake?**  
More secure than password auth — private key never transmitted over the network.


# Airflow (trying to run it locally)
python3.11 -m venv airflow-env
source airflow-env/bin/activate
pip install "apache-airflow==2.8.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.11.txt"

# Airflow (not completed)
airflow db init

airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com