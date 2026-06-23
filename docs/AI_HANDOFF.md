# AI Handoff — SNAIC-sg-commute-pulse

This document lets another Claude session (or any developer) continue this project
from scratch with no prior chat history.

---

## What This Project Does

A **calendar-aware Singapore commute recommendation system**.

Given a user's next calendar event, the pipeline:
1. Parses the event to extract destination and start time
2. Geocodes the destination via OneMap Singapore
3. Fetches: routing options (OneMap), real-time bus arrivals (LTA), train disruption alerts (LTA), weather forecast (data.gov.sg)
4. Stores all data in DuckDB (embedded SQL database — single file, no server)
5. Runs SQL transformation to rank routes, calculate leave-by time, apply weather/disruption penalties
6. Serves the recommendation via Streamlit dashboard + FastAPI REST endpoint

**Output:** "Leave by 09:23 AM — take Bus 65 then EWL. ⚠ Rain expected, avoid long walks."

---

## Repo

| Field | Value |
|---|---|
| GitHub | `https://github.com/DLim86/SNAIC-sg-commute-pulse` |
| Owner | DLim86 |
| Local path | `e:\SNAIC\Week 2\Assessment` |
| Branch | `main` |
| Assessment deadline | Sept 14 2026 |
| Target submission | August 2026 |

---

## Current State

### Done
- Project folder structure created
- `.gitignore` protects credentials and database files
- `config_example.py` — credential template (real keys in `config.py`, gitignored)
- `requirements.txt` — all dependencies pinned
- `README.md` — project overview for GitHub
- `docs/roadmap.html` — interactive 12-station roadmap with code examples
- DuckDB schema fully designed (7 tables)
- Core SQL view `v_enriched_routes` designed with window functions
- All API endpoints researched and documented

### Not yet written (scripts/ folder is empty)
Every file below needs to be created from scratch:

| File | Purpose | Build order |
|---|---|---|
| `scripts/schema.py` | Create DuckDB tables | **1st** |
| `scripts/ingest.py` | Fetch all 4 APIs + retry/backoff + upsert | 2nd |
| `scripts/transform.py` | SQL transformation, populate recommendations | 3rd |
| `scripts/serve.py` | Streamlit dashboard | 4th |
| `scripts/__init__.py` | Makes scripts importable (needed for Airflow) | with schema.py |
| `scripts/api.py` | FastAPI serving layer | 5th |
| `dags/__init__.py` | Airflow DAG folder init | with DAG |
| `dags/commute_pipeline_dag.py` | Airflow DAG orchestration | 6th |
| `docker-compose.yml` | Container orchestration | 7th |
| `Dockerfile` | Shared container image | 7th |

---

## Install & Run

```bash
# 1. Clone
git clone https://github.com/DLim86/SNAIC-sg-commute-pulse.git
cd SNAIC-sg-commute-pulse

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your API keys
copy config_example.py config.py
# Edit config.py with your real keys — NEVER commit config.py

# 5. Create the database schema (run once)
python scripts/schema.py

# 6. Run the ingestion pipeline
python scripts/ingest.py

# 7. Run the transformation
python scripts/transform.py

# 8. Open the dashboard
streamlit run scripts/serve.py
# → http://localhost:8501

# 9. (Optional) Start the API server
uvicorn scripts.api:app --reload --port 8000
# → http://localhost:8000/docs  (Swagger UI)
```

---

## Environment Variables

All secrets go in `config.py` (gitignored). Never commit this file.

```python
# config.py — copy from config_example.py and fill in real values
LTA_API_KEY = "<LTA_API_KEY>"       # From datamall.lta.gov.sg — free, approval 1-2 days
ONEMAP_EMAIL = "<ONEMAP_EMAIL>"     # Your onemap.gov.sg account email
ONEMAP_PASSWORD = "<ONEMAP_PASSWORD>"  # Your onemap.gov.sg password
```

For Docker, create a `.env` file (also gitignored):
```
LTA_API_KEY=<LTA_API_KEY>
ONEMAP_EMAIL=<ONEMAP_EMAIL>
ONEMAP_PASSWORD=<ONEMAP_PASSWORD>
```

**Note:** There is no `.env` file yet. Create it when adding Docker support.

---

## APIs

### LTA DataMall
- **Registration:** https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html
- **Auth:** HTTP header `AccountKey: <LTA_API_KEY>`
- **Endpoints used:**

| Endpoint | What it returns |
|---|---|
| `GET https://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2?BusStopCode={code}` | Next 3 bus arrivals for a stop: estimated time, load (SEA/SDA/LSD) |
| `GET https://datamall2.mytransport.sg/ltaodataservice/TrainServiceAlerts` | Active MRT disruptions: affected line, message, severity |

### OneMap Singapore
- **Registration:** https://www.onemap.gov.sg (requires SingPass, instant)
- **Auth:** POST token, then use as Authorization header
- **Token endpoint:** `POST https://www.onemap.gov.sg/api/auth/post/getToken`
  - Body: `{"email": "<ONEMAP_EMAIL>", "password": "<ONEMAP_PASSWORD>"}`
  - Returns: `{"access_token": "...", "expiry_timestamp": "..."}`
  - **Token expires every 3 days** — always refresh at pipeline start
- **Endpoints used:**

| Endpoint | What it returns |
|---|---|
| `GET https://www.onemap.gov.sg/api/common/elastic/search?searchVal={address}&returnGeom=Y&getAddrDetails=Y` | GPS coordinates for a Singapore address |
| `GET https://www.onemap.gov.sg/api/public/routingsvc/route?start={lat,lng}&end={lat,lng}&routeType=pt&mode=TRANSIT&numItineraries=3` | 3 public transport route options with duration, walk distance, fare, steps |

### data.gov.sg Weather
- **Registration:** None required — open API
- **Endpoint:** `GET https://api.data.gov.sg/v1/environment/2-hour-weather-forecast`
- **Returns:** Per-area weather forecast (e.g. "Ang Mo Kio": "Moderate Rain") for the next 2 hours

---

## Database Schema

**File:** `db/commute.duckdb` (gitignored — never commit)
**Engine:** DuckDB 0.10 (embedded, no server)

```sql
CREATE TABLE IF NOT EXISTS calendar_events (
    event_id     VARCHAR PRIMARY KEY,
    title        VARCHAR,
    start_time   TIMESTAMPTZ,        -- Singapore time +08:00
    location_raw VARCHAR,
    dest_lat     DOUBLE,
    dest_lng     DOUBLE,
    ingested_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS route_options (
    option_id          VARCHAR PRIMARY KEY,
    event_id           VARCHAR REFERENCES calendar_events(event_id),
    total_duration_min INTEGER,
    walk_distance_m    INTEGER,
    num_transfers      INTEGER,
    fare               DECIMAL(4,2),
    fetched_at         TIMESTAMP
);

CREATE TABLE IF NOT EXISTS weather_forecast (
    area        VARCHAR,
    forecast    VARCHAR,
    is_rainy    BOOLEAN,
    valid_start TIMESTAMPTZ,
    valid_end   TIMESTAMPTZ,
    fetched_at  TIMESTAMP,
    PRIMARY KEY (area, valid_start)
);

CREATE TABLE IF NOT EXISTS bus_arrivals (
    bus_stop_code VARCHAR,
    service_no    VARCHAR,
    next_bus_mins INTEGER,
    load          VARCHAR,   -- SEA = seats available, SDA = standing, LSD = limited standing
    fetched_at    TIMESTAMP,
    PRIMARY KEY (bus_stop_code, service_no, fetched_at)
);

CREATE TABLE IF NOT EXISTS train_alerts (
    alert_id      VARCHAR PRIMARY KEY,
    affected_line VARCHAR,
    message       VARCHAR,
    severity      VARCHAR,   -- 'HEAVY', 'MODERATE', etc.
    fetched_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recommendations (
    event_id           VARCHAR PRIMARY KEY,
    recommended_mode   VARCHAR,
    total_duration_min INTEGER,
    leave_by           TIMESTAMPTZ,
    estimated_arrival  TIMESTAMPTZ,
    weather_warning    VARCHAR,
    disruption_warning VARCHAR,
    reason             VARCHAR,
    created_at         TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        VARCHAR PRIMARY KEY,
    source        VARCHAR,       -- 'lta_bus', 'weather', 'onemap_route', etc.
    rows_upserted INTEGER,
    duration_ms   INTEGER,
    status        VARCHAR,       -- 'success' or 'error'
    error_msg     VARCHAR,
    ran_at        TIMESTAMP DEFAULT now()
);
```

---

## Core SQL Transformation

This view is the heart of the recommendation logic. Write it in `scripts/transform.py`.

```sql
CREATE OR REPLACE VIEW v_enriched_routes AS
SELECT
    r.option_id,
    r.event_id,
    r.total_duration_min,
    r.walk_distance_m,
    r.num_transfers,
    r.fare,
    e.start_time,
    e.title,
    e.dest_lat,
    e.dest_lng,
    -- Leave-by time: event start minus travel time minus 10-minute buffer
    e.start_time - INTERVAL (r.total_duration_min + 10) MINUTE AS leave_by,
    w.forecast AS weather_forecast,
    w.is_rainy,
    CASE WHEN ta.alert_id IS NOT NULL THEN ta.message ELSE NULL END AS alert_msg,
    CASE
        WHEN w.is_rainy AND r.walk_distance_m > 400
            THEN '⚠ Rainy — take covered transport'
        WHEN ta.alert_id IS NOT NULL
            THEN '⚠ MRT disruption — add 20 min buffer'
        WHEN r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest option'
        ELSE 'Alternative route'
    END AS recommendation_reason,
    -- Rank routes per event: 1 = best recommendation
    ROW_NUMBER() OVER (
        PARTITION BY r.event_id
        ORDER BY
            CASE WHEN w.is_rainy AND r.walk_distance_m > 400 THEN 1 ELSE 0 END,
            r.total_duration_min
    ) AS route_rank
FROM route_options r
JOIN calendar_events e ON r.event_id = e.event_id
LEFT JOIN weather_forecast w
    ON w.fetched_at = (SELECT MAX(fetched_at) FROM weather_forecast)
LEFT JOIN train_alerts ta
    ON ta.severity = 'HEAVY'
   AND ta.fetched_at > NOW() - INTERVAL '30 minutes';
```

---

## Target Stack ("Without Regret" Project)

| Component | Status | Priority |
|---|---|---|
| Core pipeline: schema, ingest, transform, serve | Not started | Must have |
| Retry/backoff on all API calls | Not started | Must have |
| Idempotency (INSERT OR REPLACE) | Not started | Must have |
| GPS coordinate validation | Not started | Must have |
| FastAPI serving layer | Not started | High |
| Parquet raw data zone | Not started | Medium |
| Airflow DAG (5 tasks, 10-min schedule) | Not started | High |
| Docker Compose (3 services) | Not started | High |

---

## Ports

| Service | Port | URL |
|---|---|---|
| Streamlit dashboard | 8501 | `http://localhost:8501` |
| FastAPI server | 8000 | `http://localhost:8000/docs` |
| Airflow UI | 8080 | `http://localhost:8080` |

---

## Mentoring Notes

- Student is new to GitHub — give explicit step-by-step `git` commands
- Student needs to present this in a 15-minute video — frame every technical explanation with "in your video, say this..." guidance
- Explain WHY before HOW for every new tool (Airflow, Docker, FastAPI)
- Do not add features beyond what is requested
- Check whether LTA API key has been received before writing any live API calls
