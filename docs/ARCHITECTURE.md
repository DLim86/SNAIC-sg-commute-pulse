# Architecture — SNAIC-sg-commute-pulse

**Last updated: 2026-06-24**

---

## System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                             │
│                                                             │
│  📅 Google Calendar   OneMap API      LTA DataMall          │
│  (OAuth2 — real       (routing,        (bus arrivals,        │
│   upcoming events)    geocoding)       train alerts)        │
│                                                             │
│                       data.gov.sg      ip-api.com           │
│                       (weather)        (origin detect)      │
└──────────────────────┬──────────────────────────────────────┘
                       │ Python requests + fetch_with_retry()
                       │ Retry: 1s → 2s → 4s (exponential backoff)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              RAW ZONE — DATA LAKE                           │
│         data/raw/{source}/date=YYYY-MM-DD/*.parquet         │
│         Parquet files — gitignored, date-partitioned        │
│         Replayable: fix SQL bug → re-transform without API  │
└──────────────────────┬──────────────────────────────────────┘
                       │ pd.read_parquet → INSERT OR REPLACE
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              DUCKDB STORAGE — DATA WAREHOUSE                │
│              db/commute.duckdb  (gitignored)                │
│                                                             │
│  FACT TABLE:                                                │
│    route_options   — duration_min, fare, walk_distance_m    │
│                                                             │
│  DIMENSION TABLES (star schema):                            │
│    calendar_events  route_legs     weather_forecast         │
│    bus_arrivals     train_alerts   pipeline_runs            │
│                                                             │
│  OUTPUT TABLES:                                             │
│    recommendations  predictions (ML — predicted vs actual)  │
└──────────┬───────────────────────────┬──────────────────────┘
           │ SQL VIEW                  │ model.py
           │ v_enriched_routes         │ scikit-learn RF
           │ JOIN + CASE WHEN +        │ train → predict → eval
           │ ROW_NUMBER() OVER         ▼
           │                  ┌────────────────────────┐
           │                  │   ML LAYER             │
           │                  │                        │
           │                  │  RandomForestRegressor │
           │                  │  models/commute_       │
           │                  │  predictor.pkl         │
           │                  │                        │
           │                  │  predictions table:    │
           │                  │  predicted_min         │
           │                  │  actual_min (backfill) │
           │                  │  mae_7day              │
           │                  └────────────────────────┘
           │                            │
           └──────────────┬─────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                            │
│                                                             │
│  Streamlit Dashboard  :8501                                 │
│  → leave-by · ML prediction · fare · weather warning        │
│  → step-by-step legs · predicted vs actual · 7-day MAE     │
│  → @st.cache_data(ttl=300) auto-refresh every 5 min        │
│                                                             │
│  FastAPI  :8000                                             │
│  GET /api/v1/recommendation/{event_id}                      │
│  GET /api/v1/prediction/{event_id}                          │
│  GET /api/v1/pipeline/status                                │
│  GET /health                                                │
│  GET /docs  (auto-generated Swagger UI)                     │
└─────────────────────────────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    ORCHESTRATION                            │
│                                                             │
│  Airflow DAG: commute_pipeline                              │
│  Schedule: */10 * * * * (every 10 minutes)                  │
│                                                             │
│  fetch_calendar → geocode_destination                       │
│                        ↓         ↓          ↓              │
│                   fetch_weather  fetch_bus  fetch_alerts    │
│                        ↓         ↓          ↓              │
│                        └────────┬─────────┘               │
│                                 ↓                          │
│                           sql_transform                    │
│                                 ↓                          │
│                         predict_commute  ← NEW (model.py)  │
│                                                             │
│  Separate daily schedule (0 8 * * *):                       │
│                         evaluate_model   ← NEW (model.py)  │
└─────────────────────────────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    INFRASTRUCTURE (Docker)                  │
│                                                             │
│  Service: pipeline   → python -m scripts.ingest             │
│  Service: api        → uvicorn scripts.api:app :8000        │
│  Service: dashboard  → streamlit run scripts/serve.py :8501 │
│                                                             │
│  Volume: ./db:/app/db      (DuckDB persists)                │
│  Volume: ./data:/app/data  (Parquet raw zone)               │
│  Volume: ./models:/app/models  (ML .pkl files)              │
│  Secrets: .env file (gitignored)                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```
Google Calendar API (OAuth2)
  → fetch_next_calendar_event(): scan next 10 upcoming events
  → skip all-day events (no dateTime) and events with no location
  → geocode() with progressive fallback: full address → strip ", Singapore" → first token
  → if all geocode attempts fail AND WORK_ADDRESS is set: use WORK_ADDRESS as destination
  → event_id = f"GCAL_{google_event_id}"
  → INSERT OR REPLACE INTO calendar_events
  → save Parquet → data/raw/calendar/date=.../

If no calendar event found → get_smart_default():
  → 8–10 AM: use WORK_ADDRESS (event_id = "DEFAULT_WORK_COMMUTE", start = today 9 AM SGT)
  → 4–6 PM:  use HOME_ADDRESS (event_id = "DEFAULT_HOME_COMMUTE", start = today 6:30 PM SGT)
  → after 6 PM: geocode HOME_ADDRESS, compare vs IP location — skip if within 3 km (at home)
  → other hours: skip pipeline, log INFO

For each event (real or smart default):
  → OneMap Routing API (start = home, end = dest_lat/dest_lng)
      → 3 itineraries: duration_min, walk_distance_m, transfers, fare
      → step-by-step legs: mode, service_no, from_name, to_name, duration_min
      → INSERT OR REPLACE INTO route_options, route_legs
      → save Parquet → data/raw/onemap_route/date=.../

  → data.gov.sg 2-hour weather forecast
      → area forecasts: Bedok: "Moderate Rain" → is_rainy = TRUE
      → INSERT OR REPLACE INTO weather_forecast
      → save Parquet → data/raw/weather/date=.../

  → LTA BusArrivalv2 (nearest bus stop code via Haversine against 5,205 stops)
      → next 3 arrivals: estimated_mins, load (SEA/SDA/LSD)
      → 404 = no active services for that stop (handle gracefully, not an error)
      → INSERT OR REPLACE INTO bus_arrivals

  → LTA TrainServiceAlerts
      → active disruptions: line, message, severity
      → INSERT OR REPLACE INTO train_alerts

SQL Transformation (v_enriched_routes view):
  → JOIN route_options + calendar_events + weather_forecast + train_alerts
  → Calculate leave_by = start_time - INTERVAL (duration + 10) MINUTE
  → Rain penalty: walk_distance_m > 400 AND is_rainy → route pushed down ranking
  → Disruption warning: severity='HEAVY' AND fetched within last 30 minutes
  → ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY penalty, duration)
  → route_rank = 1 is the recommended option

ML Pipeline (model.py):
  → --train: load historical route_options + calendar_events + weather
     → features: hour, dow, is_rainy, walk_distance_m, num_transfers
     → target: total_duration_min
     → fit RandomForestRegressor, save models/commute_predictor.pkl
  → --predict: load pkl, score next event, INSERT into predictions table
  → --evaluate: compute 7-day MAE (predicted_min vs actual_min), log to pipeline_runs

Serving:
  → Streamlit reads v_enriched_routes + predictions (read_only=True, TTL=300s)
  → FastAPI reads same views and tables (read_only=True)
  → Swagger UI at /docs for live API testing
```

---

## DuckDB Schema (9 tables)

```sql
-- Entry point: calendar events with geocoded GPS
CREATE TABLE IF NOT EXISTS calendar_events (
    event_id     VARCHAR PRIMARY KEY,    -- format: GCAL_{google_event_id}
    title        VARCHAR,
    start_time   TIMESTAMPTZ,            -- Singapore time +08:00
    location_raw VARCHAR,
    dest_lat     DOUBLE,
    dest_lng     DOUBLE,
    ingested_at  TIMESTAMP DEFAULT now()
);

-- FACT TABLE: one row per route option per event
CREATE TABLE IF NOT EXISTS route_options (
    option_id          VARCHAR PRIMARY KEY,  -- format: {event_id}_ROUTE_{n}
    event_id           VARCHAR REFERENCES calendar_events(event_id),
    total_duration_min INTEGER,
    walk_distance_m    INTEGER,
    num_transfers      INTEGER,
    fare               DECIMAL(4,2),
    fetched_at         TIMESTAMP
);

-- Step-by-step legs for each route option
CREATE TABLE IF NOT EXISTS route_legs (
    option_id    VARCHAR,
    leg_sequence INTEGER,
    mode         VARCHAR,      -- WALK, BUS, MRT, LRT
    service_no   VARCHAR,      -- bus number or MRT line code
    from_name    VARCHAR,
    to_name      VARCHAR,
    duration_min INTEGER,
    distance_m   INTEGER,
    PRIMARY KEY (option_id, leg_sequence)
);

-- DIMENSION: 2-hour weather forecast per area
CREATE TABLE IF NOT EXISTS weather_forecast (
    area        VARCHAR,
    forecast    VARCHAR,
    is_rainy    BOOLEAN,        -- TRUE if forecast contains "Rain" or "Shower"
    valid_start TIMESTAMPTZ,
    valid_end   TIMESTAMPTZ,
    fetched_at  TIMESTAMP,
    PRIMARY KEY (area, valid_start)
);

-- DIMENSION: real-time bus arrivals (near-real-time, short history)
CREATE TABLE IF NOT EXISTS bus_arrivals (
    bus_stop_code VARCHAR,
    service_no    VARCHAR,
    next_bus_mins INTEGER,
    load          VARCHAR,      -- SEA=seats, SDA=standing, LSD=limited standing
    fetched_at    TIMESTAMP,
    PRIMARY KEY (bus_stop_code, service_no, fetched_at)
);

-- DIMENSION: active MRT disruptions
CREATE TABLE IF NOT EXISTS train_alerts (
    alert_id      VARCHAR PRIMARY KEY,
    affected_line VARCHAR,
    message       VARCHAR,
    severity      VARCHAR,      -- HEAVY, MODERATE, etc.
    fetched_at    TIMESTAMP
);

-- OUTPUT: one recommendation per event (written by transform.py)
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

-- MONITORING: every pipeline execution logged here
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        VARCHAR PRIMARY KEY,
    source        VARCHAR,       -- 'ingest', 'transform', 'model_train', etc.
    rows_upserted INTEGER,
    duration_ms   INTEGER,
    status        VARCHAR,       -- 'success', 'error', 'skipped'
    error_msg     VARCHAR,
    ran_at        TIMESTAMP DEFAULT now()
);

-- ML OUTPUT: predictions vs actuals (written by model.py)
CREATE TABLE IF NOT EXISTS predictions (
    prediction_id VARCHAR PRIMARY KEY,
    event_id      VARCHAR,
    predicted_min INTEGER,
    actual_min    INTEGER,       -- NULL until commute window passes, then backfilled
    model_version VARCHAR,       -- e.g. '1.0', 'synthetic' for bootstrap rows
    mae_7day      DOUBLE,        -- NULL until evaluate_model runs
    predicted_at  TIMESTAMP DEFAULT now()
);
```

---

## Core SQL View

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
    -- Business rule: 10-minute buffer before event start
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

## Folder Structure

```
e:\SNAIC\Week 2\Assessment\
├── CLAUDE.md                        ← Claude Code reads this automatically
├── README.md
├── .gitignore
├── config_example.py                ← credential template (committed)
├── config.py                        ← real credentials (GITIGNORED)
├── requirements.txt                 ← add scikit-learn>=1.4.0, joblib>=1.3.0
├── Dockerfile                       ← to create
├── docker-compose.yml               ← to create
│
├── scripts/
│   ├── __init__.py                  ✓ DONE
│   ├── schema.py                    ✓ DONE (add predictions table)
│   ├── ingest.py                    ✓ DONE
│   ├── transform.py                 ✓ DONE
│   ├── serve.py                     ← NEXT
│   ├── model.py                     ← CRITICAL (30-mark criterion)
│   └── api.py                       ← TODO
│
├── dags/
│   ├── __init__.py                  ← to create
│   └── commute_pipeline_dag.py      ← to create (7 tasks)
│
├── models/
│   ├── .gitkeep                     ← commit this (folder placeholder)
│   └── commute_predictor.pkl        ← GITIGNORED (binary artifact)
│
├── data/
│   ├── raw/                         ← GITIGNORED — Parquet raw zone (data lake)
│   │   ├── bus_arrivals/date=YYYY-MM-DD/*.parquet
│   │   ├── weather/date=YYYY-MM-DD/*.parquet
│   │   ├── train_alerts/date=YYYY-MM-DD/*.parquet
│   │   └── onemap_route/date=YYYY-MM-DD/*.parquet
│   └── processed/                   ← placeholder (unused)
│
├── db/
│   └── commute.duckdb               ← GITIGNORED — created by schema.py
│
├── credentials.json                 ← GITIGNORED — Google OAuth2
├── token.json                       ← GITIGNORED — Google OAuth2 token
│
└── docs/
    ├── roadmap.html                 ← interactive build roadmap (3 phases)
    ├── AI_HANDOFF.md                ← full project context for AI sessions
    ├── ARCHITECTURE.md              ← this file
    ├── DECISIONS.md                 ← design decision log (D01–D20+)
    └── video_script.html            ← complete 15-min assessment video script
```

---

## Dependencies

```
requests>=2.31.0       # HTTP API calls
pandas>=2.2.2          # DataFrame, Parquet write (>= for Python 3.14)
duckdb>=1.0.0          # embedded analytical DB (>= for Python 3.14)
streamlit>=1.35.0      # live dashboard
fastapi>=0.111.0       # REST API serving layer
uvicorn>=0.29.0        # ASGI server for FastAPI
google-api-python-client>=2.0.0  # Google Calendar API
google-auth-httplib2>=0.2.0
google-auth-oauthlib>=1.0.0
shapely>=2.0.4         # GPS validation (>= for Python 3.14)
pytz>=2024.1           # timezone handling for Singapore +08:00
pyarrow>=16.0.0        # Parquet write (>= for Python 3.14)
garminconnect>=0.2.0   # optional fitness integration
scikit-learn>=1.4.0    # ML model training (RandomForestRegressor)
joblib>=1.3.0          # model serialization (.pkl save/load)
# apache-airflow — install separately, not pinned in requirements.txt
```

NOTE: All C-extension packages (pandas, duckdb, pyarrow, shapely, scikit-learn) use `>=` not `==`.
Python 3.14 requires pre-built wheels which only exist in newer versions.

---

## Key Technical Constraints

### Google Calendar OAuth2
- `credentials.json` — downloaded from Google Cloud Console (OAuth 2.0 Desktop app). Gitignored.
- `token.json` — written on first run after browser consent, auto-refreshed. Gitignored.
- First run opens a browser — must be on machine with a browser
- For Docker: pre-generate `token.json` locally and volume-mount into the container
- `GOOGLE_CALENDAR_ID = "primary"` in `config.py`

### OneMap JWT Token
- Expires every **3 days** — always call `get_onemap_token()` at pipeline start
- Never cache the token to disk
- Token obtained via POST with email/password from `config.py`

### LTA Bus Stop Lookup
- `BusArrivalv2` requires a `BusStopCode` (5-digit)
- LTA does not return GPS coordinates in this endpoint
- Solution: cache the bus stop list (`data/raw/bus_stops/bus_stops.parquet`, 5,205 stops), compute Haversine distance from dest_lat/dest_lng to find nearest stop code

### DuckDB Write Lock
- DuckDB allows **only one write connection at a time**
- `ingest.py` and `transform.py` must close their connections before `serve.py` or `api.py` open read connections
- FastAPI and Streamlit: always open with `duckdb.connect("db/commute.duckdb", read_only=True)`
- model.py: open write connection only for INSERT into predictions table, then close immediately

### Singapore GPS Bounds
- Valid: `lat 1.15 – 1.47`, `lng 103.6 – 104.1`
- OneMap occasionally returns `(0, 0)` for unrecognised addresses
- Always validate before inserting: raise `ValueError` if out of bounds

### ML Model
- `models/commute_predictor.pkl` is gitignored — binary artifact, not source code
- Commit `models/.gitkeep` so the folder exists in the repo for other environments
- Cold start: bootstrap with ~500 synthetic historical rows (rush hour +15%, rain +8%, weekend −20%)
- `evaluate_model` requires at least 7 rows with non-null `actual_min` — skip gracefully if not enough data

---

## Port Reference

| Service | Port | Notes |
|---|---|---|
| Streamlit | 8501 | `http://localhost:8501` |
| FastAPI | 8000 | `http://localhost:8000/docs` for Swagger |
| Airflow | 8080 | `http://localhost:8080` — `airflow standalone` |
| DuckDB | — | Embedded — no network port |
