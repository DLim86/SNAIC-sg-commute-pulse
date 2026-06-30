# Architecture — SNAIC-sg-commute-pulse

**Last updated: 2026-06-30 (7-state adaptive scheduler refactor)**

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
│  → FRESH read-only DuckDB connection every 60s rerun        │
│    (session 9 fix: removed @st.cache_resource — was         │
│     returning stale rows after pipeline wrote new data)     │
│                                                             │
│  FastAPI  :8000                                             │
│  GET /health                                                │
│  GET /api/v1/recommendation/next       ← defined FIRST      │
│  GET /api/v1/recommendation/{event_id}                      │
│  GET /api/v1/prediction/{event_id}                          │
│  GET /api/v1/pipeline/status                                │
│  GET /api/v1/alerts                                         │
│  GET /docs  (auto-generated Swagger UI)                     │
│  Per-request contextmanager get_db() — opens+closes         │
│  read_only DuckDB on each request, no held file handle      │
└─────────────────────────────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    ORCHESTRATION                            │
│                                                             │
│  Airflow DAG: commute_pipeline  (dags/commute_pipeline_dag.py)│
│  Schedule: */30 * * * *  catchup=False                      │
│                                                             │
│  SEQUENTIAL 7-task chain:                                   │
│  schema_check >> ingest >> transform >> predict_commute     │
│    >> backfill_actuals >> gate_evaluate >> evaluate_model   │
│                                                             │
│  gate_evaluate = ShortCircuitOperator                       │
│    → passes ONLY when datetime.now(SGT).hour == 8           │
│    → evaluate_model only runs once per day at 8 AM SGT      │
│                                                             │
│  All tasks use BashOperator with absolute PROJECT_DIR path  │
└─────────────────────────────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    INFRASTRUCTURE (Docker)                  │
│                                                             │
│  Service: pipeline   → python scripts/scheduler.py          │
│    7-state adaptive machine:                                │
│      NO_EVENT → EVENT_DETECTED_BURST → WATCHING             │
│      → LEAVE_WINDOW → IN_TRANSIT                           │
│      → ARRIVAL_VERIFY → POST_ARRIVAL_COOLDOWN              │
│    Calendar-check every tick (cheap, cached geocode)        │
│    Routes only on event/location change (OneMap + LTA)      │
│    Weather every 30 min, independent of state               │
│                                                             │
│  Service: api        → uvicorn scripts.api:app :8000        │
│  Service: dashboard  → streamlit run scripts/serve.py :8501 │
│                                                             │
│  Named volume: db_data → /app/db  (DuckDB persists)        │
│  Bind mounts: ./token.json  ./credentials.json (OAuth2)     │
│  Image: python:3.12-slim + libgomp1 (scikit-learn threads)  │
│                                                             │
│  WAL lock gotcha: if pipeline killed mid-write, run         │
│  "docker compose down -v" to wipe stale .duckdb.wal         │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```
Google Calendar API (OAuth2)
  → fetch_next_calendar_event(): scan next 10 upcoming events
  → skip all-day events (no dateTime) and events with no location
  → geocode() with progressive fallback (session 7):
      0. re.findall(r'\b\d{6}\b', address) — postal code extracted first (most reliable)
      1. full address string
      2. address with ", Singapore" stripped
      3. first comma-delimited token
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

  → LTA v3/BusArrival (5 nearest bus stop codes via Haversine against 5,205 stops, tried in order)
      → stops tried in order until one returns live data; 404 on a stop = no active services, try next
      → BusStopCode stripped of float suffix: str(code).split(".")[0]
      → next 3 arrivals: estimated_mins, load (SEA/SDA/LSD)
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
  → --train:
     Duration model: RandomForestRegressor (11 features) → models/commute_predictor.pkl
       features: base_duration, next_bus_mins, walk_distance_m, num_transfers,
                 is_rainy, rain_exposure, rush_hour, is_weekend, hour_of_day, day_of_week, bus_crowd_score
       target: total_duration_min
     Crowd model: RandomForestClassifier (10 features) → models/crowd_predictor.pkl
       features: leave_hour, day_of_week, is_rainy, rush_hour,
                 next_bus_mins, next_bus2_mins, bus_headway_gap,
                 walk_distance_m, num_transfers, base_duration
       target: actual_crowd (SEA/SDA/LSD labels from LTA load snapshots — NOT the prediction itself)
       note: bus_headway_gap = next_bus2_mins - next_bus_mins (large gap → crowd builds at stop)
  → --predict: score ALL 3 route options individually — each gets its own crowd prediction
     prediction_id = "{option_id}_pred"; weather scalar subquery avoids 47-area cross-join
  → --evaluate: compute 7-day MAE (predicted_min vs actual_min), store in predictions.mae_7day, log to pipeline_runs with mae summary

Serving:
  → Streamlit: fresh read_only=True connection every 60s rerun (no cache decorator)
  → FastAPI: per-request contextmanager get_db() opens+closes read_only connection
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
    num_stops    INTEGER,      -- stops between boarding and alighting (WALK = NULL); session 5
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
    bus_stop_code  VARCHAR,
    service_no     VARCHAR,
    next_bus_mins  INTEGER,
    next_bus2_mins INTEGER,     -- ETA of second upcoming bus (session 5)
    load           VARCHAR,     -- SEA=seats, SDA=standing, LSD=limited standing
    fetched_at     TIMESTAMP,
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
    -- Dynamic reason — 9 labels via window functions (session 6)
    CASE
        WHEN w.is_rainy AND r.walk_distance_m > 400
            THEN '⚠ Rain — ' || CAST(r.walk_distance_m AS VARCHAR) || 'm exposed walk'
        WHEN ta.alert_id IS NOT NULL
            THEN '⚠ Service disruption — check alternatives'
        WHEN r.num_transfers = 0
             AND r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest + direct (no transfers)'
        WHEN r.num_transfers = 0
            THEN '✓ Direct — no transfers'
        WHEN r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
             AND r.num_transfers = MIN(r.num_transfers) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest + fewest transfers'
        WHEN r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest (' || CAST(r.total_duration_min AS VARCHAR) || ' min)'
        WHEN r.num_transfers = MIN(r.num_transfers) OVER (PARTITION BY r.event_id)
            THEN '✓ Fewest transfers (' || CAST(r.num_transfers AS VARCHAR) || ')'
        WHEN r.walk_distance_m = MIN(r.walk_distance_m) OVER (PARTITION BY r.event_id)
            THEN '✓ Least walking (' || CAST(r.walk_distance_m AS VARCHAR) || 'm)'
        WHEN r.fare > 0
             AND r.fare = MIN(CASE WHEN r.fare > 0 THEN r.fare END) OVER (PARTITION BY r.event_id)
            THEN '✓ Cheapest fare'
        ELSE '✓ Best overall'
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
├── Dockerfile                       ✓ DONE (session 9) — python:3.12-slim + libgomp1
├── docker-compose.yml               ✓ DONE (session 9) — 3 services + scheduler.py
│
├── scripts/
│   ├── __init__.py                  ✓ DONE
│   ├── schema.py                    ✓ DONE — 9 tables + v_enriched_routes view
│   ├── ingest.py                    ✓ DONE — 3 argparse modes (session 9)
│   ├── transform.py                 ✓ DONE — clock-time arrivals, walk-only, alt routes
│   ├── serve.py                     ✓ DONE — fresh connection per rerun (session 9 fix)
│   ├── model.py                     ✓ DONE — --train/--predict/--evaluate/--backfill
│   ├── api.py                       ✓ DONE — 6 FastAPI endpoints (session 9)
│   └── scheduler.py                 ✓ DONE — 7-state adaptive machine
│
├── dags/
│   ├── __init__.py                  ✓ DONE (session 9)
│   └── commute_pipeline_dag.py      ✓ DONE — 7-task sequential chain (session 9)
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
scikit-learn>=1.4.0    # ML model training (RandomForestRegressor + RandomForestClassifier)
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
- `v3/BusArrival` requires a `BusStopCode` (5-digit) — old `BusArrivalv2` endpoint retired August 2024 (LTA DataMall API v6.0)
- LTA does not return GPS coordinates in this endpoint
- Solution: cache the bus stop list (`data/raw/bus_stops/bus_stops.parquet`, 5,205 stops), compute Haversine distance from **origin_lat/origin_lng** to find 5 nearest candidates
- Try candidates in order; stop on the first one that returns live data; skip on 404 (no active services)
- Always strip float suffix: `str(stop_code).split(".")[0]` — Parquet promotes int to float64 when NaN rows exist

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
