# Architecture — SNAIC-sg-commute-pulse

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
│                       data.gov.sg                           │
│                       (weather forecast)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │ Python requests + fetch_with_retry()
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    RAW ZONE (optional)                      │
│         data/raw/{source}/date=YYYY-MM-DD/*.parquet         │
│         Parquet files — gitignored, replay-able             │
└──────────────────────┬──────────────────────────────────────┘
                       │ pd.read_parquet → INSERT OR REPLACE
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    DUCKDB STORAGE                           │
│              db/commute.duckdb  (gitignored)                │
│                                                             │
│  calendar_events  route_options  route_legs (NEW)           │
│  weather_forecast bus_arrivals   train_alerts               │
│  recommendations  pipeline_runs                             │
└──────────────────────┬──────────────────────────────────────┘
                       │ SQL VIEW: v_enriched_routes
                       │ JOIN + CASE WHEN + ROW_NUMBER() OVER
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                            │
│                                                             │
│  FastAPI  ←──────────────────────────────────────────────  │
│  :8000/api/v1/recommendation/{event_id}                     │
│  :8000/health                                               │
│  :8000/docs  (Swagger UI)                                   │
│       │                                                     │
│       ▼                                                     │
│  Streamlit Dashboard  :8501                                 │
│  Leave-by time · Route options · Weather warning            │
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
└─────────────────────────────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    INFRASTRUCTURE (Docker)                  │
│                                                             │
│  Service: pipeline   → python -m scripts.ingest             │
│  Service: api        → uvicorn scripts.api:app :8000        │
│  Service: dashboard  → streamlit run scripts/serve.py :8501 │
│                                                             │
│  Volume: ./db:/app/db   (DuckDB persists)                   │
│  Volume: ./data:/app/data (Parquet raw zone)                │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```
Google Calendar API (OAuth2)
  → fetch_next_calendar_event(): scan next 10 upcoming events
  → skip all-day events (no dateTime) and events with no location
  → geocode first valid location via OneMap → dest_lat, dest_lng
  → event_id = f"GCAL_{google_event_id}"
  → INSERT OR REPLACE INTO calendar_events

For each event:
  → OneMap Routing API (start = home, end = dest_lat/dest_lng)
      → 3 itineraries: duration_min, walk_distance_m, transfers, fare
      → INSERT OR REPLACE INTO route_options

  → data.gov.sg 2-hour weather forecast
      → area forecasts: Bedok: "Moderate Rain" → is_rainy = TRUE
      → INSERT OR REPLACE INTO weather_forecast

  → LTA BusArrivalv2 (nearest bus stop code via Haversine)
      → next 3 arrivals: estimated_mins, load (SEA/SDA/LSD)
      → INSERT OR REPLACE INTO bus_arrivals

  → LTA TrainServiceAlerts
      → active disruptions: line, message, severity
      → INSERT OR REPLACE INTO train_alerts

SQL Transformation (v_enriched_routes view):
  → JOIN route_options + calendar_events + weather_forecast + train_alerts
  → Calculate leave_by = start_time - INTERVAL (duration + 10) MINUTE
  → CASE WHEN logic: rain penalty (walk > 400m), disruption warning, fastest label
  → ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY penalty, duration)
  → route_rank = 1 is the recommended option

Serving:
  → FastAPI reads v_enriched_routes WHERE route_rank = 1
  → Streamlit reads the same view, caches 5 minutes
```

---

## Folder Structure

```
e:\SNAIC\Week 2\Assessment\          ← working directory
├── CLAUDE.md                        ← Claude Code reads this automatically
├── README.md                        ← GitHub project page
├── .gitignore                       ← protects credentials + DB + raw data
├── config_example.py                ← credential template (committed)
├── config.py                        ← real credentials (GITIGNORED)
├── requirements.txt                 ← pinned Python dependencies
├── Dockerfile                       ← shared Docker image (to create)
├── docker-compose.yml               ← 3-service stack (to create)
│
├── scripts/
│   ├── __init__.py                  ← makes scripts importable by Airflow ✓ DONE
│   ├── schema.py                    ← 8 tables + v_enriched_routes view   ✓ DONE
│   ├── ingest.py                    ← 4 APIs, retry, Parquet, legs        ✓ DONE
│   ├── transform.py                 ← leave-latest/now, legs, warnings    ✓ DONE
│   ├── serve.py                     ← Streamlit dashboard                 ← NEXT
│   └── api.py                       ← FastAPI server                      ← TODO
│
├── dags/                            ← to create
│   ├── __init__.py
│   └── commute_pipeline_dag.py      ← Airflow DAG definition
│
├── data/
│   ├── raw/                         ← GITIGNORED — Parquet raw zone
│   │   ├── bus_arrivals/date=YYYY-MM-DD/*.parquet
│   │   ├── weather/date=YYYY-MM-DD/*.parquet
│   │   └── train_alerts/date=YYYY-MM-DD/*.parquet
│   └── processed/                   ← placeholder (currently unused)
│
├── db/
│   └── commute.duckdb               ← GITIGNORED — created by schema.py
│
├── LTA/                             ← GITIGNORED — registration documents
├── OneMap/                          ← GITIGNORED — registration + token
├── Prompt.txt                       ← GITIGNORED — context prompt
│
└── docs/
    ├── roadmap.html                 ← interactive 12-station build roadmap
    ├── AI_HANDOFF.md                ← this file's sibling — full project context
    ├── ARCHITECTURE.md              ← this file
    └── DECISIONS.md                 ← design decision log
```

---

## Dependencies

```
requests>=2.31.0     # HTTP API calls
pandas>=2.2.2        # DataFrame handling, Parquet write (>= for Python 3.14)
duckdb>=1.0.0        # embedded analytical DB (>= for Python 3.14)
streamlit>=1.35.0    # dashboard serving
icalendar>=5.0.11    # parse .ics calendar files
shapely>=2.0.4       # GPS point-in-polygon (>= for Python 3.14)
pytz>=2024.1         # timezone handling for Singapore +08:00
fastapi>=0.111.0     # REST API serving layer
uvicorn>=0.29.0      # ASGI server for FastAPI
pyarrow>=16.0.0      # Parquet file write (>= for Python 3.14)
apache-airflow       # pipeline orchestration (install separately)
```
NOTE: All C-extension packages use `>=` not `==` — Python 3.14 requires
pre-built wheels which only exist in newer versions.

---

## Key Technical Constraints

### Google Calendar OAuth2
- `credentials.json` — downloaded from Google Cloud Console (OAuth 2.0 Desktop app). Gitignored.
- `token.json` — written on first run after browser consent, auto-refreshed by `google-auth`. Gitignored.
- First run opens a browser window — must be on a machine with a browser
- `GOOGLE_CALENDAR_ID = "primary"` in `config.py` — reads your main Google Calendar
- Skips events with no `location` field and all-day events (no `dateTime`)

### OneMap JWT Token
- Expires every **3 days** — must call `get_onemap_token()` at the start of every pipeline run
- Do not cache the token to disk — always refresh programmatically
- Token is obtained via POST with email/password from `config.py`

### LTA Bus Stop Lookup
- The `BusArrivalv2` endpoint requires a `BusStopCode` (a 5-digit code)
- LTA does not return the stop's GPS coordinates in this endpoint
- Solution: download the bus stop list (`BusStops` endpoint), store it, compute Haversine distance from `dest_lat/dest_lng` to find the nearest stop code

### DuckDB Write Lock
- DuckDB allows **only one write connection at a time**
- `scripts/ingest.py` and `scripts/transform.py` must close their connections before `scripts/serve.py` or `scripts/api.py` can open read connections
- In FastAPI: always open with `duckdb.connect("db/commute.duckdb", read_only=True)`
- In Airflow: pipeline task closes its connection before the serve tasks run

### Singapore GPS Bounds
- Valid Singapore coordinates: `lat 1.15 – 1.47`, `lng 103.6 – 104.1`
- OneMap occasionally returns `(0, 0)` for unrecognised addresses
- Always validate before inserting: raise `ValueError` if out of bounds

### Weather Area Mapping
- `data.gov.sg` returns weather per named area: "Ang Mo Kio", "Bedok", "Marina South", etc.
- GPS coordinates ARE available in the `area_metadata` field of the API response (`label_location.latitude/longitude`)
- All 47 areas are upserted into `weather_forecast` table each run
- The view joins weather by `fetched_at` (latest batch) — creates a cross-join of 47 areas × N routes
- `route_rank = 1` still gives exactly one row per event (ROW_NUMBER partitioned by event_id)
- Nearest area to destination is identified in Python using Haversine and logged — informational only

---

## Port Reference

| Service | Port | Notes |
|---|---|---|
| Streamlit | 8501 | `http://localhost:8501` |
| FastAPI | 8000 | `http://localhost:8000/docs` for Swagger |
| Airflow | 8080 | `http://localhost:8080` — default `airflow standalone` port |
| DuckDB | — | Embedded — no network port |
