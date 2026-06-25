# SNAIC-sg-commute-pulse

> A calendar-aware Singapore commute recommendation system — reads your next Google Calendar event, checks real-time bus arrivals, MRT disruptions, and weather, then tells you **when to leave and how to get there**, with an ML-predicted journey time and a live dashboard.

Built as a Data Engineering assessment project at SIT (Singapore Institute of Technology).

---

## Pipeline Architecture

```
Google Calendar (OAuth2)
       │
       ▼
OneMap Geocoding ──────────────────────────────────────────┐
       │                                                   │
       ▼                                                   │
┌──────────────────────────────────────────────────────┐  │
│                  DATA INGESTION (ingest.py)           │  │
│  ┌────────────┐ ┌───────────────┐ ┌───────────────┐  │  │
│  │OneMap Route│ │LTA v3/BusArr. │ │ data.gov.sg   │  │  │
│  │  +legs     │ │5-stop fallback│ │ 2-hr weather  │  │  │
│  └────────────┘ └───────────────┘ └───────────────┘  │  │
│  ┌────────────────────────────────────────────────┐  │  │
│  │ LTA TrainServiceAlerts  │  ip-api.com origin   │  │  │
│  └────────────────────────────────────────────────┘  │  │
│  Retry/backoff · Parquet raw zone · Idempotent upsert │  │
└──────────────────────────────────────────────────────┘  │
       │                                                   │
       ▼                                                   │
┌──────────────────────────────────────────────────────┐  │
│         DuckDB — 9 tables + v_enriched_routes view    │◄─┘
│  calendar_events │ route_options │ route_legs         │
│  weather_forecast │ bus_arrivals │ train_alerts       │
│  recommendations │ pipeline_runs │ predictions        │
└──────────────────────────────────────────────────────┘
       │
       ▼
SQL Transformation (transform.py)
  · Leave-by calc · Rain penalty · Route ranking · Live bus board · Alt routes
       │
       ├──▶ ML Prediction (model.py)
       │      RandomForestRegressor → predictions table → 7-day MAE evaluation
       │
       ├──▶ FastAPI (api.py) :8000
       │      /health · /recommendation/{id} · /pipeline/status · /prediction/{id}
       │
       └──▶ Streamlit Dashboard (serve.py) :8501
              Leave-by · ML prediction · Fare · Weather warnings · Bus board
```

---

## Data Sources

| Source | What it provides | Auth |
|--------|-----------------|------|
| [Google Calendar API](https://developers.google.com/calendar) | Next upcoming event with location | OAuth2 — credentials.json + token.json |
| [OneMap Singapore](https://www.onemap.gov.sg) | Geocoding, routing (walk/bus/MRT), fares | Email + password → JWT (expires every 3 days) |
| [LTA DataMall](https://datamall.lta.gov.sg) | Real-time bus arrivals (v3/BusArrival), MRT alerts | API key (free, 1–2 day approval) |
| [data.gov.sg](https://api.data.gov.sg) | 2-hour weather forecast by area | None |
| [ip-api.com](http://ip-api.com) | Current location (routing origin) | None |
| Garmin Connect | Today's step count | Email + password (optional) |
| Whoop API | Recovery score | Access token (optional) |

---

## Tech Stack

- **Python** — requests, pandas, duckdb, streamlit, scikit-learn, joblib, google-api-python-client
- **DuckDB** — embedded analytical database (9 tables, star schema, no server needed)
- **Apache Airflow** — 7-task DAG, 10-minute schedule, parallel fetch phase
- **FastAPI + uvicorn** — REST API with auto-generated Swagger UI
- **Streamlit** — live dashboard, auto-refreshes every 5 minutes
- **Docker Compose** — 3 services: pipeline, api, dashboard
- **scikit-learn** — RandomForestRegressor for commute time prediction

---

## Project Structure

```
SNAIC-sg-commute-pulse/
├── data/
│   └── raw/                    # Parquet raw zone (gitignored — regenerated each run)
│       ├── bus_stops/          # LTA bus stop cache (5,205 stops)
│       ├── bus_arrivals/date=YYYY-MM-DD/
│       ├── weather/date=YYYY-MM-DD/
│       └── onemap_route/date=YYYY-MM-DD/
├── db/
│   └── commute.duckdb          # DuckDB database (gitignored)
├── docs/
│   ├── roadmap.html            # Interactive project roadmap (12 stations)
│   ├── video_script.html       # Timed 15-min assessment video script
│   ├── AI_HANDOFF.md           # Full context for new AI sessions
│   ├── DECISIONS.md            # D01–D26 design decision log
│   └── ARCHITECTURE.md         # Full schema DDL and data flow
├── models/
│   └── .gitkeep                # models/*.pkl gitignored — binary artifacts
├── scripts/
│   ├── __init__.py             # Required for Airflow DAG imports
│   ├── schema.py               # ✅ DONE — 9 tables + v_enriched_routes view
│   ├── ingest.py               # ✅ DONE — Calendar + 4 APIs + retry + Parquet + legs
│   ├── transform.py            # ✅ DONE — SQL ranking, leave-by, live bus board, alt routes
│   ├── serve.py                # ⬅ NEXT — Streamlit dashboard
│   ├── model.py                # 🔴 CRITICAL (30 marks) — RF train/predict/evaluate
│   └── api.py                  # FastAPI REST layer
├── dags/
│   ├── __init__.py             # Required for Airflow
│   └── commute_pipeline_dag.py # 7-task Airflow DAG
├── config_example.py           # ✅ Committed — template with placeholders only
├── config.py                   # ❌ GITIGNORED — real credentials here
├── requirements.txt            # ✅ Complete
├── docker-compose.yml          # 3 services: pipeline, api, dashboard
├── Dockerfile                  # Shared — each service overrides command
└── .gitignore                  # config.py, *.duckdb, data/raw/, models/*.pkl
```

---

## DuckDB Schema — 9 Tables

```sql
calendar_events   — event_id PK, title, start_time TIMESTAMPTZ, location_raw, dest_lat, dest_lng
route_options     — option_id PK, event_id FK, total_duration_min, walk_distance_m, num_transfers, fare
route_legs        — (option_id, leg_sequence) PK, mode, service_no, from_name, to_name, duration_min, num_stops
weather_forecast  — (area, valid_start) PK, forecast, is_rainy BOOLEAN, fetched_at
bus_arrivals      — (bus_stop_code, service_no, fetched_at) PK, next_bus_mins, next_bus2_mins, load
train_alerts      — alert_id PK, affected_line, message, severity, fetched_at
recommendations   — event_id PK, leave_by TIMESTAMPTZ, reason, created_at
pipeline_runs     — run_id PK, source, rows_upserted, duration_ms, status, error_msg, ran_at
predictions       — prediction_id PK, event_id, predicted_min, actual_min, model_version, mae_7day, predicted_at
```

Star schema: `route_options` is the fact table. `weather_forecast`, `bus_arrivals`, `train_alerts` are dimension tables.
`v_enriched_routes` view joins all four with a `ROW_NUMBER()` window function for route ranking.

---

## Setup

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

# 4. Add your credentials (never commit config.py)
copy config_example.py config.py
# Edit config.py — fill in LTA_API_KEY, ONEMAP_EMAIL, ONEMAP_PASSWORD
# Add credentials.json from Google Cloud Console for Calendar OAuth2

# 5. Create DuckDB schema (run once)
python scripts/schema.py

# 6. Run full pipeline
python scripts/ingest.py       # fetch all APIs → DuckDB + Parquet
python scripts/transform.py    # SQL transformation → recommendations

# 7. ML pipeline
python scripts/model.py --train    # train RandomForest on historical data
python scripts/model.py --predict  # score next event → predictions table
python scripts/model.py --evaluate # compute 7-day MAE

# 8. Dashboard
streamlit run scripts/serve.py             # http://localhost:8501

# 9. FastAPI
uvicorn scripts.api:app --reload --port 8000   # http://localhost:8000/docs

# 10. Airflow (orchestration)
airflow standalone                         # http://localhost:8080

# 11. Full Docker stack
docker compose up              # starts pipeline + api + dashboard
docker compose up --build      # after code changes
```

---

## API Registration

Before running the pipeline, register for:

1. **LTA DataMall** — [datamall.lta.gov.sg](https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html) (approval takes 1–2 days)
2. **OneMap** — [onemap.gov.sg](https://www.onemap.gov.sg) (instant, requires SingPass)
3. **Google Calendar** — Enable Calendar API in Google Cloud Console → create OAuth 2.0 Desktop credential → download `credentials.json`

`data.gov.sg` and `ip-api.com` need no registration.

---

## Security Rules

- `config.py`, `credentials.json`, `token.json`, `*.duckdb`, `data/raw/`, `models/*.pkl`, `.env` are all in `.gitignore`
- Only `config_example.py` is committed — it contains placeholder strings only
- Run `git status` to confirm sensitive files are absent before any push

---

## Known Gotchas

| Issue | Fix |
|---|---|
| LTA `BusArrivalv2` retired Aug 2024 | Use `v3/BusArrival` endpoint |
| LTA 404 on valid stops (no active service) | Try 5 nearest stops by Haversine, skip 404s |
| BusStopCode becomes float64 in Parquet | `str(code).split(".")[0]` before API call |
| OneMap `leg.route` returns string or dict | Check `isinstance(route_field, dict)` before `.get("shortName")` |
| OneMap token expires every 3 days | Call `get_onemap_token()` fresh on every run |
| `v_enriched_routes` cross-joins 47 weather areas | Query `route_options` directly for alt routes |
| Stale calendar events after reschedule | `_purge_stale_events()` clears old future events after each ingest |
| DuckDB FK on `route_legs → route_options` | Delete `route_legs` before upserting `route_options` |
| HOME_ADDRESS must not be routing origin | Origin is always IP geolocation (ip-api.com) |
| `datetime.utcnow()` deprecated (Python 3.12+) | `datetime.now(timezone.utc).replace(tzinfo=None)` |

---

## Airflow DAG — 7 Tasks

```
fetch_calendar → geocode_destination → [fetch_weather, fetch_bus_arrivals, fetch_train_alerts]
    → sql_transform → predict_commute
```

Schedule: `*/10 * * * *` (every 10 minutes)
Daily at 8 AM: `evaluate_model` — computes 7-day MAE from `predictions` table.

---

## Rubric

| Criterion | Marks | Status |
|---|---|---|
| End-to-End Pipeline | 30 | ingest + transform done; need serve.py, api.py, Airflow DAG, Docker |
| **ML and Real-Time Output** | **30** | **CRITICAL — need model.py, predictions wired into dashboard** |
| Technical Depth & Robustness | 10 | retry, idempotency, coord validation, pipeline_runs log all done |
| Presentation & Explanation | 30 | video script at docs/video_script.html |

---

*SIT Data Engineering Assessment · Deadline: 14 September 2026 · github.com/DLim86/SNAIC-sg-commute-pulse*
