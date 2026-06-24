# AI Handoff — SNAIC-sg-commute-pulse

This document lets another Claude session (or any developer) continue this project
from scratch with no prior chat history.

**Last updated: 2026-06-24 (session 3)**

---

## What This Project Does

A **calendar-aware Singapore commute recommendation system** with ML predictions.

Given a user's next Google Calendar event, the pipeline:
1. Reads the event's location and geocodes it via OneMap Singapore
2. Fetches routing options (OneMap), real-time bus arrivals (LTA), train alerts (LTA), and weather (data.gov.sg)
3. Stores everything in DuckDB using a lake-warehouse pattern
4. Runs SQL transformation to rank routes, calculate leave-by time, apply weather/disruption penalties
5. Runs an ML model (Random Forest) to predict journey duration and stores prediction vs actual
6. Serves results via Streamlit dashboard + FastAPI REST endpoint

**Output:** "Leave by 09:23 AM — take Bus 65 then EWL. Predicted 38 min. ⚠ Rain — avoid 600m walk."

---

## Repo

| Field | Value |
|---|---|
| GitHub | `https://github.com/DLim86/SNAIC-sg-commute-pulse` |
| Owner | DLim86 |
| Local path | `e:\SNAIC\Week 2\Assessment` |
| Branch | `main` |
| Assessment deadline | 14 September 2026, 11:59 PM |
| Submit via | xSite → Assessments → Dropbox → Week 02 |
| Submit: | 1 video file + 1 PDF (3–5 resume bullets) |

---

## Rubric (actual — reviewed 2026-06-24)

| Criterion | Marks | Current Gap |
|---|---|---|
| End-to-End Pipeline | 30 | Need: serve.py, api.py, Airflow DAG, Docker |
| **ML and Real-Time Output** | **30** | **CRITICAL: need model.py — zero ML without it** |
| Technical Depth & Robustness | 10 | Strong: retry, idempotency, logging, coord validation |
| Presentation & Explanation | 30 | Needs video practice, reflection answers |

**ML criterion requires:** batch processing ✅ + model training/inference ❌ + live dashboard ✅ (pending) + model evaluation ❌

The rubric example: "Generate predictions for the next two hours and compare earlier predictions with actual data. Evaluate the prediction model every day at 8:00 AM." — This is exactly what model.py + the Airflow evaluate_model task must do.

---

## Build Status (as of 2026-06-24)

### Done and tested

| File | Status |
|---|---|
| `.gitignore` | Complete |
| `config_example.py` | Complete — template with HOME_ADDRESS, GARMIN_EMAIL/PASSWORD, WHOOP_ACCESS_TOKEN, all empty strings |
| `config.py` | Exists locally, gitignored — real credentials inside |
| `requirements.txt` | Uses `>=` for C-extension packages (Python 3.14 compat). Includes `garminconnect>=0.2.0`. **Needs `scikit-learn>=1.4.0` and `joblib>=1.3.0` added when building model.py** |
| `README.md` | Complete |
| `docs/roadmap.html` | Updated 2026-06-24 — 3 phases: Day1 pipeline, Day2 production, Day3 architecture+ML |
| `docs/AI_HANDOFF.md` | This file |
| `docs/video_script.html` | Updated 2026-06-24 — complete 15-min script for full project including ML, reflection |
| `docs/ARCHITECTURE.md` | Needs ML layer added (model.py, predictions table) |
| `docs/DECISIONS.md` | Has D01–D17. Needs D18 (batch vs Kafka), D19 (DuckDB vs Spark), D20 (RF model choice) |
| `scripts/__init__.py` | Done — empty, required for Airflow imports |
| `scripts/schema.py` | Done — 8 tables + `v_enriched_routes` view. **Needs `predictions` table added before model.py** |
| `scripts/ingest.py` | Done — Calendar + 4 APIs + retry/backoff + Parquet + legs + idempotent upsert + IP-geolocation origin + **progressive geocoding fallback** + **WORK_ADDRESS event-location fallback** + **`get_smart_default()` time-of-day heuristic** |
| `scripts/transform.py` | Done — **`AND start_time > NOW()` filter** (prevents stale past-event output), LEAVE LATEST + LEAVE NOW, step-by-step legs, rain/delay warnings, walk alternative (Zone 1/2), optional Garmin/Whoop |

### Still to build (in this order)

| File | Purpose | Why this order |
|---|---|---|
| `scripts/serve.py` | **NEXT** — Streamlit dashboard (read_only=True) | Shows ML predictions live — needed first |
| `scripts/model.py` | **CRITICAL** — train RF, predict, evaluate, 30-mark rubric criterion | Built after serve.py so there's somewhere to display predictions |
| `scripts/api.py` | FastAPI: `/health`, `/api/v1/recommendation/{event_id}`, `/api/v1/pipeline/status`, `/api/v1/prediction/{event_id}` | After model.py so prediction endpoint can be included |
| `dags/__init__.py` + `dags/commute_pipeline_dag.py` | Airflow DAG — 7 tasks, `schedule="*/10 * * * *"` | After all scripts exist |
| `docker-compose.yml` + `Dockerfile` | 3 services: pipeline, api, dashboard | Last — wraps everything |

---

## DuckDB Schema — 9 Tables

```
calendar_events   — event_id PK, title, start_time TIMESTAMPTZ, location_raw, dest_lat, dest_lng, ingested_at
route_options     — option_id PK, event_id FK, total_duration_min, walk_distance_m, num_transfers, fare, fetched_at
route_legs        — (option_id, leg_sequence) PK, mode, service_no, from_name, to_name, duration_min, distance_m
weather_forecast  — (area, valid_start) PK, forecast, is_rainy BOOLEAN, valid_end, fetched_at
bus_arrivals      — (bus_stop_code, service_no, fetched_at) PK, next_bus_mins, load
train_alerts      — alert_id PK, affected_line, message, severity, fetched_at
recommendations   — event_id PK, recommended_mode, total_duration_min, leave_by, estimated_arrival, weather_warning, disruption_warning, reason, created_at
pipeline_runs     — run_id PK, source, rows_upserted, duration_ms, status, error_msg, ran_at
predictions       — prediction_id PK, event_id, predicted_min, actual_min (nullable — backfilled), model_version, mae_7day (nullable), predicted_at
```

**`predictions` table must be added to `scripts/schema.py` before `scripts/model.py` is built.**
`actual_min` is filled in after the commute window passes by comparing with `route_options.total_duration_min` for the same `event_id`.

View: `v_enriched_routes` — JOINs route_options + calendar_events + weather_forecast + train_alerts. Returns `route_rank` via `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY rain_penalty, total_duration_min)`.

---

## model.py Design (to build)

**Purpose:** Satisfy the 30-mark "ML and Real-Time Output" rubric criterion.

**Model:** `RandomForestRegressor` (scikit-learn)
- **Features:** `hour_of_day` (0–23), `day_of_week` (0–6), `is_rainy` (0/1), `walk_distance_m`, `num_transfers`
- **Target:** `total_duration_min`
- **Saved to:** `models/commute_predictor.pkl` (models/*.pkl in .gitignore, models/.gitkeep committed)

**Three modes (CLI args):**
- `python scripts/model.py --train` — trains model, saves .pkl, logs to pipeline_runs
- `python scripts/model.py --predict` — loads .pkl, scores next calendar event, inserts into predictions table
- `python scripts/model.py --evaluate` — computes 7-day MAE of predicted_min vs actual_min, logs to pipeline_runs

**Cold start solution:** Bootstrap with ~500 synthetic historical rows using known patterns (rush hour 7–9am/5–7pm: +15%, rain: +8%, weekend: −20%) before training. Mark these with `model_version = "synthetic"` so they can be filtered later.

**Airflow integration:** The main DAG adds `predict_commute` task (runs each cycle after sql_transform). A separate daily DAG or scheduled task runs `evaluate_model` at 8 AM.

**Requirements to add:** `scikit-learn>=1.4.0`, `joblib>=1.3.0`

---

## Airflow DAG — 7 Tasks (to build)

```
fetch_calendar → geocode_destination → [fetch_weather, fetch_bus_arrivals, fetch_train_alerts] → sql_transform → predict_commute
```

Schedule: `*/10 * * * *` (every 10 minutes)
Additionally: `evaluate_model` task at `0 8 * * *` (8 AM daily) — can be a separate DAG or CronJob.

---

## serve.py Design (to build)

Key requirements:
- `duckdb.connect(str(DB_PATH), read_only=True)` — never write from dashboard
- `@st.cache_data(ttl=300)` — 5-minute refresh
- Show: event title, leave_by metric, predicted_min metric, fare metric
- Show: step-by-step legs table from `route_legs`
- Show: weather warning (st.warning) if `is_rainy = True`
- Show: disruption alert (st.error) if `alert_msg` is not None
- Show: ML prediction vs actual, 7-day MAE from predictions table
- `st.rerun()` or `time.sleep(300)` loop for auto-refresh

---

## api.py Design (to build)

Endpoints:
- `GET /health` — returns `{"status": "ok"}`
- `GET /api/v1/recommendation/{event_id}` — reads v_enriched_routes WHERE route_rank=1
- `GET /api/v1/pipeline/status` — reads pipeline_runs ORDER BY ran_at DESC LIMIT 10
- `GET /api/v1/prediction/{event_id}` — reads predictions table for that event

All connections: `duckdb.connect(str(DB_PATH), read_only=True)`
Run with: `uvicorn scripts.api:app --reload --port 8000`

---

## Data Flow Summary

```
Google Calendar API (OAuth2)
  → calendar_events (DuckDB) + data/raw/calendar/ (Parquet)  [DATA LAKE]
      ↓
OneMap Routing API
  → route_options + route_legs (DuckDB) + data/raw/onemap_route/ (Parquet)
      ↓
LTA Bus API      → bus_arrivals (DuckDB)   ──┐
LTA Alerts       → train_alerts (DuckDB)   ──┤  [DATA WAREHOUSE]
data.gov.sg      → weather_forecast        ──┘
                            ↓
                  v_enriched_routes (SQL view — JOIN + CASE WHEN + ROW_NUMBER)
                            ↓
                  recommendations (DuckDB)
                            ↓
                  predictions (DuckDB)  ← model.py
                            ↓
          FastAPI /api/v1/recommendation/{id}   :8000
          FastAPI /api/v1/prediction/{id}
          Streamlit dashboard                   :8501
```

---

## Calendar Data

Events are read from Google Calendar via OAuth2 (`fetch_next_calendar_event()` in ingest.py).
- Scans next 10 upcoming events, skips all-day events and events with no location
- Geocodes the first valid location via OneMap
- `event_id` format: `GCAL_{google_event_id}`
- First run opens a browser for Google consent; writes `token.json` to project root (gitignored)
- `GOOGLE_CALENDAR_ID = "primary"` in `config.py`

---

## APIs and Credentials

All credentials in `config.py` (gitignored). Template in `config_example.py`.

| API | Auth | Key location |
|---|---|---|
| Google Calendar | OAuth2 — `credentials.json` + `token.json` (both gitignored) | Project root |
| LTA DataMall | Header: `AccountKey: <LTA_API_KEY>` | `config.py → LTA_API_KEY` |
| OneMap | JWT from POST `/api/auth/post/getToken` — expires every 3 days | `config.py → ONEMAP_EMAIL, ONEMAP_PASSWORD` |
| data.gov.sg | None — open API | — |
| ip-api.com | None — free, no key | — |
| Garmin Connect | email + password (unofficial library) | `config.py → GARMIN_EMAIL, GARMIN_PASSWORD` (leave blank to skip) |
| Whoop | Bearer token | `config.py → WHOOP_ACCESS_TOKEN` (leave blank to skip) |

---

## Ports

| Service | Port | URL |
|---|---|---|
| Streamlit dashboard | 8501 | `http://localhost:8501` |
| FastAPI + Swagger | 8000 | `http://localhost:8000/docs` |
| Airflow UI | 8080 | `http://localhost:8080` |

---

## Known Issues / Gotchas

### Core pipeline
- **OneMap token TTL:** expires every 3 days — call `get_onemap_token()` on every pipeline run, never cache to disk
- **LTA BusArrivalv2 404:** returns 404 (not empty) for stops with no active services — catch `HTTPError(404)` and treat as "no data"
- **OneMap `leg.get("route")` returns string or dict:** always check `isinstance(route_field, dict)` before calling `.get("shortName")` — fixed in ingest.py
- **`v_enriched_routes` cross-join:** 47 weather areas × 3 routes = 141 rows. `route_rank=1` still gives one row per event — safe
- **`BEST_ROUTE_QUERY` has `AND start_time > NOW()`** — without this, `ORDER BY start_time LIMIT 1` picks the oldest stored event (yesterday's), not the next upcoming one
- **DuckDB write lock:** only one write connection at a time — pipeline must close before FastAPI opens
- **`datetime.utcnow()` deprecated:** use `datetime.now(timezone.utc).replace(tzinfo=None)` for naive UTC
- **OneMap routing `duration` in seconds:** divide by 60 for `total_duration_min`
- **`sys.path.insert(0, str(Path(__file__).parent.parent))`** before `from config import ...` in all scripts/
- **Geocoding progressive fallback:** `geocode()` tries full address → strips ", Singapore" → first comma-token. Postal codes are most reliable. Obscure street names may not exist in OneMap's index.
- **`WORK_ADDRESS` in config.py:** destination fallback when event location fails geocoding; also 8–10 AM default when no calendar event
- **`HOME_ADDRESS` in config.py:** used for after-4 PM go-home default and after-6 PM at-home proximity check. NOT the routing origin — origin is always IP geolocation.
- **`get_smart_default()` windows:** 8–10 AM → WORK; 4–6 PM → HOME (depart ~6:30 PM); after 6 PM → check IP location vs home (3 km threshold), skip if at home; outside windows → skip quietly
- **`SGT = timezone(timedelta(hours=8))`** — module-level constant in ingest.py for Singapore timezone arithmetic

### Google Calendar
- **First run opens browser** for OAuth2 consent — must be on machine with browser. Writes `token.json` to project root.
- **For Docker/Airflow:** pre-generate `token.json` locally and volume-mount it into the container
- **No-event case:** pipeline logs a warning and exits cleanly, records 'skipped' in pipeline_runs — does NOT crash

### ML (model.py — to build)
- **Cold start:** bootstrap with ~500 synthetic rows before real data accumulates (rush hour +15%, rain +8%, weekend −20%)
- **`evaluate_model` cold start:** skip gracefully if fewer than 7 actual_min values exist — log warning, do not crash
- **`actual_min` backfill:** compare predicted event's `event_id` against `route_options.total_duration_min` after event time passes
- **`models/*.pkl` gitignored** — commit `models/.gitkeep` so folder exists in repo
- **`scikit-learn>=1.4.0` and `joblib>=1.3.0`** must be added to `requirements.txt`

### IP Geolocation
- **ip-api.com** returns city-level accuracy (~1–5 km), free, no API key
- **VPN:** returns non-SG coords — falls back to Bishan (1.3521, 103.8198) with warning
- **HTTP only** on free tier — not HTTPS

### Garmin / Whoop (optional)
- **Garmin/Whoop are fully optional.** Three protection layers: (1) ImportError catch defaults to `""`, (2) early return None if credential is blank, (3) output only shown if result is not None
- **Garmin uses unofficial email/password auth** — may break if Garmin changes their API

---

## Coding Conventions

- **Retry wrapper:** every `requests.get()` through `fetch_with_retry(url, headers, params, max_retries=3)` — backoff 1s, 2s, 4s
- **Idempotency:** always `INSERT OR REPLACE INTO table SELECT ...` — never bare `INSERT`
- **Coordinate validation:** reject GPS outside Singapore bounds (`lat 1.15–1.47`, `lng 103.6–104.1`)
- **Timestamps:** `TIMESTAMPTZ` for event/schedule columns; `TIMESTAMP` for internal tracking (fetched_at, ran_at)
- **DuckDB connections:** `read_only=True` in Streamlit/FastAPI; write-capable only in ingestion/transform/model scripts
- **Comments:** one line max, only when WHY is non-obvious
- **No print statements:** use `logging.info()` / `logging.warning()`
- **Parquet naming:** `data/raw/{source}/date={YYYY-MM-DD}/{source}_{YYYY-MM-DD}.parquet`
- **Path creation:** `Path.mkdir(parents=True, exist_ok=True)` before writing

---

## Install & Run

```bash
# 1. Clone
git clone https://github.com/DLim86/SNAIC-sg-commute-pulse.git
cd SNAIC-sg-commute-pulse

# 2. Virtual environment
python -m venv venv
venv\Scripts\activate   # Windows

# 3. Install
pip install -r requirements.txt

# 4. Add credentials
copy config_example.py config.py
# Edit config.py with real keys — NEVER commit config.py

# 5. Schema (run once — or re-run after adding predictions table)
python scripts/schema.py

# 6. Ingest
python scripts/ingest.py

# 7. Transform
python scripts/transform.py

# 8. ML — train then predict
python scripts/model.py --train
python scripts/model.py --predict

# 9. Dashboard
streamlit run scripts/serve.py   # → http://localhost:8501

# 10. API
uvicorn scripts.api:app --reload --port 8000   # → http://localhost:8000/docs

# 11. Airflow
airflow standalone   # → http://localhost:8080

# 12. Full stack
docker compose up
```

---

## Mentoring Notes

- Student is new to GitHub — give explicit step-by-step `git add / commit / push` commands
- Explain WHY before HOW — student presents this in a 15-minute video
- After every concept, include "In your video, say this..." guidance
- Do not add features beyond what is asked
- Ask "do you have your LTA API key?" before any session involving LTA API calls
- The three rubric criteria visible in the video: pipeline (30) + ML (30) + technical depth (10)
- Reflection section in video is mandatory — 3 questions pre-drafted in video_script.html
