# CLAUDE.md — Project Instructions for AI Coding Sessions

This file is read automatically by Claude Code at the start of every session.
It gives complete context so you can mentor and build without needing prior chat history.

---

## Your Role

You are both **project mentor** and **project manager** for this SIT Data Engineering assessment.

- Explain WHY before HOW — the student needs to understand decisions, not just copy code
- After every new concept, include a **"In your video, say this..."** block so the student can present it confidently in a 15-minute assessment video
- When giving git commands, be step-by-step — the student is new to GitHub
- Ask "do you have your LTA API key yet?" at the start of any session involving API calls
- Keep code comments minimal: only when the WHY is non-obvious (a hidden constraint, a workaround, a business rule)
- Do not add features beyond what is asked; do not refactor code the student hasn't written yet

---

## Student Profile

| Field | Value |
|---|---|
| Name | DLim86 (GitHub) |
| Institution | SIT (Singapore Institute of Technology) |
| Course | Data Engineering |
| Python level | Comfortable |
| DE tools | New — explain from first principles |
| GitHub | New — give explicit `git add / commit / push` commands |
| Assessment format | 15-minute screen-recorded video of the working system |
| Target submission | August 2026 (deadline: Sept 14 2026) |

---

## Security Rules — Absolute, Never Violate

1. `config.py` is gitignored — **never suggest committing it, never print its contents**
2. `*.duckdb` and `*.duckdb.wal` are gitignored — never commit database files
3. `LTA/`, `OneMap/`, `Prompt.txt` are gitignored — contain real credentials
4. `.env` is gitignored — used for Docker secrets
5. `data/raw/` is gitignored — regenerated each run
6. `credentials.json` and `token.json` are gitignored — Google Calendar OAuth2 files, never commit
7. All API keys in documentation must use placeholders: `<LTA_API_KEY>`, `<ONEMAP_TOKEN>`, `<ONEMAP_EMAIL>`, `<ONEMAP_PASSWORD>`, `<GOOGLE_CALENDAR_ID>`

If a file contains a real key or token, flag it before reading it aloud or including it in output.

---

## Project Overview

**SNAIC-sg-commute-pulse** — a calendar-aware Singapore commute recommendation system.

Given a next calendar event, the pipeline:
1. Reads the destination from the event
2. Geocodes it with OneMap
3. Fetches routing options, real-time bus arrivals, train alerts, and weather
4. Stores everything in DuckDB
5. Runs SQL transformation to produce a ranked recommendation
6. Serves it via Streamlit dashboard + FastAPI endpoint

**GitHub:** `https://github.com/DLim86/SNAIC-sg-commute-pulse`
**Working directory:** `e:\SNAIC\Week 2\Assessment`

---

## Current State (as of 24 June 2026)

### Done — tested and working
| File | Status |
|---|---|
| `.gitignore` | Complete |
| `config_example.py` | Complete — template includes HOME_ADDRESS, WORK_ADDRESS, GARMIN_EMAIL/PASSWORD, WHOOP_ACCESS_TOKEN |
| `config.py` | Exists locally, gitignored — real credentials inside |
| `requirements.txt` | Uses `>=` for C-extension packages; includes `garminconnect>=0.2.0` |
| `README.md` | Complete |
| `docs/roadmap.html` | Complete — interactive 12-station roadmap |
| `docs/AI_HANDOFF.md` | Complete — full handoff context (keep updated) |
| `docs/video_script.html` | Complete — timed video script |
| `scripts/__init__.py` | Empty — required for Airflow DAG imports |
| `scripts/schema.py` | **DONE** — 8 tables + `v_enriched_routes` view. Run once. |
| `scripts/ingest.py` | **DONE** — Calendar + 4 APIs + retry/backoff + Parquet + legs + idempotent upsert + IP-geolocation origin + progressive geocoding fallback (strips ", Singapore", tries first token) + WORK_ADDRESS event-location fallback + `get_smart_default()` time-of-day heuristic (8–10AM→WORK, 4–6PM→HOME, after 6PM→at-home check) |
| `scripts/transform.py` | **DONE** — next-event-only (`AND start_time > NOW()` filter prevents stale past events), LEAVE LATEST + LEAVE NOW always shown, step-by-step legs, "Why chosen" label, walk alternative with Zone 1/2 stats, optional Garmin steps + Whoop recovery |
| `db/commute.duckdb` | Exists locally, gitignored — populated by real Google Calendar events |
| `data/raw/bus_stops/bus_stops.parquet` | Cached — 5,205 LTA bus stops |
| `data/raw/weather/` | Populated — 47 weather areas |
| `data/raw/onemap_route/` | Populated — 3 route options |

### Still to build (in order)
| File | Purpose | Rubric criterion |
|---|---|---|
| `scripts/serve.py` | Streamlit dashboard — **NEXT** | Pipeline (30) + ML output (30) |
| `scripts/model.py` | ML pipeline: train → predict → evaluate — **CRITICAL, 30 marks at risk without this** | ML and Real-Time Output (30) |
| `scripts/api.py` | FastAPI: `/health`, `/api/v1/recommendation/{event_id}`, `/api/v1/pipeline/status` | Pipeline (30) |
| `dags/__init__.py` + `dags/commute_pipeline_dag.py` | Airflow DAG, 7 tasks (5 existing + `predict_commute` + `evaluate_model`) | Technical Depth (10) |
| `docker-compose.yml` + `Dockerfile` | 3 services: pipeline, api, dashboard | Technical Depth (10) |

---

## Calendar Data

Events are read from Google Calendar via OAuth2 (`fetch_next_calendar_event()` in `ingest.py`).
- Scans next 10 upcoming events, skips all-day events and events with no location
- Geocodes the first valid location via OneMap
- `event_id` format: `GCAL_{google_event_id}`
- First run opens a browser for Google consent; writes `token.json` to project root (gitignored)
- `GOOGLE_CALENDAR_ID = "primary"` in `config.py` — change to a specific calendar ID if needed

---

## Next Tasks — Build in This Order

> **RUBRIC ALERT:** "ML and Real-Time Output" is worth 30 marks. The project currently scores 0 on this criterion. `scripts/model.py` is the fix and must be built after `serve.py`.

1. **`scripts/serve.py`** — Streamlit dashboard (read_only=True DuckDB connection). Shows leave-by, prediction, and MAE.
2. **`scripts/model.py`** — ML pipeline: train `RandomForestRegressor`, save to `models/commute_predictor.pkl`, score next event, store in `predictions` table, evaluate 7-day MAE. Bootstrap with synthetic historical data so training works from day 1.
3. **`scripts/api.py`** — FastAPI: `/health`, `/api/v1/recommendation/{event_id}`, `/api/v1/pipeline/status`, `/api/v1/prediction/{event_id}`
4. **`dags/commute_pipeline_dag.py`** — Airflow DAG with 7 tasks: original 5 + `predict_commute` (each run) + `evaluate_model` (daily 8 AM)
5. **`docker-compose.yml` + `Dockerfile`** — 3 services: pipeline, api, dashboard

---

## APIs and Credentials

Real credentials are stored locally in gitignored files — **never commit them**.

Use `config.py` (gitignored) for all real secrets. Template is in `config_example.py`.

| API | Auth method | Key location |
|---|---|---|
| Google Calendar | OAuth2 — `credentials.json` + `token.json` (both gitignored) | Project root |
| LTA DataMall | Header: `AccountKey: <LTA_API_KEY>` | `config.py` → `LTA_API_KEY` |
| OneMap | JWT from POST `/api/auth/post/getToken` | `config.py` → `ONEMAP_EMAIL`, `ONEMAP_PASSWORD` |
| data.gov.sg | None — open API | — |

OneMap token expires every 3 days — always call `get_onemap_token()` fresh per pipeline run.
Google Calendar OAuth2 token auto-refreshes via `google-auth` — no manual refresh needed.

---

## DuckDB Schema — `db/commute.duckdb`

Eight tables (schema.py creates all of these — run once, or re-run after adding `predictions`):

```
calendar_events   — event_id PK, title, start_time TIMESTAMPTZ, dest_lat, dest_lng
route_options     — option_id PK, event_id FK, total_duration_min, walk_distance_m, fare
weather_forecast  — (area, valid_start) PK, forecast, is_rainy BOOLEAN, fetched_at
bus_arrivals      — (bus_stop_code, service_no, fetched_at) PK, next_bus_mins, load
train_alerts      — alert_id PK, affected_line, message, severity, fetched_at
recommendations   — event_id PK, leave_by TIMESTAMPTZ, reason, created_at
pipeline_runs     — run_id PK, source, rows_upserted, status, error_msg, ran_at
predictions       — prediction_id PK, event_id, predicted_min, actual_min, model_version, mae_7day, predicted_at
```

The `predictions` table is required for the ML rubric criterion (30 marks). Add it to `scripts/schema.py` before building `scripts/model.py`.

Full `CREATE TABLE` statements are in `docs/ARCHITECTURE.md`.

---

## Coding Conventions

- **Retry wrapper:** every `requests.get()` must use `fetch_with_retry(url, headers, params, max_retries=3)` — exponential backoff: 1s, 2s, 4s
- **Idempotency:** always use `INSERT OR REPLACE INTO table SELECT ...` — never bare `INSERT`
- **Coordinate validation:** reject any GPS outside Singapore bounds (`lat 1.15–1.47`, `lng 103.6–104.1`) before inserting
- **Timestamps:** use `TIMESTAMPTZ` for all event/schedule columns; `TIMESTAMP` (no TZ) for internal tracking columns (fetched_at, ran_at)
- **DuckDB connections:** open with `read_only=True` in Streamlit/FastAPI; open without flag only in ingestion/transform scripts
- **Comments:** one line max, only when WHY is non-obvious — no docstrings, no section headers
- **No print statements in production code** — use `logging.info()` / `logging.warning()`
- **Parquet naming:** `data/raw/{source_name}/date={YYYY-MM-DD}/{source_name}_{YYYY-MM-DD}.parquet`
- **Folder creation:** always use `Path.mkdir(parents=True, exist_ok=True)` before writing files

---

## Important Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Create DuckDB schema (run once)
python scripts/schema.py

# Run ingestion
python scripts/ingest.py

# Run transformation
python scripts/transform.py

# Train ML model (first time, or after enough new data accumulates)
python scripts/model.py --train

# Run prediction for next event
python scripts/model.py --predict

# Evaluate model (compare predictions vs actuals)
python scripts/model.py --evaluate

# Run Streamlit dashboard
streamlit run scripts/serve.py

# Run FastAPI server
uvicorn scripts.api:app --reload --port 8000

# Run Airflow locally
airflow standalone
# → UI at http://localhost:8080

# Run full stack with Docker
docker compose up
docker compose up --build   # after code changes
docker compose down

# Daily git workflow
git add scripts/schema.py
git commit -m "add DuckDB schema with 7 tables"
git push
```

---

## Known Issues / Gotchas

- **OneMap token TTL:** expires every 3 days — call `get_onemap_token()` on every pipeline run, never cache it to disk
- **LTA bus stops vs GPS:** LTA bus stop codes don't have GPS coordinates in the BusArrivalv2 endpoint — use Haversine distance against a bus stop list to find the nearest stop. Bus stop list is cached at `data/raw/bus_stops/bus_stops.parquet`
- **LTA BusArrivalv2 returns 404 (not empty array) for stops with no active services** — handle gracefully, treat as "no data" not an error
- **data.gov.sg weather areas:** `area_metadata` in the API response includes `label_location` with lat/lng for each area — no separate lookup needed
- **DuckDB write lock:** only one connection can write at a time; the pipeline must close its connection before FastAPI opens one
- **`scripts/` imports `config.py` from project root** — all scripts must add `sys.path.insert(0, str(Path(__file__).parent.parent))` before `from config import ...`
- **Python 3.14 compatibility:** C-extension packages (pandas, duckdb, pyarrow, shapely) must use `>=` version pins — no pre-built wheels for old pinned versions on 3.14
- **`datetime.utcnow()` deprecated in Python 3.12+** — use `datetime.now(timezone.utc).replace(tzinfo=None)` for naive UTC into TIMESTAMP columns
- **OneMap routing `duration` is in seconds** — divide by 60 for `total_duration_min`
- **`get_onemap_token()` uses `requests.post`, not `fetch_with_retry`** — has its own retry loop with 30s timeout
- **OneMap `leg.get("route", {})` can return a string** — always check `isinstance(route_field, dict)` before calling `.get("shortName")` on it (fixed in ingest.py)
- **`v_enriched_routes` cross-join:** view joins all 47 weather areas per route (141 rows for 3 routes). `route_rank=1` still gives exactly one row per event — safe to use in transform.py
- **transform.py BEST_ROUTE_QUERY requires `AND start_time > NOW()`** — without this filter, the query picks the oldest stored event (even yesterday's), not the next upcoming one. The fix is in BEST_ROUTE_QUERY in transform.py.
- **`route_legs` table** added in latest schema.py — run `python scripts/schema.py` then `python scripts/ingest.py` to populate legs
- **Geocoding progressive fallback:** `geocode()` in ingest.py tries three search terms in order: (1) full address, (2) address with ", Singapore" stripped, (3) first comma-delimited token. Use postal codes for most reliable results. Obscure street names like "Sentul Walk" may not be in OneMap's index.
- **WORK_ADDRESS in config.py:** set this to your school/work postal code. Used as destination fallback when event geocoding fails AND as smart default destination 8–10 AM (no calendar event).
- **HOME_ADDRESS in config.py:** set this for the go-home smart default (after 4 PM) and for the after-6 PM at-home detection. NOT used as routing origin — origin is always IP geolocation.
- **`get_smart_default()` time windows:** 8–10 AM → WORK_ADDRESS; 4–6 PM → HOME_ADDRESS (depart 6:30 PM); after 6 PM → geocode HOME_ADDRESS, compare IP location, if within 3 km return None (skip, already home). Outside these windows → skip pipeline quietly.
- **Google Calendar no-event case:** pipeline now calls `get_smart_default()` before skipping. Only truly skips if WORK_ADDRESS/HOME_ADDRESS are not set OR it's outside the routing windows OR user is already home.
- **Google Calendar first run:** browser opens for OAuth2 consent — must be on a machine with a browser. Writes `token.json` to project root. For Docker/Airflow: pre-generate `token.json` locally and volume-mount it.
- **IP geolocation (`ip-api.com`):** returns city-level accuracy (~1–5 km), free, no API key. Returns non-SG coords if user is on a VPN — falls back to Bishan (1.3521, 103.8198) with a warning. HTTP not HTTPS on free tier.
- **Walk suggestion:** only shown when `is_rainy = False` AND Haversine distance from origin to dest < 5 km. Uses `_detect_origin()` in transform.py (IP geolocation — does NOT call OneMap, no token needed there).
- **Garmin steps:** requires `pip install garminconnect` — already in requirements.txt. Uses unofficial email/password auth. Leave `GARMIN_EMAIL = ""` in config to skip silently.
- **Whoop recovery:** requires `WHOOP_ACCESS_TOKEN` in config.py — generate from developer.whoop.com. Returns recovery score 0–100. Leave blank to skip.
- **model.py — cold start problem:** pipeline may only have a few days of real data. Bootstrap `predictions` with 500 synthetic historical rows using known patterns (rush hour +15%, rain +8%, weekend −20%) before fitting the model. Synthetic rows can be marked with `model_version = "synthetic"` so they can be filtered out later.
- **model.py — `scikit-learn` and `joblib` must be added to requirements.txt** — `scikit-learn>=1.4.0` and `joblib>=1.3.0`
- **model.py — `models/` folder must be gitignored** — `.pkl` files are binary artifacts, not source code. Add `models/*.pkl` to `.gitignore` (but commit `models/.gitkeep` so the folder exists in the repo).
- **evaluate_model task** — only makes sense once `predictions` has at least 7 rows with non-null `actual_min`. Handle the cold start gracefully: if fewer than 7 actuals exist, log a warning and skip evaluation rather than crashing.
- **`actual_min` backfill** — after the commute time passes, a separate pipeline task should compare `predicted_min` to the route that was actually taken (`total_duration_min` from `route_options` for the same `event_id`) and fill in `actual_min`.

---

## Course Context

- Day 1 content: pipeline fundamentals, DuckDB, SQL transformation, Streamlit — **DONE (design phase)**
- Day 2 content: retry/backoff, Parquet, FastAPI, Airflow, Docker — **IN PROGRESS**
- Day 3 content: Kafka, Flink, dbt, Spark, data modelling (star schema, lake vs warehouse) — **RUBRIC REVIEWED — MLOps added to build plan (30-mark criterion)**
- Day 4–5: not yet released — update roadmap after each class at `docs/roadmap.html`
