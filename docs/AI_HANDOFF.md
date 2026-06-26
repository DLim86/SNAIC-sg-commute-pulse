# AI Handoff — SNAIC-sg-commute-pulse

This document lets another Claude session (or any developer) continue this project
from scratch with no prior chat history.

**Last updated: 2026-06-26 (session 9 — FastAPI, Airflow DAG, Docker, smart scheduler, serve.py stale-connection fix)**

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
| End-to-End Pipeline | 30 | Need: api.py, Airflow DAG, Docker |
| **ML and Real-Time Output** | **30** | **model.py DONE ✅ — per-route predictions, mode-aware actual backfill, dashboard panel** |
| Technical Depth & Robustness | 10 | Strong: retry, idempotency, logging, coord validation |
| Presentation & Explanation | 30 | Needs video practice, reflection answers |

**ML criterion requires:** batch processing ✅ + model training/inference ✅ + live dashboard ✅ + model evaluation ✅

The rubric example: "Generate predictions for the next two hours and compare earlier predictions with actual data. Evaluate the prediction model every day at 8:00 AM." — This is exactly what model.py + the Airflow evaluate_model task must do.

---

## Build Status (as of 2026-06-26 session 9) — ALL COMPLETE ✅

### All files done and tested

| File | Status |
|---|---|
| `.gitignore` | Complete |
| `config_example.py` | Complete — template with HOME_ADDRESS, WORK_ADDRESS, GARMIN_EMAIL/PASSWORD, WHOOP_ACCESS_TOKEN, all empty strings |
| `config.py` | Exists locally, gitignored — real credentials present |
| `requirements.txt` | Complete — `>=` pins for Python 3.14 compat; `garminconnect>=0.2.0`, `scikit-learn>=1.4.0`, `joblib>=1.3.0` |
| `README.md` | Complete |
| `docs/roadmap.html` | Updated session 9 — stations 09/10/11 marked DONE, scheduler described |
| `docs/AI_HANDOFF.md` | This file — updated session 9 |
| `docs/video_script.html` | Updated session 9 — Docker section mentions scheduler.py |
| `docs/ARCHITECTURE.md` | Updated — includes ML layer, smart default data flow, geocoding fallback |
| `docs/DECISIONS.md` | Complete — D01–D37 |
| `scripts/__init__.py` | Done — empty, required for Airflow imports |
| `scripts/schema.py` | Done — 9 tables + `v_enriched_routes` view. Run `python scripts/schema.py` to apply all migrations. |
| `scripts/ingest.py` | Done — full pipeline + **session 9: 3 argparse modes** (`--mode calendar-check` / `--mode routes` / `--mode weather`). Default (no mode) = full pipeline. `_fetch_raw_calendar_event()` helper for cheap Google Calendar-only check (no OneMap). |
| `scripts/transform.py` | Done — all session 6/7/8 features: dynamic mode, clock-time arrivals, walk-only inline, alt routes skipped for walk-only |
| `scripts/model.py` | Done (session 8) — `--train`, `--predict` (all 3 routes, `{option_id}_pred`), `--evaluate` (7-day MAE), `--backfill` (mode-aware) |
| `scripts/serve.py` | Done — **session 9 critical fix: removed `@st.cache_resource` from `get_connection()`** — was caching stale DuckDB connection; now opens fresh read-only connection every 60s rerun. |
| `scripts/api.py` | **Done (session 9)** — FastAPI, 6 endpoints. Per-request `contextmanager get_db()` opens+closes read-only DuckDB connection. `/recommendation/next` defined before `/{event_id}` (FastAPI routing order). Swagger at `http://localhost:8000/docs`. |
| `scripts/scheduler.py` | **Done (session 9)** — 4-state machine: NO_EVENT (60s day / 1hr night) → WATCHING (sleep to leave_by - 30 min) → IMMINENT (1s poll, cached geocode) → EXPIRED. Group 1: `calendar-check` every tick. Group 2: `routes` only on key change. Group 3: `weather` every 30 min. Replaces shell loop in docker-compose.yml. |
| `dags/__init__.py` | Done (session 9) — empty, required for Airflow imports |
| `dags/commute_pipeline_dag.py` | Done (session 9) — 7-task chain: `schema_check >> ingest >> transform >> predict_commute >> backfill_actuals >> gate_evaluate >> evaluate_model`. `gate_evaluate` = `ShortCircuitOperator` (passes only at 8 AM SGT). Schedule `*/30 * * * *`. |
| `Dockerfile` | Done (session 9) — `python:3.12-slim` + `libgomp1` + pip install + `mkdir -p db data/raw models` |
| `.dockerignore` | Done (session 9) — excludes `.git/`, `__pycache__/`, `*.duckdb`, `models/*.pkl`, `LTA/`, `OneMap/`, `Prompt.txt`, `*.log`, `docs/` |
| `docker-compose.yml` | Done (session 9) — 3 services sharing `db_data` named volume. Pipeline: `python scripts/scheduler.py`. API on :8000. Dashboard on :8501. Volume mounts: `./token.json`, `./credentials.json`. |
| `db/commute.duckdb` | Exists locally, gitignored — populated by real Google Calendar events |
| `data/raw/bus_stops/bus_stops.parquet` | Cached — 5,205 LTA bus stops |

---

## DuckDB Schema — 9 Tables

```
calendar_events   — event_id PK, title, start_time TIMESTAMPTZ, location_raw, dest_lat, dest_lng, ingested_at
route_options     — option_id PK, event_id FK, total_duration_min, walk_distance_m, num_transfers, fare, fetched_at
route_legs        — (option_id, leg_sequence) PK, mode, service_no, from_name, to_name, duration_min, distance_m, num_stops [session 5]
weather_forecast  — (area, valid_start) PK, forecast, is_rainy BOOLEAN, valid_end, fetched_at
bus_arrivals      — (bus_stop_code, service_no, fetched_at) PK, next_bus_mins, next_bus2_mins [session 5], load
train_alerts      — alert_id PK, affected_line, message, severity, fetched_at
recommendations   — event_id PK, recommended_mode, total_duration_min, leave_by, estimated_arrival, weather_warning, disruption_warning, reason, created_at
pipeline_runs     — run_id PK, source, rows_upserted, duration_ms, status, error_msg, ran_at
predictions       — prediction_id PK, event_id, option_id [s8], predicted_min, predicted_crowd [s8], actual_min (nullable), actual_crowd [s8], model_version, mae_7day (nullable), boarding_stop_code [s8], alighting_stop_code [s8], transit_service_no [s8], predicted_at
```

**`predictions` table:** base columns added session 4; 6 new columns added session 8 via `MIGRATIONS` in schema.py. `prediction_id` format changed from `{event_id}_pred` to `{option_id}_pred` (session 8) — one row per route option, not per event. Old rows (pre-session-8) have `option_id IS NULL` and are silently ignored by serve.py.
`actual_min` is mode-aware backfill (session 8): BUS → LTA v3/BusArrival at alighting stop; MRT → `total_duration_min + 4` headway (or +20 disruption); LRT → `total_duration_min + 7`; WALK → `total_duration_min`.

View: `v_enriched_routes` — JOINs route_options + calendar_events + weather_forecast + train_alerts. Returns `route_rank` via `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY rain_penalty, total_duration_min)`.

---

## Day 4 MLOps Additions (unlocked 2026-06-25)

**MLflow Experiment Tracking** — wrap `train_model()` in `mlflow.start_run()`:
- `mlflow.log_param("n_estimators", 100)` + `feature_count` + `training_rows`
- `mlflow.log_metric("mae_validation", mae)` + `rmse`
- `mlflow.sklearn.log_model(model, "commute_predictor")` — .pkl stored as artifact
- `mlflow ui` → `http://localhost:5000` shows all runs side-by-side

**Model Registry** — version lifecycle:
- Staging → register newly trained model, shadow-test for 7 pipeline cycles
- Production → promoted only when challenger MAE beats current by >5%
- Archived → retired models kept for rollback

**Drift Detection** — via MAE threshold in `evaluate_model()`:
- If `mae_7day > DRIFT_THRESHOLD` (default 10 min): `logging.warning("Drift detected")`
- Optionally insert a `pipeline_runs` entry to trigger Airflow retraining task
- Features most likely to drift: `hour_of_day` (schedule changes), `is_rainy` (seasonal), `walk_distance_m` (user moves)

**Canary Deploy** — staged promotion pattern:
1. New model → registered as "staging"
2. Run staging + production in parallel for 7 days
3. Auto-promote if staging MAE < production by >5%; otherwise archive challenger

**New dependency:** `mlflow>=2.12.0` — add to `requirements.txt`.

**Design decision:** See D29 (MLflow adoption) and D30 (drift detection) in `docs/DECISIONS.md`.

---

## model.py — BUILT (session 8)

**Purpose:** Satisfy the 30-mark "ML and Real-Time Output" rubric criterion.

**Models:**
- `RandomForestRegressor` → `models/commute_predictor.pkl` — predicts `total_duration_min`
- `GradientBoostingClassifier` → `models/crowd_classifier.pkl` — predicts crowd level (SEA/SDA/LSD)
- **Features:** `hour_of_day`, `day_of_week`, `is_rainy`, `walk_distance_m`, `num_transfers`, `next_bus_mins`

**Four CLI modes:**
- `python scripts/model.py --train` — trains both models on synthetic + real data, saves .pkl
- `python scripts/model.py --predict` — scores ALL 3 route options for next event, `prediction_id = f"{option_id}_pred"`
- `python scripts/model.py --evaluate` — 7-day MAE, skips gracefully if <7 actuals exist
- `python scripts/model.py --backfill` — fills `actual_min` per transit mode (see gotchas)

**Key helper:** `_match_stop_name(to_name, stops_df)` — matches `route_legs.to_name` against `bus_stops.Description` (exact lowercase, then 15-char prefix). Loads `bus_stops.parquet` with `dropna(subset=["BusStopCode","Description"])` to prevent NaN float promotion.

---

## Airflow DAG — BUILT (session 9)

**Actual implementation** (sequential, not parallel):
```
schema_check >> ingest >> transform >> predict_commute >> backfill_actuals >> gate_evaluate >> evaluate_model
```

- `gate_evaluate` = `ShortCircuitOperator` — only passes when `datetime.now(SGT).hour == 8`
- Schedule: `*/30 * * * *` (every 30 minutes, not 10 — changed from early design)
- `ingest` calls `python scripts/ingest.py` (full pipeline — weather + routes + bus + train internally)
- Run: `airflow standalone` → UI at `http://localhost:8080`

---

## api.py — BUILT (session 9)

6 endpoints — all read-only, `contextmanager get_db()` opens/closes per request:
- `GET /health` → `{"status": "ok", "db": "connected"}`
- `GET /api/v1/recommendation/next` → next upcoming event's full recommendation + legs + live arrival
- `GET /api/v1/recommendation/{event_id}` → same for specific event_id
- `GET /api/v1/prediction/{event_id}` → all 3 ML predictions for that event
- `GET /api/v1/pipeline/status` → last 10 pipeline_runs
- `GET /api/v1/alerts` → active HEAVY train alerts from last 30 min

Run: `uvicorn scripts.api:app --reload --port 8000` → Swagger at `http://localhost:8000/docs`

---

## scheduler.py — BUILT (session 9)

Replaces the fixed `sleep 1800` shell loop in docker-compose.yml. Pure Python state machine.

**3 groups:**
- Group 1 (`--mode calendar-check`): every tick. Cached geocode — no OneMap during IMMINENT.
- Group 2 (`--mode routes`): fires only when `new_key != prev_key` (event or dest changed >10m). Calls OneMap + LTA + transform + predict + backfill.
- Group 3 (`--mode weather`): every 30 min regardless of state. Calls weather + transform.

**4 states:**
| State | Condition | Sleep |
|---|---|---|
| NO_EVENT | no event key returned | 60s (6am–midnight) · 1hr (midnight–6am) |
| WATCHING | leave_by > 30 min away | sleep until `leave_by - 30 min` (max 1hr) |
| IMMINENT | ≤ 30 min to leave_by | 1s — fastest response, geocode fully cached |
| EXPIRED | start_time < now | 5s then reset `prev_key=""` |

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
- **LTA Bus Arrival URL changed:** correct endpoint is `https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival` — the old `BusArrivalv2` path was retired in LTA DataMall API v6.0 (August 2024). Calling `BusArrivalv2` returns 404 "The requested API was not found" for ALL stop codes regardless of whether buses exist. Fixed in `ingest.py` session 4.
- **5-nearest-stop bus fallback:** `ingest_bus_arrivals` tries the 5 nearest bus stops (Haversine from origin) in order, using a single request per stop with no retry on 404. Many 65xxx Punggol/Sengkang stops are in the BusStops database but not in the real-time v3/BusArrival system.
- **BusStopCode float suffix:** Parquet promotes integer BusStopCode to float64 if any NaN rows exist → "65721.0" is rejected by LTA. Fix: `str(code).split(".")[0]` strips the suffix.
- **v3/BusArrival still returns 404 for stops with genuinely no active services** — distinct from the endpoint-not-found 404. Log a warning and continue to the next candidate.
- **OneMap `leg.get("route")` returns string or dict:** always check `isinstance(route_field, dict)` before calling `.get("shortName")` — fixed in ingest.py
- **`v_enriched_routes` cross-join:** 47 weather areas × 3 routes = 141 rows. `route_rank=1` still gives one row per event — safe
- **`BEST_ROUTE_QUERY` has `AND start_time > NOW()`** — without this, `ORDER BY start_time LIMIT 1` picks the oldest stored event (yesterday's), not the next upcoming one
- **DuckDB write lock:** only one write connection at a time — pipeline must close before FastAPI opens
- **`datetime.utcnow()` deprecated:** use `datetime.now(timezone.utc).replace(tzinfo=None)` for naive UTC
- **OneMap routing `duration` in seconds:** divide by 60 for `total_duration_min`
- **`sys.path.insert(0, str(Path(__file__).parent.parent))`** before `from config import ...` in all scripts/
- **Geocoding progressive fallback:** `geocode()` tries full address → strips ", Singapore" → first comma-token. Postal codes are most reliable. Obscure street names may not exist in OneMap's index.
- **`WORK_ADDRESS` in config.py:** destination fallback when event location fails geocoding; also 8–10 AM default when no calendar event
- **`HOME_ADDRESS` in config.py:** used for after-4 PM go-home default and after-6 PM at-home proximity check. NOT the routing origin — origin is always IP geolocation. A previous session incorrectly geocoded HOME_ADDRESS as origin; this caused OneMap 404 when the calendar event destination was also home (origin = destination). Reverted in session 5.
- **Stale calendar event cleanup:** `_purge_stale_events(con, keep_event_id)` in ingest.py deletes all future `calendar_events` + their `route_options` + `route_legs` that don't match the active event_id. Called after every successful fetch. Without this, rescheduling a calendar event leaves the old entry (with old start_time) in the DB; since both old and new start_time are `> NOW()`, transform would pick whichever it encountered first.
- **Destination bus stop not stored:** origin-side live arrivals are fetched from the nearest stop to the *user's location*. The nearest bus stop to the *destination* is not looked up. OneMap `route_legs.to_name` has the stop name where you alight, but not the LTA stop code. Future: store `dest_bus_stop_code` in `calendar_events` (schema change needed) for display in serve.py.
- **DuckDB FK constraint on `INSERT OR REPLACE` into `route_options`** — `route_legs` FKs `route_options(option_id)`. DuckDB's replace deletes the old row first, but that delete fails when route_legs still holds a reference. Fixed in session 5: `ingest_routes()` deletes all route_legs for the event before upserting route_options.
- **`bus_arrivals` now stores `next_bus2_mins`** — session 5 added this column. `schema.py` applies `ALTER TABLE bus_arrivals ADD COLUMN IF NOT EXISTS next_bus2_mins INTEGER` as a migration on existing DBs. Always run `python scripts/schema.py` after pulling to pick up migrations.
- **`get_smart_default()` windows:** 8–10 AM → WORK; 4–6 PM → HOME (depart ~6:30 PM); after 6 PM → check IP location vs home (3 km threshold), skip if at home; outside windows → skip quietly
- **`SGT = timezone(timedelta(hours=8))`** — module-level constant in ingest.py for Singapore timezone arithmetic

### Google Calendar
- **First run opens browser** for OAuth2 consent — must be on machine with browser. Writes `token.json` to project root.
- **For Docker/Airflow:** pre-generate `token.json` locally and volume-mount it into the container
- **No-event case:** pipeline logs a warning and exits cleanly, records 'skipped' in pipeline_runs — does NOT crash

### Session 7 — ingest.py + transform.py additions
- **Postal code extraction (session 7):** `geocode()` now runs `re.findall(r'\b\d{6}\b', address)` before any other candidate. Word boundary `\b` prevents matching 7-digit strings. Postal codes geocode with near-perfect accuracy on OneMap — tried first, before full address.
- **Location-change detection log (session 7):** before the ingest loop in `main()`, stored `dest_lat`/`dest_lng` is queried and compared to newly geocoded values. If Haversine shift > 50m, logs `📍 Destination updated (NNN m shift)`. Routes are always re-fetched regardless.
- **Clock-time arrivals (session 7):** `transform.py` shows `HH:MM SGT` clock times instead of relative minutes. Computed as `(now_sgt + timedelta(minutes=x)).strftime("%H:%M")` where `now_sgt = datetime.now(timezone.utc).astimezone(SGT)`. MRT headway: x1=4 min. LRT: x1=7 min.
- **Walk-only inline display (session 7):** `is_walk_only = bool(legs) and all(l["mode"] == "WALK" for l in legs)`. When true, `_walk_metrics()` folds into the recommended route section. The 5km and `is_rainy` guards do NOT apply to walk-only routes. `print_walk_suggestion()` is skipped.

### Session 8 — model.py built; serve.py + transform.py updated
- **Weather cross-join (session 8):** `predict()` uses scalar subquery `(SELECT COALESCE(is_rainy, FALSE) FROM weather_forecast ORDER BY fetched_at DESC LIMIT 1)` instead of `LEFT JOIN weather_forecast w ON w.fetched_at = (SELECT MAX...)`. The JOIN produced 47× rows (one per weather area). Scalar subquery returns one boolean.
- **`prediction_id` format change (session 8):** changed from `{event_id}_pred` to `{option_id}_pred`. Old rows (pre-session-8) have `option_id IS NULL` and are silently ignored by serve.py's `PREDICTION_QUERY WHERE prediction_id = ?`. Re-run `--predict` to generate new-format rows.
- **`_match_stop_name()` (session 8):** matches `route_legs.to_name` against `bus_stops.Description` — exact lowercase, then first-15-char prefix (if len ≥ 5). BUS legs only. MRT/LRT set `transit_service_no` but leave `alighting_stop_code = None`.
- **Mode-aware backfill (session 8):** `--backfill` determines `dominant_mode` from `route_legs` for each `option_id`. BUS: LTA v3/BusArrival at alighting stop within 3h, else proxy. MRT: `total_duration_min + 4` (or +20 if HEAVY disruption in `train_alerts` window). LRT: `total_duration_min + 7` (or +20). WALK: `total_duration_min` deterministic. Old predictions without `option_id` use legacy `route_options` proxy.
- **`serve.py` ML panel keyed by `prediction_id` (session 8):** `PREDICTION_QUERY` uses `WHERE prediction_id = ?`. Recommended route passes `f"{option_id}_pred"`. Each alt expander passes `f"{alt_id}_pred"`. `CROWD_ICON`/`CROWD_LABEL` moved to module level so both sections share them.
- **Alt routes skipped for walk-only (session 8, transform.py):** `alt_rows = [] if is_walk_only else con.execute(ALT_ROUTES_QUERY, ...).fetchall()` — one-line guard before the alt routes block.
- **MRT/LRT have no public real-time arrival API:** Singapore SMRT/SBS Transit do not publish a real-time train arrival API. "Next at HH:MM" for MRT/LRT in transform.py is `now_sgt + headway_estimate` — an educated approximation. Same estimate is used in `--backfill` for MRT/LRT actual_min.
- **`models/*.pkl` gitignored** — `models/.gitkeep` committed so folder exists in repo.

### Session 9 — api.py, Airflow DAG, Docker, scheduler, serve.py fix
- **`serve.py` `@st.cache_resource` removed:** the decorator cached the DuckDB connection at startup. Every 60s rerun saw the same stale connection — new rows written by the pipeline never appeared. Fix: removed decorator, now opens fresh `read_only=True` connection every rerun. Restart Streamlit to clear old cached connection.
- **`api.py` FastAPI routing order:** `/api/v1/recommendation/next` must be defined BEFORE `/{event_id}`. FastAPI matches routes top-to-bottom; "next" would be captured as an event_id parameter if order is wrong.
- **`api.py` per-request DuckDB connection:** `contextmanager get_db()` opens `read_only=True`, yields, closes in `finally`. Multiple simultaneous read-only connections to DuckDB are safe.
- **`scheduler.py` no ip-api during IMMINENT:** ip-api.com free tier limit is 45 req/min. During IMMINENT 1s polling, Group 1 uses cached geocode — no ip-api call. ip-api is only called in Group 2 (`run_routes()`) which fires at most once per cycle when event/dest changes.
- **`ingest.py` `_fetch_raw_calendar_event()`:** lightweight Google Calendar only — no geocoding, no ip-api. Returns `(event_id, location_raw, start_dt_str)`. Used in `run_calendar_check()` to skip OneMap when event_id and location string are unchanged.
- **Airflow DAG `gate_evaluate`:** `ShortCircuitOperator` returns `True` only when `datetime.now(SGT).hour == 8`. Skips `evaluate_model` on all other runs without failing the DAG.
- **DuckDB WAL lock in Docker named volume:** killed pipeline container leaves stale `.duckdb.wal` in `db_data`. Fix: `docker compose down -v` then `docker compose up --build`. Data lost — repopulate by triggering pipeline once.
- **`docker compose restart` does NOT rebuild:** reuses existing image. Code changes require `docker compose up --build`.

### Session 6 — transform.py improvements

- **OneMap `numItineraries` hard-capped at 3** — requesting `numItineraries: 4` returns HTTP 400. OneMap's public transit routing API caps at 3 itineraries. The system always returns exactly 3 routes (1 recommended + 2 alternatives). The alt routes section will always show exactly 2 alternatives, never 3.
- **MRT/LRT have no public real-time arrival API** — Singapore MRT/LRT does not expose a public real-time arrival feed. The system uses fixed headway estimates: ~3-5 min for MRT, ~5-10 min for LRT. Live bus data is only available via LTA v3/BusArrival at the origin stop.
- **Dynamic disruption filtering** — `train_alerts.affected_line` is filtered against `{leg.service_no for leg in legs if leg.mode in ("MRT","LRT")}`. Alerts not matching the route's actual rail service codes are ignored. Bus-only routes show no disruption section. Pure MRT routes show "No active MRT disruptions" (not "MRT/LRT").
- **`recommended_mode` dynamic derivation** — derived from actual leg modes in the route (`modes_in_legs = {l["mode"] for l in legs}`). Combinations produce: "Bus + MRT", "Bus + LRT", "Bus + MRT and LRT", "MRT", "LRT", "Direct Bus", "Walk". Previously approximated from `num_transfers`.
- **Inline first-transit live arrival** — replaces the separate "Live arrivals — Stop XXXXX" bus board. After the step-by-step legs, one line shows: for bus — X1 + X2 (minutes to first and second bus) + crowding; for MRT/LRT — headway estimate. Alt routes show X1 only in the notes line. See D33 in DECISIONS.md.
- **`MRT_LINE_NAMES` dict** — "NE" (Northeast Line) was previously missing, causing raw "NE" to display instead of "Northeast Line". Fixed in session 6. Dict now covers: EW, NS, NE, CC, DT, TE, CR, JR (MRT mainline), BP, SE, PE (LRT).
- **Per-alt route disruption notes** — each alt route in "Other route options" section shows its own first-transit X1 and a disruption/delay note specific to that alt's rail lines and bus services. Global alerts not relevant to a specific route are suppressed.

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

# 8. ML — train, predict (all 3 routes), evaluate, backfill actuals
python scripts/model.py --train
python scripts/model.py --predict
python scripts/model.py --evaluate
python scripts/model.py --backfill

# 9. Dashboard
streamlit run scripts/serve.py   # → http://localhost:8501

# 10. API
uvicorn scripts.api:app --reload --port 8000   # → http://localhost:8000/docs

# 11. Airflow (optional — for DAG demo)
airflow standalone   # → http://localhost:8080

# 12. Full stack with smart scheduler
docker compose up          # uses scheduler.py — adaptive polling
docker compose up --build  # rebuild after code changes
docker compose down -v     # wipe DB volume (clears stale WAL lock)
docker compose logs pipeline -f   # stream pipeline + scheduler logs
```

---

## Scratch / Debug Scripts (project root, untracked)

These files exist at the project root and are **not production code** — they are one-off debug utilities from session 5 investigations. They are untracked by git and can be deleted or committed to a `scratch/` folder.

| File | Purpose |
|---|---|
| `bus_service.py` | Tests LTA v3/BusArrival endpoint directly against 3 specific stop codes (54719, 65721, 65141). Used to confirm the old BusArrivalv2 endpoint was dead and the v3 URL works. |
| `nearest_busstop.py` | Finds 5 nearest bus stops to a given coordinate using `haversine()` from `scripts/ingest`. Used to debug which stop codes the pipeline was trying. |

---

## Mentoring Notes

- Student is new to GitHub — give explicit step-by-step `git add / commit / push` commands
- Explain WHY before HOW — student presents this in a 15-minute video
- After every concept, include "In your video, say this..." guidance
- Do not add features beyond what is asked
- Ask "do you have your LTA API key?" before any session involving LTA API calls
- The three rubric criteria visible in the video: pipeline (30) + ML (30) + technical depth (10)
- Reflection section in video is mandatory — 3 questions pre-drafted in video_script.html
