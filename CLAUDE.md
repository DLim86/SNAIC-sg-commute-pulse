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

## Current State (as of 26 June 2026, session 9)

### Done — tested and working
| File | Status |
|---|---|
| `.gitignore` | Complete |
| `config_example.py` | Complete — template includes HOME_ADDRESS, WORK_ADDRESS, GARMIN_EMAIL/PASSWORD, WHOOP_ACCESS_TOKEN |
| `config.py` | Exists locally, gitignored — real credentials + GARMIN_EMAIL/PASSWORD/WHOOP_ACCESS_TOKEN added as `""` (optional, skip by leaving blank) |
| `requirements.txt` | Complete — `>=` pins for C-extension compat; includes `garminconnect>=0.2.0`, `scikit-learn>=1.4.0`, `joblib>=1.3.0` |
| `README.md` | Complete |
| `docs/roadmap.html` | Complete — 12-station roadmap. **2026-06-28: Station D5 unlocked** (GPU/Triton/BI/Technology Selection Exercise). Phase 4 divider updated. |
| `docs/AI_HANDOFF.md` | Complete — full handoff context. **2026-06-28: rubric table updated to ALL DONE, Day 5 section added, file list updated to D38** |
| `docs/video_script.html` | Complete — timed 15-min script. **2026-06-28: "Start All Services" setup section added at top; Docker showcase is Section 9 (11:00–12:00); Q3 4th paragraph covers Day 5 GPU/Triton rationale; RUNBOOK.md merged in** |
| `scripts/__init__.py` | Empty — required for Airflow DAG imports |
| `scripts/schema.py` | **DONE** — 9 tables + `v_enriched_routes` view. `predictions` table added (session 4). **session 6 — `recommendation_reason` CASE expanded to 9 dynamic labels using `MIN() OVER (PARTITION BY event_id)` window functions.** **session 8 — 4 new `predictions` columns: `option_id`, `boarding_stop_code`, `alighting_stop_code`, `transit_service_no`.** Run once (or re-run to refresh view). |
| `scripts/ingest.py` | **DONE** — Calendar + 4 APIs + retry/backoff + Parquet + legs + idempotent upsert + IP-geolocation origin (always) + progressive geocoding fallback + WORK_ADDRESS fallback + `get_smart_default()` + `v3/BusArrival` + 5-nearest-stop fallback + float-suffix strip + **`_purge_stale_events()`** (session 5) + **`next_bus2_mins`** (session 5) + **FK fix in `ingest_routes`** (session 5) + **postal code extraction in `geocode()`** (session 7) + **location-change detection log** (session 7) + **session 9 — 3 argparse modes: `--mode calendar-check` (Group 1: cached geocode, no ip-api during IMMINENT), `--mode routes` (Group 2: OneMap + LTA, triggered only on event/dest change), `--mode weather` (Group 3: data.gov.sg only). Default (no flag) = full pipeline, unchanged.** |
| `scripts/transform.py` | **DONE** — next-event-only filter, LEAVE LATEST + LEAVE NOW, step-by-step legs, "Why chosen", **alt routes** (session 5), walk alternative with Zone 1/2, optional Garmin/Whoop + **session 6:** dynamic `recommended_mode`; disruption filtered to route's rail lines; inline live arrival; `MRT_LINE_NAMES` NE/CR/JR + **session 7:** HH:MM SGT clock times; `_walk_metrics()` helper; walk-only inline display + **session 8:** `alt_rows = [] if is_walk_only` |
| `scripts/serve.py` | **DONE** — Streamlit dashboard, `read_only=True` DuckDB, **60s auto-refresh** via `time.sleep(60); st.rerun()`, event card, 3-column leave-by/duration/fare metrics, "Why chosen" info block, conditional weather/disruption warnings, step-by-step legs with mode icons, inline live arrivals with clock times, alt route expanders [2]/[3] with compact tokens + ML captions, **ML prediction panel**. **session 8:** PREDICTION_QUERY keyed by `{option_id}_pred`. **session 9 CRITICAL FIX: removed `@st.cache_resource` from `get_connection()` — the cached connection never saw new DuckDB data after the pipeline wrote to it. Now opens a fresh read-only connection every 60s rerun.** Run: `streamlit run scripts/serve.py` |
| `scripts/model.py` | **DONE** (session 8) — `--train` (RandomForest + GradientBoosting, 500 synthetic bootstrap rows); `--predict` (scores ALL 3 route options, `prediction_id = f"{option_id}_pred"`); `--evaluate` (7-day MAE); `--backfill` (mode-aware: BUS → LTA alighting stop API, MRT → `total_duration_min + 4` or +20 disruption, LRT → +7/+20, WALK → deterministic) |
| `scripts/api.py` | **DONE** (session 9) — FastAPI, 6 endpoints: `GET /health`, `GET /api/v1/recommendation/next`, `GET /api/v1/recommendation/{event_id}`, `GET /api/v1/prediction/{event_id}`, `GET /api/v1/pipeline/status`, `GET /api/v1/alerts`. Pydantic v2 response models (Leg, LiveArrival, RouteOption, RecommendationResponse, MLPrediction, PipelineRun, Alert). Per-request `contextmanager get_db()` — opens and closes a read-only DuckDB connection on each request. `/recommendation/next` defined BEFORE `/{event_id}` to avoid FastAPI route collision. Run: `uvicorn scripts.api:app --reload --port 8000` → Swagger at `http://localhost:8000/docs` |
| `scripts/scheduler.py` | **DONE** (session 9) — Smart 4-state pipeline manager replacing the fixed shell loop in docker-compose.yml. States: **NO_EVENT** (60s day / 1hr midnight–6am) → **WATCHING** (sleep until `leave_by - 30 min`, capped 1hr) → **IMMINENT** (1s poll, cached geocode, no ip-api) → **EXPIRED** (immediate reset). **Group 1** (`--mode calendar-check`): runs every tick, checks Google Calendar, uses cached coords if event_id+location_raw unchanged — no OneMap call during IMMINENT. **Group 2** (`--mode routes`): fires only when printed key changes (event_id or dest shift >10m) — OneMap routing + LTA bus/train + transform + predict + backfill. **Group 3** (`--mode weather`): fires every 30 min regardless of state — weather + transform. |
| `dags/__init__.py` | **DONE** (session 9) — Empty file, required so Airflow can import from `dags/` as a package |
| `dags/commute_pipeline_dag.py` | **DONE** (session 9) — 7-task sequential chain: `schema_check >> ingest >> transform >> predict_commute >> backfill_actuals >> gate_evaluate >> evaluate_model`. `gate_evaluate` is `ShortCircuitOperator` — passes only when `datetime.now(SGT).hour == 8`. Schedule: `*/30 * * * *`. `catchup=False`. Uses `BashOperator` with absolute `PROJECT_DIR` path. Run: `airflow standalone` → UI at `http://localhost:8080` |
| `Dockerfile` | **DONE** (session 9) — `python:3.12-slim` + `libgomp1` (scikit-learn parallel trees) + `pip install -r requirements.txt` + `mkdir -p db data/raw models`. Default CMD: `python scripts/ingest.py` (overridden per service in docker-compose.yml) |
| `.dockerignore` | **DONE** (session 9) — excludes `.git/`, `__pycache__/`, `*.duckdb`, `models/*.pkl`, `LTA/`, `OneMap/`, `Prompt.txt`, `*.log`, `docs/` |
| `docker-compose.yml` | **DONE** (session 9) — 3 services sharing `db_data` named volume. pipeline: `python scripts/scheduler.py` (state machine). api: `uvicorn scripts.api:app --host 0.0.0.0 --port 8000 --workers 1`. dashboard: `streamlit run scripts/serve.py --server.headless=true`. Volume mounts: `db_data:/app/db`, `./token.json`, `./credentials.json`. **IMPORTANT: `docker compose down -v` needed to wipe stale WAL lock if pipeline container was killed mid-write.** |
| `db/commute.duckdb` | Exists locally, gitignored — populated by real Google Calendar events |
| `data/raw/bus_stops/bus_stops.parquet` | Cached — 5,205 LTA bus stops |
| `data/raw/weather/` | Populated — 47 weather areas |
| `data/raw/onemap_route/` | Populated — 3 route options |

### All build tasks complete ✅
All scripts, DAG, Docker, and scheduler are built and committed. The project is ready for documentation review and video recording. See "Next Tasks" for video prep guidance.

---

## Calendar Data

Events are read from Google Calendar via OAuth2 (`fetch_next_calendar_event()` in `ingest.py`).
- Scans next 10 upcoming events, skips all-day events and events with no location
- Geocodes the first valid location via OneMap
- `event_id` format: `GCAL_{google_event_id}`
- First run opens a browser for Google consent; writes `token.json` to project root (gitignored)
- `GOOGLE_CALENDAR_ID = "primary"` in `config.py` — change to a specific calendar ID if needed

---

## Next Tasks — Video Prep (all code is complete)

All build tasks from sessions 1–9 are complete. The remaining work is:

1. **Video recording** — use `docs/video_script.html` as the script. The "Start All Services" setup section at the top has all startup commands and URLs. Docker showcase is Section 9 (11:00–12:00). Services to start before recording:
   - Terminal 1: `python scripts/ingest.py` then `python scripts/transform.py` then `python scripts/model.py --predict`
   - Terminal 2: `streamlit run scripts/serve.py` → `http://localhost:8501`
   - Terminal 3: `uvicorn scripts.api:app --reload --port 8000` → `http://localhost:8000/docs`
   - Terminal 4: `airflow standalone` → `http://localhost:8080`
   - Terminal 5 (just before Section 9): `docker compose up`
2. **PDF resume bullets** — copy from Section 11 in `docs/video_script.html` or Station 12 in `docs/roadmap.html`
3. **Day 5 complete** — Station D5 unlocked in roadmap.html, D38 added to DECISIONS.md, AI_HANDOFF.md updated. No further code changes needed.

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

Nine tables (schema.py creates all of these — run once):

```
calendar_events   — event_id PK, title, start_time TIMESTAMPTZ, dest_lat, dest_lng
route_options     — option_id PK, event_id FK, total_duration_min, walk_distance_m, fare
route_legs        — (option_id, leg_sequence) PK, mode, service_no, from_name, to_name
weather_forecast  — (area, valid_start) PK, forecast, is_rainy BOOLEAN, fetched_at
bus_arrivals      — (bus_stop_code, service_no, fetched_at) PK, next_bus_mins, load
train_alerts      — alert_id PK, affected_line, message, severity, fetched_at
recommendations   — event_id PK, leave_by TIMESTAMPTZ, reason, created_at
pipeline_runs     — run_id PK, source, rows_upserted, status, error_msg, ran_at
predictions       — prediction_id PK, event_id, predicted_min, actual_min, model_version, mae_7day, predicted_at
```

`predictions` table is already in `scripts/schema.py` — no changes needed before building `scripts/model.py`.

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
- **LTA Bus Arrival endpoint URL:** correct URL is `https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival` — the old `BusArrivalv2` path was retired in LTA DataMall API v6.0 (August 2024) and returns "The requested API was not found" (404) for ALL stop codes. This was fixed in `ingest.py` session 4.
- **LTA bus stops vs GPS:** LTA bus stop codes don't have GPS coordinates in the v3/BusArrival endpoint — use Haversine distance against a bus stop list to find the nearest stop. Bus stop list is cached at `data/raw/bus_stops/bus_stops.parquet`. Try 5 nearest candidates in order — some stops in the 65xxx Punggol/Sengkang range exist in the BusStops list but are not in the real-time system. Skip those and try the next.
- **LTA BusStopCode float suffix:** if the Parquet bus stop file has any NaN rows, pandas promotes the BusStopCode column from int to float64 (e.g. "65721" becomes "65721.0"). LTA rejects codes with a decimal point. Fix: always use `str(code).split(".")[0]` when formatting the stop code for the API request.
- **LTA v3/BusArrival returns 404 (not empty array) for stops with no active services** — for 404 on a valid stop, handle gracefully (treat as "no data" not an error). Distinguish from the endpoint-not-found 404 by checking the response body for "The requested API was not found".
- **data.gov.sg weather areas:** `area_metadata` in the API response includes `label_location` with lat/lng for each area — no separate lookup needed
- **DuckDB write lock:** only one connection can write at a time; the pipeline must close its connection before FastAPI opens one
- **`scripts/` imports `config.py` from project root** — all scripts must add `sys.path.insert(0, str(Path(__file__).parent.parent))` before `from config import ...`
- **Python 3.14 compatibility:** C-extension packages (pandas, duckdb, pyarrow, shapely) must use `>=` version pins — no pre-built wheels for old pinned versions on 3.14
- **`datetime.utcnow()` deprecated in Python 3.12+** — use `datetime.now(timezone.utc).replace(tzinfo=None)` for naive UTC into TIMESTAMP columns
- **OneMap routing `duration` is in seconds** — divide by 60 for `total_duration_min`
- **`get_onemap_token()` uses `requests.post`, not `fetch_with_retry`** — has its own retry loop with 30s timeout
- **OneMap `leg.get("route", {})` can return a string** — always check `isinstance(route_field, dict)` before calling `.get("shortName")` on it (fixed in ingest.py)
- **`v_enriched_routes` cross-join:** view joins all 47 weather areas per route (141 rows for 3 routes). `route_rank=1` still gives exactly one row per event — safe to use in transform.py. Do NOT query `route_rank > 1` from the view to get alternative routes — you will get N×47 duplicates. Query `route_options` directly instead (skips weather join, which is not needed for alt routes).
- **transform.py BEST_ROUTE_QUERY requires `AND start_time > NOW()`** — without this filter, the query picks the oldest stored event (even yesterday's), not the next upcoming one. The fix is in BEST_ROUTE_QUERY in transform.py.
- **`route_legs` table** added in latest schema.py — run `python scripts/schema.py` then `python scripts/ingest.py` to populate legs
- **Geocoding progressive fallback:** `geocode()` in ingest.py tries candidates in order: (0) 6-digit postal code extracted via `re.findall(r'\b\d{6}\b', address)` if present — most reliable (session 7); (1) full address; (2) address with ", Singapore" stripped; (3) first comma-delimited token. Postal codes geocode with near-perfect accuracy on OneMap. Obscure street names like "Sentul Walk" may not be in OneMap's index.
- **WORK_ADDRESS in config.py:** set this to your school/work postal code. Used as destination fallback when event geocoding fails AND as smart default destination 8–10 AM (no calendar event).
- **HOME_ADDRESS in config.py:** set this for the go-home smart default (after 4 PM) and for the after-6 PM at-home detection. NOT used as routing origin — origin is always IP geolocation.
- **`get_smart_default()` time windows:** 8–10 AM → WORK_ADDRESS; 4–6 PM → HOME_ADDRESS (depart 6:30 PM); after 6 PM → geocode HOME_ADDRESS, compare IP location, if within 3 km return None (skip, already home). Outside these windows → skip pipeline quietly.
- **Google Calendar no-event case:** pipeline now calls `get_smart_default()` before skipping. Only truly skips if WORK_ADDRESS/HOME_ADDRESS are not set OR it's outside the routing windows OR user is already home.
- **Google Calendar first run:** browser opens for OAuth2 consent — must be on a machine with a browser. Writes `token.json` to project root. For Docker/Airflow: pre-generate `token.json` locally and volume-mount it.
- **IP geolocation (`ip-api.com`):** returns city-level accuracy (~1–5 km), free, no API key. Returns non-SG coords if user is on a VPN — falls back to Bishan (1.3521, 103.8198) with a warning. HTTP not HTTPS on free tier.
- **Walk suggestion (transit route):** only shown when `is_rainy = False` AND Haversine distance from origin to dest < 5 km. Uses `_detect_origin()` in transform.py (IP geolocation — does NOT call OneMap, no token needed there).
- **Walk-only route inline display (session 7):** when `is_walk_only = bool(legs) and all(l["mode"] == "WALK" for l in legs)`, `_walk_metrics()` is called inline after the step-by-step legs — regardless of distance or weather. The 5km and `is_rainy` guards do NOT apply. `print_walk_suggestion()` is skipped for walk-only routes. `_walk_metrics(origin_lat, origin_lng, dest_lat, dest_lng)` is a helper extracted from `print_walk_suggestion()` and shared by both code paths.
- **Garmin steps:** requires `pip install garminconnect` — already in requirements.txt. Uses unofficial email/password auth. Leave `GARMIN_EMAIL = ""` in config to skip silently.
- **Whoop recovery:** requires `WHOOP_ACCESS_TOKEN` in config.py — generate from developer.whoop.com. Returns recovery score 0–100. Leave blank to skip.
- **model.py — cold start problem:** pipeline may only have a few days of real data. Bootstrap `predictions` with 500 synthetic historical rows using known patterns (rush hour +15%, rain +8%, weekend −20%) before fitting the model. Synthetic rows can be marked with `model_version = "synthetic"` so they can be filtered out later.
- **model.py — `scikit-learn` and `joblib` must be added to requirements.txt** — `scikit-learn>=1.4.0` and `joblib>=1.3.0`
- **model.py — `models/` folder must be gitignored** — `.pkl` files are binary artifacts, not source code. Add `models/*.pkl` to `.gitignore` (but commit `models/.gitkeep` so the folder exists in the repo).
- **evaluate_model task** — only makes sense once `predictions` has at least 7 rows with non-null `actual_min`. Handle the cold start gracefully: if fewer than 7 actuals exist, log a warning and skip evaluation rather than crashing.
- **`actual_min` backfill** — after the commute time passes, a separate pipeline task should compare `predicted_min` to the route that was actually taken (`total_duration_min` from `route_options` for the same `event_id`) and fill in `actual_min`.
- **Stale event cleanup (`_purge_stale_events`)** — ingest.py accumulates future calendar events across runs. If you reschedule a calendar event, the old entry (with old start_time) stays in the DB and transform picks it (both appear in `start_time > NOW()`). Fixed in session 5: after fetching the active event, `_purge_stale_events(con, event_id)` deletes all other future `calendar_events` + their `route_options` + `route_legs`. Called for both calendar and smart-default paths.
- **DuckDB FK constraint on `INSERT OR REPLACE` into `route_options`** — `route_legs` has a FK referencing `route_options(option_id)`. DuckDB's `INSERT OR REPLACE` deletes then re-inserts, but the delete fails if `route_legs` still references that `option_id`. Fix: `ingest_routes()` deletes all `route_legs` for the event before upserting `route_options`. Added in session 5.
- **HOME_ADDRESS must NOT be routing origin** — a previous session incorrectly geocoded HOME_ADDRESS as the routing origin. If the calendar event destination is also home (e.g. an event called "Home"), origin = destination → OneMap returns 404. Reverted in session 5: origin is always IP geolocation only. HOME_ADDRESS is used only as a *destination* (go-home default, at-home proximity check).
- **Debug scripts at project root (untracked by git):** `bus_service.py` — one-off script to test LTA v3/BusArrival against 3 stop codes; `nearest_busstop.py` — one-off script to print 5 nearest stops to a given coordinate using `haversine()` from ingest.py. Both are debug utilities from session 5 investigations, not production code.
- **Destination bus stop not stored** — `ingest_bus_arrivals` finds the nearest bus stop to the *origin* and fetches live arrival times. The nearest bus stop to the *destination* is never looked up or stored. OneMap route_legs already has `to_name` (the stop name where you alight), but not the LTA bus stop code for that stop. Future enhancement: after `ingest_routes()`, call `nearest_bus_stops(dest_lat, dest_lng, stops_df, n=1)` and store the result as `dest_bus_stop_code` in `calendar_events` (requires schema change). Would let serve.py show "Board stop 65141 → Alight stop 65019 (3 min walk to destination)".
- **`route_legs.num_stops`** — OneMap `intermediateStops` array gives stops between boarding and alighting. `num_stops = len(intermediateStops) + 1` (the +1 counts the alighting stop). WALK legs store `NULL`. Added in session 5 via `ALTER TABLE route_legs ADD COLUMN IF NOT EXISTS num_stops INTEGER` migration in schema.py.
- **Alt routes cross-join gotcha** — `v_enriched_routes` cross-joins 47 weather areas per route (47 × N rows). Using `route_rank > 1` returns 47×(N-1) duplicates, not N-1 distinct alternatives. **Always query `route_options` directly for alt routes** — the view is only safe with `route_rank = 1 LIMIT 1`.
- **OneMap `numItineraries` capped at 3** — requesting `numItineraries: 4` in the routing call returns HTTP 400 Bad Request. OneMap's public routing API hard-caps at 3 itineraries. The system therefore always returns exactly 3 routes total: [1] recommended + [2] + [3] as alternatives. The alt routes section will always show 2 alternatives, never 3.
- **MRT/LRT have no public real-time arrival API** — Singapore's SMRT and SBS Transit do not publish a real-time train arrival API for developers. The system displays fixed headway estimates: ~3-5 min for MRT (peak), ~5-10 min for LRT. These are educated approximations, not live data. Live data is only available for buses via LTA v3/BusArrival.
- **Dynamic disruption filtering (session 6)** — `train_alerts.affected_line` is now matched against the actual `service_no` values of rail legs in the route before showing a disruption status. A bus-only route shows no disruption section. A pure MRT route shows "No active MRT disruptions" (not "MRT/LRT"). Build `route_rail_lines = {leg.service_no for leg in legs if leg.mode in ("MRT","LRT")}` then filter `relevant_alerts = [a for a in alerts if a[0] in route_rail_lines]`.
- **`recommended_mode` dynamic derivation (session 6)** — `recommended_mode` written to the `recommendations` table is now derived from the actual set of modes in the route's legs: `modes_in_legs = {l["mode"] for l in legs}`. Combinations: Bus+MRT+LRT → "Bus + MRT and LRT"; Bus+MRT → "Bus + MRT"; MRT only → "MRT"; Bus only → "Direct Bus"; Walk only → "Walk". Previously this was approximated from `num_transfers`.
- **Inline first-transit live arrival (session 6)** — the separate "Live arrivals — Stop XXXXX" bus board section has been removed. A single first-transit arrival now appears inline after the step-by-step legs: X1 and X2 (minutes) for bus, or headway estimate for MRT/LRT. Per-alt route notes show X1 only. See D33 in DECISIONS.md.
- **`MRT_LINE_NAMES` dict** — "NE" (Northeast Line) was missing from the dict, causing `NE` to appear as the display name instead of "Northeast Line". Fixed in session 6. Dict now includes: EW, NS, NE, CC, DT, TE, CR, JR (mainline MRT), BP, SE, PE (LRT).
- **Alt routes show per-route disruption (session 6)** — each alt route in the "Other route options" section includes its own first-transit X1 live arrival and a disruption/delay note filtered to that alt's specific rail lines and bus services.
- **Live arrival clock times (session 7)** — `transform.py` now shows actual HH:MM SGT clock times instead of abstract relative minutes. Computed as `(now_sgt + timedelta(minutes=x)).strftime("%H:%M")` where `now_sgt = datetime.now(timezone.utc).astimezone(SGT)`. MRT headway midpoints: x1=4 min, x2=8 min. LRT: x1=7 min, x2=14 min.
- **`serve.py` auto-refresh** — uses `time.sleep(60); st.rerun()` at the bottom of the script. The page shows a "Running…" spinner for 60 seconds before refreshing. This does NOT trigger ingest or transform — it only re-queries DuckDB. To update the data, run `python scripts/ingest.py` then `python scripts/transform.py` in a separate terminal; `serve.py` picks up new data on its next 60s refresh.
- **`serve.py` ML panel** — shows placeholder "Model not yet trained — run `python scripts/model.py --train` then `--predict`" until the `predictions` table has at least one row for the current `event_id`. The panel renders automatically once model.py has been run.
- **`geocode()` postal code extraction (session 7)** — `re.findall(r'\b\d{6}\b', address)` extracts the first 6-digit sequence and prepends it to the candidates list. Word boundary `\b` prevents matching 7-digit strings (phone numbers, IDs). Case-insensitive "singapore" keyword check via `address.lower()`. Both detection paths (with/without "singapore") result in the same action.
- **Location-change detection log (session 7)** — before the ingest loop in `main()`, stored `dest_lat`/`dest_lng` is queried and compared against the newly geocoded values. If Haversine shift > 50m, logs `📍 Destination updated (NNN m shift) — will re-fetch routes`. Purely diagnostic — routes are always re-fetched regardless.
- **model.py `predict()` weather cross-join (session 8)** — do NOT join `weather_forecast` with `ON w.fetched_at = (SELECT MAX(fetched_at) ...)` inside the predict query. That matches all 47 weather areas and produces 47× rows (same gotcha as `v_enriched_routes`). Use a scalar subquery instead: `(SELECT COALESCE(is_rainy, FALSE) FROM weather_forecast ORDER BY fetched_at DESC LIMIT 1)`.
- **model.py `prediction_id` format (session 8)** — changed from `{event_id}_pred` to `{option_id}_pred`. Old rows (pre-session-8) have `option_id IS NULL` and `prediction_id = {event_id}_pred`. serve.py queries by `prediction_id` so old rows are silently ignored (user sees "not yet trained" until `--predict` is re-run).
- **model.py `_match_stop_name()` (session 8)** — matches `route_legs.to_name` text against `bus_stops.Description` (exact lowercase, then first-15-char prefix). Only applies to BUS legs. MRT/LRT set `transit_service_no` but leave `alighting_stop_code = None` (no stop-level lookup for rail). `stops_df` loaded from `bus_stops.parquet` with `dropna(subset=["BusStopCode","Description"])` to avoid NaN float promotion issue.
- **model.py `backfill()` mode dispatch (session 8)** — mode determined per `option_id` from `route_legs`. BUS: calls LTA v3/BusArrival for alighting stop if within 3 hours of event; falls back to `total_duration_min + boarding_wait`. MRT: `total_duration_min + 4` (headway) unless HEAVY disruption in `train_alerts` window → `+20`. LRT: same with headway=7. WALK: `total_duration_min` (deterministic). Old predictions without `option_id` use legacy route_options proxy.
- **`serve.py` ML panel keyed by `prediction_id` (session 8)** — `PREDICTION_QUERY` uses `WHERE prediction_id = ?`. Recommended route passes `f"{option_id}_pred"`. Each alt expander passes `f"{alt_id}_pred"`. `CROWD_ICON`/`CROWD_LABEL` moved to module level (line 16-17) so both recommended and alt route sections can use them.
- **`serve.py` `@st.cache_resource` stale connection bug (session 9 fix)** — `@st.cache_resource` on `get_connection()` cached the DuckDB connection at Streamlit startup. Every 60s rerun called the same connection object and never saw rows written by the pipeline. Fix: removed the decorator. Now opens a fresh `duckdb.connect(DB_PATH, read_only=True)` every rerun. Requires Streamlit restart (Ctrl+C + re-run) to clear the old cached connection from memory.
- **`scripts/scheduler.py` state machine (session 9)** — replaces the fixed `sleep 1800` shell loop in docker-compose.yml. **4 states**: NO_EVENT (60s day / 1hr midnight–6am), WATCHING (sleep until `leave_by - 30min`), IMMINENT (1s polling), EXPIRED (reset). **Group 1** `calendar-check` mode runs every tick; uses cached geocode during IMMINENT so no OneMap API call per second. **Group 2** `routes` mode fires only when `new_key != prev_key` (event_id or dest shifted >10m). **Group 3** `weather` mode fires every 30 min independent of state. `docker-compose.yml` pipeline command is now `python scripts/scheduler.py`.
- **`ingest.py --mode calendar-check` geocode cache** — `_fetch_raw_calendar_event()` fetches only event_id + `location_raw` string from Google Calendar (no OneMap). If `stored.location_raw == new_location_raw` and coords are in DB → prints cached key, returns. Only calls `get_onemap_token()` + `geocode()` when event or location string actually changes.
- **`ingest.py --mode routes` origin refresh** — calls `get_current_location()` (ip-api) fresh on every routes run. ip-api is only called in Group 2 (route refresh), never during IMMINENT 1s polling. Rate limit: 45 req/min free — Group 2 fires at most once per 60s cycle.
- **`api.py` FastAPI routing order** — `/api/v1/recommendation/next` must be defined BEFORE `/{event_id}` in the file. FastAPI matches routes in definition order; if `/{event_id}` is first, the string "next" is captured as the event_id parameter and the next endpoint never fires.
- **`api.py` per-request DuckDB connection** — uses `contextmanager get_db()` which opens `duckdb.connect(DB_PATH, read_only=True)` and closes it in the `finally` block. Avoids holding a file handle that could block the pipeline's write connection. Multiple API requests can open simultaneous read-only connections to DuckDB safely.
- **Airflow DAG vs scheduler.py** — two separate orchestration paths: (1) `airflow standalone` → runs the DAG via the Airflow UI (good for demo, shows the task graph); (2) `docker compose up` → runs `scheduler.py` which manages the state machine directly (production use, no Airflow dependency). The Airflow DAG calls `ingest.py` (full pipeline, no modes) — it was built before `scheduler.py` was added. Both paths are valid.
- **DuckDB WAL lock in Docker** — if the pipeline container is killed mid-write, a `.duckdb.wal` file is left behind in the named volume. Next container start fails with `Could not set lock on file ... Conflicting lock is held in PID 0`. Fix: `docker compose down -v` (removes the named volume) then `docker compose up --build`. Data in `db_data` volume is lost — repopulate by running the pipeline once after restart.
- **Airflow DAG task chain** — actual implementation is a **sequential** chain: `schema_check >> ingest >> transform >> predict_commute >> backfill_actuals >> gate_evaluate >> evaluate_model`. There is no parallel branch in the actual code (the roadmap shows a planned parallel design that was simplified). `ingest.py` handles weather + routes + bus + train internally in sequence. `gate_evaluate` is `ShortCircuitOperator` — only passes when `datetime.now(SGT).hour == 8`.

---

## Course Context

- Day 1 content: pipeline fundamentals, DuckDB, SQL transformation, Streamlit — **DONE**
- Day 2 content: retry/backoff, Parquet, FastAPI, Airflow, Docker — **DONE (session 9)**
- Day 3 content: Kafka, Flink, dbt, Spark, data modelling (star schema, lake vs warehouse) — **DONE (review complete)**
- Day 4 (2026-06-25): MLOps lifecycle — MLflow experiment tracking, Model Registry `@champion` alias, data drift / concept drift / training-serving skew, shadow → canary deployment, Prometheus + Grafana monitoring. Decisions D29–D31 added.
- Session 6 (2026-06-25): transform.py improvements — dynamic `recommended_mode`, inline first-transit live arrivals, dynamic disruption filtering, `MRT_LINE_NAMES` NE/CR/JR. schema.py `recommendation_reason` CASE expanded. Decisions D32–D34 added.
- Session 8 (2026-06-25): Per-route ML predictions (all 3 options, keyed by option_id); mode-aware actual backfill; serve.py ML panel per alt route; weather cross-join bug fixed; walk-only alt routes skipped.
- Session 9 (2026-06-26): api.py (6 FastAPI endpoints), Airflow DAG (7 tasks + ShortCircuit), Docker (3-service compose), serve.py @st.cache_resource bug fixed, ingest.py 3 argparse modes, scheduler.py state machine (4 states, 3 groups).
- Day 5 (2026-06-28): GPU Acceleration (RAPIDS/cuDF/cuML/Dask-cuDF/CuPy), NVIDIA Triton Inference Server, Feature Store, Business Intelligence (Metabase). Technology Selection Exercise validates existing choices: low-traffic prediction API → FastAPI ✅; 100MB data → pandas + Parquet ✅. No code additions. `docs/roadmap.html` Station D5 unlocked; `docs/DECISIONS.md` D38 added; `docs/AI_HANDOFF.md` updated.
