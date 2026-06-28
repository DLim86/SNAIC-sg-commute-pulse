# Design Decisions — SNAIC-sg-commute-pulse

Each decision is recorded with the reasoning, so future developers (or AI sessions) understand
why the code is the way it is — not just what it does.

---

## D01 — DuckDB over PostgreSQL or SQLite

**Decision:** Use DuckDB as the storage engine.

**Why:**
- No server to install or manage — DuckDB is a single `.duckdb` file opened in-process with Python
- Supports the full analytical SQL feature set needed: window functions (`ROW_NUMBER() OVER`), `INTERVAL` arithmetic, `TIMESTAMPTZ`, `LEFT JOIN`, correlated subqueries
- SQLite supports SQL but lacks window functions without extensions
- PostgreSQL would require a running server, a connection string, Docker-level setup — overkill for a single-user pipeline
- DuckDB can query Parquet files directly (`FROM 'data/raw/**/*.parquet'`), which enables the raw-zone replay pattern without a second tool

**Trade-off:** Only one write connection at a time — pipeline and serving layer must not write simultaneously.

---

## D02 — ELT Pattern (Extract → Load Raw → Transform in DB)

**Decision:** Load raw API data into DuckDB first, then transform with SQL — not ETL (transform before loading).

**Why:**
- The transformation logic (`v_enriched_routes` view) uses SQL window functions — best expressed in SQL, not Python
- If the transformation SQL has a bug, raw data is already in DuckDB and can be re-transformed without re-calling APIs
- The Day 1 course content explicitly teaches ELT with DuckDB
- Keeps Python code simple: fetch → flatten → insert. All business logic lives in SQL

**Trade-off:** The raw tables hold unjoined data. The view joins them at query time, which is fine for DuckDB at this data volume.

---

## D03 — Retry with Exponential Backoff on All API Calls

**Decision:** Every `requests.get()` must go through `fetch_with_retry(url, headers, params, max_retries=3)`.

**Why:**
- LTA DataMall returns HTTP 429 (rate limit) and 500 (server error) during morning peak hours (07:00–09:00)
- Without retry, a single API blip kills the entire pipeline run
- Exponential backoff (1s, 2s, 4s) reduces retry pressure on the server
- `raise_for_status()` ensures non-2xx responses are caught, not silently accepted

**Implementation:**
```python
for attempt in range(max_retries):
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        if attempt == max_retries - 1:
            raise
        time.sleep(2 ** attempt)
```

---

## D04 — Idempotency via INSERT OR REPLACE

**Decision:** All inserts use `INSERT OR REPLACE INTO table SELECT ...` — never bare `INSERT`.

**Why:**
- The pipeline runs every 10 minutes via Airflow
- If it crashes mid-run and restarts, it must not create duplicate rows
- `INSERT OR REPLACE` silently overwrites on primary key conflict — same result whether run once or ten times
- This property is called **idempotency** — critical for any production pipeline

**Trade-off:** Slightly slower than `INSERT` because DuckDB checks the PK before writing. Negligible for this data volume.

---

## D05 — GPS Coordinate Validation Before Any Insert

**Decision:** Reject any coordinate outside Singapore's bounding box before inserting to DuckDB.

**Bounds:** `lat 1.15 – 1.47`, `lng 103.6 – 104.1`

**Why:**
- OneMap's geocoding API occasionally returns `(0, 0)` — the null island — for addresses it cannot resolve
- `(0, 0)` is valid SQL data — DuckDB accepts it without error
- All downstream calculations (nearest bus stop, nearest weather area, Haversine distance) would compute nonsense results silently
- A `ValueError` at ingestion time is far easier to debug than a recommendation of "leave by 00:00 to catch a bus in the Atlantic Ocean"

---

## D06 — TIMESTAMPTZ for Event/Schedule Columns

**Decision:** All columns holding event times or scheduled times use `TIMESTAMPTZ`. Internal tracking columns (`fetched_at`, `ran_at`) use `TIMESTAMP` (no timezone).

**Why:**
- Singapore is UTC+08:00. Calendar events from `.ics` files include the offset
- `TIMESTAMPTZ` stores the UTC value and converts correctly when displaying in SGT
- `TIMESTAMP` without timezone causes subtle bugs when comparing event times to `NOW()` (which DuckDB returns in UTC)
- Internal tracking columns use `TIMESTAMP` because they're always written by the local pipeline and compared locally — no timezone ambiguity

---

## D07 — 10-Minute Buffer in Leave-By Calculation

**Decision:** `leave_by = start_time - INTERVAL (total_duration_min + 10) MINUTE`

**Why:**
- The 10-minute buffer accounts for: walking from your home to the bus stop, waiting for the bus (not captured by routing time), unexpected delays
- This is a documented **business rule** — not a magic number
- It is adjustable: if a user is consistently early/late, the buffer can be increased to 15 or decreased to 5

---

## D08 — 400m Walk Distance Threshold for Rain Penalty

**Decision:** Routes with `walk_distance_m > 400` are penalised when `is_rainy = TRUE`.

**Why:**
- 400 metres is approximately a 5-minute walk in Singapore heat
- Under rain, a 5-minute+ outdoor walk is meaningfully uncomfortable and would affect route choice
- Shorter walks (< 400m) are considered acceptable under a brief umbrella
- Routes are not *excluded* when rainy — they are *penalised* in the `ROW_NUMBER()` ordering. The user can still see them as alternatives

---

## D09 — 30-Minute Staleness Window for Train Alerts

**Decision:** `LEFT JOIN train_alerts ta ON ta.severity = 'HEAVY' AND ta.fetched_at > NOW() - INTERVAL '30 minutes'`

**Why:**
- Train alerts from LTA may take minutes to be posted or retracted after an incident
- An alert from 2 hours ago that was never deleted from the table should not continue affecting recommendations
- 30 minutes is a conservative window — most MRT disruption alerts resolve within that time or are updated
- If the alert table is empty or stale, the LEFT JOIN produces NULL for `alert_msg`, and the CASE WHEN handles this gracefully

---

## D10 — Parquet Raw Zone as Intermediate Layer

**Decision:** Save raw API JSON responses as Parquet files before loading to DuckDB.

**Pattern:** `data/raw/{source}/date=YYYY-MM-DD/{source}_{YYYY-MM-DD}.parquet`

**Why:**
- Enables **replay**: if the SQL transformation has a bug, re-run the transform from raw Parquet without re-calling the API
- Creates an **audit trail**: what did LTA actually return at 08:47 AM on July 15?
- DuckDB can query Parquet directly (`FROM 'data/raw/bus_arrivals/date=2026-07-15/*.parquet'`) — no second tool needed
- Follows the **raw zone / curated zone** data lake pattern taught in the course
- `data/raw/` is gitignored — it's regenerated every run and too large to commit

---

## D11 — FastAPI as Serving Layer (Separate from Streamlit)

**Decision:** Add FastAPI as a REST API between DuckDB and the Streamlit dashboard.

**Why:**
- Without FastAPI, Streamlit queries DuckDB directly — tight coupling
- With FastAPI, the recommendation logic is exposed as an API any client can call (Streamlit, Telegram bot, mobile app)
- FastAPI auto-generates Swagger UI at `/docs` — interactive documentation that is impressive in a video demo
- Demonstrates the "data product" concept: the recommendation engine is a service, not a dashboard
- FastAPI is explicitly taught in Day 2 of the course

**Architecture:** `Streamlit → FastAPI → DuckDB` (not `Streamlit → DuckDB`)

---

## D12 — Airflow for Orchestration (Not a Cron Job or While Loop)

**Decision:** Use Apache Airflow to schedule the pipeline — not `schedule.every(10).minutes` or a cron entry.

**Why:**
- Airflow provides a **web UI** to visualise task runs, see failures, and inspect logs — critical for a video demo
- A cron job or `while True: sleep(600)` has no visibility: you can't tell if a run failed or what it did
- Airflow's `PythonOperator` wraps existing Python functions with no refactoring required
- The DAG definition documents the task graph explicitly — `t1 >> t2 >> [t3, t4, t5] >> t6` is self-describing
- Airflow is explicitly taught in Day 2 of the course and is a standard DE employer requirement

**Local setup:** `airflow standalone` starts scheduler + webserver + SQLite backend in one command — no Docker required for development.

---

## D13 — Docker Compose Over Manual Deployment

**Decision:** Containerise the full stack as 3 services: `pipeline`, `api`, `dashboard`.

**Why:**
- Any reviewer can clone the repo and run `docker compose up` to start the full system — no Python/venv setup required
- The DuckDB file is volume-mounted (`./db:/app/db`) so data persists across container restarts
- Services communicate by name (`http://api:8000`) — no hardcoded IPs
- Demonstrates production awareness: "it works on my machine" is not sufficient for a real system

**Services:**
| Service | Command | Port |
|---|---|---|
| `pipeline` | `python -m scripts.ingest` | — |
| `api` | `uvicorn scripts.api:app --host 0.0.0.0 --port 8000` | 8000 |
| `dashboard` | `streamlit run scripts/serve.py --server.port 8501` | 8501 |

---

## D14 — Single Dockerfile for All 3 Services

**Decision:** One `Dockerfile` shared by all three Docker Compose services, each overriding `command`.

**Why:**
- All three services use the same Python dependencies (`requirements.txt`)
- Maintaining one Dockerfile is simpler than three
- Each service specifies its own `command:` in `docker-compose.yml` — the image has no default `CMD`

---

## D15 — Google Calendar API via OAuth2

**Decision:** Use the Google Calendar API (not a static `.ics` file) to fetch the next real upcoming event with a Singapore location.

**Why:**
- A live Google Calendar feed makes the demo genuinely useful — the pipeline recommends a commute for an event that actually exists in your calendar
- The Google Calendar API returns structured JSON: `summary`, `start.dateTime`, `location` — no `.ics` parsing required
- OAuth2 with `InstalledAppFlow` handles the first-run browser consent and caches a `token.json` for subsequent runs (auto-refreshes before expiry)
- `credentials.json` and `token.json` are both gitignored — same security model as `config.py`
- Demonstrates a third-party API OAuth2 integration, which is a stronger DE portfolio piece than a static file

**Implementation:**
- `credentials.json` — downloaded from Google Cloud Console (OAuth 2.0 Desktop app credential)
- `token.json` — written on first run after browser consent, auto-refreshed by `google-auth`
- `fetch_next_calendar_event()` scans the next 10 upcoming events, skips all-day events (no `dateTime`) and events with no location, geocodes the first valid location via OneMap
- `GOOGLE_CALENDAR_ID = "primary"` in `config.py` — change to a specific calendar ID if needed

**Trade-off:** First run opens a browser window for Google consent — not fully headless. In Docker/Airflow, `token.json` must be pre-generated on a machine with a browser and volume-mounted into the container.

---

## D16 — IP Geolocation as Routing Origin (ip-api.com)

**Decision:** Always use `http://ip-api.com/json/` as the routing origin. `HOME_ADDRESS` is never used as origin — it is a destination-only value (go-home default, at-home proximity check).

**Why:**
- The original code hardcoded `ORIGIN_LAT = 1.3521, ORIGIN_LNG = 103.8198` (Bishan) — wrong for anyone not in Bishan
- Device GPS is not accessible from a desktop Python script
- `ip-api.com` is free, requires no API key, and returns city-level coordinates (~1–5 km accuracy)
- In Singapore's compact geography, city-level accuracy puts you within 1–2 MRT stations of your real location — good enough for route ranking
- The function validates returned coordinates are within Singapore bounds — rejects VPN-induced foreign IPs gracefully
- **Why HOME_ADDRESS is excluded from origin:** if a calendar event's destination is the home address (e.g. an event called "Home" with location = HOME_ADDRESS postal code), using HOME_ADDRESS as origin produces origin = destination. OneMap routing returns HTTP 404 for zero-distance routes. IP geolocation is always the user's *current* position, which is never the same as the destination.

**Priority order:** IP geolocation (`ip-api.com`) → Bishan hardcoded fallback (1.3521, 103.8198) if outside SG or call fails

**Trade-off:** IP geolocation accuracy degrades on corporate networks or VPNs (~1–5 km error). For route ranking in Singapore this is acceptable — a 3 km error shifts you by 1–2 MRT stations, not a different part of the island.

---

## D17 — Walk Alternative Suggestion with Fitness API Integration

**Decision:** After the primary transit recommendation, check if walking to the destination is viable (< 5 km, no rain) and if so, display a walk alternative with Zone 1/2 heart rate context and optional fitness data.

**Why:**
- Singapore's urban density means many destinations are walkable — the pipeline would never surface this without an explicit check
- Walking data enriches the recommendation beyond pure transit: health context (steps toward daily goal, calorie burn, heart rate zone) makes the output genuinely useful
- Zone 1 and Zone 2 guidance is medically established and easy to explain in the video demo
- Fitness APIs (Garmin, Whoop) are entirely optional — the walk suggestion appears with or without them, so the feature degrades gracefully

**Fitness API priority:**
- Garmin Connect (`garminconnect` pip package) — today's step count via email/password (unofficial API, may break)
- Whoop developer API — recovery score 0–100% via personal access token from `developer.whoop.com`
- Both optional: `GARMIN_EMAIL = ""` or `WHOOP_ACCESS_TOKEN = ""` in config silently skips that source

**Distance threshold:** 5 km straight-line (Haversine). Reasons:
- At 5 km/h walking pace, 5 km = 60 min — still practical for a morning commute with buffer time
- Straight-line is conservative (actual path is longer) — so 5 km straight = roughly 6–7 km walking route
- Routing distance would require another OneMap API call per run; Haversine is instant

**Trade-off:** Walk suggestion uses `_detect_origin()` (IP geolocation) in transform.py, not the geocoded `HOME_ADDRESS`. This is because transform.py has no OneMap token. Accuracy is city-level — acceptable given the 5 km threshold.

---

## D18 — Batch Scheduling (Airflow) Over Streaming (Kafka)

**Decision:** Use Airflow on a 10-minute cron schedule instead of a Kafka event stream.

**Why:**
- LTA bus arrival data updates every 1–2 minutes. Weather updates every 2 hours. There is no data source in this pipeline that changes faster than once per minute — polling every 10 minutes captures all meaningful updates.
- Kafka is designed for sub-second event volumes (click streams, payments, IoT sensor bursts). Adding Kafka for data that updates every 2 minutes adds broker setup, consumer group management, partition logic, and offset tracking — all overhead with no benefit for this data velocity.
- Airflow gives a web UI for visualising task runs, viewing logs, and seeing failures — a cron job or while-loop has no visibility.
- Airflow is explicitly taught in the course and is a standard DE employer requirement.

**When Kafka would be the right choice:** If we were consuming raw GPS pings from all taxis in real time (thousands per second), or if we needed sub-10-second dashboard updates for a live operations room.

**Trade-off:** The dashboard is stale by up to 10 minutes. For a commute recommendation used to plan departures, this is acceptable. Real-time to the second is not needed here.

---

## D19 — DuckDB Over Spark/PySpark

**Decision:** Use DuckDB as the analytical processing engine instead of Apache Spark.

**Why:**
- **Data volume:** Our largest table is `weather_forecast` at 47 rows per run. `route_options` has 3 rows per event. `bus_arrivals` accumulates a few thousand rows over days. Total dataset size is measured in megabytes, not gigabytes or terabytes.
- **Spark's cost:** Spark requires JVM startup, cluster coordination, and shuffle operations even for local mode. The overhead of Spark on a 50-row dataset would dwarf actual processing time.
- **DuckDB's strengths:** Columnar storage, vectorised execution, full analytical SQL (window functions, INTERVAL arithmetic, TIMESTAMPTZ) — all in a single embedded file with zero infrastructure. For GB-scale analytical workloads on a single machine, DuckDB outperforms Spark.
- **Parquet compatibility:** DuckDB queries Parquet files directly with `FROM 'data/raw/**/*.parquet'` — no Spark, no schema registry, no separate loading step.

**When Spark would be the right choice:** If this pipeline needed to process millions of historical route records daily, or if data was distributed across a cluster, Spark PySpark would be the correct tool.

**Trade-off:** DuckDB has a single-writer lock — only one process can write at a time. This requires careful sequencing of pipeline tasks and means the API and dashboard must open DuckDB in read-only mode.

---

## D20 — RandomForestRegressor for Commute Time Prediction

**Decision:** Use `sklearn.ensemble.RandomForestRegressor` to predict `total_duration_min`.

**Why:**
- **Non-linear interactions:** Rush hour combined with rain produces a longer delay than the sum of rush hour alone and rain alone. Linear Regression cannot model interaction effects without explicit feature engineering. Random Forest handles this natively.
- **Small dataset tolerance:** RF performs well with hundreds of training examples. Linear Regression is also viable but RF provides feature importance scores, which are useful for explaining to assessors which factors matter most.
- **No hyperparameter tuning required at first:** `n_estimators=100, random_state=42` produces stable results without a grid search — appropriate for a portfolio project.
- **scikit-learn ecosystem:** Integrates directly with pandas DataFrames from DuckDB `.df()` queries. No additional data conversion needed.

**Features used:**
- `hour_of_day` (0–23) — captures rush hour patterns
- `day_of_week` (0=Mon, 6=Sun) — captures weekend reduction in traffic
- `is_rainy` (0/1) — rain adds walking time and may affect bus frequency
- `walk_distance_m` — longer walk distance makes total time more variable
- `num_transfers` — more transfers = more variance in journey time

**Cold start solution:** Pipeline may only have a few days of real data at submission time. Bootstrap with ~500 synthetic historical rows generated from known patterns (rush hour +15%, rain +8%, weekend −20%) to allow training from day 1. Mark these with `model_version = "synthetic"` so they can be identified and excluded once enough real data accumulates.

**Trade-off:** The model is only as good as its training data. With limited real history, predictions will have higher error. The `mae_7day` metric shown in the dashboard makes this honest — users can see how accurate the model actually is.

---

## D25 — LTA Bus Arrival API v3 Migration

**Decision:** Use `https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival` as the bus arrival endpoint, not the legacy `BusArrivalv2` path.

**Why:**
- LTA DataMall released API documentation v6.0 in August 2024, which introduced the `v3/BusArrival` endpoint and deprecated `BusArrivalv2`.
- The old `BusArrivalv2` URL now returns HTTP 404 with body "The requested API was not found" for **all** stop codes — not because the stop is wrong, but because the endpoint itself no longer exists on the server.
- This was discovered when every stop (including confirmed active stops like 54719) returned 404. The user provided the official LTA DataMall API PDF v6.8 (April 2026) which confirmed the URL change.

**Related fix — 5-nearest-stop fallback:** Rather than trying a single stop, `ingest_bus_arrivals()` now fetches the 5 nearest stops by Haversine distance from the routing origin and tries each in order. Stops in the 65xxx Punggol/Sengkang range appear in the BusStops database but are absent from the real-time system — the fallback ensures a live stop is found.

**Related fix — float-suffix strip:** Parquet promotes integer BusStopCode columns to float64 when any NaN rows exist, turning "65721" into "65721.0". The LTA API rejects codes with a decimal point. Fix: `str(code).split(".")[0]` on every code before sending to the API.

**Trade-off:** Single-attempt per candidate (no exponential backoff). This is intentional — a 404 on a valid endpoint is a server-side "no data" signal, not a transient error. Retrying would add 3–12 seconds of delay per dead stop (3 attempts × up to 5 stops).

---

## D24 — serve.py Before model.py (ML Workflow Order)

**Decision:** Build `scripts/serve.py` (Streamlit dashboard) before `scripts/model.py` (ML training and prediction). Wire predictions into the dashboard after both exist.

**Why:**
- Fine-tuning is meaningless without a feedback loop. Without a dashboard showing predicted vs actual MAE, you are tuning blind with no way to see if changes helped.
- The dashboard needs to exist before ML outputs can be displayed — build the screen first, then produce data to fill it.
- With limited real data at submission time (1–2 events), RandomForest will be dominated by 500 synthetic bootstrap rows. Hyperparameter tuning on synthetic data has negligible value. Get the end-to-end pipeline working first; meaningful fine-tuning happens naturally as real `actual_min` data accumulates over days.

**Workflow:**
1. `serve.py` — dashboard showing leave-by, route, weather (no ML yet)
2. `model.py --train` → `--predict` → `--evaluate` — model working end-to-end
3. Wire predictions into `serve.py` — dashboard now shows predicted duration + 7-day MAE
4. Fine-tune — adjust features or hyperparameters once MAE is visible and real data exists

**Trade-off:** The dashboard is temporarily incomplete (no ML panel) between steps 1 and 3. This is acceptable — a working pipeline with a partial dashboard is better than no dashboard while waiting for perfect ML.

---

## D22 — Progressive Geocoding Fallback

**Decision:** `geocode()` in `ingest.py` tries three progressively simpler search terms before raising an error: (1) full address string, (2) address with ", Singapore" stripped, (3) first comma-delimited token only.

**Why:**
- Calendar event location fields are free-text — users type "1 Sentul Walk, Singapore", "Raffles Place MRT", "10 Dover Drive #05-01 SIT", or just a postal code. No single format is reliable.
- OneMap's elastic search handles postal codes and landmark names well but fails on obscure street names or verbose unit number formats.
- A single exact-match attempt would silently skip valid events. The fallback gives OneMap three chances with progressively simpler inputs.
- Postal codes (6 digits) are the most reliable input to OneMap — always set `WORK_ADDRESS` and `HOME_ADDRESS` in `config.py` as "Street Name, Singapore XXXXXX".

**Trade-off:** The fallback may geocode to a slightly less precise location (e.g. street-level rather than building-level). For commute routing this is acceptable — a 50m error in the destination has no impact on route selection.

---

## D23 — Time-of-Day Smart Default Destination (`get_smart_default()`)

**Decision:** When no calendar event is found, the pipeline uses a time-of-day heuristic to infer the most likely destination rather than skipping:
- **8–10 AM**: route to `WORK_ADDRESS` (morning commute window)
- **4–6 PM**: route to `HOME_ADDRESS`, depart ~6:30 PM (evening commute window)
- **After 6 PM**: geocode `HOME_ADDRESS`, compare against IP-geolocated current position — if within 3 km, the user is already home and the pipeline exits cleanly with no recommendation
- **Other hours (10 AM–4 PM)**: pipeline skips quietly — no sensible default applies mid-day

**Why:**
- A commute recommender that only works when a Google Calendar event exists is fragile. A student may forget to add a location, or may want a recommendation for a routine commute not in their calendar.
- The time windows match real Singapore commute patterns: most people leave home 8–9 AM, leave work/school 5–7 PM.
- The after-6 PM at-home check avoids sending a "route home" recommendation to someone already sitting at home — it uses the same IP geolocation that drives origin detection.
- Calendar events ALWAYS take priority. `get_smart_default()` only runs when `fetch_next_calendar_event()` raises a `ValueError`.

**3 km threshold for "at home":**
- IP geolocation accuracy in Singapore is ~1–5 km (city-level). A 3 km threshold correctly identifies "in my neighbourhood" without being so large it triggers false positives across Singapore.
- If `HOME_ADDRESS` is not set in `config.py`, the after-4 PM and after-6 PM defaults are silently skipped.

**Trade-off:** The smart default uses a fixed 9 AM start time for the work event and 6:30 PM for the home event. These are approximations. Users who want precision should add a Google Calendar event with an exact time.

---

## D26 — Stale Event Cleanup on Every Ingest Run

**Decision:** After storing the active calendar event, call `_purge_stale_events(con, keep_event_id)` to delete all other future `calendar_events` + their `route_options` + `route_legs` rows from DuckDB.

**Why:**
- `ingest.py` runs every 10 minutes and processes one event per run (the next upcoming event). Over time, DuckDB accumulates one row per unique `event_id` in `calendar_events`.
- If a user reschedules a calendar event, the old entry (with the old `start_time`) stays in the database. Both old and new `start_time` values can be `> NOW()`, so `BEST_ROUTE_QUERY` in transform.py (`ORDER BY start_time LIMIT 1`) may pick the stale entry rather than the freshly-ingested one.
- This was discovered when "Collect Aye Sim card" was moved from 5 PM to 7 PM, while "Home" was moved to 5 PM. The old "Collect Aye Sim card" at 5 PM persisted in the DB. The new "Home" event failed routing (origin = destination bug), so had no route_options. Transform returned the old stale event.
- The purge runs AFTER the new event and its routes are successfully stored, so there is no window where the DB is empty.

**Scope of purge:** only future events (`start_time > NOW()`). Past events are preserved — `recommendations` and `predictions` reference them by `event_id` and must remain consistent.

**Trade-off:** The database can only hold one active future event at a time. If a future feature needs to pre-load several upcoming events (e.g. "show me tomorrow's commute too"), this purge would need to be relaxed to keep multiple future events.

---

## D27 — next_bus2_mins Column in bus_arrivals (Session 5)

**Decision:** Add `next_bus2_mins INTEGER` to `bus_arrivals` to store the ETA of the second upcoming bus in addition to the first.

**Why:**
- `transform.py` displays a live bus arrivals board showing x₁ (first bus) and x₂ (second bus) minutes from now
- Without storing `next_bus2_mins`, the board could only show one arrival — which is less useful if the first bus is about to leave as the user reads the recommendation
- The LTA v3/BusArrival response already provides `NextBus2.EstimatedArrival` — the field was available at no extra API cost
- Enables "next bus in 3 min, one after in 11 min" display, letting users decide whether to rush or wait

**Schema change:** `ALTER TABLE bus_arrivals ADD COLUMN IF NOT EXISTS next_bus2_mins INTEGER` (applied as migration in schema.py).

---

## D28 — num_stops Column in route_legs (Session 5)

**Decision:** Add `num_stops INTEGER` to `route_legs` to store the number of stops between boarding and alighting for each leg.

**Why:**
- The alt routes section in `transform.py` displays compact leg summaries in the format `🚌65 8m/4st` (service, duration, stop count)
- Stop count gives users a sense of distance and how often they need to pay attention to their stop without showing a full map
- `num_stops = len(intermediateStops) + 1` — the +1 counts the alighting stop itself. `WALK` legs store `NULL` since walking doesn't have intermediate stops in the OneMap response
- OneMap's `intermediateStops` array was already available in the route response — zero additional API calls needed

**Schema change:** `ALTER TABLE route_legs ADD COLUMN IF NOT EXISTS num_stops INTEGER` (applied as migration in schema.py).

---

## D29 — MLflow for Experiment Tracking (Day 4)

**Decision:** Integrate MLflow into `scripts/model.py` to track each training run with parameters, metrics, and the saved model artifact. Use MLflow's model registry and `@champion` alias for serving.

**Why:**
- Without experiment tracking, `--train` overwrites `models/commute_predictor.pkl` silently — there is no record of which parameters were used, what the validation MAE was, or which training date produced which model
- `pipeline_runs` table logs pipeline events, not model experiments. These are different concerns: `pipeline_runs` tells you "the ingest ran at 08:10, 47 rows, success". MLflow tells you "training run #7 used n_estimators=100, mae=4.2 min, trained on 520 historical rows"
- MLflow's local tracking server runs as a single process (`mlflow ui`) and stores runs in `mlruns/` — zero infrastructure overhead for a local project
- The model registry enables staging → production → archive transitions with rollback, which pairs with the shadow/canary deployment pattern (D31)
- Demonstrates "MLOps" vocabulary the rubric rewards

**Full MLflow API pattern (Day 4):**
```python
import mlflow

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("commute_prediction")

with mlflow.start_run(run_name="rf_v1"):
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("feature_count", 5)
    mlflow.log_param("training_rows", len(df))
    mlflow.log_metric("mae_validation", mae)
    mlflow.log_metric("rmse", rmse)
    mlflow.sklearn.log_model(model, "commute_predictor")

# Register for serving:
mlflow.register_model("runs:/<run_id>/commute_predictor", "commute_predictor")
# Set alias: client.set_registered_model_alias("commute_predictor", "champion", version)

# Load model for inference (from registry with @champion alias):
model = mlflow.pyfunc.load_model("models:/commute_predictor@champion")
```

**Local MLflow server (Day 4 Docker image):** `ghcr.io/mlflow/mlflow:v3.13.0`

**Model Registry lifecycle:**
- `Staging` — newly trained, not yet serving. Shadow-tested against production.
- `Production` / `@champion` — current model serving predictions. Alias `@champion` decouples version number from serving code.
- `Archived` — retired models preserved for rollback.

**Two logging concerns:** System logs track request latency, endpoint, status code. Model logs track model URI, feature version, input features, prediction value. Both needed for full monitoring.

---

## D30 — Drift Detection via MAE Threshold (Day 4)

**Decision:** Treat rising 7-day MAE as the primary signal for model drift, rather than statistical feature distribution tests. Distinguish three types of degradation: data drift, concept drift, and training-serving skew.

**Three types of ML degradation (Day 4 taxonomy):**

| Type | Definition | Example in this project |
|---|---|---|
| **Data Drift** | Input feature distributions change after deployment | User changes jobs: `hour_of_day` distribution shifts from 8 AM to 10 AM starts |
| **Concept Drift** | Relationship between features and target changes | Bus network restructuring: same `hour_of_day` → different `total_duration_min` mapping |
| **Training-Serving Skew** | Feature computation differs between training and production | `is_rainy` encoded as `True/False` in training but `1/0` in production, or a weather API format change |

**Why MAE threshold over distribution tests:**
- Distribution-based tests (KL divergence, PSI) require enough recent production data — at submission time the pipeline may only have days of real data
- MAE is already computed by `evaluate_model` (daily 8 AM Airflow task) and stored in `predictions.mae_7day` — drift detection requires no new infrastructure
- A rising MAE captures the *effect* of all three drift types regardless of root cause — a practical primary signal
- If MAE-based detection is insufficient (feature distributions shift but accuracy hasn't degraded yet), PSI or KS-test can be added per-feature later

**Implementation:** In `evaluate_model()`, compare `mae_7day` against a configurable threshold (default 10 min). If exceeded, log `logging.warning("Model drift detected: MAE %s > threshold 10")` and optionally trigger a retraining pipeline_run entry so the Airflow DAG can pick it up.

**Before retraining, diagnose root cause:** data problem (stale Parquet)? Feature problem (API format change)? Temporary event (public holiday spike)? Real-world change (new MRT line)? Only the last case requires model retraining — the others require fixing the data or feature pipeline.

---

## D31 — Shadow Deployment Before Canary (Day 4)

**Decision:** Use a two-phase promotion pattern for new model versions: **shadow** first, then **canary**, before full promotion.

**Shadow deployment:**
- The new (challenger) model receives the same live prediction requests as the production model
- Its predictions are logged to a shadow table but **never returned to users** — users always see the production model's output
- Purpose: validate that the challenger produces sensible predictions on real production inputs before any user risk
- Zero user-facing impact — ideal for testing an untested model version

**Canary deployment:**
- A small fraction of real user requests is routed to the challenger; the rest continue to production
- Traffic split: 95%/5% → 50%/50% → 100%/0% — stepped over days or pipeline cycles as confidence grows
- Warning signals that should pause promotion: API error rate increase, prediction latency spike, prediction distribution shift, delayed MAE degradation
- Only reached after shadow deployment confirms the challenger is stable

**Why this order matters:** Shadow catches crashes and format errors safely. Canary catches accuracy regressions on a small user slice before they affect everyone. Full promote only after both gates pass.

**Monitoring tools (Day 4):** Prometheus scrapes metrics from the FastAPI `/metrics` endpoint (request count, prediction latency, error rate). Grafana dashboards visualise these metrics alongside 7-day MAE from the `predictions` table. Together they cover system health (Prometheus/Grafana) and model health (MAE tracking).

**For this project's scope:** Shadow testing is simulated by running `model.py --predict` with both production and staging model URIs and comparing results in the `predictions` table. Full traffic routing infrastructure (load balancer, feature flags) is out of scope for the submission.

---

## D32 — Dynamic `recommendation_reason` in `v_enriched_routes` View (Session 6)

**Decision:** Expand the `recommendation_reason` CASE WHEN in `v_enriched_routes` from 4 static labels to 9 dynamic labels computed using DuckDB window functions.

**Why:**
- The original view produced only 4 labels: "⚠ Rainy", "⚠ MRT disruption", "✓ Fastest option", "Alternative route"
- "✓ Fastest option" was hardcoded regardless of whether the route was also direct (no transfers), cheapest, or least walking — so the displayed reason was often imprecise or misleading
- DuckDB window functions (`MIN() OVER (PARTITION BY event_id)`) allow each route row to compare itself against all sibling routes for the same event — enabling truly dynamic labels without Python logic

**9 labels (priority order):**
1. `⚠ Rain — Xm exposed walk` — rainy + walk > 400m
2. `⚠ Service disruption — check alternatives` — active heavy alert
3. `✓ Fastest + direct (no transfers)` — fastest AND zero transfers
4. `✓ Direct — no transfers` — zero transfers (not necessarily fastest)
5. `✓ Fastest + fewest transfers` — fastest AND fewest transfers
6. `✓ Fastest (X min)` — fastest duration
7. `✓ Fewest transfers (N)` — fewest transfers
8. `✓ Least walking (Xm)` — least walk distance
9. `✓ Cheapest fare` / `✓ Best overall` — fallbacks

**Trade-off:** The CASE evaluates each condition in order, so only the highest-priority matching label is shown. A route that is both cheapest and fastest will show "✓ Fastest" not "✓ Cheapest" — this is intentional (time > money for commuters).

---

## D33 — Inline First-Transit Live Arrival (Replacing Separate Bus Board) (Session 6)

**Decision:** Remove the standalone "Live arrivals — Stop XXXXX" bus board section and instead embed first-transit live arrival directly under the recommended route's step-by-step legs. Alt routes show X1 only as a collapsed note.

**Why:**
- The separate board showed every bus service at the origin stop — useful for browsing, but noisy when you already have a route recommendation. What the commuter needs to know is: when does the specific vehicle I'm boarding arrive?
- Embedding the live arrival immediately below the legs maintains the recommendation's context ("you take the NE Line — here's when the next train comes")
- Two arrivals (X1+X2) for the recommended route let users decide whether to rush out the door for the first bus or wait for the next one 8 minutes later
- Alt routes show X1 only (collapsed) in the notes line — enough to compare without clutter

**Display format (updated session 7 — actual clock times instead of relative minutes):**
- Recommended route (bus): `🚌  Bus 65     : next at 09:34 (~4 min from now) | if missed → 09:39 (~9 min from now)  seats`
- Recommended route (MRT): `🚇  Northeast Line        : next at 09:34 (~4 min from now) | if missed → 09:38 (~8 min from now)`
- Recommended route (LRT): `🚈  Sengkang LRT         : next at 09:37 (~7 min from now) | if missed → 09:44 (~14 min from now)`
- Alt routes (bus): `🚌Bus 65: 09:34 (~4m)` or `🚌Bus 65: 09:34 (~12m) ⚠`
- Alt routes (MRT): `🚇East West Line: ~09:34 (~4m)`

**Clock time derivation:** `now_sgt = datetime.now(timezone.utc).astimezone(SGT)`. X1/X2 clock times computed as `(now_sgt + timedelta(minutes=x)).strftime("%H:%M")`. SGT = UTC+8.

**MRT/LRT limitation:** Singapore's MRT and LRT have no public real-time arrival API. The system uses fixed headway estimates: MRT x1=4 min, x2=8 min; LRT x1=7 min, x2=14 min. These are midpoints of the typical headway ranges, not live data.

---

## D34 — Dynamic Disruption Filtering to Route's Actual Rail Lines (Session 6)

**Decision:** Filter `train_alerts` to only those whose `affected_line` matches a `service_no` present in the current route's legs before displaying the disruption status label.

**Why:**
- The old code showed "No active MRT/LRT disruptions" even on routes with no MRT or LRT legs at all — the label was meaningless for a bus-only route
- And the label hardcoded "MRT/LRT" rather than reflecting what the route actually uses — a pure MRT route should say "No active MRT disruptions", not "No active MRT/LRT disruptions"
- The fix: after fetching leg data, build `route_rail_lines = {leg.service_no for leg in legs if leg.mode in ("MRT", "LRT")}`, then `relevant_alerts = [a for a in alerts if a.affected_line in route_rail_lines]`

**Dynamic label logic:**
- Route has both MRT and LRT legs → rail_label = "MRT/LRT"
- Route has only MRT legs → rail_label = "MRT"
- Route has only LRT legs → rail_label = "LRT"
- Route has no rail legs → skip disruption section entirely

**Alt routes:** The same filtering is applied per-alt. Each alt route's notes line shows its own disruption status (e.g., "✅ No MRT disruption") rather than a global system-wide status.

---

## D35 — Walk-Only Route Inline Metrics (Session 7)

**Decision:** When all legs in the recommended route have `mode == "WALK"`, display walk metrics (distance, time, steps, calories, heart-rate zones, Garmin, Whoop) inline immediately after the step-by-step legs. Skip the separate `print_walk_suggestion()` call at the bottom of the output.

**Why:**
- The original "WALK ALTERNATIVE" section appeared below the alt routes block. When walking IS the recommendation, showing it there implied there was a transit option above it — confusing and redundant.
- Folding the walk details directly into the recommended route section makes the output coherent: "your recommendation is to walk, here are the details."
- The 5km distance guard and `is_rainy` guard in `print_walk_suggestion()` deliberately do NOT apply to the inline case — if the route is walk-only, metrics must always be shown regardless of distance or weather.

**Implementation:** `is_walk_only = bool(legs) and all(l["mode"] == "WALK" for l in legs)`. If True, call `_walk_metrics(origin_lat, origin_lng, dest_lat, dest_lng)` inline after legs. Guard `print_walk_suggestion()` with `if not is_walk_only`. Helper `_walk_metrics()` extracted from the original `print_walk_suggestion()` body so both paths share the same logging logic.

---

## D36 — Postal Code Extraction in geocode() (Session 7)

**Decision:** Use `re.findall(r'\b\d{6}\b', address)` to detect a 6-digit Singapore postal code in the calendar location string and prepend it as the highest-priority geocoding candidate before all existing fallbacks.

**Why:**
- OneMap's search API returns accurate, consistent results for 6-digit postal codes — far more reliably than free-text address strings.
- Calendar event locations typed by users vary wildly: "Bishan CC", "51 Bishan St 13 S579799", "Singapore 579799", "NUS UTown". Postal codes are the stable, machine-readable part.
- The existing progressive fallback (full address → strip ", Singapore" → first comma token) sometimes fails for obscure or abbreviated street names (e.g. "Sentul Walk"). A postal code check never fails on OneMap.

**Detection:** `re.findall(r'\b\d{6}\b', address)` — word boundary `\b` ensures 7-digit or longer strings (phone numbers, IDs) are not matched. `address.lower()` used for case-insensitive "singapore" keyword check. Both "singapore present + 6 digits" and "no singapore word + 6 digits" paths extract and prepend the same way — the keyword check adds no differentiation in practice.

**Candidate list after change (example):** `["579799", "Bishan CC, Singapore 579799", "Bishan CC, 579799", "Bishan CC"]`

---

## D37 — Location-Change Detection Log in ingest.main() (Session 7)

**Decision:** Before the ingest loop in `main()`, query the currently stored `dest_lat`/`dest_lng` for the active `event_id` from `calendar_events` and compare against the newly geocoded values. If the Haversine shift exceeds 50m, log `📍 Destination updated (NNN m shift) — will re-fetch routes`.

**Why:**
- Routes are always re-fetched on every `ingest.py` run (`ingest_routes()` unconditionally deletes old routes and calls OneMap fresh). There is no stale-route bug for the standard "edit calendar location" case.
- However, there was no visible signal that the system had detected and acted on a location change. Users correcting a wrong calendar address had no confirmation the fix was picked up.
- The log line is purely diagnostic — no behaviour change. It closes the feedback loop: user edits calendar → re-runs ingest → sees "📍 Destination updated" in the log → confident the correction propagated.

**Threshold:** 50m Haversine shift chosen to avoid false positives from IP-geolocation variance while still catching any real address correction.

---

## D21 — Star Schema for DuckDB Tables

**Decision:** Design DuckDB tables as an informal star schema with `route_options` as the fact table.

**Why:**
- A star schema separates measurable facts from descriptive dimensions, making queries faster and more readable.
- `route_options` is the natural fact table: each row records one measurable route option (duration, fare, walk distance, transfers) linked to a specific event.
- `weather_forecast`, `bus_arrivals`, and `train_alerts` are dimension tables: they describe context that routes are joined against.
- The `v_enriched_routes` view is the "mart" — the final joined, enriched, ranked table that answers business questions directly.
- Star schema terminology also helps in the video — "my DuckDB schema is a star schema" demonstrates Day 3 data modelling knowledge.

**SCD Type:** All dimension tables use SCD Type 1 (INSERT OR REPLACE overwrites the previous value). The 8am weather reading is replaced by the 10am reading — no history of past forecasts is kept in the DuckDB tables (raw history is preserved in the Parquet raw zone instead).

**Trade-off:** SCD Type 2 (keep full history with valid_from/valid_to dates) would allow historical trend analysis. For this project scope, SCD Type 1 simplicity is the right trade-off. The Parquet raw zone serves as the historical record if needed.

---

## D38 — Technology Selection for GPU, Triton, and BI (Day 5 lens)

**Decision:** Do NOT add GPU acceleration (cuDF/cuML), NVIDIA Triton Inference Server, or Metabase to this project.

**Why — using the Day 5 Technology Selection Exercise table directly:**

| Day 5 scenario | Recommended tool | Does this project fit? |
|---|---|---|
| 100MB daily CSV, one dashboard | pandas + Parquet | ✅ YES — we process ~50 rows per pipeline run |
| Low traffic prediction API | FastAPI | ✅ YES — one user, one commute event at a time |
| High-throughput inference (1000s req/s) | Triton Inference Server | ❌ NO — we score 3 route options per pipeline run |
| Many ML experiments are slow (training bottleneck) | cuML / GPU XGBoost | ❌ NO — 500-row bootstrap trains in <1s on CPU |

**GPU acceleration (cuDF, cuML, Dask-cuDF, CuPy):** RAPIDS speeds up data operations on NVIDIA GPUs. Our dataset is: 3 route options, 47 weather rows, 5,205 bus stop lookups. This is megabytes, not gigabytes. The pipeline is I/O-bound (API latency, ~2s per LTA call), not compute-bound. CPU is idle while waiting for LTA responses. Adding cuDF would increase system requirements without any measurable performance gain.

**Triton Inference Server:** Designed for high-throughput GPU inference with dynamic batching, multiple model formats (TensorRT, ONNX, TorchScript), and Prometheus metrics. Our inference workload is 3 predictions per pipeline run — `model.predict()` in Python takes <1ms on CPU. Triton's deployment overhead (GPU server, model repository, gRPC client) would dwarf the problem it solves.

**Metabase:** A BI tool for non-technical users to build dashboards with SQL queries. Streamlit already serves our dashboard function and is more flexible (Python code, not drag-and-drop SQL). Adding Metabase would require a separate Docker service, a PostgreSQL metadata store, and a second DuckDB connection — significant overhead for a single-user project.

**Feature Store:** Useful at scale for sharing engineered features across multiple models and teams. With one model and one engineer, features are computed inline in the SQL query. A Feature Store adds indirection without value at this project's size.

**Trade-off accepted:** If the project scaled to a multi-user commute recommendation service (thousands of concurrent users, multiple city deployments), the choices would change: Triton for high-throughput batched inference, Dask-cuDF for GPU-accelerated batch processing, Metabase for analyst dashboards. Knowing when to scale up — and when not to — is the skill the Day 5 exercise teaches.
