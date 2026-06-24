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

**Decision:** Detect the user's current location via `http://ip-api.com/json/` when `HOME_ADDRESS` is not set in `config.py`. Use this as the routing origin passed to OneMap.

**Why:**
- The original code hardcoded `ORIGIN_LAT = 1.3521, ORIGIN_LNG = 103.8198` (Bishan) — wrong for anyone not in Bishan
- Device GPS is not accessible from a desktop Python script
- `ip-api.com` is free, requires no API key, and returns city-level coordinates (~1–5 km accuracy)
- In Singapore's compact geography, city-level accuracy puts you within 1–2 MRT stations of your real location — good enough for route ranking
- The function validates returned coordinates are within Singapore bounds — rejects VPN-induced foreign IPs gracefully

**Priority order:** `HOME_ADDRESS` in config (geocoded via OneMap, address-level precision) → IP geolocation → Bishan hardcoded fallback

**Trade-off:** IP geolocation accuracy degrades on corporate networks or VPNs. Users who want precision must set `HOME_ADDRESS` in `config.py`.

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
