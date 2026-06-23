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

## D15 — No `.ics` File Parsing in MVP

**Decision:** MVP uses a hardcoded test event (`event_id = "EVT_TEST_001"`) rather than parsing a real `.ics` file.

**Why:**
- The core value of the project is the pipeline, transformation, and serving — not the calendar parsing
- `.ics` parsing with `icalendar` is straightforward but adds a dependency on having a real calendar file available
- A hardcoded test event lets the pipeline be tested immediately with any LTA/OneMap credentials
- The `icalendar` library is in `requirements.txt` and the `calendar_events` table schema supports it — the upgrade path is clear

**Upgrade path:** Replace the hardcoded `event` dict in `ingest.py` with `icalendar.Calendar.from_ical(open("calendar.ics").read())` and iterate over `VEVENT` components.
