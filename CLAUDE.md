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
6. All API keys in documentation must use placeholders: `<LTA_API_KEY>`, `<ONEMAP_TOKEN>`, `<ONEMAP_EMAIL>`, `<ONEMAP_PASSWORD>`

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

## Current State (as of June 2026)

### What exists
| File / Folder | Status |
|---|---|
| `.gitignore` | Complete |
| `config_example.py` | Complete — template only, no real keys |
| `requirements.txt` | Complete |
| `README.md` | Complete |
| `docs/roadmap.html` | Complete — interactive 12-station roadmap |
| `data/processed/.gitkeep` | Placeholder only |
| `db/.gitkeep` | Placeholder only |

### What is EMPTY (needs to be written)
| File | Purpose |
|---|---|
| `scripts/schema.py` | Create DuckDB tables — write this FIRST |
| `scripts/ingest.py` | Fetch all 4 APIs + retry/backoff + upsert |
| `scripts/transform.py` | Run SQL transformation, write to recommendations |
| `scripts/serve.py` | Streamlit dashboard |
| `scripts/api.py` | FastAPI serving layer |
| `dags/commute_pipeline_dag.py` | Airflow DAG |
| `docker-compose.yml` | Containerise the full stack |
| `Dockerfile` | Shared image for all 3 Docker services |

---

## Next Tasks — Build in This Order

1. **`scripts/schema.py`** — Create all 7 DuckDB tables. Run once. (`python scripts/schema.py`)
2. **`scripts/ingest.py`** — Fetch APIs with `fetch_with_retry()`, validate SG coordinates, upsert to DuckDB
3. **`scripts/transform.py`** — Execute `v_enriched_routes` view SQL, write ranked recommendations
4. **`scripts/serve.py`** — Streamlit dashboard reading from `v_enriched_routes`
5. **`scripts/api.py`** — FastAPI: `/health`, `/api/v1/recommendation/{event_id}`, `/api/v1/pipeline/status`
6. **`dags/commute_pipeline_dag.py`** — Airflow DAG with 5 tasks, `schedule="*/10 * * * *"`
7. **`docker-compose.yml` + `Dockerfile`** — 3 services: pipeline, api, dashboard

---

## APIs and Credentials

Real credentials are stored locally in gitignored files — **never commit them**.

Use `config.py` (gitignored) for all real secrets. Template is in `config_example.py`.

| API | Auth method | Key location |
|---|---|---|
| LTA DataMall | Header: `AccountKey: <LTA_API_KEY>` | `config.py` → `LTA_API_KEY` |
| OneMap | JWT from POST `/api/auth/post/getToken` | `config.py` → `ONEMAP_EMAIL`, `ONEMAP_PASSWORD` |
| data.gov.sg | None — open API | — |

OneMap token expires every 3 days — always call `get_onemap_token()` fresh per pipeline run.

---

## DuckDB Schema — `db/commute.duckdb`

Seven tables (sixth schema.py adds a pipeline_runs monitoring table):

```
calendar_events   — event_id PK, title, start_time TIMESTAMPTZ, dest_lat, dest_lng
route_options     — option_id PK, event_id FK, total_duration_min, walk_distance_m, fare
weather_forecast  — (area, valid_start) PK, forecast, is_rainy BOOLEAN, fetched_at
bus_arrivals      — (bus_stop_code, service_no, fetched_at) PK, next_bus_mins, load
train_alerts      — alert_id PK, affected_line, message, severity, fetched_at
recommendations   — event_id PK, leave_by TIMESTAMPTZ, reason, created_at
pipeline_runs     — run_id PK, source, rows_upserted, status, error_msg, ran_at
```

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
- **LTA bus stops vs GPS:** LTA bus stop codes don't have GPS coordinates in the BusArrivalv2 endpoint — use Haversine distance against a bus stop list to find the nearest stop
- **data.gov.sg weather areas:** the 2-hour forecast returns areas like "Ang Mo Kio", "Bedok" — not GPS. Map the destination coordinates to the nearest area by name lookup
- **DuckDB write lock:** only one connection can write at a time; the pipeline must close its connection before FastAPI opens one
- **`scripts/` folder has no `__init__.py`:** add one if importing between scripts (e.g., Airflow DAG imports from `scripts.ingest`)

---

## Course Context

- Day 1 content: pipeline fundamentals, DuckDB, SQL transformation, Streamlit — **DONE (design phase)**
- Day 2 content: retry/backoff, Parquet, FastAPI, Airflow, Docker — **IN PROGRESS**
- Day 3–5: not yet released — update roadmap after each class at `docs/roadmap.html`
