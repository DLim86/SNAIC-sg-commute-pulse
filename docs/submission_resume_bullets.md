# Submission — Résumé Bullets

**Project:** SG Commute Pulse — calendar-aware Singapore commute recommender
**Author:** DLim86 · SIT Data Engineering, Week 2 Assessment

These five bullets are the content for the submitted PDF. Every claim below was
verified against running code, not documentation.

---

**1 — Problem and outcome**

Built a calendar-aware commute recommender that answers a single question — *when do I
need to leave?* — by joining a user's next Google Calendar event to live Singapore
transport and weather data, replacing a three-app manual check with one ranked
leave-by recommendation delivered through a live dashboard and a REST API.

**2 — End-to-end data pipeline**

Engineered a five-source ingestion pipeline (Google Calendar via OAuth2, OneMap
geocoding and routing, LTA DataMall real-time bus arrivals and rail disruption alerts,
data.gov.sg weather, IP geolocation) that lands date-partitioned Parquet and loads a
typed nine-table DuckDB store, then ranks three route options and derives the leave-by
time in a SQL view using `ROW_NUMBER()` and `MIN() OVER (PARTITION BY …)` window
functions with a weather-based ranking penalty.

**3 — Production robustness and containerisation**

Hardened the pipeline with exponential-backoff retries (3 attempts, 1/2/4 s), idempotent
upserts proven by re-running ingestion and confirming unchanged row counts, Singapore
coordinate-bounds validation, graceful degradation from expired OAuth credentials to a
time-of-day fallback destination, and a `pipeline_runs` monitoring table capturing
status, row counts, duration and errors per source; containerised the runtime as three
Docker Compose services over a shared named-volume DuckDB, with all API keys and OAuth
tokens excluded from the image and mounted at runtime (verified by inspecting the built
image filesystem).

**4 — Machine learning and real-time output**

Diagnosed target-proxy dominance in an initial duration model — the routing engine's
own estimate carried 0.875 feature importance because the training target was itself
largely derived from that baseline, so the model re-derived the schedule rather than
learning contextual delay — and redesigned it to predict the *residual delay* over that
baseline instead of the total journey, cutting that feature's importance to 0.041 and
removing a hard prediction ceiling; engineered `bus_headway_gap`
(`next_bus2_mins − next_bus_mins`) as a crowd-buildup signal feeding both a
`RandomForestRegressor` for delay and a `RandomForestClassifier` for bus crowding,
scoring all three route options individually and refreshing a Streamlit dashboard and
FastAPI endpoints every 60 seconds.

**5 — Measurable evaluation and monitoring**

Implemented a once-daily automated MLOps evaluation that computes 7-day mean absolute
error against a reconstructed outcome estimate and against a no-ML schedule baseline —
so the model's added value is measured rather than assumed — writes the result to the
monitoring table, and raises a drift warning on the dashboard when error exceeds a
10-minute threshold; each stored outcome records the reconstruction method used, keeping
proxy values explicitly distinct from observed measurements.

---

## Notes for the PDF (not for inclusion)

- Bullet 4 is the strongest differentiator: it shows a defect found by measurement, a
  redesign, and a measured improvement. Keep the two numbers.
- `bus_headway_gap` is the one genuinely original feature — keep it named.
- Nothing here claims GPS ground truth, a formal star schema, replayable raw archives,
  or that one command starts Airflow. Those claims were removed after verification.
- If a bullet must be cut for length, cut bullet 1 and open with bullet 2.
