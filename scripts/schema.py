import logging
from pathlib import Path
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "db" / "commute.duckdb"

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS calendar_events (
        event_id     VARCHAR PRIMARY KEY,
        title        VARCHAR,
        start_time   TIMESTAMPTZ,
        location_raw VARCHAR,
        dest_lat     DOUBLE,
        dest_lng     DOUBLE,
        ingested_at  TIMESTAMP DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS route_options (
        option_id          VARCHAR PRIMARY KEY,
        event_id           VARCHAR REFERENCES calendar_events(event_id),
        total_duration_min INTEGER,
        walk_distance_m    INTEGER,
        num_transfers      INTEGER,
        fare               DECIMAL(4,2),
        fetched_at         TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS weather_forecast (
        area        VARCHAR,
        forecast    VARCHAR,
        is_rainy    BOOLEAN,
        valid_start TIMESTAMPTZ,
        valid_end   TIMESTAMPTZ,
        fetched_at  TIMESTAMP,
        PRIMARY KEY (area, valid_start)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bus_arrivals (
        bus_stop_code VARCHAR,
        service_no    VARCHAR,
        next_bus_mins INTEGER,
        load          VARCHAR,
        fetched_at    TIMESTAMP,
        PRIMARY KEY (bus_stop_code, service_no, fetched_at)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS train_alerts (
        alert_id      VARCHAR PRIMARY KEY,
        affected_line VARCHAR,
        message       VARCHAR,
        severity      VARCHAR,
        fetched_at    TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recommendations (
        event_id           VARCHAR PRIMARY KEY,
        recommended_mode   VARCHAR,
        total_duration_min INTEGER,
        leave_by           TIMESTAMPTZ,
        estimated_arrival  TIMESTAMPTZ,
        weather_warning    VARCHAR,
        disruption_warning VARCHAR,
        reason             VARCHAR,
        created_at         TIMESTAMP DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id        VARCHAR PRIMARY KEY,
        source        VARCHAR,
        rows_upserted INTEGER,
        duration_ms   INTEGER,
        status        VARCHAR,
        error_msg     VARCHAR,
        ran_at        TIMESTAMP DEFAULT now()
    )
    """,
]

VIEW = """
CREATE OR REPLACE VIEW v_enriched_routes AS
SELECT
    r.option_id,
    r.event_id,
    r.total_duration_min,
    r.walk_distance_m,
    r.num_transfers,
    r.fare,
    e.start_time,
    e.title,
    e.dest_lat,
    e.dest_lng,
    e.start_time - INTERVAL (r.total_duration_min + 10) MINUTE AS leave_by,
    w.forecast AS weather_forecast,
    w.is_rainy,
    CASE WHEN ta.alert_id IS NOT NULL THEN ta.message ELSE NULL END AS alert_msg,
    CASE
        WHEN w.is_rainy AND r.walk_distance_m > 400
            THEN '⚠ Rainy — take covered transport'
        WHEN ta.alert_id IS NOT NULL
            THEN '⚠ MRT disruption — add 20 min buffer'
        WHEN r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest option'
        ELSE 'Alternative route'
    END AS recommendation_reason,
    ROW_NUMBER() OVER (
        PARTITION BY r.event_id
        ORDER BY
            CASE WHEN w.is_rainy AND r.walk_distance_m > 400 THEN 1 ELSE 0 END,
            r.total_duration_min
    ) AS route_rank
FROM route_options r
JOIN calendar_events e ON r.event_id = e.event_id
LEFT JOIN weather_forecast w
    ON w.fetched_at = (SELECT MAX(fetched_at) FROM weather_forecast)
LEFT JOIN train_alerts ta
    ON ta.severity = 'HEAVY'
   AND ta.fetched_at > NOW() - INTERVAL '30 minutes'
"""


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        for sql in TABLES:
            con.execute(sql)
        log.info("7 tables created")
        con.execute(VIEW)
        log.info("v_enriched_routes view created")
    finally:
        con.close()
    log.info("Schema ready — %s", DB_PATH)


if __name__ == "__main__":
    main()
