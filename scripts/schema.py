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
        bus_stop_code  VARCHAR,
        service_no     VARCHAR,
        next_bus_mins  INTEGER,
        next_bus2_mins INTEGER,
        load           VARCHAR,
        fetched_at     TIMESTAMP,
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
    """
    CREATE TABLE IF NOT EXISTS route_legs (
        leg_id       VARCHAR PRIMARY KEY,
        option_id    VARCHAR REFERENCES route_options(option_id),
        leg_sequence INTEGER,
        mode         VARCHAR,
        service_no   VARCHAR,
        from_name    VARCHAR,
        to_name      VARCHAR,
        duration_min INTEGER,
        distance_m   INTEGER,
        num_stops    INTEGER,
        fetched_at   TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS predictions (
        prediction_id VARCHAR PRIMARY KEY,
        event_id      VARCHAR,
        predicted_min INTEGER,
        actual_min    INTEGER,
        model_version VARCHAR,
        mae_7day      DOUBLE,
        predicted_at  TIMESTAMP DEFAULT now()
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
            THEN '⚠ Rain — ' || CAST(r.walk_distance_m AS VARCHAR) || 'm exposed walk'
        WHEN ta.alert_id IS NOT NULL
            THEN '⚠ Service disruption — check alternatives'
        WHEN r.num_transfers = 0
             AND r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest + direct (no transfers)'
        WHEN r.num_transfers = 0
            THEN '✓ Direct — no transfers'
        WHEN r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
             AND r.num_transfers = MIN(r.num_transfers) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest + fewest transfers'
        WHEN r.total_duration_min = MIN(r.total_duration_min) OVER (PARTITION BY r.event_id)
            THEN '✓ Fastest (' || CAST(r.total_duration_min AS VARCHAR) || ' min)'
        WHEN r.num_transfers = MIN(r.num_transfers) OVER (PARTITION BY r.event_id)
            THEN '✓ Fewest transfers (' || CAST(r.num_transfers AS VARCHAR) || ')'
        WHEN r.walk_distance_m = MIN(r.walk_distance_m) OVER (PARTITION BY r.event_id)
            THEN '✓ Least walking (' || CAST(r.walk_distance_m AS VARCHAR) || 'm)'
        WHEN r.fare > 0
             AND r.fare = MIN(CASE WHEN r.fare > 0 THEN r.fare END) OVER (PARTITION BY r.event_id)
            THEN '✓ Cheapest fare'
        ELSE '✓ Best overall'
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


MIGRATIONS = [
    "ALTER TABLE bus_arrivals ADD COLUMN IF NOT EXISTS next_bus2_mins INTEGER",
    "ALTER TABLE route_legs ADD COLUMN IF NOT EXISTS num_stops INTEGER",
    "ALTER TABLE predictions ADD COLUMN IF NOT EXISTS predicted_crowd VARCHAR",
    "ALTER TABLE predictions ADD COLUMN IF NOT EXISTS actual_crowd VARCHAR",
]


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        for sql in TABLES:
            con.execute(sql)
        log.info("9 tables created")
        con.execute(VIEW)
        log.info("v_enriched_routes view created")
        for sql in MIGRATIONS:
            con.execute(sql)
        log.info("migrations applied")
    finally:
        con.close()
    log.info("Schema ready — %s", DB_PATH)


if __name__ == "__main__":
    main()
