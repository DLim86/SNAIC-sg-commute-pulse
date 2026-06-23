import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "db" / "commute.duckdb"

MODE_ICON = {"WALK": "🚶", "BUS": "🚌", "MRT": "🚇", "LRT": "🚈"}

MRT_LINE_NAMES = {
    "EW": "East West Line", "NS": "North South Line", "CC": "Circle Line",
    "DT": "Downtown Line",  "TE": "Thomson-East Coast Line",
    "BP": "Bukit Panjang LRT", "SE": "Sengkang LRT", "PE": "Punggol LRT",
}

BEST_ROUTE_QUERY = """
SELECT
    event_id, title, start_time, leave_by,
    total_duration_min, walk_distance_m, num_transfers, fare,
    weather_forecast, is_rainy, alert_msg, recommendation_reason
FROM v_enriched_routes
WHERE route_rank = 1
"""

LEGS_QUERY = """
SELECT leg_sequence, mode, service_no, from_name, to_name, duration_min, distance_m
FROM route_legs
WHERE option_id = ?
ORDER BY leg_sequence
"""

BUS_WAIT_QUERY = """
SELECT next_bus_mins, load
FROM bus_arrivals
WHERE service_no = ?
ORDER BY fetched_at DESC
LIMIT 1
"""

ACTIVE_ALERTS_QUERY = """
SELECT affected_line, message
FROM train_alerts
WHERE severity = 'HEAVY'
  AND fetched_at > NOW() - INTERVAL '30 minutes'
ORDER BY fetched_at DESC
LIMIT 3
"""


def log_run(con, rows, duration_ms, status, error_msg=None):
    con.execute(
        """INSERT OR REPLACE INTO pipeline_runs
           (run_id, source, rows_upserted, duration_ms, status, error_msg)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [str(uuid.uuid4()), "transform", rows, duration_ms, status, error_msg],
    )


def format_time(dt):
    if dt is None:
        return "—"
    return dt.strftime("%I:%M %p").lstrip("0")


def main():
    log.info("=== Transform starting ===")
    t0 = time.time()
    con = duckdb.connect(str(DB_PATH))

    try:
        cursor = con.execute(BEST_ROUTE_QUERY)
        cols = [d[0] for d in cursor.description]
        results = cursor.fetchall()

        if not results:
            log.warning("v_enriched_routes returned no rows — run ingest.py first")
            log_run(con, 0, int((time.time() - t0) * 1000), "error", "no view rows")
            return

        now_utc = datetime.now(timezone.utc)
        written = 0

        for row in results:
            r = dict(zip(cols, row))
            option_id = f"{r['event_id']}_ROUTE_1"

            # ── Legs ──────────────────────────────────────────────────────────
            legs = con.execute(LEGS_QUERY, [option_id]).fetchall()
            leg_cols = ["leg_sequence", "mode", "service_no", "from_name",
                        "to_name", "duration_min", "distance_m"]
            legs = [dict(zip(leg_cols, l)) for l in legs]

            # ── Bus waiting time (first BUS leg) ───────────────────────────────
            bus_wait_min = 0
            bus_wait_note = ""
            first_bus = next((l for l in legs if l["mode"] == "BUS"), None)
            if first_bus and first_bus["service_no"]:
                row_bw = con.execute(BUS_WAIT_QUERY, [first_bus["service_no"]]).fetchone()
                if row_bw:
                    raw_wait, load = row_bw
                    # Only add extra wait if bus is more than 5 min away
                    if raw_wait and raw_wait > 5:
                        bus_wait_min = raw_wait - 5
                        load_desc = {"SEA": "seats available", "SDA": "standing",
                                     "LSD": "very crowded"}.get(load, "")
                        bus_wait_note = f"next Bus {first_bus['service_no']} in {raw_wait} min ({load_desc})"

            # ── Active MRT disruptions ─────────────────────────────────────────
            alerts = con.execute(ACTIVE_ALERTS_QUERY).fetchall()

            # ── Leave-latest already in DB; compute leave-now scenario ─────────
            # estimated_arrival with standard conditions: event_start - 10 min buffer
            estimated_arrival = r["start_time"] - timedelta(minutes=10)

            # leave-now arrival = now + journey time + any extra bus wait
            adjusted_min = r["total_duration_min"] + bus_wait_min
            leave_now_arrival = now_utc + timedelta(minutes=adjusted_min)
            start_aware = r["start_time"]
            if start_aware.tzinfo is None:
                start_aware = start_aware.replace(tzinfo=timezone.utc)
            hours_until_event = (start_aware - now_utc).total_seconds() / 3600
            mins_early = (start_aware - leave_now_arrival).total_seconds() / 60

            # ── Rain impact on walk legs ───────────────────────────────────────
            rainy_walk_warnings = []
            if r["is_rainy"]:
                for leg in legs:
                    if leg["mode"] == "WALK" and leg["distance_m"] > 400:
                        rainy_walk_warnings.append(
                            f"⚠ Rain during {leg['duration_min']}-min walk "
                            f"({leg['distance_m']}m): {leg['from_name']} → {leg['to_name']}"
                        )

            # ── Write recommendation ───────────────────────────────────────────
            disruption_warning = r["alert_msg"] or (alerts[0][1] if alerts else None)
            weather_warning = rainy_walk_warnings[0] if rainy_walk_warnings else None

            con.execute(
                """INSERT OR REPLACE INTO recommendations
                   (event_id, recommended_mode, total_duration_min, leave_by,
                    estimated_arrival, weather_warning, disruption_warning, reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    r["event_id"],
                    "Bus + MRT" if r["num_transfers"] > 0 else "Direct Bus",
                    r["total_duration_min"],
                    r["leave_by"],
                    estimated_arrival,
                    weather_warning,
                    disruption_warning,
                    r["recommendation_reason"],
                ],
            )
            written += 1

            # ── Print recommendation ───────────────────────────────────────────
            divider = "═" * 56
            log.info(divider)
            log.info("📅  %s", r["title"])
            log.info("    Event starts : %s", format_time(r["start_time"]))
            log.info("─" * 56)

            # Leave latest
            log.info("🕐  LEAVE LATEST : %s  (arrive 10 min early)", format_time(r["leave_by"]))

            # Leave now — only meaningful if event is within 6 hours
            if hours_until_event > 6:
                log.info(
                    "🔵  LEAVE NOW    : Event in %.0fh — check back closer to departure",
                    hours_until_event,
                )
            elif mins_early >= 0:
                log.info(
                    "🟢  LEAVE NOW    : Arrive %s — %.0f min early ✓",
                    format_time(leave_now_arrival), mins_early,
                )
            else:
                log.warning(
                    "🔴  LEAVE NOW    : Arrive %s — %.0f min LATE — leave earlier!",
                    format_time(leave_now_arrival), abs(mins_early),
                )

            log.info("─" * 56)
            log.info("    Route: %d min | $%.2f | %d transfer(s)",
                     r["total_duration_min"], r["fare"] or 0, r["num_transfers"])
            log.info("")

            # Step-by-step legs
            if legs:
                for leg in legs:
                    icon = MODE_ICON.get(leg["mode"], "  ")
                    label = leg["service_no"] or leg["mode"]
                    if leg["mode"] == "MRT":
                        label = MRT_LINE_NAMES.get(leg["service_no"], leg["service_no"] or "MRT")
                    rain_flag = ""
                    if r["is_rainy"] and leg["mode"] == "WALK" and leg["distance_m"] > 400:
                        rain_flag = "  ☔ wet walk!"
                    wait_flag = ""
                    if first_bus and leg["mode"] == "BUS" and leg["service_no"] == first_bus["service_no"] and bus_wait_note:
                        wait_flag = f"  ⏳ {bus_wait_note}"
                    log.info(
                        "    %s  %-18s %3d min  %s → %s%s%s",
                        icon, label, leg["duration_min"],
                        leg["from_name"][:22], leg["to_name"][:22],
                        rain_flag, wait_flag,
                    )
            else:
                log.info("    (no leg detail — re-run ingest.py to populate route_legs)")

            log.info("")

            # Warnings
            if rainy_walk_warnings:
                for w in rainy_walk_warnings:
                    log.warning("    %s", w)
            else:
                log.info("    ☀  Weather: %s — no rain impact",
                         r.get("weather_forecast", "Clear"))

            if alerts:
                for affected_line, msg in alerts:
                    log.warning("    🚨 Disruption [%s]: %s", affected_line, msg[:80])
            else:
                log.info("    ✅  No active MRT/LRT disruptions")

            log.info(divider)

        log_run(con, written, int((time.time() - t0) * 1000), "success")
        log.info("Recommendations written: %d", written)

    except Exception as exc:
        log_run(con, 0, int((time.time() - t0) * 1000), "error", str(exc))
        raise
    finally:
        con.close()

    log.info("=== Transform complete ===")


if __name__ == "__main__":
    main()
