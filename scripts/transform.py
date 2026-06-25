import logging
import sys
import time
import uuid
from datetime import datetime, date, timedelta, timezone
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import duckdb
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from config import HOME_ADDRESS
except ImportError:
    HOME_ADDRESS = None
try:
    from config import GARMIN_EMAIL, GARMIN_PASSWORD
except ImportError:
    GARMIN_EMAIL = GARMIN_PASSWORD = ""
try:
    from config import WHOOP_ACCESS_TOKEN
except ImportError:
    WHOOP_ACCESS_TOKEN = ""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "db" / "commute.duckdb"

MODE_ICON = {"WALK": "🚶 ", "BUS": "🚌 ", "MRT": "🚇 ", "LRT": "🚈 "}

MRT_LINE_NAMES = {
    "EW": "East West Line",  "NS": "North South Line", "NE": "Northeast Line",
    "CC": "Circle Line",     "DT": "Downtown Line",    "TE": "Thomson-East Coast Line",
    "CR": "Cross Island Line","JR": "Jurong Region Line",
    "BP": "Bukit Panjang LRT","SE": "Sengkang LRT",   "PE": "Punggol LRT",
}

BEST_ROUTE_QUERY = """
SELECT
    event_id, title, start_time, leave_by,
    total_duration_min, walk_distance_m, num_transfers, fare,
    weather_forecast, is_rainy, alert_msg, recommendation_reason,
    dest_lat, dest_lng
FROM v_enriched_routes
WHERE route_rank = 1
  AND start_time > NOW()
ORDER BY start_time
LIMIT 1
"""

LEGS_QUERY = """
SELECT leg_sequence, mode, service_no, from_name, to_name, duration_min, distance_m, num_stops
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

FIRST_TRANSIT_FULL_QUERY = """
SELECT next_bus_mins, next_bus2_mins, load
FROM bus_arrivals
WHERE service_no = ?
ORDER BY fetched_at DESC
LIMIT 1
"""

ALT_ROUTES_QUERY = """
SELECT option_id, total_duration_min, fare, num_transfers
FROM route_options
WHERE event_id = ?
  AND option_id != ?
ORDER BY total_duration_min
"""

ACTIVE_ALERTS_QUERY = """
SELECT affected_line, message
FROM train_alerts
WHERE severity = 'HEAVY'
  AND fetched_at > NOW() - INTERVAL '30 minutes'
ORDER BY fetched_at DESC
LIMIT 3
"""


def _haversine_m(lat1, lng1, lat2, lng2):
    R = 6_371_000
    d_lat = radians(lat2 - lat1)
    d_lng = radians(lng2 - lng1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lng / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _detect_origin():
    """IP geolocation as routing origin — falls back to Bishan if outside SG or call fails."""
    try:
        r = requests.get("http://ip-api.com/json/", timeout=5)
        data = r.json()
        if data.get("status") == "success":
            lat, lng = data["lat"], data["lon"]
            if 1.15 <= lat <= 1.47 and 103.6 <= lng <= 104.1:
                return lat, lng
    except Exception:
        pass
    return 1.3521, 103.8198


def get_garmin_steps():
    if not (GARMIN_EMAIL and GARMIN_PASSWORD):
        return None
    try:
        from garminconnect import Garmin  # type: ignore[import-untyped]
        client = Garmin(GARMIN_EMAIL, GARMIN_PASSWORD)
        client.login()
        steps_data = client.get_steps_data(date.today().isoformat())
        return sum(item.get("steps", 0) for item in steps_data if item.get("steps"))
    except Exception as exc:
        log.warning("Garmin steps fetch failed: %s", exc)
        return None


def get_whoop_recovery():
    if not WHOOP_ACCESS_TOKEN:
        return None
    try:
        today_start = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)
        r = requests.get(
            "https://api.prod.whoop.com/developer/v1/recovery",
            headers={"Authorization": f"Bearer {WHOOP_ACCESS_TOKEN}"},
            params={
                "start": today_start.isoformat(),
                "end": (today_start + timedelta(days=1)).isoformat(),
            },
            timeout=10,
        )
        records = r.json().get("records", [])
        if records:
            return records[0].get("score", {}).get("recovery_score")
    except Exception as exc:
        log.warning("Whoop recovery fetch failed: %s", exc)
    return None


def _walk_metrics(origin_lat, origin_lng, dest_lat, dest_lng):
    distance_m = _haversine_m(origin_lat, origin_lng, dest_lat, dest_lng)
    distance_km = distance_m / 1000
    walk_min = round(distance_km / 5.0 * 60)
    steps_est = round(distance_m / 0.75)
    calories_est = round(distance_km * 63)
    garmin_steps = get_garmin_steps()
    whoop_recovery = get_whoop_recovery()
    log.info("    Distance   : %.1f km straight line", distance_km)
    log.info("    Est. time  : ~%d min at 5 km/h", walk_min)
    log.info("    Steps      : ~%s steps", f"{steps_est:,}")
    log.info("    Calories   : ~%d kcal", calories_est)
    log.info("")
    log.info("    Zone 1 (easy, 50-60%% max HR) — fat burn, active recovery")
    log.info("    Zone 2 (brisk, 60-70%% max HR) — aerobic base, best long-term benefit")
    if garmin_steps is not None:
        projected = garmin_steps + steps_est
        pct = min(100, round(projected / 10_000 * 100))
        log.info("")
        log.info("    [Garmin] Today so far : %s steps", f"{garmin_steps:,}")
        log.info("             After walk   : %s steps (%d%% of 10,000 goal)", f"{projected:,}", pct)
    if whoop_recovery is not None:
        if whoop_recovery >= 67:
            zone_rec = "green — Zone 2 effort, your body is ready"
        elif whoop_recovery >= 34:
            zone_rec = "yellow — Zone 1 only, moderate recovery today"
        else:
            zone_rec = "red — rest day, skip the walk"
        log.info("    [Whoop]  Recovery     : %d%% → %s", whoop_recovery, zone_rec)


def print_walk_suggestion(dest_lat, dest_lng, is_rainy):
    if is_rainy:
        return
    origin_lat, origin_lng = _detect_origin()
    distance_m = _haversine_m(origin_lat, origin_lng, dest_lat, dest_lng)
    distance_km = distance_m / 1000
    if distance_km > 5.0:
        return
    log.info("")
    log.info("─" * 56)
    log.info("🚶  WALK ALTERNATIVE  (%.1f km · no rain)", distance_km)
    _walk_metrics(origin_lat, origin_lng, dest_lat, dest_lng)


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

        SGT = timezone(timedelta(hours=8))
        now_utc = datetime.now(timezone.utc)
        now_sgt = now_utc.astimezone(SGT)
        written = 0

        for row in results:
            r = dict(zip(cols, row))
            option_id = f"{r['event_id']}_ROUTE_1"

            # ── Legs ──────────────────────────────────────────────────────────
            legs = con.execute(LEGS_QUERY, [option_id]).fetchall()
            leg_cols = ["leg_sequence", "mode", "service_no", "from_name",
                        "to_name", "duration_min", "distance_m", "num_stops"]
            legs = [dict(zip(leg_cols, l)) for l in legs]
            is_walk_only = bool(legs) and all(l["mode"] == "WALK" for l in legs)

            modes_in_legs = {l["mode"] for l in legs}
            _mrt = "MRT" in modes_in_legs
            _lrt = "LRT" in modes_in_legs
            _bus = "BUS" in modes_in_legs
            if _mrt and _lrt:
                recommended_mode = "Bus + MRT and LRT" if _bus else "MRT and LRT"
            elif _mrt:
                recommended_mode = "Bus + MRT" if _bus else "MRT"
            elif _lrt:
                recommended_mode = "Bus + LRT" if _bus else "LRT"
            elif _bus:
                recommended_mode = "Direct Bus"
            else:
                recommended_mode = "Walk"

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

            # ── Active rail disruptions (filtered to route's lines) ──────────────
            alerts = con.execute(ACTIVE_ALERTS_QUERY).fetchall()
            route_rail_modes = {l["mode"] for l in legs if l["mode"] in ("MRT", "LRT")}
            route_rail_lines = {l["service_no"] for l in legs
                                if l["mode"] in ("MRT", "LRT") and l["service_no"]}
            if "MRT" in route_rail_modes and "LRT" in route_rail_modes:
                rail_label = "MRT/LRT"
            elif "MRT" in route_rail_modes:
                rail_label = "MRT"
            elif "LRT" in route_rail_modes:
                rail_label = "LRT"
            else:
                rail_label = None
            relevant_alerts = [a for a in alerts if a[0] in route_rail_lines]

            # ── Leave-latest already in DB; compute leave-now scenario ─────────
            # estimated_arrival with standard conditions: event_start - 10 min buffer
            estimated_arrival = r["start_time"] - timedelta(minutes=10)

            # leave-now arrival = now + journey time + any extra bus wait
            adjusted_min = r["total_duration_min"] + bus_wait_min
            leave_now_arrival = (now_utc + timedelta(minutes=adjusted_min)).astimezone(SGT)
            start_aware = r["start_time"]
            if start_aware.tzinfo is None:
                start_aware = start_aware.replace(tzinfo=timezone.utc)
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
                    recommended_mode,
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

            if mins_early >= 0:
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
            log.info("    Why chosen: %s", r["recommendation_reason"])
            if rainy_walk_warnings:
                for w in rainy_walk_warnings:
                    log.warning("    %s", w)
            else:
                log.info("    ☀  Weather: %s — no rain impact",
                         r.get("weather_forecast", "Clear"))
            if relevant_alerts:
                for affected_line, msg in relevant_alerts:
                    log.warning("    🚨 Disruption [%s]: %s", affected_line, msg[:80])
            elif rail_label:
                log.info("    ✅  No active %s disruptions", rail_label)
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

            # ── Live arrival for first transit (recommended route, X1 + X2) ────────
            first_transit = next((l for l in legs if l["mode"] != "WALK"), None)
            if first_transit:
                ft_mode = first_transit["mode"]
                ft_svc = first_transit["service_no"] or ""
                ft_icon = MODE_ICON.get(ft_mode, "  ")
                if ft_mode == "BUS" and ft_svc:
                    bw = con.execute(FIRST_TRANSIT_FULL_QUERY, [ft_svc]).fetchone()
                    if bw and bw[0] is not None:
                        x1, x2, load = bw
                        load_desc = {"SEA": "seats", "SDA": "standing", "LSD": "full"}.get(load or "", "")
                        x1_time = (now_sgt + timedelta(minutes=x1)).strftime("%H:%M")
                        x2_str = ""
                        if x2 is not None:
                            x2_time = (now_sgt + timedelta(minutes=x2)).strftime("%H:%M")
                            x2_str = f" | if missed → {x2_time} (~{x2} min from now)"
                        log.info("    %s  Bus %-6s : next at %s (~%d min from now)%s  %s",
                                 ft_icon, ft_svc, x1_time, x1, x2_str, load_desc)
                    else:
                        log.info("    %s  Bus %-6s : no live data at origin stop", ft_icon, ft_svc)
                elif ft_mode == "MRT":
                    ft_name = MRT_LINE_NAMES.get(ft_svc, ft_svc or "MRT")
                    x1_min, x2_min = 4, 8
                    x1_time = (now_sgt + timedelta(minutes=x1_min)).strftime("%H:%M")
                    x2_time = (now_sgt + timedelta(minutes=x2_min)).strftime("%H:%M")
                    log.info("    %s  %-22s : next at %s (~%d min from now) | if missed → %s (~%d min from now)",
                             ft_icon, ft_name, x1_time, x1_min, x2_time, x2_min)
                elif ft_mode == "LRT":
                    ft_name = ft_svc or "LRT"
                    x1_min, x2_min = 7, 14
                    x1_time = (now_sgt + timedelta(minutes=x1_min)).strftime("%H:%M")
                    x2_time = (now_sgt + timedelta(minutes=x2_min)).strftime("%H:%M")
                    log.info("    %s  %-22s : next at %s (~%d min from now) | if missed → %s (~%d min from now)",
                             ft_icon, ft_name, x1_time, x1_min, x2_time, x2_min)
            log.info("")

            if is_walk_only:
                origin_lat, origin_lng = _detect_origin()
                _walk_metrics(origin_lat, origin_lng, r["dest_lat"], r["dest_lng"])

            # ── Alt routes (compact) ───────────────────────────────────────────
            alt_rows = [] if is_walk_only else con.execute(ALT_ROUTES_QUERY, [r["event_id"], option_id]).fetchall()
            if alt_rows:
                _lcols = ["leg_sequence", "mode", "service_no", "from_name",
                          "to_name", "duration_min", "distance_m", "num_stops"]

                scored = []
                for alt_oid, alt_dur, alt_fare, _ in alt_rows:
                    alt_legs = [dict(zip(_lcols, l))
                                for l in con.execute(LEGS_QUERY, [alt_oid]).fetchall()]
                    # First non-WALK leg drives arrival scoring and live display
                    ft_leg = next((l for l in alt_legs if l["mode"] != "WALK"), None)
                    ft_svc = (ft_leg["service_no"] or "") if ft_leg else ""
                    ft_x1 = None
                    if ft_leg and ft_leg["mode"] == "BUS" and ft_svc:
                        bw = con.execute(BUS_WAIT_QUERY, [ft_svc]).fetchone()
                        ft_x1 = bw[0] if bw else None
                    # Extra BUS delays: buses that are not the first transit
                    extra_bus_notes = []
                    for leg in alt_legs:
                        svc = leg["service_no"] or ""
                        if leg["mode"] == "BUS" and svc and svc != ft_svc:
                            bw = con.execute(BUS_WAIT_QUERY, [svc]).fetchone()
                            if bw and bw[0] is not None:
                                flag = " ⚠" if bw[0] > 10 else ""
                                extra_bus_notes.append(f"Bus {svc} in {bw[0]}m{flag}")
                    scored.append((alt_dur + (ft_x1 or 0), alt_oid, alt_dur, alt_fare,
                                   alt_legs, ft_leg, ft_x1, extra_bus_notes))

                scored.sort(key=lambda x: x[0])

                log.info("─" * 56)
                log.info("🔄  Other route options  (sorted by arrival time)")
                for display_rank, (_, alt_oid, alt_dur, alt_fare, alt_legs,
                                   ft_leg, ft_x1, extra_bus_notes) in enumerate(scored[:3], 2):
                    parts = []
                    ai = 0
                    while ai < len(alt_legs):
                        leg = alt_legs[ai]
                        mode = leg["mode"]
                        dur = leg["duration_min"]
                        svc = leg["service_no"] or ""
                        stops = leg["num_stops"]
                        st = f"/{stops}st" if stops else ""
                        if mode == "WALK":
                            parts.append(f"🚶 {dur}m")
                            ai += 1
                        elif mode == "BUS":
                            parts.append(f"🚌 {svc} {dur}m{st}")
                            ai += 1
                        elif mode in ("MRT", "LRT"):
                            rail = []
                            while ai < len(alt_legs) and alt_legs[ai]["mode"] in ("MRT", "LRT"):
                                rail.append(alt_legs[ai])
                                ai += 1
                            r_mrt = any(l["mode"] == "MRT" for l in rail)
                            r_lrt = any(l["mode"] == "LRT" for l in rail)
                            r_dur = sum(l["duration_min"] for l in rail)
                            if r_mrt and r_lrt:
                                parts.append(f"🚇🚈 MRT+LRT {r_dur}m")
                            elif r_mrt:
                                parts.append(f"🚇 {rail[0]['service_no'] or 'MRT'} {r_dur}m")
                            else:
                                parts.append(f"🚈 {rail[0]['service_no'] or 'LRT'} {r_dur}m")
                        else:
                            ai += 1
                    leg_str = " → ".join(parts) if parts else "(no legs)"
                    log.info("    [%d]  %d min  $%.2f    %s",
                             display_rank, alt_dur, alt_fare or 0, leg_str)

                    # Notes line: first transit X1 | disruption | extra bus delays
                    notes = []
                    if ft_leg:
                        at_icon = MODE_ICON.get(ft_leg["mode"], "  ")
                        at_svc = ft_leg["service_no"] or ""
                        if ft_leg["mode"] == "BUS" and at_svc:
                            if ft_x1 is not None:
                                x1_time = (now_sgt + timedelta(minutes=ft_x1)).strftime("%H:%M")
                                flag = " ⚠" if ft_x1 > 10 else ""
                                notes.append(f"{at_icon}Bus {at_svc}: {x1_time} (~{ft_x1}m){flag}")
                            else:
                                notes.append(f"{at_icon}Bus {at_svc}: no live data")
                        elif ft_leg["mode"] == "MRT":
                            ft_name = MRT_LINE_NAMES.get(at_svc, at_svc or "MRT")
                            x1_time = (now_sgt + timedelta(minutes=4)).strftime("%H:%M")
                            notes.append(f"{at_icon}{ft_name}: ~{x1_time} (~4m)")
                        elif ft_leg["mode"] == "LRT":
                            x1_time = (now_sgt + timedelta(minutes=7)).strftime("%H:%M")
                            notes.append(f"{at_icon}{at_svc or 'LRT'}: ~{x1_time} (~7m)")
                    alt_rail_modes = {l["mode"] for l in alt_legs if l["mode"] in ("MRT", "LRT")}
                    alt_rail_lines = {l["service_no"] for l in alt_legs
                                      if l["mode"] in ("MRT", "LRT") and l["service_no"]}
                    alt_alerts = [a for a in alerts if a[0] in alt_rail_lines]
                    if alt_alerts:
                        for aline, _ in alt_alerts:
                            notes.append(f"⚠ {aline} disruption")
                    elif alt_rail_modes:
                        if "MRT" in alt_rail_modes and "LRT" in alt_rail_modes:
                            notes.append("✅ No MRT/LRT disruption")
                        elif "MRT" in alt_rail_modes:
                            notes.append("✅ No MRT disruption")
                        else:
                            notes.append("✅ No LRT disruption")
                    notes.extend(extra_bus_notes)
                    if notes:
                        log.info("         %s", " | ".join(notes))
                log.info("")

            if not is_walk_only:
                print_walk_suggestion(r["dest_lat"], r["dest_lng"], r["is_rainy"])

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
