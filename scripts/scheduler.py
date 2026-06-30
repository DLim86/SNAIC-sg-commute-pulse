"""
Adaptive 7-state polling scheduler for SNAIC-sg-commute-pulse.
Runs as the Docker pipeline service: python scripts/scheduler.py

States:
  NO_EVENT → EVENT_DETECTED_BURST → WATCHING → LEAVE_WINDOW
    → IN_TRANSIT → ARRIVAL_VERIFY → POST_ARRIVAL_COOLDOWN → NO_EVENT

Weather runs every 30 min regardless of state.
Calendar-check (cheap, cached geocode) runs every tick.
Routes (expensive: OneMap + LTA + ip-api) run only on event/location change.
"""
import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SGT = timezone(timedelta(hours=8))
DB_PATH = str(Path(__file__).parent.parent / "db" / "commute.duckdb")
PYTHON = sys.executable
PROJ = str(Path(__file__).parent.parent)

# ── Timing constants — tune here, not in the loop logic ─────────────────────
NO_EVENT_DAY_SEC        = 60     # Calendar check interval during the day
NO_EVENT_NIGHT_SEC      = 3600   # Calendar check interval midnight–6 AM SGT
BURST_CHECKS            = 5      # Consecutive stable checks before trusting an event
BURST_INTERVAL_SEC      = 30     # Interval between burst stability checks
WATCHING_INTERVAL_SEC   = 600    # Calendar check interval in WATCHING (10 min)
LEAVE_WINDOW_MARGIN_MIN = 10     # Minutes before leave_by to enter LEAVE_WINDOW
LEAVE_BURST_SECONDS     = 60     # Duration of the 1-second polling burst
LEAVE_BACKOFF_SEC       = 30     # Calendar check interval after burst expires
BUS_REFRESH_SEC         = 60     # Route/bus refresh interval in post-burst LEAVE_WINDOW
INTRANSIT_INTERVAL_SEC  = 600    # Calendar + route check interval in IN_TRANSIT (10 min)
ARRIVAL_VERIFY_CHECKS   = 5      # Calendar checks before declaring trip complete
ARRIVAL_VERIFY_SEC      = 30     # Interval between arrival verify checks
COOLDOWN_DURATION_SEC   = 600    # How long to stay in POST_ARRIVAL_COOLDOWN (10 min)
COOLDOWN_INTERVAL_SEC   = 60     # Calendar check interval during cooldown
WEATHER_INTERVAL_SEC    = 1800   # Weather refresh interval in all states (30 min)


# ── Script runners ────────────────────────────────────────────────────────────

def _py(*args):
    """Run a Python script and return stdout (stripped). Logs stderr on failure."""
    cmd = [PYTHON] + [str(a) for a in args]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJ)
    if result.returncode != 0:
        log.warning("Command failed %s:\n%s", " ".join(str(a) for a in args), result.stderr[-500:])
    return result.stdout.strip()


def _run(*script_args):
    """Run a Python script (no stdout capture). Logs warning on non-zero exit."""
    rc = subprocess.run([PYTHON] + [str(a) for a in script_args], cwd=PROJ).returncode
    if rc != 0:
        log.warning("Step failed: %s", " ".join(str(a) for a in script_args))


def _run_routes():
    """Expensive refresh: ip-api + OneMap routing + LTA arrivals + train alerts + transform + predict."""
    log.info("Event/location changed — refreshing routes")
    _run("scripts/ingest.py", "--mode", "routes")
    _run("scripts/transform.py")
    _run("scripts/model.py", "--predict")
    _run("scripts/model.py", "--backfill")


def _run_weather():
    """Weather refresh + transform re-run (independent of state)."""
    log.info("Weather refresh due — running weather mode")
    _run("scripts/ingest.py", "--mode", "weather")
    _run("scripts/transform.py")


def _get_event_timing():
    """Return (leave_by, start_time, estimated_arrival) from DB, or (None, None, None)."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        row = con.execute("""
            SELECT v.leave_by, v.start_time, r.estimated_arrival
            FROM v_enriched_routes v
            LEFT JOIN recommendations r ON r.event_id = v.event_id
            WHERE v.route_rank = 1 AND v.start_time > NOW()
            ORDER BY v.start_time LIMIT 1
        """).fetchone()
        con.close()
        if row:
            return row[0], row[1], row[2]
    except Exception as exc:
        log.debug("Timing query skipped: %s", exc)
    return None, None, None


def _ensure_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def main():
    log.info("=== Scheduler starting ===")
    _py("scripts/schema.py")
    _py("scripts/model.py", "--train")

    now_utc = datetime.now(timezone.utc)

    state           = "NO_EVENT"
    prev_key        = ""    # last confirmed stable event key
    stable_key      = ""    # candidate key being confirmed in EVENT_DETECTED_BURST
    burst_count     = 0     # consecutive stable checks so far
    arrival_count   = 0     # checks completed in ARRIVAL_VERIFY
    leave_burst_end = None  # datetime: when the 1s burst in LEAVE_WINDOW expires
    bus_refresh_last = now_utc - timedelta(seconds=BUS_REFRESH_SEC)
    cooldown_end    = None  # datetime: when POST_ARRIVAL_COOLDOWN ends
    weather_last    = now_utc - timedelta(seconds=WEATHER_INTERVAL_SEC)

    while True:
        now_utc = datetime.now(timezone.utc)
        now_sgt_hour = now_utc.astimezone(SGT).hour

        # ── Weather: independent of state, every 30 min ───────────────────────
        if (now_utc - weather_last).total_seconds() >= WEATHER_INTERVAL_SEC:
            _run_weather()
            weather_last = datetime.now(timezone.utc)

        # ── State: NO_EVENT ───────────────────────────────────────────────────
        if state == "NO_EVENT":
            sleep_sec = NO_EVENT_NIGHT_SEC if (0 <= now_sgt_hour < 6) else NO_EVENT_DAY_SEC
            log.info("State: NO_EVENT — checking calendar every %ds", sleep_sec)
            new_key = _py("scripts/ingest.py", "--mode", "calendar-check")
            if new_key:
                log.info("New event detected — entering EVENT_DETECTED_BURST")
                state = "EVENT_DETECTED_BURST"
                stable_key = new_key
                burst_count = 0
            time.sleep(sleep_sec)

        # ── State: EVENT_DETECTED_BURST ───────────────────────────────────────
        elif state == "EVENT_DETECTED_BURST":
            new_key = _py("scripts/ingest.py", "--mode", "calendar-check")
            if not new_key:
                log.info("Event disappeared during burst — returning to NO_EVENT")
                state = "NO_EVENT"
                prev_key = ""
                time.sleep(NO_EVENT_DAY_SEC)
            elif new_key == stable_key:
                burst_count += 1
                log.info("Burst check %d/%d — key stable", burst_count, BURST_CHECKS)
                if burst_count >= BURST_CHECKS:
                    log.info("Event stable after %d checks — entering WATCHING", BURST_CHECKS)
                    _run_routes()
                    prev_key = stable_key
                    state = "WATCHING"
                    time.sleep(WATCHING_INTERVAL_SEC)
                else:
                    time.sleep(BURST_INTERVAL_SEC)
            else:
                log.info("Event/location changed during burst — restarting burst")
                stable_key = new_key
                burst_count = 0
                _run_routes()
                time.sleep(BURST_INTERVAL_SEC)

        # ── State: WATCHING ───────────────────────────────────────────────────
        elif state == "WATCHING":
            new_key = _py("scripts/ingest.py", "--mode", "calendar-check")
            now_utc = datetime.now(timezone.utc)

            if not new_key:
                log.info("Event cancelled — returning to NO_EVENT")
                state = "NO_EVENT"
                prev_key = ""
                time.sleep(NO_EVENT_DAY_SEC)
                continue

            if new_key != prev_key:
                log.info("Event/location changed — refreshing routes")
                _run_routes()
                prev_key = new_key

            leave_by, start_time, _ = _get_event_timing()
            leave_by   = _ensure_utc(leave_by)
            start_time = _ensure_utc(start_time)
            now_utc    = datetime.now(timezone.utc)

            if start_time and start_time < now_utc:
                log.info("Stale event detected — returning to NO_EVENT")
                state = "NO_EVENT"
                prev_key = ""
                time.sleep(NO_EVENT_DAY_SEC)
                continue

            if leave_by:
                time_to_leave = (leave_by - now_utc).total_seconds()
                if time_to_leave <= LEAVE_WINDOW_MARGIN_MIN * 60:
                    log.info(
                        "Near leave_by (%.0fm away) — entering LEAVE_WINDOW (1s burst for %ds)",
                        time_to_leave / 60, LEAVE_BURST_SECONDS,
                    )
                    state = "LEAVE_WINDOW"
                    leave_burst_end = datetime.now(timezone.utc) + timedelta(seconds=LEAVE_BURST_SECONDS)
                    bus_refresh_last = now_utc
                    time.sleep(1)
                    continue

            log.info("State: WATCHING — next check in %ds", WATCHING_INTERVAL_SEC)
            time.sleep(WATCHING_INTERVAL_SEC)

        # ── State: LEAVE_WINDOW ───────────────────────────────────────────────
        elif state == "LEAVE_WINDOW":
            new_key = _py("scripts/ingest.py", "--mode", "calendar-check")
            now_utc = datetime.now(timezone.utc)

            if not new_key:
                log.info("Event cancelled in LEAVE_WINDOW — returning to NO_EVENT")
                state = "NO_EVENT"
                prev_key = ""
                time.sleep(NO_EVENT_DAY_SEC)
                continue

            if new_key != prev_key:
                log.info("Event/location changed in LEAVE_WINDOW — refreshing routes")
                _run_routes()
                prev_key = new_key

            in_burst = leave_burst_end is not None and now_utc < leave_burst_end

            # Log "burst completed" once — only in the first LEAVE_BACKOFF_SEC window after burst ends
            if not in_burst and leave_burst_end is not None:
                since_burst = (now_utc - leave_burst_end).total_seconds()
                if since_burst < LEAVE_BACKOFF_SEC:
                    log.info("1s burst completed — backing off to %ds checks", LEAVE_BACKOFF_SEC)

            if not in_burst:
                if (now_utc - bus_refresh_last).total_seconds() >= BUS_REFRESH_SEC:
                    log.info("Bus arrival refresh in LEAVE_WINDOW")
                    _run_routes()
                    bus_refresh_last = datetime.now(timezone.utc)

            leave_by, _, _ = _get_event_timing()
            leave_by = _ensure_utc(leave_by)
            now_utc  = datetime.now(timezone.utc)

            if leave_by and now_utc >= leave_by:
                log.info("Trip active — entering IN_TRANSIT")
                state = "IN_TRANSIT"
                time.sleep(INTRANSIT_INTERVAL_SEC)
                continue

            sleep_sec = 1 if in_burst else LEAVE_BACKOFF_SEC
            time.sleep(sleep_sec)

        # ── State: IN_TRANSIT ─────────────────────────────────────────────────
        elif state == "IN_TRANSIT":
            new_key = _py("scripts/ingest.py", "--mode", "calendar-check")
            now_utc = datetime.now(timezone.utc)

            if not new_key:
                log.info("Event cancelled in transit — returning to NO_EVENT")
                state = "NO_EVENT"
                prev_key = ""
                time.sleep(NO_EVENT_DAY_SEC)
                continue

            if new_key != prev_key:
                log.info("Event/location changed in transit — refreshing routes")
                _run_routes()
                prev_key = new_key

            _, start_time, est_arrival = _get_event_timing()
            arrival_trigger = _ensure_utc(est_arrival) or _ensure_utc(start_time)

            if arrival_trigger and now_utc >= arrival_trigger:
                log.info("Estimated arrival reached — entering ARRIVAL_VERIFY")
                state = "ARRIVAL_VERIFY"
                arrival_count = 0
                time.sleep(ARRIVAL_VERIFY_SEC)
                continue

            # Refresh routes to pick up any origin shift during the journey
            _run_routes()
            log.info("State: IN_TRANSIT — next check in %ds", INTRANSIT_INTERVAL_SEC)
            time.sleep(INTRANSIT_INTERVAL_SEC)

        # ── State: ARRIVAL_VERIFY ─────────────────────────────────────────────
        elif state == "ARRIVAL_VERIFY":
            new_key = _py("scripts/ingest.py", "--mode", "calendar-check")
            arrival_count += 1
            log.info("Arrival check %d/%d", arrival_count, ARRIVAL_VERIFY_CHECKS)

            if new_key and new_key != prev_key:
                log.info("New event after arrival — entering EVENT_DETECTED_BURST")
                state = "EVENT_DETECTED_BURST"
                stable_key = new_key
                burst_count = 0
                time.sleep(BURST_INTERVAL_SEC)
                continue

            if arrival_count >= ARRIVAL_VERIFY_CHECKS:
                log.info(
                    "No new event after %d checks — entering POST_ARRIVAL_COOLDOWN",
                    ARRIVAL_VERIFY_CHECKS,
                )
                state = "POST_ARRIVAL_COOLDOWN"
                cooldown_end = datetime.now(timezone.utc) + timedelta(seconds=COOLDOWN_DURATION_SEC)
                prev_key = ""
                time.sleep(COOLDOWN_INTERVAL_SEC)
                continue

            time.sleep(ARRIVAL_VERIFY_SEC)

        # ── State: POST_ARRIVAL_COOLDOWN ──────────────────────────────────────
        elif state == "POST_ARRIVAL_COOLDOWN":
            new_key = _py("scripts/ingest.py", "--mode", "calendar-check")
            now_utc = datetime.now(timezone.utc)

            if new_key:
                log.info("New event during cooldown — entering EVENT_DETECTED_BURST")
                state = "EVENT_DETECTED_BURST"
                stable_key = new_key
                burst_count = 0
                time.sleep(BURST_INTERVAL_SEC)
                continue

            if cooldown_end and now_utc >= cooldown_end:
                log.info("No new event after cooldown — returning to NO_EVENT")
                state = "NO_EVENT"
                time.sleep(NO_EVENT_DAY_SEC)
                continue

            remaining = int((cooldown_end - now_utc).total_seconds()) if cooldown_end else 0
            log.info("State: POST_ARRIVAL_COOLDOWN — %ds remaining", remaining)
            time.sleep(COOLDOWN_INTERVAL_SEC)


if __name__ == "__main__":
    main()
