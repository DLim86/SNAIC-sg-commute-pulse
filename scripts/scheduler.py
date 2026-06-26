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
BUFFER_MIN = 30  # minutes before leave_by to enter IMMINENT state


def _py(*args):
    """Run a Python script, return stdout stripped. Logs stderr on non-zero exit."""
    cmd = [PYTHON] + [str(a) for a in args]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJ)
    if result.returncode != 0:
        log.warning("Command failed %s:\n%s", " ".join(args), result.stderr[-500:])
    return result.stdout.strip()


def _run(*script_args):
    """Run a Python script (no stdout capture). Logs warning on failure."""
    rc = subprocess.run([PYTHON] + list(script_args), cwd=PROJ).returncode
    if rc != 0:
        log.warning("Step failed: %s", " ".join(script_args))


def _run_group2():
    """Group 2: routes + live data + transform + predict. Only on event/dest change."""
    log.info("[GROUP2] fetching routes, bus arrivals, train alerts")
    _run("scripts/ingest.py", "--mode", "routes")
    _run("scripts/transform.py")
    _run("scripts/model.py", "--predict")
    _run("scripts/model.py", "--backfill")


def _run_weather():
    """Group 3: weather refresh + transform re-run."""
    log.info("[WEATHER] refreshing forecast")
    _run("scripts/ingest.py", "--mode", "weather")
    _run("scripts/transform.py")


def _get_event_timing():
    """Return (leave_by, start_time) from DB recommendations view, or (None, None)."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        row = con.execute(
            "SELECT leave_by, start_time FROM v_enriched_routes "
            "WHERE route_rank = 1 AND start_time > NOW() ORDER BY start_time LIMIT 1"
        ).fetchone()
        con.close()
        if row:
            return row[0], row[1]
    except Exception as exc:
        log.debug("Timing query skipped: %s", exc)
    return None, None


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

    prev_key = ""
    weather_last = datetime.now(timezone.utc) - timedelta(hours=1)

    while True:
        now_utc = datetime.now(timezone.utc)

        # ── Group 3: weather every 30 min ─────────────────────────────────────
        if (now_utc - weather_last).total_seconds() >= 1800:
            _run_weather()
            weather_last = datetime.now(timezone.utc)

        # ── Group 1: calendar check ────────────────────────────────────────────
        new_key = _py("scripts/ingest.py", "--mode", "calendar-check")

        if new_key and new_key != prev_key:
            log.info("[GROUP2] change detected: %s → %s", prev_key or "none", new_key)
            _run_group2()
            prev_key = new_key
        elif not new_key and prev_key:
            log.info("[NO_EVENT] event ended or cancelled")
            prev_key = ""

        # ── State machine: decide sleep duration ──────────────────────────────
        leave_by, start_time = _get_event_timing()
        leave_by = _ensure_utc(leave_by)
        start_time = _ensure_utc(start_time)
        now_utc = datetime.now(timezone.utc)
        now_sgt_hour = now_utc.astimezone(SGT).hour

        if start_time is not None and start_time < now_utc:
            # EXPIRED — event already started
            log.info("[EXPIRED] event started, resetting for next event")
            prev_key = ""
            time.sleep(5)
            continue

        if leave_by is None:
            # NO_EVENT — long sleep at night, normal during day
            sleep_sec = 3600 if (0 <= now_sgt_hour < 6) else 60
            log.info("[NO_EVENT] sleeping %ds", sleep_sec)
            time.sleep(sleep_sec)
            continue

        time_to_leave_sec = (leave_by - now_utc).total_seconds()

        if time_to_leave_sec > BUFFER_MIN * 60:
            # WATCHING — sleep until 30 min before leave_by (capped at 1 hr for safety)
            sleep_sec = min(time_to_leave_sec - BUFFER_MIN * 60, 3600)
            log.info("[WATCHING] %.0fm to leave — sleeping %.0fm", time_to_leave_sec / 60, sleep_sec / 60)
            time.sleep(sleep_sec)
        else:
            # IMMINENT — 1s polling, calendar-check uses cached geocode (no OneMap call)
            log.debug("[IMMINENT] %.0fs to leave_by", time_to_leave_sec)
            time.sleep(1)


if __name__ == "__main__":
    main()
