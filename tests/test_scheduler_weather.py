"""Weather-consistency tests for scripts/scheduler.py.

Run with:  python tests/test_scheduler_weather.py

Covers the micro-fix: a brand-new event has calendar_events.nearest_weather_area
= NULL until ingest --mode weather runs. model.py COALESCEs that miss to
is_rainy = FALSE, so with weather on a fixed 30-minute timer the ML prediction
could silently assume dry weather for up to 30 minutes after a new event.
_run_routes() must resolve destination weather BEFORE predicting.
"""
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import duckdb

from scripts import model, scheduler
from test_model_features import _Recorder, _seed_db

failures = []


def check(cond, msg):
    print(("  PASS  " if cond else "  FAIL  ") + msg)
    if not cond:
        failures.append(msg)


def _fresh_db(dest_area):
    """Temp DB holding one future event. dest_area=None mimics a brand-new event."""
    tmp = Path(tempfile.mkdtemp()) / "sched_wx.duckdb"
    con = _seed_db(tmp, dest_area, False, "Tuas", False)
    con.close()
    return tmp


def _naive_utc():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _simulate_weather_ingest(db_path, area="Bishan", is_rainy=True):
    """What ingest --mode weather does that matters here: set the area, store its forecast."""
    con = duckdb.connect(str(db_path))
    con.execute("INSERT INTO weather_forecast VALUES (?,?,?,?)",
                [area, "Thundery Showers" if is_rainy else "Fair", is_rainy, _naive_utc()])
    con.execute("UPDATE calendar_events SET nearest_weather_area = ?", [area])
    con.close()


# ------------------------------------------------- A. readiness probe
def test_readiness_probe():
    print("\n[A1] _dest_weather_ready() detects an unresolved destination area")
    db = _fresh_db(None)
    scheduler.DB_PATH = str(db)
    check(scheduler._dest_weather_ready() is False,
          "new event with nearest_weather_area = NULL -> not ready")

    _simulate_weather_ingest(db)
    check(scheduler._dest_weather_ready() is True,
          "after the weather run resolves the area -> ready")


def test_readiness_probe_stale():
    print("\n[A2] A forecast older than the refresh interval counts as not ready")
    db = _fresh_db("Bishan")
    scheduler.DB_PATH = str(db)
    con = duckdb.connect(str(db))
    con.execute("DELETE FROM weather_forecast")
    stale = _naive_utc() - timedelta(seconds=scheduler.WEATHER_INTERVAL_SEC + 60)
    con.execute("INSERT INTO weather_forecast VALUES (?,?,?,?)",
                ["Bishan", "Fair", False, stale])
    con.close()
    check(scheduler._dest_weather_ready() is False,
          "forecast %ds old -> not ready" % (scheduler.WEATHER_INTERVAL_SEC + 60))

    con = duckdb.connect(str(db))
    con.execute("INSERT INTO weather_forecast VALUES (?,?,?,?)",
                ["Bishan", "Fair", False, _naive_utc()])
    con.close()
    check(scheduler._dest_weather_ready() is True,
          "a fresh forecast for the same area -> ready")


def test_missing_db_does_not_force_refresh():
    print("\n[A3] An unreadable DB does not trigger a needless data.gov.sg call")
    scheduler.DB_PATH = str(Path(tempfile.mkdtemp()) / "does_not_exist.duckdb")
    check(scheduler._dest_weather_ready() is True,
          "unreadable DB is treated as no evidence of staleness")


# ------------------------------------------------- B. ordering inside one cycle
def _record_cycle(db_path, weather_side_effect):
    """Run _run_routes() with subprocess calls stubbed; return the ordered call list."""
    calls = []
    real_run = scheduler._run

    def fake_run(*args):
        joined = " ".join(str(a) for a in args)
        calls.append(joined)
        if "--mode weather" in joined and weather_side_effect:
            _simulate_weather_ingest(db_path)

    scheduler._run = fake_run
    try:
        scheduler._run_routes()
    finally:
        scheduler._run = real_run
    return calls


def test_weather_runs_before_prediction():
    print("\n[B1] New event: weather is resolved BEFORE transform and predict")
    db = _fresh_db(None)
    scheduler.DB_PATH = str(db)
    calls = _record_cycle(db, weather_side_effect=True)
    for c in calls:
        print("         -> " + c)

    wx = next((i for i, c in enumerate(calls) if "--mode weather" in c), None)
    tr = next((i for i, c in enumerate(calls) if "transform.py" in c), None)
    pr = next((i for i, c in enumerate(calls) if "--predict" in c), None)

    check(wx is not None, "weather mode ran in the same cycle as the route refresh")
    check(tr is not None and pr is not None, "transform and predict both ran")
    if wx is not None and tr is not None and pr is not None:
        check(wx < tr, "weather (idx %s) precedes transform (idx %s)" % (wx, tr))
        check(wx < pr, "weather (idx %s) precedes predict (idx %s)" % (wx, pr))
    check(calls[0] == "scripts/ingest.py --mode routes",
          "routes still runs first, so the destination is stored before weather")


def test_no_redundant_weather_when_fresh():
    print("\n[B2] Already-fresh destination weather is not re-fetched by _run_routes()")
    db = _fresh_db("Bishan")
    scheduler.DB_PATH = str(db)
    _simulate_weather_ingest(db, is_rainy=False)
    calls = _record_cycle(db, weather_side_effect=False)
    check(not any("--mode weather" in c for c in calls),
          "no extra weather call when the forecast is current (%d step(s) run)" % len(calls))


def test_weather_timer_is_stamped():
    print("\n[B3] An on-demand weather run resets the shared 30-min timer")
    db = _fresh_db(None)
    scheduler.DB_PATH = str(db)
    scheduler._weather_last = None
    real_run = scheduler._run

    def fake_run(*args):
        if "weather" in " ".join(str(a) for a in args):
            _simulate_weather_ingest(db)

    scheduler._run = fake_run
    try:
        scheduler._run_routes()
    finally:
        scheduler._run = real_run
    check(scheduler._weather_last is not None,
          "_weather_last stamped -> the 30-min timer will not immediately re-fetch")


# ------------------------------------------------- C. end-to-end feature value
def test_model_sees_destination_rain_same_cycle():
    print("\n[C1] The model receives the destination is_rainy value in the SAME cycle")
    db = _fresh_db(None)
    scheduler.DB_PATH = str(db)

    con = duckdb.connect(str(db), read_only=True)
    area_before = con.execute("SELECT nearest_weather_area FROM calendar_events").fetchone()[0]
    con.close()
    check(area_before is None,
          "before the cycle the event has no weather area (got %r)" % area_before)

    # Destination is rainy. Without the fix, predict would run before this is known.
    _record_cycle(db, weather_side_effect=True)

    con = duckdb.connect(str(db), read_only=True)
    area_after = con.execute("SELECT nearest_weather_area FROM calendar_events").fetchone()[0]
    con.close()
    check(area_after == "Bishan",
          "after the cycle the weather area is set (got %r)" % area_after)

    captured = []
    con = duckdb.connect(str(db))
    real_load, real_exists = model.joblib.load, Path.exists
    try:
        model.joblib.load = lambda _p: _Recorder(captured)
        Path.exists = lambda self: True if str(self).endswith(".pkl") else real_exists(self)
        model.predict(con)
    finally:
        model.joblib.load = real_load
        Path.exists = real_exists
        con.close()

    check(len(captured) >= 1, "predict() scored the route option")
    if captured:
        check(captured[0]["is_rainy"] == 1,
              "model received is_rainy=%s for a rainy destination (expected 1; "
              "the pre-fix bug silently passed 0)" % captured[0]["is_rainy"])
        check(captured[0]["rain_exposure"] > 0,
              "rain_exposure follows through -> %.3f km of wet walking"
              % captured[0]["rain_exposure"])


if __name__ == "__main__":
    print("=" * 70)
    print("scheduler.py weather-consistency tests")
    print("=" * 70)
    test_readiness_probe()
    test_readiness_probe_stale()
    test_missing_db_does_not_force_refresh()
    test_weather_runs_before_prediction()
    test_no_redundant_weather_when_fresh()
    test_weather_timer_is_stamped()
    test_model_sees_destination_rain_same_cycle()
    print("\n" + "=" * 70)
    if failures:
        print("FAILED: %d check(s)" % len(failures))
        for f in failures:
            print("  - " + f)
        sys.exit(1)
    print("ALL CHECKS PASSED")
