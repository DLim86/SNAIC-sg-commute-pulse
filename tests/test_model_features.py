"""Feature-correctness tests for scripts/model.py.

Run with:  python tests/test_model_features.py

Covers the two bugs fixed in the close-out pass:
  A. DuckDB EXTRACT(DOW) is 0=Sunday..6=Saturday, so the weekend is {0, 6}.
  B. is_rainy must come from the event's nearest_weather_area, not the
     globally-latest weather row, and must agree between training and serving.
"""
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from scripts import model

DOW_NAMES = {0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday",
             4: "Thursday", 5: "Friday", 6: "Saturday"}
EXPECTED_WEEKEND = {0: 1, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 1}

failures = []


def check(cond, msg):
    print(("  PASS  " if cond else "  FAIL  ") + msg)
    if not cond:
        failures.append(msg)


# ---------------------------------------------------------------- A. DOW
def test_weekend_mapping():
    print("\n[A1] _is_weekend maps all seven DuckDB DOW values correctly")
    for dow, expected in EXPECTED_WEEKEND.items():
        got = model._is_weekend(dow)
        check(got == expected,
              "dow=%d (%s) -> is_weekend=%d (expected %d)"
              % (dow, DOW_NAMES[dow], got, expected))


def test_rush_hour_mapping():
    print("\n[A2] _rush_hour fires on Mon-Fri peak hours only")
    for dow in range(7):
        weekday = EXPECTED_WEEKEND[dow] == 0
        got = model._rush_hour(8, dow)
        check(got == (1 if weekday else 0),
              "dow=%d (%s) 08:00 -> rush=%d" % (dow, DOW_NAMES[dow], got))
    check(model._rush_hour(13, 1) == 0, "Monday 13:00 is not rush hour")
    check(model._rush_hour(19, 5) == 1,
          "Friday 19:00 IS rush hour (the old dow<5 test wrongly excluded it)")
    check(model._rush_hour(8, 0) == 0,
          "Sunday 08:00 is NOT rush hour (the old dow<5 test wrongly included it)")


def test_inference_row_matches_convention():
    print("\n[A3] Inference feature row uses the same convention")
    i_rush = model.FEATURE_COLS_DUR.index("rush_hour")
    i_wknd = model.FEATURE_COLS_DUR.index("is_weekend")
    for dow in range(7):
        row = model._build_duration_row(40, 3, 11, 500, 1, False, 8, dow, 0)
        check(row[i_wknd] == EXPECTED_WEEKEND[dow],
              "_build_duration_row dow=%d -> is_weekend=%s" % (dow, row[i_wknd]))
        check(row[i_rush] == model._rush_hour(8, dow),
              "_build_duration_row dow=%d -> rush_hour=%s" % (dow, row[i_rush]))


def test_training_row_matches_convention():
    print("\n[A4] Synthetic TRAINING rows use the same convention as inference")
    df = model._bootstrap_synthetic_duration(n=300)
    bad_w = df[df["is_weekend"] != df["day_of_week"].apply(model._is_weekend)]
    check(len(bad_w) == 0, "all %d synthetic duration rows agree on is_weekend" % len(df))
    bad_r = df[df["rush_hour"] != df.apply(
        lambda r: model._rush_hour(r["hour_of_day"], r["day_of_week"]), axis=1)]
    check(len(bad_r) == 0, "all %d synthetic duration rows agree on rush_hour" % len(df))
    check(sorted(df["day_of_week"].unique()) == list(range(7)),
          "synthetic day_of_week spans the full 0-6 DuckDB domain")

    crd = model._bootstrap_synthetic_crowd(n=300)
    bad_c = crd[crd["rush_hour"] != crd.apply(
        lambda r: model._rush_hour(r["leave_hour"], r["day_of_week"]), axis=1)]
    check(len(bad_c) == 0, "all %d synthetic crowd rows agree on rush_hour" % len(crd))


def test_duckdb_dow_domain():
    print("\n[A5] DuckDB really does return 0=Sunday (the assumption behind the fix)")
    con = duckdb.connect(":memory:")
    # 2026-08-30 Sunday, 2026-09-04 Friday, 2026-09-05 Saturday.
    got = con.execute(
        "SELECT EXTRACT(DOW FROM DATE '2026-08-30'), "
        "       EXTRACT(DOW FROM DATE '2026-09-04'), "
        "       EXTRACT(DOW FROM DATE '2026-09-05')").fetchone()
    con.close()
    check(int(got[0]) == 0, "Sunday 2026-08-30 -> DOW %d (expected 0)" % got[0])
    check(int(got[1]) == 5, "Friday 2026-09-04 -> DOW %d (expected 5)" % got[1])
    check(int(got[2]) == 6, "Saturday 2026-09-05 -> DOW %d (expected 6)" % got[2])
    check(model._is_weekend(int(got[0])) == 1, "DuckDB Sunday classified as weekend")
    check(model._is_weekend(int(got[1])) == 0, "DuckDB Friday classified as weekday")
    check(model._is_weekend(int(got[2])) == 1, "DuckDB Saturday classified as weekend")


# ------------------------------------------------- B. destination weather
class _Recorder:
    """Stands in for a fitted estimator and captures the feature row it is handed."""

    def __init__(self, out):
        self.out = out

    def predict(self, X):
        self.out.append(X.iloc[0].to_dict())
        return [1.0]   # valid for both: 1 min of delay, and crowd class 1 (SDA)


def _seed_db(path, dest_area, dest_rainy, other_area, other_rainy):
    """The destination's forecast is deliberately OLDER than the other area's."""
    con = duckdb.connect(str(path))
    con.execute("""CREATE TABLE calendar_events (
        event_id VARCHAR PRIMARY KEY, title VARCHAR, start_time TIMESTAMPTZ,
        dest_lat DOUBLE, dest_lng DOUBLE, nearest_weather_area VARCHAR)""")
    con.execute("""CREATE TABLE route_options (
        option_id VARCHAR PRIMARY KEY, event_id VARCHAR, total_duration_min INTEGER,
        walk_distance_m INTEGER, num_transfers INTEGER, fare DOUBLE)""")
    con.execute("""CREATE TABLE route_legs (
        option_id VARCHAR, leg_sequence INTEGER, mode VARCHAR, service_no VARCHAR,
        from_name VARCHAR, to_name VARCHAR)""")
    con.execute("""CREATE TABLE weather_forecast (
        area VARCHAR, forecast VARCHAR, is_rainy BOOLEAN, fetched_at TIMESTAMP)""")
    con.execute("""CREATE TABLE recommendations (
        event_id VARCHAR PRIMARY KEY, leave_by TIMESTAMPTZ)""")
    con.execute("""CREATE TABLE bus_arrivals (
        bus_stop_code VARCHAR, service_no VARCHAR, next_bus_mins INTEGER,
        next_bus2_mins INTEGER, load VARCHAR, fetched_at TIMESTAMP)""")
    con.execute("""CREATE TABLE predictions (
        prediction_id VARCHAR PRIMARY KEY, event_id VARCHAR, option_id VARCHAR,
        predicted_min INTEGER, predicted_delay_min INTEGER, predicted_crowd VARCHAR,
        actual_min INTEGER, actual_crowd VARCHAR, actual_source VARCHAR,
        boarding_stop_code VARCHAR, alighting_stop_code VARCHAR,
        transit_service_no VARCHAR, model_version VARCHAR, mae_7day DOUBLE,
        predicted_at TIMESTAMP)""")

    start = datetime.now(timezone.utc) + timedelta(hours=3)
    con.execute("INSERT INTO calendar_events VALUES (?,?,?,?,?,?)",
                ["EV1", "Test event", start, 1.35, 103.82, dest_area])
    con.execute("INSERT INTO route_options VALUES (?,?,?,?,?,?)",
                ["EV1_opt1", "EV1", 40, 600, 1, 1.7])
    con.execute("INSERT INTO route_legs VALUES (?,?,?,?,?,?)",
                ["EV1_opt1", 1, "MRT", "NS", "A", "B"])
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute("INSERT INTO weather_forecast VALUES (?,?,?,?)",
                [dest_area, "Destination forecast", dest_rainy, now - timedelta(hours=2)])
    con.execute("INSERT INTO weather_forecast VALUES (?,?,?,?)",
                [other_area, "Elsewhere forecast", other_rainy, now])  # newest globally
    return con


def _capture_features(dest_area, dest_rainy, other_area, other_rainy):
    captured = []
    tmp = Path(tempfile.mkdtemp()) / "wx_test.duckdb"
    con = _seed_db(tmp, dest_area, dest_rainy, other_area, other_rainy)
    real_load, real_exists = model.joblib.load, Path.exists
    try:
        model.joblib.load = lambda _p: _Recorder(captured)
        Path.exists = lambda self: True if str(self).endswith(".pkl") else real_exists(self)
        model.predict(con)
    finally:
        model.joblib.load = real_load
        Path.exists = real_exists
        con.close()
    return captured


def test_destination_weather_used():
    print("\n[B1] Inference reads the DESTINATION area's forecast, not the newest row")
    # Destination dry, but the newest row globally (a different area) is rainy.
    cap = _capture_features("Bishan", False, "Tuas", True)
    check(len(cap) >= 1, "predict() scored at least one route option")
    if cap:
        dur = cap[0]
        check(dur["is_rainy"] == 0,
              "destination dry + elsewhere rainy -> is_rainy=%s (expected 0)" % dur["is_rainy"])
        check(dur["rain_exposure"] == 0,
              "rain_exposure follows is_rainy -> %s (expected 0)" % dur["rain_exposure"])

    # Inverse: destination rainy, newest row globally dry.
    cap2 = _capture_features("Bishan", True, "Tuas", False)
    if cap2:
        dur2 = cap2[0]
        check(dur2["is_rainy"] == 1,
              "destination rainy + elsewhere dry -> is_rainy=%s (expected 1)" % dur2["is_rainy"])
        check(dur2["rain_exposure"] > 0,
              "rain_exposure scales with wet walking -> %.3f km" % dur2["rain_exposure"])


def test_walk_route_gets_no_crowd_class():
    print("\n[C1] A walk-only route is scored for delay but given NO crowd class")
    captured = []
    tmp = Path(tempfile.mkdtemp()) / "walk_test.duckdb"
    con = _seed_db(tmp, "Bishan", False, "Tuas", True)
    # Replace the MRT leg with a walk-only itinerary.
    con.execute("DELETE FROM route_legs")
    con.execute("INSERT INTO route_legs VALUES (?,?,?,?,?,?)",
                ["EV1_opt1", 1, "WALK", None, "Origin", "Destination"])
    real_load, real_exists = model.joblib.load, Path.exists
    try:
        model.joblib.load = lambda _p: _Recorder(captured)
        Path.exists = lambda self: True if str(self).endswith(".pkl") else real_exists(self)
        model.predict(con)
        row = con.execute(
            "SELECT predicted_min, predicted_delay_min, predicted_crowd "
            "FROM predictions WHERE prediction_id = 'EV1_opt1_pred'").fetchone()
    finally:
        model.joblib.load = real_load
        Path.exists = real_exists
        con.close()

    check(row is not None, "a prediction row was written for the walk-only route")
    if row:
        check(row[1] is not None, "walk route still receives a delay prediction (%s min)" % row[1])
        check(row[0] is not None, "walk route still receives a total (%s min)" % row[0])
        check(row[2] is None,
              "walk route predicted_crowd is NULL (got %r) -- no vehicle to be full" % (row[2],))
    check(len(captured) == 1,
          "only the DELAY model was invoked for a walk route, not the crowd model "
          "(%d model call(s))" % len(captured))


def test_train_and_predict_agree_on_weather():
    print("\n[B2] Training and serving use the same weather predicate")
    src = (Path(__file__).parent.parent / "scripts" / "model.py").read_text(encoding="utf-8")
    predicate = "WHERE w.area = e.nearest_weather_area"
    check(src.count(predicate) == 3,
          "all 3 weather lookups (train-duration, train-crowd, predict) are area-scoped "
          "-- found %d" % src.count(predicate))
    check("ORDER BY fetched_at DESC LIMIT 1) AS is_rainy" not in src,
          "no unscoped globally-latest weather lookup remains")


if __name__ == "__main__":
    print("=" * 70)
    print("model.py feature-correctness tests")
    print("=" * 70)
    test_weekend_mapping()
    test_rush_hour_mapping()
    test_inference_row_matches_convention()
    test_training_row_matches_convention()
    test_duckdb_dow_domain()
    test_destination_weather_used()
    test_walk_route_gets_no_crowd_class()
    test_train_and_predict_agree_on_weather()
    print("\n" + "=" * 70)
    if failures:
        print("FAILED: %d check(s)" % len(failures))
        for f in failures:
            print("  - " + f)
        sys.exit(1)
    print("ALL CHECKS PASSED")
