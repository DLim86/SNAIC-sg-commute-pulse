import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "db" / "commute.duckdb"
MODEL_PATH_DUR = Path(__file__).parent.parent / "models" / "commute_predictor.pkl"
MODEL_PATH_CRD = Path(__file__).parent.parent / "models" / "crowd_predictor.pkl"
BUS_STOPS_PATH = Path(__file__).parent.parent / "data" / "raw" / "bus_stops" / "bus_stops.parquet"
SGT = timezone(timedelta(hours=8))

# The duration model predicts a DELAY on top of OneMap's scheduled duration, not
# the total journey time. Predicting the total made base_duration a near-perfect
# predictor of its own target (importance 0.87) — the model was re-deriving
# OneMap's number rather than adding anything. Modelling the residual removes
# that: base_duration stays as context for how much exposure a journey has, but
# it no longer contains the answer.
#
#     predicted_total = OneMap baseline + predicted delay
#
# This also removes the hard prediction ceiling. The total is anchored to
# OneMap's figure, so a 125-minute route can never be capped at the top of the
# training range the way a total-duration model was.
FEATURE_COLS_DUR = [
    "base_duration", "next_bus_mins", "next_bus2_mins", "bus_headway_gap",
    "walk_distance_m", "num_transfers",
    "is_rainy", "rain_exposure", "rush_hour", "is_weekend",
    "hour_of_day", "day_of_week", "bus_crowd_score",
]
MODEL_VERSION = "rf_delay_v2"
FEATURE_COLS_CRD = [
    "leave_hour", "day_of_week", "is_rainy", "rush_hour",
    "next_bus_mins", "next_bus2_mins", "bus_headway_gap",
    "walk_distance_m", "num_transfers", "base_duration",
]
CROWD_MAP = {"SEA": 0, "SDA": 1, "LSD": 2}
CROWD_INV = {0: "SEA", 1: "SDA", 2: "LSD"}
DRIFT_THRESHOLD_MIN = 10


# DuckDB EXTRACT(DOW) is 0=Sunday .. 6=Saturday, so the weekend is {0, 6}.
# `dow >= 5` would call Sunday a weekday and Friday a weekend.
def _is_weekend(dow):
    return 1 if int(dow) in (0, 6) else 0


def _rush_hour(hour, dow):
    return 1 if (hour in [7, 8, 17, 18, 19] and not _is_weekend(dow)) else 0


def _build_duration_row(base_duration, next_bus_mins, next_bus2_mins, walk_m, transfers,
                         is_rainy, hour, dow, crowd_score):
    rain = int(bool(is_rainy))
    rain_exposure = rain * walk_m / 1000
    rush = _rush_hour(hour, dow)
    weekend = _is_weekend(dow)
    gap = max(0, (next_bus2_mins or 0) - (next_bus_mins or 0))
    return [base_duration, next_bus_mins, next_bus2_mins, gap, walk_m, transfers,
            rain, rain_exposure, rush, weekend, hour, dow, crowd_score]


def _build_crowd_row(leave_hour, dow, is_rainy, rush_hour_flag,
                     next_bus_mins=5, next_bus2_mins=5,
                     walk_m=0, transfers=0, base_dur=30):
    gap = max(0, (next_bus2_mins or 0) - (next_bus_mins or 0))
    return [leave_hour, dow, int(bool(is_rainy)), rush_hour_flag,
            next_bus_mins or 5, next_bus2_mins or 5, gap,
            walk_m or 0, transfers or 0, base_dur or 30]


def _match_stop_name(to_name: str, stops_df: pd.DataFrame):
    """Return BusStopCode string matching the given stop name, or None if no match."""
    if not to_name or stops_df.empty:
        return None
    norm = to_name.lower().strip()
    desc_lower = stops_df["Description"].str.lower().str.strip()
    mask = desc_lower == norm
    if mask.any():
        return str(stops_df.loc[mask.idxmax(), "BusStopCode"]).split(".")[0]
    prefix = norm[:15]
    if len(prefix) >= 5:
        mask2 = desc_lower.str.contains(prefix, na=False, regex=False)
        if mask2.any():
            return str(stops_df.loc[mask2.idxmax(), "BusStopCode"]).split(".")[0]
    return None


def _bootstrap_synthetic_duration(n=500, seed=42):
    """SYNTHETIC cold-start data. Target is delay_min, not total duration.

    These rows are generated from documented Singapore commuting patterns, never
    persisted to the database, and exist only so the model produces sensible
    output before real outcomes accumulate. They are not observations.
    """
    rng = np.random.default_rng(seed)
    rows = []
    for _ in range(n):
        base = int(rng.integers(10, 150))     # widened: real routes run well past 60 min
        hour = int(rng.integers(6, 23))
        dow = int(rng.integers(0, 7))
        walk_m = int(rng.integers(0, 1500))
        transfers = int(rng.integers(0, 3))
        is_rainy = rng.random() < 0.25
        crowd_raw = rng.choice(["SEA", "SDA", "LSD"], p=[0.5, 0.35, 0.15])
        crowd_score = CROWD_MAP.get(crowd_raw, 0)
        next_bus = int(rng.integers(1, 20))
        next_bus2 = int(next_bus + rng.integers(3, 18))
        gap = next_bus2 - next_bus

        # Delay is driven by waiting and exposure, not by how long you sit on the
        # train — so it scales with the live context, only weakly with base.
        delay = 0.0
        delay += next_bus * 0.6                       # you wait for the bus you can see
        if gap > 8:
            delay += 4                                # irregular service bunches
        if _rush_hour(hour, dow):
            delay += 6 + base * 0.05
        if is_rainy:
            delay += 4 + walk_m / 400                 # wet walking is slower
        if _is_weekend(dow):
            delay -= 3                                # weekend roads are clearer
        delay += transfers * 2.5                      # each change costs a wait
        if crowd_raw == "LSD":
            delay += 3                                # slower boarding when full
        delay += rng.normal(0, 2.5)                   # irreducible noise
        delay = float(max(0.0, delay))

        rows.append({
            "base_duration": base, "next_bus_mins": next_bus,
            "next_bus2_mins": next_bus2, "bus_headway_gap": gap,
            "walk_distance_m": walk_m, "num_transfers": transfers,
            "is_rainy": int(is_rainy), "rain_exposure": int(is_rainy) * walk_m / 1000,
            "rush_hour": _rush_hour(hour, dow), "is_weekend": _is_weekend(dow),
            "hour_of_day": hour, "day_of_week": dow, "bus_crowd_score": crowd_score,
            "delay_min": delay,
        })
    return pd.DataFrame(rows)


def _bootstrap_synthetic_crowd(n=500, seed=43):
    rng = np.random.default_rng(seed)
    rows = []
    for _ in range(n):
        hour = rng.integers(6, 23)
        dow = rng.integers(0, 7)
        is_rainy = rng.random() < 0.25
        rush = _rush_hour(hour, dow)
        next_bus = int(rng.integers(1, 20))
        next_bus2 = int(next_bus + rng.integers(3, 15))
        gap = next_bus2 - next_bus
        large_gap = gap > 8
        walk_m = int(rng.integers(0, 1200))
        transfers = int(rng.integers(0, 3))
        base_dur = int(rng.integers(10, 150))

        if rush and is_rainy:
            crowd = rng.choice([0, 1, 2], p=[0.10, 0.40, 0.50])
        elif rush and large_gap:
            crowd = rng.choice([0, 1, 2], p=[0.05, 0.45, 0.50])
        elif rush:
            crowd = rng.choice([0, 1, 2], p=[0.10, 0.70, 0.20])
        elif is_rainy:
            crowd = rng.choice([0, 1, 2], p=[0.30, 0.50, 0.20])
        elif large_gap:
            crowd = rng.choice([0, 1, 2], p=[0.30, 0.45, 0.25])
        else:
            crowd = rng.choice([0, 1, 2], p=[0.80, 0.15, 0.05])

        rows.append({
            "leave_hour": hour, "day_of_week": dow,
            "is_rainy": int(is_rainy), "rush_hour": rush,
            "next_bus_mins": next_bus, "next_bus2_mins": next_bus2,
            "bus_headway_gap": gap,
            "walk_distance_m": walk_m, "num_transfers": transfers,
            "base_duration": base_dur, "actual_crowd_int": int(crowd),
        })
    return pd.DataFrame(rows)


def train(con):
    Path(MODEL_PATH_DUR.parent).mkdir(parents=True, exist_ok=True)

    real_dur_rows = con.execute("""
        SELECT p.actual_min,
               r.total_duration_min, r.walk_distance_m, r.num_transfers,
               b.next_bus_mins, b.next_bus2_mins, b.load,
               CAST(EXTRACT(HOUR FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS hour_of_day,
               CAST(EXTRACT(DOW FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS day_of_week,
               COALESCE((SELECT w.is_rainy FROM weather_forecast w
                         WHERE w.area = e.nearest_weather_area
                         ORDER BY w.fetched_at DESC LIMIT 1), FALSE) AS is_rainy
        FROM predictions p
        JOIN calendar_events e ON p.event_id = e.event_id
        JOIN route_options r ON r.option_id = p.option_id
        LEFT JOIN bus_arrivals b
               ON b.bus_stop_code = p.boarding_stop_code
              AND b.service_no    = p.transit_service_no
              AND b.fetched_at    = (
                  SELECT MAX(fetched_at) FROM bus_arrivals ba
                  WHERE ba.bus_stop_code = p.boarding_stop_code
                    AND ba.service_no    = p.transit_service_no
                    AND ba.fetched_at   <= e.start_time
              )
        WHERE p.actual_min IS NOT NULL
          AND p.option_id IS NOT NULL
    """).df()

    synth_dur = _bootstrap_synthetic_duration(n=500)
    n_real_dur = len(real_dur_rows)

    if n_real_dur > 0:
        real_dur_rows["bus_crowd_score"] = real_dur_rows["load"].map(CROWD_MAP).fillna(0)
        real_dur_rows = real_dur_rows.rename(columns={"total_duration_min": "base_duration"})
        real_dur_rows["next_bus_mins"] = real_dur_rows["next_bus_mins"].fillna(5)
        real_dur_rows["next_bus2_mins"] = real_dur_rows["next_bus2_mins"].fillna(5)
        real_dur_rows["bus_headway_gap"] = (
            real_dur_rows["next_bus2_mins"] - real_dur_rows["next_bus_mins"]
        ).clip(lower=0)
        real_dur_rows["is_rainy"] = real_dur_rows["is_rainy"].fillna(False).astype(int)
        real_dur_rows["rain_exposure"] = real_dur_rows["is_rainy"] * real_dur_rows["walk_distance_m"] / 1000
        real_dur_rows["rush_hour"] = real_dur_rows.apply(lambda r: _rush_hour(r["hour_of_day"], r["day_of_week"]), axis=1)
        real_dur_rows["is_weekend"] = real_dur_rows["day_of_week"].apply(_is_weekend)
        # Residual target: how much longer the journey ran than OneMap's schedule.
        real_dur_rows["delay_min"] = (
            real_dur_rows["actual_min"] - real_dur_rows["base_duration"]
        ).clip(lower=0, upper=90)
        X_dur = pd.concat([real_dur_rows[FEATURE_COLS_DUR], synth_dur[FEATURE_COLS_DUR]], ignore_index=True)
        y_dur = pd.concat([real_dur_rows["delay_min"], synth_dur["delay_min"]], ignore_index=True)
    else:
        X_dur = synth_dur[FEATURE_COLS_DUR]
        y_dur = synth_dur["delay_min"]

    dur_model = RandomForestRegressor(n_estimators=100, random_state=42)
    dur_model.fit(X_dur, y_dur)
    joblib.dump(dur_model, MODEL_PATH_DUR)
    importances = sorted(zip(FEATURE_COLS_DUR, dur_model.feature_importances_), key=lambda x: -x[1])
    log.info("Delay model trained on %d real + 500 SYNTHETIC bootstrap rows "
             "(target = delay over OneMap baseline)", n_real_dur)
    log.info("Top features: %s", ", ".join(f"{k}={v:.3f}" for k, v in importances[:5]))

    real_crd_rows = con.execute("""
        SELECT p.actual_crowd,
               CAST(EXTRACT(HOUR FROM rec.leave_by AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS leave_hour,
               CAST(EXTRACT(DOW FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS day_of_week,
               COALESCE((SELECT w.is_rainy FROM weather_forecast w
                         WHERE w.area = e.nearest_weather_area
                         ORDER BY w.fetched_at DESC LIMIT 1), FALSE) AS is_rainy,
               COALESCE(b.next_bus_mins, 5)       AS next_bus_mins,
               COALESCE(b.next_bus2_mins, 5)      AS next_bus2_mins,
               COALESCE(r.walk_distance_m, 0)     AS walk_distance_m,
               COALESCE(r.num_transfers, 0)       AS num_transfers,
               COALESCE(r.total_duration_min, 30) AS base_duration
        FROM predictions p
        JOIN calendar_events e   ON p.event_id = e.event_id
        JOIN recommendations rec ON rec.event_id = p.event_id
        LEFT JOIN route_options r ON r.option_id = p.option_id
        LEFT JOIN bus_arrivals b
               ON b.bus_stop_code = p.boarding_stop_code
              AND b.service_no    = p.transit_service_no
              AND b.fetched_at    = (
                  SELECT MAX(fetched_at) FROM bus_arrivals ba
                  WHERE ba.bus_stop_code = p.boarding_stop_code
                    AND ba.service_no    = p.transit_service_no
                    AND ba.fetched_at   <= e.start_time
              )
        WHERE p.actual_crowd IS NOT NULL
    """).df()

    synth_crd = _bootstrap_synthetic_crowd(n=500)
    n_real_crd = len(real_crd_rows)

    if n_real_crd > 0:
        real_crd_rows["actual_crowd_int"] = real_crd_rows["actual_crowd"].map(CROWD_MAP).fillna(0).astype(int)
        real_crd_rows["rush_hour"] = real_crd_rows.apply(lambda r: _rush_hour(r["leave_hour"], r["day_of_week"]), axis=1)
        real_crd_rows["is_rainy"] = real_crd_rows["is_rainy"].fillna(False).astype(int)
        real_crd_rows["bus_headway_gap"] = (
            real_crd_rows["next_bus2_mins"] - real_crd_rows["next_bus_mins"]
        ).clip(lower=0)
        X_crd = pd.concat([real_crd_rows[FEATURE_COLS_CRD], synth_crd[FEATURE_COLS_CRD]], ignore_index=True)
        y_crd = pd.concat([real_crd_rows["actual_crowd_int"], synth_crd["actual_crowd_int"]], ignore_index=True)
    else:
        X_crd = synth_crd[FEATURE_COLS_CRD]
        y_crd = synth_crd["actual_crowd_int"]

    crd_model = RandomForestClassifier(n_estimators=100, random_state=42)
    crd_model.fit(X_crd, y_crd)
    joblib.dump(crd_model, MODEL_PATH_CRD)
    crd_imp = sorted(zip(FEATURE_COLS_CRD, crd_model.feature_importances_), key=lambda x: -x[1])
    log.info("Crowd model trained on %d real (observed LTA Load) + 500 SYNTHETIC bootstrap rows",
             n_real_crd)
    log.info("Top features: %s", ", ".join(f"{k}={v:.3f}" for k, v in crd_imp[:5]))


def predict(con):
    if not MODEL_PATH_DUR.exists() or not MODEL_PATH_CRD.exists():
        log.warning("Models not found — run --train first")
        return

    dur_model = joblib.load(MODEL_PATH_DUR)
    crd_model = joblib.load(MODEL_PATH_CRD)

    stops_df = pd.DataFrame()
    if BUS_STOPS_PATH.exists():
        stops_df = pd.read_parquet(BUS_STOPS_PATH).dropna(subset=["BusStopCode", "Description"])

    rows = con.execute("""
        SELECT r.option_id, r.event_id, r.total_duration_min, r.walk_distance_m, r.num_transfers,
               CAST(EXTRACT(HOUR FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS hour_of_day,
               CAST(EXTRACT(DOW FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS day_of_week,
               COALESCE((SELECT w.is_rainy FROM weather_forecast w
                         WHERE w.area = e.nearest_weather_area
                         ORDER BY w.fetched_at DESC LIMIT 1), FALSE) AS is_rainy,
               rec.leave_by
        FROM route_options r
        JOIN calendar_events e ON r.event_id = e.event_id
        LEFT JOIN recommendations rec ON rec.event_id = r.event_id
        WHERE e.start_time > NOW()
        ORDER BY e.start_time, r.total_duration_min
    """).fetchall()

    if not rows:
        log.warning("No upcoming event found — run ingest.py first")
        return

    for row in rows:
        (option_id, event_id, base_dur, walk_m, transfers,
         hour, dow, is_rainy, leave_by) = row

        is_rainy_int = int(bool(is_rainy))
        next_bus_mins = 5
        next_bus2_mins_val = 5
        crowd_score = 0
        boarding_stop_code = None
        transit_service_no = None
        alighting_stop_code = None

        leg = con.execute("""
            SELECT mode, service_no, from_name, to_name FROM route_legs
            WHERE option_id = ? AND mode IN ('BUS', 'MRT', 'LRT')
            ORDER BY leg_sequence LIMIT 1
        """, [option_id]).fetchone()

        if leg:
            transit_mode, transit_service_no, from_name, to_name = leg
            if transit_mode == "BUS":
                ba = con.execute("""
                    SELECT next_bus_mins, next_bus2_mins, load, bus_stop_code FROM bus_arrivals
                    WHERE service_no = ?
                      AND fetched_at = (SELECT MAX(fetched_at) FROM bus_arrivals)
                    LIMIT 1
                """, [transit_service_no]).fetchone()
                if ba:
                    next_bus_mins      = ba[0] or 5
                    next_bus2_mins_val = ba[1] or 5
                    crowd_score        = CROWD_MAP.get(ba[2] or "SEA", 0)
                    boarding_stop_code = ba[3]
                alighting_stop_code = _match_stop_name(to_name, stops_df)

        dur_vals = _build_duration_row(
            base_dur, next_bus_mins, next_bus2_mins_val, walk_m or 0, transfers or 0,
            is_rainy_int, hour, dow, crowd_score
        )
        X_dur = pd.DataFrame([dur_vals], columns=FEATURE_COLS_DUR)
        predicted_delay = max(0, int(round(dur_model.predict(X_dur)[0])))
        predicted_min = int(base_dur or 0) + predicted_delay

        # Crowding only means something where you board a vehicle. A walking
        # route has no load to predict, so leave it NULL rather than inventing one.
        predicted_crowd = None
        if leg:
            leave_hour_val = hour
            if leave_by is not None:
                try:
                    lb = leave_by.astimezone(SGT) if hasattr(leave_by, "astimezone") else leave_by
                    leave_hour_val = lb.hour
                except Exception:
                    pass
            rush_flag = _rush_hour(leave_hour_val, dow)
            X_crd = pd.DataFrame(
                [_build_crowd_row(leave_hour_val, dow, is_rainy_int, rush_flag,
                                  next_bus_mins, next_bus2_mins_val,
                                  walk_m or 0, transfers or 0, base_dur)],
                columns=FEATURE_COLS_CRD,
            )
            predicted_crowd = CROWD_INV[int(crd_model.predict(X_crd)[0])]

        prediction_id = f"{option_id}_pred"
        con.execute("""
            INSERT INTO predictions (prediction_id, event_id, option_id, predicted_min,
                                      predicted_delay_min, predicted_crowd,
                                      boarding_stop_code, alighting_stop_code, transit_service_no,
                                      model_version, predicted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (prediction_id) DO UPDATE SET
                predicted_min = excluded.predicted_min,
                predicted_delay_min = excluded.predicted_delay_min,
                predicted_crowd = excluded.predicted_crowd,
                boarding_stop_code = excluded.boarding_stop_code,
                alighting_stop_code = excluded.alighting_stop_code,
                transit_service_no = excluded.transit_service_no,
                model_version = excluded.model_version,
                predicted_at = excluded.predicted_at
        """, [prediction_id, event_id, option_id, predicted_min, predicted_delay,
               predicted_crowd, boarding_stop_code, alighting_stop_code,
               transit_service_no, MODEL_VERSION])

        log.info("Prediction [%s]: %d min baseline + %d min predicted delay = %d min | crowd: %s",
                 option_id, base_dur or 0, predicted_delay, predicted_min,
                 predicted_crowd or "n/a (walking route)")


def backfill(con):
    """Reconstruct an outcome estimate for events that have already happened.

    IMPORTANT — this is NOT GPS ground truth. There is no trip-tracking in this
    system, so a true door-to-door journey time is not observable. What is
    reconstructed here is:

        OneMap scheduled duration
      + the wait actually observed at the boarding stop (BUS only — real LTA data)
      + a published headway allowance where no live feed exists (MRT / LRT)
      + a disruption penalty when LTA reported a HEAVY alert on the route's line
      = reconstructed outcome estimate

    Every row records which of those paths produced it in `actual_source`, so the
    proxy is never mistaken for an observation.

    A previous version called LTA's arrival API at the ALIGHTING stop and treated
    "minutes until the next bus reaches that stop" as journey duration. That is
    not a duration — it produced a 40-minute "actual" for a 125-minute route — so
    that path has been removed rather than patched.
    """
    MRT_HEADWAY_MIN = 4
    LRT_HEADWAY_MIN = 7
    DISRUPTION_PENALTY_MIN = 20

    past = con.execute("""
        SELECT p.prediction_id, p.event_id, p.option_id, p.actual_min, p.actual_crowd,
               p.boarding_stop_code, p.transit_service_no,
               e.start_time, r.total_duration_min
        FROM predictions p
        JOIN calendar_events e ON p.event_id = e.event_id
        LEFT JOIN route_options r ON r.option_id = p.option_id
        WHERE e.start_time < NOW()
          AND (p.actual_min IS NULL OR p.actual_crowd IS NULL)
    """).fetchall()

    if not past:
        log.info("No past events needing backfill")
        return

    for (prediction_id, event_id, option_id, actual_min, actual_crowd,
         boarding_stop_code, transit_service_no, start_time, total_dur) in past:

        base = total_dur or 0

        if actual_min is None:
            dominant_mode = "WALK"
            if option_id:
                mode_row = con.execute("""
                    SELECT mode FROM route_legs
                    WHERE option_id = ? AND mode IN ('BUS', 'MRT', 'LRT')
                    ORDER BY leg_sequence LIMIT 1
                """, [option_id]).fetchone()
                dominant_mode = mode_row[0] if mode_row else "WALK"

            # Disruption penalty applies to any rail leg on the route.
            disruption = 0
            if dominant_mode in ("MRT", "LRT") and transit_service_no and start_time:
                dis_row = con.execute("""
                    SELECT COUNT(*) FROM train_alerts
                    WHERE affected_line = ? AND severity = 'HEAVY'
                      AND fetched_at BETWEEN ? - INTERVAL '30 minutes'
                                         AND ? + INTERVAL '3 hours'
                """, [transit_service_no, start_time, start_time]).fetchone()
                if dis_row and dis_row[0] > 0:
                    disruption = DISRUPTION_PENALTY_MIN

            if dominant_mode == "BUS":
                # The only genuinely observed component: what the live LTA feed
                # said the wait was at the boarding stop, nearest in time to departure.
                wait_row = None
                if boarding_stop_code and transit_service_no:
                    wait_row = con.execute("""
                        SELECT next_bus_mins FROM bus_arrivals
                        WHERE bus_stop_code = ? AND service_no = ?
                        ORDER BY ABS(DATEDIFF('minute', fetched_at, ?))
                        LIMIT 1
                    """, [boarding_stop_code, transit_service_no, start_time]).fetchone()
                if wait_row and wait_row[0] is not None:
                    computed_actual = base + int(wait_row[0])
                    source = "observed_boarding_wait"
                else:
                    computed_actual = base
                    source = "schedule_only"

            elif dominant_mode in ("MRT", "LRT"):
                headway = MRT_HEADWAY_MIN if dominant_mode == "MRT" else LRT_HEADWAY_MIN
                computed_actual = base + headway + disruption
                source = "headway_allowance_disrupted" if disruption else "headway_allowance"

            else:
                computed_actual = base
                source = "deterministic_walk"

            con.execute(
                "UPDATE predictions SET actual_min = ?, actual_source = ? WHERE prediction_id = ?",
                [computed_actual, source, prediction_id],
            )
            log.info("Backfill [%s] %s: %d min baseline -> %d min reconstructed (%s)",
                     prediction_id, dominant_mode, base, computed_actual, source)

        # actual_crowd IS a real observation — LTA's Load field for the boarding
        # stop and service. Only bus services report it.
        if actual_crowd is None and transit_service_no and boarding_stop_code:
            crd = con.execute("""
                SELECT load FROM bus_arrivals
                WHERE bus_stop_code = ? AND service_no = ?
                ORDER BY ABS(DATEDIFF('minute', fetched_at, ?))
                LIMIT 1
            """, [boarding_stop_code, transit_service_no, start_time]).fetchone()
            if crd and crd[0]:
                con.execute("UPDATE predictions SET actual_crowd = ? WHERE prediction_id = ?",
                            [crd[0], prediction_id])
                log.info("Observed actual_crowd=%s for %s (LTA Load at stop %s)",
                         crd[0], prediction_id, boarding_stop_code)


def evaluate(con):
    import time as _time
    t0 = _time.time()

    rows = con.execute("""
        SELECT p.predicted_min, p.actual_min, p.predicted_crowd, p.actual_crowd,
               r.total_duration_min
        FROM predictions p
        LEFT JOIN route_options r ON r.option_id = p.option_id
        WHERE p.predicted_at > NOW() - INTERVAL '7 days'
          AND p.actual_min IS NOT NULL
    """).fetchall()

    if len(rows) < 7:
        log.warning("Only %d actuals in last 7 days — need 7 for evaluation, skipping", len(rows))
        return

    preds_min = np.array([r[0] for r in rows])
    actuals_min = np.array([r[1] for r in rows])
    mae = float(np.mean(np.abs(preds_min - actuals_min)))

    # Baseline for comparison: OneMap's scheduled duration with no ML adjustment.
    # If the model is not beating this, the model is not earning its place.
    baseline_pairs = [(r[4], r[1]) for r in rows if r[4] is not None]
    baseline_mae = None
    if baseline_pairs:
        baseline_mae = float(np.mean([abs(b - a) for b, a in baseline_pairs]))

    crowd_rows = [(r[2], r[3]) for r in rows if r[2] is not None and r[3] is not None]
    crowd_acc = None
    if crowd_rows:
        correct = sum(1 for p, a in crowd_rows if p == a)
        crowd_acc = correct / len(crowd_rows)

    con.execute("""
        UPDATE predictions SET mae_7day = ?
        WHERE predicted_at > NOW() - INTERVAL '7 days'
    """, [mae])

    log.info("7-day MAE (model): %.1f min over %d predictions", mae, len(rows))
    if baseline_mae is not None:
        delta = baseline_mae - mae
        verdict = "model improves on baseline" if delta > 0 else "model does NOT beat baseline"
        log.info("7-day MAE (OneMap baseline, no ML): %.1f min — %s by %.1f min",
                 baseline_mae, verdict, abs(delta))
    if mae > DRIFT_THRESHOLD_MIN:
        log.warning(
            "Model drift warning: 7-day MAE %.1f min exceeds %.1f min threshold",
            mae,
            DRIFT_THRESHOLD_MIN,
        )
    if crowd_acc is not None:
        log.info("7-day crowd accuracy: %.0f%% over %d events", crowd_acc * 100, len(crowd_rows))

    summary = f"mae={mae:.2f}min"
    if baseline_mae is not None:
        summary += f" baseline_mae={baseline_mae:.2f}min"
    if crowd_acc is not None:
        summary += f" crowd_acc={crowd_acc * 100:.0f}%"
    con.execute(
        """INSERT INTO pipeline_runs
           (run_id, source, rows_upserted, duration_ms, status, error_msg)
           VALUES (?, 'model_evaluate', ?, ?, 'success', ?)""",
        [str(uuid.uuid4()), len(rows), int((_time.time() - t0) * 1000), summary],
    )


def main():
    parser = argparse.ArgumentParser(description="Commute ML pipeline")
    parser.add_argument("--train", action="store_true", help="Train both models")
    parser.add_argument("--predict", action="store_true", help="Predict next event (all routes)")
    parser.add_argument("--backfill", action="store_true", help="Fill actuals for past events")
    parser.add_argument("--evaluate", action="store_true", help="Compute 7-day MAE + accuracy")
    args = parser.parse_args()

    if not any([args.train, args.predict, args.backfill, args.evaluate]):
        parser.print_help()
        return

    import duckdb
    con = duckdb.connect(str(DB_PATH))
    try:
        if args.train:
            train(con)
        if args.predict:
            predict(con)
        if args.backfill:
            backfill(con)
        if args.evaluate:
            evaluate(con)
    finally:
        con.close()


if __name__ == "__main__":
    main()
