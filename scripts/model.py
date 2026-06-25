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
SGT = timezone(timedelta(hours=8))

FEATURE_COLS_DUR = [
    "base_duration", "next_bus_mins", "walk_distance_m", "num_transfers",
    "is_rainy", "rain_exposure", "rush_hour", "is_weekend",
    "hour_of_day", "day_of_week", "bus_crowd_score",
]
FEATURE_COLS_CRD = [
    "leave_hour", "day_of_week", "is_rainy", "rush_hour",
]
CROWD_MAP = {"SEA": 0, "SDA": 1, "LSD": 2}
CROWD_INV = {0: "SEA", 1: "SDA", 2: "LSD"}


def _rush_hour(hour, dow):
    return 1 if (hour in [7, 8, 17, 18, 19] and dow < 5) else 0


def _build_duration_row(base_duration, next_bus_mins, walk_m, transfers,
                         is_rainy, hour, dow, crowd_score):
    rain = int(bool(is_rainy))
    rain_exposure = rain * walk_m / 1000
    rush = _rush_hour(hour, dow)
    weekend = 1 if dow >= 5 else 0
    return [base_duration, next_bus_mins, walk_m, transfers,
            rain, rain_exposure, rush, weekend, hour, dow, crowd_score]


def _build_crowd_row(leave_hour, dow, is_rainy, rush_hour_flag):
    return [leave_hour, dow, int(bool(is_rainy)), rush_hour_flag]


def _bootstrap_synthetic_duration(n=500, seed=42):
    rng = np.random.default_rng(seed)
    rows = []
    now = datetime.now(timezone.utc)
    for i in range(n):
        base = rng.integers(15, 60)
        hour = rng.integers(6, 23)
        dow = rng.integers(0, 7)
        walk_m = rng.integers(0, 1200)
        transfers = rng.integers(0, 3)
        is_rainy = rng.random() < 0.25
        crowd_raw = rng.choice(["SEA", "SDA", "LSD"], p=[0.5, 0.35, 0.15])
        crowd_score = CROWD_MAP.get(crowd_raw, 0)
        next_bus = rng.integers(1, 20)

        overhead = 0.0
        if is_rainy:
            overhead += base * 0.08 + 5
        if _rush_hour(hour, dow):
            overhead += base * 0.15
        if dow >= 5:
            overhead -= base * 0.05
        if next_bus > 10:
            overhead += 8
        if crowd_raw == "LSD":
            overhead += 3
        actual = max(int(base + overhead), base)

        days_ago = rng.integers(1, 30)
        predicted_at = (now - timedelta(days=int(days_ago))).replace(tzinfo=None)

        rows.append({
            "base_duration": base, "next_bus_mins": next_bus,
            "walk_distance_m": walk_m, "num_transfers": transfers,
            "is_rainy": int(is_rainy), "rain_exposure": int(is_rainy) * walk_m / 1000,
            "rush_hour": _rush_hour(hour, dow), "is_weekend": 1 if dow >= 5 else 0,
            "hour_of_day": hour, "day_of_week": dow, "bus_crowd_score": crowd_score,
            "actual_min": actual, "model_version": "synthetic",
            "predicted_at": predicted_at,
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

        if rush and is_rainy:
            crowd = rng.choice([0, 1, 2], p=[0.10, 0.40, 0.50])
        elif rush:
            crowd = rng.choice([0, 1, 2], p=[0.10, 0.70, 0.20])
        elif is_rainy:
            crowd = rng.choice([0, 1, 2], p=[0.30, 0.50, 0.20])
        else:
            crowd = rng.choice([0, 1, 2], p=[0.80, 0.15, 0.05])

        rows.append({
            "leave_hour": hour, "day_of_week": dow,
            "is_rainy": int(is_rainy), "rush_hour": rush,
            "actual_crowd_int": crowd,
        })
    return pd.DataFrame(rows)


def train(con):
    Path(MODEL_PATH_DUR.parent).mkdir(parents=True, exist_ok=True)

    real_dur_rows = con.execute("""
        SELECT p.actual_min,
               r.total_duration_min, r.walk_distance_m, r.num_transfers,
               b.next_bus_mins, b.load,
               CAST(EXTRACT(HOUR FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS hour_of_day,
               CAST(EXTRACT(DOW FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS day_of_week,
               w.is_rainy
        FROM predictions p
        JOIN calendar_events e ON p.event_id = e.event_id
        JOIN route_options r ON r.event_id = p.event_id
        LEFT JOIN bus_arrivals b ON b.fetched_at = (
            SELECT MAX(fetched_at) FROM bus_arrivals ba
            WHERE ba.fetched_at <= e.start_time
        )
        LEFT JOIN weather_forecast w ON w.fetched_at = (
            SELECT MAX(fetched_at) FROM weather_forecast
        )
        WHERE p.actual_min IS NOT NULL
    """).df()

    synth_dur = _bootstrap_synthetic_duration(n=500)
    n_real_dur = len(real_dur_rows)

    if n_real_dur > 0:
        real_dur_rows["bus_crowd_score"] = real_dur_rows["load"].map(CROWD_MAP).fillna(0)
        real_X = real_dur_rows[FEATURE_COLS_DUR].rename(columns={"total_duration_min": "base_duration"})
        # FEATURE_COLS_DUR uses "base_duration" but real query returns "total_duration_min"
        real_dur_rows = real_dur_rows.rename(columns={"total_duration_min": "base_duration"})
        real_dur_rows["rain_exposure"] = real_dur_rows.get("is_rainy", 0).astype(int) * real_dur_rows["walk_distance_m"] / 1000
        real_dur_rows["rush_hour"] = real_dur_rows.apply(lambda r: _rush_hour(r["hour_of_day"], r["day_of_week"]), axis=1)
        real_dur_rows["is_weekend"] = (real_dur_rows["day_of_week"] >= 5).astype(int)
        real_dur_rows["is_rainy"] = real_dur_rows["is_rainy"].fillna(False).astype(int)
        X_dur = pd.concat([real_dur_rows[FEATURE_COLS_DUR], synth_dur[FEATURE_COLS_DUR]], ignore_index=True)
        y_dur = pd.concat([real_dur_rows["actual_min"], synth_dur["actual_min"]], ignore_index=True)
    else:
        X_dur = synth_dur[FEATURE_COLS_DUR]
        y_dur = synth_dur["actual_min"]

    dur_model = RandomForestRegressor(n_estimators=100, random_state=42)
    dur_model.fit(X_dur, y_dur)
    joblib.dump(dur_model, MODEL_PATH_DUR)
    importances = sorted(zip(FEATURE_COLS_DUR, dur_model.feature_importances_), key=lambda x: -x[1])
    log.info("Duration model trained on %d real + 500 synthetic rows", n_real_dur)
    log.info("Top features: %s", ", ".join(f"{k}={v:.3f}" for k, v in importances[:4]))

    real_crd_rows = con.execute("""
        SELECT p.actual_crowd,
               CAST(EXTRACT(HOUR FROM rec.leave_by AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS leave_hour,
               CAST(EXTRACT(DOW FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS day_of_week,
               w.is_rainy
        FROM predictions p
        JOIN calendar_events e ON p.event_id = e.event_id
        JOIN recommendations rec ON rec.event_id = p.event_id
        LEFT JOIN weather_forecast w ON w.fetched_at = (
            SELECT MAX(fetched_at) FROM weather_forecast
        )
        WHERE p.actual_crowd IS NOT NULL
    """).df()

    synth_crd = _bootstrap_synthetic_crowd(n=500)
    n_real_crd = len(real_crd_rows)

    if n_real_crd > 0:
        real_crd_rows["actual_crowd_int"] = real_crd_rows["actual_crowd"].map(CROWD_MAP).fillna(0).astype(int)
        real_crd_rows["rush_hour"] = real_crd_rows.apply(lambda r: _rush_hour(r["leave_hour"], r["day_of_week"]), axis=1)
        real_crd_rows["is_rainy"] = real_crd_rows["is_rainy"].fillna(False).astype(int)
        X_crd = pd.concat([real_crd_rows[FEATURE_COLS_CRD], synth_crd[FEATURE_COLS_CRD]], ignore_index=True)
        y_crd = pd.concat([real_crd_rows["actual_crowd_int"], synth_crd["actual_crowd_int"]], ignore_index=True)
    else:
        X_crd = synth_crd[FEATURE_COLS_CRD]
        y_crd = synth_crd["actual_crowd_int"]

    crd_model = RandomForestClassifier(n_estimators=100, random_state=42)
    crd_model.fit(X_crd, y_crd)
    joblib.dump(crd_model, MODEL_PATH_CRD)
    log.info("Crowd model trained on %d real + 500 synthetic rows", n_real_crd)


def predict(con):
    if not MODEL_PATH_DUR.exists() or not MODEL_PATH_CRD.exists():
        log.warning("Models not found — run --train first")
        return

    dur_model = joblib.load(MODEL_PATH_DUR)
    crd_model = joblib.load(MODEL_PATH_CRD)

    row = con.execute("""
        SELECT r.event_id, r.total_duration_min, r.walk_distance_m, r.num_transfers,
               b.next_bus_mins, b.load,
               CAST(EXTRACT(HOUR FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS hour_of_day,
               CAST(EXTRACT(DOW FROM e.start_time AT TIME ZONE 'Asia/Singapore') AS INTEGER) AS day_of_week,
               w.is_rainy,
               rec.leave_by
        FROM route_options r
        JOIN calendar_events e ON r.event_id = e.event_id
        LEFT JOIN bus_arrivals b ON b.fetched_at = (SELECT MAX(fetched_at) FROM bus_arrivals)
        LEFT JOIN weather_forecast w ON w.fetched_at = (SELECT MAX(fetched_at) FROM weather_forecast)
        LEFT JOIN recommendations rec ON rec.event_id = r.event_id
        WHERE e.start_time > NOW()
        ORDER BY e.start_time, r.total_duration_min
        LIMIT 1
    """).fetchone()

    if row is None:
        log.warning("No upcoming event found — run ingest.py first")
        return

    (event_id, base_dur, walk_m, transfers, next_bus, load,
     hour, dow, is_rainy, leave_by) = row

    next_bus = next_bus or 5
    crowd_score = CROWD_MAP.get(load or "SEA", 0)
    is_rainy_int = int(bool(is_rainy))

    dur_vals = _build_duration_row(
        base_dur, next_bus, walk_m or 0, transfers or 0,
        is_rainy_int, hour, dow, crowd_score
    )
    X_dur = pd.DataFrame([dur_vals], columns=FEATURE_COLS_DUR)
    predicted_min = int(round(dur_model.predict(X_dur)[0]))

    leave_hour = hour
    if leave_by is not None:
        try:
            lb = leave_by.astimezone(SGT) if hasattr(leave_by, "astimezone") else leave_by
            leave_hour = lb.hour
        except Exception:
            pass
    rush = _rush_hour(leave_hour, dow)
    X_crd = pd.DataFrame([_build_crowd_row(leave_hour, dow, is_rainy_int, rush)],
                          columns=FEATURE_COLS_CRD)
    predicted_crowd = CROWD_INV[int(crd_model.predict(X_crd)[0])]

    prediction_id = f"{event_id}_pred"
    con.execute("""
        INSERT INTO predictions (prediction_id, event_id, predicted_min, predicted_crowd,
                                  model_version, predicted_at)
        VALUES (?, ?, ?, ?, 'rf_v1', CURRENT_TIMESTAMP)
        ON CONFLICT (prediction_id) DO UPDATE SET
            predicted_min = excluded.predicted_min,
            predicted_crowd = excluded.predicted_crowd,
            model_version = excluded.model_version,
            predicted_at = excluded.predicted_at
    """, [prediction_id, event_id, predicted_min, predicted_crowd])

    log.info("Prediction for %s: %d min | crowd: %s", event_id, predicted_min, predicted_crowd)


def backfill(con):
    past = con.execute("""
        SELECT p.prediction_id, p.event_id, p.actual_min, p.actual_crowd,
               e.start_time
        FROM predictions p
        JOIN calendar_events e ON p.event_id = e.event_id
        WHERE e.start_time < NOW()
          AND (p.actual_min IS NULL OR p.actual_crowd IS NULL)
    """).fetchall()

    if not past:
        log.info("No past events needing backfill")
        return

    for prediction_id, event_id, actual_min, actual_crowd, start_time in past:
        if actual_min is None:
            row = con.execute("""
                SELECT r.total_duration_min, b.next_bus_mins
                FROM route_options r
                LEFT JOIN bus_arrivals b ON b.fetched_at = (
                    SELECT MAX(fetched_at) FROM bus_arrivals ba
                    WHERE ba.fetched_at <= ?
                )
                WHERE r.event_id = ?
                ORDER BY r.total_duration_min
                LIMIT 1
            """, [start_time, event_id]).fetchone()
            if row:
                dur, wait = row
                computed = (dur or 0) + (wait or 0)
                con.execute("UPDATE predictions SET actual_min = ? WHERE prediction_id = ?",
                            [computed, prediction_id])
                log.info("Backfilled actual_min=%d for %s", computed, event_id)

        if actual_crowd is None:
            row = con.execute("""
                SELECT rl.service_no
                FROM route_legs rl
                JOIN route_options r ON rl.option_id = r.option_id
                WHERE r.event_id = ?
                  AND rl.mode IN ('BUS', 'MRT', 'LRT')
                ORDER BY rl.leg_sequence
                LIMIT 1
            """, [event_id]).fetchone()
            if row:
                svc = row[0]
                crd = con.execute("""
                    SELECT load FROM bus_arrivals
                    WHERE service_no = ?
                    ORDER BY ABS(DATEDIFF('minute', fetched_at, ?))
                    LIMIT 1
                """, [svc, start_time]).fetchone()
                if crd:
                    con.execute("UPDATE predictions SET actual_crowd = ? WHERE prediction_id = ?",
                                [crd[0], prediction_id])
                    log.info("Backfilled actual_crowd=%s for %s", crd[0], event_id)


def evaluate(con):
    rows = con.execute("""
        SELECT predicted_min, actual_min, predicted_crowd, actual_crowd
        FROM predictions
        WHERE predicted_at > NOW() - INTERVAL '7 days'
          AND actual_min IS NOT NULL
    """).fetchall()

    if len(rows) < 7:
        log.warning("Only %d actuals in last 7 days — need 7 for evaluation, skipping", len(rows))
        return

    preds_min = np.array([r[0] for r in rows])
    actuals_min = np.array([r[1] for r in rows])
    mae = float(np.mean(np.abs(preds_min - actuals_min)))

    crowd_rows = [(r[2], r[3]) for r in rows if r[2] is not None and r[3] is not None]
    crowd_acc = None
    if crowd_rows:
        correct = sum(1 for p, a in crowd_rows if p == a)
        crowd_acc = correct / len(crowd_rows)

    con.execute("""
        UPDATE predictions SET mae_7day = ?
        WHERE predicted_at > NOW() - INTERVAL '7 days'
    """, [mae])

    log.info("7-day MAE (duration): %.1f min over %d events", mae, len(rows))
    if crowd_acc is not None:
        log.info("7-day crowd accuracy: %.0f%% over %d events", crowd_acc * 100, len(crowd_rows))


def main():
    parser = argparse.ArgumentParser(description="Commute ML pipeline")
    parser.add_argument("--train", action="store_true", help="Train both models")
    parser.add_argument("--predict", action="store_true", help="Predict next event")
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
