import sys
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = str(Path(__file__).parent.parent / "db" / "commute.duckdb")

app = FastAPI(
    title="SG Commute Pulse API",
    description="Calendar-aware Singapore commute recommendations with ML predictions.",
    version="1.0.0",
)

# ── SQL queries (mirrors serve.py exactly) ────────────────────────────────────

_NEXT_ROUTE_SQL = """
    SELECT r.option_id, r.event_id, r.title, r.start_time, r.leave_by,
           r.total_duration_min, r.walk_distance_m, r.num_transfers, r.fare,
           r.weather_forecast, r.is_rainy, r.alert_msg, r.recommendation_reason,
           e.location_raw
    FROM v_enriched_routes r
    JOIN calendar_events e ON r.event_id = e.event_id
    WHERE r.route_rank = 1 AND r.start_time > NOW()
    ORDER BY r.start_time
    LIMIT 1
"""

_EVENT_ROUTE_SQL = """
    SELECT r.option_id, r.event_id, r.title, r.start_time, r.leave_by,
           r.total_duration_min, r.walk_distance_m, r.num_transfers, r.fare,
           r.weather_forecast, r.is_rainy, r.alert_msg, r.recommendation_reason,
           e.location_raw
    FROM v_enriched_routes r
    JOIN calendar_events e ON r.event_id = e.event_id
    WHERE r.route_rank = 1 AND r.event_id = ?
"""

_LEGS_SQL = """
    SELECT leg_sequence, mode, service_no, from_name, to_name,
           duration_min, distance_m, num_stops
    FROM route_legs
    WHERE option_id = ?
    ORDER BY leg_sequence
"""

_FIRST_TRANSIT_SQL = """
    SELECT next_bus_mins, next_bus2_mins, load
    FROM bus_arrivals
    WHERE service_no = ?
    ORDER BY fetched_at DESC
    LIMIT 1
"""

_ALT_ROUTES_SQL = """
    SELECT option_id, total_duration_min, fare, num_transfers, walk_distance_m
    FROM route_options
    WHERE event_id = ? AND option_id != ?
    ORDER BY total_duration_min
"""

_PREDICTIONS_SQL = """
    SELECT p.prediction_id, p.option_id, p.predicted_min, p.predicted_crowd,
           p.actual_min, p.mae_7day, p.model_version, p.predicted_at
    FROM predictions p
    JOIN route_options r ON p.option_id = r.option_id
    WHERE r.event_id = ? AND p.option_id IS NOT NULL
    ORDER BY r.total_duration_min
"""

_PIPELINE_STATUS_SQL = """
    SELECT run_id, source, status, rows_upserted, duration_ms, error_msg, ran_at
    FROM pipeline_runs
    ORDER BY ran_at DESC
    LIMIT 10
"""

_ALERTS_SQL = """
    SELECT affected_line, message
    FROM train_alerts
    WHERE severity = 'HEAVY'
      AND fetched_at > NOW() - INTERVAL '30 minutes'
    ORDER BY fetched_at DESC
    LIMIT 5
"""

# ── Pydantic models ───────────────────────────────────────────────────────────

class Leg(BaseModel):
    leg_sequence: int
    mode: str
    service_no: Optional[str] = None
    from_name: str
    to_name: str
    duration_min: int
    distance_m: Optional[int] = None
    num_stops: Optional[int] = None


class LiveArrival(BaseModel):
    next_bus_mins: Optional[int] = None
    next_bus2_mins: Optional[int] = None
    load: Optional[str] = None


class RouteOption(BaseModel):
    option_id: str
    total_duration_min: int
    walk_distance_m: int
    num_transfers: int
    fare: float
    legs: List[Leg]
    live_arrival: Optional[LiveArrival] = None


class RecommendationResponse(BaseModel):
    event_id: str
    title: str
    start_time: str
    location_raw: Optional[str] = None
    leave_by: str
    recommendation_reason: str
    weather_forecast: Optional[str] = None
    is_rainy: bool
    alert_msg: Optional[str] = None
    recommended_route: RouteOption
    alternative_routes: List[RouteOption]


class MLPrediction(BaseModel):
    prediction_id: str
    option_id: str
    predicted_min: Optional[int] = None
    predicted_crowd: Optional[str] = None
    actual_min: Optional[int] = None
    mae_7day: Optional[float] = None
    model_version: Optional[str] = None
    predicted_at: Optional[str] = None


class PipelineRun(BaseModel):
    run_id: str
    source: str
    status: str
    rows_upserted: Optional[int] = None
    duration_ms: Optional[int] = None
    error_msg: Optional[str] = None
    ran_at: str


class Alert(BaseModel):
    affected_line: str
    message: str


# ── DB helper ─────────────────────────────────────────────────────────────────

@contextmanager
def get_db():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        yield con
    finally:
        con.close()


def _build_route_option(con, option_id: str, total_duration_min: int,
                        walk_distance_m: int, num_transfers: int, fare) -> RouteOption:
    legs_rows = con.execute(_LEGS_SQL, [option_id]).fetchall()
    legs = [
        Leg(leg_sequence=r[0], mode=r[1], service_no=r[2], from_name=r[3],
            to_name=r[4], duration_min=r[5], distance_m=r[6], num_stops=r[7])
        for r in legs_rows
    ]
    live = None
    for leg in legs:
        if leg.mode in ("BUS", "MRT", "LRT") and leg.service_no:
            ba = con.execute(_FIRST_TRANSIT_SQL, [leg.service_no]).fetchone()
            if ba:
                live = LiveArrival(next_bus_mins=ba[0], next_bus2_mins=ba[1], load=ba[2])
            break
    return RouteOption(
        option_id=option_id,
        total_duration_min=total_duration_min,
        walk_distance_m=walk_distance_m or 0,
        num_transfers=num_transfers or 0,
        fare=float(fare or 0),
        legs=legs,
        live_arrival=live,
    )


def _build_recommendation(con, row) -> RecommendationResponse:
    option_id, event_id, title, start_time, leave_by = row[0], row[1], row[2], row[3], row[4]
    total_dur, walk_m, transfers, fare = row[5], row[6], row[7], row[8]
    weather_forecast, is_rainy, alert_msg, reason, location_raw = row[9], row[10], row[11], row[12], row[13]

    recommended = _build_route_option(con, option_id, total_dur, walk_m, transfers, fare)

    alt_rows = con.execute(_ALT_ROUTES_SQL, [event_id, option_id]).fetchall()
    alts = [
        _build_route_option(con, ar[0], ar[1], ar[4], ar[3], ar[2])
        for ar in alt_rows
    ]

    return RecommendationResponse(
        event_id=event_id,
        title=title or "",
        start_time=str(start_time),
        location_raw=location_raw,
        leave_by=str(leave_by),
        recommendation_reason=reason or "",
        weather_forecast=weather_forecast,
        is_rainy=bool(is_rainy),
        alert_msg=alert_msg,
        recommended_route=recommended,
        alternative_routes=alts,
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health", summary="Health check")
def health():
    try:
        with get_db() as con:
            con.execute("SELECT 1").fetchone()
        return {"status": "ok", "db": "connected"}
    except Exception as exc:
        log.warning("Health check DB error: %s", exc)
        return {"status": "degraded", "db": str(exc)}


@app.get(
    "/api/v1/recommendation/next",
    response_model=RecommendationResponse,
    summary="Recommendation for next upcoming calendar event",
)
def recommendation_next():
    with get_db() as con:
        row = con.execute(_NEXT_ROUTE_SQL).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No upcoming events found")
    with get_db() as con:
        return _build_recommendation(con, row)


@app.get(
    "/api/v1/recommendation/{event_id}",
    response_model=RecommendationResponse,
    summary="Recommendation for a specific calendar event",
)
def recommendation_by_event(event_id: str):
    with get_db() as con:
        row = con.execute(_EVENT_ROUTE_SQL, [event_id]).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Event not found: {event_id}")
        return _build_recommendation(con, row)


@app.get(
    "/api/v1/prediction/{event_id}",
    response_model=List[MLPrediction],
    summary="ML predictions for all route options of a calendar event",
)
def prediction_by_event(event_id: str):
    with get_db() as con:
        rows = con.execute(_PREDICTIONS_SQL, [event_id]).fetchall()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No predictions found for event {event_id} — run: python scripts/model.py --predict",
        )
    return [
        MLPrediction(
            prediction_id=r[0],
            option_id=r[1],
            predicted_min=r[2],
            predicted_crowd=r[3],
            actual_min=r[4],
            mae_7day=r[5],
            model_version=r[6],
            predicted_at=str(r[7]) if r[7] else None,
        )
        for r in rows
    ]


@app.get(
    "/api/v1/pipeline/status",
    response_model=List[PipelineRun],
    summary="Last 10 pipeline run records",
)
def pipeline_status():
    with get_db() as con:
        rows = con.execute(_PIPELINE_STATUS_SQL).fetchall()
    return [
        PipelineRun(
            run_id=r[0],
            source=r[1],
            status=r[2],
            rows_upserted=r[3],
            duration_ms=r[4],
            error_msg=r[5],
            ran_at=str(r[6]),
        )
        for r in rows
    ]


@app.get(
    "/api/v1/alerts",
    response_model=List[Alert],
    summary="Active HEAVY train disruption alerts (last 30 min)",
)
def active_alerts():
    with get_db() as con:
        rows = con.execute(_ALERTS_SQL).fetchall()
    return [Alert(affected_line=r[0], message=r[1]) for r in rows]
