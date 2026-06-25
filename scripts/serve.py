import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = str(Path(__file__).parent.parent / "db" / "commute.duckdb")

SGT = timezone(timedelta(hours=8))

MODE_ICON = {"WALK": "🚶", "BUS": "🚌", "MRT": "🚇", "LRT": "🚈"}

MRT_LINE_NAMES = {
    "EW": "East West Line",  "NS": "North South Line", "NE": "Northeast Line",
    "CC": "Circle Line",     "DT": "Downtown Line",    "TE": "Thomson-East Coast Line",
    "CR": "Cross Island Line","JR": "Jurong Region Line",
    "BP": "Bukit Panjang LRT","SE": "Sengkang LRT",   "PE": "Punggol LRT",
}

BEST_ROUTE_QUERY = """
SELECT
    r.option_id, r.event_id, r.title, r.start_time, r.leave_by,
    r.total_duration_min, r.walk_distance_m, r.num_transfers, r.fare,
    r.weather_forecast, r.is_rainy, r.alert_msg, r.recommendation_reason,
    r.dest_lat, r.dest_lng
FROM v_enriched_routes r
WHERE r.route_rank = 1
  AND r.start_time > NOW()
ORDER BY r.start_time
LIMIT 1
"""

LEGS_QUERY = """
SELECT leg_sequence, mode, service_no, from_name, to_name, duration_min, distance_m, num_stops
FROM route_legs
WHERE option_id = ?
ORDER BY leg_sequence
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

PREDICTION_QUERY = """
SELECT predicted_min, actual_min, predicted_crowd, actual_crowd,
       model_version, mae_7day, predicted_at
FROM predictions
WHERE event_id = ?
ORDER BY predicted_at DESC
LIMIT 1
"""

CALENDAR_EVENT_QUERY = """
SELECT title, start_time, location_raw
FROM calendar_events
WHERE event_id = ?
LIMIT 1
"""

PIPELINE_RUN_QUERY = """
SELECT ran_at, status, rows_upserted
FROM pipeline_runs
ORDER BY ran_at DESC
LIMIT 1
"""

st.set_page_config(page_title="SG Commute Pulse", page_icon="🚇", layout="centered")


@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


def fmt_sgt(dt):
    if dt is None:
        return "—"
    if hasattr(dt, "astimezone"):
        return dt.astimezone(SGT).strftime("%a %d %b, %H:%M")
    return str(dt)


def fmt_time(dt):
    if dt is None:
        return "—"
    if hasattr(dt, "astimezone"):
        return dt.astimezone(SGT).strftime("%H:%M")
    return str(dt)


def mins_from_now(dt):
    if dt is None:
        return None
    now = datetime.now(timezone.utc)
    if not hasattr(dt, "tzinfo") or dt.tzinfo is None:
        return None
    delta = (dt - now).total_seconds() / 60
    return int(delta)


con = get_connection()

st.title("🚇 SG Commute Pulse")

last_run = con.execute(PIPELINE_RUN_QUERY).fetchone()
if last_run:
    ran_at, status, rows = last_run
    status_icon = "✅" if status == "ok" else "⚠️"
    st.caption(f"{status_icon} Last pipeline run: {fmt_sgt(ran_at)} · {rows} rows · {status}")
else:
    st.caption("Pipeline has not run yet — run `python scripts/ingest.py` then `python scripts/transform.py`")

st.divider()

row = con.execute(BEST_ROUTE_QUERY).fetchone()

if row is None:
    st.warning("No upcoming events found. Run `python scripts/ingest.py` then `python scripts/transform.py` first.")
    time.sleep(60)
    st.rerun()

(
    option_id, event_id, title, start_time, leave_by,
    total_duration_min, walk_distance_m, num_transfers, fare,
    weather_forecast, is_rainy, alert_msg, recommendation_reason,
    dest_lat, dest_lng
) = row

now_sgt = datetime.now(timezone.utc).astimezone(SGT)

# --- Event card ---
st.subheader(f"📅 {title}")
mins_left = mins_from_now(leave_by)
if mins_left is not None and mins_left > 0:
    st.caption(f"Event starts {fmt_sgt(start_time)}  ·  You have **{mins_left} min** until you need to leave")
elif mins_left is not None and mins_left <= 0:
    st.error(f"⏰ You should have left already! Event at {fmt_sgt(start_time)}")
else:
    st.caption(f"Event starts {fmt_sgt(start_time)}")

# --- Leave-by panel ---
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Leave by", fmt_time(leave_by))
with col2:
    st.metric("Journey time", f"{total_duration_min} min")
with col3:
    fare_str = f"${fare:.2f}" if fare else "—"
    st.metric("Est. fare", fare_str)

if recommendation_reason:
    st.info(f"**Why chosen:** {recommendation_reason}")

# --- Warnings ---
if is_rainy and weather_forecast:
    st.warning(f"🌧️ {weather_forecast}")
if alert_msg:
    active_alerts = con.execute(ACTIVE_ALERTS_QUERY).fetchall()
    if active_alerts:
        for line, msg in active_alerts:
            st.error(f"🚨 {line}: {msg}")

st.divider()

# --- Step-by-step legs ---
st.subheader("🗺️ Journey steps")

legs = con.execute(LEGS_QUERY, [option_id]).fetchall()

if legs:
    ft_mode = None
    ft_svc = None
    for leg in legs:
        seq, mode, service_no, from_name, to_name, duration_min, distance_m, num_stops = leg
        icon = MODE_ICON.get(mode, "")
        svc_display = service_no or ""
        if mode == "MRT":
            svc_display = MRT_LINE_NAMES.get(service_no, service_no or "MRT")
        elif mode == "LRT":
            svc_display = MRT_LINE_NAMES.get(service_no, service_no or "LRT")

        stops_str = f" ({num_stops} stops)" if num_stops else ""
        dist_str = f" · {distance_m}m" if distance_m else ""

        cols = st.columns([0.08, 0.15, 0.35, 0.35, 0.07])
        cols[0].write(icon)
        cols[1].write(svc_display if mode != "WALK" else "Walk")
        cols[2].write(from_name or "—")
        cols[3].write(to_name or "—")
        cols[4].write(f"{duration_min}m{stops_str}")

        if ft_mode is None and mode in ("BUS", "MRT", "LRT"):
            ft_mode = mode
            ft_svc = service_no

    # --- Inline first-transit live arrival ---
    st.divider()
    st.subheader("🔴 Live arrivals")

    if ft_mode == "BUS" and ft_svc:
        bw = con.execute(FIRST_TRANSIT_FULL_QUERY, [ft_svc]).fetchone()
        if bw and bw[0] is not None:
            x1, x2, load = bw
            load_desc = {"SEA": "seats available", "SDA": "standing room", "LSD": "very full"}.get(load or "", "")
            x1_time = (now_sgt + timedelta(minutes=x1)).strftime("%H:%M")
            col1, col2 = st.columns(2)
            with col1:
                st.metric(f"🚌 Bus {ft_svc} — next", f"{x1_time} (~{x1} min)")
            if x2 is not None:
                x2_time = (now_sgt + timedelta(minutes=x2)).strftime("%H:%M")
                with col2:
                    st.metric("If missed → next", f"{x2_time} (~{x2} min)")
            if load_desc:
                st.caption(f"Bus crowding: {load_desc}")
        else:
            st.caption(f"🚌 Bus {ft_svc}: no live data at origin stop")

    elif ft_mode == "MRT":
        ft_name = MRT_LINE_NAMES.get(ft_svc, ft_svc or "MRT")
        x1_min, x2_min = 4, 8
        x1_time = (now_sgt + timedelta(minutes=x1_min)).strftime("%H:%M")
        x2_time = (now_sgt + timedelta(minutes=x2_min)).strftime("%H:%M")
        col1, col2 = st.columns(2)
        with col1:
            st.metric(f"🚇 {ft_name} — next", f"{x1_time} (~{x1_min} min)")
        with col2:
            st.metric("If missed → next", f"{x2_time} (~{x2_min} min)")
        st.caption("MRT headway is an estimate — no public real-time MRT API in Singapore.")

    elif ft_mode == "LRT":
        ft_name = MRT_LINE_NAMES.get(ft_svc, ft_svc or "LRT")
        x1_min, x2_min = 7, 14
        x1_time = (now_sgt + timedelta(minutes=x1_min)).strftime("%H:%M")
        x2_time = (now_sgt + timedelta(minutes=x2_min)).strftime("%H:%M")
        col1, col2 = st.columns(2)
        with col1:
            st.metric(f"🚈 {ft_name} — next", f"{x1_time} (~{x1_min} min)")
        with col2:
            st.metric("If missed → next", f"{x2_time} (~{x2_min} min)")
        st.caption("LRT headway is an estimate — no public real-time LRT API in Singapore.")

else:
    st.caption("No leg data — re-run ingest.py to populate route legs.")

st.divider()

# --- Alt routes ---
alt_rows = con.execute(ALT_ROUTES_QUERY, [event_id, option_id]).fetchall()

if alt_rows:
    st.subheader("🔀 Other route options (sorted by arrival time)")
    for idx, alt in enumerate(alt_rows[:2], start=2):
        alt_id, alt_dur, alt_fare, alt_transfers = alt
        alt_legs = con.execute(LEGS_QUERY, [alt_id]).fetchall()

        compact_tokens = []
        i = 0
        while i < len(alt_legs):
            _, mode, svc, _, _, dur, _, stops = alt_legs[i]
            icon = MODE_ICON.get(mode, "")
            if mode == "WALK":
                compact_tokens.append(f"🚶{dur}m")
                i += 1
            elif mode == "BUS":
                stops_str = f"/{stops}st" if stops else ""
                compact_tokens.append(f"🚌{svc} {dur}m{stops_str}")
                i += 1
            elif mode in ("MRT", "LRT"):
                # group consecutive MRT+LRT
                rail_dur = dur
                j = i + 1
                rail_modes = {mode}
                while j < len(alt_legs) and alt_legs[j][1] in ("MRT", "LRT"):
                    rail_dur += alt_legs[j][5]
                    rail_modes.add(alt_legs[j][1])
                    j += 1
                if "MRT" in rail_modes and "LRT" in rail_modes:
                    compact_tokens.append(f"🚇🚈MRT+LRT {rail_dur}m")
                elif "MRT" in rail_modes:
                    line = MRT_LINE_NAMES.get(svc, svc or "MRT")
                    compact_tokens.append(f"🚇{line} {rail_dur}m")
                else:
                    compact_tokens.append(f"🚈LRT {rail_dur}m")
                i = j
            else:
                i += 1

        route_str = " → ".join(compact_tokens)
        fare_str = f"${alt_fare:.2f}" if alt_fare else "—"

        # first transit note
        alt_ft_mode = alt_ft_svc = None
        for _, mode, svc, *_ in alt_legs:
            if mode in ("BUS", "MRT", "LRT") and alt_ft_mode is None:
                alt_ft_mode, alt_ft_svc = mode, svc

        notes = []
        if alt_ft_mode == "BUS" and alt_ft_svc:
            bw = con.execute(FIRST_TRANSIT_FULL_QUERY, [alt_ft_svc]).fetchone()
            if bw and bw[0] is not None:
                x1 = bw[0]
                x1_time = (now_sgt + timedelta(minutes=x1)).strftime("%H:%M")
                flag = " ⚠" if x1 > 10 else ""
                notes.append(f"🚌Bus {alt_ft_svc}: {x1_time} (~{x1}m){flag}")
            else:
                notes.append(f"🚌Bus {alt_ft_svc}: no live data")
        elif alt_ft_mode == "MRT":
            ft_name = MRT_LINE_NAMES.get(alt_ft_svc, alt_ft_svc or "MRT")
            x1_time = (now_sgt + timedelta(minutes=4)).strftime("%H:%M")
            notes.append(f"🚇{ft_name}: ~{x1_time} (~4m)")
        elif alt_ft_mode == "LRT":
            x1_time = (now_sgt + timedelta(minutes=7)).strftime("%H:%M")
            notes.append(f"🚈{alt_ft_svc or 'LRT'}: ~{x1_time} (~7m)")

        notes_str = "  ·  ".join(notes) if notes else ""

        with st.expander(f"[{idx}]  {route_str}  ·  {alt_dur} min  ·  {fare_str}"):
            if notes_str:
                st.caption(notes_str)
            for _, mode, svc, from_n, to_n, dur, dist, stops in alt_legs:
                icon = MODE_ICON.get(mode, "")
                svc_display = MRT_LINE_NAMES.get(svc, svc or "") if mode in ("MRT", "LRT") else (svc or "")
                stops_str = f" ({stops} stops)" if stops else ""
                st.write(f"{icon} **{svc_display or 'Walk'}** · {from_n} → {to_n} · {dur}m{stops_str}")

st.divider()

# --- ML prediction panel ---
st.subheader("🤖 ML Prediction")

pred = con.execute(PREDICTION_QUERY, [event_id]).fetchone()

CROWD_ICON = {"SEA": "🟢", "SDA": "🟡", "LSD": "🔴"}
CROWD_LABEL = {"SEA": "Seats available", "SDA": "Standing", "LSD": "Very full"}

if pred is None:
    st.info("Model not yet trained — run `python scripts/model.py --train` then `python scripts/model.py --predict`")
else:
    predicted_min, actual_min, predicted_crowd, actual_crowd, model_version, mae_7day, predicted_at = pred
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Predicted journey", f"{predicted_min} min")
    with col2:
        actual_str = f"{actual_min} min" if actual_min is not None else "not yet recorded"
        st.metric("Actual (after trip)", actual_str)
    with col3:
        mae_str = f"{mae_7day:.1f} min" if mae_7day is not None else "—"
        st.metric("7-day MAE", mae_str)

    if predicted_crowd:
        icon = CROWD_ICON.get(predicted_crowd, "")
        label = CROWD_LABEL.get(predicted_crowd, predicted_crowd)
        actual_crd_str = ""
        if actual_crowd:
            a_icon = CROWD_ICON.get(actual_crowd, "")
            a_label = CROWD_LABEL.get(actual_crowd, actual_crowd)
            actual_crd_str = f"  ·  Actual crowd: {a_icon} {a_label}"
        st.caption(f"Predicted crowd at boarding: {icon} {label}{actual_crd_str}")

    st.caption(f"Model: {model_version or '—'}  ·  Predicted at {fmt_sgt(predicted_at)}")

# --- Footer + auto-refresh ---
st.divider()
st.caption("Auto-refreshes every 60 seconds  ·  SG Commute Pulse — SIT Data Engineering Assessment")

time.sleep(60)
st.rerun()
