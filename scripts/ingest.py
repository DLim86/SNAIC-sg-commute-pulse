import logging
import os
import sys
import time
import uuid
from datetime import datetime, date, timezone
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

# Allow `from config import ...` to find config.py in the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
import requests

try:
    from config import LTA_API_KEY, ONEMAP_EMAIL, ONEMAP_PASSWORD
except ImportError:
    LTA_API_KEY = os.environ["LTA_API_KEY"]
    ONEMAP_EMAIL = os.environ["ONEMAP_EMAIL"]
    ONEMAP_PASSWORD = os.environ["ONEMAP_PASSWORD"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "db" / "commute.duckdb"
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

SG_LAT = (1.15, 1.47)
SG_LNG = (103.6, 104.1)

LTA_HEADERS = {"AccountKey": LTA_API_KEY, "accept": "application/json"}
RAINY_KEYWORDS = {"rain", "shower", "thunder", "drizzle"}


# ── Utilities ─────────────────────────────────────────────────────────────────

def fetch_with_retry(url, headers=None, params=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt  # 1 s, 2 s, 4 s
            log.warning("Attempt %d failed (%s) — retry in %ds", attempt + 1, exc, wait)
            time.sleep(wait)


def validate_sg_coords(lat, lng):
    return SG_LAT[0] <= lat <= SG_LAT[1] and SG_LNG[0] <= lng <= SG_LNG[1]


def haversine(lat1, lng1, lat2, lng2):
    R = 6_371_000
    d_lat = radians(lat2 - lat1)
    d_lng = radians(lng2 - lng1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lng / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def save_parquet(records, source_name):
    today = date.today().isoformat()
    out_dir = RAW_DIR / source_name / f"date={today}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{source_name}_{today}.parquet"
    pd.DataFrame(records).to_parquet(out, index=False)
    log.info("Parquet saved → %s (%d rows)", out, len(records))


def log_run(con, source, rows, duration_ms, status, error_msg=None):
    con.execute(
        """INSERT OR REPLACE INTO pipeline_runs
           (run_id, source, rows_upserted, duration_ms, status, error_msg)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [str(uuid.uuid4()), source, rows, duration_ms, status, error_msg],
    )


# ── OneMap auth ───────────────────────────────────────────────────────────────

def get_onemap_token(max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.post(
                "https://www.onemap.gov.sg/api/auth/post/getToken",
                json={"email": ONEMAP_EMAIL, "password": ONEMAP_PASSWORD},
                timeout=30,  # SSL handshake can be slow on first connect
            )
            r.raise_for_status()
            token = r.json()["access_token"]
            log.info("OneMap token obtained")
            return token
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            log.warning("Token request attempt %d failed (%s) — retry in %ds", attempt + 1, exc, wait)
            time.sleep(wait)


def geocode(address, token):
    data = fetch_with_retry(
        "https://www.onemap.gov.sg/api/common/elastic/search",
        headers={"Authorization": token},
        params={"searchVal": address, "returnGeom": "Y", "getAddrDetails": "Y"},
    )
    results = data.get("results", [])
    if not results:
        raise ValueError(f"No geocode result for: {address}")
    lat = float(results[0]["LATITUDE"])
    lng = float(results[0]["LONGITUDE"])
    if not validate_sg_coords(lat, lng):
        raise ValueError(f"Coordinates outside Singapore: {lat}, {lng}")
    return lat, lng


# ── Calendar event (test seed) ────────────────────────────────────────────────

def seed_calendar_event(con, token):
    row = con.execute(
        "SELECT event_id, dest_lat, dest_lng FROM calendar_events LIMIT 1"
    ).fetchone()
    if row:
        log.info("Using existing calendar event: %s", row[0])
        return row[0], row[1], row[2]

    address = "Singapore Management University, 81 Victoria Street"
    lat, lng = geocode(address, token)
    event_id = "EVT_TEST_001"

    con.execute(
        """INSERT OR REPLACE INTO calendar_events
           (event_id, title, start_time, location_raw, dest_lat, dest_lng)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [event_id, "Morning Meeting at SMU", "2026-06-24 10:00:00+08:00", address, lat, lng],
    )
    log.info("Seeded calendar event → %s (%.5f, %.5f)", address, lat, lng)
    return event_id, lat, lng


# ── OneMap routing ────────────────────────────────────────────────────────────

# Fixed origin — in production this comes from user's home address in config
ORIGIN_LAT = 1.3521
ORIGIN_LNG = 103.8198


def ingest_routes(con, event_id, dest_lat, dest_lng, token):
    t0 = time.time()
    data = fetch_with_retry(
        "https://www.onemap.gov.sg/api/public/routingsvc/route",
        headers={"Authorization": token},
        params={
            "start": f"{ORIGIN_LAT},{ORIGIN_LNG}",
            "end": f"{dest_lat},{dest_lng}",
            "routeType": "pt",
            "mode": "TRANSIT",
            "numItineraries": 3,
            "date": date.today().strftime("%m-%d-%Y"),
            "time": "08:00:00",
        },
    )

    if not isinstance(data, dict):
        raise ValueError(f"Unexpected OneMap response type: {type(data).__name__} — {str(data)[:120]}")
    itineraries = data.get("plan", {}).get("itineraries", [])
    if not itineraries:
        log.warning("No route itineraries returned from OneMap")
        log_run(con, "onemap_route", 0, int((time.time() - t0) * 1000), "error", "no itineraries")
        return

    records = []
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

    for i, it in enumerate(itineraries):
        option_id = f"{event_id}_ROUTE_{i + 1}"
        duration_min = round(it.get("duration", 0) / 60)
        walk_m = round(it.get("walkDistance", 0))
        transit_legs = [l for l in it.get("legs", []) if l.get("mode") != "WALK"]
        transfers = max(0, len(transit_legs) - 1)
        try:
            fare = float(str(it.get("fare", 0)).replace("$", ""))
        except (ValueError, TypeError):
            fare = 0.0

        con.execute(
            """INSERT OR REPLACE INTO route_options
               (option_id, event_id, total_duration_min, walk_distance_m, num_transfers, fare, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [option_id, event_id, duration_min, walk_m, transfers, fare, fetched_at],
        )
        records.append({
            "option_id": option_id, "event_id": event_id,
            "total_duration_min": duration_min, "walk_distance_m": walk_m,
            "num_transfers": transfers, "fare": fare, "fetched_at": fetched_at.isoformat(),
        })

        # Save individual legs so transform can show step-by-step directions
        mode_map = {"SUBWAY": "MRT", "TRAM": "LRT", "RAIL": "MRT"}
        for j, leg in enumerate(it.get("legs", [])):
            raw_mode = leg.get("mode", "WALK")
            leg_mode = mode_map.get(raw_mode, raw_mode)
            service_no = (leg.get("routeId") or
                          leg.get("route", {}).get("shortName") or "")
            from_name = leg.get("from", {}).get("name", "")
            to_name = leg.get("to", {}).get("name", "")
            leg_dur = round(leg.get("duration", 0) / 60)
            leg_dist = round(leg.get("distance", 0))
            leg_id = f"{option_id}_LEG_{j + 1}"
            con.execute(
                """INSERT OR REPLACE INTO route_legs
                   (leg_id, option_id, leg_sequence, mode, service_no,
                    from_name, to_name, duration_min, distance_m, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [leg_id, option_id, j + 1, leg_mode, service_no,
                 from_name, to_name, leg_dur, leg_dist, fetched_at],
            )

    save_parquet(records, "onemap_route")
    log_run(con, "onemap_route", len(records), int((time.time() - t0) * 1000), "success")
    log.info("Routes upserted: %d", len(records))


# ── Bus stops (cached daily) ──────────────────────────────────────────────────

def fetch_all_bus_stops():
    cache = RAW_DIR / "bus_stops" / "bus_stops.parquet"
    if cache.exists():
        df = pd.read_parquet(cache)
        log.info("Bus stops loaded from cache (%d stops)", len(df))
        return df

    cache.parent.mkdir(parents=True, exist_ok=True)
    stops, skip = [], 0
    while True:
        data = fetch_with_retry(
            "https://datamall2.mytransport.sg/ltaodataservice/BusStops",
            headers=LTA_HEADERS,
            params={"$skip": skip},
        )
        batch = data.get("value", [])
        if not batch:
            break
        stops.extend(batch)
        skip += 500
        log.info("Fetched %d bus stops so far...", len(stops))

    df = pd.DataFrame(stops)
    df.to_parquet(cache, index=False)
    log.info("Bus stops cached: %d total", len(df))
    return df


def nearest_bus_stop(lat, lng, stops_df):
    df = stops_df.dropna(subset=["Latitude", "Longitude"]).copy()
    df["Latitude"] = df["Latitude"].astype(float)
    df["Longitude"] = df["Longitude"].astype(float)
    df = df[df["Latitude"].between(*SG_LAT) & df["Longitude"].between(*SG_LNG)]
    distances = df.apply(
        lambda r: haversine(lat, lng, r["Latitude"], r["Longitude"]), axis=1
    )
    idx = distances.idxmin()
    return df.loc[idx, "BusStopCode"], float(distances[idx])


# ── Bus arrivals ──────────────────────────────────────────────────────────────

def ingest_bus_arrivals(con, dest_lat, dest_lng):
    t0 = time.time()
    stops_df = fetch_all_bus_stops()
    stop_code, dist_m = nearest_bus_stop(dest_lat, dest_lng, stops_df)
    log.info("Nearest bus stop: %s (%.0f m away)", stop_code, dist_m)

    try:
        data = fetch_with_retry(
            "https://datamall2.mytransport.sg/ltaodataservice/BusArrivalv2",
            headers=LTA_HEADERS,
            params={"BusStopCode": stop_code},
        )
    except requests.exceptions.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            # LTA returns 404 when a stop has no active services at this moment
            log.warning("No active bus services at stop %s (404)", stop_code)
            log_run(con, "lta_bus", 0, int((time.time() - t0) * 1000), "success", f"no services at {stop_code}")
            return
        raise

    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
    records = []

    for svc in data.get("Services", []):
        service_no = svc.get("ServiceNo", "")
        next_bus = svc.get("NextBus", {})
        eta_str = next_bus.get("EstimatedArrival", "")
        load = next_bus.get("Load", "")
        if not eta_str:
            continue
        try:
            eta_dt = datetime.fromisoformat(eta_str)
            now_aware = datetime.now(eta_dt.tzinfo)
            next_bus_mins = max(0, round((eta_dt - now_aware).total_seconds() / 60))
        except (ValueError, TypeError):
            next_bus_mins = 0

        con.execute(
            """INSERT OR REPLACE INTO bus_arrivals
               (bus_stop_code, service_no, next_bus_mins, load, fetched_at)
               VALUES (?, ?, ?, ?, ?)""",
            [stop_code, service_no, next_bus_mins, load, fetched_at],
        )
        records.append({
            "bus_stop_code": stop_code, "service_no": service_no,
            "next_bus_mins": next_bus_mins, "load": load, "fetched_at": fetched_at.isoformat(),
        })

    if records:
        save_parquet(records, "lta_bus")
    log_run(con, "lta_bus", len(records), int((time.time() - t0) * 1000), "success")
    log.info("Bus arrivals upserted: %d services at stop %s", len(records), stop_code)


# ── Train alerts ──────────────────────────────────────────────────────────────

def ingest_train_alerts(con):
    t0 = time.time()
    data = fetch_with_retry(
        "https://datamall2.mytransport.sg/ltaodataservice/TrainServiceAlerts",
        headers=LTA_HEADERS,
    )

    value = data.get("value", {})
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Status 1 = normal operations, no alerts to store
    if value.get("Status", 1) == 1:
        log.info("No active train disruptions")
        log_run(con, "lta_train", 0, int((time.time() - t0) * 1000), "success")
        return

    records = []
    for msg in value.get("Message", []):
        message_text = msg.get("Message", "")
        severity = "HEAVY" if "heavy" in message_text.lower() else "MODERATE"
        alert_id = f"ALERT_{fetched_at.strftime('%Y%m%d%H%M%S')}_{msg.get('CreatedDate', '')}"

        con.execute(
            """INSERT OR REPLACE INTO train_alerts
               (alert_id, affected_line, message, severity, fetched_at)
               VALUES (?, ?, ?, ?, ?)""",
            [alert_id, msg.get("AffectedLines", ""), message_text, severity, fetched_at],
        )
        records.append({
            "alert_id": alert_id, "affected_line": msg.get("AffectedLines", ""),
            "message": message_text, "severity": severity, "fetched_at": fetched_at.isoformat(),
        })

    if records:
        save_parquet(records, "lta_train")
    log_run(con, "lta_train", len(records), int((time.time() - t0) * 1000), "success")
    log.info("Train alerts upserted: %d", len(records))


# ── Weather ───────────────────────────────────────────────────────────────────

def ingest_weather(con, dest_lat, dest_lng):
    t0 = time.time()
    data = fetch_with_retry("https://api.data.gov.sg/v1/environment/2-hour-weather-forecast")

    area_meta = {a["name"]: a["label_location"] for a in data.get("area_metadata", [])}
    items = data.get("items", [])
    if not items:
        log.warning("No weather data returned")
        log_run(con, "weather", 0, int((time.time() - t0) * 1000), "error", "no items")
        return

    item = items[0]
    valid_start = item["valid_period"]["start"]
    valid_end = item["valid_period"]["end"]
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
    records = []

    for fc in item.get("forecasts", []):
        area = fc["area"]
        forecast = fc["forecast"]
        is_rainy = any(kw in forecast.lower() for kw in RAINY_KEYWORDS)
        loc = area_meta.get(area, {})

        con.execute(
            """INSERT OR REPLACE INTO weather_forecast
               (area, forecast, is_rainy, valid_start, valid_end, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [area, forecast, is_rainy, valid_start, valid_end, fetched_at],
        )
        records.append({
            "area": area, "forecast": forecast, "is_rainy": is_rainy,
            "valid_start": valid_start, "valid_end": valid_end,
            "fetched_at": fetched_at.isoformat(),
            "area_lat": loc.get("latitude"), "area_lng": loc.get("longitude"),
        })

    if records:
        save_parquet(records, "weather")

    # Log the forecast for the area nearest to destination (informational only)
    geo_records = [r for r in records if r["area_lat"] and r["area_lng"]]
    if geo_records:
        nearest = min(
            geo_records,
            key=lambda r: haversine(dest_lat, dest_lng, r["area_lat"], r["area_lng"]),
        )
        log.info(
            "Weather near destination (%s): %s | rainy=%s",
            nearest["area"], nearest["forecast"], nearest["is_rainy"],
        )

    log_run(con, "weather", len(records), int((time.time() - t0) * 1000), "success")
    log.info("Weather areas upserted: %d", len(records))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Ingest pipeline starting ===")
    token = get_onemap_token()
    con = duckdb.connect(str(DB_PATH))

    try:
        event_id, dest_lat, dest_lng = seed_calendar_event(con, token)

        for name, fn, kwargs in [
            ("weather",      ingest_weather,      {"dest_lat": dest_lat, "dest_lng": dest_lng}),
            ("routes",       ingest_routes,       {"event_id": event_id, "dest_lat": dest_lat, "dest_lng": dest_lng, "token": token}),
            ("bus_arrivals", ingest_bus_arrivals,  {"dest_lat": dest_lat, "dest_lng": dest_lng}),
            ("train_alerts", ingest_train_alerts, {}),
        ]:
            try:
                fn(con, **kwargs)
            except Exception as exc:
                log.error("%s failed: %s", name, exc)
                log_run(con, name, 0, 0, "error", str(exc))
    finally:
        con.close()

    log.info("=== Ingest pipeline complete ===")


if __name__ == "__main__":
    main()
