import logging
import os
import sys
import time
import uuid
from datetime import datetime, date, timedelta, timezone
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

# Allow `from config import ...` to find config.py in the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd
import requests

try:
    from config import LTA_API_KEY, ONEMAP_EMAIL, ONEMAP_PASSWORD
    try:
        from config import GOOGLE_CALENDAR_ID
    except ImportError:
        GOOGLE_CALENDAR_ID = "primary"
    try:
        from config import HOME_ADDRESS
    except ImportError:
        HOME_ADDRESS = None
    try:
        from config import WORK_ADDRESS
    except ImportError:
        WORK_ADDRESS = ""
except ImportError:
    LTA_API_KEY = os.environ["LTA_API_KEY"]
    ONEMAP_EMAIL = os.environ["ONEMAP_EMAIL"]
    ONEMAP_PASSWORD = os.environ["ONEMAP_PASSWORD"]
    GOOGLE_CALENDAR_ID = os.environ.get("GOOGLE_CALENDAR_ID", "primary")
    HOME_ADDRESS = os.environ.get("HOME_ADDRESS")
    WORK_ADDRESS = os.environ.get("WORK_ADDRESS", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "db" / "commute.duckdb"
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

SG_LAT = (1.15, 1.47)
SG_LNG = (103.6, 104.1)
SGT = timezone(timedelta(hours=8))

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


# ── IP geolocation ───────────────────────────────────────────────────────────

def get_current_location():
    try:
        r = requests.get("http://ip-api.com/json/", timeout=5)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "success":
            raise ValueError(f"ip-api returned: {data.get('message', 'unknown error')}")
        lat, lng = data["lat"], data["lon"]
        if not validate_sg_coords(lat, lng):
            raise ValueError(f"IP location ({lat}, {lng}) is outside Singapore — VPN or inaccurate fix")
        log.info("IP geolocation: %s, %s → (%.5f, %.5f)", data.get("city"), data.get("regionName"), lat, lng)
        return lat, lng
    except Exception as exc:
        log.warning("IP geolocation failed: %s — falling back to default origin (Bishan)", exc)
        return 1.3521, 103.8198


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
    # Try progressively simpler search terms if the full address fails
    candidates = [address]
    stripped = address.replace(", Singapore", "").strip()
    if stripped != address:
        candidates.append(stripped)
    # Also try just the first comma-delimited token (e.g. "1 Sentul Walk")
    first_token = stripped.split(",")[0].strip()
    if first_token and first_token not in candidates:
        candidates.append(first_token)

    for search_val in candidates:
        data = fetch_with_retry(
            "https://www.onemap.gov.sg/api/common/elastic/search",
            headers={"Authorization": token},
            params={"searchVal": search_val, "returnGeom": "Y", "getAddrDetails": "Y"},
        )
        results = data.get("results", [])
        if not results:
            log.warning("No geocode result for '%s' — trying simpler term", search_val)
            continue
        lat = float(results[0]["LATITUDE"])
        lng = float(results[0]["LONGITUDE"])
        if not validate_sg_coords(lat, lng):
            log.warning("Geocode for '%s' returned coords outside SG: %.5f, %.5f", search_val, lat, lng)
            continue
        if search_val != address:
            log.info("Geocoded '%s' using simplified search '%s'", address, search_val)
        return lat, lng

    raise ValueError(f"No geocode result for: {address}")


# ── Google Calendar ───────────────────────────────────────────────────────────

CREDENTIALS_PATH = Path(__file__).parent.parent / "credentials.json"
TOKEN_PATH = Path(__file__).parent.parent / "token.json"
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]


def _get_calendar_service():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json())
    return build("calendar", "v3", credentials=creds)


def fetch_next_calendar_event(con, token):
    service = _get_calendar_service()
    now_utc = datetime.now(timezone.utc).isoformat()

    result = service.events().list(
        calendarId=GOOGLE_CALENDAR_ID,
        timeMin=now_utc,
        maxResults=10,
        singleEvents=True,
        orderBy="startTime",
    ).execute()

    for event in result.get("items", []):
        location = event.get("location", "").strip()
        start_dt_str = event.get("start", {}).get("dateTime")
        if not location or not start_dt_str:
            continue  # skip all-day events and events with no location

        try:
            lat, lng = geocode(location, token)
        except ValueError as exc:
            log.warning("Geocode failed for '%s': %s", event.get("summary", ""), exc)
            if WORK_ADDRESS:
                try:
                    lat, lng = geocode(WORK_ADDRESS, token)
                    location = WORK_ADDRESS
                    log.info("Using WORK_ADDRESS as fallback destination")
                except ValueError:
                    log.warning("WORK_ADDRESS fallback also failed — skipping event")
                    continue
            else:
                log.warning("No WORK_ADDRESS set — skipping event")
                continue

        event_id = f"GCAL_{event['id']}"
        title = event.get("summary", "Untitled Event")

        con.execute(
            """INSERT OR REPLACE INTO calendar_events
               (event_id, title, start_time, location_raw, dest_lat, dest_lng)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [event_id, title, start_dt_str, location, lat, lng],
        )
        log.info("Calendar event → '%s' at %s (%.5f, %.5f)", title, location, lat, lng)
        return event_id, lat, lng

    raise ValueError("No upcoming events with a geocodable Singapore location found")


# ── OneMap routing ────────────────────────────────────────────────────────────

def ingest_routes(con, event_id, dest_lat, dest_lng, token, origin_lat, origin_lng):
    t0 = time.time()
    data = fetch_with_retry(
        "https://www.onemap.gov.sg/api/public/routingsvc/route",
        headers={"Authorization": token},
        params={
            "start": f"{origin_lat},{origin_lng}",
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
            route_field = leg.get("route")
            service_no = (
                leg.get("routeId") or
                (route_field.get("shortName") if isinstance(route_field, dict) else None) or
                ""
            )
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


# ── Smart time-based default destination ─────────────────────────────────────

def get_smart_default(con, token):
    """
    Returns (event_id, dest_lat, dest_lng) using time-of-day heuristic, or None to skip.
      8–10 AM  → WORK_ADDRESS  (morning commute)
      4–6 PM   → HOME_ADDRESS  (evening commute, depart ~6:30 PM)
      After 6 PM → HOME_ADDRESS unless current location is already within 3 km of home
      Other hours → None (skip pipeline, no sensible default)
    Calendar events always take priority — this only runs when there is no calendar event.
    """
    now_sgt = datetime.now(SGT)
    hour = now_sgt.hour

    if 8 <= hour < 10:
        if not WORK_ADDRESS:
            log.info("8–10 AM window but WORK_ADDRESS not set in config — skipping")
            return None
        try:
            dest_lat, dest_lng = geocode(WORK_ADDRESS, token)
        except ValueError as exc:
            log.warning("WORK_ADDRESS geocode failed: %s", exc)
            return None
        start_time = now_sgt.replace(hour=9, minute=0, second=0, microsecond=0)
        event_id, title, location_raw = "DEFAULT_WORK_COMMUTE", "Work / School (default)", WORK_ADDRESS

    elif hour >= 16:
        if not HOME_ADDRESS:
            log.info("Afternoon/evening window but HOME_ADDRESS not set in config — skipping")
            return None
        try:
            dest_lat, dest_lng = geocode(HOME_ADDRESS, token)
        except ValueError as exc:
            log.warning("HOME_ADDRESS geocode failed: %s", exc)
            return None

        if hour >= 18:
            # Check if already at home — IP geolocation vs home coords, 3 km threshold
            curr_lat, curr_lng = get_current_location()
            dist_m = haversine(curr_lat, curr_lng, dest_lat, dest_lng)
            if dist_m < 3000:
                log.info("After 6 PM and %.0f m from home — already home, no routing needed", dist_m)
                return None
            # Still out: suggest heading home in the next 30 min
            start_time = now_sgt + timedelta(minutes=30)
        else:
            # 4–6 PM: assume leaving work around 6:30 PM
            start_time = now_sgt.replace(hour=18, minute=30, second=0, microsecond=0)

        event_id, title, location_raw = "DEFAULT_HOME_COMMUTE", "Home (default destination)", HOME_ADDRESS

    else:
        log.info("No calendar event and outside routing window (8–10 AM, after 4 PM) — skipping")
        return None

    con.execute(
        """INSERT OR REPLACE INTO calendar_events
           (event_id, title, start_time, location_raw, dest_lat, dest_lng)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [event_id, title, start_time.isoformat(), location_raw, dest_lat, dest_lng],
    )
    log.info("Smart default: '%s' → (%.5f, %.5f)", title, dest_lat, dest_lng)
    return event_id, dest_lat, dest_lng


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Ingest pipeline starting ===")
    token = get_onemap_token()
    con = duckdb.connect(str(DB_PATH))

    try:
        if HOME_ADDRESS:
            try:
                origin_lat, origin_lng = geocode(HOME_ADDRESS, token)
                log.info("Origin: home address geocoded → (%.5f, %.5f)", origin_lat, origin_lng)
            except ValueError as exc:
                log.warning("HOME_ADDRESS geocode failed: %s — falling back to IP geolocation", exc)
                origin_lat, origin_lng = get_current_location()
        else:
            log.info("HOME_ADDRESS not set — using IP geolocation as origin")
            origin_lat, origin_lng = get_current_location()

        try:
            event_id, dest_lat, dest_lng = fetch_next_calendar_event(con, token)
        except ValueError as exc:
            log.warning("No usable calendar event — %s", exc)
            result = get_smart_default(con, token)
            if result is None:
                log_run(con, "calendar", 0, 0, "skipped", str(exc))
                return
            event_id, dest_lat, dest_lng = result

        for name, fn, kwargs in [
            ("weather",      ingest_weather,      {"dest_lat": dest_lat, "dest_lng": dest_lng}),
            ("routes",       ingest_routes,       {"event_id": event_id, "dest_lat": dest_lat, "dest_lng": dest_lng, "token": token, "origin_lat": origin_lat, "origin_lng": origin_lng}),
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
