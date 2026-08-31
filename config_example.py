# config_example.py
# Copy this file to config.py and fill in your real API keys.
# config.py is in .gitignore — never commit your real keys.

LTA_API_KEY = "your_lta_datamall_api_key_here"

ONEMAP_EMAIL = "your_onemap_registered_email_here"
ONEMAP_PASSWORD = "your_onemap_password_here"

# Google Calendar — download credentials.json from Google Cloud Console
# credentials.json and token.json are gitignored — never commit them
GOOGLE_CALENDAR_ID = "primary"  # "primary" = your main Google Calendar

# Your home address — used ONLY as a destination (go-home default, at-home check).
# It is NOT the routing origin. Use a postal code for best geocoding results.
HOME_ADDRESS = "your home block and street, Singapore"

# Where you currently are, as a 6-digit Singapore postal code. This is the routing
# ORIGIN when set. Leave "" to fall back to ip-api.com.
# ip-api geolocates the public/ISP IP, not the device — on a residential CGNAT
# connection it can land 15-20 km from where you actually are, and it moves as the
# ISP reassigns addresses. Set this whenever the origin has to be right.
CURRENT_ORIGIN_POSTAL = ""

# Your default work or school address — used as fallback destination when the
# calendar event location cannot be geocoded by OneMap
# e.g. "10 Dover Drive, Singapore 138683"  (SIT Dover campus)
WORK_ADDRESS = ""

# Optional fitness integrations — leave blank ("") to skip any you don't have
GARMIN_EMAIL = ""           # Garmin Connect account email
GARMIN_PASSWORD = ""        # Garmin Connect account password
WHOOP_ACCESS_TOKEN = ""     # Whoop personal access token from developer.whoop.com
