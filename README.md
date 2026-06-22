# SNAIC-sg-commute-pulse 🚇

> A calendar-aware Singapore commute recommendation system — reads your next event, checks real-time bus arrivals, MRT disruptions, and weather, then tells you **when to leave and how to get there**.

Built as a Data Engineering course assessment project at SIT (Singapore Institute of Technology).

---

## Pipeline Architecture

```
Calendar Event
     │
     ▼
OneMap Geocoding ───────────────────────────────────────┐
     │                                                  │
     ▼                                                  │
┌─────────────────────────────────────────────────────┐ │
│                  DATA INGESTION                     │ │
│  OneMap Routing  │  LTA Bus Arrival  │  LTA Alerts  │ │
│  data.gov.sg Weather Forecast        │              │ │
└─────────────────────────────────────────────────────┘ │
     │                                                  │
     ▼                                                  │
DuckDB (6 tables)  ◄────────────────────────────────────┘
     │
     ▼
SQL Transformation (JOIN routes + weather + alerts)
     │
     ▼
Streamlit Dashboard → Best Route · Leave-By Time · Warnings
```

## Data Sources

| Source | What it provides | Auth |
|--------|-----------------|------|
| [LTA DataMall](https://datamall.lta.gov.sg) | Real-time bus arrivals, train service alerts | API key (free, registration required) |
| [OneMap Singapore](https://www.onemap.gov.sg) | Routing (walk/bus/MRT), geocoding | Email + password → JWT token |
| [data.gov.sg](https://api.data.gov.sg) | 2-hour weather forecast by area | None |

## Tech Stack

- **Python** — requests, pandas, duckdb, streamlit, icalendar
- **DuckDB** — embedded analytical database (no server needed)
- **Streamlit** — recommendation dashboard

## Project Structure

```
sg-commute-pulse/
├── data/
│   ├── raw/          # raw API JSON responses
│   └── processed/    # cleaned CSVs
├── db/               # DuckDB database file (gitignored)
├── docs/
│   └── roadmap.html  # interactive project roadmap
├── scripts/
│   ├── schema.py     # create DuckDB tables
│   ├── ingest.py     # fetch all APIs
│   ├── transform.py  # SQL cleaning + enrichment
│   └── serve.py      # Streamlit dashboard
├── config_example.py # copy to config.py and add real keys
├── requirements.txt
└── .gitignore
```

## Setup

```bash
# 1. Clone
git clone https://github.com/DLim86/SNAIC-sg-commute-pulse.git
cd SNAIC-sg-commute-pulse

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your API keys
copy config_example.py config.py
# Edit config.py with your real keys
```

## API Registration

Before running the pipeline, register for:

1. **LTA DataMall** — [datamall.lta.gov.sg](https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html) (approval takes 1–2 days)
2. **OneMap** — [onemap.gov.sg](https://www.onemap.gov.sg) (instant, requires SingPass)

`data.gov.sg` needs no registration.

## Running the Dashboard

```bash
streamlit run scripts/serve.py
```

---

*SIT Data Engineering Assessment · 2026*
