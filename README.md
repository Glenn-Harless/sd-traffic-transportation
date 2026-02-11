# San Diego Traffic & Transportation

Regional traffic congestion, transit ridership recovery, collision safety, and new mobility analysis for San Diego. Part of the San Diego Civic Data series.

**Live Dashboard**: https://sd-traffic-transportation.streamlit.app/

## Key Questions

1. Where are the worst congestion bottlenecks, and are they shifting?
2. Is transit ridership recovering post-COVID?
3. Are billions in transit investment paying off in ridership gains?
4. Where is bike/pedestrian infrastructure most and least used?

## Data Sources (10 datasets)

**SANDAG** (opendata.sandag.org):
- Transit Ridership by Route (2019-2024)
- Vehicle Miles Traveled / PeMS (2013-2024)
- Highway Travel Times (2019-2024)
- SWITRS Collision Summary (2006-2024)
- SWITRS Detailed Collisions (2013-2022, ~239K records with lat/lon)
- Youth Opportunity Pass (Apr 2022-Sep 2025)
- Flexible Fleet / Microtransit (Jan 2022-Sep 2025)

**City of San Diego** (seshat.datasd.org):
- Traffic Volume Counts (2005-2022)
- Police-Reported Collisions (2015-2026)
- Transit Route Reference Table

## Quick Start

```bash
uv sync
uv run python -m pipeline.build   # ingest + transform + validate
uv run streamlit run dashboard/app.py
```

## Dashboard (6 tabs)

- **Overview**: KPIs, ridership recovery, VMT trends, collision severity
- **Congestion**: Travel times, VMT by freeway, street volumes
- **Transit**: Route boardings, Youth Opportunity Pass, Flexible Fleet
- **Safety**: SWITRS severity trends, bike/ped collisions, weather/lighting
- **Collision Map**: pydeck heatmap of 222K collision points
- **Deep Dive**: Route lookup, top streets, data sources

## API

```bash
uv run uvicorn api.main:app --reload
# Open http://localhost:8000/docs
```

14 endpoints covering ridership, congestion, safety, transit programs, and traffic volumes.

## MCP Server

Configured in `.mcp.json` for Claude Code. 10 tools for querying traffic/transit data directly.
