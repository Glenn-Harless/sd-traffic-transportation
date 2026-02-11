# SD Traffic & Transportation — Regional Mobility Dashboard

## Project Overview
San Diego traffic congestion, transit ridership recovery, collision safety, and new mobility programs.
10 datasets from SANDAG Socrata API and City of San Diego open data portal.
**Repo**: https://github.com/Glenn-Harless/sd-traffic-transportation
**Series**: Project #4 in SD Civic Data (after sd-city-budget, sd-get-it-done)

## Architecture

### Project Structure
```
pipeline/
  ingest.py          # Downloads 7 Socrata JSON + 3 seshat CSV
  transform.py       # DuckDB SQL → 12 aggregated parquets
  validate.py        # 78 data quality checks
  build.py           # Orchestrator: ingest → transform → validate
data/raw/            # ~3.7GB gitignored (JSON + CSV)
data/aggregated/     # 12 parquets, 2.5MB total, committed to git
dashboard/app.py     # 6-tab Streamlit (Overview, Congestion, Transit, Safety, Collision Map, Deep Dive)
api/
  queries.py         # Shared query layer — ALL SQL lives here
  models.py          # 14 Pydantic response models
  main.py            # FastAPI with 14 endpoints
  mcp_server.py      # FastMCP with 10 tools
```

### Commands
```bash
uv run python -m pipeline.build          # Full pipeline (ingest + transform + validate)
uv run python -m pipeline.build --force  # Re-download everything
uv run python -m pipeline.validate       # Validation only
uv run streamlit run dashboard/app.py    # Dashboard
uv run uvicorn api.main:app --reload     # API (http://localhost:8000/docs)
```

### Dashboard Rules
- **Use DuckDB for all data access** — no Polars/pandas for loading full datasets. Streamlit Cloud has 1GB RAM limit.
- `query()` helper: fresh `duckdb.connect()` per call, returns pandas DataFrame.
- `_year_where()` and `_mode_where()` for sidebar filters across all tabs.
- Each query should return small aggregated DataFrames (~10-50 rows).
- `requirements.txt` at project root for Streamlit Cloud (not pyproject.toml).

### API Design
- `api/queries.py` is the single source of truth — FastAPI and MCP both call it
- `_where()` builder with safe string escaping (same pattern as sd-city-budget)
- `_run()` opens a fresh DuckDB connection per call, returns list[dict]

## Data Sources

### SANDAG Socrata (JSON, `opendata.sandag.org/resource/{id}.json`)
| Dataset | Resource ID | Rows | Key Gotchas |
|---------|------------|------|-------------|
| transit_ridership | q5rv-a6w8 | 823 | Column typo: `calenadr_year` |
| vmt_pems | kzvf-xgyu | 672 | All fields are strings |
| highway_travel_times | sx8b-e5xp | 144 | `mean` column = minutes |
| switrs_summary | ta2f-7tx9 | 95 | |
| switrs_detailed | uzct-sb5t | 239K | Literal "NULL" strings; lat/lon are `latitude_sandag`/`longitude_sandag`; boolean fields (not string "1"/"Y") for bicycle/pedestrian/motorcycle_accident; killed/injured are `number_killed`/`number_injured` |
| youth_opp_pass | 34ep-6uyj | 96.5K | Has `the_geom` column (dropped); use `category='Total Rides'` only; `month` is timestamp string |
| flexible_fleet | bkj2-54gq | 7.4K | EAV pattern; filter `am_pm='Total' AND weekday_weekend='Total'` for aggregation |

### City of San Diego (CSV, `seshat.datasd.org`)
| Dataset | Rows | Notes |
|---------|------|-------|
| traffic_volumes | 13.7K | Directional counts mutually exclusive (N/S OR E/W) |
| traffic_collisions | 73K | `date_time` as timestamp |
| transit_routes | 838 | Reference table only |

## 12 Aggregated Parquets
| File | Rows | Size | Used By |
|------|------|------|---------|
| ridership_trends | 6 | 577B | Overview, Transit |
| ridership_by_route | 823 | 5KB | Transit, Deep Dive |
| vmt_trends | 672 | 5KB | Overview, Congestion |
| travel_time_trends | 144 | 2KB | Congestion |
| collision_severity | 95 | 1KB | Overview, Safety |
| collision_by_type | 9,381 | 30KB | Safety |
| collision_map_points | 222,267 | 2.3MB | Collision Map |
| city_collision_trends | 12 | 751B | Safety, Deep Dive |
| traffic_volume_trends | 18 | 986B | Congestion |
| traffic_volume_streets | 13,689 | 160KB | Deep Dive |
| youth_pass_trends | 42 | 1KB | Transit |
| flex_fleet_trends | 890 | 4KB | Transit |

## Deployment
- **Streamlit Cloud**: `requirements.txt` at root
- **Render (API)**: `requirements-api.txt`
- **GitHub Actions**: `.github/workflows/refresh.yml` — monthly on the 1st
- **MCP**: `.mcp.json` → `uv run python -m api.mcp_server`
- Parquet files committed to git (2.5MB total)
- Raw data gitignored (3.7GB)
