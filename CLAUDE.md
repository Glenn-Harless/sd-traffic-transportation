# SD Traffic & Transportation — Regional Mobility Dashboard

## Project Overview
San Diego traffic congestion, transit ridership recovery, collision safety, and new mobility programs.
10 datasets from SANDAG Socrata API and City of San Diego open data portal.

## Architecture — Follow sd-city-budget Pattern

### Project Structure
```
pipeline/       # Data ingestion + transformation
data/raw/       # Raw source data (gitignored)
data/processed/ # Cleaned parquets
data/aggregated/# Pre-aggregated parquets for dashboard (12 files)
dashboard/      # Streamlit app (6 tabs)
api/            # FastAPI + MCP server
```

### Dashboard Rules
- **Use DuckDB for all data access** — no Polars/pandas for loading full datasets. Streamlit Cloud has 1GB RAM limit.
- `query()` helper: fresh `duckdb.connect()` per call, returns pandas DataFrame.
- Shared `_where_clause()` for sidebar filters across all tabs.
- Each query should return small aggregated DataFrames (~10-50 rows).
- `requirements.txt` at project root for Streamlit Cloud (not pyproject.toml).

### Pipeline
- Use DuckDB for transforms (consistent with other SD civic data projects)
- `uv` for dependency management, `pyproject.toml` for project config
- Data sources: SANDAG Socrata API (JSON) + seshat.datasd.org (CSV)

### Data Quality Notes
- All Socrata fields arrive as strings — must TRY_CAST everything
- SWITRS detailed has literal `"NULL"` strings — use NULLIF
- Transit ridership has column typo: `calenadr_year` not `calendar_year`
- Youth Opportunity Pass: use only `category = 'Total Rides'` to avoid double-counting
- Flexible Fleet: EAV pattern, filter to am_pm='Total' AND weekday_weekend='Total' for aggregation

### Deployment
- GitHub Actions for monthly data refresh
- `.gitignore`: use `dir/*` pattern (not `dir/`) when negation exceptions are needed
- Parquet files under 100MB can be committed directly to git
