"""Shared query layer — all SQL lives here.

Both the FastAPI endpoints and MCP tools call these functions.
Each function creates a fresh DuckDB connection, queries parquet files,
and returns list[dict] (or dict for single-row responses).
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
_AGG = str(_ROOT / "data" / "aggregated")


def _q(where: str, condition: str) -> str:
    """Append a condition to a WHERE clause safely."""
    if not where:
        return f"WHERE {condition}"
    return f"{where} AND {condition}"


def _where(
    year_min: int | None,
    year_max: int | None,
    peak: str | None = None,
) -> str:
    """Build a WHERE clause from optional filter params."""
    clauses: list[str] = []
    if year_min is not None:
        clauses.append(f"year >= {int(year_min)}")
    if year_max is not None:
        clauses.append(f"year <= {int(year_max)}")
    if peak:
        clauses.append(f"peak = '{peak.replace(chr(39), chr(39)*2)}'")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def _run(sql: str) -> list[dict]:
    """Execute SQL and return list of row dicts."""
    con = duckdb.connect()
    df = con.execute(sql).fetchdf()
    con.close()
    return df.to_dict(orient="records")


# ── 1. Filter options ──

def get_filter_options() -> dict:
    """Available years, routes, freeways, severities, peaks."""
    con = duckdb.connect()
    years = sorted(set(
        r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM '{_AGG}/ridership_trends.parquet' WHERE year IS NOT NULL"
        ).fetchall()
    ) | set(
        r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM '{_AGG}/vmt_trends.parquet' WHERE year IS NOT NULL"
        ).fetchall()
    ) | set(
        r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM '{_AGG}/collision_severity.parquet' WHERE year IS NOT NULL"
        ).fetchall()
    ))
    routes = sorted(
        r[0] for r in con.execute(
            f"SELECT DISTINCT route FROM '{_AGG}/ridership_by_route.parquet' WHERE route IS NOT NULL ORDER BY route"
        ).fetchall()
    )
    freeways = sorted(
        r[0] for r in con.execute(
            f"SELECT DISTINCT freeway FROM '{_AGG}/vmt_trends.parquet' WHERE freeway IS NOT NULL ORDER BY freeway"
        ).fetchall()
    )
    severities = sorted(
        r[0] for r in con.execute(
            f"SELECT DISTINCT collision_severity FROM '{_AGG}/collision_severity.parquet' WHERE collision_severity IS NOT NULL"
        ).fetchall()
    )
    peaks = sorted(
        r[0] for r in con.execute(
            f"SELECT DISTINCT peak FROM '{_AGG}/travel_time_trends.parquet' WHERE peak IS NOT NULL"
        ).fetchall()
    )
    con.close()
    return {
        "years": [int(y) for y in years],
        "routes": routes,
        "freeways": freeways,
        "severities": severities,
        "peaks": peaks,
    }


# ── 2. Overview ──

def get_overview(year_min: int = 2019, year_max: int = 2024) -> dict:
    """Headline KPIs across all datasets."""
    con = duckdb.connect()
    w = _where(year_min, year_max)

    boardings = con.execute(
        f"SELECT SUM(total_weekday_boardings) AS v FROM '{_AGG}/ridership_trends.parquet' {w}"
    ).fetchone()[0] or 0

    vmt = con.execute(
        f"SELECT SUM(vmt) AS v FROM '{_AGG}/vmt_trends.parquet' {w}"
    ).fetchone()[0] or 0

    collisions = con.execute(
        f"SELECT SUM(num_collisions) AS v FROM '{_AGG}/collision_severity.parquet' {w}"
    ).fetchone()[0] or 0

    fatal_w = _q(w, "collision_severity = 'Fatal'")
    fatalities = con.execute(
        f"SELECT SUM(num_collisions) AS v FROM '{_AGG}/collision_severity.parquet' {fatal_w}"
    ).fetchone()[0] or 0

    con.close()
    return {
        "total_weekday_boardings": float(boardings),
        "total_vmt": float(vmt),
        "total_collisions": int(collisions),
        "total_fatalities": int(fatalities),
    }


# ── 3. Ridership trends ──

def get_ridership_trends(year_min: int = 2019, year_max: int = 2024) -> list[dict]:
    """Yearly ridership totals."""
    w = _where(year_min, year_max)
    return _run(
        f"SELECT year, total_weekday_boardings, num_routes "
        f"FROM '{_AGG}/ridership_trends.parquet' {w} ORDER BY year"
    )


# ── 4. Ridership by route ──

def get_ridership_by_route(
    year_min: int = 2019,
    year_max: int = 2024,
    route: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Per-route boardings, optionally filtered."""
    w = _where(year_min, year_max)
    if route:
        w = _q(w, f"route = '{route.replace(chr(39), chr(39)*2)}'")
    return _run(
        f"SELECT year, route, avg_weekday_boardings "
        f"FROM '{_AGG}/ridership_by_route.parquet' {w} "
        f"ORDER BY avg_weekday_boardings DESC LIMIT {int(limit)}"
    )


# ── 5. VMT ──

def get_vmt(
    year_min: int = 2013,
    year_max: int = 2024,
    peak: str | None = None,
    freeway: str | None = None,
) -> list[dict]:
    """VMT by freeway/peak/year."""
    w = _where(year_min, year_max, peak)
    if freeway:
        w = _q(w, f"freeway = '{freeway.replace(chr(39), chr(39)*2)}'")
    return _run(
        f"SELECT year, peak, freeway, vmt "
        f"FROM '{_AGG}/vmt_trends.parquet' {w} ORDER BY year, freeway"
    )


# ── 6. Travel times ──

def get_travel_times(
    year_min: int = 2019,
    year_max: int = 2024,
    peak: str | None = None,
    route: str | None = None,
) -> list[dict]:
    """Travel times by route/peak/year."""
    w = _where(year_min, year_max, peak)
    if route:
        w = _q(w, f"route = '{route.replace(chr(39), chr(39)*2)}'")
    return _run(
        f"SELECT year, route, peak, mean_minutes "
        f"FROM '{_AGG}/travel_time_trends.parquet' {w} ORDER BY year, route"
    )


# ── 7. Collision summary (SWITRS) ──

def get_collision_summary(
    year_min: int = 2006,
    year_max: int = 2024,
    severity: str | None = None,
) -> list[dict]:
    """Collision severity trends from SWITRS summary."""
    w = _where(year_min, year_max)
    if severity:
        w = _q(w, f"collision_severity = '{severity.replace(chr(39), chr(39)*2)}'")
    return _run(
        f"SELECT year, collision_severity, num_collisions "
        f"FROM '{_AGG}/collision_severity.parquet' {w} ORDER BY year"
    )


# ── 8. Collision detail ──

def get_collision_detail(
    year_min: int = 2013,
    year_max: int = 2022,
    bicycle: bool | None = None,
    pedestrian: bool | None = None,
    limit: int = 50,
) -> list[dict]:
    """Collision breakdown by type/mode from SWITRS detailed."""
    w = _where(year_min, year_max)
    if bicycle is True:
        w = _q(w, "is_bicycle = TRUE")
    if pedestrian is True:
        w = _q(w, "is_pedestrian = TRUE")
    return _run(
        f"SELECT year, collision_severity, type_of_collision, "
        f"  is_bicycle, is_pedestrian, is_motorcycle, "
        f"  weather, lighting, num_collisions, total_killed, total_injured "
        f"FROM '{_AGG}/collision_by_type.parquet' {w} "
        f"ORDER BY num_collisions DESC LIMIT {int(limit)}"
    )


# ── 9. City collision trends ──

def get_city_collision_trends(
    year_min: int = 2015,
    year_max: int = 2026,
) -> list[dict]:
    """City of SD police-reported collision trends."""
    w = _where(year_min, year_max)
    return _run(
        f"SELECT year, num_collisions, total_injured, total_killed "
        f"FROM '{_AGG}/city_collision_trends.parquet' {w} ORDER BY year"
    )


# ── 10. Youth pass trends ──

def get_youth_pass_trends() -> list[dict]:
    """YOP monthly totals."""
    return _run(
        f"SELECT month, total_rides, num_routes, num_communities "
        f"FROM '{_AGG}/youth_pass_trends.parquet' ORDER BY month"
    )


# ── 11. Youth pass communities ──

def get_youth_pass_communities() -> list[dict]:
    """YOP rides by community."""
    pq_path = Path(f"{_AGG}/youth_pass_communities.parquet")
    if not pq_path.exists():
        # Fallback to raw JSON if parquet not yet generated
        path = _ROOT / "data" / "raw" / "youth_opp_pass.json"
        if not path.exists():
            return []
        con = duckdb.connect()
        df = con.execute(f"""
            SELECT community, SUM(TRY_CAST(rides AS DOUBLE)) AS total_rides
            FROM read_json_auto('{path}', maximum_object_size=100000000)
            WHERE category = 'Total Rides'
              AND community IS NOT NULL
            GROUP BY community
            ORDER BY total_rides DESC
            LIMIT 25
        """).fetchdf()
        con.close()
        return df.to_dict(orient="records")
    return _run(f"""
        SELECT community, total_rides
        FROM '{pq_path}'
        ORDER BY total_rides DESC
        LIMIT 25
    """)


# ── 12. Flex fleet ──

def get_flex_fleet(
    location: str | None = None,
    category: str | None = None,
) -> list[dict]:
    """Flex fleet by location/category."""
    w = ""
    clauses = []
    if location:
        clauses.append(f"location_name = '{location.replace(chr(39), chr(39)*2)}'")
    if category:
        clauses.append(f"category = '{category.replace(chr(39), chr(39)*2)}'")
    if clauses:
        w = "WHERE " + " AND ".join(clauses)
    return _run(
        f"SELECT month, location_name, category, total_value "
        f"FROM '{_AGG}/flex_fleet_trends.parquet' {w} ORDER BY month"
    )


# ── 13. Traffic volumes ──

def get_traffic_volumes(
    year_min: int = 2005,
    year_max: int = 2022,
    limit: int = 25,
) -> list[dict]:
    """Top streets by traffic volume."""
    w = _where(year_min, year_max)
    return _run(
        f"SELECT street_name, limits, year, total_count "
        f"FROM '{_AGG}/traffic_volume_streets.parquet' {w} "
        f"ORDER BY total_count DESC LIMIT {int(limit)}"
    )
