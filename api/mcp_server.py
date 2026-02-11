"""MCP server for San Diego traffic & transportation data.

Exposes 10 tools that let Claude query traffic/transit parquet files directly.
Uses FastMCP (v2) with stdio transport — spawned by Claude Code as a subprocess.
"""

from __future__ import annotations

from fastmcp import FastMCP

from api import queries

mcp = FastMCP(
    "San Diego Traffic & Transportation",
    instructions=(
        "San Diego regional traffic, transit, and safety data. "
        "Covers transit ridership (2019-2024), VMT (2013-2024), highway travel times, "
        "SWITRS collisions (2006-2024), city collisions (2015-2026), Youth Opportunity Pass, "
        "and Flexible Fleet microtransit. Call get_filter_options first to see available values."
    ),
)


@mcp.tool()
def get_filter_options() -> dict:
    """Get available filter values: years, routes, freeways, severities, peaks.

    Call this first to see what values are valid for other tools.
    """
    return queries.get_filter_options()


@mcp.tool()
def get_overview(
    year_min: int = 2019,
    year_max: int = 2024,
) -> dict:
    """Get headline KPIs: total weekday boardings, VMT, collisions, fatalities.

    Returns totals across the given year range.
    """
    return queries.get_overview(year_min, year_max)


@mcp.tool()
def get_ridership_trends(
    year_min: int = 2019,
    year_max: int = 2024,
) -> list[dict]:
    """Get yearly transit ridership totals.

    Returns year, total_weekday_boardings, num_routes.
    """
    return queries.get_ridership_trends(year_min, year_max)


@mcp.tool()
def get_ridership_by_route(
    year_min: int = 2019,
    year_max: int = 2024,
    route: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """Get per-route transit boardings, optionally filtered by route number.

    Returns year, route, avg_weekday_boardings.
    """
    return queries.get_ridership_by_route(year_min, year_max, route, limit)


@mcp.tool()
def get_vmt(
    year_min: int = 2013,
    year_max: int = 2024,
    peak: str | None = None,
    freeway: str | None = None,
) -> list[dict]:
    """Get vehicle miles traveled by freeway and peak period.

    Returns year, peak, freeway, vmt. Filter by peak (AM/PM) or freeway name.
    """
    return queries.get_vmt(year_min, year_max, peak, freeway)


@mcp.tool()
def get_travel_times(
    year_min: int = 2019,
    year_max: int = 2024,
    peak: str | None = None,
    route: str | None = None,
) -> list[dict]:
    """Get highway travel times by route and peak period.

    Returns year, route, peak, mean_minutes.
    """
    return queries.get_travel_times(year_min, year_max, peak, route)


@mcp.tool()
def get_collision_summary(
    year_min: int = 2006,
    year_max: int = 2024,
    severity: str | None = None,
) -> list[dict]:
    """Get SWITRS collision severity trends.

    Returns year, collision_severity, num_collisions.
    Severities include: Fatal, Injury (Severe), Injury (Other Visible),
    Injury (Complaint of Pain), Property Damage Only.
    """
    return queries.get_collision_summary(year_min, year_max, severity)


@mcp.tool()
def get_collision_detail(
    year_min: int = 2013,
    year_max: int = 2022,
    bicycle: bool | None = None,
    pedestrian: bool | None = None,
    limit: int = 50,
) -> list[dict]:
    """Get detailed collision breakdown by type, mode, weather, and lighting.

    Set bicycle=True or pedestrian=True to filter. Returns counts by combination.
    """
    return queries.get_collision_detail(year_min, year_max, bicycle, pedestrian, limit)


@mcp.tool()
def get_youth_pass_trends() -> list[dict]:
    """Get Youth Opportunity Pass monthly ridership totals.

    The YOP provides free transit to youth (ages 6-18) in San Diego.
    Returns month, total_rides, num_routes, num_communities.
    """
    return queries.get_youth_pass_trends()


@mcp.tool()
def get_youth_pass_communities() -> list[dict]:
    """Get Youth Opportunity Pass rides by community.

    Returns top 25 communities by total rides.
    """
    return queries.get_youth_pass_communities()


def main():
    mcp.run()


if __name__ == "__main__":
    main()
