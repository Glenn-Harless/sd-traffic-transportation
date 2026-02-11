"""FastAPI app — thin wrappers around the shared query layer."""

from __future__ import annotations

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from api import queries
from api.models import (
    CityCollisionTrend,
    CollisionDetail,
    CollisionSummary,
    FilterOptions,
    FlexFleetRecord,
    OverviewResponse,
    RidershipRoute,
    RidershipTrend,
    TravelTime,
    TrafficVolume,
    VMTRecord,
    YouthPassCommunity,
    YouthPassTrend,
)

app = FastAPI(
    title="San Diego Traffic & Transportation API",
    description=(
        "Query San Diego traffic congestion, transit ridership, collision safety, "
        "and new mobility data. Sources: SANDAG and City of San Diego open data."
    ),
    version="0.1.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/")
def root():
    return {
        "message": "San Diego Traffic & Transportation API",
        "docs": "/docs",
        "endpoints": [
            "/health", "/filters", "/overview",
            "/ridership/trends", "/ridership/routes",
            "/congestion/vmt", "/congestion/travel-times",
            "/safety/summary", "/safety/detailed", "/safety/city-trends",
            "/transit/youth-pass", "/transit/youth-pass/communities",
            "/transit/flex-fleet", "/traffic-volumes",
        ],
    }


@app.get("/health")
def health():
    """Check parquet file availability."""
    from pathlib import Path
    agg = Path(__file__).resolve().parent.parent / "data" / "aggregated"
    expected = [
        "ridership_trends", "ridership_by_route", "vmt_trends",
        "travel_time_trends", "collision_severity", "collision_by_type",
        "collision_map_points", "city_collision_trends",
        "traffic_volume_trends", "traffic_volume_streets",
        "youth_pass_trends", "flex_fleet_trends",
    ]
    status = {name: (agg / f"{name}.parquet").exists() for name in expected}
    all_ok = all(status.values())
    return {"status": "ok" if all_ok else "degraded", "files": status}


@app.get("/filters", response_model=FilterOptions)
def filters():
    """Available years, routes, freeways, severities, and peaks."""
    return queries.get_filter_options()


@app.get("/overview", response_model=OverviewResponse)
def overview(
    year_min: int = Query(2019, description="Start year"),
    year_max: int = Query(2024, description="End year"),
):
    """Headline KPIs: boardings, VMT, collisions, fatalities."""
    return queries.get_overview(year_min, year_max)


@app.get("/ridership/trends", response_model=list[RidershipTrend])
def ridership_trends(
    year_min: int = Query(2019, description="Start year"),
    year_max: int = Query(2024, description="End year"),
):
    """Yearly ridership totals."""
    return queries.get_ridership_trends(year_min, year_max)


@app.get("/ridership/routes", response_model=list[RidershipRoute])
def ridership_routes(
    year_min: int = Query(2019, description="Start year"),
    year_max: int = Query(2024, description="End year"),
    route: str | None = Query(None, description="Filter by route number"),
    limit: int = Query(20, ge=1, le=500, description="Max rows"),
):
    """Per-route boardings."""
    return queries.get_ridership_by_route(year_min, year_max, route, limit)


@app.get("/congestion/vmt", response_model=list[VMTRecord])
def vmt(
    year_min: int = Query(2013, description="Start year"),
    year_max: int = Query(2024, description="End year"),
    peak: str | None = Query(None, description="AM or PM"),
    freeway: str | None = Query(None, description="Filter by freeway"),
):
    """VMT by freeway/peak/year."""
    return queries.get_vmt(year_min, year_max, peak, freeway)


@app.get("/congestion/travel-times", response_model=list[TravelTime])
def travel_times(
    year_min: int = Query(2019, description="Start year"),
    year_max: int = Query(2024, description="End year"),
    peak: str | None = Query(None, description="AM or PM"),
    route: str | None = Query(None, description="Filter by route"),
):
    """Travel times by route/peak/year."""
    return queries.get_travel_times(year_min, year_max, peak, route)


@app.get("/safety/summary", response_model=list[CollisionSummary])
def safety_summary(
    year_min: int = Query(2006, description="Start year"),
    year_max: int = Query(2024, description="End year"),
    severity: str | None = Query(None, description="Filter by severity"),
):
    """SWITRS collision severity trends."""
    return queries.get_collision_summary(year_min, year_max, severity)


@app.get("/safety/detailed", response_model=list[CollisionDetail])
def safety_detailed(
    year_min: int = Query(2013, description="Start year"),
    year_max: int = Query(2022, description="End year"),
    bicycle: bool | None = Query(None, description="Filter bicycle collisions"),
    pedestrian: bool | None = Query(None, description="Filter pedestrian collisions"),
    limit: int = Query(50, ge=1, le=500, description="Max rows"),
):
    """SWITRS detailed collision breakdown."""
    return queries.get_collision_detail(year_min, year_max, bicycle, pedestrian, limit)


@app.get("/safety/city-trends", response_model=list[CityCollisionTrend])
def city_trends(
    year_min: int = Query(2015, description="Start year"),
    year_max: int = Query(2026, description="End year"),
):
    """City of SD police-reported collision trends."""
    return queries.get_city_collision_trends(year_min, year_max)


@app.get("/transit/youth-pass", response_model=list[YouthPassTrend])
def youth_pass():
    """Youth Opportunity Pass monthly totals."""
    return queries.get_youth_pass_trends()


@app.get("/transit/youth-pass/communities", response_model=list[YouthPassCommunity])
def youth_pass_communities():
    """YOP rides by community."""
    return queries.get_youth_pass_communities()


@app.get("/transit/flex-fleet", response_model=list[FlexFleetRecord])
def flex_fleet(
    location: str | None = Query(None, description="Filter by location"),
    category: str | None = Query(None, description="Filter by category"),
):
    """Flexible Fleet by location/category."""
    return queries.get_flex_fleet(location, category)


@app.get("/traffic-volumes", response_model=list[TrafficVolume])
def traffic_volumes(
    year_min: int = Query(2005, description="Start year"),
    year_max: int = Query(2022, description="End year"),
    limit: int = Query(25, ge=1, le=500, description="Max rows"),
):
    """Top streets by traffic volume."""
    return queries.get_traffic_volumes(year_min, year_max, limit)
