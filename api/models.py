"""Pydantic response models for FastAPI's auto-generated OpenAPI docs."""

from __future__ import annotations

from pydantic import BaseModel


class FilterOptions(BaseModel):
    years: list[int]
    routes: list[str]
    freeways: list[str]
    severities: list[str]
    peaks: list[str]


class OverviewResponse(BaseModel):
    total_weekday_boardings: float
    total_vmt: float
    total_collisions: int
    total_fatalities: int


class RidershipTrend(BaseModel):
    year: int
    total_weekday_boardings: float
    num_routes: int


class RidershipRoute(BaseModel):
    year: int
    route: str
    avg_weekday_boardings: float


class VMTRecord(BaseModel):
    year: int
    peak: str | None
    freeway: str | None
    vmt: float


class TravelTime(BaseModel):
    year: int
    route: str | None
    peak: str | None
    mean_minutes: float


class CollisionSummary(BaseModel):
    year: int
    collision_severity: str | None
    num_collisions: int


class CollisionDetail(BaseModel):
    year: int
    collision_severity: str | None
    type_of_collision: str | None
    is_bicycle: bool
    is_pedestrian: bool
    is_motorcycle: bool
    weather: str | None
    lighting: str | None
    num_collisions: int
    total_killed: int | None
    total_injured: int | None


class CityCollisionTrend(BaseModel):
    year: int
    num_collisions: int
    total_injured: int | None
    total_killed: int | None


class YouthPassTrend(BaseModel):
    month: str
    total_rides: float
    num_routes: int
    num_communities: int


class YouthPassCommunity(BaseModel):
    community: str
    total_rides: float


class FlexFleetRecord(BaseModel):
    month: str
    location_name: str | None
    category: str | None
    total_value: float | None


class TrafficVolume(BaseModel):
    street_name: str | None
    limits: str | None
    year: int
    total_count: int
