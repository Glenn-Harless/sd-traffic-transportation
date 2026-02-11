"""Clean and aggregate San Diego traffic/transit data using DuckDB.

Loads 7 Socrata JSON + 3 seshat CSV sources, cleans types, and exports
12 aggregated parquet files for the dashboard.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"
AGGREGATED_DIR = Path(__file__).resolve().parent.parent / "data" / "aggregated"
DB_PATH = Path(__file__).resolve().parent.parent / "db" / "traffic.duckdb"


def transform(*, db_path: Path | None = None) -> None:
    """Load raw data, clean, and export aggregated parquets."""
    db = db_path or DB_PATH
    db.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AGGREGATED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db))

    # ── Phase 1 & 2: Load and clean raw data ──
    _load_transit_ridership(con)
    _load_vmt(con)
    _load_travel_times(con)
    _load_switrs_summary(con)
    _load_switrs_detailed(con)
    _load_youth_opp_pass(con)
    _load_flexible_fleet(con)
    _load_traffic_volumes(con)
    _load_traffic_collisions(con)
    _load_transit_routes(con)

    # ── Phase 3: Export aggregated parquets ──
    _build_aggregations(con)

    con.close()
    print("Transform complete.")


# ── Phase 1 & 2: Load + Clean ──


def _load_transit_ridership(con: duckdb.DuckDBPyConnection) -> None:
    """Transit ridership by route — note column typo `calenadr_year`."""
    path = RAW_DIR / "transit_ridership.json"
    if not path.exists():
        print("  [warn] transit_ridership.json not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS transit_ridership")
    con.execute(f"""
        CREATE TABLE transit_ridership AS
        SELECT
            TRY_CAST(calenadr_year AS INTEGER) AS year,
            route,
            TRY_CAST(average_weekday_boardings AS DOUBLE) AS avg_weekday_boardings
        FROM read_json_auto('{path}')
        WHERE TRY_CAST(calenadr_year AS INTEGER) IS NOT NULL
    """)
    count = con.execute("SELECT count(*) FROM transit_ridership").fetchone()[0]
    print(f"  Loaded transit_ridership: {count:,} rows")


def _load_vmt(con: duckdb.DuckDBPyConnection) -> None:
    """Vehicle miles traveled from PeMS."""
    path = RAW_DIR / "vmt_pems.json"
    if not path.exists():
        print("  [warn] vmt_pems.json not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS vmt")
    con.execute(f"""
        CREATE TABLE vmt AS
        SELECT
            TRY_CAST(year AS INTEGER) AS year,
            peak,
            freeway,
            TRY_CAST(vmt AS DOUBLE) AS vmt
        FROM read_json_auto('{path}')
        WHERE TRY_CAST(year AS INTEGER) IS NOT NULL
    """)
    count = con.execute("SELECT count(*) FROM vmt").fetchone()[0]
    print(f"  Loaded vmt: {count:,} rows")


def _load_travel_times(con: duckdb.DuckDBPyConnection) -> None:
    """Highway travel times."""
    path = RAW_DIR / "highway_travel_times.json"
    if not path.exists():
        print("  [warn] highway_travel_times.json not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS travel_times")
    con.execute(f"""
        CREATE TABLE travel_times AS
        SELECT
            TRY_CAST(year AS INTEGER) AS year,
            route,
            peak,
            TRY_CAST(mean AS DOUBLE) AS mean_minutes
        FROM read_json_auto('{path}')
        WHERE TRY_CAST(year AS INTEGER) IS NOT NULL
    """)
    count = con.execute("SELECT count(*) FROM travel_times").fetchone()[0]
    print(f"  Loaded travel_times: {count:,} rows")


def _load_switrs_summary(con: duckdb.DuckDBPyConnection) -> None:
    """SWITRS collision severity summary."""
    path = RAW_DIR / "switrs_summary.json"
    if not path.exists():
        print("  [warn] switrs_summary.json not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS switrs_summary")
    con.execute(f"""
        CREATE TABLE switrs_summary AS
        SELECT
            TRY_CAST(accident_year AS INTEGER) AS year,
            collision_severity,
            TRY_CAST(number_of_collisions AS INTEGER) AS num_collisions
        FROM read_json_auto('{path}')
        WHERE TRY_CAST(accident_year AS INTEGER) IS NOT NULL
    """)
    count = con.execute("SELECT count(*) FROM switrs_summary").fetchone()[0]
    print(f"  Loaded switrs_summary: {count:,} rows")


def _load_switrs_detailed(con: duckdb.DuckDBPyConnection) -> None:
    """SWITRS detailed collision records with lat/lon.

    Handles literal "NULL" strings and 1900-01-01 time prefix.
    """
    path = RAW_DIR / "switrs_detailed.json"
    if not path.exists():
        print("  [warn] switrs_detailed.json not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS switrs_detailed")
    con.execute(f"""
        CREATE TABLE switrs_detailed AS
        SELECT
            TRY_CAST(accident_year AS INTEGER) AS year,
            NULLIF(collision_severity, 'NULL') AS collision_severity,
            NULLIF(type_of_collision, 'NULL') AS type_of_collision,
            NULLIF(pcf_viol_category, 'NULL') AS pcf_violation_category,
            COALESCE(bicycle_accident::BOOLEAN, FALSE) AS is_bicycle,
            COALESCE(pedestrian_accident::BOOLEAN, FALSE) AS is_pedestrian,
            COALESCE(motorcycle_accident::BOOLEAN, FALSE) AS is_motorcycle,
            NULLIF(weather_1, 'NULL') AS weather,
            NULLIF(lighting, 'NULL') AS lighting,
            TRY_CAST(NULLIF(latitude_sandag, 'NULL') AS DOUBLE) AS latitude,
            TRY_CAST(NULLIF(longitude_sandag, 'NULL') AS DOUBLE) AS longitude,
            TRY_CAST(NULLIF(number_killed, 'NULL') AS INTEGER) AS killed_victims,
            TRY_CAST(NULLIF(number_injured, 'NULL') AS INTEGER) AS injured_victims
        FROM read_json_auto('{path}', maximum_object_size=100000000)
        WHERE TRY_CAST(accident_year AS INTEGER) IS NOT NULL
    """)
    count = con.execute("SELECT count(*) FROM switrs_detailed").fetchone()[0]
    print(f"  Loaded switrs_detailed: {count:,} rows")


def _load_youth_opp_pass(con: duckdb.DuckDBPyConnection) -> None:
    """Youth Opportunity Pass — filter to 'Total Rides' only to avoid double-counting."""
    path = RAW_DIR / "youth_opp_pass.json"
    if not path.exists():
        print("  [warn] youth_opp_pass.json not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS youth_opp_pass")
    con.execute(f"""
        CREATE TABLE youth_opp_pass AS
        SELECT
            route,
            service,
            TRY_CAST(month AS DATE) AS month,
            category,
            TRY_CAST(rides AS DOUBLE) AS rides,
            community,
            vehicle
        FROM read_json_auto('{path}', maximum_object_size=100000000)
    """)
    count = con.execute("SELECT count(*) FROM youth_opp_pass").fetchone()[0]
    print(f"  Loaded youth_opp_pass: {count:,} rows")


def _load_flexible_fleet(con: duckdb.DuckDBPyConnection) -> None:
    """Flexible Fleet — EAV pattern with am_pm and weekday_weekend rollups."""
    path = RAW_DIR / "flexible_fleet.json"
    if not path.exists():
        print("  [warn] flexible_fleet.json not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS flexible_fleet")
    con.execute(f"""
        CREATE TABLE flexible_fleet AS
        SELECT
            month,
            location_name,
            am_pm,
            weekday_weekend,
            TRY_CAST(value AS DOUBLE) AS value,
            category
        FROM read_json_auto('{path}')
    """)
    count = con.execute("SELECT count(*) FROM flexible_fleet").fetchone()[0]
    print(f"  Loaded flexible_fleet: {count:,} rows")


def _load_traffic_volumes(con: duckdb.DuckDBPyConnection) -> None:
    """City of San Diego traffic volume counts."""
    path = RAW_DIR / "traffic_volumes.csv"
    if not path.exists():
        print("  [warn] traffic_volumes.csv not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS traffic_volumes")
    con.execute(f"""
        CREATE TABLE traffic_volumes AS
        SELECT
            street_name,
            limits,
            TRY_CAST(total_count AS INTEGER) AS total_count,
            TRY_CAST(date_count AS DATE) AS date_count,
            YEAR(TRY_CAST(date_count AS DATE)) AS year
        FROM read_csv('{path}', header=true, ignore_errors=true)
        WHERE TRY_CAST(total_count AS INTEGER) IS NOT NULL
    """)
    count = con.execute("SELECT count(*) FROM traffic_volumes").fetchone()[0]
    print(f"  Loaded traffic_volumes: {count:,} rows")


def _load_traffic_collisions(con: duckdb.DuckDBPyConnection) -> None:
    """City of San Diego police-reported traffic collisions."""
    path = RAW_DIR / "traffic_collisions.csv"
    if not path.exists():
        print("  [warn] traffic_collisions.csv not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS city_collisions")
    con.execute(f"""
        CREATE TABLE city_collisions AS
        SELECT
            report_id,
            TRY_CAST(date_time AS TIMESTAMP) AS date_time,
            YEAR(TRY_CAST(date_time AS TIMESTAMP)) AS year,
            police_beat,
            address_road_primary,
            charge_desc,
            TRY_CAST(injured AS INTEGER) AS injured,
            TRY_CAST(killed AS INTEGER) AS killed
        FROM read_csv('{path}', header=true, ignore_errors=true)
        WHERE TRY_CAST(date_time AS TIMESTAMP) IS NOT NULL
    """)
    count = con.execute("SELECT count(*) FROM city_collisions").fetchone()[0]
    print(f"  Loaded city_collisions: {count:,} rows")


def _load_transit_routes(con: duckdb.DuckDBPyConnection) -> None:
    """City of San Diego transit route reference table."""
    path = RAW_DIR / "transit_routes.csv"
    if not path.exists():
        print("  [warn] transit_routes.csv not found, skipping")
        return
    con.execute("DROP TABLE IF EXISTS transit_routes")
    con.execute(f"""
        CREATE TABLE transit_routes AS
        SELECT *
        FROM read_csv('{path}', header=true, ignore_errors=true)
    """)
    count = con.execute("SELECT count(*) FROM transit_routes").fetchone()[0]
    print(f"  Loaded transit_routes: {count:,} rows")


# ── Phase 3: Aggregated Parquets ──


def _build_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Build 12 pre-computed parquet files for the dashboard."""

    # 1. ridership_trends — year-level totals
    _try_agg(con, "ridership_trends", f"""
        SELECT
            year,
            SUM(avg_weekday_boardings) AS total_weekday_boardings,
            COUNT(DISTINCT route) AS num_routes
        FROM transit_ridership
        GROUP BY year
        ORDER BY year
    """)

    # 2. ridership_by_route — year × route
    _try_agg(con, "ridership_by_route", f"""
        SELECT
            year,
            route,
            avg_weekday_boardings
        FROM transit_ridership
        ORDER BY year, route
    """)

    # 3. vmt_trends — year × peak × freeway
    _try_agg(con, "vmt_trends", f"""
        SELECT
            year,
            peak,
            freeway,
            vmt
        FROM vmt
        ORDER BY year, peak, freeway
    """)

    # 4. travel_time_trends — year × route × peak
    _try_agg(con, "travel_time_trends", f"""
        SELECT
            year,
            route,
            peak,
            mean_minutes
        FROM travel_times
        ORDER BY year, route, peak
    """)

    # 5. collision_severity — year × severity from SWITRS summary
    _try_agg(con, "collision_severity", f"""
        SELECT
            year,
            collision_severity,
            num_collisions
        FROM switrs_summary
        ORDER BY year, collision_severity
    """)

    # 6. collision_by_type — aggregated from SWITRS detailed
    _try_agg(con, "collision_by_type", f"""
        SELECT
            year,
            collision_severity,
            type_of_collision,
            is_bicycle,
            is_pedestrian,
            is_motorcycle,
            weather,
            lighting,
            COUNT(*) AS num_collisions,
            SUM(killed_victims) AS total_killed,
            SUM(injured_victims) AS total_injured
        FROM switrs_detailed
        GROUP BY year, collision_severity, type_of_collision,
                 is_bicycle, is_pedestrian, is_motorcycle, weather, lighting
        ORDER BY year, num_collisions DESC
    """)

    # 7. collision_map_points — per-record lat/lon from SWITRS detailed
    _try_agg(con, "collision_map_points", f"""
        SELECT
            year,
            collision_severity,
            type_of_collision,
            is_bicycle,
            is_pedestrian,
            is_motorcycle,
            latitude,
            longitude,
            killed_victims,
            injured_victims
        FROM switrs_detailed
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude BETWEEN 32.5 AND 33.3
          AND longitude BETWEEN -117.7 AND -116.8
    """)

    # 8. city_collision_trends — year-level from city collisions
    _try_agg(con, "city_collision_trends", f"""
        SELECT
            year,
            COUNT(*) AS num_collisions,
            SUM(injured) AS total_injured,
            SUM(killed) AS total_killed
        FROM city_collisions
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)

    # 9. traffic_volume_trends — year-level averages
    _try_agg(con, "traffic_volume_trends", f"""
        SELECT
            year,
            COUNT(*) AS num_counts,
            AVG(total_count) AS avg_daily_traffic,
            SUM(total_count) AS total_volume
        FROM traffic_volumes
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)

    # 10. traffic_volume_streets — street × year detail
    _try_agg(con, "traffic_volume_streets", f"""
        SELECT
            street_name,
            limits,
            year,
            total_count,
            date_count
        FROM traffic_volumes
        WHERE year IS NOT NULL
        ORDER BY total_count DESC
    """)

    # 11. youth_pass_trends — monthly totals (Total Rides only)
    _try_agg(con, "youth_pass_trends", f"""
        SELECT
            month,
            SUM(rides) AS total_rides,
            COUNT(DISTINCT route) AS num_routes,
            COUNT(DISTINCT community) AS num_communities
        FROM youth_opp_pass
        WHERE category = 'Total Rides'
        GROUP BY month
        ORDER BY month
    """)

    # 12. youth_pass_communities — rides by community (Total Rides only)
    _try_agg(con, "youth_pass_communities", f"""
        SELECT
            community,
            SUM(rides) AS total_rides
        FROM youth_opp_pass
        WHERE category = 'Total Rides'
          AND community IS NOT NULL
        GROUP BY community
        ORDER BY total_rides DESC
    """)

    # 13. flex_fleet_trends — month × location × category (Total rollups)
    _try_agg(con, "flex_fleet_trends", f"""
        SELECT
            month,
            location_name,
            category,
            SUM(value) AS total_value
        FROM flexible_fleet
        WHERE am_pm = 'Total'
          AND weekday_weekend = 'Total'
        GROUP BY month, location_name, category
        ORDER BY month, location_name, category
    """)


def _try_agg(con: duckdb.DuckDBPyConnection, name: str, sql: str) -> None:
    """Export an aggregation to parquet, handling missing tables gracefully."""
    dest = AGGREGATED_DIR / f"{name}.parquet"
    try:
        con.execute(f"COPY ({sql}) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        size_kb = dest.stat().st_size / 1024
        count = con.execute(f"SELECT count(*) FROM '{dest}'").fetchone()[0]
        print(f"  [agg] {name}: {count:,} rows ({size_kb:.0f} KB)")
    except duckdb.CatalogException as e:
        print(f"  [warn] {name}: skipped — {e}")


if __name__ == "__main__":
    transform()
