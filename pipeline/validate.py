"""Data validation checks for SD Traffic & Transportation pipeline outputs.

Run after the pipeline to catch data quality issues before publishing.

Usage:
    uv run python -m pipeline.validate
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

AGG = Path(__file__).resolve().parent.parent / "data" / "aggregated"

passed = 0
failed = 0
warnings = 0


def _check(name: str, ok: bool, detail: str = "") -> None:
    global passed, failed
    if ok:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        msg = f"  FAIL  {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)


def _warn(name: str, detail: str) -> None:
    global warnings
    warnings += 1
    print(f"  WARN  {name} — {detail}")


def validate() -> int:
    """Run all validation checks. Returns number of failures."""
    global passed, failed, warnings
    passed = 0
    failed = 0
    warnings = 0

    con = duckdb.connect()

    print("=" * 60)
    print("Data Validation — SD Traffic & Transportation")
    print("=" * 60)

    # ── 1. File existence ──
    print("\n-- 1. File existence --")
    expected_aggs = [
        "ridership_trends", "ridership_by_route", "vmt_trends",
        "travel_time_trends", "collision_severity", "collision_by_type",
        "collision_map_points", "city_collision_trends",
        "traffic_volume_trends", "traffic_volume_streets",
        "youth_pass_trends", "flex_fleet_trends",
    ]
    for name in expected_aggs:
        path = AGG / f"{name}.parquet"
        _check(f"{name}.parquet exists", path.exists())

    # ── 2. Row counts (non-empty) ──
    print("\n-- 2. Row counts --")
    for name in expected_aggs:
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        count = con.execute(f"SELECT count(*) FROM '{path}'").fetchone()[0]
        _check(f"{name} has rows", count > 0, f"got {count:,} rows")

    # ── 3. Year range sanity ──
    print("\n-- 3. Year range sanity --")
    year_checks = {
        "ridership_trends": (2019, 2024),
        "vmt_trends": (2013, 2024),
        "travel_time_trends": (2019, 2024),
        "collision_severity": (2006, 2024),
        "city_collision_trends": (2015, 2026),
        "traffic_volume_trends": (2005, 2022),
    }
    for name, (expected_min, expected_max) in year_checks.items():
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        yr_range = con.execute(f"SELECT MIN(year), MAX(year) FROM '{path}'").fetchone()
        min_yr, max_yr = yr_range
        _check(
            f"{name} year range",
            min_yr is not None and min_yr <= expected_min + 2 and max_yr >= expected_max - 2,
            f"got {min_yr}-{max_yr}, expected ~{expected_min}-{expected_max}",
        )

    # ── 4. Numeric sanity ──
    print("\n-- 4. Numeric sanity --")
    # Boardings should be > 0
    path = AGG / "ridership_trends.parquet"
    if path.exists():
        neg = con.execute(f"""
            SELECT count(*) FROM '{path}' WHERE total_weekday_boardings < 0
        """).fetchone()[0]
        _check("No negative boardings", neg == 0, f"found {neg}")

    # VMT should be > 0
    path = AGG / "vmt_trends.parquet"
    if path.exists():
        neg = con.execute(f"""
            SELECT count(*) FROM '{path}' WHERE vmt < 0
        """).fetchone()[0]
        _check("No negative VMT", neg == 0, f"found {neg}")

    # No negative killed/injured in city collisions
    path = AGG / "city_collision_trends.parquet"
    if path.exists():
        neg = con.execute(f"""
            SELECT count(*) FROM '{path}' WHERE total_killed < 0 OR total_injured < 0
        """).fetchone()[0]
        _check("No negative killed/injured", neg == 0, f"found {neg}")

    # ── 5. Geographic bounds on collision_map_points ──
    print("\n-- 5. Geographic bounds --")
    path = AGG / "collision_map_points.parquet"
    if path.exists():
        out_of_bounds = con.execute(f"""
            SELECT count(*) FROM '{path}'
            WHERE latitude NOT BETWEEN 32.5 AND 33.3
               OR longitude NOT BETWEEN -117.7 AND -116.8
        """).fetchone()[0]
        _check("All collision points within SD bounds", out_of_bounds == 0,
               f"{out_of_bounds} out of bounds")

    # ── 6. No literal "NULL" strings remaining ──
    print("\n-- 6. No literal NULL strings --")
    path = AGG / "collision_by_type.parquet"
    if path.exists():
        null_str = con.execute(f"""
            SELECT count(*) FROM '{path}'
            WHERE collision_severity = 'NULL'
               OR type_of_collision = 'NULL'
               OR weather = 'NULL'
               OR lighting = 'NULL'
        """).fetchone()[0]
        _check("No literal 'NULL' strings in collision_by_type", null_str == 0,
               f"found {null_str}")

    # ── 7. No double-counting ──
    print("\n-- 7. Double-counting guards --")
    # Youth pass should only have Total Rides
    path = AGG / "youth_pass_trends.parquet"
    if path.exists():
        row_count = con.execute(f"SELECT count(*) FROM '{path}'").fetchone()[0]
        _check("youth_pass_trends has reasonable rows (< 100 months)", row_count < 100,
               f"got {row_count}")

    # Flex fleet should be filtered to Total/Total
    path = AGG / "flex_fleet_trends.parquet"
    if path.exists():
        row_count = con.execute(f"SELECT count(*) FROM '{path}'").fetchone()[0]
        _check("flex_fleet_trends has reasonable rows (< 5000)", row_count < 5000,
               f"got {row_count}")

    # ── 8. Column existence matches queries.py guards ──
    print("\n-- 8. Column compatibility --")
    col_checks = {
        "ridership_trends": ["year", "total_weekday_boardings"],
        "vmt_trends": ["year", "peak", "freeway", "vmt"],
        "travel_time_trends": ["year", "route", "peak", "mean_minutes"],
        "collision_severity": ["year", "collision_severity", "num_collisions"],
        "collision_map_points": ["year", "latitude", "longitude", "collision_severity"],
        "city_collision_trends": ["year", "num_collisions", "total_killed", "total_injured"],
        "youth_pass_trends": ["month", "total_rides"],
        "flex_fleet_trends": ["month", "location_name", "category", "total_value"],
    }
    for name, expected_cols in col_checks.items():
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()]
        for col in expected_cols:
            _check(f"{name} has column '{col}'", col in cols, f"columns: {cols}")

    # ── 9. File sizes ──
    print("\n-- 9. File sizes --")
    for name in expected_aggs:
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        size_mb = path.stat().st_size / (1024 * 1024)
        limit = 50 if name == "collision_map_points" else 10
        _check(f"{name} < {limit}MB", size_mb < limit, f"{size_mb:.1f}MB")

    total_agg = sum(
        (AGG / f"{n}.parquet").stat().st_size for n in expected_aggs
        if (AGG / f"{n}.parquet").exists()
    ) / (1024 * 1024)
    _check("Total aggregated < 100MB", total_agg < 100, f"{total_agg:.1f}MB")

    # ── 10. Cross-dataset year overlap ──
    print("\n-- 10. Cross-dataset year overlap --")
    overlap_sets = {}
    for name in ["ridership_trends", "vmt_trends", "collision_severity"]:
        path = AGG / f"{name}.parquet"
        if not path.exists():
            continue
        years = set(
            r[0] for r in con.execute(f"SELECT DISTINCT year FROM '{path}'").fetchall()
        )
        overlap_sets[name] = years

    if len(overlap_sets) >= 2:
        common = set.intersection(*overlap_sets.values())
        _check("At least 3 years overlap across ridership/vmt/collisions",
               len(common) >= 3, f"overlap years: {sorted(common)}")

    # ── 11. Summary ──
    con.close()
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed, {warnings} warnings")
    print("=" * 60)

    return failed


def main() -> None:
    failures = validate()
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
