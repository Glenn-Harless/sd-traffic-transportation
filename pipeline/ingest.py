"""Download traffic/transit data from SANDAG Socrata API and seshat.datasd.org."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# ── SANDAG Socrata JSON sources ──
SOCRATA_BASE = "https://opendata.sandag.org/resource"
SOCRATA_SOURCES: dict[str, tuple[str, int]] = {
    "transit_ridership":    ("q5rv-a6w8", 5_000),
    "vmt_pems":             ("kzvf-xgyu", 5_000),
    "highway_travel_times": ("sx8b-e5xp", 5_000),
    "switrs_summary":       ("ta2f-7tx9", 5_000),
    "switrs_detailed":      ("uzct-sb5t", 300_000),
    "youth_opp_pass":       ("34ep-6uyj", 150_000),
    "flexible_fleet":       ("bkj2-54gq", 50_000),
}

# ── City of San Diego CSV sources ──
SESHAT_SOURCES: dict[str, str] = {
    "traffic_volumes":    "https://seshat.datasd.org/traffic_adt_counts/traffic_counts_datasd.csv",
    "traffic_collisions": "https://seshat.datasd.org/traffic_collisions/pd_collisions_datasd.csv",
    "transit_routes":     "https://seshat.datasd.org/gis_transit_routes/transit_routes_datasd.csv",
}


def _download_socrata(name: str, resource_id: str, limit: int, *, force: bool = False) -> Path:
    """Download a Socrata dataset as JSON. Skips if file exists and force=False."""
    dest = RAW_DIR / f"{name}.json"
    if dest.exists() and not force:
        print(f"  [skip] {name} (already exists, {dest.stat().st_size:,} bytes)")
        return dest

    url = f"{SOCRATA_BASE}/{resource_id}.json"
    print(f"  [download] {name} from Socrata ({resource_id}) ...")
    resp = httpx.get(url, params={"$limit": limit}, timeout=300, follow_redirects=True)
    resp.raise_for_status()
    data = resp.json()
    dest.write_text(json.dumps(data, indent=None))
    print(f"  [done] {name} -> {dest.stat().st_size:,} bytes ({len(data):,} rows)")
    return dest


def _download_csv(name: str, url: str, *, force: bool = False) -> Path:
    """Download a CSV from seshat.datasd.org. Skips if file exists and force=False."""
    dest = RAW_DIR / f"{name}.csv"
    if dest.exists() and not force:
        print(f"  [skip] {name} (already exists, {dest.stat().st_size:,} bytes)")
        return dest

    print(f"  [download] {name} ...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
    print(f"  [done] {name} -> {dest.stat().st_size:,} bytes")
    return dest


def ingest(*, force: bool = False) -> list[Path]:
    """Download all source datasets. Returns list of downloaded file paths."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []

    # Socrata JSON downloads
    for name, (resource_id, limit) in SOCRATA_SOURCES.items():
        try:
            paths.append(_download_socrata(name, resource_id, limit, force=force))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                print(f"  [warn] {name}: 403 forbidden, skipping")
            else:
                raise

    # seshat CSV downloads
    for name, url in SESHAT_SOURCES.items():
        try:
            paths.append(_download_csv(name, url, force=force))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                print(f"  [warn] {name}: 403 forbidden, skipping")
            else:
                raise

    return paths


if __name__ == "__main__":
    ingest()
