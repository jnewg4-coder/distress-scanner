#!/usr/bin/env python3
"""
Test 10 properties through the full distress scan pipeline.

Picks 10 parcels from DB (mix of Gaston + Mecklenburg),
runs full scan (NAIP + FEMA + Landsat + Planet search),
saves results to data/test_results.json for dashboard review.

Usage:
    PYTHONPATH=. python scripts/test_10_properties.py
    PYTHONPATH=. python scripts/test_10_properties.py --skip-planet  # save API calls
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from src.db import get_db_connection
from src.naip.baseline import naip_baseline
from src.fema.flood import fema_flood
from src.landsat.client import landsat_trends
from src.planet.client import planet_refine, PlanetClient
from src.analysis.flags import generate_all_flags

OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "test_results.json"


def compute_score(flags: list[dict]) -> float:
    """Compute distress score from flags."""
    weights = {
        "vegetation_overgrowth": 2.0,
        "flood_risk": 1.5,
        "structural_change": 2.5,
    }
    score = sum(weights.get(f["signal_code"], 1.0) * f["confidence"] for f in flags)
    return round(min(score, 10.0), 2)


def get_test_parcels(conn, count: int = 10) -> list[dict]:
    """Pick test parcels: 5 from Gaston, 5 from Mecklenburg (if available)."""
    from psycopg2.extras import RealDictCursor

    parcels = []

    # 5 from Gaston (varied value range for diverse results)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT parcel_id, latitude, longitude, owner_name, situs_address,
                   total_value, property_class, 'Gaston' as county, 'NC' as state
            FROM gis_parcels_core
            WHERE county = 'Gaston' AND state_code = 'NC'
                AND latitude IS NOT NULL AND longitude IS NOT NULL
                AND property_class = 'Residential 1 Family'
                AND total_value BETWEEN 75000 AND 300000
            ORDER BY random()
            LIMIT %s
        """, (min(count // 2 + count % 2, count),))
        parcels.extend([dict(r) for r in cur.fetchall()])

    # 5 from Mecklenburg (if geocoded)
    remaining = count - len(parcels)
    if remaining > 0:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT parcel_id, latitude, longitude, owner_name, situs_address,
                       total_value, property_class, 'Mecklenburg' as county, 'NC' as state
                FROM gis_parcels_core
                WHERE county = 'Mecklenburg' AND state_code = 'NC'
                    AND latitude IS NOT NULL AND longitude IS NOT NULL
                    AND property_class = 'Single-Family'
                    AND total_value BETWEEN 75000 AND 300000
                ORDER BY random()
                LIMIT %s
            """, (remaining,))
            parcels.extend([dict(r) for r in cur.fetchall()])

    return parcels


def scan_property(parcel: dict, skip_planet: bool = False) -> dict:
    """Run full pipeline on a single parcel."""
    lat = float(parcel["latitude"])
    lng = float(parcel["longitude"])
    scan_start = datetime.now()

    result = {
        "parcel_id": parcel["parcel_id"],
        "lat": lat,
        "lng": lng,
        "county": parcel.get("county", ""),
        "address": parcel.get("situs_address", ""),
        "owner": parcel.get("owner_name", ""),
        "total_value": parcel.get("total_value"),
        "scan_date": scan_start.strftime("%Y-%m-%d"),
        "scan_timestamp": scan_start.isoformat(),
        "naip": None,
        "fema": None,
        "landsat": None,
        "sentinel": None,
        "planet": None,
        "flags": [],
        "distress_score": 0.0,
        "sentinel_worthy": False,
        "summary": "",
        "errors": [],
    }

    # NAIP (full history for test — not batch mode)
    try:
        print(f"    NAIP...", end="", flush=True)
        result["naip"] = naip_baseline(lat, lng, skip_image_export=True)
        ndvi = result["naip"].get("current_ndvi")
        print(f" NDVI={ndvi:.3f}" if ndvi else " no data", end="")
    except Exception as e:
        result["errors"].append(f"naip: {e}")
        print(f" ERR", end="")

    # FEMA
    try:
        print(f" | FEMA...", end="", flush=True)
        result["fema"] = fema_flood(lat, lng, skip_map=True)
        zone = result["fema"].get("flood_zone", "--")
        risk = result["fema"].get("risk_level", "?")
        print(f" {zone}/{risk}", end="")
    except Exception as e:
        result["errors"].append(f"fema: {e}")
        print(f" ERR", end="")

    # Landsat (trends — test the backup source)
    try:
        print(f" | LSAT...", end="", flush=True)
        result["landsat"] = landsat_trends(lat, lng, months=12)
        lsat_months = result["landsat"].get("months_with_data", 0)
        lsat_trend = result["landsat"].get("trend_direction", "?")
        print(f" {lsat_months}mo/{lsat_trend}", end="")
    except Exception as e:
        result["errors"].append(f"landsat: {e}")
        print(f" ERR", end="")

    # Planet (search only — conservative with 30K limit)
    if not skip_planet:
        try:
            print(f" | PLNT...", end="", flush=True)
            result["planet"] = planet_refine(lat, lng, months_back=18)
            scenes = result["planet"].get("scene_count", 0)
            print(f" {scenes} scenes", end="")
        except Exception as e:
            result["errors"].append(f"planet: {e}")
            print(f" ERR", end="")
    else:
        result["planet"] = {"status": "skipped", "scene_count": 0}
        print(f" | PLNT skipped", end="")

    # Flags (use Landsat as trend source since we're testing it)
    trends = result.get("landsat") if result.get("landsat", {}).get("months_with_data", 0) > 0 else None
    result["flags"] = generate_all_flags(
        naip=result["naip"],
        sentinel=trends,
        fema=result["fema"],
    )

    result["distress_score"] = compute_score(result["flags"])

    # Sentinel-worthy check
    from src.analysis.scanner import is_sentinel_worthy
    result["sentinel_worthy"] = is_sentinel_worthy(result)

    # Summary
    elapsed = (datetime.now() - scan_start).total_seconds()
    flag_codes = [f["signal_code"] for f in result["flags"]]
    result["summary"] = (
        f"Score: {result['distress_score']:.2f} | "
        f"NDVI: {result['naip'].get('current_ndvi', 'N/A') if result['naip'] else 'N/A'} | "
        f"FEMA: {result['fema'].get('flood_zone', 'N/A') if result['fema'] else 'N/A'} | "
        f"Flags: {flag_codes or 'none'} | "
        f"{elapsed:.1f}s"
    )

    print(f" | score={result['distress_score']:.2f} ({elapsed:.1f}s)")
    return result


def main():
    parser = argparse.ArgumentParser(description="Test 10 properties through full pipeline")
    parser.add_argument("--count", type=int, default=10, help="Number of properties")
    parser.add_argument("--skip-planet", action="store_true", help="Skip Planet API calls")
    args = parser.parse_args()

    # Check Planet availability
    planet_client = PlanetClient()
    planet_status = "AVAILABLE" if planet_client.available else "NOT CONFIGURED"
    print(f"Planet API: {planet_status}")
    if args.skip_planet:
        print("  (--skip-planet flag set, skipping Planet calls)")

    # Get test parcels from DB
    print(f"\nFetching {args.count} test parcels from DB...")
    conn = get_db_connection()
    parcels = get_test_parcels(conn, count=args.count)
    conn.close()

    if not parcels:
        print("No parcels found with coordinates. Run geocode first.")
        return

    print(f"Got {len(parcels)} parcels:")
    for i, p in enumerate(parcels):
        print(f"  {i+1}. {p['parcel_id']} — {p.get('county')} — "
              f"({float(p['latitude']):.4f}, {float(p['longitude']):.4f}) — "
              f"${p.get('total_value', 0):,.0f}")

    print(f"\n{'='*70}")
    print(f"SCANNING {len(parcels)} PROPERTIES — FULL PIPELINE")
    print(f"Sources: NAIP + FEMA + Landsat + {'Planet' if not args.skip_planet else 'Planet(skip)'}")
    print(f"{'='*70}")

    results = []
    scan_start = datetime.now()

    for i, parcel in enumerate(parcels):
        print(f"\n[{i+1}/{len(parcels)}] {parcel['parcel_id']} ({parcel.get('county', '')})")
        result = scan_property(parcel, skip_planet=args.skip_planet)
        results.append(result)
        if i < len(parcels) - 1:
            time.sleep(0.5)  # Light rate limiting

    # Save results
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(results, indent=2, default=str))
    print(f"\nResults saved to: {OUTPUT_FILE}")

    # Summary
    elapsed = (datetime.now() - scan_start).total_seconds()
    flagged = sum(1 for r in results if r["flags"])
    worthy = sum(1 for r in results if r.get("sentinel_worthy"))
    scores = [r["distress_score"] for r in results]
    avg_score = sum(scores) / len(scores) if scores else 0
    planet_hits = sum(1 for r in results
                      if r.get("planet", {}).get("scene_count", 0) > 0)

    print(f"\n{'='*70}")
    print(f"TEST COMPLETE")
    print(f"  Properties:       {len(results)}")
    print(f"  Flagged:          {flagged}")
    print(f"  Sentinel-worthy:  {worthy}")
    print(f"  Avg score:        {avg_score:.2f}")
    print(f"  Planet scenes:    {planet_hits}/{len(results)} have coverage")
    print(f"  Elapsed:          {elapsed:.0f}s ({elapsed/len(results):.1f}s/property)")
    print(f"  Errors:           {sum(len(r['errors']) for r in results)}")
    print(f"{'='*70}")
    print(f"\nOpen Command Center to review:")
    print(f"  uvicorn src.api.app:app --port 8001")
    print(f"  http://localhost:8001/dashboard")
    print(f"  → Load File → data/test_results.json")


if __name__ == "__main__":
    main()
