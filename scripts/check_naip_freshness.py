#!/usr/bin/env python3
"""
NAIP Freshness Checker — detect when new NAIP imagery is available.

Queries Planetary Computer STAC for the latest NAIP year per state,
compares against what's in the DB (ndvi_history_years), and reports
which counties have new data available for --rescan-new-years.

Usage:
    PYTHONPATH=. python scripts/check_naip_freshness.py
    PYTHONPATH=. python scripts/check_naip_freshness.py --state NC
    PYTHONPATH=. python scripts/check_naip_freshness.py --force-refresh
"""

import argparse

from dotenv import load_dotenv
load_dotenv()

from src.db import get_db_connection
from src.naip.planetary import (
    STATE_PROBE_POINTS,
    discover_latest_naip_year,
    discover_all_available_years,
)


def get_county_max_years(conn, state_code: str) -> dict[str, dict]:
    """Get the max NAIP year in ndvi_history_years per county for a state.

    Parses the comma-separated ndvi_history_years field, extracts max year.
    Returns {county: {"max_year": int, "parcels": int}}.
    """
    query = """
        SELECT county,
               MAX(split_part_year::int) AS max_year,
               COUNT(*) AS parcels_with_slope
        FROM (
            SELECT county,
                   UNNEST(string_to_array(ndvi_history_years, ',')) AS split_part_year
            FROM gis_parcels_core
            WHERE state_code = %s
                AND ndvi_slope_5yr IS NOT NULL
                AND ndvi_history_years IS NOT NULL
        ) sub
        WHERE split_part_year ~ '^\\d{4}$'
        GROUP BY county
        ORDER BY county
    """
    with conn.cursor() as cur:
        cur.execute(query, (state_code,))
        rows = cur.fetchall()

    return {row[0]: {"max_year": row[1], "parcels": row[2]} for row in rows}


def main():
    parser = argparse.ArgumentParser(description="Check NAIP freshness across markets")
    parser.add_argument("--state", default=None,
                        help="Check single state (default: all in STATE_PROBE_POINTS)")
    parser.add_argument("--force-refresh", action="store_true",
                        help="Bypass 7-day STAC cache for discovery")
    args = parser.parse_args()

    states = [args.state] if args.state else list(STATE_PROBE_POINTS.keys())

    print("\n=== NAIP Freshness Check ===\n")

    conn = get_db_connection()
    stale_counties = []

    for state in states:
        print(f"  [{state}] Checking STAC for available years"
              f"{' (force refresh)' if args.force_refresh else ''}...")
        all_years = discover_all_available_years(state, force_refresh=args.force_refresh)
        latest = all_years[-1] if all_years else None

        if not latest:
            print(f"  [{state}] WARNING: Could not discover any NAIP years")
            continue

        print(f"  [{state}] STAC years: {all_years}")
        print(f"  [{state}] Latest available: {latest}")

        # Check DB
        county_data = get_county_max_years(conn, state)
        if not county_data:
            print(f"  [{state}] No counties with slope data in DB\n")
            continue

        for county, info in sorted(county_data.items()):
            max_year = info["max_year"]
            parcels = info["parcels"]
            if max_year < latest:
                status = f"STALE (has {max_year}, missing {latest})"
                stale_counties.append((county, state, max_year, latest, parcels))
            else:
                status = f"OK (has {max_year})"
            print(f"    {county:20s} max_year={max_year}  parcels={parcels:>6,}  {status}")

        print()

    conn.close()

    # Summary
    if stale_counties:
        print("=== Counties Needing Rescan ===\n")
        for county, state, max_year, latest, parcels in stale_counties:
            print(f"  {county} ({state}): {max_year} → {latest}  ({parcels:,} parcels)")
            print(f"    PYTHONPATH=. python scripts/batch_historical_slope.py "
                  f"--county {county} --state {state} --rescan-new-years\n")
    else:
        print("=== All counties up to date ===\n")


if __name__ == "__main__":
    main()
