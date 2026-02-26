#!/usr/bin/env python3
"""
Batch geocode parcels using the US Census Geocoder.

Free, no API key. Batch endpoint accepts CSV of up to 10,000 addresses.
Backfills latitude/longitude into gis_parcels_core.

Usage:
    python scripts/geocode_parcels.py --county Mecklenburg --state NC \
        --property-class "Single-Family" --min-value 75000 --max-value 300000 \
        --min-sqft 700 --max-sqft 4000 --limit 7000
"""

import argparse
import csv
import io
import re
import time

from dotenv import load_dotenv
load_dotenv()

import requests
from psycopg2.extras import RealDictCursor

from src.db import get_db_connection

CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BATCH_SIZE = 9000  # Census max is 10K, leave margin


def parse_situs_address(situs: str) -> tuple[str, str, str]:
    """
    Parse situs_address like '2665 OLANDO ST CHARLOTTE NC' into (street, city, state).
    """
    if not situs:
        return ("", "", "")

    situs = situs.strip()

    # Pattern: street ... CITY STATE
    match = re.match(r'^(.+?)\s+([A-Z]+)\s+(NC|SC)$', situs)
    if match:
        return (match.group(1).strip(), match.group(2).strip(), match.group(3).strip())

    # Fallback: try splitting on last two tokens
    parts = situs.rsplit(None, 2)
    if len(parts) >= 3 and len(parts[-1]) == 2:
        return (" ".join(parts[:-2]), parts[-2], parts[-1])

    return (situs, "", "NC")


def geocode_batch(rows: list[dict]) -> dict[str, tuple[float, float]]:
    """
    Send a batch of addresses to Census Geocoder.

    rows: list of dicts with 'parcel_id' and 'situs_address'
    Returns: {parcel_id: (lat, lng)} for matched addresses
    """
    buf = io.StringIO()
    writer = csv.writer(buf)

    for row in rows:
        street, city, state = parse_situs_address(row["situs_address"])
        if not street:
            continue
        writer.writerow([row["parcel_id"], street, city, state, ""])

    csv_content = buf.getvalue()
    if not csv_content.strip():
        return {}

    resp = requests.post(
        CENSUS_BATCH_URL,
        files={"addressFile": ("addresses.csv", csv_content, "text/csv")},
        data={"benchmark": "Public_AR_Current", "returntype": "locations"},
        timeout=120,
    )
    resp.raise_for_status()

    results = {}
    for line in resp.text.strip().split("\n"):
        if not line.strip():
            continue
        # CSV format: "id","input_address","match_status","match_type","matched_address","coords","tiger_id","side"
        reader = csv.reader(io.StringIO(line))
        for fields in reader:
            if len(fields) >= 6 and fields[2] == "Match":
                parcel_id = fields[0].strip('"')
                coords = fields[5].strip('"')
                if "," in coords:
                    lng_str, lat_str = coords.split(",")
                    try:
                        lat = float(lat_str)
                        lng = float(lng_str)
                        results[parcel_id] = (lat, lng)
                    except ValueError:
                        pass

    return results


def run_geocode(county: str, state: str, limit: int = 7000,
                property_class: str = None,
                min_value: float = None, max_value: float = None,
                min_sqft: float = None, max_sqft: float = None,
                dry_run: bool = False):
    """Geocode parcels missing coordinates and backfill into DB."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Build query for parcels WITHOUT coordinates
    query = """
        SELECT parcel_id, situs_address
        FROM gis_parcels_core
        WHERE county = %s AND state_code = %s
            AND (latitude IS NULL OR longitude IS NULL)
            AND situs_address IS NOT NULL AND situs_address != ''
    """
    params = [county, state]

    if property_class:
        query += " AND property_class = %s"
        params.append(property_class)

    if min_value is not None:
        query += " AND total_value >= %s"
        params.append(min_value)

    if max_value is not None:
        query += " AND total_value <= %s"
        params.append(max_value)

    if min_sqft is not None:
        query += " AND sqft >= %s"
        params.append(min_sqft)

    if max_sqft is not None:
        query += " AND sqft <= %s"
        params.append(max_sqft)

    query += " ORDER BY parcel_id LIMIT %s"
    params.append(limit)

    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        print(f"No parcels need geocoding for {county}, {state} with given filters")
        conn.close()
        return

    print(f"Found {len(rows)} parcels to geocode in {county}, {state}")

    # Process in batches
    total_matched = 0
    total_updated = 0

    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch = rows[batch_start:batch_start + BATCH_SIZE]
        batch_num = (batch_start // BATCH_SIZE) + 1
        total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\nBatch {batch_num}/{total_batches}: {len(batch)} addresses...")

        try:
            results = geocode_batch(batch)
            total_matched += len(results)
            print(f"  Matched: {len(results)}/{len(batch)} ({len(results)/len(batch)*100:.1f}%)")

            if not dry_run and results:
                # Batch update lat/lng in DB
                update_cur = conn.cursor()
                for parcel_id, (lat, lng) in results.items():
                    update_cur.execute("""
                        UPDATE gis_parcels_core
                        SET latitude = %s, longitude = %s
                        WHERE county = %s AND state_code = %s AND parcel_id = %s
                            AND latitude IS NULL
                    """, (lat, lng, county, state, parcel_id))
                    total_updated += update_cur.rowcount
                conn.commit()
                update_cur.close()
                print(f"  Updated DB: {total_updated} rows")
            elif dry_run:
                # Show samples
                samples = list(results.items())[:3]
                for pid, (lat, lng) in samples:
                    print(f"    {pid}: ({lat:.6f}, {lng:.6f})")

        except Exception as e:
            print(f"  ERROR: {e}")

        # Rate limit between batches
        if batch_start + BATCH_SIZE < len(rows):
            time.sleep(2)

    # Summary
    match_rate = total_matched / len(rows) * 100 if rows else 0
    print("\n" + "=" * 60)
    print(f"GEOCODING COMPLETE — {county}, {state}")
    print(f"  Total addresses:  {len(rows)}")
    print(f"  Matched:          {total_matched} ({match_rate:.1f}%)")
    if not dry_run:
        print(f"  DB rows updated:  {total_updated}")
    else:
        print(f"  (DRY RUN — no DB updates)")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch geocode parcels via Census API")
    parser.add_argument("--county", required=True, help="County name")
    parser.add_argument("--state", required=True, help="State code")
    parser.add_argument("--property-class", help="Filter by property_class")
    parser.add_argument("--min-value", type=float, help="Min total_value")
    parser.add_argument("--max-value", type=float, help="Max total_value")
    parser.add_argument("--min-sqft", type=float, help="Min sqft")
    parser.add_argument("--max-sqft", type=float, help="Max sqft")
    parser.add_argument("--limit", type=int, default=7000, help="Max parcels to geocode")
    parser.add_argument("--dry-run", action="store_true", help="Don't update DB")

    args = parser.parse_args()

    run_geocode(
        county=args.county,
        state=args.state,
        limit=args.limit,
        property_class=args.property_class,
        min_value=args.min_value,
        max_value=args.max_value,
        min_sqft=args.min_sqft,
        max_sqft=args.max_sqft,
        dry_run=args.dry_run,
    )
