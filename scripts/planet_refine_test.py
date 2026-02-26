#!/usr/bin/env python3
"""Planet refinement test on flagged parcels."""

from dotenv import load_dotenv
load_dotenv()

from src.planet.client import planet_refine
from src.db import get_db_connection
from psycopg2.extras import RealDictCursor


def main():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''
        SELECT parcel_id, latitude, longitude, situs_address, property_class,
               distress_score, distress_flags, ndvi_score, fema_zone, total_value
        FROM gis_parcels_core
        WHERE scan_date IS NOT NULL AND distress_score > 0
          AND county = 'Gaston' AND state_code = 'NC'
        ORDER BY distress_score DESC
        LIMIT 15
    ''')
    flagged = cur.fetchall()
    conn.close()

    print(f"Running Planet refinement on {len(flagged)} flagged parcels...\n")
    print("=" * 80)

    results = []
    for p in flagged:
        pid = p['parcel_id']
        lat, lng = float(p['latitude']), float(p['longitude'])
        score = float(p['distress_score']) if p['distress_score'] else 0
        ndvi = float(p['ndvi_score']) if p['ndvi_score'] is not None else None
        addr = (p['situs_address'] or '--')[:35]
        flags = p['distress_flags'] or '--'
        val = f"${int(p['total_value']):,}" if p.get('total_value') else '--'

        ndvi_str = f"{ndvi:.3f}" if ndvi is not None else "NULL"
        print(f"\n{pid} | {addr} | {flags} | score={score:.1f} NDVI={ndvi_str} | {val}")

        pr = planet_refine(lat, lng)
        results.append({"parcel": dict(p), "planet": pr})

        status = pr.get("status")
        scenes = pr.get("scene_count", 0)
        span = pr.get("temporal_span_days")
        change = pr.get("change_score")
        latest_url = pr.get("thumbnail_latest_url")
        earliest_url = pr.get("thumbnail_earliest_url")

        print(f"  Planet: status={status} scenes={scenes} span={span or '--'}d change_score={change or '--'}")
        if latest_url:
            print(f"  Latest thumb:   {latest_url}")
        if earliest_url:
            print(f"  Earliest thumb: {earliest_url}")
        if pr.get("latest_scene"):
            ls = pr["latest_scene"]
            print(f"  Latest scene:   {ls['id']} ({ls['acquired'][:10]}) cloud={ls.get('cloud_cover')}")
        if pr.get("earliest_scene"):
            es = pr["earliest_scene"]
            print(f"  Earliest scene: {es['id']} ({es['acquired'][:10]}) cloud={es.get('cloud_cover')}")
        if pr.get("errors"):
            print(f"  Errors: {pr['errors']}")

    # Summary
    print(f"\n{'='*80}")
    print("PLANET REFINEMENT SUMMARY")
    print(f"{'='*80}")
    with_scenes = sum(1 for r in results if r["planet"].get("scene_count", 0) > 0)
    with_latest = sum(1 for r in results if r["planet"].get("thumbnail_latest_url"))
    with_pairs = sum(1 for r in results if r["planet"].get("thumbnail_earliest_url"))
    with_change = sum(1 for r in results if r["planet"].get("change_score") is not None)
    spans = [r["planet"]["temporal_span_days"] for r in results if r["planet"].get("temporal_span_days")]
    changes = [r["planet"]["change_score"] for r in results if r["planet"].get("change_score") is not None]

    print(f"Total refined:      {len(results)}")
    print(f"With scenes:        {with_scenes}")
    print(f"With latest thumb:  {with_latest}")
    print(f"With temporal pair: {with_pairs}")
    print(f"With change_score:  {with_change}")
    if spans:
        print(f"Temporal spans:     {min(spans)}-{max(spans)} days (avg {sum(spans)/len(spans):.0f})")
    if changes:
        print(f"Change scores:      {min(changes):.3f}-{max(changes):.3f} (avg {sum(changes)/len(changes):.3f})")

    # Verdict
    print(f"\n{'='*80}")
    print("VERDICT")
    print(f"{'='*80}")
    visually_confirmed = with_latest  # parcels with at least a thumbnail for visual review
    print(f"Parcels with Planet imagery: {visually_confirmed}/{len(results)}")
    if visually_confirmed >= 5:
        print("PASS: 5+ flagged parcels have Planet imagery for visual confirmation.")
        print("RECOMMENDATION: Scale up to 50-parcel full run.")
    else:
        print(f"BELOW THRESHOLD: Only {visually_confirmed} have imagery.")


if __name__ == "__main__":
    main()
