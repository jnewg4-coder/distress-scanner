#!/usr/bin/env python3
"""Diverse 50-parcel test scan — validates pipeline across property types and geography."""

import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import RealDictCursor

from dotenv import load_dotenv
load_dotenv()

from src.db import get_db_connection, batch_update_scan_results
from src.naip.baseline import naip_ndvi_fast
from src.fema.flood import fema_flood
from src.analysis.flags import generate_all_flags
from src.planet.client import planet_refine

SIGNAL_WEIGHTS = {
    "vegetation_overgrowth": 2.0,
    "vegetation_neglect": 1.5,
    "flood_risk": 1.5,
    "structural_change": 2.5,
}


def scan_one(p):
    pid = p["parcel_id"]
    lat, lng = float(p["latitude"]), float(p["longitude"])
    try:
        naip = naip_ndvi_fast(lat, lng)
        fema = None
        try:
            fema = fema_flood(lat, lng, skip_map=True)
        except Exception:
            pass

        naip_for_flags = None
        if naip and naip.get("ndvi") is not None:
            naip_for_flags = {
                "current_ndvi": naip["ndvi"],
                "mean_historical_ndvi": None,
                "errors": [naip["error"]] if naip.get("error") else [],
            }

        flags = generate_all_flags(naip=naip_for_flags, sentinel=None, fema=fema)

        score = 0.0
        for flag in flags:
            w = SIGNAL_WEIGHTS.get(flag["signal_code"], 1.0)
            score += w * flag["confidence"]
        score = round(min(score, 10.0), 2)

        flag_codes = {f["signal_code"] for f in flags}
        flag_confs = {f["signal_code"]: f["confidence"] for f in flags}

        return {
            "parcel_id": pid,
            "county": p["county"],
            "property_class": p.get("property_class"),
            "situs_address": p.get("situs_address"),
            "zip5": p.get("zip5"),
            "total_value": p.get("total_value"),
            "ndvi_score": naip.get("ndvi"),
            "ndvi_date": naip.get("date"),
            "ndvi_category": naip.get("category"),
            "fema_zone": fema.get("flood_zone") if fema else None,
            "fema_risk": fema.get("risk_level") if fema else None,
            "fema_sfha": fema.get("is_sfha", False) if fema else False,
            "distress_score": score,
            "distress_flags": ",".join(sorted(flag_codes)) if flag_codes else None,
            "flag_veg": "vegetation_overgrowth" in flag_codes,
            "flag_flood": "flood_risk" in flag_codes,
            "flag_structural": "structural_change" in flag_codes,
            "flag_neglect": "vegetation_neglect" in flag_codes,
            "veg_confidence": flag_confs.get("vegetation_overgrowth") or flag_confs.get("vegetation_neglect"),
            "flood_confidence": flag_confs.get("flood_risk"),
            "scan_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_pass": 1,
            "sentinel_worthy": bool(flags) or (naip.get("ndvi") is not None and naip["ndvi"] > 0.50),
            "lat": lat,
            "lng": lng,
        }
    except Exception as e:
        return {
            "parcel_id": pid, "county": p["county"],
            "property_class": p.get("property_class"),
            "situs_address": p.get("situs_address"),
            "zip5": p.get("zip5"),
            "total_value": p.get("total_value"),
            "ndvi_score": None, "ndvi_category": "error",
            "error": str(e),
            "ndvi_date": None, "fema_zone": None, "fema_risk": None, "fema_sfha": False,
            "distress_score": None, "distress_flags": None,
            "flag_veg": False, "flag_flood": False, "flag_structural": False, "flag_neglect": False,
            "veg_confidence": None, "flood_confidence": None,
            "scan_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_pass": 1, "sentinel_worthy": False,
            "lat": lat, "lng": lng,
        }


def main():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Select diverse sample
    cur.execute('''
        (SELECT parcel_id, latitude, longitude, county, property_class,
                total_value, situs_address, SUBSTRING(mailing_zip FROM 1 FOR 5) as zip5
         FROM gis_parcels_core
         WHERE county = 'Gaston' AND state_code = 'NC'
           AND latitude IS NOT NULL AND scan_date IS NULL
           AND property_class = 'Vacant'
         ORDER BY RANDOM() LIMIT 20)
        UNION ALL
        (SELECT parcel_id, latitude, longitude, county, property_class,
                total_value, situs_address, SUBSTRING(mailing_zip FROM 1 FOR 5) as zip5
         FROM gis_parcels_core
         WHERE county = 'Gaston' AND state_code = 'NC'
           AND latitude IS NOT NULL AND scan_date IS NULL
           AND property_class = 'Residential 1 Family'
         ORDER BY RANDOM() LIMIT 20)
        UNION ALL
        (SELECT parcel_id, latitude, longitude, county, property_class,
                total_value, situs_address, SUBSTRING(mailing_zip FROM 1 FOR 5) as zip5
         FROM gis_parcels_core
         WHERE county = 'Gaston' AND state_code = 'NC'
           AND latitude IS NOT NULL AND scan_date IS NULL
           AND property_class IN ('Mult-Sect Manufactured', 'Vacant 10 Acres & Up')
         ORDER BY RANDOM() LIMIT 10)
    ''')
    parcels = [dict(r) for r in cur.fetchall()]
    print(f"Selected {len(parcels)} parcels for diverse scan\n")

    # --- Pass 1: NAIP + FEMA ---
    print("=" * 70)
    print("PASS 1: NAIP + FEMA SCAN")
    print("=" * 70)

    start = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futs = {executor.submit(scan_one, p): p["parcel_id"] for p in parcels}
        for f in as_completed(futs):
            r = f.result()
            results.append(r)
            n = len(results)
            if n % 10 == 0 or n == len(parcels):
                print(f"  [{n}/{len(parcels)}]")

    elapsed = time.time() - start

    # Write to DB
    db_fields = [
        "parcel_id", "county", "ndvi_score", "ndvi_date", "ndvi_category",
        "fema_zone", "fema_risk", "fema_sfha", "distress_score", "distress_flags",
        "flag_veg", "flag_flood", "flag_structural", "flag_neglect",
        "veg_confidence", "flood_confidence", "scan_date", "scan_pass", "sentinel_worthy",
    ]
    db_results = [{k: r[k] for k in db_fields if k in r} for r in results]
    updated = batch_update_scan_results(conn, db_results)

    # --- Analysis ---
    flagged = [r for r in results if r["distress_flags"]]
    errors = [r for r in results if r.get("error") or r["ndvi_category"] == "error"]

    print(f"\nScanned: {len(results)} | Flagged: {len(flagged)} ({len(flagged)/len(results)*100:.0f}%) | Errors: {len(errors)}")
    print(f"Time: {elapsed:.1f}s | Rate: {len(results)/elapsed:.1f}/sec | Written: {updated}")

    # NDVI distribution
    cats = {}
    for r in results:
        c = r["ndvi_category"] or "unknown"
        cats[c] = cats.get(c, 0) + 1
    print(f"\nNDVI Categories: {dict(sorted(cats.items()))}")

    # Flag breakdown
    flag_counts = {"veg_overgrowth": 0, "neglect": 0, "flood": 0, "structural": 0}
    for r in results:
        if r["flag_veg"]: flag_counts["veg_overgrowth"] += 1
        if r["flag_neglect"]: flag_counts["neglect"] += 1
        if r["flag_flood"]: flag_counts["flood"] += 1
        if r["flag_structural"]: flag_counts["structural"] += 1
    print(f"Flags: {flag_counts}")

    # By property class
    print(f"\nBy Property Class:")
    for pclass in ["Vacant", "Residential 1 Family", "Mult-Sect Manufactured", "Vacant 10 Acres & Up"]:
        subset = [r for r in results if r.get("property_class") == pclass]
        f = [r for r in subset if r["distress_flags"]]
        if subset:
            ndvis = [r["ndvi_score"] for r in subset if r["ndvi_score"] is not None]
            avg_ndvi = sum(ndvis) / len(ndvis) if ndvis else 0
            print(f"  {pclass:30} n={len(subset):2} flagged={len(f):2} ({len(f)/len(subset)*100:4.0f}%) avg_ndvi={avg_ndvi:.3f}")

    # Score distribution
    scores = [r["distress_score"] for r in results if r["distress_score"] and r["distress_score"] > 0]
    if scores:
        print(f"\nDistress Scores (flagged only): min={min(scores):.1f} max={max(scores):.1f} avg={sum(scores)/len(scores):.1f}")

    # Top flagged parcels
    print(f"\n{'='*70}")
    print(f"TOP FLAGGED PARCELS (sorted by distress score)")
    print(f"{'='*70}")
    for r in sorted(results, key=lambda x: x["distress_score"] or 0, reverse=True)[:20]:
        ndvi = f"{r['ndvi_score']:.3f}" if r['ndvi_score'] is not None else "NULL"
        score = f"{r['distress_score']:.1f}" if r['distress_score'] else "--"
        flags = r["distress_flags"] or "--"
        addr = (r.get("situs_address") or "--")[:35]
        val = f"${int(r['total_value']):,}" if r.get("total_value") else "--"
        fema_z = r.get("fema_zone") or "--"
        conf = f"{float(r['veg_confidence']):.2f}" if r.get("veg_confidence") else "--"
        print(f"  {r['parcel_id']:10} {(r.get('property_class') or '')[:20]:20} NDVI={ndvi:7} "
              f"score={score:4} fema={fema_z:3} conf={conf:5} {flags}")
        print(f"{'':12}{addr:35} {val:>12} zip={r.get('zip5', '--')}")

    # --- Pass 2: Planet refinement on flagged parcels ---
    print(f"\n{'='*70}")
    print(f"PASS 2: PLANET REFINEMENT ON FLAGGED PARCELS")
    print(f"{'='*70}")

    if not flagged:
        print("  No flagged parcels to refine.")
    else:
        planet_results = []
        for r in sorted(flagged, key=lambda x: x["distress_score"] or 0, reverse=True)[:10]:
            print(f"\n  Refining {r['parcel_id']} (score={r['distress_score']}, {r['distress_flags']})...")
            try:
                pr = planet_refine(float(r["lat"]), float(r["lng"]))
                planet_results.append({"parcel": r, "planet": pr})
                status = pr.get("status", "unknown")
                scenes = pr.get("scene_count", 0)
                span = pr.get("temporal_span_days")
                change = pr.get("change_score")
                thumb_latest = bool(pr.get("thumbnail_latest_url"))
                thumb_earliest = bool(pr.get("thumbnail_earliest_url"))
                print(f"    Status: {status} | Scenes: {scenes} | Span: {span or '--'}d | "
                      f"Change: {change or '--'} | Thumbs: latest={thumb_latest} earliest={thumb_earliest}")
                if pr.get("errors"):
                    print(f"    Errors: {pr['errors']}")
            except Exception as e:
                print(f"    ERROR: {e}")
                planet_results.append({"parcel": r, "planet": {"error": str(e)}})

        # Planet summary
        print(f"\n{'='*70}")
        print(f"PLANET SUMMARY")
        print(f"{'='*70}")
        with_scenes = [p for p in planet_results if p["planet"].get("scene_count", 0) > 0]
        with_thumbs = [p for p in planet_results if p["planet"].get("thumbnail_latest_url")]
        with_pairs = [p for p in planet_results if p["planet"].get("thumbnail_earliest_url")]
        with_change = [p for p in planet_results if p["planet"].get("change_score") is not None]
        print(f"  Refined: {len(planet_results)} | With scenes: {len(with_scenes)} | "
              f"With thumbs: {len(with_thumbs)} | With pairs: {len(with_pairs)} | "
              f"With change_score: {len(with_change)}")

    # --- Final verdict ---
    print(f"\n{'='*70}")
    print(f"VERDICT")
    print(f"{'='*70}")
    flag_rate = len(flagged) / len(results) * 100 if results else 0
    print(f"  Flag rate: {flag_rate:.0f}% ({len(flagged)}/{len(results)})")
    print(f"  Error rate: {len(errors)/len(results)*100:.0f}%")

    if len(flagged) >= 5:
        print(f"  PASS: >= 5 flagged parcels. Scores justify scale-up.")
    else:
        print(f"  BELOW THRESHOLD: {len(flagged)} flagged < 5 required.")
        if flag_rate < 10:
            print(f"  Note: Low flag rate may indicate thresholds need tuning or sample bias.")

    conn.close()


if __name__ == "__main__":
    main()
