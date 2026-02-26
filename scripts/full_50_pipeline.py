#!/usr/bin/env python3
"""
Full 50-parcel pipeline — Pass 1 (NAIP+FEMA) + Pass 2 (Planet) on flagged.

Diverse sample: 20 Vacant + 20 Residential + 10 Manufactured/Large.
Writes Pass 1 results to DB, then runs Planet refinement on all flagged.
"""

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
            "zip5": p.get("zip5"), "total_value": p.get("total_value"),
            "ndvi_score": None, "ndvi_category": "error",
            "error": str(e),
            "ndvi_date": None, "fema_zone": None, "fema_risk": None, "fema_sfha": False,
            "distress_score": None, "distress_flags": None,
            "flag_veg": False, "flag_flood": False, "flag_structural": False, "flag_neglect": False,
            "veg_confidence": None, "flood_confidence": None,
            "scan_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_pass": 1, "sentinel_worthy": False,
            "lat": float(p["latitude"]), "lng": float(p["longitude"]),
        }


def main():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Select diverse 50 parcels
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

    print(f"\n{'='*70}")
    print(f"FULL 50-PARCEL PIPELINE")
    print(f"{'='*70}")
    vac = sum(1 for p in parcels if "Vacant" in (p["property_class"] or ""))
    res = sum(1 for p in parcels if "Residential" in (p["property_class"] or ""))
    oth = len(parcels) - vac - res
    print(f"Selected: {len(parcels)} parcels (Vacant={vac}, Residential={res}, Other={oth})")

    # --- PASS 1 ---
    print(f"\n{'='*70}")
    print(f"PASS 1: NAIP + FEMA")
    print(f"{'='*70}")

    start = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futs = {executor.submit(scan_one, p): p["parcel_id"] for p in parcels}
        for f in as_completed(futs):
            r = f.result()
            results.append(r)
            n = len(results)
            if n % 10 == 0 or n == len(parcels):
                flagged_so_far = sum(1 for x in results if x.get("distress_flags"))
                print(f"  [{n}/{len(parcels)}] flagged={flagged_so_far}")

    elapsed_p1 = time.time() - start

    # Write Pass 1 to DB
    db_fields = [
        "parcel_id", "county", "ndvi_score", "ndvi_date", "ndvi_category",
        "fema_zone", "fema_risk", "fema_sfha", "distress_score", "distress_flags",
        "flag_veg", "flag_flood", "flag_structural", "flag_neglect",
        "veg_confidence", "flood_confidence", "scan_date", "scan_pass", "sentinel_worthy",
    ]
    db_results = [{k: r[k] for k in db_fields if k in r} for r in results]
    updated = batch_update_scan_results(conn, db_results)

    flagged = [r for r in results if r["distress_flags"]]
    errors = [r for r in results if r.get("error") or r["ndvi_category"] == "error"]

    print(f"\nPass 1 Complete: {len(results)} scanned | {len(flagged)} flagged ({len(flagged)/len(results)*100:.0f}%) | {len(errors)} errors")
    print(f"Time: {elapsed_p1:.1f}s | Rate: {len(results)/elapsed_p1:.1f}/sec | Written: {updated}")

    # Categories & flags
    cats = {}
    for r in results:
        c = r["ndvi_category"] or "unknown"
        cats[c] = cats.get(c, 0) + 1
    print(f"NDVI: {dict(sorted(cats.items()))}")

    fc = {"veg": 0, "neglect": 0, "flood": 0, "structural": 0}
    for r in results:
        if r["flag_veg"]: fc["veg"] += 1
        if r["flag_neglect"]: fc["neglect"] += 1
        if r["flag_flood"]: fc["flood"] += 1
        if r["flag_structural"]: fc["structural"] += 1
    print(f"Flags: {fc}")

    # By class
    for pclass in ["Vacant", "Residential 1 Family", "Mult-Sect Manufactured", "Vacant 10 Acres & Up"]:
        sub = [r for r in results if r.get("property_class") == pclass]
        f = [r for r in sub if r["distress_flags"]]
        if sub:
            ndvis = [r["ndvi_score"] for r in sub if r["ndvi_score"] is not None]
            avg = sum(ndvis) / len(ndvis) if ndvis else 0
            print(f"  {pclass:30} n={len(sub):2} flagged={len(f):2} ({len(f)/len(sub)*100:4.0f}%) avg_ndvi={avg:.3f}")

    # Top flagged
    print(f"\n{'='*70}")
    print(f"TOP 20 FLAGGED PARCELS")
    print(f"{'='*70}")
    for r in sorted(results, key=lambda x: x["distress_score"] or 0, reverse=True)[:20]:
        ndvi_s = f"{r['ndvi_score']:.3f}" if r['ndvi_score'] is not None else "NULL"
        score_s = f"{r['distress_score']:.1f}" if r['distress_score'] else "--"
        flags_s = r["distress_flags"] or "--"
        addr = (r.get("situs_address") or "--")[:35]
        val = f"${int(r['total_value']):,}" if r.get("total_value") else "--"
        fema_z = r.get("fema_zone") or "--"
        conf_s = f"{float(r['veg_confidence']):.2f}" if r.get("veg_confidence") else "--"
        pclass = (r.get("property_class") or "")[:20]
        print(f"  {r['parcel_id']:10} {pclass:20} NDVI={ndvi_s:7} score={score_s:4} fema={fema_z:3} conf={conf_s:5} {flags_s}")
        print(f"{'':12}{addr:35} {val:>12} zip={r.get('zip5', '--')}")

    # --- PASS 2: Planet on flagged ---
    print(f"\n{'='*70}")
    print(f"PASS 2: PLANET REFINEMENT ({len(flagged)} flagged parcels)")
    print(f"{'='*70}")

    start_p2 = time.time()
    planet_results = []

    for i, r in enumerate(sorted(flagged, key=lambda x: x["distress_score"] or 0, reverse=True), 1):
        pid = r["parcel_id"]
        addr = (r.get("situs_address") or "--")[:30]
        score = r["distress_score"] or 0
        flags_s = r["distress_flags"] or "--"

        print(f"\n  [{i}/{len(flagged)}] {pid} {addr} score={score:.1f} {flags_s}")

        try:
            pr = planet_refine(float(r["lat"]), float(r["lng"]))
            planet_results.append({"parcel": r, "planet": pr})

            span = pr.get("temporal_span_days")
            change = pr.get("change_score")
            has_pair = bool(pr.get("thumbnail_earliest_url"))
            print(f"    scenes={pr.get('scene_count', 0)} span={span or '--'}d change={change or '--'} pair={has_pair}")
            if pr.get("thumbnail_latest_url"):
                print(f"    latest:   {pr['thumbnail_latest_url']}")
            if pr.get("thumbnail_earliest_url"):
                print(f"    earliest: {pr['thumbnail_earliest_url']}")
        except Exception as e:
            print(f"    ERROR: {e}")
            planet_results.append({"parcel": r, "planet": {"error": str(e)}})

    elapsed_p2 = time.time() - start_p2

    # --- FINAL SUMMARY ---
    print(f"\n{'='*70}")
    print(f"FINAL SUMMARY")
    print(f"{'='*70}")

    with_scenes = sum(1 for p in planet_results if p["planet"].get("scene_count", 0) > 0)
    with_pairs = sum(1 for p in planet_results if p["planet"].get("thumbnail_earliest_url"))
    with_change = sum(1 for p in planet_results if p["planet"].get("change_score") is not None)
    spans = [p["planet"]["temporal_span_days"] for p in planet_results if p["planet"].get("temporal_span_days")]
    changes = [p["planet"]["change_score"] for p in planet_results if p["planet"].get("change_score") is not None]

    print(f"Pass 1: {len(results)} scanned, {len(flagged)} flagged ({len(flagged)/len(results)*100:.0f}%), {len(errors)} errors")
    print(f"Pass 1 time: {elapsed_p1:.1f}s")
    print(f"Pass 2: {len(planet_results)} refined, {with_scenes} with scenes, {with_pairs} with pairs, {with_change} with change_score")
    print(f"Pass 2 time: {elapsed_p2:.1f}s")
    print(f"Total time: {elapsed_p1 + elapsed_p2:.1f}s")
    if spans:
        print(f"Temporal spans: {min(spans)}-{max(spans)} days (avg {sum(spans)/len(spans):.0f})")
    if changes:
        print(f"Change scores: {min(changes):.3f}-{max(changes):.3f} (avg {sum(changes)/len(changes):.3f})")

    # High-change parcels (change_score > 0.5 = significant visual difference)
    high_change = [p for p in planet_results if (p["planet"].get("change_score") or 0) > 0.5]
    if high_change:
        print(f"\nHIGH-CHANGE PARCELS (change_score > 0.5): {len(high_change)}")
        for p in sorted(high_change, key=lambda x: x["planet"].get("change_score", 0), reverse=True):
            r = p["parcel"]
            pr = p["planet"]
            print(f"  {r['parcel_id']:10} change={pr['change_score']:.3f} span={pr['temporal_span_days']}d "
                  f"score={r['distress_score']:.1f} {r['distress_flags']}")
            print(f"    {r.get('situs_address', '--')}")
            if pr.get("thumbnail_latest_url"):
                print(f"    latest:   {pr['thumbnail_latest_url']}")
            if pr.get("thumbnail_earliest_url"):
                print(f"    earliest: {pr['thumbnail_earliest_url']}")

    # API calls used
    planet_calls = len(planet_results) * 4  # 2 searches + 2 thumbs per parcel
    print(f"\nPlanet API calls used: ~{planet_calls} (budget: 30K)")

    conn.close()
    print(f"\nDone. Results in DB. Dashboard at http://localhost:8001/dashboard")


if __name__ == "__main__":
    main()
