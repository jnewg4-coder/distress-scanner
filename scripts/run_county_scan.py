#!/usr/bin/env python3
"""
Batch distress scanner for a county.

Usage:
    # Pass 1: Free scan (NAIP + FEMA only, no Sentinel budget)
    python scripts/run_county_scan.py --county Gaston --state NC --zip 28052 \
        --property-class "Residential 1 Family" --min-value 75000 --max-value 300000 --pass 1

    # Pass full: All sources including Sentinel (default)
    python scripts/run_county_scan.py --county Gaston --state NC --limit 10 --dry-run
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import structlog

from src.db import get_db_connection, get_parcels_with_coords
from src.analysis.scanner import scan_free, scan_distress, scan_and_write

logger = structlog.get_logger("county_scan")

FLAGGED_DIR = Path("data/flagged")


def run_county_scan(county: str, state: str, scan_pass: str = "full",
                    limit: int | None = None, dry_run: bool = False,
                    resume_from: str | None = None, delay: float = 1.0,
                    sentinel_months: int = 12,
                    mailing_zip: str | None = None,
                    property_class: str | None = None,
                    min_value: float | None = None, max_value: float | None = None,
                    min_sqft: float | None = None, max_sqft: float | None = None):
    """Run distress scan for parcels in a county."""
    conn = get_db_connection()

    parcels = get_parcels_with_coords(
        conn, county, state_code=state, limit=limit,
        mailing_zip=mailing_zip, property_class=property_class,
        min_value=min_value, max_value=max_value,
        min_sqft=min_sqft, max_sqft=max_sqft,
    )
    conn.close()

    if not parcels:
        print(f"No parcels found matching filters for {county}, {state}")
        return

    # Build filter description
    filters = []
    if mailing_zip:
        filters.append(f"zip={mailing_zip}")
    if property_class:
        filters.append(f"class={property_class}")
    if min_value or max_value:
        filters.append(f"val={min_value or 0}-{max_value or '∞'}")
    if min_sqft or max_sqft:
        filters.append(f"sqft={min_sqft or 0}-{max_sqft or '∞'}")
    filter_str = f" [{', '.join(filters)}]" if filters else ""

    print(f"Found {len(parcels)} parcels in {county}, {state}{filter_str}")

    # Resume support
    if resume_from:
        for i, p in enumerate(parcels):
            if p["parcel_id"] == resume_from:
                skip_count = i
                parcels = parcels[i:]
                print(f"Resuming from {resume_from} (skipped {skip_count} parcels)")
                break

    mode_label = {"1": "PASS 1 (NAIP+FEMA)", "full": "FULL SCAN"}[scan_pass]
    dry_label = " DRY RUN" if dry_run else ""
    print(f"Mode: {mode_label}{dry_label}")
    print(f"Scanning {len(parcels)} parcels")
    print("-" * 60)

    stats = {
        "total": len(parcels),
        "scanned": 0,
        "flagged": 0,
        "sentinel_worthy": 0,
        "written": 0,
        "errors": 0,
        "start_time": datetime.now(),
    }

    flagged_parcels = []

    for i, parcel in enumerate(parcels):
        parcel_id = parcel["parcel_id"]
        lat = float(parcel["latitude"])
        lng = float(parcel["longitude"])

        print(f"\n[{i+1}/{stats['total']}] {parcel_id} ({lat:.4f}, {lng:.4f})", end="")

        try:
            if scan_pass == "1":
                # Pass 1: Free scan only
                result = scan_free(lat, lng)
                flags = result.get("flags", [])
                score = result.get("distress_score", 0)
                worthy = result.get("sentinel_worthy", False)

                status_parts = [f"score={score:.2f}"]
                if flags:
                    status_parts.append(f"flags={[f['signal_code'] for f in flags]}")
                if worthy:
                    status_parts.append("SENTINEL_WORTHY")
                print(f"  {' | '.join(status_parts)}")

                if worthy:
                    stats["sentinel_worthy"] += 1
                    flagged_parcels.append({
                        "parcel_id": parcel_id,
                        "lat": lat,
                        "lng": lng,
                        "score": score,
                        "flags": [f["signal_code"] for f in flags],
                        "naip_ndvi": result.get("naip", {}).get("current_ndvi") if result.get("naip") else None,
                        "fema_risk": result.get("fema", {}).get("risk_level") if result.get("fema") else None,
                    })

            else:
                # Full scan
                if dry_run:
                    result = scan_distress(lat, lng, months=sentinel_months)
                else:
                    result = scan_and_write(
                        lat=lat, lng=lng, parcel_id=parcel_id,
                        county_name=county, state_code=state,
                        months=sentinel_months,
                    )
                    db_status = result.get("db_write", {})
                    if db_status.get("success", 0) > 0:
                        stats["written"] += db_status["success"]

                flags = result.get("flags", [])
                score = result.get("distress_score", 0)
                print(f"  score={score:.2f} flags={[f['signal_code'] for f in flags]}")

            stats["scanned"] += 1
            if flags:
                stats["flagged"] += 1

            if result.get("errors"):
                for err in result["errors"]:
                    logger.warning("scan_error", parcel_id=parcel_id, error=err)

        except KeyboardInterrupt:
            print(f"\n\nInterrupted at parcel {parcel_id}")
            print(f"Resume with: --resume-from {parcel_id}")
            break
        except Exception as e:
            stats["errors"] += 1
            print(f"  ERROR: {e}")
            logger.error("parcel_scan_failed", parcel_id=parcel_id, error=str(e))

        # Rate limiting
        if i < len(parcels) - 1:
            time.sleep(delay)

    # Save flagged parcels for pass 2
    if scan_pass == "1" and flagged_parcels:
        FLAGGED_DIR.mkdir(parents=True, exist_ok=True)
        zip_tag = f"_{mailing_zip}" if mailing_zip else ""
        flagged_file = FLAGGED_DIR / f"{county}{zip_tag}_{datetime.now().strftime('%Y%m%d')}.json"
        flagged_file.write_text(json.dumps(flagged_parcels, indent=2))
        print(f"\nFlagged parcels saved to: {flagged_file}")

    # Summary
    elapsed = (datetime.now() - stats["start_time"]).total_seconds()
    print("\n" + "=" * 60)
    print(f"SCAN COMPLETE — {county}, {state} ({mode_label})")
    print(f"  Scanned:          {stats['scanned']}/{stats['total']}")
    print(f"  Flagged:          {stats['flagged']}")
    if scan_pass == "1":
        print(f"  Sentinel-worthy:  {stats['sentinel_worthy']}")
    if stats["written"]:
        print(f"  Signals written:  {stats['written']}")
    print(f"  Errors:           {stats['errors']}")
    print(f"  Elapsed:          {elapsed:.0f}s ({elapsed/max(stats['scanned'],1):.1f}s/parcel)")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch distress scanner")
    parser.add_argument("--county", required=True, help="County name")
    parser.add_argument("--state", required=True, help="State code")
    parser.add_argument("--pass", dest="scan_pass", choices=["1", "full"], default="full",
                        help="1=free scan (NAIP+FEMA), full=all sources")
    parser.add_argument("--zip", dest="mailing_zip", help="Filter by mailing zip prefix")
    parser.add_argument("--property-class", help="Filter by property_class")
    parser.add_argument("--min-value", type=float, help="Min total_value")
    parser.add_argument("--max-value", type=float, help="Max total_value")
    parser.add_argument("--min-sqft", type=float, help="Min sqft")
    parser.add_argument("--max-sqft", type=float, help="Max sqft")
    parser.add_argument("--limit", type=int, help="Max parcels to scan")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, don't write to DB")
    parser.add_argument("--resume-from", help="Resume from this parcel ID")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between parcels")
    parser.add_argument("--months", type=int, default=12, help="Sentinel lookback months")

    args = parser.parse_args()

    run_county_scan(
        county=args.county,
        state=args.state,
        scan_pass=args.scan_pass,
        limit=args.limit,
        dry_run=args.dry_run,
        resume_from=args.resume_from,
        delay=args.delay,
        sentinel_months=args.months,
        mailing_zip=args.mailing_zip,
        property_class=args.property_class,
        min_value=args.min_value,
        max_value=args.max_value,
        min_sqft=args.min_sqft,
        max_sqft=args.max_sqft,
    )
