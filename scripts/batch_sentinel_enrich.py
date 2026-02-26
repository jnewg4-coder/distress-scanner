#!/usr/bin/env python3
"""
Batch Sentinel-2 enrichment — Pass 1.5.

Enriches sentinel_worthy parcels with monthly NDVI trends from Sentinel-2
(fallback to Landsat). Rate-limited to respect CDSE quotas (10K req/month).

Usage:
    PYTHONPATH=. python scripts/batch_sentinel_enrich.py --county Gaston --state NC --limit 50 --dry-run
    PYTHONPATH=. python scripts/batch_sentinel_enrich.py --county Gaston --state NC --rate 40
    PYTHONPATH=. python scripts/batch_sentinel_enrich.py --county Gaston --state NC --max-requests 5000
"""

import argparse
import signal
import time
from collections import deque
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import structlog

from src.db import (
    get_db_connection,
    migrate_add_sentinel_columns,
    get_sentinel_worthy_parcels,
    batch_update_sentinel_results,
)
from src.analysis.scanner import enrich_sentinel, rescore_with_sentinel

logger = structlog.get_logger("batch_sentinel")

# Thread-safe results buffer
results_buffer = deque()
shutdown_event = False

# Counters
stats = {
    "enriched": 0,
    "landsat_fallback": 0,
    "errors": 0,
    "flushed": 0,
    "cdse_requests": 0,
}


def flush_buffer(dry_run=False):
    """Flush results buffer to DB using a fresh connection (avoids idle timeout)."""
    if not results_buffer:
        return

    batch = list(results_buffer)
    results_buffer.clear()

    if dry_run:
        for r in batch:
            trend = r.get("sentinel_trend_direction", "--")
            slope = r.get("sentinel_trend_slope")
            slope_str = f"{slope:.4f}" if slope is not None else "NULL"
            src = r.get("sentinel_data_source", "--")
            score = r.get("distress_score")
            score_str = f"{score:.1f}" if score is not None else "--"
            flags = r.get("distress_flags") or "--"
            print(f"  [DRY] {r['parcel_id']} trend={trend} slope={slope_str} "
                  f"src={src} score={score_str} flags={flags}")
        stats["flushed"] += len(batch)
        return

    try:
        # Fresh connection per flush — Railway PostgreSQL kills idle connections
        flush_conn = get_db_connection()
        try:
            updated = batch_update_sentinel_results(flush_conn, batch)
            stats["flushed"] += updated
            logger.info("buffer_flushed", batch_size=len(batch), updated=updated)
        finally:
            flush_conn.close()
    except Exception as e:
        logger.error("flush_failed", batch_size=len(batch), error=str(e))
        # Put back for retry
        results_buffer.extendleft(batch)


def print_progress(total, start_time, max_requests):
    """Print progress line."""
    elapsed = time.time() - start_time
    rate = stats["enriched"] / elapsed * 60 if elapsed > 0 else 0
    done = stats["enriched"] + stats["errors"]
    remaining = total - done
    eta_min = remaining / (rate / 60) / 60 if rate > 0 else 0

    pct = done / total * 100 if total > 0 else 0
    budget = f" | cdse={stats['cdse_requests']}/{max_requests}" if max_requests else ""
    print(f"\r  [{done}/{total}] {pct:.0f}% | "
          f"enriched={stats['enriched']} landsat={stats['landsat_fallback']} "
          f"err={stats['errors']} flushed={stats['flushed']}"
          f"{budget} | {rate:.0f}/min ETA {eta_min:.0f}m", end="", flush=True)


def main():
    global shutdown_event

    parser = argparse.ArgumentParser(description="Batch Sentinel enrichment — Pass 1.5")
    parser.add_argument("--county", required=True, help="County name (e.g. Gaston)")
    parser.add_argument("--state", default="NC", help="State code")
    parser.add_argument("--limit", type=int, default=None, help="Max parcels to enrich")
    parser.add_argument("--rate", type=int, default=40,
                        help="Target parcels per minute (default 40)")
    parser.add_argument("--months", type=int, default=12,
                        help="Sentinel lookback months (default 12)")
    parser.add_argument("--flush-every", type=int, default=50,
                        help="Flush to DB every N results")
    parser.add_argument("--max-requests", type=int, default=None,
                        help="Hard cap on CDSE API requests (budget guard)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results, don't write to DB")
    args = parser.parse_args()

    print(f"\n=== Batch Sentinel Enrichment — {args.county}, {args.state} ===")
    print(f"    Rate: {args.rate}/min | Months: {args.months} | "
          f"Flush every: {args.flush_every}"
          f"{' | DRY RUN' if args.dry_run else ''}")
    if args.max_requests:
        print(f"    CDSE budget cap: {args.max_requests} requests")

    # Graceful shutdown
    def handle_sigint(sig, frame):
        global shutdown_event
        print("\n\n  Ctrl+C — flushing remaining buffer...")
        shutdown_event = True

    signal.signal(signal.SIGINT, handle_sigint)

    # Setup DB
    conn = get_db_connection()

    if not args.dry_run:
        print("  Running Sentinel migration...")
        migrate_add_sentinel_columns(conn)

    # Load sentinel_worthy parcels (sorted by distress_score DESC)
    print("  Loading sentinel-worthy parcels...")
    parcels = get_sentinel_worthy_parcels(conn, args.county, args.state,
                                           limit=args.limit)
    total = len(parcels)
    print(f"  Found {total} parcels pending Sentinel enrichment")

    if total == 0:
        print("  Nothing to enrich. All sentinel_worthy parcels already processed.")
        conn.close()
        return

    start_time = time.time()
    delay = 60.0 / args.rate  # seconds between parcels
    backoff = 1.0  # adaptive backoff multiplier

    print(f"  Starting enrichment at {datetime.now().strftime('%H:%M:%S')}...\n")

    for i, parcel in enumerate(parcels):
        if shutdown_event:
            break

        # Budget guard
        if args.max_requests and stats["cdse_requests"] >= args.max_requests:
            print(f"\n\n  CDSE budget cap reached ({args.max_requests} requests). Stopping.")
            break

        pid = parcel["parcel_id"]
        lat = float(parcel["latitude"])
        lng = float(parcel["longitude"])
        county = parcel["county"]

        loop_start = time.monotonic()

        try:
            # Enrich with Sentinel (1 CDSE request, no RGB)
            sentinel_result = enrich_sentinel(lat, lng, months=args.months,
                                               include_rgb=False)

            # Track CDSE usage
            if sentinel_result.get("sentinel_data_source") == "Sentinel-2":
                stats["cdse_requests"] += 1
                backoff = max(backoff * 0.9, 1.0)  # reduce backoff on success
            elif sentinel_result.get("sentinel_data_source") == "Landsat":
                stats["landsat_fallback"] += 1

            # Re-score flags with Sentinel data
            fema_data = {
                "fema_zone": parcel.get("fema_zone"),
                "fema_risk": parcel.get("fema_risk"),
                "fema_sfha": parcel.get("fema_sfha"),
            }
            rescore = rescore_with_sentinel(
                naip_ndvi=parcel.get("ndvi_score"),
                fema_data=fema_data if parcel.get("fema_zone") else None,
                sentinel_result=sentinel_result,
            )

            # Build DB-ready result
            db_row = {
                "parcel_id": pid,
                "county": county,
                **{k: v for k, v in sentinel_result.items() if not k.startswith("_")
                   and k != "errors"},
                **rescore,
            }

            results_buffer.append(db_row)
            stats["enriched"] += 1

        except Exception as e:
            logger.error("enrich_error", parcel_id=pid, error=str(e))
            stats["errors"] += 1

            # Check for rate limit / server errors — increase backoff
            err_str = str(e).lower()
            if "429" in err_str or "503" in err_str or "500" in err_str:
                backoff = min(backoff * 2, 10.0)
                logger.warning("backoff_increased", backoff=backoff, error=err_str)

        # Flush when buffer hits threshold
        if len(results_buffer) >= args.flush_every:
            flush_buffer(dry_run=args.dry_run)

        print_progress(total, start_time, args.max_requests)

        # Adaptive rate limiting
        elapsed = time.monotonic() - loop_start
        target_delay = delay * backoff
        if elapsed < target_delay:
            time.sleep(target_delay - elapsed)

    # Final flush
    flush_buffer(dry_run=args.dry_run)

    elapsed = time.time() - start_time
    print(f"\n\n=== Enrichment Complete ===")
    print(f"  Enriched:    {stats['enriched']}")
    print(f"  Landsat FB:  {stats['landsat_fallback']}")
    print(f"  Errors:      {stats['errors']}")
    print(f"  Written:     {stats['flushed']}")
    print(f"  CDSE Reqs:   {stats['cdse_requests']}")
    print(f"  Time:        {elapsed:.0f}s ({elapsed/60:.1f}m)")
    if stats['enriched'] > 0:
        print(f"  Rate:        {stats['enriched']/elapsed*60:.1f} parcels/min")

    conn.close()


if __name__ == "__main__":
    main()
