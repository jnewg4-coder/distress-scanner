#!/usr/bin/env python3
"""
Batch NDVI historical slope pipeline — Phase 1 of Distress Fusion Engine.

For each parcel that already has Pass 1 NDVI (from batch_ndvi_scan.py),
fetches NAIP NDVI for multiple historical years, computes 5-year slope
via linear regression, and writes the slope to gis_parcels_core.

After all slopes are computed, runs SQL to:
1. Compute percentile ranks within county
2. Compute distress_composite = 0.70 × ndvi_slope_pctile + 0.30 × fema_risk

Prerequisites:
    - batch_ndvi_scan.py already ran (parcels have ndvi_score)
    - FEMA data already populated (fema_zone, fema_risk, fema_sfha)

Usage:
    PYTHONPATH=. python scripts/batch_historical_slope.py --county Gaston --state NC
    PYTHONPATH=. python scripts/batch_historical_slope.py --county Gaston --state NC --limit 100 --dry-run
    PYTHONPATH=. python scripts/batch_historical_slope.py --county Gaston --state NC --composite-only
"""

import argparse
import signal
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import structlog

from src.db import (
    get_db_connection,
    migrate_add_composite_columns,
    batch_update_slope_results,
    compute_composite_scores,
    get_parcels_needing_slope,
)
from src.naip.baseline import naip_ndvi_fast, naip_ndvi_historical, compute_ndvi_slope

logger = structlog.get_logger("batch_slope")

# NC NAIP historical years to check (most recent first)
# Each year = 1 API call per parcel. 6 years × 54K parcels = 324K calls.
HISTORICAL_YEARS = [2022, 2020, 2018, 2016, 2014, 2012]

# Thread-safe results buffer
results_buffer = deque()
buffer_lock = threading.Lock()
shutdown_event = threading.Event()

# Counters
stats = {"processed": 0, "with_slope": 0, "no_history": 0, "errors": 0, "flushed": 0}
stats_lock = threading.Lock()


def compute_slope_for_parcel(parcel: dict) -> dict | None:
    """Fetch historical NDVI and compute slope for a single parcel."""
    if shutdown_event.is_set():
        return None

    pid = parcel["parcel_id"]
    lat = float(parcel["latitude"])
    lng = float(parcel["longitude"])
    county = parcel["county"]
    current_ndvi = parcel.get("ndvi_score")
    current_date = parcel.get("ndvi_date")

    try:
        # Fetch historical NDVI (6 API calls, each cached for 7 days)
        historical = naip_ndvi_historical(lat, lng, years=HISTORICAL_YEARS)

        # Build regression points: (year, ndvi)
        points = []
        years_used = []

        for h in historical:
            points.append((h["year"], h["ndvi"]))
            years_used.append(str(h["year"]))

        # Add current NDVI if we have it
        if current_ndvi is not None and current_date:
            try:
                current_year = int(current_date[:4])
                # Avoid duplicate year
                if current_year not in [h["year"] for h in historical]:
                    points.append((current_year, current_ndvi))
                    years_used.append(str(current_year))
            except (ValueError, TypeError):
                pass

        # Sort by year
        points.sort(key=lambda p: p[0])
        years_used.sort()

        # Compute slope
        slope = compute_ndvi_slope(points)

        result = {
            "parcel_id": pid,
            "county": county,
            "ndvi_slope_5yr": slope,
            "ndvi_history_count": len(points),
            "ndvi_history_years": ",".join(years_used) if years_used else None,
        }

        with stats_lock:
            stats["processed"] += 1
            if slope is not None:
                stats["with_slope"] += 1
            else:
                stats["no_history"] += 1

        return result

    except Exception as e:
        logger.error("slope_parcel_error", parcel_id=pid, error=str(e))
        with stats_lock:
            stats["errors"] += 1
        return None


def flush_buffer(dry_run: bool = False):
    """Flush results buffer to DB using a fresh connection (avoids idle timeout)."""
    with buffer_lock:
        if not results_buffer:
            return
        batch = list(results_buffer)
        results_buffer.clear()

    if dry_run:
        for r in batch:
            slope_str = f"{r['ndvi_slope_5yr']:.6f}" if r['ndvi_slope_5yr'] is not None else "NULL"
            years_str = r['ndvi_history_years'] or '--'
            print(f"  [DRY] {r['parcel_id']} slope={slope_str} "
                  f"pts={r['ndvi_history_count']} years=[{years_str}]")
        with stats_lock:
            stats["flushed"] += len(batch)
        return

    # Filter out None results
    valid = [r for r in batch if r is not None]
    if not valid:
        return

    try:
        # Fresh connection per flush — Railway PostgreSQL kills idle connections
        flush_conn = get_db_connection()
        try:
            updated = batch_update_slope_results(flush_conn, valid)
            with stats_lock:
                stats["flushed"] += updated
            logger.info("slope_buffer_flushed", batch_size=len(valid), updated=updated)
        finally:
            flush_conn.close()
    except Exception as e:
        logger.error("slope_flush_failed", batch_size=len(valid), error=str(e))
        with buffer_lock:
            results_buffer.extendleft(valid)


def print_progress(total: int, start_time: float):
    """Print progress line."""
    with stats_lock:
        s = dict(stats)
    elapsed = time.time() - start_time
    rate = s["processed"] / elapsed if elapsed > 0 else 0
    remaining = total - s["processed"] - s["errors"]
    eta_sec = remaining / rate if rate > 0 else 0
    eta_min = eta_sec / 60

    pct = (s["processed"] + s["errors"]) / total * 100 if total > 0 else 0
    print(f"\r  [{s['processed']+s['errors']}/{total}] {pct:.0f}% | "
          f"sloped={s['with_slope']} noHist={s['no_history']} err={s['errors']} "
          f"flushed={s['flushed']} | "
          f"{rate:.1f}/sec ETA {eta_min:.0f}m", end="", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Batch NDVI slope + composite pipeline")
    parser.add_argument("--county", required=True, help="County name (e.g. Gaston)")
    parser.add_argument("--state", default="NC", help="State code")
    parser.add_argument("--limit", type=int, default=None, help="Max parcels to process")
    parser.add_argument("--workers", type=int, default=10, help="Thread pool size")
    parser.add_argument("--flush-every", type=int, default=50, help="Flush to DB every N results")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write to DB")
    parser.add_argument("--composite-only", action="store_true",
                        help="Skip slope computation, just recompute composite scores from existing slopes")
    parser.add_argument("--ndvi-weight", type=float, default=0.70,
                        help="Weight for NDVI slope in composite (default 0.70)")
    parser.add_argument("--fema-weight", type=float, default=0.30,
                        help="Weight for FEMA risk in composite (default 0.30)")
    args = parser.parse_args()

    print(f"\n=== Batch NDVI Slope + Composite — {args.county}, {args.state} ===")

    # Setup DB
    conn = get_db_connection()

    # Always run migration — even dry-run needs columns to exist for queries
    print("  Running migration...")
    migrate_add_composite_columns(conn)

    if args.composite_only:
        print(f"\n  Computing composite scores (NDVI={args.ndvi_weight}, FEMA={args.fema_weight})...")
        comp_conn = get_db_connection()
        count = compute_composite_scores(comp_conn, args.county,
                                          ndvi_weight=args.ndvi_weight,
                                          fema_weight=args.fema_weight)
        comp_conn.close()
        print(f"  Updated {count} parcels with composite scores.")
        conn.close()
        return

    # Load parcels needing slope
    print("  Loading parcels needing slope computation...")
    parcels = get_parcels_needing_slope(conn, args.county, args.state, limit=args.limit)
    total = len(parcels)
    print(f"  Found {total} parcels needing slope")

    if total == 0:
        print("  No parcels need slope computation.")
        print(f"\n  Computing composite scores (NDVI={args.ndvi_weight}, FEMA={args.fema_weight})...")
        if not args.dry_run:
            comp_conn = get_db_connection()
            count = compute_composite_scores(comp_conn, args.county,
                                              ndvi_weight=args.ndvi_weight,
                                              fema_weight=args.fema_weight)
            comp_conn.close()
            print(f"  Updated {count} parcels with composite scores.")
        conn.close()
        return

    # Graceful shutdown handler
    def handle_sigint(sig, frame):
        print("\n\n  Ctrl+C — shutting down gracefully, flushing buffer...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    start_time = time.time()
    est_calls = total * (len(HISTORICAL_YEARS) + 1)  # +1 for potential current year dedup
    est_time_min = est_calls / (4 * args.workers) / 60
    print(f"  ~{est_calls} API calls estimated, ~{est_time_min:.0f} min at {4*args.workers} calls/sec")
    print(f"  Workers: {args.workers} | Flush every: {args.flush_every}"
          f"{' | DRY RUN' if args.dry_run else ''}")
    print(f"  Starting at {datetime.now().strftime('%H:%M:%S')}...\n")

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for parcel in parcels:
            if shutdown_event.is_set():
                break
            future = executor.submit(compute_slope_for_parcel, parcel)
            futures[future] = parcel["parcel_id"]

        collected = set()
        for future in as_completed(futures):
            if shutdown_event.is_set():
                break

            collected.add(id(future))
            result = future.result()
            if result:
                with buffer_lock:
                    results_buffer.append(result)

                if len(results_buffer) >= args.flush_every:
                    flush_buffer(dry_run=args.dry_run)

            print_progress(total, start_time)

        # Drain on shutdown
        if shutdown_event.is_set():
            print("\n  Waiting for in-flight futures...")
            for f in futures:
                if id(f) in collected:
                    continue
                if f.done() and not f.cancelled():
                    try:
                        result = f.result(timeout=0.1)
                        if result:
                            with buffer_lock:
                                results_buffer.append(result)
                    except Exception:
                        pass

    # Final flush
    flush_buffer(dry_run=args.dry_run)

    elapsed = time.time() - start_time
    print(f"\n\n=== Slope Computation Complete ===")
    print(f"  Processed:  {stats['processed']}")
    print(f"  With slope: {stats['with_slope']}")
    print(f"  No history: {stats['no_history']}")
    print(f"  Errors:     {stats['errors']}")
    print(f"  Flushed:    {stats['flushed']}")
    print(f"  Time:       {elapsed:.0f}s ({elapsed/60:.1f}m)")
    if stats['processed'] > 0:
        print(f"  Rate:       {stats['processed']/elapsed:.1f} parcels/sec")

    # Step 2: Compute composite scores (fresh connection — main conn may be stale)
    if not args.dry_run and not shutdown_event.is_set():
        print(f"\n  Computing composite scores (NDVI={args.ndvi_weight}, FEMA={args.fema_weight})...")
        comp_conn = get_db_connection()
        count = compute_composite_scores(comp_conn, args.county,
                                          ndvi_weight=args.ndvi_weight,
                                          fema_weight=args.fema_weight)
        comp_conn.close()
        print(f"  Updated {count} parcels with composite scores.")

    conn.close()


if __name__ == "__main__":
    main()
