#!/usr/bin/env python3
"""
Batch NDVI scoring pipeline — Pass 1.

Scans all unscanned parcels in gis_parcels_core using NAIP NDVI + FEMA flood.
Concurrent via ThreadPoolExecutor. Resumable (skips already-scanned parcels).

Usage:
    PYTHONPATH=. python scripts/batch_ndvi_scan.py --county Gaston --state NC
    PYTHONPATH=. python scripts/batch_ndvi_scan.py --county Gaston --state NC --limit 100 --dry-run
    PYTHONPATH=. python scripts/batch_ndvi_scan.py --county Gaston --state NC --workers 15
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

from src.db import get_db_connection, migrate_add_scan_columns, batch_update_scan_results, get_unscanned_parcels
from src.naip.baseline import naip_ndvi_fast
from src.fema.flood import fema_flood
from src.analysis.flags import generate_all_flags

logger = structlog.get_logger("batch_scan")

# Signal weights (same as scanner.py)
SIGNAL_WEIGHTS = {
    "vegetation_overgrowth": 2.0,
    "vegetation_neglect": 1.5,
    "flood_risk": 1.5,
    "structural_change": 2.5,
}

# Thread-safe results buffer
results_buffer = deque()
buffer_lock = threading.Lock()
shutdown_event = threading.Event()

# Counters
stats = {"scanned": 0, "flagged": 0, "errors": 0, "flushed": 0}
stats_lock = threading.Lock()


def scan_single_parcel(parcel: dict) -> dict | None:
    """Scan a single parcel: NAIP NDVI + FEMA. Returns DB-ready result dict."""
    if shutdown_event.is_set():
        return None

    pid = parcel["parcel_id"]
    lat = float(parcel["latitude"])
    lng = float(parcel["longitude"])
    county = parcel["county"]

    try:
        # NAIP NDVI (1 API call)
        naip = naip_ndvi_fast(lat, lng)

        # FEMA flood (1 API call, skip map export for speed)
        fema = None
        try:
            fema = fema_flood(lat, lng, skip_map=True)
        except Exception as e:
            logger.debug("fema_skip", parcel_id=pid, error=str(e))

        # Build naip dict for flag evaluators (they expect different shape)
        naip_for_flags = None
        if naip and naip.get("ndvi") is not None:
            naip_for_flags = {
                "current_ndvi": naip["ndvi"],
                "mean_historical_ndvi": None,  # no history in fast mode
                "errors": [naip["error"]] if naip.get("error") else [],
            }

        # Evaluate flags
        flags = generate_all_flags(
            naip=naip_for_flags,
            sentinel=None,
            fema=fema,
        )

        # Compute distress score
        score = 0.0
        for flag in flags:
            weight = SIGNAL_WEIGHTS.get(flag["signal_code"], 1.0)
            score += weight * flag["confidence"]
        score = round(min(score, 10.0), 2)

        # Determine sentinel_worthy
        sentinel_worthy = False
        if naip.get("ndvi") is not None and naip["ndvi"] > 0.50:
            sentinel_worthy = True
        if flags:
            sentinel_worthy = True

        # Flag booleans
        flag_codes = {f["signal_code"] for f in flags}
        flag_confs = {f["signal_code"]: f["confidence"] for f in flags}

        result = {
            "parcel_id": pid,
            "county": county,
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
            "sentinel_worthy": sentinel_worthy,
        }

        with stats_lock:
            stats["scanned"] += 1
            if flags:
                stats["flagged"] += 1

        return result

    except Exception as e:
        logger.error("scan_parcel_error", parcel_id=pid, error=str(e))
        with stats_lock:
            stats["errors"] += 1
        # Return a minimal result so the parcel is marked as scanned (won't retry)
        return {
            "parcel_id": pid,
            "county": county,
            "ndvi_score": None,
            "ndvi_date": None,
            "ndvi_category": "error",
            "fema_zone": None,
            "fema_risk": None,
            "fema_sfha": False,
            "distress_score": None,
            "distress_flags": None,
            "flag_veg": False,
            "flag_flood": False,
            "flag_structural": False,
            "flag_neglect": False,
            "veg_confidence": None,
            "flood_confidence": None,
            "scan_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_pass": 1,
            "sentinel_worthy": False,
        }


def flush_buffer(dry_run: bool = False):
    """Flush results buffer to DB using a fresh connection (avoids idle timeout)."""
    with buffer_lock:
        if not results_buffer:
            return
        batch = list(results_buffer)
        results_buffer.clear()

    if dry_run:
        for r in batch:
            ndvi_str = f"{r['ndvi_score']:.3f}" if r['ndvi_score'] is not None else "NULL"
            flags_str = r['distress_flags'] or '--'
            score_str = f"{r['distress_score']:.1f}" if r['distress_score'] is not None else "--"
            print(f"  [DRY] {r['parcel_id']} NDVI={ndvi_str} cat={r['ndvi_category']} "
                  f"score={score_str} flags={flags_str}")
        with stats_lock:
            stats["flushed"] += len(batch)
        return

    try:
        # Fresh connection per flush — Railway PostgreSQL kills idle connections
        flush_conn = get_db_connection()
        try:
            updated = batch_update_scan_results(flush_conn, batch)
            with stats_lock:
                stats["flushed"] += updated
            logger.info("buffer_flushed", batch_size=len(batch), updated=updated)
        finally:
            flush_conn.close()
    except Exception as e:
        logger.error("flush_failed", batch_size=len(batch), error=str(e))
        # Put back in buffer for retry
        with buffer_lock:
            results_buffer.extendleft(batch)


def print_progress(total: int, start_time: float):
    """Print progress line."""
    with stats_lock:
        s = dict(stats)
    elapsed = time.time() - start_time
    rate = s["scanned"] / elapsed if elapsed > 0 else 0
    remaining = total - s["scanned"] - s["errors"]
    eta_sec = remaining / rate if rate > 0 else 0
    eta_min = eta_sec / 60

    pct = (s["scanned"] + s["errors"]) / total * 100 if total > 0 else 0
    print(f"\r  [{s['scanned']+s['errors']}/{total}] {pct:.0f}% | "
          f"flagged={s['flagged']} err={s['errors']} flushed={s['flushed']} | "
          f"{rate:.1f}/sec ETA {eta_min:.0f}m", end="", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Batch NDVI scan pipeline")
    parser.add_argument("--county", required=True, help="County name (e.g. Gaston)")
    parser.add_argument("--state", default="NC", help="State code")
    parser.add_argument("--limit", type=int, default=None, help="Max parcels to scan")
    parser.add_argument("--workers", type=int, default=10, help="Thread pool size")
    parser.add_argument("--flush-every", type=int, default=100, help="Flush to DB every N results")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write to DB")
    parser.add_argument("--property-class", default=None,
                        help="Filter by property_class (e.g. 'Residential 1 Family')")
    args = parser.parse_args()

    print(f"\n=== Batch NDVI Scan — {args.county}, {args.state} ===")
    print(f"    Workers: {args.workers} | Flush every: {args.flush_every}"
          f"{' | DRY RUN' if args.dry_run else ''}")

    # Setup DB
    conn = get_db_connection()

    # Always run migration — even dry-run needs columns to exist for queries
    print("  Running migration...")
    migrate_add_scan_columns(conn)

    # Load unscanned parcels
    print("  Loading unscanned parcels...")
    parcels = get_unscanned_parcels(conn, args.county, args.state, limit=args.limit,
                                        property_class=args.property_class)
    total = len(parcels)
    print(f"  Found {total} unscanned parcels")

    if total == 0:
        print("  Nothing to scan. All parcels already processed.")
        conn.close()
        return

    # Graceful shutdown handler
    def handle_sigint(sig, frame):
        print("\n\n  Ctrl+C — shutting down gracefully, flushing buffer...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    start_time = time.time()
    print(f"  Starting scan at {datetime.now().strftime('%H:%M:%S')}...\n")

    collected = set()  # Track collected future ids to avoid double-collect

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for parcel in parcels:
            if shutdown_event.is_set():
                break
            future = executor.submit(scan_single_parcel, parcel)
            futures[future] = parcel["parcel_id"]

        for future in as_completed(futures):
            if shutdown_event.is_set():
                break

            collected.add(id(future))
            result = future.result()
            if result:
                with buffer_lock:
                    results_buffer.append(result)

                # Flush when buffer hits threshold
                if len(results_buffer) >= args.flush_every:
                    flush_buffer(dry_run=args.dry_run)

            print_progress(total, start_time)

        # On shutdown: executor.__exit__ waits for all in-flight futures to
        # finish. Drain every completed future we haven't collected yet.
        if shutdown_event.is_set():
            print("\n  Waiting for in-flight futures to finish...")

    # Now executor has shut down — all futures are done or cancelled.
    # Collect any we missed during the as_completed loop.
    if shutdown_event.is_set():
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

    # Final flush (includes anything collected during shutdown drain)
    flush_buffer(dry_run=args.dry_run)

    elapsed = time.time() - start_time
    print(f"\n\n=== Scan Complete ===")
    print(f"  Scanned: {stats['scanned']}")
    print(f"  Flagged: {stats['flagged']}")
    print(f"  Errors:  {stats['errors']}")
    print(f"  Written: {stats['flushed']}")
    print(f"  Time:    {elapsed:.0f}s ({elapsed/60:.1f}m)")
    if stats['scanned'] > 0:
        print(f"  Rate:    {stats['scanned']/elapsed:.1f} parcels/sec")
        print(f"  Flag %:  {stats['flagged']/stats['scanned']*100:.1f}%")

    conn.close()


if __name__ == "__main__":
    main()
