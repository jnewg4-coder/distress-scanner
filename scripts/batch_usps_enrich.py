#!/usr/bin/env python3
"""
Batch USPS vacancy enrichment pipeline — Phase 2.

Checks top distress leads (by composite score) for USPS carrier-confirmed
vacancy. Two-phase: pre-resolve addresses via situs parsing + Nominatim,
then parallel USPS API calls via shared queue with multiple accounts.

USPS hard limit: 60 req/hr per Consumer Key. Default jitter 55-65s keeps
each account safely under quota. 2 accounts = ~116 checks/hr combined.

Prerequisites:
    - Phase 1 complete (batch_ndvi_scan.py + batch_historical_slope.py)
    - Parcels have distress_composite scores
    - USPS_CLIENT_ID / USPS_CLIENT_SECRET in .env (account 1)
    - USPS_CLIENT_ID_3 / USPS_CLIENT_SECRET_3 in .env (account 3)

Usage:
    PYTHONPATH=. python scripts/batch_usps_enrich.py --county Gaston --state NC
    PYTHONPATH=. python scripts/batch_usps_enrich.py --county Gaston --state NC --limit 50 --dry-run
    PYTHONPATH=. python scripts/batch_usps_enrich.py --county Gaston --state NC --accounts 1 --limit 100
"""

import argparse
import atexit
import json
import os
import queue
import signal
import sys
import time
import threading
from collections import deque
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import structlog

from src.db import (
    get_db_connection,
    migrate_add_usps_columns,
    get_parcels_needing_usps,
    batch_update_usps_results,
    save_usps_check,
)
from src.usps.vacancy import USPSVacancyChecker, split_situs
from src.analysis.flags import evaluate_usps_vacancy

logger = structlog.get_logger("batch_usps")

LOCK_FILE = "/tmp/batch_usps_enrich.lock"
BACKUP_DIR = "/tmp/usps_backups"

# Thread-safe results buffer
results_buffer = deque()
buffer_lock = threading.Lock()
shutdown_event = threading.Event()

# Counters
stats = {"checked": 0, "vacant": 0, "occupied": 0, "errors": 0,
         "skipped_no_addr": 0, "flushed": 0}
stats_lock = threading.Lock()


def _save_local_backup(db_batch: list[dict]):
    """Append results to a local JSON-lines file as DB outage insurance."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    backup_file = os.path.join(BACKUP_DIR, f"usps_results_{datetime.now().strftime('%Y%m%d')}.jsonl")
    with open(backup_file, "a") as f:
        for row in db_batch:
            # Serialize datetime-safe
            safe = {k: (v.isoformat() if isinstance(v, datetime) else v)
                    for k, v in row.items()}
            f.write(json.dumps(safe) + "\n")
    logger.info("usps_backup_saved", file=backup_file, records=len(db_batch))


def replay_backup(backup_file: str):
    """Replay a JSONL backup file into the database. Run after DB recovery."""
    if not os.path.exists(backup_file):
        print(f"  Backup file not found: {backup_file}")
        return

    with open(backup_file) as f:
        records = [json.loads(line) for line in f if line.strip()]

    if not records:
        print("  No records in backup file.")
        return

    print(f"  Replaying {len(records)} records from {backup_file}...")
    try:
        conn = get_db_connection()
        updated = batch_update_usps_results(conn, records)
        conn.close()
        print(f"  Replayed {updated} records to DB.")
        # Rename backup to mark as replayed
        replayed = backup_file + ".replayed"
        os.rename(backup_file, replayed)
        print(f"  Backup renamed to {replayed}")
    except Exception as e:
        print(f"  Replay failed: {e}")


def acquire_lock():
    """Prevent concurrent runs from burning double quota."""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE) as f:
                pid = int(f.read().strip())
            # Check if process is still running
            os.kill(pid, 0)
            print(f"  ERROR: Another instance is running (PID {pid}). "
                  f"Remove {LOCK_FILE} if stale.")
            sys.exit(1)
        except (ProcessLookupError, ValueError):
            # Stale lock file — remove it
            os.remove(LOCK_FILE)

    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    atexit.register(release_lock)


def release_lock():
    """Remove lock file on exit."""
    try:
        os.remove(LOCK_FILE)
    except FileNotFoundError:
        pass


def pre_resolve_addresses(parcels: list[dict], state: str = "NC") -> tuple[list[dict], list[dict]]:
    """
    Phase A: Parse situs addresses and resolve missing city/zip via Nominatim.
    Single-threaded to respect Nominatim 1 req/sec policy.

    Returns (resolved, skipped) — resolved have street + (city OR zip).
    """
    resolved = []
    skipped = []
    nominatim_calls = 0

    for i, p in enumerate(parcels):
        situs = p.get("situs_address", "")
        if not situs or not situs.strip():
            skipped.append({**p, "skip_reason": "no_situs"})
            continue

        # Parse situs
        parsed = split_situs(situs, fallback_state=state)
        street = parsed.get("street", "").strip()
        city = parsed.get("city")
        zip_code = parsed.get("zip_code")
        parsed_state = parsed.get("state") or state

        if not street:
            skipped.append({**p, "skip_reason": "no_street"})
            continue

        # If both city and zip missing, try Nominatim
        if not city and not zip_code:
            try:
                from src.usps.geocode import resolve_city_zip
                geo = resolve_city_zip(
                    street,
                    p.get("county", ""),
                    parsed_state,
                    lat=float(p["latitude"]) if p.get("latitude") else None,
                    lng=float(p["longitude"]) if p.get("longitude") else None,
                )
                nominatim_calls += 1
                city = geo.get("city") or city
                zip_code = geo.get("zip") or zip_code
            except Exception as e:
                logger.debug("nominatim_failed", parcel_id=p["parcel_id"], error=str(e))

        # Fallback: use mailing_city/mailing_zip from GIS data
        # Only when mailing_state matches property state (skip investor out-of-state)
        if not city and not zip_code:
            mail_state = (p.get("mailing_state") or "").strip().upper()
            if mail_state == parsed_state.upper():
                mail_city = (p.get("mailing_city") or "").strip()
                mail_zip = (p.get("mailing_zip") or "").strip()[:5]  # Trim 9-digit zips
                if mail_city or mail_zip:
                    city = mail_city or None
                    zip_code = mail_zip or None
                    logger.debug("mailing_fallback", street=street, city=city, zip=zip_code,
                                 reason="same_state_mailing_address")

        # Pre-call guard: need city OR zip
        if not city and not zip_code:
            skipped.append({**p, "skip_reason": "no_city_no_zip"})
            continue

        resolved.append({
            **p,
            "usps_street": street,
            "usps_city": city,
            "usps_state": parsed_state,
            "usps_zip": zip_code,
        })

        if (i + 1) % 50 == 0:
            print(f"\r  Pre-resolve: {i+1}/{len(parcels)} "
                  f"(resolved={len(resolved)}, skipped={len(skipped)}, "
                  f"nominatim={nominatim_calls})", end="", flush=True)

    print(f"\r  Pre-resolve complete: {len(resolved)} resolved, "
          f"{len(skipped)} skipped, {nominatim_calls} Nominatim calls")

    return resolved, skipped


def check_single_parcel(parcel: dict, checker: USPSVacancyChecker) -> dict:
    """Check a single pre-resolved parcel via USPS. Returns DB-ready result dict."""
    pid = parcel["parcel_id"]
    county = parcel["county"]

    result = checker.check_address(
        parcel["usps_street"],
        city=parcel.get("usps_city"),
        state=parcel.get("usps_state"),
        zip_code=parcel.get("usps_zip"),
    )

    # Evaluate vacancy flag
    usps_data = {
        "vacant": result.vacant,
        "dpv_confirmed": result.dpv_confirmed,
        "address_mismatch": result.address_mismatch,
        "usps_address": result.usps_address,
        "usps_city": result.usps_city,
        "usps_zip": result.usps_zip,
        "carrier_route": result.carrier_route,
    }
    flag = evaluate_usps_vacancy(usps_data)

    return {
        "parcel_id": pid,
        "county": county,
        "usps_vacant": result.vacant,
        "usps_dpv_confirmed": result.dpv_confirmed,
        "usps_address": result.usps_address,
        "usps_city": result.usps_city,
        "usps_zip": result.usps_zip,
        "usps_zip4": result.usps_zip4,
        "usps_business": result.business,
        "usps_carrier_route": result.carrier_route,
        "usps_address_mismatch": result.address_mismatch,
        "usps_error": result.error,
        "flag_vacancy": flag["flag"],
        "vacancy_confidence": flag.get("confidence"),
        # Keep raw result for audit
        "_result": result,
        "_account": checker.account,
    }


def consumer_thread(work_queue: queue.Queue, checker: USPSVacancyChecker,
                    flush_every: int, dry_run: bool):
    """Consumer thread: pull parcels from shared queue, check via USPS."""
    consecutive_errors = 0

    while not shutdown_event.is_set():
        try:
            parcel = work_queue.get(timeout=2)
        except queue.Empty:
            break  # Queue exhausted

        try:
            result = check_single_parcel(parcel, checker)

            with buffer_lock:
                results_buffer.append(result)

            with stats_lock:
                stats["checked"] += 1
                if result["usps_error"]:
                    stats["errors"] += 1
                    consecutive_errors += 1
                else:
                    consecutive_errors = 0
                    if result["usps_vacant"] is True:
                        stats["vacant"] += 1
                    elif result["usps_vacant"] is False:
                        stats["occupied"] += 1

            # Flush when buffer hits threshold
            if len(results_buffer) >= flush_every:
                flush_buffer(dry_run=dry_run)

            # Safety: consecutive error circuit breaker
            if consecutive_errors >= 10:
                logger.warning("consecutive_errors_pause",
                               account=checker.account,
                               consecutive=consecutive_errors)
                print(f"\n  WARN: Account {checker.account} hit {consecutive_errors} "
                      f"consecutive errors — pausing 5 min")
                time.sleep(300)
                consecutive_errors = 0

            if consecutive_errors >= 20:
                logger.error("consecutive_errors_abort",
                             account=checker.account)
                print(f"\n  ABORT: Account {checker.account} hit 20 consecutive errors")
                shutdown_event.set()
                break

        except Exception as e:
            logger.error("consumer_error", account=checker.account,
                         parcel_id=parcel.get("parcel_id"), error=str(e))
            consecutive_errors += 1
        finally:
            work_queue.task_done()


def flush_buffer(dry_run: bool = False):
    """Flush results buffer to DB using a fresh connection."""
    with buffer_lock:
        if not results_buffer:
            return
        batch = list(results_buffer)
        results_buffer.clear()

    if dry_run:
        for r in batch:
            vacant_str = ("VACANT" if r["usps_vacant"] else
                          "occupied" if r["usps_vacant"] is False else
                          "unknown")
            conf = r.get("vacancy_confidence")
            conf_str = f"{conf:.2f}" if conf else "--"
            addr = r.get("usps_address") or "--"
            err = r.get("usps_error") or ""
            print(f"  [DRY] {r['parcel_id']}  {vacant_str}  conf={conf_str}  "
                  f"addr={addr}  {err}")
        with stats_lock:
            stats["flushed"] += len(batch)
        return

    # Strip internal fields before DB write
    db_batch = []
    for r in batch:
        db_row = {k: v for k, v in r.items() if not k.startswith("_")}
        db_batch.append(db_row)

    try:
        flush_conn = get_db_connection()
        try:
            updated = batch_update_usps_results(flush_conn, db_batch)
            with stats_lock:
                stats["flushed"] += updated
            logger.info("usps_buffer_flushed", batch_size=len(db_batch), updated=updated)
        finally:
            flush_conn.close()
    except Exception as e:
        logger.error("usps_flush_failed", batch_size=len(db_batch), error=str(e))
        # Save to local JSON backup so we never lose data on DB outage
        _save_local_backup(db_batch)
        with buffer_lock:
            results_buffer.extendleft(batch)


def print_progress(total: int, start_time: float):
    """Print progress line."""
    with stats_lock:
        s = dict(stats)
    elapsed = time.time() - start_time
    rate = s["checked"] / elapsed * 3600 if elapsed > 0 else 0
    remaining = total - s["checked"]
    eta_min = remaining / (rate / 60) if rate > 0 else 0

    print(f"\r  [{s['checked']}/{total}] "
          f"vacant={s['vacant']} occ={s['occupied']} err={s['errors']} "
          f"flushed={s['flushed']} | {rate:.0f}/hr ETA {eta_min:.0f}m",
          end="", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Batch USPS vacancy enrichment")
    parser.add_argument("--county", required=True, help="County name (e.g. Gaston)")
    parser.add_argument("--state", default="NC", help="State code")
    parser.add_argument("--limit", type=int, default=500, help="Max parcels to check")
    parser.add_argument("--min-composite", type=float, default=7.0,
                        help="Min distress composite score (default 7.0)")
    parser.add_argument("--accounts", default="1,3",
                        help="Comma-separated USPS account numbers (default '1,3')")
    parser.add_argument("--delay-min", type=int, default=55,
                        help="Min delay between USPS requests per account (sec)")
    parser.add_argument("--delay-max", type=int, default=65,
                        help="Max delay between USPS requests per account (sec)")
    parser.add_argument("--flush-every", type=int, default=20,
                        help="Flush to DB every N results")
    parser.add_argument("--cache-days", type=int, default=60,
                        help="Skip parcels checked within this many days")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print results, don't write to DB")
    parser.add_argument("--property-class", default=None,
                        help="Filter by property_class")
    parser.add_argument("--replay", default=None,
                        help="Replay a JSONL backup file into DB (after outage recovery)")
    args = parser.parse_args()

    # Replay mode — just push backup to DB and exit
    if args.replay:
        replay_backup(args.replay)
        return

    account_nums = [int(a.strip()) for a in args.accounts.split(",")]

    print(f"\n=== Batch USPS Vacancy Enrichment — {args.county}, {args.state} ===")
    print(f"    Accounts: {account_nums} | Delay: {args.delay_min}-{args.delay_max}s"
          f" | Min composite: {args.min_composite}"
          f"{' | DRY RUN' if args.dry_run else ''}")

    # Lock file
    if not args.dry_run:
        acquire_lock()

    # Setup DB
    conn = get_db_connection()
    print("  Running migration...")
    migrate_add_usps_columns(conn)

    # Load parcels
    print(f"  Loading top leads (composite >= {args.min_composite})...")
    parcels = get_parcels_needing_usps(
        conn, args.county, args.state,
        limit=args.limit,
        min_composite=args.min_composite,
        cache_days=args.cache_days,
        property_class=args.property_class,
    )
    total_raw = len(parcels)
    print(f"  Found {total_raw} parcels needing USPS check")

    if total_raw == 0:
        print("  Nothing to check. All top leads already processed.")
        conn.close()
        return

    # Show score range
    if parcels:
        top_score = parcels[0].get("distress_composite")
        bot_score = parcels[-1].get("distress_composite")
        print(f"  Score range: {top_score} → {bot_score}")

    conn.close()

    # Phase A: Pre-resolve addresses
    print(f"\n  Phase A: Pre-resolving {total_raw} addresses...")
    resolved, skipped = pre_resolve_addresses(parcels, state=args.state)

    with stats_lock:
        stats["skipped_no_addr"] = len(skipped)

    if not resolved:
        print("  No resolvable addresses. Check situs_address data.")
        return

    total = len(resolved)
    est_hr = total / (len(account_nums) * 58)
    print(f"\n  Phase B: USPS checking {total} parcels with {len(account_nums)} account(s)")
    print(f"  Estimated time: ~{est_hr:.1f} hours")

    # Graceful shutdown
    def handle_sigint(sig, frame):
        print("\n\n  Ctrl+C — shutting down, flushing buffer...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    # Build shared queue
    work_queue = queue.Queue()
    for p in resolved:
        work_queue.put(p)

    # Create checker instances
    checkers = []
    for acct in account_nums:
        try:
            checker = USPSVacancyChecker(
                account=acct,
                delay_min=args.delay_min,
                delay_max=args.delay_max,
            )
            checkers.append(checker)
            print(f"  Account {acct}: initialized")
        except ValueError as e:
            print(f"  Account {acct}: SKIPPED — {e}")

    if not checkers:
        print("  ERROR: No valid USPS accounts. Check .env credentials.")
        return

    start_time = time.time()
    print(f"  Starting at {datetime.now().strftime('%H:%M:%S')}...\n")

    # Launch consumer threads
    threads = []
    for checker in checkers:
        t = threading.Thread(
            target=consumer_thread,
            args=(work_queue, checker, args.flush_every, args.dry_run),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Monitor progress
    while any(t.is_alive() for t in threads):
        time.sleep(5)
        print_progress(total, start_time)
        if shutdown_event.is_set():
            break

    # Wait for threads to finish
    for t in threads:
        t.join(timeout=30)

    # Final flush
    flush_buffer(dry_run=args.dry_run)

    # Emergency backup if buffer still has unflushed data (DB still down)
    with buffer_lock:
        if results_buffer and not args.dry_run:
            unflushed = [{k: v for k, v in r.items() if not k.startswith("_")}
                         for r in results_buffer]
            _save_local_backup(unflushed)
            print(f"\n  WARNING: {len(unflushed)} records saved to local backup "
                  f"(DB unreachable). Run --replay after DB recovery.")

    elapsed = time.time() - start_time
    print(f"\n\n=== USPS Enrichment Complete ===")
    print(f"  Checked:   {stats['checked']}")
    print(f"  Vacant:    {stats['vacant']}")
    print(f"  Occupied:  {stats['occupied']}")
    print(f"  Errors:    {stats['errors']}")
    print(f"  Skipped:   {stats['skipped_no_addr']} (no resolvable address)")
    print(f"  Flushed:   {stats['flushed']}")
    print(f"  Time:      {elapsed:.0f}s ({elapsed/60:.1f}m)")
    if stats["checked"] > 0:
        rate_hr = stats["checked"] / elapsed * 3600
        print(f"  Rate:      {rate_hr:.0f} checks/hr")
        vacancy_rate = stats["vacant"] / stats["checked"] * 100
        print(f"  Vacancy %: {vacancy_rate:.1f}%")


if __name__ == "__main__":
    main()
