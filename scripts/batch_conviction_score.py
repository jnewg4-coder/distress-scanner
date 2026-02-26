#!/usr/bin/env python3
"""
Batch Conviction Score Fusion — Phase 2.5

Fuses Distress Scanner (DS) composite + Motivated Curator (MC) signals + USPS vacancy
into a single conviction_score per parcel.

Formula (Implementation Contract v1.0):
  - Reweighted average of DS + MC components (missing = excluded, not zero)
  - Vacancy bonus added on top (only when flag_vacancy=True AND no usps_error)
  - DS-only parcels pass through as distress_composite
  - MC-only parcels use 10 * mc_component
  - No DS + no MC = NULL (not rankable)

Usage:
    PYTHONPATH=. python scripts/batch_conviction_score.py --county Gaston --state NC
    PYTHONPATH=. python scripts/batch_conviction_score.py --county Gaston --state NC --dry-run
"""

import argparse
import json
import time
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import structlog
from psycopg2.extras import execute_batch

from src.db import get_db_connection, migrate_add_conviction_columns

logger = structlog.get_logger("batch_conviction")

# --- Conviction Model Constants (v1.0) ---
W_DS = 0.35
W_MC = 0.40
MC_CAP = 7.0
VAC_BONUS_MAX = 2.5
MODEL_VERSION = "v1.0"


def clamp(x, lo, hi):
    return max(lo, min(x, hi))


def compute_conviction(ds_composite, mc_raw, mc_count, flag_vacancy, vac_conf, usps_error):
    """
    Compute conviction score per Implementation Contract v1.0.

    Returns (conviction_score, base_score, vacancy_bonus, components_list)
    All may be None if not rankable.
    """
    # ds_component
    ds_comp = None if ds_composite is None else clamp(ds_composite / 10.0, 0, 1)

    # mc_component — NULL if no signals (missing coverage, not zero evidence)
    mc_comp = None if (mc_count is None or mc_count == 0) else clamp(mc_raw / MC_CAP, 0, 1)

    # vacancy_bonus — only if flag_vacancy=True AND no usps_error
    vac_bonus = 0.0
    if flag_vacancy and not usps_error:
        vc = clamp(vac_conf if vac_conf is not None else 0.8, 0, 1)
        vac_bonus = VAC_BONUS_MAX * vc

    # base weight sum (reweighted — skip missing components)
    base_sum = (W_DS if ds_comp is not None else 0) + (W_MC if mc_comp is not None else 0)

    if base_sum == 0:
        return None, None, round(vac_bonus, 2), []

    base = 10 * ((W_DS * (ds_comp or 0)) + (W_MC * (mc_comp or 0))) / base_sum
    score = round(clamp(base + vac_bonus, 0, 10), 2)

    components = []
    if ds_comp is not None:
        components.append("DS")
    if mc_comp is not None:
        components.append("MC")
    if vac_bonus > 0:
        components.append("VAC")

    return score, round(base, 2), round(vac_bonus, 2), components


def fetch_parcel_data(conn, county: str, state: str):
    """
    Phase A: Fetch all parcels with MC signal aggregates using canonical JOIN.
    Returns list of dicts with DS + MC + USPS fields.
    """
    query = """
        SELECT
            g.parcel_id,
            g.distress_composite,
            g.flag_vacancy,
            g.vacancy_confidence,
            g.usps_error,
            COALESCE(SUM(st.base_weight * LEAST(GREATEST(ps.confidence, 0), 1)), 0) AS mc_raw_score,
            COUNT(ps.id) AS mc_signal_count,
            STRING_AGG(DISTINCT st.code, ',' ORDER BY st.code) AS mc_signal_codes
        FROM gis_parcels_core g
        JOIN counties c
            ON lower(c.name) = lower(g.county)
            AND c.state_code = g.state_code
        LEFT JOIN parcels p
            ON p.county_id = c.id
            AND p.parcel_id = g.parcel_id
        LEFT JOIN parcel_signals ps
            ON ps.parcel_id = p.id
            AND ps.is_active = true
            AND (ps.expires_at IS NULL OR ps.expires_at > NOW())
        LEFT JOIN signal_types st
            ON st.id = ps.signal_type_id
            AND st.is_active = true
        WHERE g.county = %s AND g.state_code = %s
        GROUP BY g.parcel_id, g.distress_composite, g.flag_vacancy,
                 g.vacancy_confidence, g.usps_error
    """
    with conn.cursor() as cur:
        cur.execute(query, (county, state))
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def compute_all_scores(parcels: list[dict]) -> list[dict]:
    """Phase B: Compute conviction scores for all parcels."""
    results = []
    for p in parcels:
        score, base, vac_bonus, components = compute_conviction(
            ds_composite=p["distress_composite"],
            mc_raw=float(p["mc_raw_score"]) if p["mc_raw_score"] else 0,
            mc_count=p["mc_signal_count"],
            flag_vacancy=p["flag_vacancy"],
            vac_conf=float(p["vacancy_confidence"]) if p["vacancy_confidence"] else None,
            usps_error=p["usps_error"],
        )
        results.append({
            "parcel_id": p["parcel_id"],
            "conviction_score": score,
            "conviction_base_score": base,
            "conviction_vacancy_bonus": vac_bonus,
            "conviction_mc_score": float(p["mc_raw_score"]) if p["mc_raw_score"] and p["mc_signal_count"] > 0 else None,
            "conviction_mc_signals": p["mc_signal_count"] if p["mc_signal_count"] > 0 else None,
            "conviction_mc_codes": p["mc_signal_codes"],
            "conviction_components": ",".join(components) if components else None,
        })
    return results


def flush_conviction_scores(results: list[dict], county: str, chunk_size: int = 5000):
    """Phase C: Batch UPDATE gis_parcels_core with conviction scores."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            for i in range(0, len(results), chunk_size):
                chunk = results[i:i + chunk_size]
                execute_batch(cur, """
                    UPDATE gis_parcels_core SET
                        conviction_score = %(conviction_score)s,
                        conviction_base_score = %(conviction_base_score)s,
                        conviction_vacancy_bonus = %(conviction_vacancy_bonus)s,
                        conviction_mc_score = %(conviction_mc_score)s,
                        conviction_mc_signals = %(conviction_mc_signals)s,
                        conviction_mc_codes = %(conviction_mc_codes)s,
                        conviction_components = %(conviction_components)s,
                        conviction_date = NOW()
                    WHERE parcel_id = %(parcel_id)s AND county = %(county)s
                """, [{**r, "county": county} for r in chunk], page_size=500)
                conn.commit()
                logger.info("conviction_flush", chunk=i // chunk_size + 1,
                            rows=len(chunk), total_so_far=min(i + chunk_size, len(results)))
    finally:
        conn.close()


def backfill_motivation_scores(county: str, state: str, parcels_data: list[dict]):
    """
    Phase D: Backfill MC's motivation_scores table.
    County-scoped DELETE + INSERT (not ON CONFLICT — schema uses (parcel_id, computed_at) unique).
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Delete existing motivation_scores for this county
            cur.execute("""
                DELETE FROM motivation_scores WHERE parcel_id IN (
                    SELECT p.id FROM parcels p
                    JOIN counties c ON p.county_id = c.id
                    WHERE lower(c.name) = lower(%s) AND c.state_code = %s
                )
            """, (county, state))
            deleted = cur.rowcount
            logger.info("motivation_scores_deleted", county=county, rows=deleted)

            # Insert only parcels with MC signals
            mc_parcels = [p for p in parcels_data if p["mc_signal_count"] > 0]
            if mc_parcels:
                execute_batch(cur, """
                    INSERT INTO motivation_scores (parcel_id, total_score, signal_count, score_breakdown, computed_at)
                    SELECT p.id, %(mc_raw_score)s, %(mc_signal_count)s,
                           %(score_breakdown)s::jsonb, NOW()
                    FROM parcels p
                    JOIN counties c ON p.county_id = c.id
                    WHERE p.parcel_id = %(parcel_id)s
                      AND lower(c.name) = lower(%(county)s)
                      AND c.state_code = %(state)s
                """, [{
                    "parcel_id": p["parcel_id"],
                    "mc_raw_score": float(p["mc_raw_score"]),
                    "mc_signal_count": p["mc_signal_count"],
                    "score_breakdown": json.dumps({
                        "signals": (p["mc_signal_codes"] or "").split(","),
                        "raw_score": float(p["mc_raw_score"]),
                        "model": MODEL_VERSION,
                    }),
                    "county": county,
                    "state": state,
                } for p in mc_parcels], page_size=500)

            conn.commit()
            logger.info("motivation_scores_inserted", county=county, rows=len(mc_parcels))
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Batch Conviction Score Fusion")
    parser.add_argument("--county", required=True)
    parser.add_argument("--state", default="NC")
    parser.add_argument("--dry-run", action="store_true", help="Compute but don't write")
    parser.add_argument("--skip-motivation", action="store_true", help="Skip motivation_scores backfill")
    args = parser.parse_args()

    print(f"\n=== Conviction Score Fusion — {args.county}, {args.state} ===")
    print(f"    Model: {MODEL_VERSION} | W_DS={W_DS} W_MC={W_MC} MC_CAP={MC_CAP} VAC_BONUS={VAC_BONUS_MAX}")
    start = time.time()

    # Migration
    conn = get_db_connection()
    print("  Running migration...")
    migrate_add_conviction_columns(conn)
    conn.close()

    # Phase A: Fetch + aggregate
    print("  Phase A: Fetching parcels + MC signal aggregates...")
    conn = get_db_connection()
    parcels = fetch_parcel_data(conn, args.county, args.state)
    conn.close()
    print(f"  Loaded {len(parcels)} parcels")

    mc_parcels = sum(1 for p in parcels if p["mc_signal_count"] > 0)
    ds_parcels = sum(1 for p in parcels if p["distress_composite"] is not None)
    vac_parcels = sum(1 for p in parcels if p["flag_vacancy"])
    print(f"  Coverage: {ds_parcels} DS | {mc_parcels} MC | {vac_parcels} USPS-vacant")

    # Phase B: Compute scores
    print("  Phase B: Computing conviction scores...")
    results = compute_all_scores(parcels)

    scored = sum(1 for r in results if r["conviction_score"] is not None)
    scores = [r["conviction_score"] for r in results if r["conviction_score"] is not None]
    if scores:
        avg_score = sum(scores) / len(scores)
        max_score = max(scores)
        min_score = min(scores)
    else:
        avg_score = max_score = min_score = 0

    # Component distribution
    comp_dist = {}
    for r in results:
        key = r["conviction_components"] or "NULL"
        comp_dist[key] = comp_dist.get(key, 0) + 1

    print(f"  Scored: {scored}/{len(parcels)} (avg={avg_score:.2f}, min={min_score:.2f}, max={max_score:.2f})")
    print(f"  Component distribution:")
    for k, v in sorted(comp_dist.items(), key=lambda x: -x[1]):
        print(f"    {k:<12} {v:>6} parcels")

    if args.dry_run:
        print("\n  [DRY RUN] — no writes performed")
        # Show sample results
        for r in sorted(results, key=lambda x: -(x["conviction_score"] or 0))[:10]:
            print(f"    {r['parcel_id']:<15} score={r['conviction_score']}  base={r['conviction_base_score']}  "
                  f"vac_bonus={r['conviction_vacancy_bonus']}  mc={r['conviction_mc_score']}  "
                  f"components={r['conviction_components']}")
        return

    # Phase C: Write conviction scores
    print("  Phase C: Writing conviction scores to DB...")
    flush_conviction_scores(results, args.county)
    print(f"  Written: {len(results)} rows")

    # Phase D: Backfill motivation_scores
    if not args.skip_motivation:
        print("  Phase D: Backfilling motivation_scores...")
        backfill_motivation_scores(args.county, args.state, parcels)
    else:
        print("  Phase D: Skipped (--skip-motivation)")

    elapsed = time.time() - start
    print(f"\n=== Conviction Score Complete ===")
    print(f"  Parcels:    {len(parcels)}")
    print(f"  Scored:     {scored}")
    print(f"  MC joined:  {mc_parcels}")
    print(f"  Vacant:     {vac_parcels}")
    print(f"  Time:       {elapsed:.1f}s")
    print(f"  Model:      {MODEL_VERSION}")


if __name__ == "__main__":
    main()
