"""
Database connection and signal write helpers for DistressScannerApp.

Connects to the shared Railway PostgreSQL instance used by Motivated Curator.
Reads parcel data, writes distress signals.
"""

import json
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import structlog

load_dotenv()

logger = structlog.get_logger("db")


def get_db_connection():
    """Create psycopg2 connection from DATABASE_URL env var."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return psycopg2.connect(database_url)


def ensure_county(conn, name: str, state_code: str) -> str:
    """Get or create county, return UUID."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO counties (name, state_code)
            VALUES (%s, %s)
            ON CONFLICT (name, state_code) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """, (name, state_code))
        conn.commit()
        return str(cur.fetchone()["id"])


def sync_parcels_from_gis(conn, county_id: str, county_name: str, parcel_ids: list[str]) -> int:
    """Ensure parcels exist in parcels table by syncing from gis_parcels_core."""
    if not parcel_ids:
        return 0

    synced = 0
    chunk_size = 5000
    for i in range(0, len(parcel_ids), chunk_size):
        chunk = parcel_ids[i:i + chunk_size]
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO parcels (county_id, parcel_id, owner_name, address_full)
                SELECT %s::uuid, gpc.parcel_id, gpc.owner_name, gpc.situs_address
                FROM gis_parcels_core gpc
                WHERE gpc.county = %s AND gpc.parcel_id = ANY(%s)
                ON CONFLICT (county_id, parcel_id) DO UPDATE SET
                    owner_name = COALESCE(EXCLUDED.owner_name, parcels.owner_name),
                    address_full = COALESCE(EXCLUDED.address_full, parcels.address_full)
            """, (county_id, county_name, chunk))
            synced += cur.rowcount
        conn.commit()

    logger.info("parcels_synced", county=county_name, count=synced)
    return synced


def get_signal_type_id(conn, code: str) -> str | None:
    """Get signal_type UUID by code."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id FROM signal_types WHERE code = %s", (code,))
        row = cur.fetchone()
        return str(row["id"]) if row else None


def batch_get_parcel_uuids(conn, county_id: str, parcel_ids: list[str]) -> dict[str, str]:
    """Batch-fetch parcel UUIDs. Returns {parcel_id: uuid}."""
    if not parcel_ids:
        return {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT parcel_id, id FROM parcels
            WHERE county_id = %s::uuid AND parcel_id = ANY(%s)
        """, (county_id, parcel_ids))
        return {row["parcel_id"]: str(row["id"]) for row in cur.fetchall()}


def write_signal(conn, parcel_uuid: str, signal_type_id: str,
                 signal_date, confidence: float, evidence: dict) -> bool:
    """Deactivate old signal for this parcel/type, then insert new one."""
    with conn.cursor() as cur:
        # Deactivate existing active signal
        cur.execute("""
            UPDATE parcel_signals SET is_active = FALSE
            WHERE parcel_id = %s::uuid AND signal_type_id = %s::uuid AND is_active = TRUE
        """, (parcel_uuid, signal_type_id))

        # Insert new signal
        cur.execute("""
            INSERT INTO parcel_signals (parcel_id, signal_type_id, signal_date, confidence, evidence, is_active)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, TRUE)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (parcel_uuid, signal_type_id, signal_date, confidence, json.dumps(evidence)))

        result = cur.fetchone()
        return result is not None


def backfill_coordinates_from_geometry(conn):
    """
    Auto-detect parcels missing lat/lng and backfill from parcels.geometry via PostGIS.

    Joins gis_parcels_core → counties → parcels on (county, state_code, parcel_id).
    Only runs if PostGIS is available and there are parcels to backfill.
    Idempotent — only updates rows where latitude IS NULL.
    """
    try:
        with conn.cursor() as cur:
            # Check PostGIS availability
            cur.execute("SELECT PostGIS_Version()")
            cur.fetchone()

            # Count missing coordinates that are backfillable
            cur.execute("""
                SELECT COUNT(*)
                FROM gis_parcels_core g
                JOIN counties c ON lower(c.name) = lower(g.county) AND c.state_code = g.state_code
                JOIN parcels p ON p.county_id = c.id AND p.parcel_id = g.parcel_id
                WHERE g.latitude IS NULL AND p.geometry IS NOT NULL
            """)
            missing = cur.fetchone()[0]

            if missing == 0:
                return 0

            cur.execute("""
                UPDATE gis_parcels_core g
                SET latitude = ST_Y(p.geometry::geometry),
                    longitude = ST_X(p.geometry::geometry)
                FROM parcels p
                JOIN counties c ON p.county_id = c.id
                WHERE lower(c.name) = lower(g.county)
                  AND c.state_code = g.state_code
                  AND p.parcel_id = g.parcel_id
                  AND g.latitude IS NULL
                  AND p.geometry IS NOT NULL
            """)
            updated = cur.rowcount
        conn.commit()
        if updated > 0:
            logger.info("coordinates_backfilled", count=updated)
        return updated
    except Exception as e:
        conn.rollback()
        logger.debug("coordinate_backfill_skipped", reason=str(e))
        return 0


def migrate_add_scan_columns(conn):
    """
    Idempotent migration: add scan result columns to gis_parcels_core.

    Uses IF NOT EXISTS / safe column-add pattern so it can run multiple times.
    """
    columns = [
        ("ndvi_score", "REAL"),
        ("ndvi_date", "TEXT"),
        ("ndvi_category", "TEXT"),
        ("fema_zone", "TEXT"),
        ("fema_risk", "TEXT"),
        ("fema_sfha", "BOOLEAN"),
        ("distress_score", "REAL"),
        ("distress_flags", "TEXT"),
        ("flag_veg", "BOOLEAN DEFAULT FALSE"),
        ("flag_flood", "BOOLEAN DEFAULT FALSE"),
        ("flag_structural", "BOOLEAN DEFAULT FALSE"),
        ("flag_neglect", "BOOLEAN DEFAULT FALSE"),
        ("veg_confidence", "REAL"),
        ("flood_confidence", "REAL"),
        ("scan_date", "TIMESTAMP"),
        ("scan_pass", "SMALLINT"),
        ("sentinel_worthy", "BOOLEAN DEFAULT FALSE"),
        ("planet_scan_date", "TIMESTAMP"),
    ]

    with conn.cursor() as cur:
        for col_name, col_type in columns:
            # Split off DEFAULT clause for the ADD COLUMN statement
            cur.execute(f"""
                DO $$ BEGIN
                    ALTER TABLE gis_parcels_core ADD COLUMN {col_name} {col_type};
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)

        # Create indexes for common filter queries
        indexes = [
            ("idx_gpc_ndvi_score", "ndvi_score"),
            ("idx_gpc_distress_score", "distress_score"),
            ("idx_gpc_fema_zone", "fema_zone"),
            ("idx_gpc_flag_veg", "flag_veg"),
            ("idx_gpc_flag_flood", "flag_flood"),
            ("idx_gpc_flag_neglect", "flag_neglect"),
            ("idx_gpc_scan_date", "scan_date"),
        ]
        for idx_name, col in indexes:
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS {idx_name}
                ON gis_parcels_core ({col});
            """)

    conn.commit()
    logger.info("migration_complete", table="gis_parcels_core", columns_added=len(columns))


def batch_update_scan_results(conn, results: list[dict]) -> int:
    """
    Bulk UPDATE scan results into gis_parcels_core.

    Each result dict should have:
        parcel_id, county, ndvi_score, ndvi_date, ndvi_category,
        fema_zone, fema_risk, fema_sfha, distress_score, distress_flags,
        flag_veg, flag_flood, flag_structural, flag_neglect,
        veg_confidence, flood_confidence, scan_date, scan_pass, sentinel_worthy

    Commits in 500-row chunks. Returns total updated count.
    """
    if not results:
        return 0

    update_sql = """
        UPDATE gis_parcels_core SET
            ndvi_score = %(ndvi_score)s,
            ndvi_date = %(ndvi_date)s,
            ndvi_category = %(ndvi_category)s,
            fema_zone = %(fema_zone)s,
            fema_risk = %(fema_risk)s,
            fema_sfha = %(fema_sfha)s,
            distress_score = %(distress_score)s,
            distress_flags = %(distress_flags)s,
            flag_veg = %(flag_veg)s,
            flag_flood = %(flag_flood)s,
            flag_structural = %(flag_structural)s,
            flag_neglect = %(flag_neglect)s,
            veg_confidence = %(veg_confidence)s,
            flood_confidence = %(flood_confidence)s,
            scan_date = %(scan_date)s,
            scan_pass = %(scan_pass)s,
            sentinel_worthy = %(sentinel_worthy)s
        WHERE parcel_id = %(parcel_id)s AND county = %(county)s
    """

    from psycopg2.extras import execute_batch

    chunk_size = 500
    for i in range(0, len(results), chunk_size):
        chunk = results[i:i + chunk_size]
        with conn.cursor() as cur:
            execute_batch(cur, update_sql, chunk, page_size=100)
        conn.commit()

    logger.info("batch_update_complete", total_submitted=len(results))
    return len(results)


def migrate_add_composite_columns(conn):
    """
    Idempotent migration: add NDVI slope + distress composite columns to gis_parcels_core.

    Phase 1 of Distress Fusion Engine:
    - ndvi_slope_5yr: NDVI change per year from linear regression on NAIP history
    - ndvi_slope_pctile: percentile rank within county (0-100)
    - ndvi_history_count: number of historical NAIP vintages used
    - ndvi_history_years: comma-separated years used (e.g. "2014,2016,2018,2020,2024")
    - distress_composite: weighted bulk score (0-10)
    - composite_date: when composite was last computed
    """
    columns = [
        ("ndvi_slope_5yr", "REAL"),
        ("ndvi_slope_pctile", "REAL"),
        ("ndvi_history_count", "SMALLINT"),
        ("ndvi_history_years", "TEXT"),
        ("distress_composite", "REAL"),
        ("composite_date", "TIMESTAMP"),
    ]

    with conn.cursor() as cur:
        for col_name, col_type in columns:
            cur.execute(f"""
                DO $$ BEGIN
                    ALTER TABLE gis_parcels_core ADD COLUMN {col_name} {col_type};
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)

        # Indexes for composite scoring queries
        for idx_name, col in [
            ("idx_gpc_ndvi_slope", "ndvi_slope_5yr"),
            ("idx_gpc_distress_composite", "distress_composite"),
            ("idx_gpc_ndvi_slope_pctile", "ndvi_slope_pctile"),
        ]:
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS {idx_name}
                ON gis_parcels_core ({col});
            """)

        # Partial index for parcels needing slope computation
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_gpc_slope_pending
            ON gis_parcels_core (parcel_id)
            WHERE ndvi_score IS NOT NULL AND ndvi_slope_5yr IS NULL;
        """)

    conn.commit()
    logger.info("composite_migration_complete", table="gis_parcels_core",
                columns_added=len(columns))


def batch_update_slope_results(conn, results: list[dict]) -> int:
    """
    Bulk UPDATE NDVI slope + history into gis_parcels_core.

    Each result dict should have:
        parcel_id, county, ndvi_slope_5yr, ndvi_history_count, ndvi_history_years
    """
    if not results:
        return 0

    update_sql = """
        UPDATE gis_parcels_core SET
            ndvi_slope_5yr = %(ndvi_slope_5yr)s,
            ndvi_history_count = %(ndvi_history_count)s,
            ndvi_history_years = %(ndvi_history_years)s
        WHERE parcel_id = %(parcel_id)s AND county = %(county)s
    """

    from psycopg2.extras import execute_batch

    chunk_size = 500
    for i in range(0, len(results), chunk_size):
        chunk = results[i:i + chunk_size]
        with conn.cursor() as cur:
            execute_batch(cur, update_sql, chunk, page_size=100)
        conn.commit()

    logger.info("slope_batch_update_complete", total_submitted=len(results))
    return len(results)


def compute_composite_scores(conn, county: str, ndvi_weight: float = 0.70,
                             fema_weight: float = 0.30) -> int:
    """
    Compute distress_composite for all parcels in a county using SQL.

    Two-step:
    1. Compute ndvi_slope_pctile using PERCENT_RANK() within county
    2. Compute distress_composite = ndvi_weight × slope_pctile_score + fema_weight × fema_score

    FEMA scoring:
      high risk (SFHA) = 10
      moderate risk = 6
      low risk = 2
      no data = 0

    NDVI slope scoring:
      percentile × 10 (top percentile = worst overgrowth = 10)

    Returns count of updated parcels.
    """
    with conn.cursor() as cur:
        # Step 1: Compute percentile ranks for NDVI slope within county
        # Higher slope = more overgrowth = higher percentile
        cur.execute("""
            WITH ranked AS (
                SELECT parcel_id,
                       PERCENT_RANK() OVER (
                           PARTITION BY county
                           ORDER BY ndvi_slope_5yr ASC NULLS FIRST
                       ) * 100 AS pctile
                FROM gis_parcels_core
                WHERE county = %s AND ndvi_slope_5yr IS NOT NULL
            )
            UPDATE gis_parcels_core g
            SET ndvi_slope_pctile = r.pctile
            FROM ranked r
            WHERE g.parcel_id = r.parcel_id AND g.county = %s
        """, (county, county))
        pctile_count = cur.rowcount

        # Step 2: Compute composite score
        cur.execute("""
            UPDATE gis_parcels_core
            SET distress_composite = ROUND(CAST(
                %s * COALESCE(ndvi_slope_pctile / 10.0, 0) +
                %s * CASE
                    WHEN fema_sfha = TRUE THEN 10.0
                    WHEN fema_risk = 'high' THEN 10.0
                    WHEN fema_risk = 'moderate' THEN 6.0
                    WHEN fema_risk = 'low' THEN 2.0
                    ELSE 0.0
                END
            AS NUMERIC), 2),
                composite_date = NOW()
            WHERE county = %s
              AND (ndvi_slope_5yr IS NOT NULL OR fema_zone IS NOT NULL)
        """, (ndvi_weight, fema_weight, county))
        composite_count = cur.rowcount

    conn.commit()
    logger.info("composite_scores_computed",
                county=county,
                percentiles_set=pctile_count,
                composites_set=composite_count)
    return composite_count


def get_parcels_needing_slope(conn, county: str, state: str = None,
                               limit: int = None) -> list[dict]:
    """
    Get parcels that have NDVI score (from Pass 1) but no slope yet.

    These already went through batch_ndvi_scan and need historical NDVI
    for slope computation.
    """
    query = """
        SELECT parcel_id, latitude, longitude, county, state_code,
               ndvi_score, ndvi_date
        FROM gis_parcels_core
        WHERE county = %s
            AND ndvi_score IS NOT NULL
            AND ndvi_slope_5yr IS NULL
            AND latitude IS NOT NULL AND longitude IS NOT NULL
    """
    params = [county]

    if state:
        query += " AND state_code = %s"
        params.append(state)

    # Deterministic shuffle for geographic diversity without full-table sort
    query += " ORDER BY md5(parcel_id)"

    if limit:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def migrate_add_planet_columns(conn):
    """
    Idempotent migration: add Planet refinement columns to gis_parcels_core.

    Stores Planet scene data, change score, and thumbnail URLs from planet_refine().
    """
    columns = [
        ("planet_scene_count", "SMALLINT"),
        ("planet_change_score", "REAL"),
        ("planet_temporal_span", "SMALLINT"),
        ("planet_latest_date", "TEXT"),
        ("planet_earliest_date", "TEXT"),
        ("planet_thumb_latest_url", "TEXT"),
        ("planet_thumb_earliest_url", "TEXT"),
    ]

    with conn.cursor() as cur:
        for col_name, col_type in columns:
            cur.execute(f"""
                DO $$ BEGIN
                    ALTER TABLE gis_parcels_core ADD COLUMN {col_name} {col_type};
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)

    conn.commit()
    logger.info("planet_migration_complete", table="gis_parcels_core",
                columns_added=len(columns))


def update_parcel_planet(conn, parcel_id: str, county: str, planet_data: dict) -> None:
    """Persist Planet refinement results to gis_parcels_core."""
    # Extract fields from planet_refine() result dict
    scene_count = planet_data.get("scene_count")
    change_score = planet_data.get("change_score")
    temporal_span = planet_data.get("temporal_span_days")
    thumb_latest = planet_data.get("thumbnail_latest_url")
    thumb_earliest = planet_data.get("thumbnail_earliest_url")

    # Parse dates from scene metadata
    latest_date = None
    earliest_date = None
    dr = planet_data.get("date_range", {})
    if dr.get("latest"):
        latest_date = str(dr["latest"])[:10]
    if dr.get("earliest"):
        earliest_date = str(dr["earliest"])[:10]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE gis_parcels_core SET
                planet_scan_date = NOW(),
                planet_scene_count = %s,
                planet_change_score = %s,
                planet_temporal_span = %s,
                planet_latest_date = %s,
                planet_earliest_date = %s,
                planet_thumb_latest_url = %s,
                planet_thumb_earliest_url = %s
            WHERE parcel_id = %s AND county = %s
        """, (
            scene_count, change_score, temporal_span,
            latest_date, earliest_date,
            thumb_latest, thumb_earliest,
            parcel_id, county,
        ))
    conn.commit()


def get_planet_scan_date(conn, parcel_id: str, county: str) -> datetime | None:
    """Get the last planet_scan_date for a parcel. Returns None if never scanned."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT planet_scan_date FROM gis_parcels_core
            WHERE parcel_id = %s AND county = %s
        """, (parcel_id, county))
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def set_planet_scan_date(conn, parcel_id: str, county: str):
    """Stamp planet_scan_date = NOW() for a parcel after Planet scan."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE gis_parcels_core SET planet_scan_date = NOW()
            WHERE parcel_id = %s AND county = %s
        """, (parcel_id, county))
    conn.commit()


def get_unscanned_parcels(conn, county: str, state: str = None,
                          limit: int = None,
                          property_class: str = None) -> list[dict]:
    """
    Get parcels from gis_parcels_core that haven't been scanned yet.

    Filters on scan_date IS NULL for resumability.
    Returns list of dicts with parcel_id, latitude, longitude, county.
    """
    query = """
        SELECT parcel_id, latitude, longitude, county, state_code
        FROM gis_parcels_core
        WHERE county = %s
            AND latitude IS NOT NULL AND longitude IS NOT NULL
            AND scan_date IS NULL
    """
    params = [county]

    if state:
        query += " AND state_code = %s"
        params.append(state)

    if property_class:
        query += " AND property_class = %s"
        params.append(property_class)

    query += " ORDER BY md5(parcel_id)"

    if limit:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def get_parcels_with_coords(conn, county_name: str, state_code: str = None,
                            limit: int = None, offset: int = 0,
                            mailing_zip: str = None,
                            property_class: str = None,
                            min_value: float = None, max_value: float = None,
                            min_sqft: float = None, max_sqft: float = None) -> list[dict]:
    """Get parcels with lat/lng from gis_parcels_core with optional filters."""
    query = """
        SELECT parcel_id, latitude, longitude, owner_name, situs_address,
               total_value, property_class, sqft, mailing_zip
        FROM gis_parcels_core
        WHERE county = %s AND latitude IS NOT NULL AND longitude IS NOT NULL
    """
    params = [county_name]

    if state_code:
        query += " AND state_code = %s"
        params.append(state_code)

    if mailing_zip:
        query += " AND SUBSTRING(mailing_zip FROM 1 FOR 5) = %s"
        params.append(mailing_zip[:5])

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

    query += " ORDER BY parcel_id"

    if limit:
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def migrate_add_sentinel_columns(conn):
    """
    Idempotent migration: add Sentinel enrichment columns to gis_parcels_core.

    Stores trend data from Sentinel-2 (or Landsat fallback) enrichment pass.
    """
    columns = [
        ("sentinel_trend_direction", "TEXT"),
        ("sentinel_trend_slope", "REAL"),
        ("sentinel_latest_ndvi", "REAL"),
        ("sentinel_months_data", "SMALLINT"),
        ("sentinel_mean_ndvi", "REAL"),
        ("sentinel_data_source", "TEXT"),
        ("sentinel_chart_url", "TEXT"),
        ("sentinel_scan_date", "TIMESTAMP"),
    ]

    with conn.cursor() as cur:
        for col_name, col_type in columns:
            cur.execute(f"""
                DO $$ BEGIN
                    ALTER TABLE gis_parcels_core ADD COLUMN {col_name} {col_type};
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_gpc_sentinel_scan_date
            ON gis_parcels_core (sentinel_scan_date);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_gpc_sentinel_trend
            ON gis_parcels_core (sentinel_trend_direction);
        """)
        # Partial index for pending enrichment queue
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_gpc_sentinel_pending
            ON gis_parcels_core (distress_score DESC NULLS LAST)
            WHERE sentinel_worthy = TRUE AND sentinel_scan_date IS NULL;
        """)

    conn.commit()
    logger.info("sentinel_migration_complete", table="gis_parcels_core",
                columns_added=len(columns))


def get_sentinel_worthy_parcels(conn, county: str, state: str = None,
                                 limit: int = None) -> list[dict]:
    """
    Get parcels marked sentinel_worthy that haven't been Sentinel-enriched yet.

    Ordered by distress_score DESC (highest distress first).
    Returns parcel_id, lat, lng, county, plus existing NAIP/FEMA data for rescoring.
    """
    query = """
        SELECT parcel_id, latitude, longitude, county, state_code,
               ndvi_score, fema_zone, fema_risk, fema_sfha, distress_score
        FROM gis_parcels_core
        WHERE county = %s
            AND sentinel_worthy = TRUE
            AND sentinel_scan_date IS NULL
            AND latitude IS NOT NULL AND longitude IS NOT NULL
    """
    params = [county]

    if state:
        query += " AND state_code = %s"
        params.append(state)

    query += " ORDER BY distress_score DESC NULLS LAST"

    if limit:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def batch_update_sentinel_results(conn, results: list[dict]) -> int:
    """
    Bulk UPDATE Sentinel enrichment results + re-scored flags into gis_parcels_core.

    Uses monotonic scan_pass: GREATEST(COALESCE(scan_pass,0), new_pass)
    to avoid downgrading parcels that already have a higher pass level.

    Commits in 500-row chunks. Returns total updated count.
    """
    if not results:
        return 0

    update_sql = """
        UPDATE gis_parcels_core SET
            sentinel_trend_direction = %(sentinel_trend_direction)s,
            sentinel_trend_slope = %(sentinel_trend_slope)s,
            sentinel_latest_ndvi = %(sentinel_latest_ndvi)s,
            sentinel_months_data = %(sentinel_months_data)s,
            sentinel_mean_ndvi = %(sentinel_mean_ndvi)s,
            sentinel_data_source = %(sentinel_data_source)s,
            sentinel_chart_url = %(sentinel_chart_url)s,
            sentinel_scan_date = %(sentinel_scan_date)s,
            distress_score = %(distress_score)s,
            distress_flags = %(distress_flags)s,
            flag_veg = %(flag_veg)s,
            flag_flood = %(flag_flood)s,
            flag_structural = %(flag_structural)s,
            flag_neglect = %(flag_neglect)s,
            veg_confidence = %(veg_confidence)s,
            flood_confidence = %(flood_confidence)s,
            scan_pass = GREATEST(COALESCE(scan_pass, 0), %(scan_pass)s)
        WHERE parcel_id = %(parcel_id)s AND county = %(county)s
    """

    from psycopg2.extras import execute_batch

    chunk_size = 500
    for i in range(0, len(results), chunk_size):
        chunk = results[i:i + chunk_size]
        with conn.cursor() as cur:
            execute_batch(cur, update_sql, chunk, page_size=100)
        conn.commit()

    logger.info("sentinel_batch_update_complete", total_submitted=len(results))
    return len(results)


def migrate_add_usps_columns(conn):
    """
    Idempotent migration: add USPS vacancy columns to gis_parcels_core
    and ensure the shared usps_vacancy_checks audit table exists.
    """
    columns = [
        ("usps_vacant", "BOOLEAN"),
        ("usps_dpv_confirmed", "BOOLEAN"),
        ("usps_address", "TEXT"),
        ("usps_city", "TEXT"),
        ("usps_zip", "TEXT"),
        ("usps_zip4", "TEXT"),
        ("usps_business", "BOOLEAN"),
        ("usps_carrier_route", "TEXT"),
        ("usps_address_mismatch", "BOOLEAN"),
        ("usps_check_date", "TIMESTAMP"),
        ("usps_error", "TEXT"),
        ("flag_vacancy", "BOOLEAN DEFAULT FALSE"),
        ("vacancy_confidence", "REAL"),
    ]

    with conn.cursor() as cur:
        # Check existing columns first (lightweight, no table locks)
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'gis_parcels_core'
        """)
        existing = {row[0] for row in cur.fetchall()}
        missing = [(c, t) for c, t in columns if c not in existing]

        if not missing:
            logger.info("usps_migration_complete", columns_added=0,
                        table="gis_parcels_core")
        else:
            for col_name, col_type in missing:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE gis_parcels_core ADD COLUMN {col_name} {col_type};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                """)

        # Shared audit table — matches motivation-curator's schema exactly.
        # CREATE TABLE IF NOT EXISTS is a no-op if it already exists.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS usps_vacancy_checks (
                id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                parcel_id       UUID NOT NULL REFERENCES parcels(id),
                checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                input_address   TEXT NOT NULL,
                input_state     TEXT,
                usps_address    TEXT,
                usps_city       TEXT,
                usps_state      TEXT,
                usps_zip        TEXT,
                usps_zip4       TEXT,
                vacant          BOOLEAN,
                dpv_confirmed   BOOLEAN,
                business        BOOLEAN,
                address_mismatch BOOLEAN DEFAULT false,
                carrier_route   TEXT,
                account         SMALLINT,
                error           TEXT,
                raw_response    JSONB
            );
        """)

        # Indexes on gis_parcels_core
        for idx_name, col in [
            ("idx_gpc_usps_vacant", "usps_vacant"),
            ("idx_gpc_flag_vacancy", "flag_vacancy"),
            ("idx_gpc_usps_check_date", "usps_check_date"),
        ]:
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS {idx_name}
                ON gis_parcels_core ({col});
            """)

        # Indexes on usps_vacancy_checks (no-op if they exist from MC)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_usps_vacancy_checks_parcel_date
            ON usps_vacancy_checks (parcel_id, checked_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_usps_vacancy_checks_vacant
            ON usps_vacancy_checks (vacant) WHERE vacant = true;
        """)

    conn.commit()
    if missing:
        logger.info("usps_migration_complete", table="gis_parcels_core",
                    columns_added=len(missing))


def get_usps_cache(conn, parcel_id: str, county: str, cache_days: int = 60) -> dict | None:
    """
    Check if this parcel has a recent USPS vacancy check.

    Uses gis_parcels_core.usps_check_date directly — no dependency on
    the shared usps_vacancy_checks audit table or parcels/counties UUIDs.
    This keeps distress-scanner independently runnable.

    Returns cached result dict or None if no recent check.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT usps_vacant, usps_dpv_confirmed, usps_address, usps_city,
                   usps_zip, usps_zip4, usps_business, usps_carrier_route,
                   usps_address_mismatch, usps_check_date, usps_error
            FROM gis_parcels_core
            WHERE parcel_id = %s AND county = %s
              AND usps_check_date IS NOT NULL
              AND usps_check_date > NOW() - make_interval(days => %s)
        """, (parcel_id, county, cache_days))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "vacant": row["usps_vacant"],
            "dpv_confirmed": row["usps_dpv_confirmed"],
            "usps_address": row["usps_address"],
            "usps_city": row["usps_city"],
            "usps_zip": row["usps_zip"],
            "usps_zip4": row["usps_zip4"],
            "business": row["usps_business"],
            "carrier_route": row["usps_carrier_route"],
            "address_mismatch": row["usps_address_mismatch"],
            "checked_at": row["usps_check_date"],
            "error": row["usps_error"],
        }


def save_usps_check(conn, parcel_id: str, county: str, state: str,
                    result, account: int) -> None:
    """
    Log a USPS vacancy check to the shared usps_vacancy_checks audit table.

    Resolves parcel UUID internally. If the parcels table isn't available
    (standalone mode), logs to gis_parcels_core only via update_parcel_usps().
    """
    from psycopg2.extras import Json

    # Try to resolve UUID and write to shared audit table
    try:
        county_id = ensure_county(conn, county, state)
        sync_parcels_from_gis(conn, county_id, county, [parcel_id])
        uuid_map = batch_get_parcel_uuids(conn, county_id, [parcel_id])
        parcel_uuid = uuid_map.get(parcel_id)

        if parcel_uuid:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usps_vacancy_checks (
                        parcel_id, input_address, input_state,
                        usps_address, usps_city, usps_state, usps_zip, usps_zip4,
                        vacant, dpv_confirmed, business, address_mismatch,
                        carrier_route, account, error, raw_response
                    ) VALUES (
                        %s::uuid, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """, (
                    parcel_uuid,
                    result.street_address,
                    result.state,
                    result.usps_address,
                    result.usps_city,
                    result.usps_state,
                    result.usps_zip,
                    result.usps_zip4,
                    result.vacant,
                    result.dpv_confirmed,
                    result.business,
                    result.address_mismatch,
                    result.carrier_route,
                    account,
                    result.error,
                    Json(result.raw_response) if result.raw_response else None,
                ))
            conn.commit()
    except Exception as e:
        # Shared audit table not available — standalone mode, skip it
        logger.debug("usps_audit_skip", error=str(e))


def update_parcel_usps(conn, parcel_id: str, county: str, result,
                       flag_vacancy: bool, vacancy_confidence: float | None) -> None:
    """Update gis_parcels_core with USPS vacancy data and computed flag."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE gis_parcels_core SET
                usps_vacant = %s,
                usps_dpv_confirmed = %s,
                usps_address = %s,
                usps_city = %s,
                usps_zip = %s,
                usps_zip4 = %s,
                usps_business = %s,
                usps_carrier_route = %s,
                usps_address_mismatch = %s,
                usps_check_date = NOW(),
                usps_error = %s,
                flag_vacancy = %s,
                vacancy_confidence = %s
            WHERE parcel_id = %s AND county = %s
        """, (
            result.vacant,
            result.dpv_confirmed,
            result.usps_address,
            result.usps_city,
            result.usps_zip,
            result.usps_zip4,
            result.business,
            result.carrier_route,
            result.address_mismatch,
            result.error,
            flag_vacancy,
            vacancy_confidence,
            parcel_id,
            county,
        ))
    conn.commit()


def get_parcels_needing_usps(conn, county: str, state: str = None,
                              limit: int = 500, min_composite: float = 7.0,
                              cache_days: int = 60,
                              property_class: str = None) -> list[dict]:
    """
    Get top leads by composite score that haven't been successfully USPS-checked recently.

    Includes parcels where:
    - usps_check_date is NULL (never checked)
    - usps_check_date is older than cache_days (stale)
    - usps_error IS NOT NULL (transient failure, eligible for retry regardless of date)

    Returns list of dicts sorted by distress_composite DESC.
    """
    query = """
        SELECT parcel_id, situs_address, latitude, longitude, county, state_code,
               distress_composite, ndvi_slope_5yr, fema_zone,
               mailing_city, mailing_zip, mailing_state
        FROM gis_parcels_core
        WHERE county = %s
          AND situs_address IS NOT NULL
          AND latitude IS NOT NULL AND longitude IS NOT NULL
          AND distress_composite >= %s
          AND (
              usps_check_date IS NULL
              OR usps_check_date < NOW() - make_interval(days => %s)
              OR usps_error IS NOT NULL
          )
    """
    params = [county, min_composite, cache_days]

    if state:
        query += " AND state_code = %s"
        params.append(state)

    if property_class:
        query += " AND property_class = %s"
        params.append(property_class)

    query += " ORDER BY distress_composite DESC NULLS LAST"

    if limit:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


# Transient USPS errors that should NOT cache (eligible for retry)
_USPS_TRANSIENT_ERRORS = {"rate_limited", "http_500", "http_502", "http_503", "http_504"}


def batch_update_usps_results(conn, results: list[dict]) -> int:
    """
    Batch-write USPS vacancy results to gis_parcels_core.

    Respects error/success distinction:
    - Successful checks (no error): set usps_check_date = NOW()
    - Transient errors: do NOT set usps_check_date (eligible for retry)
    - Permanent errors: set usps_check_date with error populated
    """
    from psycopg2.extras import execute_batch

    success_results = []
    transient_results = []
    permanent_results = []

    for r in results:
        err = r.get("usps_error")
        if not err:
            success_results.append(r)
        elif err in _USPS_TRANSIENT_ERRORS:
            transient_results.append(r)
        else:
            permanent_results.append(r)

    updated = 0

    with conn.cursor() as cur:
        # Successful checks — set check_date
        if success_results:
            execute_batch(cur, """
                UPDATE gis_parcels_core SET
                    usps_vacant = %(usps_vacant)s,
                    usps_dpv_confirmed = %(usps_dpv_confirmed)s,
                    usps_address = %(usps_address)s,
                    usps_city = %(usps_city)s,
                    usps_zip = %(usps_zip)s,
                    usps_zip4 = %(usps_zip4)s,
                    usps_business = %(usps_business)s,
                    usps_carrier_route = %(usps_carrier_route)s,
                    usps_address_mismatch = %(usps_address_mismatch)s,
                    usps_check_date = NOW(),
                    usps_error = NULL,
                    flag_vacancy = %(flag_vacancy)s,
                    vacancy_confidence = %(vacancy_confidence)s
                WHERE parcel_id = %(parcel_id)s AND county = %(county)s
            """, success_results, page_size=100)
            updated += len(success_results)

        # Transient errors — do NOT set check_date (retry next run)
        if transient_results:
            execute_batch(cur, """
                UPDATE gis_parcels_core SET
                    usps_error = %(usps_error)s,
                    flag_vacancy = FALSE,
                    vacancy_confidence = NULL
                WHERE parcel_id = %(parcel_id)s AND county = %(county)s
            """, transient_results, page_size=100)
            updated += len(transient_results)

        # Permanent errors — set check_date to avoid re-hitting known bad addresses
        if permanent_results:
            execute_batch(cur, """
                UPDATE gis_parcels_core SET
                    usps_error = %(usps_error)s,
                    usps_check_date = NOW(),
                    flag_vacancy = FALSE,
                    vacancy_confidence = NULL
                WHERE parcel_id = %(parcel_id)s AND county = %(county)s
            """, permanent_results, page_size=100)
            updated += len(permanent_results)

    conn.commit()
    logger.info("usps_batch_update", total=len(results),
                success=len(success_results),
                transient=len(transient_results),
                permanent=len(permanent_results))
    return updated


def migrate_add_conviction_columns(conn):
    """
    Idempotent migration: add conviction score columns to gis_parcels_core.
    Uses information_schema check to avoid ACCESS EXCLUSIVE lock when columns exist.
    """
    columns = [
        ("conviction_score", "REAL"),
        ("conviction_base_score", "REAL"),
        ("conviction_vacancy_bonus", "REAL"),
        ("conviction_mc_score", "REAL"),
        ("conviction_mc_signals", "INTEGER"),
        ("conviction_mc_codes", "TEXT"),
        ("conviction_components", "TEXT"),
        ("conviction_date", "TIMESTAMP"),
    ]

    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'gis_parcels_core'
        """)
        existing = {row[0] for row in cur.fetchall()}
        missing = [(c, t) for c, t in columns if c not in existing]

        if not missing:
            logger.info("conviction_migration_complete", columns_added=0,
                        table="gis_parcels_core")
        else:
            for col_name, col_type in missing:
                cur.execute(f"""
                    DO $$ BEGIN
                        ALTER TABLE gis_parcels_core ADD COLUMN {col_name} {col_type};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_gpc_conviction_score
            ON gis_parcels_core (conviction_score DESC NULLS LAST);
        """)

    conn.commit()
    if missing:
        logger.info("conviction_migration_complete", table="gis_parcels_core",
                    columns_added=len(missing))
