"""
Signal type definitions and batch write utilities for DistressScannerApp.

Registers new signal types (vegetation_overgrowth, flood_risk, structural_change)
into the shared signal_types table, and provides batch writing to parcel_signals.
"""

from datetime import date

from psycopg2.extras import RealDictCursor
import structlog

from src.db import (
    ensure_county,
    sync_parcels_from_gis,
    get_signal_type_id,
    batch_get_parcel_uuids,
    write_signal,
)

logger = structlog.get_logger("signals")

# Signal type definitions for distress scanner
SIGNAL_TYPES = {
    "vegetation_overgrowth": {
        "name": "Vegetation Overgrowth",
        "description": "Satellite NDVI analysis shows significant vegetation overgrowth indicating neglect",
        "base_weight": 2.0,
        "decay_type": "linear",
        "decay_days": 365,
    },
    "flood_risk": {
        "name": "Flood Zone Risk",
        "description": "Property located in FEMA high-risk flood zone (A, AE, V, VE)",
        "base_weight": 1.5,
        "decay_type": "none",
        "decay_days": None,
    },
    "structural_change": {
        "name": "Structural Change Detected",
        "description": "Satellite imagery comparison shows significant structural change on property",
        "base_weight": 2.5,
        "decay_type": "linear",
        "decay_days": 730,
    },
    "vegetation_neglect": {
        "name": "Vegetation Neglect",
        "description": "NDVI 0.10-0.30 indicates bare/abandoned lot with minimal vegetation",
        "base_weight": 1.5,
        "decay_type": "linear",
        "decay_days": 365,
    },
}


def register_signal_types(conn) -> dict[str, str]:
    """
    Insert signal types into signal_types table if not already present.
    Uses ON CONFLICT (code) DO NOTHING for idempotency.
    Returns {code: uuid_str} mapping.
    """
    result = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for code, config in SIGNAL_TYPES.items():
            cur.execute("""
                INSERT INTO signal_types (code, name, description, base_weight, decay_type, decay_days)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (code) DO NOTHING
            """, (
                code,
                config["name"],
                config["description"],
                config["base_weight"],
                config["decay_type"],
                config["decay_days"],
            ))

            # Fetch the ID (whether just inserted or already existed)
            cur.execute("SELECT id FROM signal_types WHERE code = %s", (code,))
            row = cur.fetchone()
            if row:
                result[code] = str(row["id"])
                logger.info("signal_type_registered", code=code, id=result[code])

    conn.commit()
    return result


def write_scan_results(conn, county_name: str, state_code: str,
                       results: list[dict]) -> tuple[int, int]:
    """
    Write a batch of scan results to the database.

    Each result dict should contain:
        parcel_id: str
        signal_code: str (e.g. "vegetation_overgrowth")
        signal_date: date
        confidence: float (0.0 - 1.0)
        evidence: dict (JSONB payload)

    Returns (success_count, failure_count).
    """
    if not results:
        return (0, 0)

    # 1. Ensure county exists
    county_id = ensure_county(conn, county_name, state_code)

    # 2. Collect unique parcel_ids
    parcel_ids = list({r["parcel_id"] for r in results})

    # 3. Sync parcels from GIS
    synced = sync_parcels_from_gis(conn, county_id, county_name, parcel_ids)
    logger.info("parcels_synced_for_write", count=synced)

    # 4. Get parcel UUIDs
    parcel_uuid_map = batch_get_parcel_uuids(conn, county_id, parcel_ids)

    # 5. Cache signal type IDs
    signal_type_cache = {}

    success = 0
    failure = 0

    for result in results:
        try:
            parcel_id = result["parcel_id"]
            signal_code = result["signal_code"]

            # Resolve parcel UUID
            parcel_uuid = parcel_uuid_map.get(parcel_id)
            if not parcel_uuid:
                logger.warning("parcel_uuid_not_found", parcel_id=parcel_id)
                failure += 1
                continue

            # Resolve signal type ID
            if signal_code not in signal_type_cache:
                signal_type_cache[signal_code] = get_signal_type_id(conn, signal_code)
            signal_type_id = signal_type_cache[signal_code]

            if not signal_type_id:
                logger.warning("signal_type_not_found", code=signal_code)
                failure += 1
                continue

            # Write signal
            written = write_signal(
                conn,
                parcel_uuid=parcel_uuid,
                signal_type_id=signal_type_id,
                signal_date=result.get("signal_date", date.today()),
                confidence=result["confidence"],
                evidence=result["evidence"],
            )

            if written:
                success += 1
            else:
                failure += 1

        except Exception as e:
            logger.error("write_signal_failed", parcel_id=result.get("parcel_id"), error=str(e))
            failure += 1

    conn.commit()
    logger.info("scan_results_written", success=success, failure=failure)
    return (success, failure)
