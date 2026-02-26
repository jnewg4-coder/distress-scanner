"""
Distress scan orchestrator.

Runs all data sources (NAIP, Sentinel-2, FEMA) for a location,
evaluates distress flags, computes an overall score, and optionally
writes signals to the shared database.
"""

import json
from datetime import datetime

import structlog

from src.naip.baseline import naip_baseline
from src.sentinel.trends import sentinel_trends
from src.landsat.client import landsat_trends
from src.fema.flood import fema_flood
from src.planet.client import planet_refine
from src.analysis.flags import generate_all_flags
from src.storage import upload_bytes, make_point_key

logger = structlog.get_logger("analysis.scanner")

# Shared signal weights (used by scan_free, scan_distress, rescore_with_sentinel)
SIGNAL_WEIGHTS = {
    "vegetation_overgrowth": 2.0,
    "vegetation_neglect": 1.5,
    "flood_risk": 1.5,
    "structural_change": 2.5,
    "usps_vacancy": 2.5,
}


def scan_free(lat: float, lng: float) -> dict:
    """
    Free-only scan: NAIP baseline + FEMA flood. No Sentinel (no CDSE budget).

    Use this for pass-1 bulk scanning. Sentinel enrichment runs later
    on flagged parcels only.
    """
    scan_start = datetime.now()

    result = {
        "lat": lat,
        "lng": lng,
        "scan_date": scan_start.strftime("%Y-%m-%d"),
        "scan_timestamp": scan_start.isoformat(),
        "naip": None,
        "sentinel": None,
        "fema": None,
        "flags": [],
        "distress_score": 0.0,
        "sentinel_worthy": False,
        "summary": "",
        "errors": [],
    }

    # --- NAIP Baseline (fast mode: skip historical + image export) ---
    try:
        result["naip"] = naip_baseline(lat, lng, skip_historical=True,
                                        skip_image_export=True)
    except Exception as e:
        result["errors"].append(f"naip: {e}")
        logger.error("scan_naip_failed", lat=lat, lng=lng, error=str(e))

    # --- FEMA Flood (skip map export for batch speed) ---
    try:
        result["fema"] = fema_flood(lat, lng, skip_map=True)
    except Exception as e:
        result["errors"].append(f"fema: {e}")
        logger.error("scan_fema_failed", lat=lat, lng=lng, error=str(e))

    # --- Evaluate Flags (NAIP + FEMA only, sentinel=None) ---
    result["flags"] = generate_all_flags(
        naip=result["naip"],
        sentinel=None,
        fema=result["fema"],
    )

    # --- Distress Score ---
    score = 0.0
    for flag in result["flags"]:
        weight = SIGNAL_WEIGHTS.get(flag["signal_code"], 1.0)
        score += weight * flag["confidence"]
    result["distress_score"] = round(min(score, 10.0), 2)

    # --- Check if worth sending to Sentinel later ---
    result["sentinel_worthy"] = is_sentinel_worthy(result)

    result["summary"] = _build_summary(result)

    elapsed = (datetime.now() - scan_start).total_seconds()
    logger.info("scan_free_complete",
                lat=lat, lng=lng,
                score=result["distress_score"],
                sentinel_worthy=result["sentinel_worthy"],
                flags=[f["signal_code"] for f in result["flags"]],
                elapsed_seconds=round(elapsed, 1))

    return result


def is_sentinel_worthy(result: dict) -> bool:
    """Determine if a parcel should get Sentinel-2 enrichment in pass 2."""
    naip = result.get("naip")
    fema = result.get("fema")

    if naip and not naip.get("errors"):
        ndvi = naip.get("current_ndvi")
        if ndvi is not None and ndvi > 0.50:
            return True
        if naip.get("overgrowth_flag"):
            return True
        change = naip.get("ndvi_change")
        if change is not None and change < -0.20:
            return True

    if fema:
        if fema.get("risk_level") in ("high", "moderate"):
            return True

    if result.get("flags"):
        return True

    return False


def scan_distress(lat: float, lng: float, months: int = 12,
                   skip_planet: bool = False) -> dict:
    """
    Full distress scan for a single point.

    Runs all 3 data sources in sequence (each with its own error handling),
    evaluates flags, computes distress score.

    Args:
        lat, lng: property coordinates
        months: Sentinel-2 lookback window (default 12)
        skip_planet: if True, skip Planet API call (e.g. already scanned recently)

    Returns comprehensive scan result dict.
    """
    scan_start = datetime.now()

    result = {
        "lat": lat,
        "lng": lng,
        "scan_date": scan_start.strftime("%Y-%m-%d"),
        "scan_timestamp": scan_start.isoformat(),
        "naip": None,
        "sentinel": None,
        "landsat": None,
        "fema": None,
        "planet": None,
        "flags": [],
        "distress_score": 0.0,
        "summary": "",
        "errors": [],
    }

    # --- Source 1: NAIP Baseline ---
    try:
        logger.info("scan_naip_start", lat=lat, lng=lng)
        result["naip"] = naip_baseline(lat, lng)
    except Exception as e:
        result["errors"].append(f"naip: {e}")
        logger.error("scan_naip_failed", lat=lat, lng=lng, error=str(e))

    # --- Source 2: Sentinel-2 Trends (fallback to Landsat) ---
    sentinel_ok = False
    try:
        logger.info("scan_sentinel_start", lat=lat, lng=lng)
        result["sentinel"] = sentinel_trends(lat, lng, months=months)
        if result["sentinel"] and result["sentinel"].get("months_with_data", 0) > 0:
            sentinel_ok = True
    except Exception as e:
        result["errors"].append(f"sentinel: {e}")
        logger.error("scan_sentinel_failed", lat=lat, lng=lng, error=str(e))

    # Landsat fallback if Sentinel failed or returned no data
    if not sentinel_ok:
        try:
            logger.info("scan_landsat_fallback", lat=lat, lng=lng)
            result["landsat"] = landsat_trends(lat, lng, months=months)
        except Exception as e:
            result["errors"].append(f"landsat: {e}")
            logger.error("scan_landsat_failed", lat=lat, lng=lng, error=str(e))

    # --- Source 3: FEMA Flood ---
    try:
        logger.info("scan_fema_start", lat=lat, lng=lng)
        result["fema"] = fema_flood(lat, lng)
    except Exception as e:
        result["errors"].append(f"fema: {e}")
        logger.error("scan_fema_failed", lat=lat, lng=lng, error=str(e))

    # --- Source 4: Planet (paid refinement — search only, conservative) ---
    if skip_planet:
        logger.info("scan_planet_skipped", reason="skip_planet flag")
    else:
        try:
            result["planet"] = planet_refine(lat, lng, months_back=months)
        except Exception as e:
            result["errors"].append(f"planet: {e}")
            logger.warning("scan_planet_failed", error=str(e))

    # --- Evaluate Flags ---
    # Use Sentinel if available, otherwise Landsat as trends source
    trends_source = result["sentinel"] if sentinel_ok else result.get("landsat")
    result["flags"] = generate_all_flags(
        naip=result["naip"],
        sentinel=trends_source,
        fema=result["fema"],
    )

    # --- Compute Distress Score ---
    score = 0.0
    for flag in result["flags"]:
        weight = SIGNAL_WEIGHTS.get(flag["signal_code"], 1.0)
        score += weight * flag["confidence"]
    result["distress_score"] = round(min(score, 10.0), 2)

    # --- Build Summary ---
    result["summary"] = _build_summary(result)

    # --- Upload scan result JSON to R2 ---
    try:
        key = make_point_key(lat, lng, "scan_result.json")
        # Make a serializable copy (strip large nested data for the JSON upload)
        upload_data = {
            "lat": lat,
            "lng": lng,
            "scan_date": result["scan_date"],
            "distress_score": result["distress_score"],
            "flags": result["flags"],
            "summary": result["summary"],
            "naip_ndvi": result["naip"].get("current_ndvi") if result["naip"] else None,
            "sentinel_trend": result["sentinel"].get("trend_direction") if result["sentinel"] else None,
            "landsat_trend": result["landsat"].get("trend_direction") if result.get("landsat") else None,
            "fema_zone": result["fema"].get("flood_zone") if result["fema"] else None,
            "fema_risk": result["fema"].get("risk_level") if result["fema"] else None,
            "planet_scenes": result["planet"].get("scene_count") if result.get("planet") else None,
            "planet_thumb": result["planet"].get("thumbnail_url") if result.get("planet") else None,
            "errors": result["errors"],
        }
        scan_json = json.dumps(upload_data, indent=2, default=str)
        url = upload_bytes(key, scan_json.encode(), content_type="application/json")
        result["scan_url"] = url
    except Exception as e:
        result["errors"].append(f"scan_upload: {e}")
        logger.warning("scan_upload_failed", error=str(e))

    elapsed = (datetime.now() - scan_start).total_seconds()
    logger.info("scan_complete",
                lat=lat, lng=lng,
                score=result["distress_score"],
                flags=[f["signal_code"] for f in result["flags"]],
                elapsed_seconds=round(elapsed, 1))

    return result


def _build_summary(result: dict) -> str:
    """Build human-readable scan summary."""
    parts = []

    # NAIP
    naip = result.get("naip")
    if naip and naip.get("current_ndvi") is not None:
        parts.append(f"NAIP NDVI: {naip['current_ndvi']:.2f} ({naip.get('vegetation_assessment', 'N/A')})")
        if naip.get("ndvi_change") is not None:
            parts.append(f"  Change from baseline: {naip['ndvi_change']:+.3f}")
    else:
        parts.append("NAIP: No data available")

    # Sentinel
    sentinel = result.get("sentinel")
    if sentinel and sentinel.get("months_with_data", 0) > 0:
        parts.append(f"Sentinel-2: {sentinel['months_with_data']} months of data, "
                     f"trend={sentinel.get('trend_direction', 'N/A')} "
                     f"(slope={sentinel.get('trend_slope', 'N/A')})")
    else:
        parts.append("Sentinel-2: No data available")

    # Landsat (fallback)
    landsat = result.get("landsat")
    if landsat and landsat.get("months_with_data", 0) > 0:
        parts.append(f"Landsat: {landsat['months_with_data']} months (30m backup), "
                     f"trend={landsat.get('trend_direction', 'N/A')}")

    # Planet
    planet = result.get("planet")
    if planet:
        if planet.get("status") == "ok" and planet.get("scene_count", 0) > 0:
            parts.append(f"Planet: {planet['scene_count']} scenes (3m), "
                         f"latest={planet.get('date_range', {}).get('latest', 'N/A')[:10]}")
        elif planet.get("status") == "upgrade_required":
            parts.append("Planet: Available (upgrade for 3m visuals)")

    # FEMA
    fema = result.get("fema")
    if fema and fema.get("flood_zone"):
        parts.append(f"FEMA: Zone {fema['flood_zone']} — {fema.get('risk_level', 'unknown')} risk")
    else:
        parts.append("FEMA: No flood zone data")

    # Flags
    if result["flags"]:
        parts.append(f"\nDistress signals ({len(result['flags'])}):")
        for flag in result["flags"]:
            parts.append(f"  - {flag['signal_code']}: confidence={flag['confidence']:.2f}")
    else:
        parts.append("\nNo distress signals detected.")

    parts.append(f"\nDistress score: {result['distress_score']:.2f}/10.0")

    return "\n".join(parts)


def scan_and_write(lat: float, lng: float, parcel_id: str,
                   county_name: str, state_code: str,
                   months: int = 12) -> dict:
    """
    Scan a property and write any flags as signals to the database.

    Args:
        lat, lng: property coordinates
        parcel_id: GIS parcel ID (for DB lookup)
        county_name: e.g. "Mecklenburg"
        state_code: e.g. "NC"
        months: Sentinel lookback

    Returns scan result dict with added `db_write` status.
    """
    from src.db import get_db_connection
    from src.signals import write_scan_results

    result = scan_distress(lat, lng, months=months)

    if not result["flags"]:
        result["db_write"] = {"status": "skipped", "reason": "no_flags"}
        return result

    # Format flags for the signal writer
    scan_results = []
    for flag in result["flags"]:
        scan_results.append({
            "parcel_id": parcel_id,
            "signal_code": flag["signal_code"],
            "confidence": flag["confidence"],
            "evidence": flag["evidence"],
        })

    try:
        conn = get_db_connection()
        success, fail = write_scan_results(
            conn=conn,
            county_name=county_name,
            state_code=state_code,
            results=scan_results,
        )
        conn.close()
        result["db_write"] = {"status": "ok", "success": success, "fail": fail}
        logger.info("signals_written",
                     parcel_id=parcel_id,
                     success=success, fail=fail)
    except Exception as e:
        result["db_write"] = {"status": "error", "error": str(e)}
        logger.error("signal_write_failed", parcel_id=parcel_id, error=str(e))

    return result


def enrich_sentinel(lat: float, lng: float, months: int = 12,
                    include_rgb: bool = False) -> dict:
    """
    Sentinel-only enrichment for a parcel that already has Pass 1 data.

    Runs Sentinel-2 trends (with Landsat fallback). Does NOT re-run NAIP or FEMA.
    Returns dict with DB-ready Sentinel columns.

    Args:
        lat, lng: property coordinates
        months: lookback window (default 12)
        include_rgb: download RGB image from CDSE (costs 1 extra CDSE request)
    """
    result = {
        "sentinel_trend_direction": "insufficient_data",
        "sentinel_trend_slope": None,
        "sentinel_latest_ndvi": None,
        "sentinel_months_data": 0,
        "sentinel_mean_ndvi": None,
        "sentinel_data_source": None,
        "sentinel_chart_url": None,
        "sentinel_scan_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "errors": [],
    }

    # Try Sentinel-2 first (1 CDSE request for stats, +1 if include_rgb)
    sentinel_ok = False
    sentinel_data = None
    try:
        sentinel_data = sentinel_trends(lat, lng, months=months,
                                         include_chart=True,
                                         include_rgb=include_rgb)
        if sentinel_data and sentinel_data.get("months_with_data", 0) > 0:
            sentinel_ok = True
            result["sentinel_trend_direction"] = sentinel_data["trend_direction"]
            result["sentinel_trend_slope"] = sentinel_data["trend_slope"]
            result["sentinel_latest_ndvi"] = sentinel_data["latest_ndvi"]
            result["sentinel_months_data"] = sentinel_data["months_with_data"]
            result["sentinel_mean_ndvi"] = sentinel_data["mean_ndvi"]
            result["sentinel_data_source"] = "Sentinel-2"
            result["sentinel_chart_url"] = sentinel_data.get("chart_url")
    except Exception as e:
        result["errors"].append(f"sentinel: {e}")
        logger.warning("enrich_sentinel_failed", lat=lat, lng=lng, error=str(e))

    # Landsat fallback (free, unlimited — capped at 12 months to limit calls)
    landsat_data = None
    if not sentinel_ok:
        try:
            landsat_months = min(months, 12)
            landsat_data = landsat_trends(lat, lng, months=landsat_months)
            if landsat_data and landsat_data.get("months_with_data", 0) > 0:
                result["sentinel_trend_direction"] = landsat_data["trend_direction"]
                result["sentinel_trend_slope"] = landsat_data["trend_slope"]
                result["sentinel_latest_ndvi"] = landsat_data["latest_ndvi"]
                result["sentinel_months_data"] = landsat_data["months_with_data"]
                result["sentinel_mean_ndvi"] = landsat_data["mean_ndvi"]
                result["sentinel_data_source"] = "Landsat"
        except Exception as e:
            result["errors"].append(f"landsat: {e}")
            logger.warning("enrich_landsat_failed", lat=lat, lng=lng, error=str(e))

    # Attach raw trend data for rescore_with_sentinel
    result["_trend_data"] = sentinel_data if sentinel_ok else landsat_data

    logger.info("enrich_sentinel_complete",
                lat=lat, lng=lng,
                source=result["sentinel_data_source"],
                months=result["sentinel_months_data"],
                trend=result["sentinel_trend_direction"])

    return result


def rescore_with_sentinel(naip_ndvi: float | None,
                          fema_data: dict | None,
                          sentinel_result: dict) -> dict:
    """
    Re-evaluate flags with Sentinel/Landsat trend data included.

    Takes existing Pass 1 NAIP+FEMA data and new Sentinel enrichment result,
    re-runs generate_all_flags() with trend data, recomputes distress_score.

    Returns dict with updated flag booleans, confidences, score, and scan_pass=2.
    """
    naip_for_flags = None
    if naip_ndvi is not None:
        naip_for_flags = {
            "current_ndvi": naip_ndvi,
            "mean_historical_ndvi": None,
            "errors": [],
        }

    trend_data = sentinel_result.get("_trend_data")
    sentinel_for_flags = None
    if trend_data and trend_data.get("months_with_data", 0) > 0:
        sentinel_for_flags = {
            "trend_slope": trend_data.get("trend_slope"),
            "trend_direction": trend_data.get("trend_direction"),
            "latest_ndvi": trend_data.get("latest_ndvi"),
            "earliest_ndvi": trend_data.get("earliest_ndvi"),
            "months_with_data": trend_data.get("months_with_data", 0),
            "errors": [],
        }

    fema_for_flags = None
    if fema_data:
        fema_for_flags = {
            "flood_zone": fema_data.get("flood_zone") or fema_data.get("fema_zone"),
            "risk_level": fema_data.get("risk_level") or fema_data.get("fema_risk"),
            "is_sfha": fema_data.get("is_sfha") or fema_data.get("fema_sfha", False),
        }

    flags = generate_all_flags(
        naip=naip_for_flags,
        sentinel=sentinel_for_flags,
        fema=fema_for_flags,
    )

    score = 0.0
    for flag in flags:
        weight = SIGNAL_WEIGHTS.get(flag["signal_code"], 1.0)
        score += weight * flag["confidence"]
    score = round(min(score, 10.0), 2)

    flag_codes = {f["signal_code"] for f in flags}
    flag_confs = {f["signal_code"]: f["confidence"] for f in flags}

    return {
        "distress_score": score,
        "distress_flags": ",".join(sorted(flag_codes)) if flag_codes else None,
        "flag_veg": "vegetation_overgrowth" in flag_codes,
        "flag_flood": "flood_risk" in flag_codes,
        "flag_structural": "structural_change" in flag_codes,
        "flag_neglect": "vegetation_neglect" in flag_codes,
        "veg_confidence": max(
            flag_confs.get("vegetation_overgrowth", 0.0),
            flag_confs.get("vegetation_neglect", 0.0),
        ) or None,
        "flood_confidence": flag_confs.get("flood_risk"),
        "scan_pass": 2,
    }
