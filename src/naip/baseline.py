"""
NAIP multi-year NDVI baseline analysis.

Computes current NDVI from the latest NAIP imagery, compares against historical
acquisitions (up to 3 prior cycles), and flags potential vegetation overgrowth.

NDVI interpretation:
  < 0.10 : bare soil, rock, impervious surface
  0.10-0.20 : sparse vegetation
  0.20-0.40 : moderate vegetation (maintained lawn)
  0.40-0.60 : dense vegetation (healthy yard with trees)
  > 0.60 : very dense vegetation (potential overgrowth if unexpected)
"""

import math
from datetime import datetime
from pathlib import Path

import structlog

from src.naip.client import NAIPClient
from src.storage import upload_bytes, make_point_key

logger = structlog.get_logger("naip.baseline")

# Thresholds
NDVI_OVERGROWTH_CHANGE = 0.15  # NDVI increase above historical mean to flag
NDVI_OVERGROWTH_ABSOLUTE = 0.50  # Minimum absolute NDVI for overgrowth flag
NDVI_NEGLECT_LOW = 0.10  # Below this = bare/impervious (no vegetation concern)


def make_bbox(lat: float, lng: float, buffer_meters: float = 50.0) -> tuple[float, float, float, float]:
    """
    Create a bounding box around a point.

    Approximate degree conversion:
      1 degree latitude  ~ 111,000 meters
      1 degree longitude ~ 111,000 * cos(lat_radians) meters

    buffer_meters=50 creates a ~100m x 100m box (roughly 1 parcel).

    Returns (min_lng, min_lat, max_lng, max_lat) in EPSG:4326.
    """
    lat_offset = buffer_meters / 111_000
    lng_offset = buffer_meters / (111_000 * math.cos(math.radians(lat)))

    return (
        lng - lng_offset,  # min_lng
        lat - lat_offset,  # min_lat
        lng + lng_offset,  # max_lng
        lat + lat_offset,  # max_lat
    )


def _assess_vegetation(ndvi: float | None) -> str:
    """Human-readable NDVI assessment."""
    if ndvi is None:
        return "No data available"
    if ndvi < 0.10:
        return "Bare soil / impervious surface"
    if ndvi < 0.20:
        return "Sparse vegetation"
    if ndvi < 0.40:
        return "Moderate vegetation (typical maintained property)"
    if ndvi < 0.60:
        return "Dense vegetation (healthy yard with trees)"
    return "Very dense vegetation (potential overgrowth)"


def _compute_trend(values: list[dict]) -> str:
    """Determine trend direction from chronological NDVI values."""
    ndvi_vals = [v["ndvi"] for v in values if v.get("ndvi") is not None]
    if len(ndvi_vals) < 2:
        return "insufficient_data"

    # Simple: compare first half average vs second half average
    mid = len(ndvi_vals) // 2
    first_half = sum(ndvi_vals[:mid]) / mid
    second_half = sum(ndvi_vals[mid:]) / (len(ndvi_vals) - mid)

    diff = second_half - first_half
    if diff > 0.05:
        return "increasing"
    elif diff < -0.05:
        return "decreasing"
    return "stable"


# Module-level shared client for batch thread safety (requests.Session is thread-safe for GETs)
_shared_client = None


def _get_shared_client() -> NAIPClient:
    global _shared_client
    if _shared_client is None:
        _shared_client = NAIPClient()
    return _shared_client


def naip_ndvi_fast(lat: float, lng: float) -> dict:
    """
    Fast NDVI lookup for batch scanning. Single API call, no history, no export.

    Returns:
        {
            "ndvi": float | None,
            "date": str | None,
            "category": str,  # bare/minimal/sparse/moderate/dense
            "error": str | None,
        }
    """
    client = _get_shared_client()
    try:
        result = client.compute_ndvi_at_point(lat, lng)
        ndvi = result.get("ndvi")
        # Categorize per RE distress thresholds
        if ndvi is None:
            category = "no_data"
        elif ndvi < 0.10:
            category = "bare"
        elif ndvi < 0.30:
            category = "minimal"
        elif ndvi < 0.50:
            category = "sparse"
        elif ndvi < 0.65:
            category = "moderate"
        else:
            category = "dense"

        return {
            "ndvi": round(ndvi, 4) if ndvi is not None else None,
            "date": result.get("acquisition_date"),
            "category": category,
            "error": result.get("error"),
        }
    except Exception as e:
        logger.warning("naip_ndvi_fast_failed", lat=lat, lng=lng, error=str(e))
        return {"ndvi": None, "date": None, "category": "error", "error": str(e)}


def naip_ndvi_historical(lat: float, lng: float,
                         years: list[int] = None) -> list[dict]:
    """
    Fetch NDVI for multiple historical NAIP years at a point. Batch-safe.

    Uses Microsoft Planetary Computer STAC + COG reads for true historical data.
    The USGS ImageServer only serves the most recent vintage per state,
    so we can't use it for multi-year trends.

    Returns: [{"year": 2020, "ndvi": 0.45, "date": "2020-06-15"}, ...]
    """
    if years is None:
        years = [2022, 2020, 2018, 2016, 2014, 2012]

    from src.naip.planetary import get_historical_ndvi
    return get_historical_ndvi(lat, lng, years=years)


def compute_ndvi_slope(points: list[tuple[float, float]]) -> float | None:
    """
    Compute NDVI slope (change per year) via least-squares linear regression.

    Args:
        points: list of (year, ndvi) tuples, e.g. [(2014, 0.35), (2020, 0.52)]

    Returns:
        slope (NDVI change per year) or None if < 2 data points.
        Positive slope = increasing vegetation (potential overgrowth).
        Negative slope = decreasing vegetation (potential clearing/neglect).
    """
    if len(points) < 2:
        return None

    n = len(points)
    sum_x = sum(p[0] for p in points)
    sum_y = sum(p[1] for p in points)
    sum_xy = sum(p[0] * p[1] for p in points)
    sum_x2 = sum(p[0] ** 2 for p in points)

    denom = n * sum_x2 - sum_x ** 2
    if denom == 0:
        return 0.0

    slope = (n * sum_xy - sum_x * sum_y) / denom
    return round(slope, 6)


def naip_baseline(lat: float, lng: float, skip_historical: bool = False,
                  skip_image_export: bool = False) -> dict:
    """
    Compute NDVI baseline for a location using NAIP imagery.

    Steps:
      1. Get current NDVI via identify endpoint
      2. Query available historical image dates
      3. For each historical date, compute NDVI
      4. Calculate statistics: current vs historical mean, trend
      5. Evaluate overgrowth flag

    Returns dict with current NDVI, historical comparisons, trend, and flags.
    """
    client = NAIPClient()
    bbox = make_bbox(lat, lng)

    result = {
        "lat": lat,
        "lng": lng,
        "bbox": list(bbox),
        "current_ndvi": None,
        "current_bands": None,
        "current_date": None,
        "historical_ndvi": [],
        "mean_historical_ndvi": None,
        "ndvi_change": None,
        "ndvi_change_pct": None,
        "trend": "insufficient_data",
        "vegetation_assessment": None,
        "overgrowth_flag": False,
        "overgrowth_confidence": 0.0,
        "data_source": "NAIP",
        "image_url": None,
        "errors": [],
    }

    # Step 1: Get current NDVI
    current = client.compute_ndvi_at_point(lat, lng)

    if current.get("error"):
        result["errors"].append(f"current_ndvi: {current['error']}")
        if current["ndvi"] is None:
            # Can't proceed without current NDVI
            result["vegetation_assessment"] = "Unable to compute — no NAIP imagery at this location"
            return result

    result["current_ndvi"] = current["ndvi"]
    result["current_bands"] = current["bands"]
    result["current_date"] = current["acquisition_date"]
    result["vegetation_assessment"] = _assess_vegetation(current["ndvi"])

    # Step 2: Export RGB image tile and upload to R2
    if not skip_image_export:
        try:
            image_bytes = client.export_image(bbox, width=256, height=256, format="png")
            if image_bytes:
                key = make_point_key(lat, lng, "naip_rgb.png")
                url = upload_bytes(key, image_bytes)
                result["image_url"] = url
        except Exception as e:
            result["errors"].append(f"image_export: {e}")
            logger.warning("image_export_failed", error=str(e))

    # Step 3: Find available historical years at this location
    if skip_historical:
        # Batch mode: skip the 9-year scan, just return current NDVI
        logger.info("naip_baseline_complete",
                     lat=lat, lng=lng,
                     current_ndvi=result["current_ndvi"],
                     mode="skip_historical")
        return result

    available_years = client.get_available_years(lat, lng)

    if not available_years:
        logger.info("no_historical_data", lat=lat, lng=lng)
        result["errors"].append("no_historical_years_found")
        return result

    # Step 4: Get NDVI for each historical year
    # Exclude the current year (already have it from default identify)
    current_year = None
    if result["current_date"]:
        try:
            current_year = int(result["current_date"][:4])
        except (ValueError, TypeError):
            pass

    for entry in available_years:
        year = entry["year"]
        # Skip if same year as current (we already have that NDVI)
        if current_year and year == current_year:
            continue

        historical = client.get_ndvi_for_year(lat, lng, year)
        if historical.get("ndvi") is not None:
            result["historical_ndvi"].append({
                "date": historical.get("acquisition_date", f"{year}-01-01"),
                "year": year,
                "ndvi": historical["ndvi"],
            })
        elif historical.get("error"):
            result["errors"].append(f"historical_{year}: {historical['error']}")

    # Step 5: Compute statistics
    hist_values = [h["ndvi"] for h in result["historical_ndvi"] if h["ndvi"] is not None]

    if hist_values:
        mean_hist = sum(hist_values) / len(hist_values)
        result["mean_historical_ndvi"] = round(mean_hist, 4)

        if result["current_ndvi"] is not None:
            change = result["current_ndvi"] - mean_hist
            result["ndvi_change"] = round(change, 4)
            if mean_hist != 0:
                result["ndvi_change_pct"] = round((change / abs(mean_hist)) * 100, 1)

    # Step 6: Compute trend
    all_ndvi_points = result["historical_ndvi"].copy()
    if result["current_ndvi"] is not None and result["current_date"]:
        all_ndvi_points.append({
            "date": result["current_date"],
            "ndvi": result["current_ndvi"],
        })
    all_ndvi_points.sort(key=lambda x: x["date"])
    result["trend"] = _compute_trend(all_ndvi_points)

    # Step 7: Evaluate overgrowth flag
    if (result["current_ndvi"] is not None and
            result["mean_historical_ndvi"] is not None):
        ndvi_increase = result["current_ndvi"] - result["mean_historical_ndvi"]

        if (ndvi_increase > NDVI_OVERGROWTH_CHANGE and
                result["current_ndvi"] > NDVI_OVERGROWTH_ABSOLUTE):
            result["overgrowth_flag"] = True
            # Confidence scales linearly: 0.15 change = ~0.5, 0.30+ change = 1.0
            result["overgrowth_confidence"] = round(
                min(ndvi_increase / 0.30, 1.0), 2
            )

    logger.info("naip_baseline_complete",
                lat=lat, lng=lng,
                current_ndvi=result["current_ndvi"],
                mean_historical=result["mean_historical_ndvi"],
                change=result["ndvi_change"],
                trend=result["trend"],
                overgrowth=result["overgrowth_flag"])

    return result
