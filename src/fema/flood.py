"""
FEMA flood zone analysis.

Combines flood zone query with map tile export for a complete
flood risk assessment at a point.
"""

import math

import structlog

from src.fema.client import FEMAClient
from src.storage import upload_bytes, make_point_key

logger = structlog.get_logger("fema.flood")


def _make_bbox(lat: float, lng: float, buffer_meters: float = 200.0):
    """Create bbox for flood map tile (larger than NDVI — shows surrounding area)."""
    lat_offset = buffer_meters / 111_000
    lng_offset = buffer_meters / (111_000 * math.cos(math.radians(lat)))
    return (lng - lng_offset, lat - lat_offset, lng + lng_offset, lat + lat_offset)


def fema_flood(lat: float, lng: float, skip_map: bool = False) -> dict:
    """
    Complete FEMA flood analysis for a point.

    Returns:
        {
            "lat": float,
            "lng": float,
            "flood_zone": str | None,
            "risk_level": "high" | "moderate" | "low" | "unknown",
            "is_sfha": bool,
            "floodway": str | None,
            "zone_subtype": str | None,
            "flood_risk_flag": bool,
            "flood_risk_confidence": float,
            "map_url": str | None,
            "data_source": "FEMA_NFHL",
            "errors": [],
        }
    """
    client = FEMAClient()

    result = {
        "lat": lat,
        "lng": lng,
        "flood_zone": None,
        "risk_level": "unknown",
        "is_sfha": False,
        "floodway": None,
        "zone_subtype": None,
        "flood_risk_flag": False,
        "flood_risk_confidence": 0.0,
        "map_url": None,
        "data_source": "FEMA_NFHL",
        "errors": [],
    }

    # Query flood zone
    zone_data = client.query_flood_zone(lat, lng)

    if zone_data.get("error"):
        result["errors"].append(zone_data["error"])
        return result

    result["flood_zone"] = zone_data.get("flood_zone")
    result["risk_level"] = zone_data.get("risk_level", "unknown")
    result["is_sfha"] = zone_data.get("is_sfha", False)
    result["floodway"] = zone_data.get("floodway")
    result["zone_subtype"] = zone_data.get("zone_subtype")

    # Determine flag and confidence
    if result["risk_level"] == "high":
        result["flood_risk_flag"] = True
        result["flood_risk_confidence"] = 1.0
    elif result["risk_level"] == "moderate":
        result["flood_risk_flag"] = True
        result["flood_risk_confidence"] = 0.6

    # Export flood map tile
    if not skip_map:
        try:
            bbox = _make_bbox(lat, lng)
            map_bytes = client.get_flood_map_tile(bbox)
            if map_bytes:
                key = make_point_key(lat, lng, "fema_flood_map.png")
                url = upload_bytes(key, map_bytes)
                result["map_url"] = url
        except Exception as e:
            result["errors"].append(f"map_export: {e}")
            logger.warning("fema_map_failed", error=str(e))

    logger.info("fema_flood_complete",
                lat=lat, lng=lng,
                zone=result["flood_zone"],
                risk=result["risk_level"],
                flag=result["flood_risk_flag"])

    return result
