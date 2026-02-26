"""
Nominatim (OpenStreetMap) geocoder for resolving city + ZIP from street + county.

USPS Address API v3 requires either (city + state) or ZIP. Many parcels in
gis_parcels_core only have street + county + state — no city or ZIP. This
module fills the gap using the free Nominatim API.

Usage policy: https://operations.osmfoundation.org/policies/nominatim/
  - Max 1 request/sec (enforced via time.sleep)
  - Must set a unique User-Agent
  - No bulk geocoding (we cache aggressively)
"""

import math
import time
from typing import Optional

import requests
import structlog

logger = structlog.get_logger("usps.geocode")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "DistressScanner/1.0"
_MIN_INTERVAL = 1.0  # seconds between requests (Nominatim policy)

# Session-level cache: "street|county|state" -> (result dict, timestamp)
# Negative results (confidence=none) expire after 10 minutes
_cache: dict[str, tuple[dict, float]] = {}
_NEGATIVE_TTL = 600  # 10 minutes
_last_request: float = 0


def _cache_key(street: str, county: str, state: str) -> str:
    return f"{street.strip().upper()}|{county.strip().upper()}|{state.strip().upper()}"


def _haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Haversine distance in meters between two points."""
    R = 6371000
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlng / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def resolve_city_zip(
    street: str,
    county: str,
    state: str,
    lat: Optional[float] = None,
    lng: Optional[float] = None,
) -> dict:
    """
    Resolve city and ZIP code from street + county + state via Nominatim.

    Args:
        street: Street address (e.g. "718 NORTON DR")
        county: County name (e.g. "Gaston")
        state: State code (e.g. "NC")
        lat/lng: Optional parcel coordinates for disambiguation

    Returns:
        {
            "city": str | None,
            "zip": str | None,
            "source": "nominatim" | "cache",
            "confidence": "exact" | "ambiguous" | "none",
        }
    """
    global _last_request

    key = _cache_key(street, county, state)
    if key in _cache:
        cached_result, cached_at = _cache[key]
        # Expire negative results after TTL
        if cached_result.get("confidence") == "none" and (time.time() - cached_at) > _NEGATIVE_TTL:
            del _cache[key]
        else:
            out = cached_result.copy()
            out["source"] = "cache"
            return out

    # Rate limit: 1 req/sec
    elapsed = time.time() - _last_request
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)

    params = {
        "street": street,
        "county": f"{county} County",
        "state": state,
        "country": "US",
        "format": "json",
        "addressdetails": 1,
        "limit": 5,
    }

    try:
        resp = requests.get(
            NOMINATIM_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        _last_request = time.time()
        resp.raise_for_status()
        results = resp.json()
    except Exception as e:
        logger.warning("nominatim_error", street=street, county=county, error=str(e))
        result = {"city": None, "zip": None, "source": "nominatim", "confidence": "none"}
        _cache[key] = (result, time.time())
        return result

    if not results:
        logger.debug("nominatim_no_results", street=street, county=county, state=state)
        result = {"city": None, "zip": None, "source": "nominatim", "confidence": "none"}
        _cache[key] = (result, time.time())
        return result

    # If multiple results and lat/lng available, pick closest
    if len(results) > 1 and lat is not None and lng is not None:
        results.sort(key=lambda r: _haversine(
            lat, lng,
            float(r.get("lat", 0)),
            float(r.get("lon", 0)),
        ))

    best = results[0]
    addr = best.get("address", {})
    city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("hamlet")
    zipcode = addr.get("postcode")

    # Strip extended ZIP (keep 5-digit only)
    if zipcode and len(zipcode) > 5:
        zipcode = zipcode[:5]

    confidence = "exact" if len(results) == 1 else "ambiguous"
    if not city and not zipcode:
        confidence = "none"

    result = {
        "city": city,
        "zip": zipcode,
        "source": "nominatim",
        "confidence": confidence,
    }
    _cache[key] = (result, time.time())

    logger.info("nominatim_resolved",
                street=street, county=county, state=state,
                city=city, zip=zipcode, confidence=confidence,
                candidates=len(results))
    return result
