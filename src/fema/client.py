"""
FEMA National Flood Hazard Layer (NFHL) ArcGIS REST client.

Free, no API key required.
Endpoint: https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer

Queries flood zone classification at a point and exports flood map PNG tiles.
"""

import json
import hashlib
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import structlog

from src.storage import upload_bytes, make_point_key

logger = structlog.get_logger("fema.client")

FEMA_BASE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"

# Layer 28 = S_FLD_HAZ_AR (Flood Hazard Areas)
FLOOD_HAZARD_LAYER = 28

# Flood zone risk classification
HIGH_RISK_ZONES = {"A", "AE", "AH", "AO", "AR", "A99", "V", "VE"}
MODERATE_RISK_ZONES = {"B"}  # Zone B or X (shaded) = 0.2% annual chance
# Note: Zone X can be moderate (shaded/500-year) or low (unshaded/minimal) — determined by ZONE_SUBTY

CACHE_DIR = Path("data/cache/fema")
CACHE_TTL_SECONDS = 30 * 24 * 60 * 60  # 30 days (flood zones change rarely)


class FEMAClient:
    """Client for FEMA NFHL ArcGIS MapServer."""

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = self._build_session()
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _cache_key(self, prefix: str, params: dict) -> str:
        key_str = f"{prefix}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_str.encode()).hexdigest()[:16]

    def _get_cached(self, cache_key: str) -> dict | None:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if not cache_file.exists():
            return None
        age = time.time() - cache_file.stat().st_mtime
        if age > CACHE_TTL_SECONDS:
            cache_file.unlink(missing_ok=True)
            return None
        try:
            return json.loads(cache_file.read_text())
        except (json.JSONDecodeError, OSError):
            cache_file.unlink(missing_ok=True)
            return None

    def _set_cache(self, cache_key: str, data: dict) -> None:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        try:
            cache_file.write_text(json.dumps(data))
        except OSError:
            pass

    def query_flood_zone(self, lat: float, lng: float) -> dict:
        """
        Query flood zone at a point.

        Returns:
            {
                "flood_zone": str | None,
                "floodway": str | None,
                "is_sfha": bool,
                "zone_subtype": str | None,
                "risk_level": "high" | "moderate" | "low" | "unknown",
                "source": "FEMA_NFHL",
            }
        """
        cache_key = self._cache_key("flood_zone", {"lat": lat, "lng": lng})
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        params = {
            "geometry": json.dumps({"x": lng, "y": lat, "spatialReference": {"wkid": 4326}}),
            "geometryType": "esriGeometryPoint",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "FLD_ZONE,SFHA_TF,ZONE_SUBTY,FLD_AR_ID,STATIC_BFE",
            "returnGeometry": "false",
            "f": "json",
        }

        url = f"{FEMA_BASE_URL}/{FLOOD_HAZARD_LAYER}/query"
        logger.info("fema_flood_query", lat=lat, lng=lng)

        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error("fema_query_failed", error=str(e))
            return {
                "flood_zone": None, "floodway": None, "is_sfha": False,
                "zone_subtype": None, "risk_level": "unknown",
                "source": "FEMA_NFHL", "error": str(e),
            }

        features = data.get("features", [])
        if not features:
            result = {
                "flood_zone": None, "floodway": None, "is_sfha": False,
                "zone_subtype": None, "risk_level": "unknown",
                "source": "FEMA_NFHL", "note": "no_fema_coverage",
            }
            self._set_cache(cache_key, result)
            return result

        attrs = features[0].get("attributes", {})
        zone = attrs.get("FLD_ZONE", "")
        floodway = attrs.get("FLOODWAY")
        sfha = attrs.get("SFHA_TF", "F") == "T"
        subtype = attrs.get("ZONE_SUBTY")

        # Classify risk
        subtype_str = str(subtype).upper() if subtype else ""
        if zone in HIGH_RISK_ZONES or sfha:
            risk_level = "high"
        elif zone == "X" and "500" in subtype_str:
            risk_level = "moderate"
        elif zone == "B" or (zone == "X" and "SHADED" in subtype_str and "MINIMAL" not in subtype_str):
            risk_level = "moderate"
        elif zone:
            risk_level = "low"
        else:
            risk_level = "unknown"

        result = {
            "flood_zone": zone,
            "floodway": floodway,
            "is_sfha": sfha,
            "zone_subtype": subtype,
            "risk_level": risk_level,
            "source": "FEMA_NFHL",
        }

        self._set_cache(cache_key, result)
        logger.info("fema_flood_result", lat=lat, lng=lng, zone=zone, risk=risk_level)
        return result

    def get_flood_map_tile(self, bbox: tuple[float, float, float, float],
                           width: int = 512, height: int = 512) -> bytes | None:
        """
        Get FEMA flood map overlay PNG tile.

        bbox: (min_lng, min_lat, max_lng, max_lat) in EPSG:4326
        Returns PNG bytes or None.
        """
        min_lng, min_lat, max_lng, max_lat = bbox
        params = {
            "bbox": f"{min_lng},{min_lat},{max_lng},{max_lat}",
            "bboxSR": "4326",
            "imageSR": "4326",
            "size": f"{width},{height}",
            "format": "png",
            "transparent": "true",
            "layers": f"show:{FLOOD_HAZARD_LAYER}",
            "f": "image",
        }

        url = f"{FEMA_BASE_URL}/export"
        logger.info("fema_map_export", bbox=bbox)

        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "image" in content_type:
                return resp.content
        except requests.RequestException as e:
            logger.error("fema_map_export_failed", error=str(e))

        return None
