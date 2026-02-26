"""
NAIP ArcGIS REST ImageServer client.

Provides access to USGS NAIP imagery (free, no API key):
- Point-level pixel identification (band values + acquisition dates)
- Bounding box image export (RGB + NIR)
- NDVI computation from band values
- Year-specific queries via mosaicRule for historical comparison

Endpoint:
  https://imagery.nationalmap.gov/arcgis/rest/services/USGSNAIPPlus/ImageServer

NAIP coverage is on a rotating 2-3 year cycle per state. Not all years have
data at every location. Use Sentinel-2 (Phase 2) for monthly time-series.
"""

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import structlog

logger = structlog.get_logger("naip.client")

NAIP_BASE_URL = "https://imagery.nationalmap.gov/arcgis/rest/services/USGSNAIPPlus/ImageServer"

CACHE_DIR = Path("data/cache/naip")
CACHE_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days

# Years to check for historical NAIP coverage (recent cycles)
NAIP_YEARS_TO_CHECK = [2023, 2022, 2021, 2020, 2019, 2018, 2016, 2014, 2012]


class NAIPClient:
    """Client for USGS NAIP ArcGIS ImageServer REST API."""

    def __init__(self, base_url: str = NAIP_BASE_URL, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        self.session = self._build_session()
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _build_session(self) -> requests.Session:
        """Build session with retry adapter for resilient HTTP calls."""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _cache_key(self, endpoint: str, params: dict) -> str:
        """Generate deterministic cache key from endpoint + params."""
        key_str = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_str.encode()).hexdigest()[:16]

    def _get_cached(self, cache_key: str) -> dict | None:
        """Return cached JSON response if exists and not expired."""
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
        """Write response to cache file."""
        cache_file = CACHE_DIR / f"{cache_key}.json"
        try:
            cache_file.write_text(json.dumps(data))
        except OSError as e:
            logger.warning("cache_write_failed", key=cache_key, error=str(e))

    def _extract_date_from_catalog(self, catalog_data: dict) -> str | None:
        """Extract acquisition date from identify catalog items."""
        features = catalog_data.get("features", [])
        for feat in features:
            attrs = feat.get("attributes", {})
            # Only look at primary resolution tiles (Category=1)
            if attrs.get("Category") != 1:
                continue
            # Field name is lowercase "acquisition_date" (epoch ms)
            acq = attrs.get("acquisition_date")
            if acq and isinstance(acq, (int, float)) and acq > 1e10:
                return datetime.fromtimestamp(acq / 1000).strftime("%Y-%m-%d")
        # Fallback: check any feature for Year field
        for feat in features:
            attrs = feat.get("attributes", {})
            year = attrs.get("Year")
            if year:
                return f"{year}-01-01"
        return None

    def identify(self, lat: float, lng: float, mosaic_rule: dict | None = None) -> dict:
        """
        Get pixel values and metadata at a specific point.

        Returns raw JSON response containing:
        - value: pixel band values (e.g. "185, 178, 169, 157")
        - catalogItems: acquisition metadata
        """
        params = {
            "geometry": json.dumps({"x": lng, "y": lat, "spatialReference": {"wkid": 4326}}),
            "geometryType": "esriGeometryPoint",
            "returnCatalogItems": "true",
            "returnGeometry": "false",
            "f": "json",
        }
        if mosaic_rule:
            params["mosaicRule"] = json.dumps(mosaic_rule)

        cache_key = self._cache_key("identify", {
            "lat": lat, "lng": lng,
            "mosaic": json.dumps(mosaic_rule) if mosaic_rule else "default",
        })
        cached = self._get_cached(cache_key)
        if cached:
            logger.debug("cache_hit", endpoint="identify", lat=lat, lng=lng)
            return cached

        url = f"{self.base_url}/identify"
        logger.info("naip_identify", lat=lat, lng=lng,
                     mosaic="custom" if mosaic_rule else "default")

        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        self._set_cache(cache_key, data)
        return data

    def export_image(self, bbox: tuple[float, float, float, float],
                     width: int = 256, height: int = 256,
                     format: str = "png",
                     rendering_rule: str | None = None) -> bytes | None:
        """
        Export image for a bounding box as PNG bytes.

        bbox: (min_lng, min_lat, max_lng, max_lat) in EPSG:4326
        rendering_rule: optional, e.g. "NDVI_Color" for built-in NDVI colormap
        Returns raw image bytes or None on failure.
        """
        min_lng, min_lat, max_lng, max_lat = bbox
        params = {
            "bbox": f"{min_lng},{min_lat},{max_lng},{max_lat}",
            "bboxSR": "4326",
            "imageSR": "4326",
            "size": f"{width},{height}",
            "format": format,
            "f": "image",
        }
        if rendering_rule:
            params["renderingRule"] = json.dumps({"rasterFunction": rendering_rule})

        cache_key = self._cache_key("export", params)
        cache_file = CACHE_DIR / f"{cache_key}.{format}"
        if cache_file.exists():
            age = time.time() - cache_file.stat().st_mtime
            if age <= CACHE_TTL_SECONDS:
                return cache_file.read_bytes()

        url = f"{self.base_url}/exportImage"
        logger.info("naip_export_image", bbox=bbox, size=f"{width}x{height}")

        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "image" in content_type:
            cache_file.write_bytes(resp.content)
            return resp.content

        logger.warning("export_not_image", content_type=content_type)
        return None

    def _parse_bands_and_ndvi(self, value_str: str) -> dict:
        """Parse band values from identify response and compute NDVI."""
        result = {"ndvi": None, "bands": None, "error": None}

        if not value_str or value_str in ("NoData", "Pixel value is NoData"):
            result["error"] = "no_imagery_at_location"
            return result

        try:
            parts = value_str.replace(",", " ").split()
            band_values = [float(p) for p in parts if p.strip()]

            if len(band_values) >= 4:
                red, green, blue, nir = band_values[0], band_values[1], band_values[2], band_values[3]
            elif len(band_values) == 3:
                result["error"] = "no_nir_band"
                result["bands"] = {"red": band_values[0], "green": band_values[1],
                                   "blue": band_values[2], "nir": None}
                return result
            else:
                result["error"] = f"unexpected_band_count: {len(band_values)}"
                return result

            result["bands"] = {"red": red, "green": green, "blue": blue, "nir": nir}

            denominator = nir + red
            if denominator == 0:
                result["ndvi"] = 0.0
            else:
                result["ndvi"] = (nir - red) / denominator

        except (ValueError, IndexError) as e:
            result["error"] = f"band_parse_failure: {e}"
            logger.error("band_parse_failure", value_str=value_str, error=str(e))

        return result

    def compute_ndvi_at_point(self, lat: float, lng: float) -> dict:
        """
        Compute NDVI at a single point using the identify endpoint.

        NAIP bands: Band 1=Red, Band 2=Green, Band 3=Blue, Band 4=NIR
        NDVI = (NIR - Red) / (NIR + Red)

        Returns:
            {
                "ndvi": float | None,
                "bands": {"red", "green", "blue", "nir"} | None,
                "acquisition_date": str | None,
                "error": str | None,
            }
        """
        try:
            data = self.identify(lat, lng)
        except requests.RequestException as e:
            logger.error("identify_failed", lat=lat, lng=lng, error=str(e))
            return {"ndvi": None, "bands": None, "acquisition_date": None,
                    "error": f"identify_failed: {e}"}

        parsed = self._parse_bands_and_ndvi(data.get("value", ""))
        parsed["acquisition_date"] = self._extract_date_from_catalog(
            data.get("catalogItems", {})
        )

        logger.info("ndvi_computed", lat=lat, lng=lng, ndvi=parsed["ndvi"],
                     date=parsed["acquisition_date"])
        return parsed

    def get_available_years(self, lat: float, lng: float) -> list[dict]:
        """
        Check which NAIP years have data at this point using mosaicRule queries.

        Tests each year in NAIP_YEARS_TO_CHECK and returns those with valid data.
        Results are cached for 7 days.

        Returns: [{"year": 2022, "date": "2022-06-01", "has_data": True}, ...]
        """
        cache_key = self._cache_key("available_years", {"lat": lat, "lng": lng})
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        results = []
        for year in NAIP_YEARS_TO_CHECK:
            mosaic_rule = {
                "mosaicMethod": "esriMosaicAttribute",
                "sortField": "Year",
                "sortValue": str(year),
                "ascending": True,
                "where": f"Year = {year} AND Category = 1",
            }

            try:
                data = self.identify(lat, lng, mosaic_rule=mosaic_rule)
                value_str = data.get("value", "")

                if value_str and value_str not in ("NoData", "Pixel value is NoData"):
                    acq_date = self._extract_date_from_catalog(
                        data.get("catalogItems", {})
                    )
                    results.append({
                        "year": year,
                        "date": acq_date or f"{year}-01-01",
                        "has_data": True,
                    })
                    logger.debug("year_available", year=year, lat=lat, lng=lng)
            except requests.RequestException:
                continue

        results.sort(key=lambda x: x["year"])
        self._set_cache(cache_key, results)
        logger.info("available_years_found", lat=lat, lng=lng,
                     years=[r["year"] for r in results])
        return results

    def get_ndvi_for_year(self, lat: float, lng: float, year: int) -> dict:
        """
        Get NDVI at a point for a specific year using mosaicRule.

        Returns same structure as compute_ndvi_at_point().
        """
        mosaic_rule = {
            "mosaicMethod": "esriMosaicAttribute",
            "sortField": "Year",
            "sortValue": str(year),
            "ascending": True,
            "where": f"Year = {year} AND Category = 1",
        }

        cache_key = self._cache_key("ndvi_year", {"lat": lat, "lng": lng, "year": year})
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            data = self.identify(lat, lng, mosaic_rule=mosaic_rule)
            parsed = self._parse_bands_and_ndvi(data.get("value", ""))
            parsed["acquisition_date"] = self._extract_date_from_catalog(
                data.get("catalogItems", {})
            ) or f"{year}-01-01"

            self._set_cache(cache_key, parsed)
            return parsed

        except Exception as e:
            logger.warning("historical_ndvi_failed", year=year, error=str(e))
            return {"ndvi": None, "bands": None, "acquisition_date": f"{year}-01-01",
                    "error": f"query_failed: {e}"}
