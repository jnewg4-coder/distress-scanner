"""
Historical NAIP NDVI via Microsoft Planetary Computer.

The USGS NAIP ImageServer only hosts the most recent vintage per state.
For multi-year historical NDVI (needed for slope/trend computation),
we use Planetary Computer's STAC API + Cloud-Optimized GeoTIFF reads.

NAIP archive on Planetary Computer: 2010-2023, all states, free, no auth.
COGs hosted on Azure Blob Storage, read via HTTP range requests (rasterio).

Each COG read is ~2 seconds (connection setup + range request).
For batch: 10 workers × 6 years = ~1.2 parcels/sec throughput.
Results cached 7 days to avoid redundant reads.
"""

import hashlib
import json
import time
import threading
from pathlib import Path

import requests
import rasterio
from rasterio.windows import Window
from pyproj import Transformer
import structlog

logger = structlog.get_logger("naip.planetary")

STAC_SEARCH_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
CACHE_DIR = Path("data/cache/naip_pc")
CACHE_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days

# Thread-safe transformer cache (CRS string → Transformer)
_transformer_cache = {}
_transformer_lock = threading.Lock()


def _get_transformer(crs) -> Transformer:
    """Get or create a thread-safe CRS transformer."""
    crs_str = str(crs)
    with _transformer_lock:
        if crs_str not in _transformer_cache:
            _transformer_cache[crs_str] = Transformer.from_crs(
                "EPSG:4326", crs, always_xy=True
            )
        return _transformer_cache[crs_str]


def _cache_key(prefix: str, params: dict) -> str:
    key_str = f"{prefix}:{json.dumps(params, sort_keys=True)}"
    return hashlib.sha256(key_str.encode()).hexdigest()[:16]


def _get_cached(cache_key: str) -> dict | None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
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


def _set_cache(cache_key: str, data: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{cache_key}.json"
    try:
        cache_file.write_text(json.dumps(data))
    except OSError as e:
        logger.warning("pc_cache_write_failed", key=cache_key, error=str(e))


def search_naip_items(lat: float, lng: float, years: list[int] = None) -> list[dict]:
    """
    Search Planetary Computer STAC for NAIP items at a point.

    Returns list of {year, date, cog_url} sorted by year desc.
    Results cached 7 days.
    """
    cache_key = _cache_key("stac_search", {"lat": lat, "lng": lng})
    cached = _get_cached(cache_key)
    if cached:
        items = cached
        if years:
            items = [i for i in items if i["year"] in years]
        return items

    payload = {
        "collections": ["naip"],
        "intersects": {"type": "Point", "coordinates": [lng, lat]},
        "limit": 20,
        "sortby": [{"field": "datetime", "direction": "desc"}],
    }

    try:
        resp = requests.post(STAC_SEARCH_URL, json=payload, timeout=30)
        resp.raise_for_status()
        features = resp.json().get("features", [])
    except Exception as e:
        logger.error("stac_search_failed", lat=lat, lng=lng, error=str(e))
        return []

    items = []
    seen_years = set()
    for feat in features:
        props = feat.get("properties", {})
        year_raw = props.get("naip:year")
        dt = props.get("datetime", "")[:10]
        cog_url = feat.get("assets", {}).get("image", {}).get("href")
        if year_raw and cog_url:
            # Normalize year to int (STAC may return string or int)
            year = int(year_raw) if isinstance(year_raw, str) else year_raw
            # Dedup: keep first (most recent) item per year for tile-boundary points
            if year in seen_years:
                continue
            seen_years.add(year)
            items.append({"year": year, "date": dt, "cog_url": cog_url})

    _set_cache(cache_key, items)
    logger.info("stac_search_ok", lat=lat, lng=lng, items=len(items),
                years=[i["year"] for i in items])

    if years:
        items = [i for i in items if i["year"] in years]
    return items


def read_ndvi_from_cog(lat: float, lng: float, cog_url: str,
                       window_size: int = 3) -> dict:
    """
    Read NDVI at a point from a NAIP COG via HTTP range request.

    Uses a small averaging window (default 3x3 pixels = ~2m at 0.6m GSD)
    to reduce noise from single-pixel alignment hitting driveways/shadows.

    NAIP bands: 1=Red, 2=Green, 3=Blue, 4=NIR
    NDVI = (NIR - Red) / (NIR + Red)

    Returns {"ndvi": float|None, "red": float, "nir": float, "error": str|None}
    """
    import numpy as np

    half = window_size // 2
    try:
        with rasterio.open(cog_url) as src:
            transformer = _get_transformer(src.crs)
            x, y = transformer.transform(lng, lat)
            row, col = src.index(x, y)

            # Clamp window to image bounds
            r_start = max(0, row - half)
            c_start = max(0, col - half)
            r_end = min(src.height, row + half + 1)
            c_end = min(src.width, col + half + 1)

            if r_start >= r_end or c_start >= c_end:
                return {"ndvi": None, "red": None, "nir": None,
                        "error": "pixel_out_of_bounds"}

            window = Window(c_start, r_start, c_end - c_start, r_end - r_start)
            bands = src.read(window=window)

            if bands.shape[0] < 4:
                return {"ndvi": None, "red": None, "nir": None,
                        "error": f"insufficient_bands: {bands.shape[0]}"}

            red = bands[0].astype(np.float64)
            nir = bands[3].astype(np.float64)
            denom = nir + red
            # Compute per-pixel NDVI, then average (avoids division artifacts)
            valid = denom > 0
            if not valid.any():
                return {"ndvi": 0.0, "red": float(red.mean()), "nir": float(nir.mean()),
                        "error": None}

            ndvi_pixels = np.where(valid, (nir - red) / denom, 0.0)
            ndvi = float(ndvi_pixels[valid].mean())

            return {"ndvi": round(ndvi, 4), "red": float(red.mean()),
                    "nir": float(nir.mean()), "error": None}

    except Exception as e:
        logger.warning("cog_read_failed", cog_url=cog_url[-60:], error=str(e))
        return {"ndvi": None, "red": None, "nir": None, "error": str(e)}


def get_historical_ndvi(lat: float, lng: float,
                        years: list[int] = None) -> list[dict]:
    """
    Get NDVI for multiple historical NAIP years at a point.

    Uses Planetary Computer STAC search + COG reads.
    Results cached per (lat, lng, year) for 7 days.

    Returns: [{"year": 2020, "ndvi": 0.32, "date": "2020-07-12"}, ...]
    """
    if years is None:
        years = [2022, 2020, 2018, 2016, 2014, 2012]

    # Search STAC for available items
    items = search_naip_items(lat, lng, years=years)
    if not items:
        return []

    results = []
    for item in items:
        year = item["year"]
        cog_url = item["cog_url"]
        date = item["date"]

        # Check per-year cache
        ck = _cache_key("ndvi_pc", {"lat": lat, "lng": lng, "year": year})
        cached = _get_cached(ck)
        if cached and cached.get("ndvi") is not None:
            results.append(cached)
            continue

        # Read from COG
        pixel = read_ndvi_from_cog(lat, lng, cog_url)

        if pixel["ndvi"] is not None:
            entry = {
                "year": year,
                "ndvi": pixel["ndvi"],
                "date": date,
            }
            _set_cache(ck, entry)
            results.append(entry)
            logger.debug("pc_ndvi_read", year=year, ndvi=pixel["ndvi"],
                         lat=lat, lng=lng)
        else:
            # Cache the miss too to avoid re-reading a bad pixel
            miss = {"year": year, "ndvi": None, "date": date,
                    "error": pixel.get("error")}
            _set_cache(ck, miss)
            logger.debug("pc_ndvi_miss", year=year, error=pixel.get("error"),
                         lat=lat, lng=lng)

    return results
