"""
Planet Labs Data API v2 client.

Searches PlanetScope archive (3-5m daily) for property-level imagery.
Pass 2 paid refinement: roof condition, gutters, debris, fine-grain overgrowth.

API: https://api.planet.com/data/v2/
Auth: API key from Planet_API env var.
Trial: 30K requests, 30 days.

Conservative usage — each search = 1 request, each thumbnail = 1 request.
For 10 test properties: ~30 requests total.
"""

import os
from datetime import datetime, timedelta

import requests
import structlog

from src.storage import upload_bytes, make_point_key

logger = structlog.get_logger("planet.client")

DATA_API = "https://api.planet.com/data/v1"
TILES_API = "https://tiles.planet.com/data/v1"


class PlanetClient:
    """Client for Planet Labs Data API v2."""

    def __init__(self):
        self.api_key = os.environ.get("Planet_API", "")
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({
                "Authorization": f"api-key {self.api_key}",
                "Content-Type": "application/json",
            })

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def search_scenes(self, lat: float, lng: float,
                      months_back: int = 18,
                      cloud_cover_max: float = 0.30,
                      limit: int = 10,
                      start_date: datetime | None = None,
                      end_date: datetime | None = None) -> dict:
        """
        Search for PlanetScope scenes at a point.

        Args:
            start_date/end_date: Explicit date window. If not provided,
                uses now minus months_back.

        Returns:
            {
                "available": bool,
                "scene_count": int,
                "scenes": [{"id", "acquired", "cloud_cover", "pixel_res", "quality"}],
                "date_range": {"earliest", "latest"},
                "clear_scenes": int,
                "errors": []
            }
        """
        result = {
            "available": False,
            "scene_count": 0,
            "scenes": [],
            "date_range": {},
            "clear_scenes": 0,
            "errors": [],
        }

        if not self.available:
            result["errors"].append("Planet API key not configured")
            return result

        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=months_back * 30)

        body = {
            "item_types": ["PSScene"],
            "filter": {
                "type": "AndFilter",
                "config": [
                    {
                        "type": "GeometryFilter",
                        "field_name": "geometry",
                        "config": {
                            "type": "Point",
                            "coordinates": [lng, lat],
                        },
                    },
                    {
                        "type": "DateRangeFilter",
                        "field_name": "acquired",
                        "config": {
                            "gte": start_date.strftime("%Y-%m-%dT00:00:00Z"),
                            "lte": end_date.strftime("%Y-%m-%dT23:59:59Z"),
                        },
                    },
                    {
                        "type": "RangeFilter",
                        "field_name": "cloud_cover",
                        "config": {"lte": cloud_cover_max},
                    },
                ],
            },
        }

        try:
            resp = self.session.post(
                f"{DATA_API}/quick-search",
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            pu = resp.headers.get("x-processunits")
            if pu:
                logger.info("planet_pu_search", process_units=pu)
            data = resp.json()

            features = data.get("features", [])[:limit]
            result["available"] = True
            result["scene_count"] = len(features)
            result["clear_scenes"] = len(features)

            for feat in features:
                props = feat.get("properties", {})
                result["scenes"].append({
                    "id": feat.get("id"),
                    "acquired": props.get("acquired"),
                    "cloud_cover": props.get("cloud_cover"),
                    "pixel_res": props.get("pixel_resolution"),
                    "quality": props.get("quality_category"),
                    "sun_elevation": props.get("sun_elevation"),
                    "view_angle": props.get("view_angle"),
                    "item_type": feat.get("properties", {}).get("item_type", "PSScene"),
                })

            if features:
                dates = [f["properties"]["acquired"] for f in features
                         if f.get("properties", {}).get("acquired")]
                if dates:
                    result["date_range"] = {
                        "earliest": min(dates),
                        "latest": max(dates),
                    }

            logger.info("planet_search_ok",
                        lat=lat, lng=lng,
                        scenes=result["scene_count"])

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "unknown"
            result["errors"].append(f"Planet API {status}: {e}")
            logger.error("planet_search_failed", status=status, error=str(e))
        except Exception as e:
            result["errors"].append(f"planet_search: {e}")
            logger.error("planet_search_error", error=str(e))

        return result

    def get_thumbnail(self, item_id: str,
                      item_type: str = "PSScene") -> bytes | None:
        """
        Download scene thumbnail (256x256 PNG).
        Costs 1 API request.
        """
        if not self.available:
            return None

        url = f"{TILES_API}/item-types/{item_type}/items/{item_id}/thumb"
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            pu = resp.headers.get("x-processunits")
            if pu:
                logger.info("planet_pu_thumb", item_id=item_id, process_units=pu)
            if resp.headers.get("content-type", "").startswith("image"):
                logger.info("planet_thumb_ok", item_id=item_id, size=len(resp.content))
                return resp.content
        except Exception as e:
            logger.warning("planet_thumb_failed", item_id=item_id, error=str(e))
        return None

    def get_scene_metadata(self, item_id: str,
                           item_type: str = "PSScene") -> dict | None:
        """Get full metadata for a scene."""
        if not self.available:
            return None

        url = f"{DATA_API}/item-types/{item_type}/items/{item_id}"
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning("planet_metadata_failed", item_id=item_id, error=str(e))
            return None


def planet_search(lat: float, lng: float, months_back: int = 18) -> dict:
    """
    High-level: search Planet scenes at a point and return summary.

    Returns dict with scene availability, counts, date range.
    If no API key, returns upgrade_required status.
    """
    client = PlanetClient()

    if not client.available:
        return {
            "status": "upgrade_required",
            "message": "Planet imagery available with API key. "
                       "Sign up at planet.com for 3m daily imagery.",
            "data_source": "Planet",
        }

    search = client.search_scenes(lat, lng, months_back=months_back)

    return {
        "status": "ok" if search["available"] else "no_data",
        "data_source": "Planet",
        "scene_count": search["scene_count"],
        "clear_scenes": search["clear_scenes"],
        "date_range": search["date_range"],
        "scenes": search["scenes"][:5],  # Top 5 for summary
        "errors": search["errors"],
    }


def _parse_acquired(scene: dict) -> datetime | None:
    """Parse acquired date from scene dict, return None on failure."""
    acq = scene.get("acquired", "")
    if not acq or len(acq) < 10:
        return None
    try:
        return datetime.strptime(acq[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _thumbnail_mean_brightness(thumb_bytes: bytes) -> float | None:
    """Compute mean pixel brightness from a PNG thumbnail (0-255 scale).

    Decodes the PNG to raw pixels via PIL if available, otherwise returns None.
    """
    if not thumb_bytes or len(thumb_bytes) < 100:
        return None
    try:
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(thumb_bytes)).convert("L")  # grayscale
        pixels = list(img.getdata())
        return sum(pixels) / len(pixels) if pixels else None
    except ImportError:
        # PIL not installed — skip change_score rather than fake it
        logger.debug("pillow_not_installed", msg="change_score unavailable without Pillow")
        return None
    except Exception as e:
        logger.debug("thumbnail_brightness_error", error=str(e))
        return None


MIN_TEMPORAL_SPAN_DAYS = 180   # Minimum 6-month gap
MAX_TEMPORAL_SPAN_DAYS = 365   # Cap at 12 months for relevance


def planet_refine(lat: float, lng: float, months_back: int = 18) -> dict:
    """
    Planet refinement: multi-temporal thumbnail comparison.

    Downloads thumbnails of earliest + latest clear scenes (6+ months apart)
    for visual change comparison in dashboard. Computes a rough change_score
    from thumbnail brightness difference.

    Budget: 4 API calls per parcel (2 searches + 2 thumbs).
    At 5K flagged parcels = 20K of 30K trial budget.
    """
    client = PlanetClient()

    if not client.available:
        return {
            "status": "upgrade_required",
            "message": "Planet 3m imagery: visual change detection. "
                       "Add Planet_API key to enable.",
            "data_source": "Planet",
        }

    # Two targeted searches: recent (latest scene) + historical (6-12mo window)
    search_recent = client.search_scenes(lat, lng, months_back=1, limit=5)

    result = {
        "status": "ok" if search_recent["available"] else "no_data",
        "data_source": "Planet",
        "scene_count": search_recent["scene_count"],
        "date_range": search_recent["date_range"],
        "latest_scene": None,
        "earliest_scene": None,
        "thumbnail_latest_url": None,
        "thumbnail_earliest_url": None,
        "thumbnail_url": None,  # backwards compat — latest thumb
        "temporal_span_days": None,
        "change_score": None,
        "errors": search_recent["errors"],
    }

    recent_scenes = search_recent["scenes"]
    if not recent_scenes:
        return result

    # Pick latest scene from recent search
    recent_with_dates = [(s, _parse_acquired(s)) for s in recent_scenes]
    recent_with_dates = [(s, d) for s, d in recent_with_dates if d is not None]
    recent_with_dates.sort(key=lambda x: x[1], reverse=True)

    if not recent_with_dates:
        return result

    latest, latest_dt = recent_with_dates[0]
    result["latest_scene"] = latest

    # Second search: explicitly target 6-12 months before latest scene
    hist_end = latest_dt - timedelta(days=MIN_TEMPORAL_SPAN_DAYS)
    hist_start = latest_dt - timedelta(days=MAX_TEMPORAL_SPAN_DAYS)
    search_historical = client.search_scenes(
        lat, lng, limit=5,
        start_date=hist_start,
        end_date=hist_end,
        cloud_cover_max=0.20,  # stricter for comparison baseline
    )
    historical_scenes = search_historical["scenes"]
    result["scene_count"] += search_historical["scene_count"]

    # Find best earliest scene from historical search
    hist_with_dates = [(s, _parse_acquired(s)) for s in historical_scenes]
    hist_with_dates = [(s, d) for s, d in hist_with_dates if d is not None]

    earliest = None
    earliest_dt = None
    for scene, scene_dt in sorted(hist_with_dates, key=lambda x: x[1]):
        span = (latest_dt - scene_dt).days
        if span > MAX_TEMPORAL_SPAN_DAYS:
            continue
        if span >= MIN_TEMPORAL_SPAN_DAYS:
            earliest = scene
            earliest_dt = scene_dt
            break

    if earliest:
        result["earliest_scene"] = earliest
        result["temporal_span_days"] = (latest_dt - earliest_dt).days

    # Fix date_range to reflect actual selected scenes (not just recent search)
    result["date_range"] = {}
    if latest:
        result["date_range"]["latest"] = (latest.get("acquired") or "")[:10]
    if earliest:
        result["date_range"]["earliest"] = (earliest.get("acquired") or "")[:10]
    elif latest:
        result["date_range"]["earliest"] = result["date_range"].get("latest")

    # Download thumbnails and compute change_score
    latest_thumb = client.get_thumbnail(latest["id"],
                                         item_type=latest.get("item_type", "PSScene"))
    latest_brightness = None
    if latest_thumb:
        date_label = (latest.get("acquired") or "latest")[:10]
        key = make_point_key(lat, lng, f"planet_latest_{date_label}.png")
        url = upload_bytes(key, latest_thumb)
        result["thumbnail_latest_url"] = url
        result["thumbnail_url"] = url  # backwards compat
        latest_brightness = _thumbnail_mean_brightness(latest_thumb)

    earliest_brightness = None
    if earliest and earliest["id"] != latest["id"]:
        earliest_thumb = client.get_thumbnail(earliest["id"],
                                               item_type=earliest.get("item_type", "PSScene"))
        if earliest_thumb:
            date_label = (earliest.get("acquired") or "earliest")[:10]
            key = make_point_key(lat, lng, f"planet_earliest_{date_label}.png")
            url = upload_bytes(key, earliest_thumb)
            result["thumbnail_earliest_url"] = url
            earliest_brightness = _thumbnail_mean_brightness(earliest_thumb)

    # Compute change_score from brightness difference (0.0 = no change, 1.0 = max)
    if latest_brightness is not None and earliest_brightness is not None:
        diff = abs(latest_brightness - earliest_brightness)
        # Normalize: 20+ point brightness diff on 0-255 scale = high change
        result["change_score"] = round(min(diff / 20.0, 1.0), 3)

    logger.info("planet_refine_complete",
                lat=lat, lng=lng,
                scenes=result["scene_count"],
                has_earliest=earliest is not None,
                span_days=result["temporal_span_days"],
                change_score=result["change_score"])

    return result
