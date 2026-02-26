"""
Landsat 8/9 NDVI via USGS/Esri ArcGIS Image Services.

Free, unlimited, no auth. 30m resolution, 16-day revisit.
Backup for Sentinel-2 when CDSE rate limits or service is down.

Service: Esri Living Atlas Landsat Multispectral Image Server
Same REST pattern as our NAIP client — identify endpoint returns band values.

Landsat 8/9 bands:
  Band 4 = Red (0.64-0.67 μm)
  Band 5 = NIR (0.85-0.88 μm)
  NDVI = (B5 - B4) / (B5 + B4)
"""

import math
from datetime import datetime, timedelta

import requests
import structlog

logger = structlog.get_logger("landsat.client")

SERVICE_URL = "https://landsat2.arcgis.com/arcgis/rest/services/Landsat/MS/ImageServer"


class LandsatClient:
    """Client for USGS/Esri Landsat ArcGIS Image Service."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "DistressScannerApp/1.0"

    def identify_at_point(self, lat: float, lng: float,
                          time_filter: str | None = None) -> dict:
        """
        Get band values at a point via identify endpoint.

        Args:
            lat, lng: coordinates
            time_filter: Unix ms range "start,end" or None for latest

        Returns dict with band values or error.
        """
        params = {
            "geometry": f'{{"x":{lng},"y":{lat},"spatialReference":{{"wkid":4326}}}}',
            "geometryType": "esriGeometryPoint",
            "returnGeometry": "false",
            "returnCatalogItems": "true",
            "f": "json",
        }

        if time_filter:
            params["time"] = time_filter

        # Use mosaic rule to get most recent image
        params["mosaicRule"] = (
            '{"mosaicMethod":"esriMosaicAttribute",'
            '"sortField":"AcquisitionDate",'
            '"sortValue":"2099-01-01",'
            '"ascending":false}'
        )

        try:
            resp = self.session.get(
                f"{SERVICE_URL}/identify",
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            return data
        except Exception as e:
            logger.error("landsat_identify_failed", error=str(e))
            return {"error": str(e)}

    def compute_ndvi_at_point(self, lat: float, lng: float,
                              time_filter: str | None = None) -> dict:
        """
        Compute NDVI from Landsat bands at a point.

        Returns:
            {"ndvi": float|None, "bands": dict, "acquisition_date": str, "error": str|None}
        """
        result = {
            "ndvi": None,
            "bands": {},
            "acquisition_date": None,
            "sensor": None,
            "error": None,
        }

        data = self.identify_at_point(lat, lng, time_filter=time_filter)

        if data.get("error"):
            result["error"] = data["error"]
            return result

        # Parse pixel value — Landsat service returns band values
        pixel_value = data.get("value")
        if not pixel_value or pixel_value == "NoData":
            result["error"] = "no_data_at_point"
            return result

        # The value comes as space-separated band values
        try:
            bands = pixel_value.split()
            if len(bands) >= 5:
                # Landsat 8/9: Band 4=Red, Band 5=NIR (0-indexed from service)
                red = float(bands[3])   # Band 4
                nir = float(bands[4])   # Band 5

                result["bands"] = {
                    "coastal": float(bands[0]) if len(bands) > 0 else None,
                    "blue": float(bands[1]) if len(bands) > 1 else None,
                    "green": float(bands[2]) if len(bands) > 2 else None,
                    "red": red,
                    "nir": nir,
                    "swir1": float(bands[5]) if len(bands) > 5 else None,
                    "swir2": float(bands[6]) if len(bands) > 6 else None,
                }

                if (nir + red) > 0:
                    ndvi = (nir - red) / (nir + red)
                    result["ndvi"] = round(ndvi, 4)
                else:
                    result["error"] = "zero_denominator"
        except (ValueError, IndexError) as e:
            result["error"] = f"band_parse: {e}"

        # Try to get acquisition date from catalog items
        catalog_items = data.get("catalogItems", {})
        features = catalog_items.get("features", [])
        if features:
            attrs = features[0].get("attributes", {})
            acq_date = attrs.get("AcquisitionDate")
            if acq_date:
                try:
                    # ArcGIS returns epoch ms
                    dt = datetime.fromtimestamp(acq_date / 1000)
                    result["acquisition_date"] = dt.strftime("%Y-%m-%d")
                except (ValueError, OSError):
                    pass
            result["sensor"] = attrs.get("SensorName")

        return result

    def get_monthly_ndvi(self, lat: float, lng: float,
                         months_back: int = 24) -> list[dict]:
        """
        Get NDVI values for monthly windows over the lookback period.

        Queries Landsat for each month, extracting the best available pixel.
        Returns list of {"month": "YYYY-MM", "ndvi": float, "date": str}.
        """
        results = []
        now = datetime.now()

        for i in range(months_back):
            # Work backwards from now
            target = now - timedelta(days=30 * i)
            month_start = target.replace(day=1, hour=0, minute=0, second=0)
            if month_start.month == 12:
                month_end = month_start.replace(year=month_start.year + 1, month=1)
            else:
                month_end = month_start.replace(month=month_start.month + 1)

            # ArcGIS time filter uses epoch milliseconds
            start_ms = int(month_start.timestamp() * 1000)
            end_ms = int(month_end.timestamp() * 1000)
            time_filter = f"{start_ms},{end_ms}"

            data = self.compute_ndvi_at_point(lat, lng, time_filter=time_filter)

            if data.get("ndvi") is not None:
                results.append({
                    "month": month_start.strftime("%Y-%m"),
                    "ndvi": data["ndvi"],
                    "date": data.get("acquisition_date", month_start.strftime("%Y-%m-15")),
                    "sensor": data.get("sensor"),
                })

        # Sort chronologically
        results.sort(key=lambda x: x["month"])
        logger.info("landsat_monthly_ok", lat=lat, lng=lng, months=len(results))
        return results


def landsat_trends(lat: float, lng: float, months: int = 24) -> dict:
    """
    Landsat NDVI trend analysis — drop-in backup for sentinel_trends().

    Same return shape so scanner can swap seamlessly.

    Returns:
        {
            "lat", "lng", "data_source": "Landsat",
            "months_requested", "months_with_data",
            "monthly_ndvi": [...],
            "latest_ndvi", "earliest_ndvi",
            "trend_direction", "trend_slope",
            "mean_ndvi", "errors": []
        }
    """
    client = LandsatClient()

    result = {
        "lat": lat,
        "lng": lng,
        "data_source": "Landsat",
        "resolution": "30m",
        "months_requested": months,
        "months_with_data": 0,
        "monthly_ndvi": [],
        "latest_ndvi": None,
        "earliest_ndvi": None,
        "trend_direction": "insufficient_data",
        "trend_slope": None,
        "mean_ndvi": None,
        "errors": [],
    }

    # Get current NDVI first (quick check)
    current = client.compute_ndvi_at_point(lat, lng)
    if current.get("error"):
        result["errors"].append(f"current: {current['error']}")
        if current["ndvi"] is None:
            return result

    # Get monthly time series
    try:
        monthly = client.get_monthly_ndvi(lat, lng, months_back=months)
        result["monthly_ndvi"] = monthly
        result["months_with_data"] = len(monthly)

        if monthly:
            ndvi_vals = [m["ndvi"] for m in monthly]
            result["latest_ndvi"] = monthly[-1]["ndvi"]
            result["earliest_ndvi"] = monthly[0]["ndvi"]
            result["mean_ndvi"] = round(sum(ndvi_vals) / len(ndvi_vals), 4)

            # Compute trend (same logic as sentinel)
            if len(ndvi_vals) >= 3:
                mid = len(ndvi_vals) // 2
                first_half = sum(ndvi_vals[:mid]) / mid
                second_half = sum(ndvi_vals[mid:]) / (len(ndvi_vals) - mid)
                diff = second_half - first_half

                if diff > 0.03:
                    result["trend_direction"] = "increasing"
                elif diff < -0.03:
                    result["trend_direction"] = "decreasing"
                else:
                    result["trend_direction"] = "stable"

                # Simple linear slope (NDVI/month)
                n = len(ndvi_vals)
                x_mean = (n - 1) / 2
                y_mean = sum(ndvi_vals) / n
                num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(ndvi_vals))
                den = sum((i - x_mean) ** 2 for i in range(n))
                if den > 0:
                    result["trend_slope"] = round(num / den, 6)

    except Exception as e:
        result["errors"].append(f"monthly: {e}")
        logger.error("landsat_trends_failed", error=str(e))

    logger.info("landsat_trends_complete",
                lat=lat, lng=lng,
                months_data=result["months_with_data"],
                trend=result["trend_direction"])
    return result
