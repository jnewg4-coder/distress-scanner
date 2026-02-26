"""
Sentinel-2 monthly NDVI time-series analysis.

Computes monthly NDVI trends over a configurable time window (default 12 months),
generates matplotlib trend charts, and uploads all artifacts to R2.
"""

import math
from datetime import datetime, timedelta
from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from sentinelhub import BBox, CRS
import structlog

from src.sentinel.client import SentinelClient
from src.storage import upload_bytes, make_point_key

logger = structlog.get_logger("sentinel.trends")


def _make_bbox(lat: float, lng: float, buffer_meters: float = 50.0) -> BBox:
    """Create a sentinelhub BBox around a point."""
    lat_offset = buffer_meters / 111_000
    lng_offset = buffer_meters / (111_000 * math.cos(math.radians(lat)))
    return BBox(
        bbox=[lng - lng_offset, lat - lat_offset, lng + lng_offset, lat + lat_offset],
        crs=CRS.WGS84,
    )


def _compute_linear_trend(months: list[str], values: list[float]) -> dict:
    """Compute linear regression on monthly NDVI values."""
    if len(values) < 3:
        return {"slope": None, "direction": "insufficient_data"}

    x = np.arange(len(values), dtype=float)
    y = np.array(values, dtype=float)

    # Simple linear regression
    coeffs = np.polyfit(x, y, 1)
    slope = float(coeffs[0])

    if slope > 0.005:
        direction = "increasing"
    elif slope < -0.005:
        direction = "decreasing"
    else:
        direction = "stable"

    return {"slope": round(slope, 6), "direction": direction}


def _generate_trend_chart(monthly_data: list[dict], lat: float, lng: float,
                          trend_slope: float | None) -> bytes:
    """Generate matplotlib NDVI trend chart, return PNG bytes."""
    months = [d["month"] for d in monthly_data]
    means = [d["mean_ndvi"] for d in monthly_data]
    stds = [d.get("std_ndvi", 0) or 0 for d in monthly_data]

    # Parse month strings to dates for x-axis
    dates = [datetime.strptime(m + "-15", "%Y-%m-%d") for m in months]

    fig, ax = plt.subplots(figsize=(10, 5))

    # NDVI points and error band
    ax.plot(dates, means, "o-", color="#2e7d32", linewidth=2, markersize=6, label="Monthly NDVI")
    ax.fill_between(
        dates,
        [m - s for m, s in zip(means, stds)],
        [m + s for m, s in zip(means, stds)],
        alpha=0.2, color="#66bb6a", label="+-1 std",
    )

    # Trend line
    if trend_slope is not None and len(means) >= 3:
        x = np.arange(len(means))
        coeffs = np.polyfit(x, means, 1)
        trend_y = np.polyval(coeffs, x)
        ax.plot(dates, trend_y, "--", color="#d32f2f", linewidth=1.5,
                label=f"Trend ({trend_slope:+.4f}/mo)")

    # Formatting
    ax.set_ylabel("NDVI", fontsize=12)
    ax.set_title(f"NDVI Trend — ({lat:.4f}, {lng:.4f})", fontsize=13)
    ax.set_ylim(-0.1, 1.0)
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120)
    plt.close(fig)
    return buf.getvalue()


def sentinel_trends(lat: float, lng: float, months: int = 12,
                     include_chart: bool = True, include_rgb: bool = True) -> dict:
    """
    Compute monthly NDVI trends using Sentinel-2 data.

    Args:
        lat, lng: property coordinates
        months: number of months to analyze (default 12)
        include_chart: generate and upload matplotlib trend chart (no CDSE cost, R2 upload)
        include_rgb: download latest RGB image from CDSE (costs 1 extra CDSE request)

    Returns dict with monthly NDVI, trend analysis, chart URL, etc.
    """
    result = {
        "lat": lat,
        "lng": lng,
        "months_requested": months,
        "months_with_data": 0,
        "monthly_ndvi": [],
        "trend_slope": None,
        "trend_direction": "insufficient_data",
        "chart_url": None,
        "latest_ndvi": None,
        "earliest_ndvi": None,
        "mean_ndvi": None,
        "ndvi_range": None,
        "data_source": "Sentinel-2",
        "errors": [],
    }

    bbox = _make_bbox(lat, lng)

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")

    # Get monthly NDVI stats
    try:
        client = SentinelClient()
        monthly_data = client.get_monthly_ndvi(bbox, start_date, end_date)
    except EnvironmentError as e:
        result["errors"].append(str(e))
        return result
    except Exception as e:
        result["errors"].append(f"sentinel_api_error: {e}")
        logger.error("sentinel_trends_failed", error=str(e))
        return result

    if not monthly_data:
        result["errors"].append("no_sentinel_data_available")
        return result

    result["monthly_ndvi"] = monthly_data
    result["months_with_data"] = len(monthly_data)

    # Extract key stats
    valid_means = [d["mean_ndvi"] for d in monthly_data if d["mean_ndvi"] is not None]
    if valid_means:
        result["latest_ndvi"] = valid_means[-1]
        result["earliest_ndvi"] = valid_means[0]
        result["mean_ndvi"] = round(sum(valid_means) / len(valid_means), 4)
        result["ndvi_range"] = round(max(valid_means) - min(valid_means), 4)

    # Compute trend
    valid_months = [d["month"] for d in monthly_data if d["mean_ndvi"] is not None]
    trend = _compute_linear_trend(valid_months, valid_means)
    result["trend_slope"] = trend["slope"]
    result["trend_direction"] = trend["direction"]

    # Generate and upload trend chart (local matplotlib, no CDSE cost)
    if include_chart:
        try:
            chart_bytes = _generate_trend_chart(monthly_data, lat, lng, trend["slope"])
            key = make_point_key(lat, lng, "sentinel_ndvi_trend.png")
            url = upload_bytes(key, chart_bytes)
            result["chart_url"] = url
        except Exception as e:
            result["errors"].append(f"chart_generation: {e}")
            logger.warning("chart_generation_failed", error=str(e))

    # Upload latest RGB image (costs 1 extra CDSE request)
    if include_rgb:
        try:
            if monthly_data:
                latest_month = monthly_data[-1]["month"]
                latest_date = f"{latest_month}-15"
                rgb_bytes = client.get_rgb_image(bbox, latest_date)
                if rgb_bytes:
                    key = make_point_key(lat, lng, "sentinel_rgb_latest.png")
                    url = upload_bytes(key, rgb_bytes)
                    result["rgb_url"] = url
        except Exception as e:
            result["errors"].append(f"rgb_export: {e}")
            logger.warning("rgb_export_failed", error=str(e))

    logger.info("sentinel_trends_complete",
                lat=lat, lng=lng,
                months_data=result["months_with_data"],
                trend=result["trend_direction"],
                slope=result["trend_slope"])

    return result
