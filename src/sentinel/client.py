"""
Sentinel Hub client for Copernicus Data Space Ecosystem (CDSE).

Provides monthly NDVI time-series and RGB imagery via:
  - Statistical API: monthly aggregated NDVI stats
  - Process API: RGB/NDVI image exports

Uses sentinelhub-py library with OAuth credentials from .env:
  SH_CLIENT_ID, SH_CLIENT_SECRET

CDSE endpoints:
  Base: https://sh.dataspace.copernicus.eu
  Token: https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
"""

import os
from datetime import datetime, timedelta

from sentinelhub import (
    SHConfig,
    SentinelHubStatistical,
    SentinelHubRequest,
    BBox,
    CRS,
    DataCollection,
    MimeType,
    SentinelHubStatisticalDownloadClient,
)
import structlog

from src.storage import upload_bytes, make_point_key

logger = structlog.get_logger("sentinel.client")

# Evalscript: compute NDVI from Sentinel-2 bands
NDVI_EVALSCRIPT = """
//VERSION=3
function setup() {
  return {
    input: [{bands: ["B04", "B08", "dataMask"]}],
    output: [
      {id: "ndvi", bands: 1, sampleType: "FLOAT32"},
      {id: "dataMask", bands: 1}
    ]
  };
}
function evaluatePixel(sample) {
  if (sample.dataMask === 0) {
    return { ndvi: [0], dataMask: [0] };
  }
  let ndvi = (sample.B08 - sample.B04) / (sample.B08 + sample.B04);
  return { ndvi: [ndvi], dataMask: [1] };
}
"""

# Evalscript: true-color RGB
RGB_EVALSCRIPT = """
//VERSION=3
function setup() {
  return {
    input: [{bands: ["B04", "B03", "B02"], units: "DN"}],
    output: [{id: "default", bands: 3, sampleType: "AUTO"}]
  };
}
function evaluatePixel(sample) {
  return [2.5 * sample.B04 / 10000, 2.5 * sample.B03 / 10000, 2.5 * sample.B02 / 10000];
}
"""


def _get_config() -> SHConfig:
    """Build SHConfig for Copernicus Data Space Ecosystem."""
    config = SHConfig()
    config.sh_client_id = os.environ.get("SH_CLIENT_ID", "")
    config.sh_client_secret = os.environ.get("SH_CLIENT_SECRET", "")
    config.sh_base_url = "https://sh.dataspace.copernicus.eu"
    config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

    if not config.sh_client_id or not config.sh_client_secret:
        raise EnvironmentError(
            "SH_CLIENT_ID and SH_CLIENT_SECRET must be set in .env. "
            "Register at https://dataspace.copernicus.eu and create an OAuth client."
        )
    return config


class SentinelClient:
    """Client for Sentinel Hub on Copernicus Data Space Ecosystem."""

    def __init__(self):
        self.config = _get_config()
        logger.info("sentinel_client_init", base_url=self.config.sh_base_url)

    def get_monthly_ndvi(self, bbox: BBox, start_date: str, end_date: str) -> list[dict]:
        """
        Get monthly average NDVI using the Statistical API.

        Args:
            bbox: BBox object in CRS.WGS84
            start_date: "YYYY-MM-DD"
            end_date: "YYYY-MM-DD"

        Returns list of:
            {"month": "2025-01", "mean_ndvi": 0.45, "std_ndvi": 0.08,
             "valid_pixels": 1024, "cloud_pct": 12.3}
        """
        aggregation = SentinelHubStatistical.aggregation(
            evalscript=NDVI_EVALSCRIPT,
            time_interval=(start_date, end_date),
            aggregation_interval="P1M",
            size=(50, 50),
        )

        input_data = SentinelHubStatistical.input_data(
            DataCollection.SENTINEL2_L2A.define_from(
                "s2l2a", service_url=self.config.sh_base_url
            ),
            maxcc=0.5,
        )

        request = SentinelHubStatistical(
            aggregation=aggregation,
            input_data=[input_data],
            bbox=bbox,
            config=self.config,
        )

        logger.info("sentinel_stats_request", start=start_date, end=end_date)
        response = request.get_data()

        results = []
        if not response or not response[0].get("data"):
            logger.warning("sentinel_stats_empty")
            return results

        for interval in response[0]["data"]:
            time_range = interval.get("interval", {})
            from_date = time_range.get("from", "")[:7]  # "YYYY-MM"

            stats = interval.get("outputs", {}).get("ndvi", {}).get("bands", {}).get("B0", {}).get("stats", {})
            no_data_count = stats.get("noDataCount", 0)
            sample_count = stats.get("sampleCount", 1)
            valid_count = sample_count - no_data_count

            if valid_count <= 0:
                continue

            results.append({
                "month": from_date,
                "mean_ndvi": stats.get("mean"),
                "std_ndvi": stats.get("stDev"),
                "min_ndvi": stats.get("min"),
                "max_ndvi": stats.get("max"),
                "valid_pixels": valid_count,
                "cloud_pct": round((no_data_count / max(sample_count, 1)) * 100, 1),
            })

        logger.info("sentinel_stats_ok", months=len(results))
        return results

    def get_rgb_image(self, bbox: BBox, date: str,
                      size: tuple = (512, 512)) -> bytes | None:
        """
        Get true-color RGB PNG for a specific date.

        Args:
            bbox: BBox in CRS.WGS84
            date: "YYYY-MM-DD" (searches +-5 days for best image)
            size: (width, height) pixels

        Returns PNG bytes or None.
        """
        dt = datetime.strptime(date, "%Y-%m-%d")
        time_interval = (
            (dt - timedelta(days=5)).strftime("%Y-%m-%d"),
            (dt + timedelta(days=5)).strftime("%Y-%m-%d"),
        )

        request = SentinelHubRequest(
            evalscript=RGB_EVALSCRIPT,
            input_data=[
                SentinelHubRequest.input_data(
                    data_collection=DataCollection.SENTINEL2_L2A.define_from(
                        "s2l2a", service_url=self.config.sh_base_url
                    ),
                    time_interval=time_interval,
                    maxcc=0.3,
                ),
            ],
            responses=[SentinelHubRequest.output_response("default", MimeType.PNG)],
            bbox=bbox,
            size=size,
            config=self.config,
        )

        logger.info("sentinel_rgb_request", date=date)
        data = request.get_data()

        if data and len(data) > 0:
            # sentinelhub returns numpy arrays for images
            import numpy as np
            from io import BytesIO
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            img = data[0]
            if isinstance(img, np.ndarray):
                fig, ax = plt.subplots(1, 1, figsize=(5, 5))
                ax.imshow(img)
                ax.axis("off")
                buf = BytesIO()
                fig.savefig(buf, format="png", bbox_inches="tight", pad_inches=0, dpi=100)
                plt.close(fig)
                return buf.getvalue()

        logger.warning("sentinel_rgb_empty", date=date)
        return None
