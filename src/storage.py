"""
Storage abstraction layer for DistressScannerApp.

Supports two backends:
  - R2 (production): Cloudflare R2 via S3-compatible API
  - Local (dev fallback): data/ directory

All image/tile storage goes through this module. The evidence JSONB in
parcel_signals stores the returned URLs, not local file paths.

Path convention:
  {county}/{parcel_id}/{scan_date}/{type}.png
  e.g. mecklenburg_nc/12345/2026-02-21/naip_rgb.png
"""

import os
import io
from pathlib import Path
from datetime import date

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import structlog

load_dotenv()

logger = structlog.get_logger("storage")


def _get_r2_client():
    """Create boto3 S3 client configured for Cloudflare R2."""
    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not all([account_id, access_key, secret_key]):
        return None

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
        region_name="auto",
    )


# Module-level lazy singleton
_r2_client = None


def get_r2_client():
    """Get or create the R2 client singleton."""
    global _r2_client
    if _r2_client is None:
        _r2_client = _get_r2_client()
    return _r2_client


def _get_bucket():
    return os.environ.get("R2_BUCKET_NAME", "distress-scanner")


def _get_public_url():
    return os.environ.get("R2_PUBLIC_URL", "").rstrip("/")


def _use_r2() -> bool:
    """Check if R2 credentials are configured."""
    return bool(
        os.environ.get("R2_ACCOUNT_ID")
        and os.environ.get("R2_ACCESS_KEY_ID")
        and os.environ.get("R2_SECRET_ACCESS_KEY")
    )


def make_key(county: str, state_code: str, parcel_id: str,
             scan_date: str | date | None, filename: str) -> str:
    """
    Build a storage key (path) for an image.

    Returns: "mecklenburg_nc/12345/2026-02-21/naip_rgb.png"
    """
    county_slug = f"{county.lower().replace(' ', '_')}_{state_code.lower()}"
    parcel_slug = str(parcel_id).replace("/", "_").replace(" ", "_")
    if scan_date is None:
        scan_date = date.today()
    date_str = str(scan_date)
    return f"{county_slug}/{parcel_slug}/{date_str}/{filename}"


def make_point_key(lat: float, lng: float, filename: str) -> str:
    """
    Build a storage key for a point-based scan (no parcel context yet).

    Returns: "points/35.2271_-80.8431/2026-02-21/naip_rgb.png"
    """
    point_slug = f"{lat:.4f}_{lng:.4f}"
    date_str = str(date.today())
    return f"points/{point_slug}/{date_str}/{filename}"


def upload_bytes(key: str, data: bytes, content_type: str = "image/png") -> str | None:
    """
    Upload bytes to storage. Returns the public URL or local path.

    Uses R2 if configured, falls back to local data/ directory.
    """
    if _use_r2():
        return _upload_r2(key, data, content_type)
    else:
        return _upload_local(key, data)


def _upload_r2(key: str, data: bytes, content_type: str) -> str | None:
    """Upload to Cloudflare R2, return public URL."""
    client = get_r2_client()
    if client is None:
        logger.error("r2_client_not_available")
        return _upload_local(key, data)

    bucket = _get_bucket()
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        public_url = _get_public_url()
        if public_url:
            url = f"{public_url}/{key}"
        else:
            url = f"r2://{bucket}/{key}"

        logger.info("r2_upload_ok", key=key, size=len(data))
        return url

    except ClientError as e:
        logger.error("r2_upload_failed", key=key, error=str(e))
        return _upload_local(key, data)


def _upload_local(key: str, data: bytes) -> str:
    """Fallback: save to local data/ directory."""
    local_path = Path("data") / key
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(data)
    logger.info("local_upload", path=str(local_path), size=len(data))
    return str(local_path)


def download_bytes(key: str) -> bytes | None:
    """Download bytes from storage by key."""
    if _use_r2():
        return _download_r2(key)
    else:
        return _download_local(key)


def _download_r2(key: str) -> bytes | None:
    client = get_r2_client()
    if client is None:
        return _download_local(key)
    try:
        resp = client.get_object(Bucket=_get_bucket(), Key=key)
        return resp["Body"].read()
    except ClientError:
        return None


def _download_local(key: str) -> bytes | None:
    local_path = Path("data") / key
    if local_path.exists():
        return local_path.read_bytes()
    return None


def file_exists(key: str) -> bool:
    """Check if a file exists in storage."""
    if _use_r2():
        client = get_r2_client()
        if client is None:
            return Path("data", key).exists()
        try:
            client.head_object(Bucket=_get_bucket(), Key=key)
            return True
        except ClientError:
            return False
    else:
        return Path("data", key).exists()


def get_url(key: str) -> str:
    """Get the public URL for a stored file."""
    public_url = _get_public_url()
    if public_url and _use_r2():
        return f"{public_url}/{key}"
    return str(Path("data") / key)
