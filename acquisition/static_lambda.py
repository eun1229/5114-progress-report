"""
MBTA GTFS static schedule collector.

Downloads the MBTA GTFS ZIP only when it has been updated, using
Last-Modified / If-Modified-Since per MBTA developer docs:
https://www.mbta.com/developers/gtfs

Credits: GPT 5.3 with supervision and modifications by group members
"""

import io
import json
import os
import urllib.request
import zipfile
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3

MBTA_GTFS_ZIP_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"

# GTFS files to extract and upload (for delay calculations, route/stop metadata).
# Only extract files that are relevant to our project (i.e. files like fare_media, shapes are omitted).
GTFS_FILES_TO_EXTRACT = [
    "agency.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "stops.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "feed_info.txt",
]

META_KEY = "meta/last_check.json"  # contains metadata on when the last update was collected
USER_AGENT = "MBTA-GTFS-Static-Collector/1.0"

# Same timezone as realtime collector: Boston/MBTA (ET).
TIMEZONE_ET = ZoneInfo("America/New_York")


def get_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def get_s3_meta(bucket: str, prefix: str, s3_client) -> dict:
    """Read gtfs_static/meta/last_check.json. Return {} if missing or invalid."""
    key = f"{prefix}/gtfs_static/{META_KEY}".lstrip("/") if prefix else f"gtfs_static/{META_KEY}"
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read().decode("utf-8")
        return json.loads(body)
    except (s3_client.exceptions.NoSuchKey, KeyError, json.JSONDecodeError):
        return {}


def put_s3_meta(bucket: str, prefix: str, meta: dict, s3_client) -> None:
    key = f"{prefix}/gtfs_static/{META_KEY}".lstrip("/") if prefix else f"gtfs_static/{META_KEY}"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(meta, indent=2),
        ContentType="application/json",
    )


def build_version_prefix(now: datetime) -> str:
    """e.g. v_20260227_120000 (ET, same as realtime collector)."""
    return now.strftime("v_%Y%m%d_%H%M%S")


def conditional_fetch(zip_url: str, last_modified: str | None) -> tuple[int, bytes | None, str | None]:
    """
    GET zip_url with If-Modified-Since if last_modified is set.
    Returns (status_code, body_or_none, last_modified_from_response).
    """
    headers = {"User-Agent": USER_AGENT}
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    req = urllib.request.Request(zip_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            status = getattr(resp, "status", 200)
            body = resp.read()
            new_lm = resp.headers.get("Last-Modified")
            return (status, body, new_lm)
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return (304, None, None)
        raise


def upload_gtfs_files(
    bucket: str,
    prefix: str,
    version_prefix: str,
    zip_bytes: bytes,
    s3_client,
) -> list[str]:
    """Extract selected files from zip_bytes and upload to S3. Returns list of keys uploaded."""
    base = f"{prefix}/gtfs_static/{version_prefix}".lstrip("/") if prefix else f"gtfs_static/{version_prefix}"
    uploaded = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        for name in GTFS_FILES_TO_EXTRACT:
            try:
                data = zf.read(name)
            except KeyError:
                # in the case where some files are missing
                continue
            key = f"{base}/{name}"
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                ContentType="text/plain; charset=utf-8",
            )
            uploaded.append(key)
    return uploaded


def run_collector(
    bucket: str,
    prefix: str,
    zip_url: str = MBTA_GTFS_ZIP_URL,
) -> dict:
    """
    Check for GTFS static update; if changed, download and upload to S3.
    Returns a result dict for logging/Lambda response.
    """
    s3 = boto3.client("s3")
    meta = get_s3_meta(bucket, prefix, s3)
    last_modified = meta.get("last_modified")

    status, body, new_last_modified = conditional_fetch(zip_url, last_modified)

    if status == 304:
        return {
            "updated": False,
            "reason": "304 Not Modified",
            "last_version_prefix": meta.get("version_prefix"),
        }

    if status != 200 or body is None:
        return {
            "updated": False,
            "reason": f"unexpected status or empty body: {status}",
            "error": True,
        }

    # Use ET for version prefix and timestamps (same as realtime collector).
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(TIMEZONE_ET)
    version_prefix = build_version_prefix(now_et)
    uploaded = upload_gtfs_files(bucket, prefix, version_prefix, body, s3)

    new_meta = {
        "last_modified": new_last_modified or last_modified or "",
        "version_prefix": version_prefix,
        "downloaded_at": now_et.isoformat(),
        "downloaded_at_utc": now_utc.isoformat(),
    }
    put_s3_meta(bucket, prefix, new_meta, s3)

    return {
        "updated": True,
        "version_prefix": version_prefix,
        "files_uploaded": len(uploaded),
        "keys": uploaded,
        "last_modified": new_meta["last_modified"],
    }


def lambda_handler(event, context):
    """
    Lambda entry point. Uses S3_BUCKET and optional S3_PREFIX, GTFS_STATIC_ZIP_URL.
    """
    bucket = get_env("S3_BUCKET")
    prefix = get_env("S3_PREFIX")
    zip_url = get_env("GTFS_STATIC_ZIP_URL") or MBTA_GTFS_ZIP_URL

    if not bucket:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "S3_BUCKET is required"}),
        }

    result = run_collector(bucket=bucket, prefix=prefix, zip_url=zip_url)
    if result.get("updated"):
        print(f"GTFS static updated: {result.get('version_prefix')} ({result.get('files_uploaded')} files)")
    else:
        print(f"GTFS static unchanged: {result.get('reason', '')}")

    return {
        "statusCode": 200,
        "body": json.dumps({"gtfs_static": result}),
    }


if __name__ == "__main__":
    bucket = get_env("S3_BUCKET")  # 5114-transit-project-data
    prefix = get_env("S3_PREFIX")  # boston
    if not bucket:
        print("Set S3_BUCKET (and optionally S3_PREFIX) to run. Exiting.")
        exit(1)
    result = run_collector(bucket=bucket, prefix=prefix)
    print(json.dumps(result, indent=2))