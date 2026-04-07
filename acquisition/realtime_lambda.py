"""
Transit GTFS-RT collector Lambda.
Fetches trip updates, vehicle positions, and alerts on a schedule
and uploads raw .pb to S3.

Credits: GPT 5.3 with supervision and modifications by group members
"""

import os
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import boto3
import json

# Getting config from environment
def get_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def build_feed_url(base_url: str, api_key: str) -> str:
    """Append API key as query param if provided."""
    if not api_key:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}key={api_key}"


# Fetch data and upload to bucket
def fetch_feed(url: str) -> bytes:
    """GET feed URL and return raw bytes. Raises on HTTP error."""
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=25) as resp:
        return resp.read()


def upload_to_s3(
    bucket: str,
    key: str,
    body: bytes,
    s3_client,
) -> str:
    """Upload bytes to S3. Returns the full s3:// URI."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/x-protobuf",
    )
    return f"s3://{bucket}/{key}"


def collect_one_feed(
    feed_name: str,
    feed_url: str,
    api_key: str,
    bucket: str,
    prefix: str,
    now: datetime,
    s3_client,
) -> dict:
    """
    Fetch one feed and upload to S3.
    Returns {"ok": True, "key": "..."} or {"ok": False, "error": "..."}.
    """
    dt = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    ts = now.strftime("%Y%m%d_%H%M%S")
    # key: [prefix/]rt/{trip_updates|vehicle_positions|alerts}/dt=.../hour=.../snapshot_....pb
    key_parts = ["rt", feed_name, f"dt={dt}", f"hour={hour}", f"snapshot_{ts}.pb"]
    if prefix:
        key_parts.insert(0, prefix)
    key = "/".join(key_parts)

    full_url = build_feed_url(feed_url, api_key)
    try:
        data = fetch_feed(full_url)
        uri = upload_to_s3(bucket, key, data, s3_client)
        return {"ok": True, "key": key, "uri": uri, "size_bytes": len(data)}
    except Exception as e:
        return {"ok": False, "key": key, "error": str(e)}


def lambda_handler(event, context):
    """
    Entry point for Lambda.

    Configuration comes from environment variables and the event:

    Env vars:
      S3_BUCKET - 5114-transit-project-data
      FEED_URL_TRIP_UPDATES - https://cdn.mbta.com/realtime/TripUpdates.pb
      FEED_URL_VEHICLE_POSITIONS - https://cdn.mbta.com/realtime/VehiclePositions.pb 
      FEED_URL_ALERTS - https://cdn.mbta.com/realtime/Alerts.pb
      S3_PREFIX - boston
      API_KEY

    Event (from EventBridge rule target input):
      {
        "mode": "core"   # fetch trip_updates + vehicle_positions - every 60 seconds
        "mode": "alerts" # fetch alerts only - every 5 minutes
      }
    """
    bucket = get_env("S3_BUCKET")
    prefix = get_env("S3_PREFIX")
    url_trip_updates = get_env("FEED_URL_TRIP_UPDATES")
    url_vehicle_positions = get_env("FEED_URL_VEHICLE_POSITIONS")
    url_alerts = get_env("FEED_URL_ALERTS")
    api_key = get_env("API_KEY")

    if not bucket:
        return {"statusCode": 500, "body": json.dumps({"error": "S3_BUCKET is required"})}
    if not any([url_trip_updates, url_vehicle_positions, url_alerts]):
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": (
                        "At least one of FEED_URL_TRIP_UPDATES, "
                        "FEED_URL_VEHICLE_POSITIONS, or FEED_URL_ALERTS is required"
                    )
                }
            ),
        }

    # Partition S3 keys by Boston local time (America/New_York) so dt/hour align with MBTA's timezone.
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(ZoneInfo("America/New_York"))
    s3 = boto3.client("s3")

    # Determine which feeds to collect based on event["mode"], if present.
    mode = None
    if isinstance(event, dict):
        mode = event.get("mode")

    configured_feeds = {
        "trip_updates": url_trip_updates,
        "vehicle_positions": url_vehicle_positions,
        "alerts": url_alerts,
    }

    if mode == "alerts":
        feed_order = ["alerts"]
    elif mode == "core":
        feed_order = ["trip_updates", "vehicle_positions"]
    else:
        # Default: collect all configured feeds.
        feed_order = ["trip_updates", "vehicle_positions", "alerts"]

    feeds = [(name, configured_feeds[name]) for name in feed_order if configured_feeds.get(name)]

    results = {}

    for feed_name, feed_url in feeds:
        out = collect_one_feed(
            feed_name=feed_name,
            feed_url=feed_url,
            api_key=api_key,
            bucket=bucket,
            prefix=prefix,
            now=now_local,
            s3_client=s3,
        )
        results[feed_name] = out
        if out.get("ok"):
            print(f"Uploaded {out.get('uri')} ({out.get('size_bytes')} bytes)")
        else:
            print(f"Failed {feed_name}: {out.get('error')}")

    success = all(r.get("ok") for r in results.values())
    return {
        "statusCode": 200 if success else 207,
        "body": json.dumps({
            "timestamp_utc": now_utc.isoformat(),
            "timestamp_local": now_local.isoformat(),
            "results": results,
        }),
    }
