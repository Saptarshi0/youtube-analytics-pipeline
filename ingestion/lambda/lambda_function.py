import json
import boto3
import urllib.request
import urllib.parse
import os
from datetime import datetime, timezone

# ── Clients ───────────────────────────────────────────
firehose = boto3.client("firehose")
secrets  = boto3.client("secretsmanager")

# ── Constants ─────────────────────────────────────────
FIREHOSE_STREAM = os.environ["FIREHOSE_STREAM_NAME"]
SECRET_NAME     = os.environ["SECRET_NAME"]
REGIONS         = os.environ.get("REGIONS", "US,GB,IN,CA").split(",")
MAX_RESULTS     = 50   # Max YouTube allows per call
YT_BASE_URL     = "https://www.googleapis.com/youtube/v3"


def get_api_key():
    """Retrieve YouTube API key from Secrets Manager."""
    response = secrets.get_secret_value(SecretId=SECRET_NAME)
    secret   = json.loads(response["SecretString"])
    return secret["api_key"]


def fetch_trending_videos(api_key, region_code):
    """Fetch top 50 trending videos for a given region."""
    params = {
        "part":            "snippet,statistics,contentDetails",
        "chart":           "mostPopular",
        "regionCode":      region_code,
        "maxResults":      str(MAX_RESULTS),
        "key":             api_key
    }
    url = f"{YT_BASE_URL}/videos?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def fetch_channel_stats(api_key, channel_ids):
    """Fetch channel statistics for a list of channel IDs."""
    params = {
        "part":  "statistics,snippet",
        "id":    ",".join(channel_ids),
        "key":   api_key
    }
    url = f"{YT_BASE_URL}/channels?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def parse_video(item, region_code, ingested_at):
    """Flatten a YouTube API video item into a clean record."""
    snippet    = item.get("snippet", {})
    stats      = item.get("statistics", {})
    content    = item.get("contentDetails", {})

    return {
        # Identifiers
        "video_id":          item["id"],
        "channel_id":        snippet.get("channelId"),
        "region":            region_code,
        # Content
        "title":             snippet.get("title"),
        "description":       snippet.get("description", "")[:500],  # truncate
        "channel_title":     snippet.get("channelTitle"),
        "category_id":       snippet.get("categoryId"),
        "tags":              snippet.get("tags", []),
        "duration":          content.get("duration"),
        "default_language":  snippet.get("defaultLanguage"),
        # Timestamps
        "published_at":      snippet.get("publishedAt"),
        "ingested_at":       ingested_at,
        # Statistics
        "view_count":        int(stats.get("viewCount",    0) or 0),
        "like_count":        int(stats.get("likeCount",    0) or 0),
        "comment_count":     int(stats.get("commentCount", 0) or 0),
        # Metadata
        "source":            "youtube_trending",
        "pipeline_version":  "1.0"
    }


def send_to_firehose(records):
    """Send a batch of records to Kinesis Firehose."""
    # Firehose accepts up to 500 records per batch
    batch_size = 100
    total_sent = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        firehose_records = [
            {"Data": (json.dumps(r) + "\n").encode("utf-8")}
            for r in batch
        ]
        response = firehose.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM,
            Records=firehose_records
        )
        failed = response.get("FailedPutCount", 0)
        if failed > 0:
            print(f"WARNING: {failed} records failed to deliver")
        total_sent += len(batch) - failed

    return total_sent


def lambda_handler(event, context):
    """Main Lambda handler — orchestrates multi-region fetch."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    api_key     = get_api_key()

    all_records   = []
    region_counts = {}
    errors        = []

    for region in REGIONS:
        try:
            print(f"Fetching trending videos for region: {region}")
            data   = fetch_trending_videos(api_key, region)
            items  = data.get("items", [])

            # Parse each video into a flat record
            records = [parse_video(item, region, ingested_at) for item in items]
            all_records.extend(records)
            region_counts[region] = len(records)
            print(f"Region {region}: {len(records)} videos fetched")

        except Exception as e:
            print(f"ERROR fetching region {region}: {str(e)}")
            errors.append({"region": region, "error": str(e)})

    # Send all records to Firehose in batches
    total_sent = 0
    if all_records:
        total_sent = send_to_firehose(all_records)

    # Build summary
    summary = {
        "status":        "SUCCESS" if not errors else "PARTIAL",
        "ingested_at":   ingested_at,
        "total_records": total_sent,
        "by_region":     region_counts,
        "errors":        errors
    }

    print(f"Pipeline complete: {json.dumps(summary)}")
    return summary