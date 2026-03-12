#!/usr/bin/env python3
"""
BGMI Hashtag Video Fetcher
--------------------------
Fetches videos tagged #BGMI from YouTube and Instagram daily.
Filters to only keep videos with >10,000 views.
Appends results to a master CSV (bgmi_videos.csv) with date tracking.

Usage:
    python bgmi_fetcher.py

Cron example (runs daily at 6 AM):
    0 6 * * * /usr/bin/python3 /path/to/bgmi_fetcher.py >> /var/log/bgmi_fetcher.log 2>&1

Environment variables required (set in .env or system env):
    YOUTUBE_API_KEY       - Google Cloud YouTube Data API v3 key
    INSTAGRAM_SESSION_FILE - Path to saved instaloader session file (optional)
    OUTPUT_DIR            - Directory to save CSVs (default: ./data)
"""

import os
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
YOUTUBE_API_KEY       = os.getenv("YOUTUBE_API_KEY", "")
INSTAGRAM_SESSION_FILE = os.getenv("INSTAGRAM_SESSION_FILE", "")
OUTPUT_DIR            = Path(os.getenv("OUTPUT_DIR", "./data"))
MIN_VIEWS             = 10_000
HASHTAG               = "BGMI"
MAX_RESULTS_YT        = 50   # max per API call (YouTube limit)
MAX_POSTS_IG          = 100  # how many recent IG posts to scan

MASTER_CSV = OUTPUT_DIR / "bgmi_videos.csv"
DAILY_CSV  = OUTPUT_DIR / f"bgmi_videos_{datetime.now().strftime('%Y-%m-%d')}.csv"

CSV_FIELDS = [
    "fetched_date",
    "platform",
    "video_id",
    "title",
    "channel_or_user",
    "url",
    "views",
    "likes",
    "comments",
    "published_at",
    "duration_seconds",
    "thumbnail_url",
    "description_snippet",
]

# ─── Helpers ──────────────────────────────────────────────────────────────────

def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(rows: list[dict], path: Path, append: bool = False):
    """Write rows to CSV. If append=True, skip header when file exists."""
    mode = "a" if append and path.exists() else "w"
    with open(path, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if mode == "w":
            writer.writeheader()
        writer.writerows(rows)
    log.info(f"  Wrote {len(rows)} rows → {path}")


def dedup_master(new_rows: list[dict]) -> list[dict]:
    """Skip video_ids already in master CSV (avoid duplicates across days)."""
    existing_ids = set()
    if MASTER_CSV.exists():
        with open(MASTER_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_ids.add(row.get("video_id", ""))
    unique = [r for r in new_rows if r["video_id"] not in existing_ids]
    skipped = len(new_rows) - len(unique)
    if skipped:
        log.info(f"  Dedup: skipped {skipped} already-known video(s)")
    return unique


# ─── YouTube ──────────────────────────────────────────────────────────────────

def fetch_youtube() -> list[dict]:
    """Search YouTube for recent #BGMI videos, filter by view count."""
    if not YOUTUBE_API_KEY:
        log.warning("YOUTUBE_API_KEY not set — skipping YouTube fetch")
        return []

    try:
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        import isodate
    except ImportError:
        log.error("Missing dependencies. Run: pip install google-api-python-client isodate")
        return []

    log.info("Fetching YouTube #BGMI videos …")
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")

    rows = []
    next_page_token = None

    try:
        while True:
            # Step 1: Search for video IDs
            search_params = dict(
                part="id",
                q=f"#{HASHTAG}",
                type="video",
                order="viewCount",
                publishedAfter=today,
                maxResults=MAX_RESULTS_YT,
            )
            if next_page_token:
                search_params["pageToken"] = next_page_token

            search_resp = youtube.search().list(**search_params).execute()
            video_ids = [item["id"]["videoId"] for item in search_resp.get("items", [])]

            if not video_ids:
                break

            # Step 2: Get full video stats + content details
            videos_resp = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids),
            ).execute()

            for item in videos_resp.get("items", []):
                stats   = item.get("statistics", {})
                snippet = item.get("snippet", {})
                content = item.get("contentDetails", {})

                views = int(stats.get("viewCount", 0))
                if views < MIN_VIEWS:
                    continue

                # Parse ISO 8601 duration → seconds
                duration_secs = 0
                try:
                    duration_secs = int(isodate.parse_duration(
                        content.get("duration", "PT0S")
                    ).total_seconds())
                except Exception:
                    pass

                rows.append({
                    "fetched_date":       datetime.now().strftime("%Y-%m-%d"),
                    "platform":           "YouTube",
                    "video_id":           item["id"],
                    "title":              snippet.get("title", ""),
                    "channel_or_user":    snippet.get("channelTitle", ""),
                    "url":                f"https://www.youtube.com/watch?v={item['id']}",
                    "views":              views,
                    "likes":              int(stats.get("likeCount", 0)),
                    "comments":           int(stats.get("commentCount", 0)),
                    "published_at":       snippet.get("publishedAt", ""),
                    "duration_seconds":   duration_secs,
                    "thumbnail_url":      snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                    "description_snippet": snippet.get("description", "")[:200],
                })

            next_page_token = search_resp.get("nextPageToken")
            if not next_page_token:
                break

    except HttpError as e:
        log.error(f"YouTube API error: {e}")

    log.info(f"  YouTube: {len(rows)} videos with >{MIN_VIEWS:,} views")
    return rows


# ─── Instagram ────────────────────────────────────────────────────────────────

def fetch_instagram() -> list[dict]:
    """Scrape Instagram #BGMI posts using instaloader (no official API needed)."""
    try:
        import instaloader
    except ImportError:
        log.error("Missing dependency. Run: pip install instaloader")
        return []

    log.info("Fetching Instagram #BGMI posts …")
    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        quiet=True,
    )

    # Load session if provided (avoids rate limits / login walls)
    if INSTAGRAM_SESSION_FILE and Path(INSTAGRAM_SESSION_FILE).exists():
        try:
            L.load_session_from_file(username="", filename=INSTAGRAM_SESSION_FILE)
            log.info("  Loaded Instagram session from file")
        except Exception as e:
            log.warning(f"  Could not load session: {e}")

    rows = []
    try:
        hashtag = instaloader.Hashtag.from_name(L.context, HASHTAG)
        count = 0
        for post in hashtag.get_posts():
            if count >= MAX_POSTS_IG:
                break
            count += 1

            # Only video posts
            if not post.is_video:
                continue

            views = post.video_view_count or 0
            if views < MIN_VIEWS:
                continue

            rows.append({
                "fetched_date":       datetime.now().strftime("%Y-%m-%d"),
                "platform":           "Instagram",
                "video_id":           post.shortcode,
                "title":              (post.caption or "")[:120],
                "channel_or_user":    post.owner_username,
                "url":                f"https://www.instagram.com/p/{post.shortcode}/",
                "views":              views,
                "likes":              post.likes,
                "comments":           post.comments,
                "published_at":       post.date_utc.isoformat(),
                "duration_seconds":   int(post.video_duration or 0),
                "thumbnail_url":      post.url,
                "description_snippet": (post.caption or "")[:200],
            })

    except Exception as e:
        log.error(f"Instagram fetch error: {e}")

    log.info(f"  Instagram: {len(rows)} videos with >{MIN_VIEWS:,} views")
    return rows


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info(f"=== BGMI Video Fetcher — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    ensure_output_dir()

    all_rows = []
    all_rows.extend(fetch_youtube())
    all_rows.extend(fetch_instagram())

    if not all_rows:
        log.warning("No videos fetched today. Check API keys / network.")
        return

    # Sort by views descending
    all_rows.sort(key=lambda r: r["views"], reverse=True)

    # Write today's daily CSV (full — may have some overlap with master)
    write_csv(all_rows, DAILY_CSV, append=False)

    # Append only NEW videos to master CSV
    new_rows = dedup_master(all_rows)
    if new_rows:
        write_csv(new_rows, MASTER_CSV, append=True)
    else:
        log.info("  No new unique videos to add to master CSV.")

    log.info(f"=== Done. {len(all_rows)} total today | {len(new_rows)} new added to master ===")


if __name__ == "__main__":
    main()
