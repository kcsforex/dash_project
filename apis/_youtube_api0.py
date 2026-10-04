# 2026.06.03  14.00 (updated)
from fastapi import APIRouter
from pydantic import BaseModel, Field, field_validator
from typing import List
from datetime import datetime
import aiohttp
import asyncio
import isodate
import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
import re
import os
import logging

logger = logging.getLogger(__name__)

# --- CONFIG ---
YOUTUBE_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"
router = APIRouter()

DB_CONFIG = {"host": "postgresql", "port": 5432, "database": "n8n", "username": "sql_admin", "password": "sql_pass", "connect_timeout": 15,}

STORE_KEYWORDS = ("shopify", "store", "gumroad", "etsy", "tiktokshop", "merch", "shop")

# --- PYDANTIC MODEL ---
class YouTubeRequest(BaseModel):
    channels: List[str] = Field(..., min_length=1, description="List of YouTube channel handles")
    maxVideos: int = Field(5, ge=1, le=50)
    maxComments: int = Field(5, ge=0, le=100)  # FIX: raised from 50 → 100 (YT API max per page)

    @field_validator("channels")
    @classmethod
    def validate_channels(cls, v):
        if not v:
            raise ValueError("channels list cannot be empty")
        for ch in v:
            if not ch.startswith("@"):
                raise ValueError(f"Invalid channel handle: '{ch}' — must start with @")
        return v


# --- GENERIC YT REQUEST ---
async def yt_get(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, endpoint: str, params: dict):
    """Single YouTube API call with semaphore-based rate limiting."""
    params["key"] = YOUTUBE_KEY
    async with semaphore:
        async with session.get(f"{BASE_URL}/{endpoint}", params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise Exception(f"YT API error {resp.status} on /{endpoint}: {text}")
            return await resp.json()


# --- DLT RESOURCE ---
@dlt.resource(name="youtube_metrics", max_table_nesting=0)
def youtube_resource(rows: list[dict]):
    # NOTE: max_table_nesting=0 means `comments` is stored as a JSON blob
    # in the youtube_metrics table. Query with: jsonb_array_elements(comments::jsonb)
    # To get a proper child table instead, remove max_table_nesting=0.
    for r in rows:
        yield r


# --- ENDPOINT ---
@router.post("/")
async def get_youtube_metrics_api(req: YouTubeRequest):
    raw = await fetch_youtube_multich(req.channels, req.maxVideos, req.maxComments)
    errors = [r for r in raw if not r.get("video_id") or r.get("video_id") == "ERROR"]
    data   = [r for r in raw if r.get("video_id") and r.get("video_id") != "ERROR"]

    if errors:
        logger.warning("Skipped %d error row(s): %s", len(errors), errors)

    if not data:
        return {"status": "no_data", "errors": errors}

    ts = datetime.utcnow().isoformat()
    for row in data:
        row["_ingested_at"] = ts

    pipeline = dlt.pipeline(
        pipeline_name="youtube_ingest",
        destination=dlt.destinations.postgres(credentials=DB_CONFIG),
        dataset_name="bronze")

    try:
        load_info = pipeline.run(youtube_resource(data), write_disposition="merge", primary_key="video_id")

    except PipelineStepFailed as e:
        # FIX: keep merge semantics in the fallback — append would create duplicates
        if e.step in ("load", "normalize") or "does not exist" in str(e).lower():
            logger.warning("PipelineStepFailed (%s) — retrying with merge after drop: %s", e.step, e)
            pipeline.drop_pending_packages()
            load_info = pipeline.run(youtube_resource(data), write_disposition="merge", primary_key="video_id")
        else:
            raise

    except Exception as e:
        logger.exception("Unexpected pipeline error: %s", e)
        raise

    return {"rows_loaded": len(data), "rows_skipped_errors": len(errors), "status": "loaded", "load_info": str(load_info), "sample": data[:5], "errors": errors}


# --- MULTI-CHANNEL FETCH ---
async def fetch_youtube_multich(channels: list[str], maxVideos: int, maxComments: int) -> list[dict]:
    semaphore = asyncio.Semaphore(3)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_single_channel(session, semaphore, ch, maxVideos, maxComments) for ch in channels]
        results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]


# --- SINGLE CHANNEL FETCH ---
async def fetch_single_channel(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, channel: str,maxVideos: int, maxComments: int) -> list[dict]:
    try:
        # 1. Resolve channel → uploads playlist
        ch_data = await yt_get(session, semaphore, "channels", {"part": "id,snippet,statistics,contentDetails","forHandle": channel})
        if not ch_data.get("items"):
            logger.warning("Channel not found: %s", channel)
            return [{"channel": channel, "video_id": None, "error": "Channel not found"}]

        ch_item = ch_data["items"][0]
        playlist_id = ch_item["contentDetails"]["relatedPlaylists"]["uploads"]

        # 2. Get latest video IDs from uploads playlist
        pl_data = await yt_get(session, semaphore, "playlistItems", {"part": "contentDetails", "playlistId": playlist_id, "maxResults": maxVideos})
        video_ids = [item["contentDetails"]["videoId"] for item in pl_data.get("items", [])]
        if not video_ids:
            return []

        # 3. Fetch video details in one batch call
        vids_data = await yt_get(session, semaphore, "videos", {"part": "snippet,contentDetails,statistics", "id": ",".join(video_ids) })

        results = []
        for video in vids_data.get("items", []):
            video_id  = video["id"]
            stats     = video["statistics"]
            snippet   = video["snippet"]
            details   = video["contentDetails"]

            description = snippet.get("description", "")
            links       = re.findall(r'(https?://\S+)', description)
            duration_sec = int(isodate.parse_duration(details["duration"]).total_seconds())

            # 4. Fetch top-level comments (skipped if disabled)
            comments = []
            if maxComments > 0 and int(stats.get("commentCount", 0)) > 0:
                try:
                    c_data = await yt_get(session, semaphore, "commentThreads", 
                            {"part": "snippet", "videoId": video_id, "maxResults": maxComments,  "textFormat": "plainText","order": "relevance"})
                    for c_item in c_data.get("items", []):
                        c_id = c_item["snippet"]["topLevelComment"]["id"]
                        c    = c_item["snippet"]["topLevelComment"]["snippet"]
                        comments.append({"c_id":c_id, "c_author":c["authorDisplayName"], "c_published":c["publishedAt"], "c_text":c["textDisplay"][:150],"c_like_count":c["likeCount"]})
                except Exception as e:
                    logger.warning("Comment fetch failed for %s: %s", video_id, e)
                    comments.append({"c_id": None, "c_author": None, "c_published": None, "c_text": f"[error: {str(e)[:100]}]", "c_like_count": 0})

            results.append({
                "error":               None,
                "channel":             ch_item["snippet"]["title"],
                "video_id":            video_id,
                "title":               snippet["title"][:100],       
                "description_snippet": description[:200],
                "has_store_link":      any(kw in link.lower() for link in links for kw in STORE_KEYWORDS),  
                "duration_sec":        duration_sec,
                "upload_date":         snippet["publishedAt"],
                "view_count":          int(stats.get("viewCount", 0)),
                "like_count":          int(stats.get("likeCount", 0)),
                "comment_count":       int(stats.get("commentCount", 0)),
                "comments":            comments,  # JSON blob in DB (max_table_nesting=0)
            })

        return results

    except Exception as e:
        logger.exception("Failed to fetch channel %s: %s", channel, e)
        return [{"channel": channel, "video_id": "ERROR", "error": str(e)}]
