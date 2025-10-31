# main.py — hardened, better logs, flexible auth, YT fallback, CORS, health
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from dotenv import load_dotenv

import subprocess
import sys
import os
import tempfile
import logging
import json
import re
from urllib.parse import urlparse, parse_qs

from pymongo import MongoClient
import requests

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("YT-Telegram-API")

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
API_KEY = os.getenv("API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL")

if not all([YOUTUBE_API_KEY, API_KEY, MONGO_URI, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL]):
    raise RuntimeError(
        "Missing environment variables. Need YOUTUBE_API_KEY, API_KEY, MONGO_URI, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL"
    )

logger.info("Environment loaded.")

# -------------------------------
# MongoDB Client
# -------------------------------
mongo = MongoClient(MONGO_URI)
db = mongo["yt_stream"]
collection = db["songs"]
logger.info("Mongo connected.")

# -------------------------------
# YouTube API client (lazily created on first use)
# -------------------------------
_youtube_client = None

def get_youtube_client():
    global _youtube_client
    if _youtube_client is None:
        _youtube_client = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        logger.info("YouTube API client initialized.")
    return _youtube_client

# -------------------------------
# FastAPI App
# -------------------------------
app = FastAPI(
    title="YouTube -> Telegram Streaming API",
    description="Endpoints: /health, /yt_search, /info, /stream, /download",
    version="1.2-hardened",
)

# CORS (in case bot / web client hits from elsewhere)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# Helpers
# -------------------------------

def run_yt_dlp(args: list[str], timeout: int = 45) -> str:
    """Run yt-dlp via the current Python interpreter. Returns stdout (stripped)."""
    cmd = [sys.executable, "-m", "yt_dlp"] + args
    logger.info(f"Running yt-dlp: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.error("yt-dlp timed out")
        raise HTTPException(status_code=504, detail="yt-dlp timed out")

    if result.returncode != 0:
        logger.error(f"yt-dlp failed.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        raise HTTPException(status_code=502, detail=f"yt-dlp failed: {result.stderr.strip()}")

    logger.info("yt-dlp succeeded.")
    return result.stdout.strip()


def send_to_telegram(file_path: str, caption: str | None = None):
    """Send audio file to Telegram channel, enforce ~50MB limit."""
    file_size = os.path.getsize(file_path)
    logger.info(f"Sending to Telegram: {file_path} ({file_size} bytes)")

    if file_size > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendAudio"
    with open(file_path, "rb") as f:
        files = {"audio": f}
        data = {"chat_id": TELEGRAM_CHANNEL, "caption": caption or ""}
        r = requests.post(url, files=files, data=data, timeout=60)

    if not r.ok:
        logger.error(f"Telegram upload failed: {r.status_code} {r.text}")
        raise HTTPException(status_code=502, detail=f"Telegram upload failed: {r.status_code}")

    logger.info("Telegram upload OK.")
    return r.json()


def extract_video_id(url: str) -> str | None:
    """Extract a YouTube video ID from various URL formats."""
    try:
        u = urlparse(url)
        if u.netloc.endswith("youtu.be"):
            # https://youtu.be/<id>
            vid = u.path.lstrip("/")
            return vid or None

        if "watch" in u.path:
            # https://www.youtube.com/watch?v=<id>
            q = parse_qs(u.query)
            return (q.get("v", [None])[0])

        # shorts or embed
        m = re.search(r"/(shorts|embed)/([A-Za-z0-9_-]{6,})", u.path)
        if m:
            return m.group(2)
    except Exception:
        return None
    return None


# -------------------------------
# Auth dependency (accepts multiple header styles)
# -------------------------------

def get_api_key(
    api_key: str | None = Header(None, alias="api-key"),
    x_api_key: str | None = Header(None, alias="x-api-key"),
    authorization: str | None = Header(None, alias="Authorization"),
):
    """Allow api-key, x-api-key, or Authorization: Bearer <token>."""
    token = None
    if api_key:
        token = api_key
    elif x_api_key:
        token = x_api_key
    elif authorization and authorization.startswith("Bearer "):
        token = authorization.replace("Bearer ", "", 1)

    if token != API_KEY:
        logger.warning("Auth failed: provided key does not match.")
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return True


# -------------------------------
# YouTube search (Data API with yt-dlp fallback)
# -------------------------------

def youtube_search(query: str, max_results: int = 5):
    logger.info(f"youtube_search(query={query!r}, max_results={max_results})")

    # Try official Data API first
    try:
        yt = get_youtube_client()
        request = yt.search().list(part="snippet", q=query, maxResults=max_results, type="video")
        response = request.execute()
        items = response.get("items", [])
        results = [
            {
                "title": it["snippet"]["title"],
                "videoId": it["id"]["videoId"],
                "url": f"https://www.youtube.com/watch?v={it['id']['videoId']}",
            }
            for it in items
        ]
        if results:
            return results
        logger.info("YouTube Data API returned 0 items, will try yt-dlp fallback.")
    except HttpError as e:
        logger.error(f"YouTube API error: {e}")
        # fall through to yt-dlp fallback
    except Exception as e:
        logger.error(f"YouTube API unexpected error: {e}")
        # fall through to yt-dlp fallback

    # Fallback: yt-dlp search (no quota needed)
    try:
        # yt-dlp prints one JSON per line for search results
        out = run_yt_dlp([
            "--dump-json",
            f"ytsearch{max_results}:{query}",
        ])
        results = []
        for line in out.splitlines():
            try:
                j = json.loads(line)
                if j.get("webpage_url") and j.get("title"):
                    # Attempt to get id from url if missing
                    vid = j.get("id")
                    if not vid and j.get("webpage_url"):
                        vid = extract_video_id(j["webpage_url"]) or ""
                    results.append({
                        "title": j.get("title"),
                        "videoId": vid,
                        "url": j.get("webpage_url"),
                    })
            except json.JSONDecodeError:
                continue
        logger.info(f"yt-dlp fallback returned {len(results)} items")
        return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"yt-dlp fallback failed: {e}")
        return []


# -------------------------------
# Routes
# -------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "service": "yt-telegram-api", "version": "1.2-hardened"}


@app.get("/yt_search")
def yt_search_endpoint(
    query: str = Query(..., description="Search query for YouTube"),
    limit: int = Query(5, ge=1, le=20, description="Max results (1-20)"),
    _ok: bool = Depends(get_api_key),
):
    try:
        results = youtube_search(query, max_results=limit)
        return {"results": results}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error in /yt_search")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/info")
def get_info(
    url: str = Query(..., description="YouTube URL (watch, youtu.be, shorts supported)"),
    _ok: bool = Depends(get_api_key),
):
    try:
        vid = extract_video_id(url)
        if not vid:
            # as a last resort, try query param v= in case of weird formatting
            q = parse_qs(urlparse(url).query)
            vid = (q.get("v", [None])[0])
        if not vid:
            raise HTTPException(status_code=400, detail="Could not extract video id")

        yt = get_youtube_client()
        request = yt.videos().list(part="snippet,contentDetails,statistics", id=vid)
        response = request.execute()
        items = response.get("items", [])
        if not items:
            raise HTTPException(status_code=404, detail="Video not found")
        item = items[0]
        return {
            "title": item["snippet"]["title"],
            "uploader": item["snippet"]["channelTitle"],
            "duration": item["contentDetails"]["duration"],
            "view_count": item["statistics"].get("viewCount"),
            "webpage_url": url,
            "thumbnail": item["snippet"]["thumbnails"]["high"]["url"],
        }
    except HttpError as e:
        reason = getattr(e, "_get_reason", lambda: str(e))()
        raise HTTPException(status_code=502, detail=f"YouTube API error: {reason}")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error in /info")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/stream")
def stream(
    url: str | None = Query(None, description="Full YouTube URL"),
    query: str | None = Query(None, description="Search on YouTube and take first result"),
    _ok: bool = Depends(get_api_key),
):
    try:
        if query and not url:
            search_res = youtube_search(query, max_results=1)
            if not search_res:
                raise HTTPException(status_code=404, detail="No video found")
            url = search_res[0]["url"]

        if not url:
            raise HTTPException(status_code=400, detail="You must provide url or query")

        # Ask yt-dlp for direct bestaudio URL
        output = run_yt_dlp([
            "-f", "bestaudio",
            "--no-playlist",
            "--get-url",
            url,
        ])

        # Store in Mongo (best-effort)
        try:
            collection.insert_one({"url": url, "direct_url": output})
        except Exception as mongo_err:
            logger.error(f"Mongo insert failed: {mongo_err}")

        return {"direct_url": output, "youtube_url": url}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error in /stream")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/download")
def download_mp3(
    url: str = Query(..., description="Full YouTube URL"),
    _ok: bool = Depends(get_api_key),
):
    try:
        temp_dir = tempfile.mkdtemp(prefix="ytmp3_")
        out_file = os.path.join(temp_dir, "%(title)s.%(ext)s")

        run_yt_dlp([
            "--extract-audio",
            "--audio-format", "mp3",
            "--no-playlist",
            "-o", out_file,
            url,
        ])

        files = os.listdir(temp_dir)
        if not files:
            raise HTTPException(status_code=500, detail="MP3 not found after download")
        mp3_name = files[0]
        mp3_path = os.path.join(temp_dir, mp3_name)

        # Send to Telegram (enforces 50MB)
        send_to_telegram(mp3_path, caption=mp3_name)

        return FileResponse(mp3_path, filename=mp3_name, media_type="audio/mpeg")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error in /download")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


# Optional: run with `python main.py` during local dev
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=7000, reload=True)
