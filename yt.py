import asyncio
import logging
import re
import uuid

import httpx
import uvicorn
import yt_dlp

from fastapi import FastAPI, HTTPException, Security, Header, Query
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
from logging.handlers import RotatingFileHandler
from youtubesearchpython.__future__ import VideosSearch


# ========================
# LOGGING
# ========================

logging.basicConfig(
    format="%(asctime)s [%(name)s]:: %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler("api.log", maxBytes=(1024 * 1024 * 5), backupCount=10),
        logging.StreamHandler(),
    ],
)

logging.getLogger("asyncio").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)

logs = logging.getLogger("ytapi")


# ========================
# GLOBAL STATE
# ========================

app = FastAPI(
    title="YouTube Proxy / Stream API",
    description="VPS proxy for YouTube audio/video, plus bot endpoint /info/{video_id}",
    version="1.0.0",
)

database = {}          # stream_id -> {"file_url": ..., "file_name": ...}
cached_ip = {}         # {"ip": "..."} so we don't hammer ipify
PORT = 1470

# api_keys: username -> actual key value
api_keys = {
    "private": "1a873582a7c83342f961cc0a177b2b26"
}

api_key_query = APIKeyQuery(name="api_key", auto_error=True)


# ========================
# AUTH HELPERS
# ========================

async def get_user(api_key: str = Security(api_key_query)):
    """
    Used by /youtube endpoint (query param ?api_key=...)
    """
    for user, key in api_keys.items():
        if key == api_key:
            return user
    raise HTTPException(status_code=403, detail="Invalid API key")


def validate_api_key(
    api_key_param: str | None,
    api_key_header: str | None,
) -> str:
    """
    Accept api_key from either:
    - query (?api_key=...)
    - header x-api-key: ...

    Return username, or raise HTTPException.
    """
    supplied = api_key_param if api_key_param else api_key_header
    if not supplied:
        raise HTTPException(status_code=401, detail="Not authenticated")

    for user, key in api_keys.items():
        if key == supplied:
            return user

    raise HTTPException(status_code=401, detail="Not authenticated")


# ========================
# UTILS
# ========================

async def new_uid() -> str:
    return str(uuid.uuid4())


async def get_public_ip() -> str:
    """
    Cache public IP for building public stream URLs.
    """
    if cached_ip.get("ip"):
        return cached_ip["ip"]

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://api.ipify.org")
            ip = resp.text.strip()
            cached_ip["ip"] = ip
            return ip
    except Exception as e:
        logs.error(f"Failed to fetch public IP: {e}")
        return "localhost"


async def get_youtube_url(query: str) -> str:
    """
    If query is already a YouTube link or ID -> normalize to full watch URL.
    Else -> search YouTube and return first result link.
    """
    # direct link / id?
    if bool(
        re.match(
            r"^(https?://)?(www\.)?(youtube\.com|youtu\.be)/"
            r"(?:watch\?v=|embed/|v/|shorts/|live/)?([A-Za-z0-9_-]{11})(?:[?&].*)?$",
            query,
        )
    ):
        match = re.search(
            r"(?:v=|/(?:embed|v|shorts|live)/|youtu\.be/)([A-Za-z0-9_-]{11})",
            query,
        )
        if match:
            vid = match.group(1)
            return f"https://www.youtube.com/watch?v={vid}"

    # fallback: search
    try:
        search = VideosSearch(query, limit=1)
        result = await search.next()
        return result["result"][0]["link"]
    except Exception as e:
        logs.error(f"Search failed for '{query}': {e}")
        return ""


async def extract_metadata(url: str, want_video: bool = False):
    """
    Use yt_dlp once to pull selected format direct URL (audio OR ~720p video),
    plus useful metadata.
    """
    if not url:
        return {}

    # choose format
    format_type = "best" if want_video else "bestaudio/best"

    ydl_opts = {
        "format": format_type,
        "no_warnings": True,
        "simulate": True,
        "quiet": True,
        "noplaylist": True,
        "extract_flat": True,
        "skip_download": True,
        "http_headers": {"User-Agent": "Mozilla/5.0"},
    }

    def run_extract():
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception as e:
            logs.error(f"yt_dlp metadata error: {e}")
            return {}

    loop = asyncio.get_running_loop()
    info = await loop.run_in_executor(None, run_extract)

    if not info:
        return {}

    return {
        "id": info.get("id"),
        "title": info.get("title"),
        "duration": info.get("duration"),            # seconds
        "link": info.get("webpage_url"),
        "channel": info.get("channel", "Unknown"),
        "views": info.get("view_count"),
        "thumbnail": info.get("thumbnail"),
        "stream_url": info.get("url"),               # direct CDN URL
        "stream_type": "Video" if want_video else "Audio",
    }


class Streamer:
    """
    Proxies a remote file in ~1MB chunks via /stream/{id}
    """
    def __init__(self):
        self.chunk_size = 1 * 1024 * 1024  # 1MB

    async def get_total_chunks(self, file_url: str):
        async with httpx.AsyncClient(timeout=10) as client:
            head = await client.head(file_url)
            size = head.headers.get("Content-Length")
            if not size:
                return None
            total = (int(size) + self.chunk_size - 1) // self.chunk_size
            return total

    async def fetch_chunk(self, file_url: str, chunk_id: int):
        start = chunk_id * self.chunk_size
        headers = {
            "Range": f"bytes={start}-{start + self.chunk_size - 1}",
            "User-Agent": "Mozilla/5.0",
        }
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(file_url, headers=headers)
            if r.status_code in (200, 206):
                return r.content
            return None

    async def stream_file(self, file_url: str):
        total = await self.get_total_chunks(file_url)
        received = set()
        c_id = 0

        while total is None or c_id < total:
            # prefetch next + current in parallel for smoother playback
            t_next = asyncio.create_task(self.fetch_chunk(file_url, c_id + 1))
            t_cur = asyncio.create_task(self.fetch_chunk(file_url, c_id))

            cur = await t_cur
            if cur:
                received.add(c_id)
                yield cur

            nxt = await t_next
            if nxt:
                received.add(c_id + 1)
                yield nxt

            c_id += 2

        # cleanup pass in case some chunks missed
        if total:
            for i in range(total):
                if i not in received:
                    miss = await self.fetch_chunk(file_url, i)
                    if miss:
                        yield miss


# ========================
# ROUTES
# ========================

@app.get("/youtube")
async def get_youtube_info(
    query: str,
    video: bool = False,
    user: str = Security(get_user),
):
    """
    1. resolve 'query' -> final YouTube URL
    2. yt_dlp gets direct stream + metadata
    3. generate /stream/<uuid> for download/relay
    """
    try:
        final_url = await get_youtube_url(query)
        meta = await extract_metadata(final_url, want_video=video)
        if not meta:
            return {}

        ext = "mp4" if video else "mp3"

        file_url = meta.get("stream_url")
        file_name = f"{meta.get('id')}.{ext}"

        pub_ip = await get_public_ip()
        stream_id = await new_uid()
        stream_url = f"http://{pub_ip}:{PORT}/stream/{stream_id}"

        database[stream_id] = {
            "file_url": file_url,
            "file_name": file_name,
        }

        return {
            "id": meta.get("id"),
            "title": meta.get("title"),
            "duration": meta.get("duration"),
            "link": meta.get("link"),
            "channel": meta.get("channel"),
            "views": meta.get("views"),
            "thumbnail": meta.get("thumbnail"),
            "stream_url": stream_url,
            "stream_type": meta.get("stream_type"),
        }

    except Exception as e:
        logs.error(f"/youtube failed: {e}")
        return {}


@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    """
    This proxies the media in chunks to the client
    (browser, bot, etc).
    """
    file_data = database.get(stream_id)
    if not file_data:
        return {"error": "Invalid stream request!"}

    file_url = file_data.get("file_url")
    file_name = file_data.get("file_name")

    if not file_url or not file_name:
        return {"error": "Invalid stream request!"}

    streamer = Streamer()

    try:
        headers = {
            "Content-Disposition": f'attachment; filename="{file_name}"'
        }
        return StreamingResponse(
            streamer.stream_file(file_url),
            media_type="application/octet-stream",
            headers=headers,
        )
    except Exception as e:
        logs.error(f"Stream Error: {e}")
        return {"error": "Something went wrong!"}


@app.get("/info/{video_id}")
async def info_endpoint(
    video_id: str,
    api_key: str | None = Query(
        default=None,
        description="API key (?api_key=...)"
    ),
    x_api_key: str | None = Header(
        default=None,
        alias="x-api-key",
        description="API key header for bot"
    ),
):
    """
    This is what your Telegram/AnonMusic bot calls.

    We ALWAYS return 200 OK with JSON like:
    {
        "status": "success",
        "audio_url": "...",
        "video_url": "..."
    }
    or:
    {
        "status": "error",
        "message": "..."
    }

    So the bot never chokes on 500.
    """

    # --- AUTH ---
    try:
        user = validate_api_key(api_key, x_api_key)
    except HTTPException:
        # bot expects JSON, not HTTP 401 HTML
        return {
            "status": "error",
            "message": "Not authenticated",
        }

    yt_url = f"https://www.youtube.com/watch?v={video_id}"

    # We'll ask yt_dlp twice: once for audio, once for ~720p video.
    COMMON = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "extract_flat": False,  # we want the actual stream URL
        "http_headers": {"User-Agent": "Mozilla/5.0"},
    }

    def get_audio_info():
        with yt_dlp.YoutubeDL(
            {**COMMON, "format": "bestaudio/best"}
        ) as ydl:
            return ydl.extract_info(yt_url, download=False)

    def get_video_info():
        with yt_dlp.YoutubeDL(
            {**COMMON, "format": "best[height<=?720][width<=?1280]"}
        ) as ydl:
            return ydl.extract_info(yt_url, download=False)

    loop = asyncio.get_running_loop()
    try:
        audio_info, video_info = await asyncio.gather(
            loop.run_in_executor(None, get_audio_info),
            loop.run_in_executor(None, get_video_info),
        )
    except Exception as e:
        logs.error(f"/info yt_dlp gather failed: {e}")
        return {
            "status": "error",
            "message": f"yt_dlp failed: {str(e)}",
        }

    # sanity check direct URLs
    if not audio_info or not audio_info.get("url"):
        return {
            "status": "error",
            "message": "Failed to get audio URL",
        }
    if not video_info or not video_info.get("url"):
        return {
            "status": "error",
            "message": "Failed to get video URL",
        }

    audio_direct = audio_info["url"]
    video_direct = video_info["url"]

    # create two temporary proxied streams
    audio_stream_id = await new_uid()
    video_stream_id = await new_uid()

    database[audio_stream_id] = {
        "file_url": audio_direct,
        "file_name": f"{video_id}.mp3",
    }
    database[video_stream_id] = {
        "file_url": video_direct,
        "file_name": f"{video_id}.mp4",
    }

    pub_ip = await get_public_ip()
    base_host = f"http://{pub_ip}:{PORT}"

    return {
        "status": "success",
        "audio_url": f"{base_host}/stream/{audio_stream_id}",
        "video_url": f"{base_host}/stream/{video_stream_id}",
    }


# ========================
# MAIN
# ========================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
