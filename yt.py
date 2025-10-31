import aiohttp, asyncio, httpx
import logging, re, uuid, uvicorn, yt_dlp

from fastapi import FastAPI, HTTPException, Security, Header, Query
from fastapi.responses import StreamingResponse
from fastapi.security.api_key import APIKeyQuery
from logging.handlers import RotatingFileHandler
from youtubesearchpython.__future__ import VideosSearch


# ---------------------------
# LOGGING
# ---------------------------

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

logs = logging.getLogger(__name__)


# ---------------------------
# APP + RUNTIME STATE
# ---------------------------

app = FastAPI()
database = {}
ip_address = {}

# api_keys mapping "username": "actual_key"
api_keys = {
    "private": "1a873582a7c83342f961cc0a177b2b26"
}

api_key_query = APIKeyQuery(name="api_key", auto_error=True)


async def get_user(api_key: str = Security(api_key_query)):
    for user, key in api_keys.items():
        if key == api_key:
            return user
    raise HTTPException(status_code=403, detail="Invalid API key")


def validate_api_key(
    api_key_param: str | None,
    api_key_header: str | None,
) -> str:
    """
    Accept api_key either from query (?api_key=...) OR header x-api-key: ...
    Return the username if valid, else raise.
    """
    supplied = api_key_param if api_key_param else api_key_header
    if not supplied:
        raise HTTPException(status_code=401, detail="Not authenticated")

    for user, key in api_keys.items():
        if key == supplied:
            return user

    raise HTTPException(status_code=401, detail="Not authenticated")


async def new_uid() -> str:
    return str(uuid.uuid4())


async def get_public_ip() -> str:
    # cache so we don't spam ipify
    if ip_address.get("ip_address"):
        return ip_address["ip_address"]

    try:
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.get("https://api.ipify.org")
            public_ip = response.text.strip()
            ip_address["ip_address"] = public_ip
            return public_ip
    except Exception:
        return "localhost"


async def get_youtube_url(query: str) -> str:
    # if it's already a youtube link or ID, normalize it
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
            return f"https://www.youtube.com/watch?v={match.group(1)}"

    # otherwise do a search
    try:
        search = VideosSearch(query, limit=1)
        result = await search.next()
        return result["result"][0]["link"]
    except Exception:
        return ""


async def extract_metadata(url: str, video: bool = False):
    """
    Uses yt_dlp to get single direct stream (audio or video) + metadata.
    """
    if not url:
        return {}

    format_type = "best" if video else "bestaudio/best"

    ydl_opts = {
        "format": format_type,
        "no_warnings": True,
        "simulate": True,
        "quiet": True,
        "noplaylist": True,
        "extract_flat": True,
        "skip_download": True,
        "force_generic_extractor": True,
    }

    def sync_extract_metadata():
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception:
            return {}

    loop = asyncio.get_running_loop()
    metadata = await loop.run_in_executor(None, sync_extract_metadata)

    if metadata:
        return {
            "id": metadata.get("id"),
            "title": metadata.get("title"),
            "duration": metadata.get("duration"),
            "link": metadata.get("webpage_url"),
            "channel": metadata.get("channel", "Aditya Halder"),
            "views": metadata.get("view_count"),
            "thumbnail": metadata.get("thumbnail"),
            "stream_url": metadata.get("url"),
            "stream_type": "Video" if video else "Audio",
        }

    return {}


class Streamer:
    def __init__(self):
        # 1 MB chunks
        self.chunk_size = 1 * 1024 * 1024

    async def get_total_chunks(self, file_url):
        async with httpx.AsyncClient() as client:
            response = await client.head(file_url)
            file_size = response.headers.get("Content-Length")
            return (
                (int(file_size) + self.chunk_size - 1) // self.chunk_size
                if file_size
                else None
            )

    async def fetch_chunk(self, file_url, chunk_id):
        start_byte = chunk_id * self.chunk_size
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=10
        ) as client:
            headers = {
                "Range": f"bytes={start_byte}-{start_byte + self.chunk_size - 1}",
                "User-Agent": "Mozilla/5.0",
            }
            response = await client.get(file_url, headers=headers)
            if response.status_code in {206, 200}:
                return response.content
            return None

    async def stream_file(self, file_url):
        total_chunks = await self.get_total_chunks(file_url)
        received_chunks = set()
        chunk_id = 0

        while total_chunks is None or chunk_id < total_chunks:
            next_chunk_task = asyncio.create_task(
                self.fetch_chunk(file_url, chunk_id + 1)
            )
            current_chunk_task = asyncio.create_task(
                self.fetch_chunk(file_url, chunk_id)
            )

            current_chunk = await current_chunk_task
            if current_chunk:
                received_chunks.add(chunk_id)
                yield current_chunk

            next_chunk = await next_chunk_task
            if next_chunk:
                received_chunks.add(chunk_id + 1)
                yield next_chunk

            chunk_id += 2

        if total_chunks:
            for chunk_id in range(total_chunks):
                if chunk_id not in received_chunks:
                    missing_chunk = await self.fetch_chunk(file_url, chunk_id)
                    if missing_chunk:
                        yield missing_chunk


# ---------------------------
# /youtube
# ---------------------------

@app.get("/youtube")
async def get_youtube_info(
    query: str,
    video: bool = False,
    user: str = Security(get_user),
):
    """
    1. resolve query -> yt url
    2. yt_dlp metadata
    3. create /stream/<id> link
    """
    try:
        url = await get_youtube_url(query)
        metadata = await extract_metadata(url, video)
        if not metadata:
            return {}

        extention = "mp3" if not video else "mp4"

        file_url = metadata.get("stream_url")
        file_name = f"{metadata.get('id')}.{extention}"

        public_ip = await get_public_ip()
        stream_id = await new_uid()
        stream_url = f"http://{public_ip}:1470/stream/{stream_id}"

        database[stream_id] = {
            "file_url": file_url,
            "file_name": file_name,
        }

        return {
            "id": metadata.get("id"),
            "title": metadata.get("title"),
            "duration": metadata.get("duration"),
            "link": metadata.get("link"),
            "channel": metadata.get("channel"),
            "views": metadata.get("views"),
            "thumbnail": metadata.get("thumbnail"),
            "stream_url": stream_url,
            "stream_type": metadata.get("stream_type"),
        }
    except Exception:
        return {}


# ---------------------------
# /stream/{stream_id}
# ---------------------------

@app.get("/stream/{stream_id}")
async def stream_from_stream_url(stream_id: str):
    """
    Serve the media URL through our VPS in chunks.
    """
    file_data = database.get(stream_id)
    if (
        not file_data
        or not file_data.get("file_url")
        or not file_data.get("file_name")
    ):
        return {"error": "Invalid stream request!"}

    streamer = Streamer()
    try:
        headers = {
            "Content-Disposition": f'attachment; filename="{file_data.get("file_name")}"'
        }
        return StreamingResponse(
            streamer.stream_file(file_data.get("file_url")),
            media_type="application/octet-stream",
            headers=headers,
        )
    except Exception as e:
        logging.error(f"Stream Error: {e}")
        return {"error": "Something went wrong!"}


# ---------------------------
# /info/{video_id}  (for bot)
# ---------------------------

@app.get("/info/{video_id}")
async def info_endpoint(
    video_id: str,
    api_key: str | None = Query(
        default=None,
        description="API key (optional if x-api-key header is provided)",
    ),
    x_api_key: str | None = Header(
        default=None,
        alias="x-api-key",
        description="API key header for bot",
    ),
):
    """
    Endpoint made for AnonMusic/Telegram bot.
    Bot calls /info/<video_id> and expects:
    {
      "status": "success",
      "audio_url": "...",
      "video_url": "..."
    }

    We:
    - verify api key (query OR header)
    - get direct audio+video URLs via yt_dlp
    - register them in `database`
    - return our proxy /stream/... links
    """

    # auth check (works for both bot header and manual param)
    user = validate_api_key(api_key, x_api_key)

    yt_url = f"https://www.youtube.com/watch?v={video_id}"

    # best audio
    def sync_get_audio():
        with yt_dlp.YoutubeDL(
            {
                "quiet": True,
                "format": "bestaudio/best",
                "no_warnings": True,
                "noplaylist": True,
                "skip_download": True,
                "force_generic_extractor": True,
            }
        ) as ydl:
            return ydl.extract_info(yt_url, download=False)

    # <=720p video
    def sync_get_video():
        with yt_dlp.YoutubeDL(
            {
                "quiet": True,
                "format": "best[height<=?720][width<=?1280]",
                "no_warnings": True,
                "noplaylist": True,
                "skip_download": True,
                "force_generic_extractor": True,
            }
        ) as ydl:
            return ydl.extract_info(yt_url, download=False)

    loop = asyncio.get_running_loop()
    audio_info, video_info = await asyncio.gather(
        loop.run_in_executor(None, sync_get_audio),
        loop.run_in_executor(None, sync_get_video),
    )

    if not audio_info or not audio_info.get("url"):
        raise HTTPException(status_code=500, detail="Failed to get audio URL")
    if not video_info or not video_info.get("url"):
        raise HTTPException(status_code=500, detail="Failed to get video URL")

    audio_direct = audio_info.get("url")
    video_direct = video_info.get("url")

    # create stream IDs in our in-memory DB (same style as /youtube)
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

    public_ip = await get_public_ip()
    base_host = f"http://{public_ip}:1470"

    return {
        "status": "success",
        "audio_url": f"{base_host}/stream/{audio_stream_id}",
        "video_url": f"{base_host}/stream/{video_stream_id}",
    }


# ---------------------------
# MAIN
# ---------------------------

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=1470)
