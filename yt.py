import os
import asyncio
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Dict, Any, Tuple

from AnonMusic import LOGGER

# very important:
# config.py MUST have these:
#   YT_API_KEY = "1a873582a7c83342f961cc0a177b2b26"
#   YTPROXY_URL = "http://80.211.135.205:1470"
from config import YT_API_KEY, YTPROXY_URL

logger = LOGGER(__name__)

# make sure downloads folder exists
os.makedirs("downloads", exist_ok=True)

# we'll reuse one threadpool for blocking downloads
thread_pool = ThreadPoolExecutor(max_workers=4)


class YouTubeBridge:
    """
    Ye class tera pure flow handle karti hai:
    1. user query -> video_id resolve karegi
    2. video_id -> backend /info se direct audio/video URLs le aayegi
    3. URL se file download karegi (mp3 ya mp4)
    """

    def __init__(self):
        self.api_key = YT_API_KEY
        self.base_url = YTPROXY_URL.rstrip("/")  # "http://80.211.135.205:1470"

        if not self.api_key:
            logger.error("YT_API_KEY not set in config.py")
        if not self.base_url:
            logger.error("YTPROXY_URL not set in config.py")

    # ---------------------------
    # low-level HTTP helpers
    # ---------------------------

    def _safe_session(self) -> requests.Session:
        """
        normal requests.Session with retry-ish behavior using stream chunks.
        """
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
        )
        return s

    async def _download_file(
        self,
        direct_url: str,
        outfile_path: str,
    ) -> Optional[str]:
        """
        Download file (audio/video) from direct_url (which is actually
        your VPS /stream/<uuid> URL). Save to outfile_path.
        Runs in a thread so bot loop doesn't block.
        """

        def _job():
            try:
                with self._safe_session().get(direct_url, stream=True, timeout=120) as r:
                    r.raise_for_status()
                    with open(outfile_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)
                return outfile_path
            except Exception as e:
                logger.error(f"Download failed from {direct_url}: {e}")
                if os.path.exists(outfile_path):
                    try:
                        os.remove(outfile_path)
                    except:
                        pass
                return None

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(thread_pool, _job)

    # ---------------------------
    # main API calls to your VPS
    # ---------------------------

    def _call_youtube_search(
        self,
        query: str,
        want_video: bool,
    ) -> Optional[Dict[str, Any]]:
        """
        Step 1: Hit /youtube on your VPS.
        This:
            - does the YouTube search (or link resolve)
            - returns first result info
            - important fields:
                id (youtube video id),
                title,
                duration,
                thumbnail,
                stream_url (proxy /stream/... direct playable)
        """
        try:
            params = {
                "query": query,
                "video": "true" if want_video else "false",
                "api_key": self.api_key,
            }
            r = self._safe_session().get(
                f"{self.base_url}/youtube",
                params=params,
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            # data example:
            # {
            #   "id": "9vkcYxbGdTE",
            #   "title": "...",
            #   "duration": 245,
            #   "thumbnail": "...",
            #   "stream_url": "http://<vps>:1470/stream/uuid",
            #   "stream_type": "Video" or "Audio"
            # }
            if not data or not data.get("id"):
                logger.error(f"_call_youtube_search: no usable data for {query}")
                return None
            return data
        except Exception as e:
            logger.error(f"_call_youtube_search failed for {query}: {e}")
            return None

    def _call_info_on_video_id(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Step 2: Hit /info/<video_id> on your VPS.
        /info returns:
        {
          "status": "success",
          "audio_url": "http://<vps>:1470/stream/uuid1",
          "video_url": "http://<vps>:1470/stream/uuid2"
        }
        or error info
        """
        try:
            headers = {
                "x-api-key": self.api_key,
                "User-Agent": "Mozilla/5.0",
            }
            r = self._safe_session().get(
                f"{self.base_url}/info/{video_id}",
                headers=headers,
                params={"api_key": self.api_key},
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()

            # We expect data["status"] == "success"
            if data.get("status") != "success":
                logger.error(f"_call_info_on_video_id error for {video_id}: {data}")
                return None

            # must have both URLs ideally
            if not data.get("audio_url") and not data.get("video_url"):
                logger.error(f"_call_info_on_video_id: no urls in response {data}")
                return None

            return data
        except Exception as e:
            logger.error(f"_call_info_on_video_id failed for {video_id}: {e}")
            return None

    # ---------------------------
    # public helpers for the bot
    # ---------------------------

    async def fetch_song(
        self,
        user_query: str,
        want_video: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        HIGH-LEVEL FUNCTION for your bot.
        ye wahi function hai jo tu apne /play command me bulaega.

        Steps:
          1. "siyaram" -> /youtube -> we get (video_id, title, etc)
          2. video_id -> /info/<video_id> -> audio_url / video_url
          3. download chosen URL to local file in ./downloads
          4. return ready path + title for sending

        returns dict:
        {
          "file": "downloads/<id>.mp3 or .mp4",
          "title": "<nice title>",
          "video": bool
        }
        or None if fail.
        """

        # 1. search or resolve video
        base_info = self._call_youtube_search(user_query, want_video=want_video)
        if not base_info:
            logger.error(f"fetch_song: couldn't resolve query '{user_query}'")
            return None

        video_id = base_info["id"]
        title = base_info.get("title", video_id)

        # 2. get direct urls using /info/<video_id>
        stream_info = self._call_info_on_video_id(video_id)
        if not stream_info:
            logger.error(f"fetch_song: couldn't get stream info for {video_id}")
            return None

        # 3. pick audio or video URL
        if want_video:
            direct_url = stream_info.get("video_url")
            ext = "mp4"
        else:
            direct_url = stream_info.get("audio_url")
            ext = "mp3"

        if not direct_url:
            logger.error("fetch_song: desired media url missing")
            return None

        # local file path
        # we'll name file with <video_id>.<ext> so reuse is possible
        out_path = os.path.join("downloads", f"{video_id}.{ext}")

        # skip re-download if already exists
        if not os.path.exists(out_path):
            downloaded_path = await self._download_file(direct_url, out_path)
            if not downloaded_path:
                logger.error("fetch_song: download failed")
                return None
        else:
            downloaded_path = out_path

        return {
            "file": downloaded_path,
            "title": title,
            "video": want_video,
        }


# ============================================================
# HOW TO USE IN YOUR BOT
# ============================================================
#
# 1. create global bridge:
#
#    yt_bridge = YouTubeBridge()
#
# 2. when user sends command like /play <song name>:
#
#    async def handle_play_cmd(user_message_text):
#        query = user_message_text  # e.g. "siyaram"
#        result = await yt_bridge.fetch_song(query, want_video=False)
#
#        if not result:
#            # send "nahi mila bhai 😔"
#            return
#
#        # result["file"]  -> local mp3 path
#        # result["title"] -> nice title for caption
#
#        # send_audio(...) using telethon/pyrogram/etc:
#        # await app.send_audio(
#        #     chat_id=message.chat.id,
#        #     audio=result["file"],
#        #     title=result["title"],
#        # )
#
#
# 3. for video (like /video <song name>):
#
#    result = await yt_bridge.fetch_song(query, want_video=True)
#    # then send_video with result["file"]
#
#
# IMPORTANT:
# - No direct yt_dlp calls on bot side anymore.
# - No "cookie_txt_file" needed.
# - No direct youtube scraping from bot.
# - Bot only talks to YOUR VPS.
#
# RESULT:
# user kuch bhi likhe -> tera bot:
#   - /youtube se pehla result leta hai
#   - /info se final stream URLs leta hai
#   - file download karke bhej deta hai
#   - DONE.
#
# This matches exactly what you said:
# "mai ese krna chahta hun ki kisi bhi gane ka naam likhu or wo chalne lage"
#
# बस अब तेरे command handler को `yt_bridge.fetch_song()` call करना है.
#
# ============================================================

