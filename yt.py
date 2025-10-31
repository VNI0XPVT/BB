# main.py
from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.responses import FileResponse
from googleapiclient.discovery import build
from dotenv import load_dotenv
import subprocess, sys, os, tempfile
from pymongo import MongoClient
import requests

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
    raise RuntimeError("Missing environment variables. Check your .env file")

# -------------------------------
# MongoDB Client
# -------------------------------
mongo = MongoClient(MONGO_URI)
db = mongo["yt_stream"]
collection = db["songs"]

# -------------------------------
# YouTube API client
# -------------------------------
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# -------------------------------
# FastAPI App
# -------------------------------
app = FastAPI(
    title="YouTube -> Telegram Streaming API",
    description="Endpoints: /info, /download, /stream, /yt_search",
    version="1.0"
)

# -------------------------------
# Helper: yt-dlp
# -------------------------------
def run_yt_dlp(args: list):
    cmd = [sys.executable, "-m", "yt_dlp"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"yt-dlp run failed: {result.stderr}")
    return result.stdout

# -------------------------------
# Helper: Upload to Telegram
# -------------------------------
def send_to_telegram(file_path, caption=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendAudio"
    with open(file_path, "rb") as f:
        files = {"audio": f}
        data = {"chat_id": TELEGRAM_CHANNEL, "caption": caption or ""}
        r = requests.post(url, files=files, data=data)
    return r.json()

# -------------------------------
# /yt_search endpoint
# -------------------------------
@app.get("/yt_search")
def yt_search(query: str = Query(...), api_key: str = Header(None)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    try:
        request = youtube.search().list(
            part="snippet",
            q=query,
            maxResults=5,
            type="video"
        )
        response = request.execute()
        results = []
        for item in response.get("items", []):
            results.append({
                "title": item["snippet"]["title"],
                "videoId": item["id"]["videoId"],
                "url": f"https://www.youtube.com/watch?v={item['id']['videoId']}"
            })
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /info endpoint
# -------------------------------
@app.get("/info")
def get_info(url: str = Query(...), api_key: str = Header(None)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    try:
        video_id = url.split("v=")[-1]
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        if not response["items"]:
            raise HTTPException(status_code=404, detail="Video not found")
        item = response["items"][0]
        return {
            "title": item["snippet"]["title"],
            "uploader": item["snippet"]["channelTitle"],
            "duration": item["contentDetails"]["duration"],
            "view_count": item["statistics"].get("viewCount"),
            "webpage_url": url,
            "thumbnail": item["snippet"]["thumbnails"]["high"]["url"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /stream endpoint
# -------------------------------
@app.get("/stream")
def stream(url: str = Query(None), query: str = Query(None), api_key: str = Header(None)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    try:
        if query:
            search_res = yt_search(query, api_key=API_KEY)
            if not search_res["results"]:
                raise HTTPException(status_code=404, detail="No video found")
            url = search_res["results"][0]["url"]

        if not url:
            raise HTTPException(status_code=400, detail="You must provide url or query")

        # Get audio URL using yt-dlp
        output = run_yt_dlp([
            "-f", "bestaudio",
            "--no-playlist",
            "--get-url",
            url
        ])
        
        # Log to MongoDB
        collection.insert_one({"url": url, "direct_url": output.strip()})

        return {"direct_url": output.strip(), "youtube_url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# /download endpoint (MP3 + Telegram upload)
# -------------------------------
@app.get("/download")
def download_mp3(url: str = Query(...), api_key: str = Header(None)):
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    try:
        temp_dir = tempfile.mkdtemp()
        out_file = os.path.join(temp_dir, "%(title)s.%(ext)s")
        run_yt_dlp([
            "--extract-audio",
            "--audio-format", "mp3",
            "--no-playlist",
            "-o", out_file,
            url
        ])
        files = os.listdir(temp_dir)
        if not files:
            raise HTTPException(status_code=500, detail="MP3 not found")
        mp3_path = os.path.join(temp_dir, files[0])

        # Send to Telegram
        send_to_telegram(mp3_path, caption=files[0])

        return FileResponse(mp3_path, filename=files[0], media_type="audio/mpeg")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
