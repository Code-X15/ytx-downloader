import asyncio
import json
import os
import re
from pathlib import Path
from urllib.parse import quote

import requests
import yt_dlp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, APIC

app = FastAPI()

# ---------- CORS ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- directories ----------
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ---------- PORT for cloud ----------
PORT = int(os.environ.get("PORT", 8000))

# ---------- optional cookies file for YouTube bot-detection bypass ----------
COOKIES_FILE = "cookies.txt"


def base_ydl_opts():
    opts = {
        "quiet": True,
        "noplaylist": True,
    }
    if os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    return opts


# ---------- filename cleaner ----------
def clean_filename(name: str):
    name = re.sub(r'[<>:"/\\|?*]', ' ', name)
    name = re.sub(r'[^\w\s.-]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()


# ---------- ping ----------
@app.get("/ping")
async def ping():
    return {"status": "online"}


# ---------- metadata ----------
@app.get("/metadata")
async def get_metadata(url: str):

    ydl_opts = base_ydl_opts()

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    return {
        "title": info.get("title"),
        "thumbnail": info.get("thumbnail"),
        "author_name": info.get("uploader"),
        "duration": info.get("duration")
    }


# ---------- websocket download ----------
@app.websocket("/download")
async def download(ws: WebSocket):

    await ws.accept()

    try:

        data = await ws.receive_text()
        data = json.loads(data)

        url = data["url"]
        format_type = data.get("format", "mp3")

        loop = asyncio.get_event_loop()

        # progress hook
        def progress_hook(d):

            if d["status"] == "downloading":

                percent = d.get("_percent_str", "0%")
                speed = d.get("_speed_str", "0")
                eta = d.get("_eta_str", "0")

                asyncio.run_coroutine_threadsafe(
                    ws.send_text(json.dumps({
                        "status": "downloading",
                        "progress": percent,
                        "speed": speed,
                        "eta": eta
                    })),
                    loop
                )

        ydl_opts = base_ydl_opts()
        ydl_opts.update({
            "format": "bestaudio/best",
            "outtmpl": f"{DOWNLOAD_DIR}/%(title)s.%(ext)s",
            "progress_hooks": [progress_hook],
        })

        if format_type == "mp3":

            ydl_opts["postprocessors"] = [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192"
            }]

        # ---------- run download ----------
        def run_download():

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:

                info = ydl.extract_info(url, download=True)

                filepath = ydl.prepare_filename(info)

                if format_type == "mp3":
                    filepath = os.path.splitext(filepath)[0] + ".mp3"

                # clean filename
                folder = os.path.dirname(filepath)
                base = os.path.basename(filepath)

                safe = clean_filename(base)
                new_path = os.path.join(folder, safe)

                if filepath != new_path and os.path.exists(filepath):
                    os.rename(filepath, new_path)
                    filepath = new_path

                return info, filepath

        info, filepath = await asyncio.to_thread(run_download)

        # ---------- embed metadata ----------
        if format_type == "mp3":

            try:

                audio = MP3(filepath, ID3=ID3)

                if audio.tags is None:
                    audio.add_tags()

                audio.tags.add(TIT2(encoding=3, text=info.get("title", "")))
                audio.tags.add(TPE1(encoding=3, text=info.get("uploader", "")))

                thumb = info.get("thumbnail")

                if thumb:

                    img = requests.get(thumb).content

                    audio.tags.add(APIC(
                        encoding=3,
                        mime="image/jpeg",
                        type=3,
                        desc="Cover",
                        data=img
                    ))

                audio.save()

            except Exception as e:
                print("Metadata failed:", e)

        filename = os.path.basename(filepath)

        download_url = f"/download_file?filename={quote(filename)}"

        await ws.send_text(json.dumps({
            "status": "finished",
            "filename": filename,
            "download_url": download_url,
            "metadata": {
                "title": info.get("title"),
                "author_name": info.get("uploader"),
                "thumbnail": info.get("thumbnail")
            }
        }))

    except WebSocketDisconnect:
        print("Client disconnected")

    except Exception as e:

        await ws.send_text(json.dumps({
            "status": "error",
            "message": str(e)
        }))


# ---------- download file ----------
@app.get("/download_file")
async def download_file(filename: str):

    path = Path(DOWNLOAD_DIR) / filename

    if not path.exists():
        return {"error": "file not found"}

    return FileResponse(path, filename=filename)


# ---------- start server ----------
if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        "ytx:app",
        host="0.0.0.0",
        port=PORT
    )
