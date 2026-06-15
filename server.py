import shutil
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent
MUSIC_DIR = BASE_DIR / "music"
STATIC_DIR = BASE_DIR / "static"

MUSIC_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

app = FastAPI(title="MP3Sync")


class PlayerState:
    def __init__(self):
        self.track: Optional[str] = None
        self.playing: bool = False
        self.position: float = 0.0
        self.position_ts: float = time.time()

    def effective_position(self) -> float:
        if self.playing:
            return self.position + (time.time() - self.position_ts)
        return self.position

    def to_dict(self) -> dict:
        return {
            "track": self.track,
            "playing": self.playing,
            "position": self.effective_position(),
            "server_time": time.time(),
        }


class SyncManager:
    def __init__(self):
        self.clients: list[WebSocket] = []
        self.state = PlayerState()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)
        await ws.send_json({"type": "sync", "state": self.state.to_dict()})
        count = len(self.clients)
        await self._broadcast({"type": "users", "count": count})
        await self._broadcast(
            {"type": "event", "message": f"A listener joined · {count} online"},
            exclude=ws,
        )

    def disconnect(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)

    async def _broadcast(self, msg: dict, exclude: Optional[WebSocket] = None):
        dead = []
        for ws in self.clients:
            if ws is exclude:
                continue
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def broadcast(self, msg: dict, exclude: Optional[WebSocket] = None):
        await self._broadcast(msg, exclude)

    async def broadcast_all(self, msg: dict):
        await self._broadcast(msg)

    def count(self) -> int:
        return len(self.clients)


manager = SyncManager()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            data = await ws.receive_json()
            t = data.get("type")

            if t == "play":
                manager.state.playing = True
                manager.state.position = float(data.get("position", 0))
                manager.state.position_ts = time.time()
                await manager.broadcast(
                    {"type": "sync", "state": manager.state.to_dict()}, exclude=ws
                )
                await manager.broadcast_all(
                    {"type": "event", "message": "▶ Someone pressed play"}
                )

            elif t == "pause":
                manager.state.playing = False
                manager.state.position = float(data.get("position", 0))
                manager.state.position_ts = time.time()
                await manager.broadcast(
                    {"type": "sync", "state": manager.state.to_dict()}, exclude=ws
                )
                await manager.broadcast_all(
                    {"type": "event", "message": "⏸ Someone paused"}
                )

            elif t == "seek":
                manager.state.position = float(data.get("position", 0))
                manager.state.position_ts = time.time()
                await manager.broadcast(
                    {"type": "sync", "state": manager.state.to_dict()}, exclude=ws
                )

            elif t == "track":
                track = data.get("track")
                manager.state.track = track
                manager.state.playing = True
                manager.state.position = 0.0
                manager.state.position_ts = time.time()
                await manager.broadcast(
                    {"type": "sync", "state": manager.state.to_dict()}, exclude=ws
                )
                await manager.broadcast_all(
                    {"type": "event", "message": f"🎵 Now playing: {track}"}
                )

            elif t == "ping":
                await ws.send_json({"type": "pong", "server_time": time.time()})

    except WebSocketDisconnect:
        manager.disconnect(ws)
        count = manager.count()
        await manager.broadcast_all({"type": "users", "count": count})
        await manager.broadcast_all(
            {"type": "event", "message": f"A listener left · {count} online"}
        )


@app.get("/api/tracks")
async def get_tracks():
    tracks = []
    for f in sorted(MUSIC_DIR.iterdir()):
        if f.suffix.lower() == ".mp3":
            stat = f.stat()
            tracks.append(
                {
                    "name": f.name,
                    "url": f"/music/{f.name}",
                    "size_mb": round(stat.st_size / (1024 * 1024), 1),
                }
            )
    return tracks


@app.get("/api/users")
async def get_users():
    return {"count": manager.count()}


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".mp3"):
        raise HTTPException(400, "Only MP3 files are allowed")

    safe = Path(file.filename).name.replace(" ", "_")
    dest = MUSIC_DIR / safe
    size = 0

    with dest.open("wb") as out:
        while chunk := await file.read(65536):
            size += len(chunk)
            if size > 200 * 1024 * 1024:
                dest.unlink(missing_ok=True)
                raise HTTPException(413, "File exceeds 200 MB limit")
            out.write(chunk)

    await manager.broadcast_all(
        {"type": "event", "message": f"📤 Uploaded: {safe}"}
    )
    await manager.broadcast_all({"type": "tracks_updated"})
    return {"ok": True, "name": safe}


app.mount("/music", StaticFiles(directory=str(MUSIC_DIR)), name="music")
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
