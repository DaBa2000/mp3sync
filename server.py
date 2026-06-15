import asyncio
import secrets
import shutil
import string
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent
MUSIC_DIR = BASE_DIR / "music"
PLAYLISTS_DIR = MUSIC_DIR / "_playlists"
STATIC_DIR = BASE_DIR / "static"

MUSIC_DIR.mkdir(exist_ok=True)
PLAYLISTS_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

AUDIO_EXT = {".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac", ".webm"}
ROOM_IDLE_SECONDS = 300  # 5 minutes


def is_audio(name: str) -> bool:
    return Path(name).suffix.lower() in AUDIO_EXT


def safe_filename(raw: str) -> str:
    return Path(raw).name.replace(" ", "_")


def safe_playlist_name(raw: str) -> str:
    name = raw.strip()
    if not name or "/" in name or "\\" in name or name.startswith("."):
        raise HTTPException(400, "Invalid playlist name")
    return name


app = FastAPI(title="MP3Sync")


# ── PLAYER STATE ──────────────────────────────────────────────────────────

class PlayerState:
    def __init__(self):
        self.source: Optional[str] = None
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
            "source": self.source,
            "track": self.track,
            "playing": self.playing,
            "position": self.effective_position(),
            "server_time": time.time(),
        }


# ── ROOMS ─────────────────────────────────────────────────────────────────

class Room:
    def __init__(self, code: str):
        self.code = code
        self.created_at = time.time()
        self.state = PlayerState()
        self.clients: dict[WebSocket, str] = {}   # ws → display name
        self.cleanup_task: Optional[asyncio.Task] = None

    @property
    def user_count(self) -> int:
        return len(self.clients)

    def user_names(self) -> list[str]:
        return list(self.clients.values())

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "users": self.user_count,
            "names": self.user_names(),
            "track": self.state.track,
            "source": self.state.source,
            "playing": self.state.playing,
            "created_at": self.created_at,
        }

    async def broadcast(self, msg: dict, exclude: Optional[WebSocket] = None):
        dead = []
        for ws in list(self.clients):
            if ws is exclude:
                continue
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.pop(ws, None)

    async def broadcast_all(self, msg: dict):
        await self.broadcast(msg)


class RoomManager:
    def __init__(self):
        self.rooms: dict[str, Room] = {}

    def create(self) -> Room:
        while True:
            code = "".join(secrets.choice(string.digits) for _ in range(6))
            if code not in self.rooms:
                break
        room = Room(code)
        self.rooms[code] = room
        return room

    def get(self, code: str) -> Optional[Room]:
        return self.rooms.get(code)

    def delete(self, code: str):
        self.rooms.pop(code, None)

    def list_all(self) -> list[dict]:
        return sorted(self.rooms.values(), key=lambda r: r.created_at, reverse=True)


room_manager = RoomManager()


async def _schedule_cleanup(room: Room):
    try:
        await asyncio.sleep(ROOM_IDLE_SECONDS)
        if room.user_count == 0:
            room_manager.delete(room.code)
    except asyncio.CancelledError:
        pass


# ── WEBSOCKET (per room) ──────────────────────────────────────────────────

@app.websocket("/ws/{room_code}")
async def room_ws(ws: WebSocket, room_code: str, name: str = "Anonymous"):
    room = room_manager.get(room_code)
    if not room:
        await ws.close(code=4404)
        return

    # Cancel pending idle-cleanup
    if room.cleanup_task and not room.cleanup_task.done():
        room.cleanup_task.cancel()
        room.cleanup_task = None

    await ws.accept()
    name = (name or "Anonymous").strip()[:32] or "Anonymous"
    room.clients[ws] = name

    def _users_msg() -> dict:
        return {"type": "users", "count": room.user_count, "names": room.user_names()}

    await ws.send_json({"type": "sync", "state": room.state.to_dict()})
    await ws.send_json(_users_msg())
    await room.broadcast({"type": "event", "message": f"👤 {name} joined"}, exclude=ws)
    await room.broadcast_all(_users_msg())

    try:
        while True:
            data = await ws.receive_json()
            t = data.get("type")

            if t == "play":
                room.state.playing = True
                room.state.position = float(data.get("position", 0))
                room.state.position_ts = time.time()
                await room.broadcast({"type": "sync", "state": room.state.to_dict()}, exclude=ws)
                await room.broadcast_all({"type": "event", "message": f"▶ {name} pressed play"})

            elif t == "pause":
                room.state.playing = False
                room.state.position = float(data.get("position", 0))
                room.state.position_ts = time.time()
                await room.broadcast({"type": "sync", "state": room.state.to_dict()}, exclude=ws)
                await room.broadcast_all({"type": "event", "message": f"⏸ {name} paused"})

            elif t == "seek":
                room.state.position = float(data.get("position", 0))
                room.state.position_ts = time.time()
                await room.broadcast({"type": "sync", "state": room.state.to_dict()}, exclude=ws)

            elif t == "track":
                track = data.get("track")
                source = data.get("source")
                room.state.track = track
                room.state.source = source
                room.state.playing = True
                room.state.position = 0.0
                room.state.position_ts = time.time()
                await room.broadcast({"type": "sync", "state": room.state.to_dict()}, exclude=ws)
                label = f"{source} › {track}" if source else track
                await room.broadcast_all({"type": "event", "message": f"🎵 {name}: {label}"})

            elif t == "ping":
                await ws.send_json({"type": "pong", "server_time": time.time()})

    except WebSocketDisconnect:
        room.clients.pop(ws, None)
        await room.broadcast_all({"type": "event", "message": f"👤 {name} left"})
        await room.broadcast_all(_users_msg())
        if room.user_count == 0:
            room.cleanup_task = asyncio.create_task(_schedule_cleanup(room))


# ── ROOM API ──────────────────────────────────────────────────────────────

@app.get("/api/rooms")
async def list_rooms():
    return [r.to_dict() for r in room_manager.list_all()]


@app.post("/api/rooms")
async def create_room():
    room = room_manager.create()
    return {"code": room.code}


@app.delete("/api/rooms/{code}")
async def delete_room(code: str):
    room = room_manager.get(code)
    if not room:
        raise HTTPException(404, "Room not found")
    for ws in list(room.clients):
        try:
            await ws.close(code=4001)
        except Exception:
            pass
    room_manager.delete(code)
    return {"ok": True}


@app.get("/room/{code}")
async def room_page(code: str):
    return FileResponse(STATIC_DIR / "room.html")


# ── LIBRARY ───────────────────────────────────────────────────────────────

@app.get("/api/tracks")
async def get_library():
    tracks = []
    for f in sorted(MUSIC_DIR.iterdir()):
        if f.is_file() and is_audio(f.name):
            stat = f.stat()
            tracks.append({
                "name": f.name,
                "url": f"/music/{f.name}",
                "size_mb": round(stat.st_size / (1024 * 1024), 1),
            })
    return tracks


@app.post("/api/upload")
async def upload_library(file: UploadFile = File(...)):
    if not file.filename or not is_audio(file.filename):
        raise HTTPException(400, f"Unsupported type. Allowed: {', '.join(sorted(AUDIO_EXT))}")
    sn = safe_filename(file.filename)
    await _stream_upload(file, MUSIC_DIR / sn)
    await _notify_all_rooms({"type": "tracks_updated"})
    return {"ok": True, "name": sn}


@app.delete("/api/tracks/{filename}")
async def delete_library_track(filename: str):
    sn = safe_filename(filename)
    if sn != filename:
        raise HTTPException(400, "Invalid filename")
    target = MUSIC_DIR / sn
    if not target.exists() or not target.is_file():
        raise HTTPException(404, "Track not found")
    target.unlink()
    for room in room_manager.rooms.values():
        if room.state.source is None and room.state.track == sn:
            _clear_state(room)
            await room.broadcast_all({"type": "sync", "state": room.state.to_dict()})
    await _notify_all_rooms({"type": "tracks_updated"})
    return {"ok": True}


# ── PLAYLISTS ─────────────────────────────────────────────────────────────

@app.get("/api/playlists")
async def list_playlists():
    result = []
    for d in sorted(PLAYLISTS_DIR.iterdir()):
        if d.is_dir() and not d.name.startswith("."):
            count = sum(1 for f in d.iterdir() if f.is_file() and is_audio(f.name))
            result.append({"name": d.name, "count": count})
    return result


@app.post("/api/playlists")
async def create_playlist(body: dict):
    name = safe_playlist_name(body.get("name", ""))
    pl_dir = PLAYLISTS_DIR / name
    if pl_dir.exists():
        raise HTTPException(409, "Playlist already exists")
    pl_dir.mkdir()
    await _notify_all_rooms({"type": "playlists_updated"})
    return {"ok": True, "name": name}


@app.delete("/api/playlists/{name}")
async def delete_playlist(name: str):
    pl_dir = PLAYLISTS_DIR / name
    if not pl_dir.exists() or not pl_dir.is_dir():
        raise HTTPException(404, "Playlist not found")
    shutil.rmtree(pl_dir)
    for room in room_manager.rooms.values():
        if room.state.source == name:
            _clear_state(room)
            await room.broadcast_all({"type": "sync", "state": room.state.to_dict()})
    await _notify_all_rooms({"type": "playlists_updated"})
    return {"ok": True}


@app.get("/api/playlists/{name}")
async def get_playlist_tracks(name: str):
    pl_dir = PLAYLISTS_DIR / name
    if not pl_dir.exists():
        raise HTTPException(404, "Playlist not found")
    tracks = []
    for f in sorted(pl_dir.iterdir()):
        if f.is_file() and is_audio(f.name):
            stat = f.stat()
            tracks.append({
                "name": f.name,
                "url": f"/music/_playlists/{name}/{f.name}",
                "size_mb": round(stat.st_size / (1024 * 1024), 1),
            })
    return tracks


@app.post("/api/playlists/{name}/upload")
async def upload_to_playlist(name: str, file: UploadFile = File(...)):
    pl_dir = PLAYLISTS_DIR / name
    if not pl_dir.exists():
        raise HTTPException(404, "Playlist not found")
    if not file.filename or not is_audio(file.filename):
        raise HTTPException(400, f"Unsupported type. Allowed: {', '.join(sorted(AUDIO_EXT))}")
    sn = safe_filename(file.filename)
    await _stream_upload(file, pl_dir / sn)
    await _notify_all_rooms({"type": "playlist_updated", "name": name})
    return {"ok": True, "name": sn}


@app.delete("/api/playlists/{playlist}/{filename}")
async def delete_playlist_track(playlist: str, filename: str):
    sn = Path(filename).name
    target = PLAYLISTS_DIR / playlist / sn
    if not target.exists() or not target.is_file():
        raise HTTPException(404, "Track not found")
    target.unlink()
    for room in room_manager.rooms.values():
        if room.state.source == playlist and room.state.track == sn:
            _clear_state(room)
            await room.broadcast_all({"type": "sync", "state": room.state.to_dict()})
    await _notify_all_rooms({"type": "playlist_updated", "name": playlist})
    return {"ok": True}


# ── HELPERS ───────────────────────────────────────────────────────────────

def _clear_state(room: Room):
    room.state.source = None
    room.state.track = None
    room.state.playing = False
    room.state.position = 0.0
    room.state.position_ts = time.time()


async def _notify_all_rooms(msg: dict):
    for room in room_manager.rooms.values():
        await room.broadcast_all(msg)


async def _stream_upload(file: UploadFile, dest: Path) -> None:
    size = 0
    with dest.open("wb") as out:
        while chunk := await file.read(65536):
            size += len(chunk)
            if size > 200 * 1024 * 1024:
                dest.unlink(missing_ok=True)
                raise HTTPException(413, "File exceeds 200 MB limit")
            out.write(chunk)


app.mount("/music", StaticFiles(directory=str(MUSIC_DIR)), name="music")
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
