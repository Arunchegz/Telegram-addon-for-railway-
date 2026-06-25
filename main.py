"""
TGStream — Railway deployment
FastAPI + Pyrogram + Redis (Railway addon)
No Nginx (Railway handles TLS/routing). No Docker Compose.
Env vars set in Railway dashboard.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import httpx
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pyrogram import Client, raw, utils
from pyrogram.errors import AuthBytesInvalid, FileReferenceExpired, FloodWait
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
API_ID             = int(os.getenv("API_ID", "0"))
API_HASH           = os.getenv("API_HASH", "")
SESSION_STRING     = os.getenv("SESSION_STRING", "")
BASE_URL           = os.getenv("BASE_URL", "")           # set to Railway domain
CHANNEL_USERNAME   = os.getenv("CHANNEL_USERNAME", "")
REDIS_URL          = os.getenv("REDIS_URL", "")          # auto-set by Railway Redis addon
SYNC_INTERVAL      = int(os.getenv("SYNC_INTERVAL", "300"))
STREAM_CONCURRENCY = int(os.getenv("STREAM_CONCURRENCY", "5"))

TG_CHUNK       = 1024 * 1024
STARTUP_CHUNKS = 8
TAIL_CHUNKS    = 8
CACHE_MAX      = 20

# Redis keys
R_MOVIES   = "tgstream:movies"
R_POSTER   = "tgstream:poster:{}"
R_SYNC_TS  = "tgstream:last_sync"
R_STARTUP  = "tgstream:startup:{}"
R_TAIL     = "tgstream:tail:{}"
R_SYNC_LCK = "tgstream:rate:sync"

# ── Pyrogram ──────────────────────────────────────────────────────────────────
tg = Client(
    "streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,
    workers=16,
)

redis_client: aioredis.Redis = None
byte_streamer: "ByteStreamer" = None
stream_sem: asyncio.Semaphore = None
_sync_lock = asyncio.Lock()

# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, byte_streamer, stream_sem

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=False)
    stream_sem = asyncio.Semaphore(STREAM_CONCURRENCY)

    await tg.start()
    byte_streamer = ByteStreamer(tg)
    print("✅ Pyrogram started")

    asyncio.create_task(_sync_loop())
    yield

    for session in list(tg.media_sessions.values()):
        try:
            await session.stop()
        except Exception:
            pass
    tg.media_sessions.clear()
    await tg.stop()
    await redis_client.aclose()
    print("🛑 Shutdown")


async def _sync_loop():
    while True:
        try:
            last = await redis_client.get(R_SYNC_TS)
            if not last or (time.time() - float(last)) > SYNC_INTERVAL:
                await _sync_channel()
        except Exception as e:
            print(f"[sync_loop] {e}")
        await asyncio.sleep(60)


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="TGStream", version="1.0.0", lifespan=lifespan, docs_url="/api/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
)

# ── ByteStreamer ──────────────────────────────────────────────────────────────
class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client

    async def yield_file(
        self, msg, offset: int, first_cut: int, last_cut: int, parts: int, chunk: int = TG_CHUNK
    ) -> AsyncGenerator[bytes, None]:
        async for b in self._stream(msg, offset, first_cut, last_cut, parts, chunk):
            yield b

    async def _stream(self, msg, offset, first_cut, last_cut, parts, chunk, _retry=True):
        fid = _extract_fid(msg)
        session = await self._session(fid)
        loc = _location(fid)
        current_part = 1
        current_off = offset

        try:
            r = await session.invoke(raw.functions.upload.GetFile(location=loc, offset=current_off, limit=chunk))
        except FileReferenceExpired:
            if not _retry:
                raise
            msg = await _fetch_msg(msg.id)
            async for b in self._stream(msg, offset, first_cut, last_cut, parts, chunk, False):
                yield b
            return

        if not isinstance(r, raw.types.upload.File):
            return

        while True:
            data = r.bytes
            if not data:
                break
            if parts == 1:
                yield data[first_cut:last_cut]
            elif current_part == 1:
                yield data[first_cut:]
            elif current_part == parts:
                yield data[:last_cut]
            else:
                yield data

            current_part += 1
            current_off += chunk
            if current_part > parts:
                break

            try:
                r = await session.invoke(raw.functions.upload.GetFile(location=loc, offset=current_off, limit=chunk))
            except FileReferenceExpired:
                if not _retry:
                    raise
                msg = await _fetch_msg(msg.id)
                async for b in self._stream(msg, current_off, 0, last_cut, parts - current_part + 1, chunk, False):
                    yield b
                return

    async def _session(self, fid: FileId) -> Session:
        c = self.client
        dc = fid.dc_id
        if dc in c.media_sessions:
            return c.media_sessions[dc]

        if dc != await c.storage.dc_id():
            s = Session(c, dc, await Auth(c, dc, await c.storage.test_mode()).create(), await c.storage.test_mode(), is_media=True)
            await s.start()
            for _ in range(6):
                exp = await c.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc))
                try:
                    await s.invoke(raw.functions.auth.ImportAuthorization(id=exp.id, bytes=exp.bytes))
                    break
                except AuthBytesInvalid:
                    continue
            else:
                await s.stop()
                raise AuthBytesInvalid
        else:
            s = Session(c, dc, await c.storage.auth_key(), await c.storage.test_mode(), is_media=True)
            await s.start()

        c.media_sessions[dc] = s
        return s


def _extract_fid(msg) -> FileId:
    media = msg.video or msg.document
    if not media:
        raise ValueError("No media")
    return FileId.decode(media.file_id)


def _location(fid: FileId):
    ft = fid.file_type
    if ft == FileType.CHAT_PHOTO:
        if fid.chat_id > 0:
            peer = raw.types.InputPeerUser(user_id=fid.chat_id, access_hash=fid.chat_access_hash)
        elif fid.chat_access_hash == 0:
            peer = raw.types.InputPeerChat(chat_id=-fid.chat_id)
        else:
            peer = raw.types.InputPeerChannel(channel_id=utils.get_channel_id(fid.chat_id), access_hash=fid.chat_access_hash)
        return raw.types.InputPeerPhotoFileLocation(peer=peer, volume_id=fid.volume_id, local_id=fid.local_id,
                                                    big=fid.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG)
    elif ft == FileType.PHOTO:
        return raw.types.InputPhotoFileLocation(id=fid.media_id, access_hash=fid.access_hash,
                                                file_reference=fid.file_reference, thumb_size=fid.thumbnail_size)
    else:
        return raw.types.InputDocumentFileLocation(id=fid.media_id, access_hash=fid.access_hash,
                                                   file_reference=fid.file_reference, thumb_size=fid.thumbnail_size)


# ── Helpers ───────────────────────────────────────────────────────────────────
async def _fetch_msg(msg_id: int):
    return await tg.get_messages(CHANNEL_USERNAME, msg_id)


def _get_media(msg):
    return msg.video or msg.document or None


def _is_empty(msg) -> bool:
    return getattr(msg, "empty", False) or _get_media(msg) is None


def _movie_id(filename: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", filename.lower())


def _fmt_size(size) -> str:
    if not size:
        return "Unknown"
    size = float(size)
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {u}"
        size /= 1024
    return f"{size:.1f} PB"


def _quality(fn: str) -> str:
    n = fn.lower()
    for tag in ["2160p", "4k", "1440p", "1080p", "720p", "480p", "360p"]:
        if tag in n:
            return tag.upper()
    return "Unknown"


def _source(fn: str) -> str:
    n = fn.lower()
    for tag in ["bluray", "bdrip", "web-dl", "webdl", "webrip", "hdrip", "dvdrip", "hdtv", "remux"]:
        if tag in n:
            return tag.upper()
    return ""


def _ctype(fn: str) -> str:
    n = fn.lower()
    if n.endswith(".mkv"):
        return "video/x-matroska"
    if n.endswith(".webm"):
        return "video/webm"
    return "video/mp4"


def _normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[._\-–—+]", " ", text)
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _flex_match(title: str, filename: str) -> bool:
    tn, fn = _normalize(title), _normalize(filename)
    if not tn or not fn:
        return False
    if tn in fn:
        return True
    tw, fw = tn.split(), fn.split()
    return sum(1 for w in tw if w in fw) >= max(1, len(tw) * 0.7)


def _parse_title_year(filename: str) -> tuple[str, str]:
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._]", " ", name)
    ym = re.search(r"\b(19|20)\d{2}\b", name)
    year = ym.group(0) if ym else ""
    cut = re.split(
        r"\b(?:19|20)\d{2}\b|\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|bdrip|hdrip|"
        r"remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE,
    )[0]
    return re.sub(r"\s+", " ", cut).strip().title(), year


def _parse_series(filename: str) -> Optional[dict]:
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,3})", filename)
    if m:
        return {"season": int(m.group(1)), "episode": int(m.group(2))}
    m2 = re.search(r"[Ss]eason\s*(\d+).*?[Ee]pisode\s*(\d+)", filename, re.IGNORECASE)
    if m2:
        return {"season": int(m2.group(1)), "episode": int(m2.group(2))}
    return None


# ── Redis helpers ─────────────────────────────────────────────────────────────
async def _load_movies() -> dict:
    raw_data = await redis_client.hgetall(R_MOVIES)
    return {k.decode(): json.loads(v) for k, v in raw_data.items()}


async def _save_movie(mid: str, data: dict):
    await redis_client.hset(R_MOVIES, mid, json.dumps(data))


async def _del_movie(mid: str):
    await redis_client.hdel(R_MOVIES, mid)


async def _get_poster(filename: str) -> str:
    key = R_POSTER.format(filename[:80])
    cached = await redis_client.get(key)
    if cached:
        return cached.decode()
    url = await _fetch_poster(filename)
    await redis_client.setex(key, 86400, url)
    return url


async def _fetch_poster(filename: str) -> str:
    title, year = _parse_title_year(filename)
    if not title:
        return "https://via.placeholder.com/300x450?text=No+Poster"
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f"https://v3-cinemeta.strem.io/catalog/movie/top/search={title} {year}.json")
            metas = r.json().get("metas", [])
            if metas and metas[0].get("poster"):
                return metas[0]["poster"]
    except Exception:
        pass
    return f"https://via.placeholder.com/300x450?text={title.replace(' ', '+')}"


async def _get_cinemeta(type_name: str, imdb_id: str) -> tuple[str, str]:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"https://v3-cinemeta.strem.io/meta/{type_name}/{imdb_id}.json")
            meta = r.json().get("meta", {})
            return meta.get("name", ""), str(meta.get("year", ""))
    except Exception:
        return "", ""


# ── Sync ──────────────────────────────────────────────────────────────────────
async def _sync_channel() -> int:
    async with _sync_lock:
        # Distributed lock — guards against multi-replica stampede if Railway scales
        acquired = await redis_client.set(R_SYNC_LCK, "1", ex=600, nx=True)
        if not acquired:
            return 0
        try:
            count = 0
            async for msg in tg.get_chat_history(CHANNEL_USERNAME):
                try:
                    media = _get_media(msg)
                    if not media:
                        continue
                    fn = getattr(media, "file_name", None)
                    if not fn:
                        continue
                    mid = _movie_id(fn)
                    await _save_movie(mid, {
                        "message_id":     msg.id,
                        "file_name":      fn,
                        "file_size":      media.file_size,
                        "file_size_text": _fmt_size(media.file_size),
                        "quality":        _quality(fn),
                        "source":         _source(fn),
                        "synced_at":      int(time.time()),
                    })
                    count += 1
                except Exception:
                    continue
            await redis_client.set(R_SYNC_TS, str(time.time()))
            print(f"✅ Sync: {count} movies")
            return count
        finally:
            await redis_client.delete(R_SYNC_LCK)


# ── Cache warm-up ─────────────────────────────────────────────────────────────
async def _warm_startup(msg, mid: str):
    if await redis_client.exists(R_STARTUP.format(mid)):
        return
    data = bytearray()
    async for chunk in byte_streamer.yield_file(msg, 0, 0, TG_CHUNK, STARTUP_CHUNKS):
        data.extend(chunk)
    await redis_client.setex(R_STARTUP.format(mid), 3600, bytes(data))
    print(f"[{mid}] startup warm {len(data)/1024/1024:.1f}MB")


async def _warm_tail(msg, mid: str, file_size: int):
    if await redis_client.exists(R_TAIL.format(mid)):
        return
    import base64
    offset = max(0, (file_size // TG_CHUNK) - TAIL_CHUNKS)
    data = bytearray()
    async for chunk in byte_streamer.yield_file(msg, offset * TG_CHUNK, 0, TG_CHUNK, TAIL_CHUNKS):
        data.extend(chunk)
    payload = json.dumps({"start": offset * TG_CHUNK, "data": base64.b64encode(bytes(data)).decode()})
    await redis_client.setex(R_TAIL.format(mid), 3600, payload)
    print(f"[{mid}] tail warm {len(data)/1024/1024:.1f}MB")


# ── Manifest ──────────────────────────────────────────────────────────────────
MANIFEST = {
    "id":          "org.tgstream.railway",
    "version":     "1.0.0",
    "name":        "TGStream",
    "description": "Stream Telegram channel media via Stremio",
    "resources":   ["catalog", "meta", "stream"],
    "types":       ["movie", "series"],
    "idPrefixes":  ["tgm:", "tgs:", "tt"],
    "catalogs": [
        {"type": "movie",  "id": "tgstream_movies", "name": "TG Movies"},
        {"type": "series", "id": "tgstream_series", "name": "TG Series"},
    ],
    "behaviorHints": {"configurable": False, "configurationRequired": False},
}


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/")
async def health():
    movies = await redis_client.hlen(R_MOVIES)
    last = await redis_client.get(R_SYNC_TS)
    age = round((time.time() - float(last)) / 60, 1) if last else None
    return {"status": "ok", "movies": movies, "channel": CHANNEL_USERNAME, "sync_age_min": age}


@app.get("/manifest.json")
async def manifest():
    return JSONResponse(MANIFEST)


@app.get("/sync")
async def manual_sync():
    return {"synced": await _sync_channel()}


@app.get("/debug/movies")
async def debug_movies():
    return await _load_movies()


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Lightweight admin UI — no Gradio dependency."""
    movies = await redis_client.hlen(R_MOVIES)
    last = await redis_client.get(R_SYNC_TS)
    age = round((time.time() - float(last)) / 60, 1) if last else "never"
    manifest_url = f"{BASE_URL}/manifest.json"
    stremio_url = manifest_url.replace("https://", "stremio://").replace("http://", "stremio://")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TGStream</title>
<style>
  :root{{--bg:#0d0f14;--s:#161a22;--s2:#1e2330;--b:#262d3d;--a:#5b8cf5;--a2:#3dffd0;
         --t:#e2e8f0;--m:#637089;--g:#48bb78;--y:#f6ad55;--r:#f56565;}}
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{font-family:Inter,system-ui,sans-serif;background:var(--bg);color:var(--t);
       display:flex;flex-direction:column;min-height:100vh;}}
  header{{background:var(--s);border-bottom:1px solid var(--b);padding:16px 28px;
          display:flex;align-items:center;justify-content:space-between;}}
  .logo{{font-size:18px;font-weight:700;color:var(--a);letter-spacing:-.3px;}}
  .dot{{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--g);
        margin-right:6px;animation:pulse 2s ease-in-out infinite;}}
  @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.35}}}}
  main{{max-width:900px;margin:0 auto;width:100%;padding:28px 20px;display:flex;flex-direction:column;gap:20px;}}
  .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;}}
  .card{{background:var(--s);border:1px solid var(--b);border-radius:10px;padding:18px 20px;}}
  .cl{{font-size:11px;text-transform:uppercase;letter-spacing:.7px;color:var(--m);margin-bottom:8px;}}
  .cv{{font-size:26px;font-weight:700;font-family:'JetBrains Mono',monospace;}}
  .cv.b{{color:var(--a)}} .cv.g{{color:var(--g)}} .cv.y{{color:var(--y)}} .cv.t{{color:var(--a2)}}
  .panel{{background:var(--s);border:1px solid var(--b);border-radius:10px;overflow:hidden;}}
  .ph{{padding:13px 18px;border-bottom:1px solid var(--b);font-size:13px;font-weight:600;
       display:flex;align-items:center;justify-content:space-between;}}
  .pb{{padding:18px;}}
  .url-box{{background:#0a0c10;border:1px solid var(--b);border-radius:6px;padding:11px 14px;
            font-family:monospace;font-size:12px;color:var(--a2);word-break:break-all;
            display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px;}}
  .copy-btn{{background:var(--s2);border:1px solid var(--b);border-radius:4px;padding:4px 10px;
             font-size:11px;color:var(--m);cursor:pointer;transition:all .15s;white-space:nowrap;}}
  .copy-btn:hover{{color:var(--a2);border-color:var(--a2);}}
  .btn{{padding:8px 16px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;border:none;
        transition:all .15s;display:inline-flex;align-items:center;gap:6px;text-decoration:none;}}
  .btn-p{{background:var(--a);color:#fff;}} .btn-p:hover{{background:#4a7ce8;}}
  .btn-g{{background:var(--s2);color:var(--t);border:1px solid var(--b);}} .btn-g:hover{{border-color:var(--a);color:var(--a);}}
  .btn:disabled{{opacity:.5;cursor:not-allowed;}}
  #sync-res{{font-size:13px;color:var(--g);margin-top:10px;min-height:20px;}}
  table{{width:100%;border-collapse:collapse;font-size:12px;}}
  thead th{{text-align:left;padding:8px 10px;font-size:10px;text-transform:uppercase;
            letter-spacing:.6px;color:var(--m);border-bottom:1px solid var(--b);}}
  tbody tr{{border-bottom:1px solid var(--b);}} tbody tr:hover{{background:var(--s2);}}
  tbody tr:last-child{{border-bottom:none;}}
  td{{padding:9px 10px;max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
  .badge{{display:inline-flex;padding:1px 7px;border-radius:3px;font-size:10px;font-weight:700;}}
  .b1080{{background:#1d3454;color:#60a5fa;}} .b2160{{background:#2d1f4e;color:#a78bfa;}}
  .b720{{background:#1a3a2e;color:#34d399;}}  .bdef{{background:#2d2d2d;color:#6b7280;}}
  .search{{width:100%;background:var(--s2);border:1px solid var(--b);border-radius:6px;
           padding:8px 12px;font-size:13px;color:var(--t);outline:none;font-family:inherit;margin-bottom:12px;}}
  .search:focus{{border-color:var(--a);}} .search::placeholder{{color:var(--m);}}
  footer{{margin-top:auto;padding:16px 28px;border-top:1px solid var(--b);
          font-size:11px;color:var(--m);text-align:center;}}
</style>
</head>
<body>
<header>
  <div class="logo">📡 TGStream</div>
  <div style="font-size:12px;color:var(--m);"><span class="dot"></span>Railway · {CHANNEL_USERNAME}</div>
</header>
<main>
  <div class="cards">
    <div class="card"><div class="cl">Movies Indexed</div><div class="cv b">{movies}</div></div>
    <div class="card"><div class="cl">Status</div><div class="cv g">Online</div></div>
    <div class="card"><div class="cl">Last Sync</div><div class="cv y" id="age">{age}m ago</div></div>
    <div class="card"><div class="cl">Channel</div><div class="cv t" style="font-size:14px;padding-top:6px;">{CHANNEL_USERNAME}</div></div>
  </div>

  <div class="panel">
    <div class="ph">Install in Stremio</div>
    <div class="pb">
      <div style="font-size:11px;color:var(--m);margin-bottom:6px;text-transform:uppercase;letter-spacing:.6px;">Manifest URL</div>
      <div class="url-box"><span>{manifest_url}</span><button class="copy-btn" onclick="cp('{manifest_url}',this)">Copy</button></div>
      <div style="font-size:11px;color:var(--m);margin-bottom:6px;text-transform:uppercase;letter-spacing:.6px;">Stremio deep link</div>
      <div class="url-box"><span>{stremio_url}</span><button class="copy-btn" onclick="cp('{stremio_url}',this)">Copy</button></div>
      <a href="{stremio_url}" class="btn btn-p" style="margin-top:8px;">Open in Stremio</a>
    </div>
  </div>

  <div class="panel">
    <div class="ph"><span>Library</span>
      <div style="display:flex;gap:8px;">
        <button class="btn btn-g" onclick="loadMovies()">Refresh</button>
        <button class="btn btn-p" id="sync-btn" onclick="doSync()">⚡ Sync Now</button>
      </div>
    </div>
    <div class="pb">
      <div id="sync-res"></div>
      <input class="search" placeholder="Filter by filename..." oninput="filterTable(this.value)">
      <div style="overflow-x:auto;">
        <table>
          <thead><tr><th>Filename</th><th>Quality</th><th>Source</th><th>Size</th><th>Msg ID</th></tr></thead>
          <tbody id="tbody"><tr><td colspan="5" style="text-align:center;color:var(--m);padding:20px;">Loading...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>
</main>
<footer>TGStream · Railway · <a href="/api/docs" style="color:var(--a);text-decoration:none;">API Docs</a> · <a href="/manifest.json" style="color:var(--a);text-decoration:none;">Manifest</a></footer>

<script>
let allMovies = {{}};

async function loadMovies() {{
  try {{
    const r = await fetch('/debug/movies');
    allMovies = await r.json();
    renderTable(allMovies);
  }} catch(e) {{ console.error(e); }}
}}

function badgeCls(q) {{
  if(q.includes('1080')) return 'b1080';
  if(q.includes('2160')||q.includes('4K')) return 'b2160';
  if(q.includes('720')) return 'b720';
  return 'bdef';
}}

function renderTable(movies) {{
  const entries = Object.entries(movies);
  if(!entries.length) {{
    document.getElementById('tbody').innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--m);padding:20px;">No movies — run sync</td></tr>';
    return;
  }}
  document.getElementById('tbody').innerHTML = entries.map(([id,m]) => {{
    const q = m.quality||'Unknown';
    return `<tr>
      <td title="${{m.file_name||id}}">${{m.file_name||id}}</td>
      <td><span class="badge ${{badgeCls(q)}}">${{q}}</span></td>
      <td style="color:var(--m)">${{m.source||'—'}}</td>
      <td style="font-family:monospace">${{m.file_size_text||'—'}}</td>
      <td style="font-family:monospace;color:var(--m)">${{m.message_id}}</td>
    </tr>`;
  }}).join('');
}}

function filterTable(q) {{
  q = q.toLowerCase();
  renderTable(Object.fromEntries(Object.entries(allMovies).filter(([id,m])=>(m.file_name||id).toLowerCase().includes(q))));
}}

async function doSync() {{
  const btn = document.getElementById('sync-btn');
  const res = document.getElementById('sync-res');
  btn.disabled = true;
  btn.textContent = 'Syncing...';
  res.textContent = '';
  try {{
    const r = await fetch('/sync');
    const d = await r.json();
    res.textContent = `✓ Synced ${{d.synced}} files`;
    await loadMovies();
  }} catch(e) {{
    res.style.color = 'var(--r)';
    res.textContent = '✗ Sync failed';
  }} finally {{
    btn.disabled = false;
    btn.textContent = '⚡ Sync Now';
  }}
}}

function cp(text, btn) {{
  navigator.clipboard.writeText(text);
  const orig = btn.textContent;
  btn.textContent = 'Copied!';
  btn.style.color = 'var(--g)';
  setTimeout(()=>{{ btn.textContent=orig; btn.style.color=''; }}, 1500);
}}

loadMovies();
</script>
</body>
</html>"""


@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    movies = await _load_movies()

    def is_series(m: dict) -> bool:
        return bool(re.search(r"s\d{2}e\d{2}|season\s*\d|episode\s*\d", m.get("file_name", "").lower()))

    filtered = {mid: m for mid, m in movies.items() if (type == "series") == is_series(m)}

    async def build_meta(mid: str, m: dict):
        fn = m.get("file_name", "Unknown")
        poster = await _get_poster(fn)
        title, year = _parse_title_year(fn)
        prefix = "tgm:" if type == "movie" else "tgs:"
        return {"id": f"{prefix}{mid}", "type": type, "name": title or fn,
                "poster": poster, "posterShape": "poster", "year": year}

    metas = await asyncio.gather(*[build_meta(mid, m) for mid, m in filtered.items()])
    return JSONResponse({"metas": list(metas)}, headers={"Cache-Control": "no-store"})


@app.get("/meta/{type}/{id}.json")
async def meta(type: str, id: str):
    if id.startswith("tt"):
        title, year = await _get_cinemeta(type, id)
        return JSONResponse({"meta": {"id": id, "type": type, "name": title, "year": year}})

    prefix = "tgm:" if type == "movie" else "tgs:"
    clean = id[len(prefix):] if id.startswith(prefix) else id
    movies = await _load_movies()
    movie = movies.get(clean)
    if not movie:
        return JSONResponse({"meta": {}})
    fn = movie.get("file_name", "Unknown")
    title, year = _parse_title_year(fn)
    return JSONResponse({"meta": {
        "id": id, "type": type, "name": title or fn, "year": year,
        "poster": await _get_poster(fn), "description": fn, "posterShape": "poster",
    }})


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    movies = await _load_movies()
    prefix = "tgm:" if type == "movie" else "tgs:"

    if id.startswith("tt"):
        parts = id.split(":")
        imdb_id, season, episode = parts[0], (int(parts[1]) if len(parts) > 1 else None), (int(parts[2]) if len(parts) > 2 else None)
        title, year = await _get_cinemeta(type, imdb_id)
        if not title:
            return JSONResponse({"streams": []})
        streams = []
        for mid, m in movies.items():
            fn = m.get("file_name", "")
            if not _flex_match(title, fn):
                continue
            if year:
                try:
                    my = int(year)
                    if not any(str(my + d) in fn for d in (-1, 0, 1)):
                        continue
                except ValueError:
                    if year not in fn:
                        continue
            if season and episode:
                info = _parse_series(fn)
                if info and (info["season"] != season or info["episode"] != episode):
                    continue
            q, sz, src = m.get("quality","Unknown"), m.get("file_size_text","Unknown"), m.get("source","")
            streams.append({"name": "TGStream", "title": f"{fn}\n{q}{' | '+src if src else ''} | {sz}", "url": f"{BASE_URL}/proxy/{mid}"})
        return JSONResponse({"streams": streams})

    clean = id[len(prefix):] if id.startswith(prefix) else id
    movie = movies.get(clean)
    if not movie:
        return JSONResponse({"streams": []})

    try:
        msg = await _fetch_msg(movie["message_id"])
        if _is_empty(msg):
            await _del_movie(clean)
            return JSONResponse({"streams": []})
        asyncio.create_task(_warm_startup(msg, clean))
        if movie.get("file_size"):
            asyncio.create_task(_warm_tail(msg, clean, movie["file_size"]))
    except Exception as e:
        print(f"[stream] {e}")
        return JSONResponse({"streams": []})

    fn = movie.get("file_name", "Unknown")
    q, sz, src = movie.get("quality","Unknown"), movie.get("file_size_text","Unknown"), movie.get("source","")
    return JSONResponse({"streams": [{"name": "TGStream",
        "title": f"{fn}\n⚙️ {q}{' | '+src if src else ''} | 💾 {sz}",
        "url": f"{BASE_URL}/proxy/{clean}"}]})


@app.api_route("/proxy/{movie_id}", methods=["GET", "HEAD"])
async def proxy(movie_id: str, request: Request):
    movies = await _load_movies()
    movie = movies.get(movie_id)
    if not movie:
        raise HTTPException(404, "Not found")

    try:
        msg = await _fetch_msg(movie["message_id"])
    except FloodWait as e:
        raise HTTPException(503, f"Rate limited — retry after {e.value}s")
    except Exception:
        raise HTTPException(502, "Telegram unavailable")

    if _is_empty(msg):
        await _del_movie(movie_id)
        raise HTTPException(404, "Deleted from Telegram")

    media = _get_media(msg)
    file_size = movie.get("file_size") or media.file_size
    filename = movie.get("file_name", "video.mp4")
    ctype = _ctype(filename)
    etag = f'"{movie["message_id"]}-{file_size}"'

    if request.method == "HEAD":
        return Response(status_code=206, headers={
            "Accept-Ranges": "bytes", "Content-Range": f"bytes 0-{file_size-1}/{file_size}",
            "Content-Length": str(file_size), "Content-Type": ctype,
            "Cache-Control": "public, max-age=3600", "ETag": etag,
        })

    start, end = 0, file_size - 1
    rh = request.headers.get("range", "")
    if rh.startswith("bytes="):
        spec = rh[6:]
        try:
            if spec.startswith("-"):
                start = max(0, file_size - int(spec[1:]))
                end = file_size - 1
            else:
                p = spec.split("-")
                if p[0]:
                    start = int(p[0])
                if len(p) > 1 and p[1]:
                    end = int(p[1])
        except Exception:
            raise HTTPException(416, "Invalid Range")

    if not rh:
        end = min(STARTUP_CHUNKS * TG_CHUNK - 1, file_size - 1)
    elif rh.endswith("-"):
        end = min(start + STARTUP_CHUNKS * TG_CHUNK - 1, file_size - 1)
    else:
        end = min(end, file_size - 1)

    total = end - start + 1

    # Startup cache hit
    if start < STARTUP_CHUNKS * TG_CHUNK:
        raw_cache = await redis_client.get(R_STARTUP.format(movie_id))
        if raw_cache:
            ce = min(end, len(raw_cache) - 1)
            if ce >= start:
                return Response(content=raw_cache[start:ce+1], status_code=206, headers={
                    "Accept-Ranges": "bytes", "Content-Range": f"bytes {start}-{ce}/{file_size}",
                    "Content-Length": str(ce-start+1), "Content-Type": ctype, "ETag": etag,
                })
        else:
            asyncio.create_task(_warm_startup(msg, movie_id))

    # Tail cache hit
    if start >= file_size - TAIL_CHUNKS * TG_CHUNK:
        import base64
        raw_tail = await redis_client.get(R_TAIL.format(movie_id))
        if raw_tail:
            tail = json.loads(raw_tail)
            td, ts = base64.b64decode(tail["data"]), tail["start"]
            rs, re_ = start - ts, min(end - ts, len(td) - 1)
            if rs >= 0 and re_ >= rs:
                return Response(content=td[rs:re_+1], status_code=206, headers={
                    "Accept-Ranges": "bytes", "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(re_-rs+1), "Content-Type": ctype, "ETag": etag,
                })

    # Live MTProto stream
    aligned = (start // TG_CHUNK) * TG_CHUNK
    first_cut = start - aligned
    last_cut = (end % TG_CHUNK) + 1
    part_count = math.ceil((end + 1) / TG_CHUNK) - (aligned // TG_CHUNK)

    async def _stream_gen():
        async with stream_sem:
            async for chunk in byte_streamer.yield_file(msg, aligned, first_cut, last_cut, part_count):
                if await request.is_disconnected():
                    break
                yield chunk

    return StreamingResponse(_stream_gen(), status_code=206, media_type=ctype, headers={
        "Accept-Ranges": "bytes", "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(total), "Content-Type": ctype,
        "Cache-Control": "public, max-age=3600", "ETag": etag, "Vary": "Range",
    })
