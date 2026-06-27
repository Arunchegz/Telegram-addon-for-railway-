"""
main.py — TGStream Hybrid Predictive Streamer
Railway deployment.

Proxy logic (the core of this rewrite):
  1. On first stream request -> start DownloadTask (background sequential fetch)
  2. For each Range request:
     a. Check DownloadMap: is [start,end] fully on disk?
        YES -> serve from SparseFile (pread)        <- zero Telegram cost, instant
        NO  -> serve from Telegram live (ByteStreamer)
     b. Hint downloader about play-head position
  3. Player never notices the switch.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pyrogram import Client
from pyrogram.errors import FloodWait

import state as st
from downloader import DownloadMap, download_manager, STORAGE_DIR
from streamer import ByteStreamer, TG_CHUNK

load_dotenv()

API_ID             = int(os.getenv("API_ID", "0"))
API_HASH           = os.getenv("API_HASH", "")
SESSION_STRING     = os.getenv("SESSION_STRING", "")
BASE_URL           = os.getenv("BASE_URL", "")
CHANNEL_USERNAME   = os.getenv("CHANNEL_USERNAME", "")
REDIS_URL          = os.getenv("REDIS_URL", "")
SYNC_INTERVAL      = int(os.getenv("SYNC_INTERVAL", "300"))
STREAM_CONCURRENCY = int(os.getenv("STREAM_CONCURRENCY", "5"))
WAIT_TIMEOUT_S     = float(os.getenv("WAIT_TIMEOUT_S", "2.0"))
STARTUP_CHUNKS     = int(os.getenv("STARTUP_CHUNKS", "4"))
LOCAL_READ_CHUNK   = int(os.getenv("LOCAL_READ_CHUNK", str(1024 * 1024)))

tg: Client = None
redis_client: aioredis.Redis = None
byte_streamer: ByteStreamer = None
stream_sem: asyncio.Semaphore = None
_sync_lock = asyncio.Lock()


def _schedule(coro):
    task = asyncio.create_task(coro)
    task.add_done_callback(_log_task_exception)
    return task


def _log_task_exception(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[task] {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global tg, redis_client, byte_streamer, stream_sem
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=False)
    stream_sem   = asyncio.Semaphore(STREAM_CONCURRENCY)
    tg = Client("streamer", api_id=API_ID, api_hash=API_HASH,
                session_string=SESSION_STRING, no_updates=True, workers=16)
    await tg.start()
    byte_streamer = ByteStreamer(tg)
    print("Pyrogram started")
    _schedule(_sync_loop())
    yield
    await download_manager.shutdown()
    for s in list(getattr(tg, "media_sessions", {}).values()):
        try: await s.stop()
        except: pass
    if hasattr(tg, "media_sessions"):
        tg.media_sessions.clear()
    await tg.stop()
    await redis_client.aclose()


app = FastAPI(title="TGStream", version="2.0.0", lifespan=lifespan, docs_url="/api/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET", "HEAD", "OPTIONS"], allow_headers=["*"])


async def _fetch_msg(msg_id: int):
    return await tg.get_messages(CHANNEL_USERNAME, msg_id)


async def _sync_loop():
    while True:
        try:
            last = await redis_client.get(st.R_SYNC_TS)
            if not last or (time.time() - float(last)) > SYNC_INTERVAL:
                await _sync_channel()
        except Exception as e:
            print(f"[sync_loop] {e}")
        await asyncio.sleep(60)


async def _sync_channel() -> int:
    async with _sync_lock:
        acquired = await redis_client.set(st.R_SYNC_LCK, "1", ex=600, nx=True)
        if not acquired:
            return 0
        try:
            count = 0
            async for msg in tg.get_chat_history(CHANNEL_USERNAME):
                try:
                    media = msg.video or msg.document
                    if not media: continue
                    fn = getattr(media, "file_name", None)
                    if not fn: continue
                    mid = st.movie_id(fn)
                    await st.save_movie(redis_client, mid, {
                        "message_id": msg.id, "file_name": fn,
                        "file_size": media.file_size,
                        "file_size_text": st.fmt_size(media.file_size),
                        "quality": st.quality(fn), "source": st.source(fn),
                        "synced_at": int(time.time()),
                    })
                    count += 1
                except: continue
            await redis_client.set(st.R_SYNC_TS, str(time.time()))
            print(f"Sync: {count} movies")
            return count
        finally:
            await redis_client.delete(st.R_SYNC_LCK)


MANIFEST = {
    "id": "org.tgstream.hybrid", "version": "2.0.0", "name": "TGStream",
    "description": "Hybrid predictive streaming from Telegram via Stremio",
    "resources": ["catalog", "meta", "stream"], "types": ["movie", "series"],
    "idPrefixes": ["tgm:", "tgs:", "tt"],
    "catalogs": [
        {"type": "movie",  "id": "tgstream_movies", "name": "TG Movies"},
        {"type": "series", "id": "tgstream_series", "name": "TG Series"},
    ],
    "behaviorHints": {"configurable": False, "configurationRequired": False},
}


@app.get("/")
async def health():
    movies = await redis_client.hlen(st.R_MOVIES)
    last   = await redis_client.get(st.R_SYNC_TS)
    age    = round((time.time() - float(last)) / 60, 1) if last else None
    dl     = download_manager.stats()
    return {"status": "ok", "movies": movies, "channel": CHANNEL_USERNAME,
            "sync_age_min": age, "active_downloads": len(dl), "download_stats": dl}


@app.get("/manifest.json")
async def manifest(): return JSONResponse(MANIFEST)


@app.get("/sync")
async def manual_sync(): return {"synced": await _sync_channel()}


@app.get("/debug/movies")
async def debug_movies():
    movies = await st.load_movies(redis_client)
    for mid, m in movies.items():
        task = download_manager.get(mid)
        dl_map = download_manager.get_map(mid)
        if not dl_map:
            dl_map = await download_manager._load_map(mid, redis_client)
            
        file_path = STORAGE_DIR / f"{mid}.bin"
        exists = file_path.exists()
        
        cached_bytes = dl_map.total_bytes() if exists else 0
        m["cached_bytes"] = cached_bytes
        m["cached_text"] = st.fmt_size(cached_bytes)
        
        fs = m.get("file_size", 0)
        m["pct"] = round(cached_bytes / fs * 100, 1) if fs and exists else 0
        
        is_done = False
        if exists:
            done_val = await redis_client.get(f"tgstream:dl:done:{mid}")
            is_done = done_val == b"1" or cached_bytes >= fs
            
        m["is_done"] = is_done
        m["is_active"] = bool(task and task._task and not task._task.done())
    return movies


@app.get("/debug/downloads")
async def debug_downloads():
    stats  = download_manager.stats()
    movies = await st.load_movies(redis_client)
    for mid, s in stats.items():
        movie = movies.get(mid, {})
        fs    = movie.get("file_size", 0)
        s["total_mb"]   = round(fs / 1024 / 1024, 1) if fs else 0
        s["pct_done"]   = round(s["downloaded_mb"] / s["total_mb"] * 100, 1) if s.get("total_mb") else 0
        s["file_name"]  = movie.get("file_name", mid)
    return stats


@app.get("/catalog/{type}/{id}.json")
async def catalog(type: str, id: str):
    movies = await st.load_movies(redis_client)
    def is_series(m): return bool(re.search(r"s\d{2}e\d{2}|season\s*\d|episode\s*\d", m.get("file_name","").lower()))
    
    if type == "movie":
        filtered = {mid: m for mid, m in movies.items() if not is_series(m)}
        async def build(mid, m):
            fn = m.get("file_name","Unknown")
            poster = await st.get_poster(redis_client, fn)
            title, year = st.parse_title_year(fn)
            return {"id": f"tgm:{mid}", "type": "movie", "name": title or fn,
                    "poster": poster, "posterShape": "poster", "year": year}
        metas = await asyncio.gather(*[build(mid, m) for mid, m in filtered.items()])
        return JSONResponse({"metas": list(metas)}, headers={"Cache-Control": "no-store"})
    
    else:  # type == "series"
        series_groups = {}
        for mid, m in movies.items():
            if not is_series(m): continue
            fn = m.get("file_name","Unknown")
            show_title = st.parse_show_title(fn)
            sid = st.show_id(fn)
            if sid not in series_groups:
                series_groups[sid] = {"title": show_title, "files": []}
            series_groups[sid]["files"].append((mid, m))
            
        async def build_series(sid, group):
            fn = group["files"][0][1].get("file_name","Unknown")
            poster = await st.get_poster(redis_client, fn)
            year = ""
            for _, m in group["files"]:
                _, y = st.parse_title_year(m.get("file_name",""))
                if y:
                    year = y
                    break
            return {"id": f"tgs:{sid}", "type": "series", "name": group["title"],
                    "poster": poster, "posterShape": "poster", "year": year}
        metas = await asyncio.gather(*[build_series(sid, group) for sid, group in series_groups.items()])
        return JSONResponse({"metas": list(metas)}, headers={"Cache-Control": "no-store"})


@app.get("/meta/{type}/{id}.json")
async def meta(type: str, id: str):
    if id.startswith("tt"):
        title, year = await st.get_cinemeta(type, id)
        return JSONResponse({"meta": {"id": id, "type": type, "name": title, "year": year}})
        
    prefix = "tgm:" if type == "movie" else "tgs:"
    clean  = id[len(prefix):] if id.startswith(prefix) else id
    movies = await st.load_movies(redis_client)
    
    if type == "movie":
        movie = movies.get(clean)
        if not movie: return JSONResponse({"meta": {}})
        fn = movie.get("file_name","Unknown")
        title, year = st.parse_title_year(fn)
        return JSONResponse({"meta": {"id": id, "type": type, "name": title or fn, "year": year,
            "poster": await st.get_poster(redis_client, fn), "description": fn, "posterShape": "poster"}})
    else:  # type == "series"
        matching_files = [m for m in movies.values() if st.show_id(m.get("file_name", "")) == clean]
        if not matching_files: return JSONResponse({"meta": {}})
        matching_files.sort(key=lambda m: m.get("file_name", ""))
        
        first_file = matching_files[0]
        fn = first_file.get("file_name", "Unknown")
        show_title = st.parse_show_title(fn)
        poster = await st.get_poster(redis_client, fn)
        year = ""
        for m in matching_files:
            _, y = st.parse_title_year(m.get("file_name", ""))
            if y:
                year = y
                break
                
        videos = []
        seen_episodes = set()
        for m in matching_files:
            m_fn = m.get("file_name", "")
            info = st.parse_series(m_fn)
            s = info["season"] if info else 1
            ep = info["episode"] if info else 1
            key = (s, ep)
            if key in seen_episodes: continue
            seen_episodes.add(key)
            
            vid = f"tgs:{clean}:{s}:{ep}"
            videos.append({
                "id": vid, "season": s, "episode": ep, "title": f"Episode {ep}",
                "released": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(m.get("synced_at", time.time()))),
            })
        videos.sort(key=lambda x: (x["season"], x["episode"]))
        
        return JSONResponse({"meta": {
            "id": id, "type": "series", "name": show_title, "year": year,
            "poster": poster, "description": f"Series: {show_title}", "posterShape": "poster", "videos": videos
        }})


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    movies = await st.load_movies(redis_client)
    prefix = "tgm:" if type == "movie" else "tgs:"
    if id.startswith("tt"):
        parts   = id.split(":")
        imdb_id = parts[0]
        season  = int(parts[1]) if len(parts) > 1 else None
        episode = int(parts[2]) if len(parts) > 2 else None
        title, year = await st.get_cinemeta(type, imdb_id)
        if not title: return JSONResponse({"streams": []})
        streams = []
        for mid, m in movies.items():
            fn = m.get("file_name","")
            if not st.flex_match(title, fn): continue
            if year:
                try:
                    my = int(year)
                    if not any(str(my+d) in fn for d in (-1,0,1)): continue
                except: 
                    if year not in fn: continue
            if season and episode:
                info = st.parse_series(fn)
                if info and (info["season"]!=season or info["episode"]!=episode): continue
            q,sz,src = m.get("quality","Unknown"),m.get("file_size_text","Unknown"),m.get("source","")
            streams.append({"name":"TGStream","title":f"{fn}\n{q}{' | '+src if src else ''} | {sz}","url":f"{BASE_URL}/proxy/{mid}"})
        return JSONResponse({"streams": streams})

    clean = id[len(prefix):] if id.startswith(prefix) else id
    
    if type == "series" and ":" in clean:
        parts = clean.split(":")
        sid = parts[0]
        try:
            season = int(parts[1])
            episode = int(parts[2])
        except:
            return JSONResponse({"streams": []})
            
        streams = []
        for mid, m in movies.items():
            fn = m.get("file_name", "")
            if st.show_id(fn) != sid: continue
            info = st.parse_series(fn)
            s = info["season"] if info else 1
            ep = info["episode"] if info else 1
            if s == season and ep == episode:
                try:
                    fs = m.get("file_size")
                    _schedule(_ensure_download(mid, fs, m["message_id"]))
                except Exception as e:
                    print(f"[stream] warn: {e}")
                
                q   = m.get("quality","Unknown")
                sz  = m.get("file_size_text","Unknown")
                src = m.get("source","")
                streams.append({
                    "name": "TGStream",
                    "title": f"{fn}\n{q}{' | '+src if src else ''} | {sz}",
                    "url": f"{BASE_URL}/proxy/{mid}"
                })
        return JSONResponse({"streams": streams})

    movie = movies.get(clean)
    if not movie: return JSONResponse({"streams": []})
    try:
        msg   = await _fetch_msg(movie["message_id"])
        media = msg.video or msg.document
        if not media:
            await st.del_movie(redis_client, clean)
            return JSONResponse({"streams": []})
        fs = movie.get("file_size") or media.file_size
        _schedule(_ensure_download(clean, fs, movie["message_id"]))
    except Exception as e:
        print(f"[stream] warn: {e}")
    fn  = movie.get("file_name","Unknown")
    q   = movie.get("quality","Unknown")
    sz  = movie.get("file_size_text","Unknown")
    src = movie.get("source","")
    return JSONResponse({"streams": [{"name":"TGStream",
        "title":f"{fn}\n{q}{' | '+src if src else ''} | {sz}","url":f"{BASE_URL}/proxy/{clean}"}]})


async def _ensure_download(movie_id: str, file_size: int, message_id: int):
    await download_manager.get_or_create(
        movie_id=movie_id, file_size=file_size, message_id=message_id,
        redis=redis_client, byte_streamer=byte_streamer, fetch_msg_fn=_fetch_msg,
    )
    await download_manager.evict_lru_if_needed(redis_client)


async def _yield_local_file(dl_file, start: int, length: int, request: Request):
    sent = 0
    while sent < length:
        if await request.is_disconnected():
            break
        size = min(LOCAL_READ_CHUNK, length - sent)
        data = await dl_file.pread(start + sent, size)
        if not data:
            break
        sent += len(data)
        yield data


# ─── HYBRID PROXY — the heart of v2 ──────────────────────────────────────────
@app.api_route("/proxy/{movie_id}", methods=["GET", "HEAD"])
async def proxy(movie_id: str, request: Request):
    """
    Four-path resolution (in order):
      A. Range fully in local SparseFile  -> pread, instant
      B. Short wait for downloader catch-up -> pread if ready
      C. Partial local prefix + live Telegram for remainder -> mixed stream
      D. Fully live Telegram MTProto       -> StreamingResponse fallback
    X-Source header reveals which path was used (visible in dev tools).
    """
    movies = await st.load_movies(redis_client)
    movie  = movies.get(movie_id)
    if not movie: raise HTTPException(404, "Not found")

    file_size = movie.get("file_size")
    filename  = movie.get("file_name", "video.mp4")
    ctype_val = st.ctype(filename)

    if not file_size:
        try:
            msg       = await _fetch_msg(movie["message_id"])
            file_size = (msg.video or msg.document).file_size
        except: raise HTTPException(502, "Telegram unavailable")

    etag = f'"{movie["message_id"]}-{file_size}"'

    if request.method == "HEAD":
        return Response(status_code=200, headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size), "Content-Type": ctype_val,
            "Cache-Control": "public, max-age=3600", "ETag": etag,
        })

    # Ensure download running
    _schedule(_ensure_download(movie_id, file_size, movie["message_id"]))

    # Parse Range
    start, end = 0, file_size - 1
    rh = request.headers.get("range", "")
    if rh.startswith("bytes="):
        spec = rh[6:]
        try:
            if "," in spec:
                raise ValueError("Multiple ranges are not supported")
            if spec.startswith("-"):
                suffix_len = int(spec[1:])
                if suffix_len <= 0:
                    raise ValueError("Invalid suffix range")
                start = max(0, file_size - suffix_len)
                end   = file_size - 1
            else:
                p = spec.split("-")
                if p[0]: start = int(p[0])
                if len(p) > 1 and p[1]: end = int(p[1])
        except:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})
    if not rh:
        end = min(STARTUP_CHUNKS * TG_CHUNK - 1, file_size - 1)
    elif rh.endswith("-"):
        end = min(start + STARTUP_CHUNKS * TG_CHUNK - 1, file_size - 1)
    else:
        end = min(end, file_size - 1)
    if start < 0 or start >= file_size or end < start:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    total = end - start + 1

    # Hint downloader
    task    = download_manager.get(movie_id)
    dl_map  = download_manager.get_map(movie_id)
    dl_file = download_manager.get_file(movie_id)
    if task: task.hint(start)

    headers = {
        "Accept-Ranges": "bytes", "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(total), "Content-Type": ctype_val,
        "Cache-Control": "public, max-age=3600", "ETag": etag, "Vary": "Range",
    }

    # ── Path A: fully local ───────────────────────────────────────────────────
    if dl_map and dl_file and dl_file.exists() and dl_map.has_range(start, end):
        return StreamingResponse(
            _yield_local_file(dl_file, start, total, request),
            status_code=206,
            headers={**headers, "X-Source": "local"},
            media_type=ctype_val,
        )

    # ── Path B: wait for downloader to land the range ─────────────────────────
    if task and not task.is_done() and dl_map and dl_file and dl_file.exists():
        deadline = time.time() + WAIT_TIMEOUT_S
        while time.time() < deadline:
            if dl_map.covered_prefix(start) >= total:
                return StreamingResponse(
                    _yield_local_file(dl_file, start, total, request),
                    status_code=206,
                    headers={**headers, "X-Source": "local-waited"},
                    media_type=ctype_val,
                )
            try:
                remaining = deadline - time.time()
                if remaining <= 0: break
                await asyncio.wait_for(asyncio.shield(task.progress_event().wait()),
                                       timeout=min(remaining, 0.5))
            except asyncio.TimeoutError:
                pass

    # ── Path C: local prefix + live tail ─────────────────────────────────────
    if dl_map and dl_file and dl_file.exists():
        covered = dl_map.covered_prefix(start)
        if 0 < covered < total:
            rest_start = start + covered

            async def _mixed():
                async for chunk in _yield_local_file(dl_file, start, covered, request):
                    yield chunk
                async with stream_sem:
                    try: msg = await _fetch_msg(movie["message_id"])
                    except: return
                    aligned   = (rest_start // TG_CHUNK) * TG_CHUNK
                    first_cut = rest_start - aligned
                    last_cut  = (end % TG_CHUNK) + 1
                    parts     = math.ceil((end+1)/TG_CHUNK) - (aligned//TG_CHUNK)
                    async for chunk in byte_streamer.yield_file(msg, aligned, first_cut, last_cut, parts):
                        if await request.is_disconnected(): break
                        yield chunk

            return StreamingResponse(_mixed(), status_code=206,
                                     headers={**headers, "X-Source": "mixed"}, media_type=ctype_val)

    # ── Path D: fully live Telegram ───────────────────────────────────────────
    try:
        msg = await _fetch_msg(movie["message_id"])
    except FloodWait as e:
        raise HTTPException(503, f"Rate limited — retry after {e.value}s")
    except:
        raise HTTPException(502, "Telegram unavailable")

    if not (msg.video or msg.document):
        await st.del_movie(redis_client, movie_id)
        raise HTTPException(404, "Deleted from Telegram")

    aligned   = (start // TG_CHUNK) * TG_CHUNK
    first_cut = start - aligned
    last_cut  = (end % TG_CHUNK) + 1
    parts     = math.ceil((end+1)/TG_CHUNK) - (aligned//TG_CHUNK)

    async def _live():
        async with stream_sem:
            async for chunk in byte_streamer.yield_file(msg, aligned, first_cut, last_cut, parts):
                if await request.is_disconnected(): break
                yield chunk

    return StreamingResponse(_live(), status_code=206,
                             headers={**headers, "X-Source": "telegram-live"}, media_type=ctype_val)


# ── Media Control API Endpoints ───────────────────────────────────────────────
@app.post("/api/media/{movie_id}/download")
async def start_download_media(movie_id: str):
    movies = await st.load_movies(redis_client)
    movie = movies.get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found in index")
    
    file_size = movie.get("file_size")
    if not file_size:
        try:
            msg = await _fetch_msg(movie["message_id"])
            file_size = (msg.video or msg.document).file_size
        except:
            raise HTTPException(status_code=502, detail="Telegram unavailable")
            
    _schedule(_ensure_download(movie_id, file_size, movie["message_id"]))
    return {"status": "ok"}


@app.post("/api/media/{movie_id}/pause")
async def pause_download_media(movie_id: str):
    task = download_manager.get(movie_id)
    if task:
        task.cancel()
        return {"status": "ok"}
    return {"status": "ignored"}


@app.post("/api/media/{movie_id}/evict")
async def evict_cache_media(movie_id: str):
    await download_manager.evict(movie_id, redis_client)
    return {"status": "ok"}


@app.delete("/api/media/{movie_id}")
async def delete_media(movie_id: str, delete_tg: bool = False):
    movies = await st.load_movies(redis_client)
    movie = movies.get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found in index")
    
    # 1. Evict cache from downloader
    await download_manager.evict(movie_id, redis_client)
    
    # 2. Optionally delete from Telegram
    if delete_tg:
        try:
            await tg.delete_messages(CHANNEL_USERNAME, [movie["message_id"]])
        except Exception as e:
            print(f"[delete_media] failed to delete from Telegram: {e}")
            raise HTTPException(status_code=502, detail=f"Failed to delete from Telegram: {e}")
            
    # 3. Delete from index
    await st.del_movie(redis_client, movie_id)
    return {"status": "ok"}


# ── Dashboard ─────────────────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    manifest_url = f"{BASE_URL}/manifest.json"
    stremio_url  = manifest_url.replace("https://","stremio://").replace("http://","stremio://")
    return _dashboard_html(manifest_url, stremio_url, CHANNEL_USERNAME)


def _dashboard_html(manifest_url: str, stremio_url: str, channel: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TGStream - Media Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {{
  --bg: #090a0f;
  --bg-gradient: radial-gradient(circle at top, #141724 0%, #090a0f 100%);
  --panel: rgba(20, 22, 33, 0.75);
  --panel-border: rgba(255, 255, 255, 0.05);
  --panel-hover: rgba(30, 33, 48, 0.85);
  --text: #f1f3f9;
  --text-muted: #858fa3;
  --primary: #4f46e5;
  --primary-hover: #6366f1;
  --primary-glow: rgba(79, 70, 229, 0.4);
  --accent: #06b6d4;
  --accent-glow: rgba(6, 182, 212, 0.3);
  --success: #10b981;
  --success-bg: rgba(16, 185, 129, 0.1);
  --success-text: #34d399;
  --warning: #f59e0b;
  --warning-bg: rgba(245, 158, 11, 0.1);
  --warning-text: #fbbf24;
  --danger: #ef4444;
  --danger-bg: rgba(239, 68, 68, 0.1);
  --danger-text: #f87171;
  --card-shadow: 0 8px 30px rgba(0, 0, 0, 0.35);
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
  background: var(--bg);
  background-image: var(--bg-gradient);
  color: var(--text);
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}}
header {{
  background: rgba(13, 15, 23, 0.8);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid var(--panel-border);
  padding: 16px 28px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  z-index: 100;
}}
.logo-container {{
  display: flex;
  align-items: center;
  gap: 10px;
}}
.logo {{
  font-size: 20px;
  font-weight: 700;
  background: linear-gradient(135deg, #a78bfa 0%, #4f46e5 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  letter-spacing: -0.5px;
}}
.tag-hybrid {{
  background: rgba(99, 102, 241, 0.15);
  border: 1px solid rgba(99, 102, 241, 0.3);
  color: #a5b4fc;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 600;
}}
.dot {{
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--success);
  margin-right: 8px;
  box-shadow: 0 0 10px var(--success);
  animation: pulse 2s ease-in-out infinite;
}}
@keyframes pulse {{
  0%, 100% {{ opacity: 1; transform: scale(1); }}
  50% {{ opacity: 0.4; transform: scale(0.85); }}
}}
.channel-info {{
  display: flex;
  align-items: center;
  font-size: 13px;
  color: var(--text-muted);
  background: rgba(255,255,255,0.03);
  padding: 6px 12px;
  border-radius: 6px;
  border: 1px solid rgba(255,255,255,0.05);
}}
main {{
  max-width: 1200px;
  margin: 0 auto;
  width: 100%;
  padding: 32px 24px;
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 24px;
}}
.cards {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 16px;
}}
.card {{
  background: var(--panel);
  backdrop-filter: blur(16px);
  border: 1px solid var(--panel-border);
  border-radius: 14px;
  padding: 20px;
  box-shadow: var(--card-shadow);
  transition: transform 0.2s, border-color 0.2s;
}}
.card:hover {{
  transform: translateY(-2px);
  border-color: rgba(255, 255, 255, 0.08);
}}
.cl {{
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-muted);
  margin-bottom: 8px;
}}
.cv {{
  font-size: 28px;
  font-weight: 700;
  letter-spacing: -0.5px;
}}
.cv.b {{ color: var(--accent); text-shadow: 0 0 15px var(--accent-glow); }}
.cv.g {{ color: var(--success); text-shadow: 0 0 15px var(--success-bg); }}
.cv.y {{ color: var(--warning); text-shadow: 0 0 15px var(--warning-bg); }}
.cv.pu {{ color: #a78bfa; text-shadow: 0 0 15px rgba(167, 139, 250, 0.2); }}

.panel {{
  background: var(--panel);
  backdrop-filter: blur(16px);
  border: 1px solid var(--panel-border);
  border-radius: 16px;
  overflow: hidden;
  box-shadow: var(--card-shadow);
}}
.tabs {{
  display: flex;
  gap: 6px;
  padding: 8px 20px;
  background: rgba(10, 11, 18, 0.4);
  border-bottom: 1px solid var(--panel-border);
}}
.tab {{
  padding: 12px 20px;
  font-size: 13px;
  font-weight: 600;
  color: var(--text-muted);
  cursor: pointer;
  border-radius: 8px;
  transition: all 0.2s ease;
}}
.tab:hover {{
  color: var(--text);
  background: rgba(255,255,255,0.03);
}}
.tab.active {{
  color: #fff;
  background: var(--primary);
  box-shadow: 0 4px 12px var(--primary-glow);
}}
.tab-panel {{ display: none; }}
.tab-panel.active {{ display: block; }}
.pb {{ padding: 24px; }}

.controls-row {{
  display: flex;
  gap: 12px;
  margin-bottom: 20px;
  align-items: center;
  flex-wrap: wrap;
}}
.search-wrapper {{
  position: relative;
  flex: 1;
  min-width: 250px;
}}
.search {{
  width: 100%;
  background: rgba(0, 0, 0, 0.2);
  border: 1px solid var(--panel-border);
  border-radius: 8px;
  padding: 10px 14px;
  font-size: 13px;
  color: var(--text);
  outline: none;
  font-family: inherit;
  transition: border-color 0.2s, box-shadow 0.2s;
}}
.search:focus {{
  border-color: var(--primary);
  box-shadow: 0 0 0 2px var(--primary-glow);
}}
.search::placeholder {{ color: var(--text-muted); }}

table {{
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  font-size: 13px;
}}
th {{
  text-align: left;
  padding: 12px 16px;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-muted);
  border-bottom: 1px solid var(--panel-border);
  background: rgba(0,0,0,0.1);
}}
td {{
  padding: 14px 16px;
  border-bottom: 1px solid var(--panel-border);
  vertical-align: middle;
}}
tbody tr {{ transition: background 0.15s; }}
tbody tr:hover {{ background: rgba(255, 255, 255, 0.02); }}
tbody tr:last-child td {{ border-bottom: none; }}

.badge {{
  display: inline-flex;
  padding: 3px 8px;
  border-radius: 5px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.5px;
}}
.b1080 {{ background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); }}
.b2160 {{ background: rgba(139, 92, 246, 0.15); color: #a78bfa; border: 1px solid rgba(139, 92, 246, 0.3); }}
.b720 {{ background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }}
.bdef {{ background: rgba(107, 114, 128, 0.15); color: #9ca3af; border: 1px solid rgba(107, 114, 128, 0.3); }}
.b-cached {{ background: var(--success-bg); color: var(--success-text); border: 1px solid rgba(16, 185, 129, 0.2); }}

.progress-cell {{
  min-width: 140px;
}}
.pbar-container {{
  display: flex;
  flex-direction: column;
  gap: 4px;
}}
.pbar-wrap {{
  width: 100%;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 4px;
  height: 6px;
  overflow: hidden;
}}
.pbar {{
  height: 100%;
  border-radius: 4px;
  background: linear-gradient(90deg, var(--accent) 0%, #38bdf8 100%);
  box-shadow: 0 0 8px rgba(6, 182, 212, 0.5);
  transition: width 0.4s ease;
}}
.pbar-active {{
  background: linear-gradient(90deg, var(--primary) 0%, #818cf8 100%);
  box-shadow: 0 0 8px var(--primary-glow);
}}
.progress-label {{
  display: flex;
  justify-content: space-between;
  font-size: 11px;
  color: var(--text-muted);
}}

.actions-cell {{
  white-space: nowrap;
  width: 1%;
}}
.actions-grp {{
  display: flex;
  gap: 8px;
}}
.btn {{
  padding: 8px 14px;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  border: none;
  transition: all 0.15s ease;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  text-decoration: none;
  font-family: inherit;
}}
.btn-icon-only {{
  padding: 8px;
  border-radius: 6px;
  justify-content: center;
}}
.btn-p {{
  background: var(--primary);
  color: #fff;
}}
.btn-p:hover {{
  background: var(--primary-hover);
  box-shadow: 0 0 15px var(--primary-glow);
}}
.btn-a {{
  background: var(--accent);
  color: #0f172a;
}}
.btn-a:hover {{
  background: #22d3ee;
  box-shadow: 0 0 15px var(--accent-glow);
}}
.btn-g {{
  background: rgba(255,255,255,0.03);
  color: var(--text);
  border: 1px solid var(--panel-border);
}}
.btn-g:hover {{
  border-color: rgba(255,255,255,0.15);
  background: rgba(255,255,255,0.06);
}}
.btn-d {{
  background: var(--danger-bg);
  color: var(--danger-text);
  border: 1px solid rgba(239, 68, 68, 0.2);
}}
.btn-d:hover {{
  background: var(--danger);
  color: #fff;
  box-shadow: 0 0 15px rgba(239, 68, 68, 0.4);
}}
.btn:disabled {{
  opacity: 0.4;
  cursor: not-allowed;
  box-shadow: none !important;
}}

.url-box {{
  background: rgba(0, 0, 0, 0.3);
  border: 1px solid var(--panel-border);
  border-radius: 8px;
  padding: 12px 16px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  color: var(--accent);
  word-break: break-all;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}}
.copy-btn {{
  background: rgba(255,255,255,0.03);
  border: 1px solid var(--panel-border);
  border-radius: 6px;
  padding: 4px 10px;
  font-size: 11px;
  color: var(--text-muted);
  cursor: pointer;
  transition: all 0.15s;
  white-space: nowrap;
}}
.copy-btn:hover {{
  color: #fff;
  border-color: var(--accent);
  background: rgba(6, 182, 212, 0.1);
}}
.info-box {{
  background: rgba(255, 255, 255, 0.02);
  border-radius: 12px;
  padding: 18px;
  font-size: 13px;
  color: var(--text-muted);
  line-height: 1.8;
  border: 1px solid var(--panel-border);
}}

/* Modal Styles */
.modal {{
  display: none;
  position: fixed;
  z-index: 1000;
  left: 0;
  top: 0;
  width: 100%;
  height: 100%;
  background-color: rgba(0,0,0,0.65);
  backdrop-filter: blur(6px);
  align-items: center;
  justify-content: center;
  padding: 16px;
}}
.modal.active {{
  display: flex;
  animation: fadeIn 0.2s ease-out;
}}
.modal-content {{
  background: #0f111a;
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px;
  padding: 26px;
  width: 100%;
  max-width: 480px;
  box-shadow: 0 25px 50px rgba(0,0,0,0.6);
  animation: scaleIn 0.2s cubic-bezier(0.34, 1.56, 0.64, 1);
}}
.modal h3 {{
  margin-bottom: 12px;
  font-size: 18px;
  color: var(--text);
}}
.modal p {{
  font-size: 14px;
  color: var(--text-muted);
  margin-bottom: 20px;
  line-height: 1.5;
}}
.modal-checkbox-label {{
  display: flex;
  gap: 10px;
  align-items: flex-start;
  font-size: 13px;
  color: var(--text);
  cursor: pointer;
  margin-bottom: 20px;
  user-select: none;
  background: rgba(255,255,255,0.02);
  padding: 12px;
  border-radius: 8px;
  border: 1px solid var(--panel-border);
}}
.modal-checkbox-label input {{
  margin-top: 3px;
}}
.modal-warning {{
  background: var(--danger-bg);
  border: 1px solid rgba(239, 68, 68, 0.15);
  border-radius: 8px;
  padding: 12px;
  font-size: 12px;
  color: var(--danger-text);
  margin-bottom: 24px;
  line-height: 1.4;
}}
.modal-actions {{
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}}

/* Toast Notification Styles */
.toast-container {{
  position: fixed;
  bottom: 24px;
  right: 24px;
  z-index: 1001;
  display: flex;
  flex-direction: column;
  gap: 8px;
}}
.toast {{
  background: #141724;
  border: 1px solid rgba(255,255,255,0.06);
  border-left: 4px solid var(--primary);
  color: var(--text);
  padding: 14px 20px;
  font-size: 13px;
  font-weight: 500;
  border-radius: 8px;
  box-shadow: 0 10px 25px rgba(0,0,0,0.4);
  transform: translateX(120%);
  transition: all 0.3s cubic-bezier(0.68, -0.55, 0.27, 1.55);
  min-width: 250px;
}}
.toast.show {{
  transform: translateX(0);
}}
.toast-success {{ border-left-color: var(--success); }}
.toast-warning {{ border-left-color: var(--warning); }}
.toast-danger {{ border-left-color: var(--danger); }}

@keyframes fadeIn {{
  from {{ opacity: 0; }}
  to {{ opacity: 1; }}
}}
@keyframes scaleIn {{
  from {{ transform: scale(0.95); opacity: 0; }}
  to {{ transform: scale(1); opacity: 1; }}
}}

footer {{
  padding: 24px;
  border-top: 1px solid var(--panel-border);
  font-size: 12px;
  color: var(--text-muted);
  text-align: center;
  background: rgba(10, 11, 18, 0.2);
}}
footer a {{
  color: var(--primary-hover);
  text-decoration: none;
}}
footer a:hover {{
  text-decoration: underline;
}}
</style>
</head>
<body>
<header>
  <div class="logo-container">
    <div class="logo">TGStream</div>
    <div class="tag-hybrid">v2 · Hybrid</div>
  </div>
  <div class="channel-info">
    <span class="dot"></span>
    <span>{channel}</span>
  </div>
</header>
<main>
  <div class="cards">
    <div class="card">
      <div class="cl">Total Index</div>
      <div class="cv b" id="stat-movies">—</div>
    </div>
    <div class="card">
      <div class="cl">Active Downloads</div>
      <div class="cv pu" id="stat-dl">—</div>
    </div>
    <div class="card">
      <div class="cl">System Status</div>
      <div class="cv g">Online</div>
    </div>
    <div class="card">
      <div class="cl">Last Sync</div>
      <div class="cv y" id="stat-sync">—</div>
    </div>
  </div>
  
  <div class="panel">
    <div class="tabs">
      <div class="tab active" onclick="showTab('library')">Library & Cache Manager</div>
      <div class="tab" onclick="showTab('install')">Stremio Installation</div>
    </div>
    
    <!-- Tab: Library -->
    <div id="tab-library" class="tab-panel active">
      <div class="pb">
        <div class="controls-row">
          <div class="search-wrapper">
            <input class="search" placeholder="Search filenames..." oninput="filterMovies(this.value)">
          </div>
          <button class="btn btn-g" onclick="loadMovies()">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"></path></svg>
            Refresh List
          </button>
          <button class="btn btn-p" id="sync-btn" onclick="doSync()">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 2.1l4 4-4 4M3 22l-4-4 4-4M21 6H9a4 4 0 0 0-4 4v12M3 18h12a4 4 0 0 0 4-4V2"></path></svg>
            Sync Channel
          </button>
        </div>
        
        <div style="overflow-x:auto;">
          <table>
            <thead>
              <tr>
                <th>Media Details</th>
                <th>File Size</th>
                <th>Cache Status</th>
                <th style="text-align:right;">Actions</th>
              </tr>
            </thead>
            <tbody id="movie-tbody">
              <tr>
                <td colspan="4" style="text-align:center;color:var(--text-muted);padding:40px;">
                  <div style="display:flex;align-items:center;justify-content:center;gap:10px;">
                    <span style="opacity: 0.6;">Loading library data...</span>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
    
    <!-- Tab: Install -->
    <div id="tab-install" class="tab-panel">
      <div class="pb" style="display:flex;flex-direction:column;gap:16px;">
        <div>
          <div style="font-size:11px;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.6px;">Stremio Addon Manifest URL</div>
          <div class="url-box">
            <span>{manifest_url}</span>
            <button class="copy-btn" onclick="cp('{manifest_url}',this)">Copy URL</button>
          </div>
          <div style="font-size:11px;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.6px;">Stremio Deep Link</div>
          <div class="url-box">
            <span>{stremio_url}</span>
            <button class="copy-btn" onclick="cp('{stremio_url}',this)">Copy Link</button>
          </div>
          <a href="{stremio_url}" class="btn btn-p" style="margin-top: 8px;">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path><polyline points="15 3 21 3 21 9"></polyline><line x1="10" y1="14" x2="21" y2="3"></line></svg>
            Open in Stremio Client
          </a>
        </div>
        
        <div class="info-box">
          <b style="color:var(--accent);">Hybrid Streaming Architecture (4 paths):</b><br>
          <ul style="margin-left: 20px; margin-top: 8px; display: flex; flex-direction: column; gap: 6px;">
            <li><b style="color:var(--success);">Path A (local)</b> — Byte-range exists on disk. Served instantly from local SparseFile cache.</li>
            <li><b style="color:var(--primary-hover);">Path B (local-waited)</b> — Byte-range is currently downloading. Thread waits up to 2s for download to land.</li>
            <li><b style="color:var(--warning-text);">Path C (mixed)</b> — Serves already-cached prefix locally, and transparently shifts back to Telegram live download for remainder.</li>
            <li><b style="color:var(--danger-text);">Path D (telegram-live)</b> — Direct streaming fallback via Telegram MTProto.</li>
          </ul>
        </div>
      </div>
    </div>
  </div>
</main>

<!-- Delete Modal -->
<div id="delete-modal" class="modal">
  <div class="modal-content">
    <h3>Delete Media Item</h3>
    <p>This will remove the media file from your Stremio library and permanently erase any local cached files from disk.</p>
    
    <label class="modal-checkbox-label">
      <input type="checkbox" id="delete-tg-checkbox">
      <span>Also delete the original post/file from the Telegram channel (<strong>{channel}</strong>)</span>
    </label>
    
    <div class="modal-warning">
      <strong>Caution:</strong> Deleting from the Telegram channel is permanent and cannot be undone. Make sure you have admin rights in this channel.
    </div>
    
    <div class="modal-actions">
      <button class="btn btn-g" onclick="closeDeleteModal()">Cancel</button>
      <button class="btn btn-d" id="modal-confirm-btn" onclick="confirmDelete()">Delete Permanently</button>
    </div>
  </div>
</div>

<div id="toast-container" class="toast-container"></div>

<footer>
  TGStream v2 · <a href="/api/docs" target="_blank">API Reference</a> · <a href="/manifest.json" target="_blank">Addon Manifest</a>
</footer>

<script>
let allMovies = {{}};
let movieToDeleteId = null;

function showTab(name) {{
  ['library', 'install'].forEach((n, i) => {{
    document.querySelectorAll('.tab')[i].classList.toggle('active', n === name);
    document.getElementById('tab-' + n).classList.toggle('active', n === name);
  }});
}}

async function loadStats() {{
  try {{
    const r = await fetch('/');
    const d = await r.json();
    document.getElementById('stat-movies').textContent = d.movies ?? '0';
    document.getElementById('stat-dl').textContent = d.active_downloads ?? '0';
    document.getElementById('stat-sync').textContent = d.sync_age_min !== null ? d.sync_age_min + 'm ago' : 'never';
  }} catch(e) {{}}
}}

async function loadMovies() {{
  try {{
    const r = await fetch('/debug/movies');
    allMovies = await r.json();
    renderMovies(allMovies);
  }} catch(e) {{
    showToast('Failed to load media library', 'danger');
  }}
}}

function badgeCls(q) {{
  if(q.includes('1080')) return 'b1080';
  if(q.includes('2160') || q.includes('4K')) return 'b2160';
  if(q.includes('720')) return 'b720';
  return 'bdef';
}}

function renderMovies(movies) {{
  const entries = Object.entries(movies);
  const tbody = document.getElementById('movie-tbody');
  
  if(!entries.length) {{
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:40px;">No movies indexed in the database. Run "Sync Channel" to scan your channel.</td></tr>';
    return;
  }}
  
  tbody.innerHTML = entries.map(([id, m]) => {{
    const q = m.quality || 'Unknown';
    const hasCache = m.cached_bytes > 0;
    
    // Cache UI representation
    let cacheHtml = '';
    if (m.is_done) {{
      cacheHtml = `<span class="badge b-cached">Fully Cached</span>`;
    }} else if (m.is_active || hasCache) {{
      const pctClass = m.is_active ? 'pbar pbar-active' : 'pbar';
      cacheHtml = `
        <div class="progress-cell">
          <div class="pbar-container">
            <div class="pbar-wrap"><div class="${{pctClass}}" style="width:${{m.pct}}%"></div></div>
            <div class="progress-label">
              <span>${{m.is_active ? 'Downloading' : 'Paused'}}</span>
              <span>${{m.pct}}% (${{m.cached_text}})</span>
            </div>
          </div>
        </div>
      `;
    }} else {{
      cacheHtml = `<span style="color:var(--text-muted)">Not Cached</span>`;
    }}
    
    // Actions UI representation
    let controlButtons = '';
    if (m.is_active) {{
      controlButtons = `
        <button class="btn btn-g btn-icon-only" title="Pause Download" onclick="triggerPause('${{id}}')">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="4" width="4" height="16"></rect><rect x="14" y="4" width="4" height="16"></rect></svg>
        </button>
      `;
    }} else if (!m.is_done) {{
      controlButtons = `
        <button class="btn btn-a btn-icon-only" title="Start Download" onclick="triggerDownload('${{id}}')">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"></polygon></svg>
        </button>
      `;
    }}
    
    const clearCacheButton = hasCache ? `
      <button class="btn btn-g btn-icon-only" title="Clear Disk Cache" onclick="triggerEvict('${{id}}')">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8"></path><polyline points="21 3 21 8 16 8"></polyline><path d="M12 7v5l4 2"></path></svg>
      </button>
    ` : '';
    
    return `
      <tr>
        <td>
          <div style="font-weight: 600; color: #fff; margin-bottom: 4px; max-width: 550px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${{m.file_name}}">
            ${{m.file_name || id}}
          </div>
          <div style="display:flex;gap:6px;align-items:center;">
            <span class="badge ${{badgeCls(q)}}">${{q}}</span>
            ${{m.source ? `<span class="badge bdef">${{m.source}}</span>` : ''}}
            <span style="font-size:11px;color:var(--text-muted)">Msg ID: ${{m.message_id}}</span>
          </div>
        </td>
        <td style="font-family: 'JetBrains Mono', monospace; font-weight: 500;">
          ${{m.file_size_text || '—'}}
        </td>
        <td>
          ${{cacheHtml}}
        </td>
        <td class="actions-cell">
          <div class="actions-grp" style="justify-content: flex-end;">
            ${{controlButtons}}
            ${{clearCacheButton}}
            <button class="btn btn-d btn-icon-only" title="Delete from Library" onclick="openDeleteModal('${{id}}')">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
            </button>
          </div>
        </td>
      </tr>
    `;
  }}).join('');
}}

function filterMovies(q) {{
  q = q.toLowerCase();
  const filtered = Object.fromEntries(
    Object.entries(allMovies).filter(([id, m]) => 
      (m.file_name || id).toLowerCase().includes(q)
    )
  );
  renderMovies(filtered);
}}

function openDeleteModal(id) {{
  movieToDeleteId = id;
  document.getElementById('delete-tg-checkbox').checked = false;
  document.getElementById('delete-modal').classList.add('active');
}}

function closeDeleteModal() {{
  document.getElementById('delete-modal').classList.remove('active');
  movieToDeleteId = null;
}}

async function confirmDelete() {{
  if(!movieToDeleteId) return;
  const deleteTg = document.getElementById('delete-tg-checkbox').checked;
  const btn = document.getElementById('modal-confirm-btn');
  const oldText = btn.textContent;
  
  btn.disabled = true;
  btn.textContent = 'Deleting...';
  
  try {{
    const r = await fetch(`/api/media/${{movieToDeleteId}}?delete_tg=${{deleteTg}}`, {{
      method: 'DELETE'
    }});
    if(r.ok) {{
      showToast('Media item deleted successfully', 'success');
      await loadMovies();
      await loadStats();
    }} else {{
      const err = await r.json();
      showToast('Deletion failed: ' + (err.detail || 'unknown error'), 'danger');
    }}
  }} catch(e) {{
    showToast('Network error during deletion', 'danger');
  }} finally {{
    btn.disabled = false;
    btn.textContent = oldText;
    closeDeleteModal();
  }}
}}

async function triggerDownload(id) {{
  try {{
    const r = await fetch(`/api/media/${{id}}/download`, {{ method: 'POST' }});
    if(r.ok) {{
      showToast('Download request scheduled', 'success');
      await loadMovies();
      await loadStats();
    }} else {{
      showToast('Failed to start download', 'danger');
    }}
  }} catch(e) {{
    showToast('Network error', 'danger');
  }}
}}

async function triggerPause(id) {{
  try {{
    const r = await fetch(`/api/media/${{id}}/pause`, {{ method: 'POST' }});
    if(r.ok) {{
      showToast('Download task paused', 'warning');
      await loadMovies();
      await loadStats();
    }} else {{
      showToast('Failed to pause download', 'danger');
    }}
  }} catch(e) {{
    showToast('Network error', 'danger');
  }}
}}

async function triggerEvict(id) {{
  try {{
    const r = await fetch(`/api/media/${{id}}/evict`, {{ method: 'POST' }});
    if(r.ok) {{
      showToast('Local cache wiped from storage', 'warning');
      await loadMovies();
      await loadStats();
    }} else {{
      showToast('Failed to clear cache', 'danger');
    }}
  }} catch(e) {{
    showToast('Network error', 'danger');
  }}
}}

async function doSync() {{
  const btn = document.getElementById('sync-btn');
  btn.disabled = true;
  btn.textContent = 'Syncing...';
  try {{
    const r = await fetch('/sync');
    const d = await r.json();
    showToast(`Successfully synced ${{d.synced}} movies`, 'success');
    await loadMovies();
    await loadStats();
  }} catch(e) {{
    showToast('Sync process failed', 'danger');
  }} finally {{
    btn.disabled = false;
    btn.textContent = 'Sync Channel';
  }}
}}

function cp(text, btn) {{
  navigator.clipboard.writeText(text);
  const oldText = btn.textContent;
  btn.textContent = 'Copied!';
  btn.style.borderColor = 'var(--success)';
  btn.style.color = 'var(--success-text)';
  setTimeout(() => {{
    btn.textContent = oldText;
    btn.style.borderColor = '';
    btn.style.color = '';
  }}, 1500);
}}

function showToast(message, type = 'success') {{
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast toast-${{type}}`;
  toast.textContent = message;
  container.appendChild(toast);
  
  setTimeout(() => toast.classList.add('show'), 10);
  
  setTimeout(() => {{
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }}, 3500);
}}

// Initialize
loadStats();
loadMovies();

// Auto-refresh stats and download progress every 4 seconds
setInterval(() => {{
  loadStats();
  // Auto-reload movies list to update progress bars/statuses
  loadMovies();
}}, 4000);
</script>
</body>
</html>"""
