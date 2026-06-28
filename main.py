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
from fastapi.staticfiles import StaticFiles
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

CHANNEL_USERNAME   = os.getenv("CHANNEL_USERNAME", "").strip()
if CHANNEL_USERNAME:
    try:
        if CHANNEL_USERNAME.startswith("-") and CHANNEL_USERNAME[1:].isdigit():
            CHANNEL_USERNAME = int(CHANNEL_USERNAME)
        elif CHANNEL_USERNAME.isdigit():
            CHANNEL_USERNAME = int(CHANNEL_USERNAME)
    except ValueError:
        pass

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

app.mount("/dashboard", StaticFiles(directory="static", html=True), name="dashboard")


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
            found_ids = set()
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
                    found_ids.add(mid)
                    count += 1
                except: continue
            
            # Clean up deleted movies
            current_movies = await st.load_movies(redis_client)
            for mid in list(current_movies.keys()):
                if mid not in found_ids:
                    print(f"Sync: removing deleted movie {mid} from index")
                    await st.del_movie(redis_client, mid)
                    await download_manager.evict(mid, redis_client)

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


# ── Configuration Endpoint ───────────────────────────────────────────────────
@app.get("/api/config")
async def api_config():
    manifest_url = f"{BASE_URL}/manifest.json"
    stremio_url  = manifest_url.replace("https://", "stremio://").replace("http://", "stremio://")
    return {
        "channel": str(CHANNEL_USERNAME),
        "manifest_url": manifest_url,
        "stremio_url": stremio_url
    }
