"""
state.py — Redis helpers for movie index + poster cache.
Download state lives in downloader.py (separate key namespace).
"""
from __future__ import annotations
import json
import re
import time
from typing import Optional

import httpx
import redis.asyncio as aioredis

# ── Key templates ─────────────────────────────────────────────────────────────
R_MOVIES   = "tgstream:movies"
R_POSTER   = "tgstream:poster:{}"
R_SYNC_TS  = "tgstream:last_sync"
R_SYNC_LCK = "tgstream:rate:sync"


# ── Movie index ───────────────────────────────────────────────────────────────
async def load_movies(redis: aioredis.Redis) -> dict:
    raw = await redis.hgetall(R_MOVIES)
    return {k.decode(): json.loads(v) for k, v in raw.items()}


async def save_movie(redis: aioredis.Redis, mid: str, data: dict):
    await redis.hset(R_MOVIES, mid, json.dumps(data))


async def del_movie(redis: aioredis.Redis, mid: str):
    await redis.hdel(R_MOVIES, mid)


# ── Poster cache ──────────────────────────────────────────────────────────────
async def get_poster(redis: aioredis.Redis, filename: str) -> str:
    key = R_POSTER.format(filename[:80])
    cached = await redis.get(key)
    if cached:
        return cached.decode()
    url = await _fetch_poster(filename)
    await redis.setex(key, 86400, url)
    return url


async def _fetch_poster(filename: str) -> str:
    title, year = parse_title_year(filename)
    if not title:
        return "https://via.placeholder.com/300x450?text=No+Poster"
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f"https://v3-cinemeta.strem.io/catalog/movie/top/search={title} {year}.json"
            )
            metas = r.json().get("metas", [])
            if metas and metas[0].get("poster"):
                return metas[0]["poster"]
    except Exception:
        pass
    return f"https://via.placeholder.com/300x450?text={title.replace(' ', '+')}"


async def get_cinemeta(type_name: str, imdb_id: str) -> tuple[str, str]:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"https://v3-cinemeta.strem.io/meta/{type_name}/{imdb_id}.json")
            meta = r.json().get("meta", {})
            return meta.get("name", ""), str(meta.get("year", ""))
    except Exception:
        return "", ""


# ── String helpers ────────────────────────────────────────────────────────────
def movie_id(filename: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", filename.lower())


def fmt_size(size) -> str:
    if not size:
        return "Unknown"
    size = float(size)
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {u}"
        size /= 1024
    return f"{size:.1f} PB"


def quality(fn: str) -> str:
    n = fn.lower()
    for tag in ["2160p", "4k", "1440p", "1080p", "720p", "480p", "360p"]:
        if tag in n:
            return tag.upper()
    return "Unknown"


def source(fn: str) -> str:
    n = fn.lower()
    for tag in ["bluray", "bdrip", "web-dl", "webdl", "webrip", "hdrip", "dvdrip", "hdtv", "remux"]:
        if tag in n:
            return tag.upper()
    return ""


def ctype(fn: str) -> str:
    n = fn.lower()
    if n.endswith(".mkv"):
        return "video/x-matroska"
    if n.endswith(".webm"):
        return "video/webm"
    return "video/mp4"


def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[._\-–—+]", " ", text)
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def flex_match(title: str, filename: str) -> bool:
    tn, fn = normalize(title), normalize(filename)
    if not tn or not fn:
        return False
    if tn in fn:
        return True
    tw, fw = tn.split(), fn.split()
    return sum(1 for w in tw if w in fw) >= max(1, len(tw) * 0.7)


def parse_title_year(filename: str) -> tuple[str, str]:
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", filename)
    name = re.sub(r"[._]", " ", name)
    ym   = re.search(r"\b(19|20)\d{2}\b", name)
    year = ym.group(0) if ym else ""
    cut  = re.split(
        r"\b(?:19|20)\d{2}\b|\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|"
        r"bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE,
    )[0]
    return re.sub(r"\s+", " ", cut).strip().title(), year


def parse_series(filename: str) -> Optional[dict]:
    m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,3})", filename)
    if m:
        return {"season": int(m.group(1)), "episode": int(m.group(2))}
    m2 = re.search(r"[Ss]eason\s*(\d+).*?[Ee]pisode\s*(\d+)", filename, re.IGNORECASE)
    if m2:
        return {"season": int(m2.group(1)), "episode": int(m2.group(2))}
    return None
