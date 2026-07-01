"""  
streamer.py — Pyrogram MTProto ByteStreamer with rate limit mitigation.
Extracted module; imported by main.py and downloader.py.

Rate limit strategies:
  1. Exponential backoff with jitter on FloodWait
  2. Per-DC session pooling (reuse sessions, reduce auth overhead)
  3. Request throttling between GetFile calls
  4. Adaptive chunking (smaller chunks when rate limited)
"""
from __future__ import annotations
import asyncio
import random
import time
from typing import AsyncGenerator

from pyrogram import Client, raw, utils
from pyrogram.errors import AuthBytesInvalid, FileReferenceExpired, FloodWait
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

TG_CHUNK = 1024 * 1024        # Live streaming chunk size (1MB) - balances startup speed and API calls
PREFETCH_CHUNK = 2 * 1024 * 1024   # Background prefetch logical chunk size (2MB) - fewer GetFile requests, higher throughput
TG_MAX_LIMIT = 1024 * 1024      # Telegram's maximum allowed limit per GetFile request (hard API limit)
MIN_THROTTLE_MS = 500  # 500ms between GetFile calls (~2 req/s); conservative to avoid rate limits
MAX_BACKOFF_S = 60     # Max backoff on rate limit (Telegram's max is typically 2-60s)
MAX_CONCURRENT_GETFILE = 1  # Single concurrent GetFile to prevent request storms


class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client
        self._last_invoke_time: dict = {}      # key: c_idx (None for single-client mode)
        self._throttle_locks: dict = {}        # per-client lock, created lazily
        self._session_lock = asyncio.Lock()   # Lock to serialize session creation
        self._backoff_until = {}  # Per-DC backoff state: {dc_id: until_timestamp}
        # If `client` is actually a ClientPool (has __len__), scale concurrent
        # GetFile slots to the pool size — one slot per session — instead of
        # serializing every stream in the process through a single global lock.
        pool_size = len(client) if hasattr(client, "__len__") else 1
        concurrency = max(MAX_CONCURRENT_GETFILE, pool_size)
        self._concurrent_semaphore = asyncio.Semaphore(concurrency)  # Global concurrency limit

        # Live-playback priority: counts requests currently pulling bytes
        # for active/foreground streaming (Path C tail + Path D). Background
        # downloader checks this and pauses while it's > 0, so both sessions
        # stay free for whoever is actually watching right now.
        self.live_streams = 0

    def mark_live_start(self) -> None:
        self.live_streams += 1

    def mark_live_end(self) -> None:
        self.live_streams = max(0, self.live_streams - 1)

    async def _throttle(self, c_idx=None) -> None:
        """Enforce minimum inter-request delay to avoid Telegram rate limits.

        Keyed per client (c_idx) — each session gets its own 500ms budget
        instead of all sessions sharing one global timer. A pool of N
        clients can therefore sustain ~N req/s combined instead of being
        capped at ~1 req/s system-wide regardless of pool size.
        """
        lock = self._throttle_locks.setdefault(c_idx, asyncio.Lock())
        async with lock:
            last = self._last_invoke_time.get(c_idx, 0.0)
            elapsed = (time.time() - last) * 1000
            if elapsed < MIN_THROTTLE_MS:
                await asyncio.sleep((MIN_THROTTLE_MS - elapsed) / 1000)
            self._last_invoke_time[c_idx] = time.time()

    async def _wait_backoff(self, dc_id: int, flood_wait_s: int) -> None:
        """Exponential backoff with jitter on FloodWait."""
        # Add jitter: ±20% to spread requests
        jitter = random.uniform(0.8, 1.2)
        wait_s = min(flood_wait_s * jitter, MAX_BACKOFF_S)
        until = time.time() + wait_s
        self._backoff_until[dc_id] = until
        print(f"[streamer] DC {dc_id} rate limited. Backoff {wait_s:.1f}s (Telegram req: {flood_wait_s}s)")
        try:
            from metrics import metrics
            await metrics.record_rate_limit(dc_id, wait_s)
        except Exception as e:
            print(f"[streamer] metrics error: {e}")
        await asyncio.sleep(wait_s)

    async def yield_file(
        self,
        msg,
        offset: int,
        first_cut: int,
        last_cut: int,
        parts: int,
        chunk: int = TG_CHUNK,
        _retry: bool = True,
        c: Client = None,
        c_idx: int = None,
    ) -> AsyncGenerator[bytes, None]:
        fid     = _extract_fid(msg)
        if c is None:
            if hasattr(self.client, "pick"):
                c_idx, c = await self.client.pick()
            else:
                c_idx, c = None, self.client
        session = await self._session(c, fid)
        loc     = _location(fid)
        part    = 1
        off     = offset
        dc_id   = fid.dc_id

        # Check if DC is in backoff; if so, wait
        if dc_id in self._backoff_until:
            until = self._backoff_until[dc_id]
            if time.time() < until:
                remaining = until - time.time()
                print(f"[streamer] Waiting for DC {dc_id} backoff: {remaining:.1f}s")
                await asyncio.sleep(remaining)
            del self._backoff_until[dc_id]

        try:
            async with self._concurrent_semaphore:
                await self._throttle(c_idx)  # Apply inter-request throttle
                try:
                    r = await session.invoke(
                        raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                    )
                except FloodWait as e:
                    if c_idx is not None and hasattr(self.client, "mark_cooldown"):
                        self.client.mark_cooldown(c_idx, e.value)
                    await self._wait_backoff(dc_id, e.value)
                    # Retry after backoff
                    if _retry:
                        async for b in self.yield_file(msg, offset, first_cut, last_cut, parts, chunk, False, c, c_idx):
                            yield b
                        return
                    else:
                        raise
        except FileReferenceExpired:
            if not _retry:
                raise
            msg = await c.get_messages(msg.chat.id, msg.id)
            async for b in self.yield_file(msg, offset, first_cut, last_cut, parts, chunk, False, c, c_idx):
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
            elif part == 1:
                yield data[first_cut:]
            elif part == parts:
                yield data[:last_cut]
            else:
                yield data

            part += 1
            off  += chunk
            if part > parts:
                break

            await self._throttle(c_idx)  # Throttle between chunks
            try:
                async with self._concurrent_semaphore:
                    r = await session.invoke(
                        raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                    )
            except FloodWait as e:
                if c_idx is not None and hasattr(self.client, "mark_cooldown"):
                    self.client.mark_cooldown(c_idx, e.value)
                await self._wait_backoff(dc_id, e.value)
                # Retry after backoff
                if _retry:
                    async for b in self.yield_file(msg, off, 0, last_cut, parts - part + 1, chunk, False, c, c_idx):
                        yield b
                    return
                else:
                    raise
            except FileReferenceExpired:
                if not _retry:
                    raise
                msg = await c.get_messages(msg.chat.id, msg.id)
                async for b in self.yield_file(msg, off, 0, last_cut, parts - part + 1, chunk, False, c, c_idx):
                    yield b
                return

    async def _session(self, c: Client, fid: FileId) -> Session:
        dc = fid.dc_id
        if not hasattr(c, "media_sessions"):
            c.media_sessions = {}
        if dc in c.media_sessions:
            return c.media_sessions[dc]

        async with self._session_lock:
            # Double check inside lock
            if dc in c.media_sessions:
                return c.media_sessions[dc]

            if dc != await c.storage.dc_id():
                s = Session(
                    c, dc,
                    await Auth(c, dc, await c.storage.test_mode()).create(),
                    await c.storage.test_mode(),
                    is_media=True,
                )
                await s.start()
                for _ in range(6):
                    exp = await c.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc))
                    try:
                        await s.invoke(
                            raw.functions.auth.ImportAuthorization(id=exp.id, bytes=exp.bytes)
                        )
                        break
                    except AuthBytesInvalid:
                        continue
                else:
                    await s.stop()
                    raise AuthBytesInvalid
            else:
                s = Session(
                    c, dc,
                    await c.storage.auth_key(),
                    await c.storage.test_mode(),
                    is_media=True,
                )
                await s.start()

            c.media_sessions[dc] = s
            return s


def _extract_fid(msg) -> FileId:
    media = msg.video or msg.document
    if not media:
        raise ValueError("No streamable media")
    return FileId.decode(media.file_id)


def _location(fid: FileId):
    ft = fid.file_type
    if ft == FileType.CHAT_PHOTO:
        if fid.chat_id > 0:
            peer = raw.types.InputPeerUser(user_id=fid.chat_id, access_hash=fid.chat_access_hash)
        elif fid.chat_access_hash == 0:
            peer = raw.types.InputPeerChat(chat_id=-fid.chat_id)
        else:
            peer = raw.types.InputPeerChannel(
                channel_id=utils.get_channel_id(fid.chat_id),
                access_hash=fid.chat_access_hash,
            )
        return raw.types.InputPeerPhotoFileLocation(
            peer=peer, volume_id=fid.volume_id, local_id=fid.local_id,
            big=fid.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
        )
    elif ft == FileType.PHOTO:
        return raw.types.InputPhotoFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size,
        )
    else:
        return raw.types.InputDocumentFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size,
        )
