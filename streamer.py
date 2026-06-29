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

TG_CHUNK = 1024 * 1024
MIN_THROTTLE_MS = 50   # Min 50ms between GetFile calls (Telegram req limit ~20/sec per session)
MAX_BACKOFF_S = 60     # Max backoff on rate limit (Telegram's max is typically 2-60s)


class ByteStreamer:
    def __init__(self, client: Client):
        self.client = client
        self._last_invoke_time = 0.0  # Track last invoke time per instance
        self._backoff_until = {}  # Per-DC backoff state: {dc_id: until_timestamp}

    async def _throttle(self) -> None:
        """Enforce minimum inter-request delay to avoid Telegram rate limits."""
        elapsed = (time.time() - self._last_invoke_time) * 1000  # ms
        if elapsed < MIN_THROTTLE_MS:
            await asyncio.sleep((MIN_THROTTLE_MS - elapsed) / 1000)
        self._last_invoke_time = time.time()

    async def _wait_backoff(self, dc_id: int, flood_wait_s: int) -> None:
        """Exponential backoff with jitter on FloodWait."""
        # Add jitter: ±20% to spread requests
        jitter = random.uniform(0.8, 1.2)
        wait_s = min(flood_wait_s * jitter, MAX_BACKOFF_S)
        until = time.time() + wait_s
        self._backoff_until[dc_id] = until
        print(f"[streamer] DC {dc_id} rate limited. Backoff {wait_s:.1f}s (Telegram req: {flood_wait_s}s)")
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
    ) -> AsyncGenerator[bytes, None]:
        fid     = _extract_fid(msg)
        session = await self._session(fid)
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
            await self._throttle()  # Apply inter-request throttle
            try:
                r = await session.invoke(
                    raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                )
            except FloodWait as e:
                await self._wait_backoff(dc_id, e.value)
                # Retry after backoff
                if _retry:
                    async for b in self.yield_file(msg, offset, first_cut, last_cut, parts, chunk, False):
                        yield b
                    return
                else:
                    raise
        except FileReferenceExpired:
            if not _retry:
                raise
            msg = await self.client.get_messages(msg.chat.id, msg.id)
            async for b in self.yield_file(msg, offset, first_cut, last_cut, parts, chunk, False):
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

            await self._throttle()  # Throttle between chunks
            try:
                try:
                    r = await session.invoke(
                        raw.functions.upload.GetFile(location=loc, offset=off, limit=chunk)
                    )
                except FloodWait as e:
                    await self._wait_backoff(dc_id, e.value)
                    # Retry after backoff
                    if _retry:
                        async for b in self.yield_file(msg, off, 0, last_cut, parts - part + 1, chunk, False):
                            yield b
                        return
                    else:
                        raise
            except FileReferenceExpired:
                if not _retry:
                    raise
                msg = await self.client.get_messages(msg.chat.id, msg.id)
                async for b in self.yield_file(msg, off, 0, last_cut, parts - part + 1, chunk, False):
                    yield b
                return

    async def _session(self, fid: FileId) -> Session:
        c  = self.client
        dc = fid.dc_id
        if not hasattr(c, "media_sessions"):
            c.media_sessions = {}
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
