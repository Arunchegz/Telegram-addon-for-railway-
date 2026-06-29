"""
downloader.py — Hybrid predictive download engine with rate-limit awareness.

Architecture:
  SparseFile    — pre-truncated file, pwrite at any offset
  DownloadMap   — sorted merged interval list [[start,end], ...]
                  O(log n) lookup, O(n) merge
  DownloadTask  — asyncio task per movie: sequential MTProto fetch
                  writes to SparseFile, updates DownloadMap in Redis
                  signals waiting proxy requests via asyncio.Event
                  respects rate limits via adaptive backoff

Player never knows — proxy checks DownloadMap before each range,
serves local file if available, falls back to live Telegram otherwise.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import os
import time
from pathlib import Path
from typing import Optional

import redis.asyncio as aioredis

# ── Constants ───────────────────────────────────────────────────────────[...]
TG_CHUNK      = 1024 * 1024          # 1 MB per MTProto GetFile call
DL_CHUNK      = TG_CHUNK             # download chunk size (same)
DL_LOOKAHEAD  = 8 * 1024 * 1024      # download 8 MB ahead of play-head (reduced from 32 MB for efficient streaming)
STORAGE_DIR   = Path(os.getenv("STORAGE_DIR", "/tmp/tgstream"))
MAX_LOCAL_GB  = float(os.getenv("MAX_LOCAL_GB", "10"))  # evict LRU beyond this
DL_MIN_BACKOFF = float(os.getenv("DL_MIN_BACKOFF", "2"))  # Backoff on error (seconds)

# Redis key templates
R_DL_MAP  = "tgstream:dl:map:{}"    # JSON [[start,end],...]
R_DL_DONE = "tgstream:dl:done:{}"   # "1" when fully downloaded
R_DL_PATH = "tgstream:dl:path:{}"   # local file path string
R_DL_TS   = "tgstream:dl:ts:{}"     # last access timestamp (for LRU eviction)


# ────────────────────────────────────────────────────────────────────────[...]
# DownloadMap: sorted merged interval list
# ────────────────────────────────────────────────────────────────────────[...]
class DownloadMap:
    """
    Sorted list of non-overlapping [start, end] byte intervals.
    Merge on insert. O(log n) contains check.
    """

    def __init__(self, intervals: list[list[int]] | None = None):
        self._ivs: list[list[int]] = intervals or []

    # ── Serialisation ────────────────────────────────────────────────────────[...]
    def to_json(self) -> str:
        return json.dumps(self._ivs)

    @classmethod
    def from_json(cls, s: str | bytes) -> "DownloadMap":
        return cls(json.loads(s))

    # ── Query ───────────────────────────────────────────────────────────[...]
    def has_range(self, start: int, end: int) -> bool:
        """True if [start, end] fully covered by stored intervals."""
        if not self._ivs:
            return False
        # Binary search for rightmost interval whose start <= start
        idx = bisect.bisect_right(self._ivs, [start, float("inf")]) - 1
        if idx < 0:
            return False
        iv_start, iv_end = self._ivs[idx]
        return iv_start <= start and iv_end >= end

    def covered_prefix(self, start: int) -> int:
        """
        How many contiguous bytes are available starting from `start`.
        Returns 0 if nothing available at start.
        """
        if not self._ivs:
            return 0
        idx = bisect.bisect_right(self._ivs, [start, float("inf")]) - 1
        if idx < 0:
            return 0
        iv_start, iv_end = self._ivs[idx]
        if iv_start > start:
            return 0
        # Walk forward through contiguous intervals
        covered_end = iv_end
        for i in range(idx + 1, len(self._ivs)):
            ns, ne = self._ivs[i]
            if ns <= covered_end + 1:
                covered_end = max(covered_end, ne)
            else:
                break
        return max(0, covered_end - start + 1)

    def total_bytes(self) -> int:
        return sum(e - s + 1 for s, e in self._ivs)

    # ── Mutate ──────────────────────────────────────────────────────────[...]
    def add(self, start: int, end: int) -> None:
        """Insert [start, end] and merge overlapping/adjacent intervals."""
        new_iv = [start, end]
        merged: list[list[int]] = []
        inserted = False

        for iv in self._ivs:
            if iv[1] < new_iv[0] - 1:
                merged.append(iv)
            elif iv[0] > new_iv[1] + 1:
                if not inserted:
                    merged.append(new_iv)
                    inserted = True
                merged.append(iv)
            else:
                new_iv[0] = min(new_iv[0], iv[0])
                new_iv[1] = max(new_iv[1], iv[1])

        if not inserted:
            merged.append(new_iv)

        self._ivs = merged

    def clone(self) -> "DownloadMap":
        return DownloadMap([list(iv) for iv in self._ivs])


# ────────────────────────────────────────────────────────────────────────[...]
# SparseFile: pre-truncated file, pwrite semantics
# ────────────────────────────────────────────────────────────────────────[...]
class SparseFile:
    """
    Pre-allocated (sparse) file. Supports concurrent pwrite + pread.
    Uses asyncio.to_thread for blocking I/O so event loop stays free.
    """

    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()

    @classmethod
    async def create(cls, path: Path, size: int) -> "SparseFile":
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            # ftruncate creates sparse file (no disk allocation until written)
            await asyncio.to_thread(_truncate_file, path, size)
        return cls(path)

    async def pwrite(self, data: bytes, offset: int) -> None:
        await asyncio.to_thread(_pwrite, self.path, data, offset)

    async def pread(self, offset: int, length: int) -> bytes:
        return await asyncio.to_thread(_pread, self.path, offset, length)

    def exists(self) -> bool:
        return self.path.exists()

    async def delete(self) -> None:
        await asyncio.to_thread(self.path.unlink, missing_ok=True)


def _truncate_file(path: Path, size: int):
    with open(path, "wb") as f:
        f.truncate(size)


def _pwrite(path: Path, data: bytes, offset: int):
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)


def _pread(path: Path, offset: int, length: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(length)


# ────────────────────────────────────────────────────────────────────────[...]
# DownloadTask: per-movie background downloader
# ────────────────────────────────────────────────────────────────────────[...]
class DownloadTask:
    """
    Sequentially downloads a Telegram file to local SparseFile.
    Priority: start from current play-head hint, then continue forward.
    
    Lifecycle: created on first stream request, runs until file complete
    or task cancelled (eviction / shutdown).
    
    The proxy signals play-head via hint(offset) so downloader stays ahead.
    Handles rate limits gracefully with exponential backoff.
    """

    def __init__(
        self,
        movie_id: str,
        file_size: int,
        sparse: SparseFile,
        dl_map: DownloadMap,
        redis: aioredis.Redis,
        byte_streamer,          # streamer.ByteStreamer
        fetch_msg_fn,           # async fn(msg_id) -> msg
        message_id: int,
    ):
        self.movie_id    = movie_id
        self.file_size   = file_size
        self.sparse      = sparse
        self.dl_map      = dl_map
        self.redis       = redis
        self.streamer    = byte_streamer
        self.fetch_msg   = fetch_msg_fn
        self.message_id  = message_id

        self._task: Optional[asyncio.Task] = None
        self._hint: int = 0              # play-head hint from proxy
        self._progress_event = asyncio.Event()   # fires when new bytes land
        self._done = False
        self._msg = None                 # cached fresh message
        self._msg_fetched_at = 0.0
        self._error_backoff = 1.0        # Exponential backoff multiplier

    # ── Public API ─────────────────────────────────────────────────────────[...]
    def hint(self, offset: int):
        """Proxy tells downloader where player currently is."""
        if offset > self._hint:
            self._hint = offset

    def is_done(self) -> bool:
        return self._done

    def progress_event(self) -> asyncio.Event:
        return self._progress_event

    def start(self) -> asyncio.Task:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name=f"dl:{self.movie_id}")
        return self._task

    def cancel(self):
        if self._task and not self._task.done():
            self._task.cancel()

    # ── Internal ─────────────────────────────────────────────────────────[...]
    async def _fresh_msg(self):
        """Re-fetch message if file_reference may have expired (>50min old)."""
        now = time.time()
        if self._msg is None or (now - self._msg_fetched_at) > 3000:
            self._msg = await self.fetch_msg(self.message_id)
            self._msg_fetched_at = now
        return self._msg

    async def _run(self):
        """Main download loop. Sequential from play-head, skip already-downloaded."""
        print(f"[dl:{self.movie_id}] start size={self.file_size/1024/1024:.1f}MB, lookahead={DL_LOOKAHEAD/1024/1024:.0f}MB")

        while True:
            # Find next gap starting from current play-head hint
            offset = self._find_next_gap(self._hint)
            if offset >= self.file_size:
                # No gaps after hint, check from the beginning of the file
                offset = self._find_next_gap(0)
                if offset >= self.file_size:
                    # No gaps in the entire file!
                    break

            # Align to chunk boundary
            chunk_start = (offset // DL_CHUNK) * DL_CHUNK
            chunk_end   = min(chunk_start + DL_CHUNK - 1, self.file_size - 1)
            chunk_len   = chunk_end - chunk_start + 1

            try:
                msg = await self._fresh_msg()
                data = bytearray()
                async for piece in self.streamer.yield_file(
                    msg,
                    offset=chunk_start,
                    first_cut=0,
                    last_cut=chunk_len,
                    parts=1,
                    chunk=DL_CHUNK,
                ):
                    data.extend(piece)

                if data:
                    await self.sparse.pwrite(bytes(data), chunk_start)
                    self.dl_map.add(chunk_start, chunk_start + len(data) - 1)
                    await self._persist_map()
                    self._progress_event.set()
                    self._progress_event = asyncio.Event()
                    self._error_backoff = 1.0  # Reset backoff on success
                else:
                    raise Exception("No data received from Telegram")

            except asyncio.CancelledError:
                print(f"[dl:{self.movie_id}] cancelled at {offset/1024/1024:.1f}MB")
                return
            except Exception as e:
                backoff_s = DL_MIN_BACKOFF * self._error_backoff
                self._error_backoff = min(self._error_backoff * 2, 8)  # Cap at 8x
                print(f"[dl:{self.movie_id}] error at {offset}: {e}, backoff {backoff_s:.1f}s")
                await asyncio.sleep(backoff_s)
                self._msg = None   # force re-fetch
                continue

            # Yield to event loop — downloader is lower priority than proxy
            await asyncio.sleep(0)

        # Done
        self._done = True
        await self.redis.set(R_DL_DONE.format(self.movie_id), "1")
        total = self.dl_map.total_bytes()
        print(f"[dl:{self.movie_id}] complete {total/1024/1024:.1f}MB")

    def _find_next_gap(self, from_offset: int) -> int:
        """Find first byte >= from_offset not in dl_map."""
        ivs = self.dl_map._ivs
        if not ivs:
            return (from_offset // DL_CHUNK) * DL_CHUNK
        candidate = (from_offset // DL_CHUNK) * DL_CHUNK
        for s, e in ivs:
            if candidate < s:
                return candidate
            if s <= candidate <= e:
                candidate = e + 1
        return candidate

    async def _persist_map(self):
        """Save interval map to Redis for crash recovery."""
        await self.redis.set(
            R_DL_MAP.format(self.movie_id),
            self.dl_map.to_json(),
            ex=86400,   # 24h TTL — sparse file lives in /tmp
        )
        await self.redis.set(
            R_DL_TS.format(self.movie_id),
            str(time.time()),
            ex=86400,
        )


# ────────────────────────────────────────────────────────────────────────[...]
# DownloadManager: registry of active DownloadTasks
# ────────────────────────────────────────────────────────────────────────[...]
class DownloadManager:
    """
    Singleton registry. main.py imports and uses this directly.
    Handles task creation, dedup, eviction.
    """

    def __init__(self):
        self._tasks: dict[str, DownloadTask] = {}
        self._maps:  dict[str, DownloadMap]  = {}
        self._files: dict[str, SparseFile]   = {}
        self._lock   = asyncio.Lock()

    async def get_or_create(
        self,
        movie_id: str,
        file_size: int,
        message_id: int,
        redis: aioredis.Redis,
        byte_streamer,
        fetch_msg_fn,
    ) -> DownloadTask:
        async with self._lock:
            task = self._tasks.get(movie_id)
            if task and task._task and not task._task.done():
                return task

            # Restore map from Redis if exists (crash recovery)
            dl_map = await self._load_map(movie_id, redis)

            # Check if local file still valid (may have been wiped on restart)
            sparse_path = STORAGE_DIR / f"{movie_id}.bin"
            if not sparse_path.exists():
                # Disk wiped — reset map
                dl_map = DownloadMap()
                await redis.delete(R_DL_MAP.format(movie_id))
                await redis.delete(R_DL_DONE.format(movie_id))

            sparse = await SparseFile.create(sparse_path, file_size)
            self._files[movie_id] = sparse
            self._maps[movie_id]  = dl_map

            dt = DownloadTask(
                movie_id=movie_id,
                file_size=file_size,
                sparse=sparse,
                dl_map=dl_map,
                redis=redis,
                byte_streamer=byte_streamer,
                fetch_msg_fn=fetch_msg_fn,
                message_id=message_id,
            )
            dt.start()
            self._tasks[movie_id] = dt

            # Update access timestamp for LRU eviction
            await redis.set(R_DL_TS.format(movie_id), str(time.time()), ex=86400)

            return dt

    def get(self, movie_id: str) -> Optional[DownloadTask]:
        return self._tasks.get(movie_id)

    def get_map(self, movie_id: str) -> Optional[DownloadMap]:
        return self._maps.get(movie_id)

    def get_file(self, movie_id: str) -> Optional[SparseFile]:
        return self._files.get(movie_id)

    async def evict(self, movie_id: str, redis: aioredis.Redis):
        """Cancel task, delete local file, clear Redis download state."""
        async with self._lock:
            task = self._tasks.pop(movie_id, None)
            if task:
                task.cancel()
            f = self._files.pop(movie_id, None)
            if f:
                await f.delete()
            self._maps.pop(movie_id, None)
        await redis.delete(
            R_DL_MAP.format(movie_id),
            R_DL_DONE.format(movie_id),
            R_DL_PATH.format(movie_id),
            R_DL_TS.format(movie_id),
        )
        print(f"[dm] evicted {movie_id}")

    async def evict_lru_if_needed(self, redis: aioredis.Redis):
        """Evict oldest accessed movies if total local storage > MAX_LOCAL_GB."""
        total = sum(
            f.path.stat().st_size
            for f in self._files.values()
            if f.path.exists()
        )
        limit = MAX_LOCAL_GB * 1024 ** 3
        if total <= limit:
            return

        # Build LRU order from Redis timestamps
        order = []
        for mid in list(self._files.keys()):
            ts = await redis.get(R_DL_TS.format(mid))
            order.append((float(ts) if ts else 0.0, mid))
        order.sort()

        for _, mid in order:
            if total <= limit:
                break
            f = self._files.get(mid)
            size = f.path.stat().st_size if f and f.path.exists() else 0
            await self.evict(mid, redis)
            total -= size
            print(f"[dm] LRU evict {mid} freed {size/1024/1024:.0f}MB")

    async def shutdown(self):
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(
            *[t._task for t in self._tasks.values() if t._task],
            return_exceptions=True,
        )

    async def _load_map(self, movie_id: str, redis: aioredis.Redis) -> DownloadMap:
        raw = await redis.get(R_DL_MAP.format(movie_id))
        if raw:
            try:
                return DownloadMap.from_json(raw)
            except Exception:
                pass
        return DownloadMap()

    def stats(self) -> dict:
        result = {}
        for mid, task in self._tasks.items():
            f = self._files.get(mid)
            dm = self._maps.get(mid)
            size_on_disk = f.path.stat().st_size if f and f.path.exists() else 0
            result[mid] = {
                "done":           task.is_done(),
                "downloaded_mb":  round(dm.total_bytes() / 1024 / 1024, 1) if dm else 0,
                "size_on_disk_mb": round(size_on_disk / 1024 / 1024, 1),
                "task_running":   bool(task._task and not task._task.done()),
            }
        return result


# Module-level singleton — imported by main.py
download_manager = DownloadManager()
