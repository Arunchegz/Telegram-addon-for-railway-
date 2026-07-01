"""
clients.py — Pool of Pyrogram clients for FloodWait failover.

Multiple Telegram sessions, all added to the same source channel(s).
Round-robin selection, skipping any client currently in FloodWait cooldown.
If every client is cooling down, waits for the soonest one to free up
rather than blocking forever.

Env vars:
  SESSION_STRING_1, SESSION_STRING_2, ... (preferred, any number)
  SESSION_STRING (back-compat fallback, used as the only client if no
                   numbered vars are set)
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Dict, List, Tuple

from pyrogram import Client


class ClientPool:
    def __init__(self):
        self.clients: List[Client] = []
        self._rr_counter = 0
        self._cooldown_until: Dict[int, float] = {}
        self._download_load: Dict[int, int] = {}   # idx -> # active DownloadTasks pinned to it
        self._lock = asyncio.Lock()

    @staticmethod
    def _load_sessions() -> List[str]:
        sessions = []
        i = 1
        while True:
            s = os.getenv(f"SESSION_STRING_{i}", "").strip()
            if not s:
                break
            sessions.append(s)
            i += 1
        if not sessions:
            s = os.getenv("SESSION_STRING", "").strip()
            if s:
                sessions.append(s)
        return sessions

    async def start(self, api_id: int, api_hash: str, channel_username: str | int = None):
        sessions = self._load_sessions()
        if not sessions:
            raise RuntimeError(
                "No sessions found. Set SESSION_STRING_1 (and optionally "
                "SESSION_STRING_2, ...) or fall back to SESSION_STRING."
            )
        for i, sess in enumerate(sessions):
            c = Client(
                f"streamer_{i}", api_id=api_id, api_hash=api_hash,
                session_string=sess, no_updates=True, workers=16,
            )
            await c.start()
            if channel_username:
                try:
                    await c.get_chat(channel_username)
                    print(f"[clients] client {i} successfully resolved channel {channel_username}")
                except Exception as e:
                    print(f"[clients] client {i} failed to resolve channel {channel_username}: {e}")
            else:
                try:
                    async for _ in c.get_dialogs(limit=100):
                        pass
                except Exception as e:
                    print(f"[clients] peer-cache warmup failed for client {i}: {e}")
            self.clients.append(c)
            print(f"[clients] client {i} started")
        print(f"[clients] pool ready with {len(self.clients)} client(s)")

    async def stop(self):
        for c in self.clients:
            for s in list(getattr(c, "media_sessions", {}).values()):
                try:
                    await s.stop()
                except Exception:
                    pass
            if hasattr(c, "media_sessions"):
                c.media_sessions.clear()
            try:
                await c.stop()
            except Exception:
                pass

    def mark_cooldown(self, idx: int, seconds: float):
        self._cooldown_until[idx] = time.time() + seconds
        print(f"[clients] client {idx} cooling down for {seconds:.1f}s")

    def _available(self) -> List[int]:
        now = time.time()
        return [i for i in range(len(self.clients)) if self._cooldown_until.get(i, 0) <= now]

    async def pick(self) -> Tuple[int, Client]:
        """Round-robin among clients not currently in cooldown.

        If all are cooling down, sleeps until the soonest one is free
        rather than picking a client guaranteed to FloodWait again.
        """
        async with self._lock:
            avail = self._available()
            if not avail:
                soonest = min(self._cooldown_until.values())
                wait = max(0.0, soonest - time.time())
                print(f"[clients] all {len(self.clients)} client(s) cooling down, waiting {wait:.1f}s")
                await asyncio.sleep(wait)
                avail = self._available() or list(range(len(self.clients)))
            # Rotate over the full client count, not len(avail) — avail shrinks
            # whenever a client is cooling down, which skewed the round-robin
            # toward whichever clients happened to be available at pick time.
            self._rr_counter = (self._rr_counter + 1) % len(self.clients)
            chosen = avail[self._rr_counter % len(avail)]
            return chosen, self.clients[chosen]

    def primary(self) -> Client:
        """Client used for cheap metadata calls (get_messages etc) that
        rarely trip FloodWait — no need to rotate these."""
        return self.clients[0]

    async def acquire_download_slot(self) -> Tuple[int, Client]:
        """Pick the client with the fewest active background DownloadTasks
        pinned to it (not cooling down), instead of blind round-robin.

        Plain pick() alternates purely by call count, so multiple long-lived
        DownloadTasks started close together can all land on the same client
        while others sit idle — the exact opposite of what the pool is for.
        Caller must call release_download_slot(idx) when the task ends.
        """
        async with self._lock:
            avail = self._available()
            if not avail:
                avail = list(range(len(self.clients)))
            chosen = min(avail, key=lambda i: self._download_load.get(i, 0))
            self._download_load[chosen] = self._download_load.get(chosen, 0) + 1
            return chosen, self.clients[chosen]

    def release_download_slot(self, idx: int) -> None:
        if idx in self._download_load:
            self._download_load[idx] = max(0, self._download_load[idx] - 1)

    def __len__(self):
        return len(self.clients)


pool = ClientPool()
