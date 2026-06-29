"""  
metrics.py — TGStream monitoring and metrics collection.

Tracks:
  - Rate limit events (count, duration, backoff)
  - Stream performance (paths used, cache hits)
  - Download progress (speed, ETA)
  - System health (concurrent requests, errors)
"""
from __future__ import annotations
import time
from typing import Dict, Optional
import asyncio


class Metrics:
    """Thread-safe metrics collection."""

    def __init__(self):
        self._lock = asyncio.Lock()
        
        # Rate limit tracking
        self.rate_limit_events: list = []  # [(timestamp, dc_id, wait_s)]
        self.rate_limit_total_wait = 0.0   # Total seconds spent in backoff
        self.rate_limit_count = 0          # Total rate limit events
        
        # Stream path tracking
        self.stream_paths: Dict[str, int] = {  # path -> count
            "local": 0,
            "local-waited": 0,
            "mixed": 0,
            "telegram-live": 0,
        }
        self.stream_total = 0
        
        # Cache performance
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_hit_bytes = 0
        self.cache_miss_bytes = 0
        
        # Download stats
        self.downloads_completed = 0
        self.downloads_active = 0
        self.total_downloaded_mb = 0.0
        
        # Request tracking
        self.http_requests_total = 0
        self.http_errors = 0
        self.proxy_requests = 0

    async def record_rate_limit(self, dc_id: int, wait_s: float):
        """Record a rate limit event."""
        async with self._lock:
            self.rate_limit_events.append((time.time(), dc_id, wait_s))
            self.rate_limit_total_wait += wait_s
            self.rate_limit_count += 1
            # Keep only last 1000 events
            if len(self.rate_limit_events) > 1000:
                self.rate_limit_events = self.rate_limit_events[-1000:]

    async def record_stream_path(self, path: str):
        """Record which streaming path was used."""
        async with self._lock:
            if path in self.stream_paths:
                self.stream_paths[path] += 1
            self.stream_total += 1

    async def record_cache_hit(self, bytes_read: int):
        """Record successful cache read."""
        async with self._lock:
            self.cache_hits += 1
            self.cache_hit_bytes += bytes_read

    async def record_cache_miss(self, bytes_needed: int):
        """Record cache miss (had to fetch from Telegram)."""
        async with self._lock:
            self.cache_misses += 1
            self.cache_miss_bytes += bytes_needed

    async def record_http_request(self, success: bool):
        """Record HTTP request."""
        async with self._lock:
            self.http_requests_total += 1
            if not success:
                self.http_errors += 1

    async def record_proxy_request(self):
        """Record /proxy/ request."""
        async with self._lock:
            self.proxy_requests += 1

    def get_stats(self) -> dict:
        """Return current metrics snapshot."""
        cache_total = self.cache_hits + self.cache_misses
        cache_hit_rate = (self.cache_hits / cache_total * 100) if cache_total > 0 else 0
        
        # Rate limit stats (last hour)
        now = time.time()
        hour_ago = now - 3600
        recent_events = [e for e in self.rate_limit_events if e[0] > hour_ago]
        avg_wait = (sum(e[2] for e in recent_events) / len(recent_events)) if recent_events else 0
        
        # Stream path distribution
        path_dist = {}
        if self.stream_total > 0:
            for path, count in self.stream_paths.items():
                path_dist[path] = round(count / self.stream_total * 100, 1)
        
        # Error rate
        http_success_rate = ((self.http_requests_total - self.http_errors) / self.http_requests_total * 100) if self.http_requests_total > 0 else 100
        
        return {
            "rate_limits": {
                "total_events": self.rate_limit_count,
                "total_wait_s": round(self.rate_limit_total_wait, 1),
                "avg_wait_s": round(avg_wait, 1),
                "recent_hour": len(recent_events),
            },
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate_pct": round(cache_hit_rate, 1),
                "bytes_cached": self.cache_hit_bytes,
                "bytes_uncached": self.cache_miss_bytes,
            },
            "streaming": {
                "paths": path_dist,
                "total_requests": self.stream_total,
                "local_ratio_pct": round((self.stream_paths["local"] + self.stream_paths["local-waited"]) / max(1, self.stream_total) * 100, 1),
            },
            "http": {
                "total_requests": self.http_requests_total,
                "errors": self.http_errors,
                "success_rate_pct": round(http_success_rate, 1),
                "proxy_requests": self.proxy_requests,
            },
            "downloads": {
                "active": self.downloads_active,
                "completed": self.downloads_completed,
                "total_mb": round(self.total_downloaded_mb, 1),
            },
        }


# Global singleton
metrics = Metrics()
