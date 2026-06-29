# Add these imports to main.py
# from metrics import metrics

# Add these endpoints to main.py after the existing routes:

"""
Add these imports at the top of main.py:
    from metrics import metrics

Add these route functions to main.py:
"""

async def _record_metrics_in_proxy():
    """Call this in proxy() function to track metrics."""
    await metrics.record_proxy_request()

# Monitoring Endpoints

@app.get("/api/metrics")
async def get_metrics():
    """Get comprehensive metrics snapshot."""
    return metrics.get_stats()


@app.get("/api/metrics/rate-limits")
async def get_rate_limits():
    """Get detailed rate limit analytics."""
    now = time.time()
    hour_ago = now - 3600
    day_ago = now - 86400
    
    recent_hour = [e for e in metrics.rate_limit_events if e[0] > hour_ago]
    recent_day = [e for e in metrics.rate_limit_events if e[0] > day_ago]
    
    # Group by DC
    dc_stats = {}
    for ts, dc_id, wait_s in recent_day:
        if dc_id not in dc_stats:
            dc_stats[dc_id] = {"count": 0, "total_wait": 0, "max_wait": 0}
        dc_stats[dc_id]["count"] += 1
        dc_stats[dc_id]["total_wait"] += wait_s
        dc_stats[dc_id]["max_wait"] = max(dc_stats[dc_id]["max_wait"], wait_s)
    
    return {
        "hour": {
            "events": len(recent_hour),
            "total_wait_s": round(sum(e[2] for e in recent_hour), 1),
        },
        "day": {
            "events": len(recent_day),
            "total_wait_s": round(sum(e[2] for e in recent_day), 1),
            "avg_wait_s": round(sum(e[2] for e in recent_day) / max(1, len(recent_day)), 1),
        },
        "by_datacenter": {str(dc): stats for dc, stats in dc_stats.items()},
    }


@app.get("/api/metrics/cache")
async def get_cache_metrics():
    """Get cache performance metrics."""
    stats = metrics.get_stats()
    cache_stats = stats["cache"]
    total_requests = cache_stats["hits"] + cache_stats["misses"]
    
    # Estimate bandwidth saved
    bandwidth_saved_mb = cache_stats["bytes_cached"] / 1024 / 1024
    
    return {
        **cache_stats,
        "total_requests": total_requests,
        "bandwidth_saved_mb": round(bandwidth_saved_mb, 1),
        "avg_hit_size_kb": round(cache_stats["bytes_cached"] / max(1, cache_stats["hits"]) / 1024, 1),
    }


@app.get("/api/metrics/streaming")
async def get_streaming_metrics():
    """Get streaming path statistics."""
    stats = metrics.get_stats()
    return stats["streaming"]


@app.get("/api/metrics/health")
async def get_health_metrics():
    """Get system health indicators."""
    stats = metrics.get_stats()
    dl_stats = download_manager.stats()
    
    return {
        "http": stats["http"],
        "downloads": stats["downloads"],
        "rate_limit_pressure": {
            "events_per_hour": stats["rate_limits"]["recent_hour"],
            "avg_backoff_s": stats["rate_limits"]["avg_wait_s"],
        },
        "active_tasks": len(dl_stats),
        "memory_usage_estimate_mb": sum(
            s.get("size_on_disk_mb", 0) for s in dl_stats.values()
        ),
    }


@app.get("/api/metrics/export")
async def export_metrics():
    """Export metrics in Prometheus format."""
    stats = metrics.get_stats()
    lines = [
        "# HELP tgstream_rate_limit_events_total Total rate limit events",
        f"tgstream_rate_limit_events_total {stats['rate_limits']['total_events']}",
        "# HELP tgstream_rate_limit_wait_seconds Total time spent in rate limit backoff",
        f"tgstream_rate_limit_wait_seconds {stats['rate_limits']['total_wait_s']}",
        "# HELP tgstream_cache_hits_total Successful cache reads",
        f"tgstream_cache_hits_total {stats['cache']['hits']}",
        "# HELP tgstream_cache_misses_total Cache misses (fetched from Telegram)",
        f"tgstream_cache_misses_total {stats['cache']['misses']}",
        "# HELP tgstream_http_requests_total Total HTTP requests",
        f"tgstream_http_requests_total {stats['http']['total_requests']}",
        "# HELP tgstream_http_errors_total HTTP errors",
        f"tgstream_http_errors_total {stats['http']['errors']}",
        "# HELP tgstream_downloads_active Active download tasks",
        f"tgstream_downloads_active {stats['downloads']['active']}",
    ]
    return "\n".join(lines), 200, {"Content-Type": "text/plain"}
