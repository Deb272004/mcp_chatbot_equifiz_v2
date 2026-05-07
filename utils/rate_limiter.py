"""
Redis-backed sliding-window rate limiter.
Keyed by client IP. Works correctly across multiple workers and restarts
because all workers share the same Redis instance.

Falls back to the in-memory limiter automatically if Redis is not reachable
(e.g. during local dev without Docker). Set REDIS_URL in .env to enable.
"""
import os
import time
import threading
from collections import defaultdict, deque
from fastapi import Request, HTTPException, status

REDIS_URL = os.environ.get("REDIS_URL", "")

# ── Try to connect to Redis once at import time ────────────────────────────────
_redis_client = None
if REDIS_URL:
    try:
        import redis
        _redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        _redis_client.ping()
    except Exception as e:
        import logging
        logging.getLogger("RateLimiter").warning(
            f"Redis not reachable ({e}) — falling back to in-memory rate limiter. "
            "This is fine for a single worker but will not work across multiple workers."
        )
        _redis_client = None


class RateLimiter:
    """Allow `max_calls` requests per `window_seconds` per IP."""

    def __init__(self, max_calls: int, window_seconds: int = 60):
        self.max_calls      = max_calls
        self.window         = window_seconds
        # in-memory fallback
        self._calls: dict   = defaultdict(deque)
        self._lock          = threading.Lock()

    def __call__(self, request: Request):
        ip = request.client.host if request.client else "unknown"
        if _redis_client:
            self._check_redis(ip)
        else:
            self._check_memory(ip)

    # ── Redis sliding window ───────────────────────────────────────────────────
    def _check_redis(self, ip: str):
        key = f"rl:{self.max_calls}:{self.window}:{ip}"
        now = time.time()
        window_start = now - self.window

        pipe = _redis_client.pipeline()
        # Remove timestamps older than the window
        pipe.zremrangebyscore(key, "-inf", window_start)
        # Count remaining
        pipe.zcard(key)
        # Add current request timestamp
        pipe.zadd(key, {str(now): now})
        # Expire the key after the window so Redis doesn't fill up
        pipe.expire(key, self.window + 1)
        results = pipe.execute()

        count = results[1]   # zcard result — before adding the new request
        if count >= self.max_calls:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Max {self.max_calls} requests per {self.window}s.",
            )

    # ── In-memory fallback (single worker only) ────────────────────────────────
    def _check_memory(self, ip: str):
        now = time.monotonic()
        with self._lock:
            q = self._calls[ip]
            while q and now - q[0] > self.window:
                q.popleft()
            if len(q) >= self.max_calls:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded. Max {self.max_calls} requests per {self.window}s.",
                )
            q.append(now)
