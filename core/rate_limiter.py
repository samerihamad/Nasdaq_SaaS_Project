"""
Rate Limiter — NATB v2.0

Token-bucket rate limiter for Capital.com API calls.

Capital.com enforces limits per API key (per user session), not per IP,
so each user gets their own bucket. The coordinator ensures no single user
floods their session, which could cause 429 errors and missed trade exits.

Default: 8 requests/second per user (conservative below the broker's 10/s limit).

Usage:
    from core.rate_limiter import rate_limiter

    rate_limiter.throttle("user_chat_id")   # blocks until a token is available
    response = requests.get(url, headers=headers)
"""

import threading
import time
from collections import defaultdict


class _TokenBucket:
    """
    Thread-safe token bucket.

    Tokens refill at `rate` per second up to `capacity`.
    `consume()` returns immediately if a token is available, otherwise
    blocks until one becomes available (max 1 token per call).
    """

    def __init__(self, rate: float = 8.0, capacity: int = 10):
        self._rate     = rate       # tokens added per second
        self._capacity = capacity   # max tokens
        self._tokens   = float(capacity)
        self._last     = time.monotonic()
        self._lock     = threading.Lock()

    def _refill(self):
        now     = time.monotonic()
        elapsed = now - self._last
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last   = now

    def consume(self) -> bool:
        """Try to consume one token. Returns True if immediately available."""
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def wait(self):
        """Block until a token is available, then consume it."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            # Wait for ~1 token to accumulate before retrying
            time.sleep(1.0 / self._rate)


class RateLimitCoordinator:
    """
    Per-user token bucket pool.

    Each chat_id gets its own isolated bucket, so one high-frequency user
    cannot consume tokens allocated to another user's session.
    """

    def __init__(self, rate: float = 8.0, capacity: int = 10):
        self._rate     = rate
        self._capacity = capacity
        self._buckets: dict[str, _TokenBucket] = {}
        self._lock     = threading.Lock()

    def _get_bucket(self, chat_id: str) -> _TokenBucket:
        with self._lock:
            if chat_id not in self._buckets:
                self._buckets[chat_id] = _TokenBucket(self._rate, self._capacity)
            return self._buckets[chat_id]

    def throttle(self, chat_id: str):
        """
        Block until a token is available for this user's session.
        Call this before every Capital.com API request.
        """
        self._get_bucket(chat_id).wait()

    def try_acquire(self, chat_id: str) -> bool:
        """
        Non-blocking token check.
        Returns True if a token was available and consumed, False otherwise.
        """
        return self._get_bucket(chat_id).consume()

    def stats(self) -> dict:
        """Return current token levels for all tracked users (debugging)."""
        with self._lock:
            return {uid: round(b._tokens, 1) for uid, b in self._buckets.items()}


# ── Singleton ─────────────────────────────────────────────────────────────────

rate_limiter = RateLimitCoordinator(rate=8.0, capacity=10)
