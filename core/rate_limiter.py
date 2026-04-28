"""
Rate Limiter — NATB v2.0

Token-bucket rate limiter for Capital.com API calls.

Capital.com enforces limits per API key (per user session), not per IP,
so each user gets their own bucket. The coordinator ensures no single user
floods their session, which could cause 429 errors and missed trade exits.

FIX (Apr 28): Reduced defaults from 8 req/s to 2 req/s (capacity 5) to match
observed Capital.com rate limits. Previous 8 req/s caused HTTP 429 bursts.

Usage:
    from core.rate_limiter import rate_limiter, global_rate_limiter

    rate_limiter.throttle("user_chat_id")   # per-user throttle
    global_rate_limiter.throttle()          # global cross-user throttle
    response = requests.get(url, headers=headers)
"""

import os
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

    def __init__(self, rate: float = 2.0, capacity: int = 5):
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

    @property
    def available(self) -> float:
        """Current token count (for observability)."""
        with self._lock:
            self._refill()
            return self._tokens


class RateLimitCoordinator:
    """
    Per-user token bucket pool.

    Each chat_id gets its own isolated bucket, so one high-frequency user
    cannot consume tokens allocated to another user's session.
    """

    def __init__(self, rate: float = 2.0, capacity: int = 5):
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


class GlobalRateLimiter:
    """
    Single shared token bucket for ALL Capital.com requests across ALL users.

    This prevents the "thundering herd" where multiple users' requests
    fire simultaneously and collectively exceed the broker's rate limit.

    FIX (Apr 28): Added to coordinate cross-user request spacing.
    """

    def __init__(self, rate: float = 2.0, capacity: int = 5):
        self._bucket = _TokenBucket(rate=rate, capacity=capacity)

    def throttle(self):
        """Block until a global token is available. Call before every broker request."""
        self._bucket.wait()

    def try_acquire(self) -> bool:
        """Non-blocking global token check."""
        return self._bucket.consume()

    @property
    def available(self) -> float:
        """Current global token count (for observability)."""
        return self._bucket.available


# ── Singletons ────────────────────────────────────────────────────────────────

# Per-user rate limiter: 2 req/s per user session (matches Capital.com per-key limit)
_user_rate = float(os.getenv("RATE_LIMITER_USER_RPS", "2.0"))
_user_cap  = int(os.getenv("RATE_LIMITER_USER_CAPACITY", "5"))
rate_limiter = RateLimitCoordinator(rate=_user_rate, capacity=_user_cap)

# Global cross-user rate limiter: 2 req/s total across ALL users
_global_rate = float(os.getenv("RATE_LIMITER_GLOBAL_RPS", "2.0"))
_global_cap  = int(os.getenv("RATE_LIMITER_GLOBAL_CAPACITY", "5"))
global_rate_limiter = GlobalRateLimiter(rate=_global_rate, capacity=_global_cap)
