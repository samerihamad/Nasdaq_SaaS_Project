"""
Unit tests for core/rate_limiter.py — Token Bucket rate limiter.

Run: python -m pytest tests/test_rate_limiter.py -v
"""

import threading
import time
import pytest
from core.rate_limiter import _TokenBucket, RateLimitCoordinator, GlobalRateLimiter


# ── _TokenBucket Tests ─────────────────────────────────────────────────────────

class TestTokenBucket:
    """Test the internal _TokenBucket class."""

    def test_initial_tokens_equal_capacity(self):
        bucket = _TokenBucket(rate=2.0, capacity=5)
        assert bucket.available == 5.0

    def test_consume_reduces_tokens(self):
        bucket = _TokenBucket(rate=2.0, capacity=5)
        assert bucket.consume() is True
        assert bucket.available == 4.0

    def test_consume_all_tokens_then_fail(self):
        bucket = _TokenBucket(rate=2.0, capacity=3)
        assert bucket.consume() is True
        assert bucket.consume() is True
        assert bucket.consume() is True
        assert bucket.consume() is False  # no tokens left

    def test_refill_over_time(self):
        bucket = _TokenBucket(rate=10.0, capacity=3)
        # Drain all tokens
        bucket.consume()
        bucket.consume()
        bucket.consume()
        assert bucket.available == 0.0
        # Wait for refill
        time.sleep(0.2)  # 10 req/s * 0.2s = 2 tokens
        assert bucket.available >= 1.9  # ~2 tokens refilled

    def test_refill_capped_at_capacity(self):
        bucket = _TokenBucket(rate=100.0, capacity=3)
        time.sleep(0.1)  # Would refill 10 tokens, but capped at 3
        assert bucket.available <= 3.0

    def test_wait_blocks_until_token_available(self):
        bucket = _TokenBucket(rate=100.0, capacity=1)
        bucket.consume()  # drain
        start = time.monotonic()
        bucket.wait()  # should unblock after ~10ms
        elapsed = time.monotonic() - start
        assert elapsed < 0.1  # should be fast at 100 req/s

    def test_concurrent_access(self):
        """Thread-safety: multiple threads consuming from same bucket."""
        bucket = _TokenBucket(rate=100.0, capacity=50)
        consumed = []
        lock = threading.Lock()

        def consumer():
            if bucket.consume():
                with lock:
                    consumed.append(1)

        threads = [threading.Thread(target=consumer) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(consumed) == 50  # all 50 tokens consumed


# ── RateLimitCoordinator Tests ─────────────────────────────────────────────────

class TestRateLimitCoordinator:
    """Test per-user rate limiting."""

    def test_isolated_user_buckets(self):
        coord = RateLimitCoordinator(rate=2.0, capacity=3)
        # User A consumes 3 tokens
        assert coord.try_acquire("user_a") is True
        assert coord.try_acquire("user_a") is True
        assert coord.try_acquire("user_a") is True
        assert coord.try_acquire("user_a") is False  # exhausted
        # User B still has tokens
        assert coord.try_acquire("user_b") is True

    def test_throttle_blocks(self):
        coord = RateLimitCoordinator(rate=100.0, capacity=1)
        coord.try_acquire("user_a")  # drain
        start = time.monotonic()
        coord.throttle("user_a")  # should unblock quickly
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_stats_returns_all_users(self):
        coord = RateLimitCoordinator(rate=2.0, capacity=5)
        coord.try_acquire("user_a")
        coord.try_acquire("user_b")
        stats = coord.stats()
        assert "user_a" in stats
        assert "user_b" in stats


# ── GlobalRateLimiter Tests ───────────────────────────────────────────────────

class TestGlobalRateLimiter:
    """Test cross-user global rate limiting."""

    def test_shared_bucket(self):
        limiter = GlobalRateLimiter(rate=2.0, capacity=3)
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False  # all consumed

    def test_available_property(self):
        limiter = GlobalRateLimiter(rate=2.0, capacity=5)
        limiter.try_acquire()
        assert limiter.available == 4.0

    def test_throttle_blocking(self):
        limiter = GlobalRateLimiter(rate=100.0, capacity=1)
        limiter.try_acquire()  # drain
        start = time.monotonic()
        limiter.throttle()  # should unblock quickly
        elapsed = time.monotonic() - start
        assert elapsed < 0.1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
