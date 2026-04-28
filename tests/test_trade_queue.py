"""
Unit tests for core/trade_queue.py — Sequential trade execution queue.

Run: python -m pytest tests/test_trade_queue.py -v
"""

import threading
import time
import pytest
from core.trade_queue import TradeQueue, QueuedTrade, QUEUE_MAX_RETRIES


# ── QueuedTrade Tests ──────────────────────────────────────────────────────────

class TestQueuedTrade:
    """Test the QueuedTrade dataclass."""

    def test_priority_sorting(self):
        """Higher confidence trades should sort first (lower sort_key)."""
        high = QueuedTrade(sort_key=(-90.0, 1), trade_id="T1", chat_id="a", symbol="AAPL", action="BUY", confidence=90.0, kwargs={})
        low = QueuedTrade(sort_key=(-70.0, 2), trade_id="T2", chat_id="b", symbol="MSFT", action="SELL", confidence=70.0, kwargs={})
        assert high < low  # higher confidence = lower sort_key = dequeued first

    def test_same_confidence_fifo(self):
        """Same confidence trades should be FIFO by sequence number."""
        first = QueuedTrade(sort_key=(-80.0, 1), trade_id="T1", chat_id="a", symbol="AAPL", action="BUY", confidence=80.0, kwargs={})
        second = QueuedTrade(sort_key=(-80.0, 2), trade_id="T2", chat_id="b", symbol="MSFT", action="SELL", confidence=80.0, kwargs={})
        assert first < second


# ── TradeQueue Tests ───────────────────────────────────────────────────────────

class TestTradeQueue:
    """Test the TradeQueue class."""

    def _make_queue(self, executor_fn=None):
        q = TradeQueue(executor_fn=executor_fn)
        return q

    def test_enqueue_returns_trade_id(self):
        q = self._make_queue()
        trade_id = q.enqueue(chat_id="123", symbol="AAPL", action="BUY", confidence=80.0)
        assert trade_id.startswith("TQ-")
        assert "AAPL" in trade_id

    def test_enqueue_increases_depth(self):
        q = self._make_queue()
        assert q.depth == 0
        q.enqueue(chat_id="123", symbol="AAPL", action="BUY", confidence=80.0)
        assert q.depth == 1
        q.enqueue(chat_id="456", symbol="MSFT", action="SELL", confidence=70.0)
        assert q.depth == 2

    def test_stats_initial(self):
        q = self._make_queue()
        stats = q.stats()
        assert stats["enqueued"] == 0
        assert stats["processed"] == 0
        assert stats["failed"] == 0

    def test_worker_processes_trades(self):
        results = []
        lock = threading.Lock()

        def mock_executor(chat_id, symbol, action, confidence, **kwargs):
            with lock:
                results.append({"chat_id": chat_id, "symbol": symbol, "action": action})
            return {"status": "opened"}

        q = self._make_queue(executor_fn=mock_executor)
        q.enqueue(chat_id="123", symbol="AAPL", action="BUY", confidence=90.0)
        q.enqueue(chat_id="456", symbol="MSFT", action="SELL", confidence=80.0)
        q.start()
        time.sleep(2.0)  # allow worker to process
        q.stop()

        assert len(results) == 2
        assert results[0]["symbol"] == "AAPL"  # higher confidence first
        assert results[1]["symbol"] == "MSFT"

    def test_retry_on_429(self):
        attempt_count = {"n": 0}
        lock = threading.Lock()

        def failing_executor(chat_id, symbol, action, confidence, **kwargs):
            with lock:
                attempt_count["n"] += 1
                if attempt_count["n"] <= 2:
                    return {"status": "error", "message": "HTTP 429 rate limited"}
            return {"status": "opened"}

        q = self._make_queue(executor_fn=failing_executor)
        # Override retry delay for fast test
        import core.trade_queue as tq_module
        original_base = tq_module.QUEUE_RETRY_BASE_DELAY_SEC
        tq_module.QUEUE_RETRY_BASE_DELAY_SEC = 0.1

        q.enqueue(chat_id="123", symbol="AAPL", action="BUY", confidence=90.0)
        q.start()
        time.sleep(3.0)
        q.stop()

        tq_module.QUEUE_RETRY_BASE_DELAY_SEC = original_base
        assert attempt_count["n"] >= 3  # initial + 2 retries

    def test_permanent_failure_no_retry(self):
        """Non-transient errors (400, 403) should not be retried."""
        call_count = {"n": 0}
        lock = threading.Lock()

        def perm_fail_executor(chat_id, symbol, action, confidence, **kwargs):
            with lock:
                call_count["n"] += 1
            return {"status": "rejected"}

        q = self._make_queue(executor_fn=perm_fail_executor)
        q.enqueue(chat_id="123", symbol="AAPL", action="BUY", confidence=90.0)
        q.start()
        time.sleep(1.0)
        q.stop()

        assert call_count["n"] == 1  # no retries for permanent failures

    def test_stop_graceful(self):
        q = self._make_queue()
        q.start()
        assert q._worker is not None
        assert q._worker.is_alive()
        q.stop(timeout=2.0)
        assert not q._worker.is_alive()

    def test_exponential_backoff_calculation(self):
        """Verify backoff increases exponentially with jitter."""
        delays = set()
        for attempt in range(1, 5):
            delay = TradeQueue._exponential_backoff_with_jitter(attempt)
            # delay should be in [0, 2^(attempt-1))
            assert delay >= 0
            assert delay <= 2 ** (attempt - 1)
            delays.add(round(delay, 4))
        # With jitter, delays should vary (not all identical)
        assert len(delays) >= 2  # at least some variation


# ── Integration Test ───────────────────────────────────────────────────────────

class TestTradeQueueIntegration:
    """Integration test: queue + rate limiter."""

    def test_rate_limiter_integrated(self):
        """Verify queue uses global rate limiter before each execution."""
        call_times = []
        lock = threading.Lock()

        def tracked_executor(chat_id, symbol, action, confidence, **kwargs):
            with lock:
                call_times.append(time.monotonic())
            return {"status": "opened"}

        q = self._make_queue(executor_fn=tracked_executor)
        for i in range(3):
            q.enqueue(chat_id=f"user_{i}", symbol=f"SYM{i}", action="BUY", confidence=80.0 + i)

        q.start()
        time.sleep(3.0)
        q.stop()

        # Verify executions were spaced (not all at once)
        if len(call_times) >= 2:
            gaps = [call_times[i+1] - call_times[i] for i in range(len(call_times)-1)]
            # With rate limiter at 2 req/s, gaps should be >= 0.3s
            assert all(g >= 0.2 for g in gaps), f"Gaps too short: {gaps}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
