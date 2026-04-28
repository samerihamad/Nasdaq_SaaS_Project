"""
Trade Queue — NATB v2.0

FIFO priority queue for trade execution with rate limiting.

Serializes trade execution to prevent concurrent HTTP 429 bursts on Capital.com.
Each trade is queued with a priority (signal confidence) and processed sequentially
by a single worker thread, with global rate limiting between each execution.

FIX (Apr 28): Created to solve the "thundering herd" problem where multiple
signals × multiple users fire broker requests simultaneously, exceeding
Capital.com's rate limits and causing 100% execution failure.

Usage:
    from core.trade_queue import trade_queue

    # Queue a trade (non-blocking, returns immediately)
    trade_id = trade_queue.enqueue(
        chat_id="123456",
        symbol="AAPL",
        action="BUY",
        confidence=75.0,
        kwargs={...},
    )

    # Start the worker thread (call once at engine startup)
    trade_queue.start()

    # Stop the worker thread (call at engine shutdown)
    trade_queue.stop()
"""

import heapq
import os
import random
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ── Configuration ──────────────────────────────────────────────────────────────

QUEUE_WORKER_INTERVAL_SEC = float(os.getenv("TRADE_QUEUE_WORKER_INTERVAL", "0.1"))
QUEUE_MAX_RETRIES = int(os.getenv("TRADE_QUEUE_MAX_RETRIES", "3"))
QUEUE_RETRY_BASE_DELAY_SEC = float(os.getenv("TRADE_QUEUE_RETRY_BASE_DELAY", "1.0"))
QUEUE_RETRY_MAX_DELAY_SEC = float(os.getenv("TRADE_QUEUE_RETRY_MAX_DELAY", "30.0"))
QUEUE_POST_EXECUTION_DELAY_SEC = float(os.getenv("TRADE_QUEUE_POST_EXEC_DELAY", "0.5"))


# ── Data Classes ───────────────────────────────────────────────────────────────

@dataclass(order=True)
class QueuedTrade:
    """A trade waiting for execution, sorted by priority (confidence desc)."""
    sort_key: tuple = field(compare=True)   # (-confidence, sequence_number)
    trade_id: str   = field(compare=False)
    chat_id: str    = field(compare=False)
    symbol: str     = field(compare=False)
    action: str     = field(compare=False)
    confidence: float = field(compare=False)
    kwargs: dict    = field(compare=False)
    retry_count: int  = field(compare=False, default=0)
    enqueue_time: float = field(compare=False, default=0.0)


# ── Trade Queue ────────────────────────────────────────────────────────────────

class TradeQueue:
    """
    Priority FIFO queue with single-worker execution and rate limiting.

    Design:
    - Producer: dispatch_signal() enqueues trades (non-blocking)
    - Consumer: single worker thread dequeues and executes sequentially
    - Rate limiting: global_rate_limiter.throttle() before each execution
    - Retry: exponential backoff with jitter on transient failures (429, 503)
    - Permanent failures (400, 403): logged and discarded
    """

    def __init__(self, executor_fn: Optional[Callable] = None):
        self._heap: list[QueuedTrade] = []
        self._lock = threading.Lock()
        self._event = threading.Event()       # signal worker when item enqueued
        self._stop_event = threading.Event()  # signal worker to stop
        self._worker: Optional[threading.Thread] = None
        self._executor_fn = executor_fn       # place_trade_for_user by default
        self._seq = 0                         # tiebreaker for same-priority items

        # Metrics
        self._metrics_lock = threading.Lock()
        self._enqueued = 0
        self._processed = 0
        self._failed = 0
        self._retried = 0

    # ── Public API ────────────────────────────────────────────────────────────

    def set_executor(self, fn: Callable):
        """Set the execution function (place_trade_for_user). Call before start()."""
        self._executor_fn = fn

    def enqueue(
        self,
        chat_id: str,
        symbol: str,
        action: str,
        confidence: float = 75.0,
        kwargs: Optional[dict] = None,
    ) -> str:
        """
        Add a trade to the queue. Returns a trade ID for tracking.

        Non-blocking: returns immediately, execution happens in worker thread.
        """
        with self._lock:
            self._seq += 1
            trade_id = f"TQ-{self._seq:06d}-{symbol}-{int(time.time())}"
            # Negative confidence so higher confidence = lower sort key = dequeued first
            sort_key = (-confidence, self._seq)
            trade = QueuedTrade(
                sort_key=sort_key,
                trade_id=trade_id,
                chat_id=chat_id,
                symbol=symbol,
                action=action,
                confidence=confidence,
                kwargs=kwargs or {},
                enqueue_time=time.monotonic(),
            )
            heapq.heappush(self._heap, trade)
            self._enqueued += 1

        self._event.set()  # wake worker
        return trade_id

    def start(self):
        """Start the worker thread. Call once at engine startup."""
        if self._worker and self._worker.is_alive():
            return
        self._stop_event.clear()
        self._worker = threading.Thread(target=self._worker_loop, daemon=True, name="TradeQueueWorker")
        self._worker.start()

    def stop(self, timeout: float = 5.0):
        """Stop the worker thread gracefully."""
        self._stop_event.set()
        self._event.set()  # unblock worker if waiting
        if self._worker and self._worker.is_alive():
            self._worker.join(timeout=timeout)

    @property
    def depth(self) -> int:
        """Current number of trades in the queue."""
        with self._lock:
            return len(self._heap)

    def stats(self) -> dict:
        """Return queue metrics for observability."""
        with self._metrics_lock:
            return {
                "depth": self.depth,
                "enqueued": self._enqueued,
                "processed": self._processed,
                "failed": self._failed,
                "retried": self._retried,
            }

    # ── Worker Loop ───────────────────────────────────────────────────────────

    def _worker_loop(self):
        """Main worker loop: dequeue and execute trades sequentially."""
        while not self._stop_event.is_set():
            # Wait for a trade to be enqueued
            self._event.wait(timeout=QUEUE_WORKER_INTERVAL_SEC)
            self._event.clear()

            # Process all available trades
            while True:
                trade = self._dequeue()
                if trade is None:
                    break
                self._execute_trade(trade)

    def _dequeue(self) -> Optional[QueuedTrade]:
        """Pop the highest-priority trade from the queue."""
        with self._lock:
            if self._heap:
                return heapq.heappop(self._heap)
            return None

    def _execute_trade(self, trade: QueuedTrade):
        """Execute a single trade with rate limiting and retry logic."""
        if self._executor_fn is None:
            print(f"[TRADE QUEUE] No executor function set, discarding {trade.trade_id}", flush=True)
            with self._metrics_lock:
                self._failed += 1
            return

        # FIX (Apr 28): Acquire global rate limiter before execution
        from core.rate_limiter import global_rate_limiter
        global_rate_limiter.throttle()

        try:
            result = self._executor_fn(
                chat_id=trade.chat_id,
                symbol=trade.symbol,
                action=trade.action,
                confidence=trade.confidence,
                **trade.kwargs,
            )

            # Classify result
            if isinstance(result, dict):
                status = result.get("status", "unknown")
            elif isinstance(result, str):
                status = "opened" if result.startswith("✅") else "failed"
            else:
                status = "unknown"

            if status in ("opened", "filled"):
                print(
                    f"[TRADE QUEUE] ✅ {trade.trade_id} | {trade.symbol} {trade.action} | "
                    f"user={trade.chat_id[:8]}... | status={status}",
                    flush=True,
                )
                with self._metrics_lock:
                    self._processed += 1
            elif status in ("pending_limit",):
                print(
                    f"[TRADE QUEUE] 🧾 {trade.trade_id} | {trade.symbol} {trade.action} | "
                    f"user={trade.chat_id[:8]}... | status={status}",
                    flush=True,
                )
                with self._metrics_lock:
                    self._processed += 1
            elif status in ("skipped", "rejected"):
                # Permanent failure — don't retry
                print(
                    f"[TRADE QUEUE] ⏭️ {trade.trade_id} | {trade.symbol} {trade.action} | "
                    f"user={trade.chat_id[:8]}... | status={status} | msg={str(result)[:100]}",
                    flush=True,
                )
                with self._metrics_lock:
                    self._failed += 1
            else:
                # Transient or unknown — retry with backoff
                self._handle_retry_or_fail(trade, str(result))

        except Exception as exc:
            print(
                f"[TRADE QUEUE] ❌ {trade.trade_id} | exception: {exc}",
                flush=True,
            )
            self._handle_retry_or_fail(trade, str(exc))

        # FIX (Apr 28): Space executions to prevent burst
        if QUEUE_POST_EXECUTION_DELAY_SEC > 0:
            time.sleep(QUEUE_POST_EXECUTION_DELAY_SEC)

    def _handle_retry_or_fail(self, trade: QueuedTrade, error_msg: str):
        """Retry with exponential backoff or mark as permanently failed."""
        is_transient = any(kw in error_msg.lower() for kw in ("429", "too-many", "timeout", "503", "connection"))

        if is_transient and trade.retry_count < QUEUE_MAX_RETRIES:
            trade.retry_count += 1
            delay = self._exponential_backoff_with_jitter(trade.retry_count)
            print(
                f"[TRADE QUEUE] 🔄 Retrying {trade.trade_id} | attempt={trade.retry_count}/{QUEUE_MAX_RETRIES} | "
                f"delay={delay:.2f}s | error={error_msg[:80]}",
                flush=True,
            )
            time.sleep(delay)
            # Re-enqueue at same priority
            with self._lock:
                heapq.heappush(self._heap, trade)
            with self._metrics_lock:
                self._retried += 1
        else:
            print(
                f"[TRADE QUEUE] ❌ FAILED {trade.trade_id} | {trade.symbol} {trade.action} | "
                f"user={trade.chat_id[:8]}... | retries={trade.retry_count} | error={error_msg[:100]}",
                flush=True,
            )
            with self._metrics_lock:
                self._failed += 1

    @staticmethod
    def _exponential_backoff_with_jitter(attempt: int) -> float:
        """
        AWS-style exponential backoff with full jitter.

        attempt=1 → base=1s  → jitter ∈ [0, 1s)
        attempt=2 → base=2s  → jitter ∈ [0, 2s)
        attempt=3 → base=4s  → jitter ∈ [0, 4s)
        """
        exponential = QUEUE_RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1))
        capped = min(exponential, QUEUE_RETRY_MAX_DELAY_SEC)
        return random.uniform(0, capped)


# ── Singleton ─────────────────────────────────────────────────────────────────

trade_queue = TradeQueue()
