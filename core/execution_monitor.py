"""
Execution Monitor — NATB v2.0

Real-time execution health tracking and reporting.

Collects metrics from every trade execution attempt and produces
a health report at the end of each scan cycle. The report includes:
- Success/failure rates
- HTTP 429 rate limit event counts
- Average execution latency
- Queue depth and throughput
- Per-symbol breakdown

FIX (Apr 28): Phase 3 observability — full visibility into execution failures.

Usage:
    from core.execution_monitor import execution_monitor

    # Record each trade outcome
    execution_monitor.record_trade(symbol="AAPL", action="BUY", status="FILLED",
                                   attempts=1, elapsed_sec=0.8, chat_id="123")
    execution_monitor.record_trade(symbol="MSFT", action="SELL", status="FAILED",
                                   attempts=5, elapsed_sec=12.3, chat_id="456",
                                   error="HTTP 429")

    # Get health report at end of cycle
    report = execution_monitor.get_health_report()
    print(report["summary"])  # One-line summary
    print(report["details"])  # Full breakdown
"""

import os
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Optional


# ── Configuration ──────────────────────────────────────────────────────────────

LOG_ROOT = os.getenv("ENGINE_LOG_ROOT", "logs")


# ── Data Classes ───────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    """Single trade execution record."""
    symbol: str
    action: str
    status: str          # FILLED, FAILED, SKIPPED, DEAL_CONFIRM_FAILED, etc.
    attempts: int
    elapsed_sec: float
    chat_id: str = ""
    error: str = ""
    http_status: int = 0
    timestamp: float = field(default_factory=time.monotonic)


# ── Execution Monitor ─────────────────────────────────────────────────────────

class ExecutionMonitor:
    """
    Thread-safe execution health tracker.

    Collects per-trade metrics and produces aggregate health reports.
    Designed to be called from the main engine loop and the executor.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._trades: list[TradeRecord] = []
        self._rate_limit_hits = 0
        self._cycle_start: Optional[float] = None
        self._cycle_count = 0

    # ── Recording ────────────────────────────────────────────────────────────

    def start_cycle(self) -> None:
        """Mark the beginning of a new scan cycle."""
        with self._lock:
            self._cycle_start = time.monotonic()
            self._cycle_count += 1

    def record_trade(
        self,
        symbol: str,
        action: str,
        status: str,
        attempts: int = 1,
        elapsed_sec: float = 0.0,
        chat_id: str = "",
        error: str = "",
        http_status: int = 0,
    ) -> None:
        """Record a single trade execution outcome."""
        with self._lock:
            rec = TradeRecord(
                symbol=symbol,
                action=action,
                status=status,
                attempts=attempts,
                elapsed_sec=elapsed_sec,
                chat_id=chat_id,
                error=error,
                http_status=http_status,
            )
            self._trades.append(rec)
            if http_status == 429 or "429" in error:
                self._rate_limit_hits += 1

    def record_rate_limit(self) -> None:
        """Record a rate limit event (called from executor 429 handler)."""
        with self._lock:
            self._rate_limit_hits += 1

    # ── Reporting ────────────────────────────────────────────────────────────

    def get_health_report(self) -> dict:
        """
        Produce a comprehensive health report for the current cycle.

        Returns dict with keys:
          - summary: one-line human-readable summary
          - total_trades: int
          - filled: int
          - failed: int
          - skipped: int
          - success_rate_pct: float
          - rate_limit_hits: int
          - avg_attempts: float
          - avg_latency_sec: float
          - max_latency_sec: float
          - per_symbol: dict[str, dict]
          - cycle_elapsed_sec: float
        """
        with self._lock:
            trades = list(self._trades)
            rl_hits = self._rate_limit_hits
            cycle_start = self._cycle_start

        total = len(trades)
        if total == 0:
            return {
                "summary": "No trades executed this cycle",
                "total_trades": 0,
                "filled": 0,
                "failed": 0,
                "skipped": 0,
                "success_rate_pct": 0.0,
                "rate_limit_hits": rl_hits,
                "avg_attempts": 0.0,
                "avg_latency_sec": 0.0,
                "max_latency_sec": 0.0,
                "per_symbol": {},
                "cycle_elapsed_sec": time.monotonic() - cycle_start if cycle_start else 0.0,
            }

        filled = sum(1 for t in trades if t.status == "FILLED")
        failed = sum(1 for t in trades if t.status in ("FAILED", "DEAL_CONFIRM_FAILED"))
        skipped = sum(1 for t in trades if t.status == "SKIPPED")
        success_rate = (filled / total * 100.0) if total else 0.0
        avg_attempts = sum(t.attempts for t in trades) / total
        avg_latency = sum(t.elapsed_sec for t in trades) / total
        max_latency = max(t.elapsed_sec for t in trades)

        # Per-symbol breakdown
        per_symbol: dict[str, dict] = defaultdict(lambda: {"filled": 0, "failed": 0, "total": 0})
        for t in trades:
            key = t.symbol
            per_symbol[key]["total"] += 1
            if t.status == "FILLED":
                per_symbol[key]["filled"] += 1
            elif t.status in ("FAILED", "DEAL_CONFIRM_FAILED"):
                per_symbol[key]["failed"] += 1

        cycle_elapsed = time.monotonic() - cycle_start if cycle_start else 0.0

        summary = (
            f"Trades: {filled}/{total} filled ({success_rate:.1f}%) | "
            f"429s: {rl_hits} | "
            f"avg_latency: {avg_latency:.2f}s | "
            f"avg_attempts: {avg_attempts:.1f} | "
            f"cycle: {cycle_elapsed:.1f}s"
        )

        return {
            "summary": summary,
            "total_trades": total,
            "filled": filled,
            "failed": failed,
            "skipped": skipped,
            "success_rate_pct": round(success_rate, 1),
            "rate_limit_hits": rl_hits,
            "avg_attempts": round(avg_attempts, 2),
            "avg_latency_sec": round(avg_latency, 3),
            "max_latency_sec": round(max_latency, 3),
            "per_symbol": dict(per_symbol),
            "cycle_elapsed_sec": round(cycle_elapsed, 1),
        }

    def reset(self) -> None:
        """Clear all metrics for a fresh cycle."""
        with self._lock:
            self._trades.clear()
            self._rate_limit_hits = 0
            self._cycle_start = time.monotonic()

    def write_cycle_report(self) -> str:
        """
        Write the health report to execution_summary.txt and return the summary line.

        Called at the end of each scan cycle from main.py.
        """
        report = self.get_health_report()

        # Write detailed report to daily log
        try:
            day_dir = os.path.join(LOG_ROOT, time.strftime("%Y-%m-%d", time.gmtime()))
            os.makedirs(day_dir, exist_ok=True)
            path = os.path.join(day_dir, "execution_summary.txt")
            ts = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
            with open(path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] CYCLE_REPORT | {report['summary']}\n")
                # Per-symbol details
                for sym, data in report.get("per_symbol", {}).items():
                    f.write(
                        f"[{ts}]   symbol={sym} filled={data['filled']}/{data['total']} "
                        f"failed={data['failed']}\n"
                    )
        except Exception as exc:
            print(f"[EXEC MONITOR] Failed writing cycle report: {exc}", flush=True)

        return report["summary"]


# ── Singleton ─────────────────────────────────────────────────────────────────

execution_monitor = ExecutionMonitor()
