"""
Signal Engine — NATB v2.0

Responsibilities:
  1. Iterate the WATCHLIST and fetch multi-timeframe data for each ticker.
  2. Run Mean Reversion and Momentum strategies in parallel (thread pool).
  3. Select the best signal per ticker (highest score; must exceed MIN_CONFIDENCE).
  4. Gate every signal through the risk engine (can_open_trade).
  5. Enforce the per-user daily trade cap (MAX_DAILY_TRADES).
  6. Dispatch valid signals to place_trade_for_user for EVERY eligible subscriber.
  7. Log every step: scan start/end, signal found, risk gate result, order outcome.

Called from main.py on a SCAN_INTERVAL_SEC schedule.
Never places a trade without passing can_open_trade().
"""

import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from config import (
    WATCHLIST,
    MIN_CONFIDENCE,
    MAX_DAILY_TRADES,
    SCAN_INTERVAL_SEC,
)
from core.strategy_meanrev  import analyze as analyze_meanrev
from core.strategy_momentum import analyze as analyze_momentum
from core.risk_manager      import can_open_trade
from core.executor          import place_trade_for_user
from utils.market_scanner   import scan_multi_timeframe

DB_PATH = "database/trading_saas.db"

log = logging.getLogger(__name__)


# ── Subscriber helpers ────────────────────────────────────────────────────────

def _get_active_subscribers() -> list[str]:
    """Return chat_ids of all active, credentialed subscribers."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c    = conn.cursor()
        c.execute(
            "SELECT chat_id FROM subscribers "
            "WHERE is_active=1 AND email IS NOT NULL"
        )
        rows = c.fetchall()
        conn.close()
        return [str(r[0]) for r in rows]
    except Exception as exc:
        log.error("Failed to fetch subscribers: %s", exc)
        return []


def _daily_trade_count(chat_id: str) -> int:
    """Count positions opened today for this user."""
    try:
        today = str(date.today())
        conn  = sqlite3.connect(DB_PATH)
        c     = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM trades "
            "WHERE chat_id=? AND DATE(opened_at)=?",
            (chat_id, today),
        )
        count = c.fetchone()[0]
        conn.close()
        return int(count)
    except Exception:
        return 0


# ── Per-ticker analysis ───────────────────────────────────────────────────────

def _analyze_ticker(symbol: str) -> dict | None:
    """
    Fetch multi-timeframe data and run both strategies.
    Returns the best signal dict or None.
    """
    log.debug("Scanning %s ...", symbol)

    timeframes = scan_multi_timeframe(symbol)
    if not timeframes:
        log.warning("[%s] Could not fetch timeframe data — skipped", symbol)
        return None

    signals = []

    # Mean Reversion
    try:
        sig = analyze_meanrev(symbol, timeframes)
        if sig:
            signals.append(sig)
    except Exception as exc:
        log.error("[%s] MeanRev error: %s", symbol, exc)

    # Momentum
    try:
        sig = analyze_momentum(symbol, timeframes)
        if sig:
            signals.append(sig)
    except Exception as exc:
        log.error("[%s] Momentum error: %s", symbol, exc)

    if not signals:
        return None

    # Pick the signal with the highest score; filter by confidence
    best = max(signals, key=lambda s: s["score"])

    if best["confidence"] < MIN_CONFIDENCE:
        log.debug(
            "[%s] Best signal confidence %.1f%% below MIN_CONFIDENCE %.1f%% — discarded",
            symbol, best["confidence"], MIN_CONFIDENCE,
        )
        return None

    best["symbol"] = symbol
    log.info(
        "SIGNAL [%s] %s | strategy=%s | score=%d | confidence=%.1f%% | %s",
        symbol,
        best["action"],
        best["strategy"],
        best["score"],
        best["confidence"],
        best["reason"],
    )
    return best


# ── Signal dispatch ───────────────────────────────────────────────────────────

def _dispatch_signal(signal: dict, subscribers: list[str]) -> int:
    """
    Attempt to place a trade for every eligible subscriber.

    Gates per subscriber:
      1. can_open_trade() — Circuit Breaker / Hard Block check
      2. Daily trade cap  — MAX_DAILY_TRADES per user per day

    Returns the number of users the trade was dispatched to.
    """
    symbol     = signal["symbol"]
    action     = signal["action"]
    confidence = signal["confidence"]
    strategy   = signal["strategy"]
    dispatched = 0

    for chat_id in subscribers:
        # ── Risk gate ─────────────────────────────────────────────────────────
        allowed, reason = can_open_trade(chat_id)
        if not allowed:
            log.info(
                "[Dispatch %s] user=%s blocked by risk engine: %s",
                symbol, chat_id, reason,
            )
            continue

        # ── Daily cap gate ────────────────────────────────────────────────────
        today_count = _daily_trade_count(chat_id)
        if today_count >= MAX_DAILY_TRADES:
            log.info(
                "[Dispatch %s] user=%s hit daily cap (%d/%d)",
                symbol, chat_id, today_count, MAX_DAILY_TRADES,
            )
            continue

        # ── Place order ───────────────────────────────────────────────────────
        try:
            # Use the strategy-provided stop_loss_pct so TP/SL are derived
            # from a controlled distance (not an unconstrained ATR fallback).
            result = place_trade_for_user(
                chat_id,
                symbol,
                action,
                confidence,
                stop_loss_pct=signal.get("stop_loss_pct"),
                strategy_label=strategy,
            )
            success = isinstance(result, str) and result.startswith("✅")
            log.info(
                "[Dispatch %s] user=%s strategy=%s result: %s",
                symbol, chat_id, strategy, result,
            )
            if success:
                dispatched += 1
        except Exception as exc:
            log.error(
                "[Dispatch %s] user=%s unhandled error: %s",
                symbol, chat_id, exc,
            )

    return dispatched


# ── Main scan loop (called externally) ───────────────────────────────────────

def run_scan() -> list[dict]:
    """
    Execute one full market scan across the WATCHLIST.

    Steps:
      1. Fetch active subscribers.
      2. Scan all tickers concurrently (ThreadPoolExecutor).
      3. Dispatch valid signals to all eligible users.
      4. Return a list of triggered signal dicts (for logging / Telegram summary).

    This function is STATELESS — it doesn't sleep or loop.
    The scheduler in main.py is responsible for repeating calls.
    """
    log.info("=== Market scan started — %d tickers ===", len(WATCHLIST))

    subscribers = _get_active_subscribers()
    if not subscribers:
        log.warning("No active subscribers — scan aborted")
        return []

    triggered = []

    # Parallel ticker scanning — IO-bound (yfinance HTTP calls)
    with ThreadPoolExecutor(max_workers=min(8, len(WATCHLIST))) as pool:
        futures = {pool.submit(_analyze_ticker, sym): sym for sym in WATCHLIST}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                signal = future.result()
            except Exception as exc:
                log.error("[%s] Unexpected analysis error: %s", sym, exc)
                signal = None

            if signal:
                count = _dispatch_signal(signal, subscribers)
                signal["dispatched_to"] = count
                triggered.append(signal)

    log.info(
        "=== Scan complete — %d signal(s) triggered across %d ticker(s) ===",
        len(triggered), len(WATCHLIST),
    )
    return triggered
