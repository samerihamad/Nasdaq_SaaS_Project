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

import asyncio
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import aiohttp

from utils.market_hours import utc_today
from config import (
    WATCHLIST,
    MIN_CONFIDENCE,
    SIGNAL_MIN_CONFIDENCE,
    MAX_DAILY_TRADES,
    SCAN_INTERVAL_SEC,
)
from core.strategy_meanrev  import analyze as analyze_meanrev
from core.strategy_momentum import analyze as analyze_momentum
from core.risk_manager      import can_open_trade
from core.executor          import place_trade_for_user
from utils.market_scanner import (
    scan_multi_timeframe,
    scan_multi_timeframe_async,
    CAPITAL_CLIENT_TIMEOUT,
    CAPITAL_HTTP_CONCURRENCY,
)
from utils.ai_model         import analyze_multi_timeframe

DB_PATH = "database/trading_saas.db"

log = logging.getLogger(__name__)
ACTIVE_MIN_CONFIDENCE = float(SIGNAL_MIN_CONFIDENCE if SIGNAL_MIN_CONFIDENCE is not None else MIN_CONFIDENCE)


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
        today = str(utc_today())
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
    structural_rejections = []

    # Mean Reversion
    try:
        sig = analyze_meanrev(symbol, timeframes)
        if sig and sig.get("rejected"):
            structural_rejections.append({
                "symbol": symbol,
                "strategy": sig.get("strategy", "MeanRev"),
                "reason": sig.get("reason", "Rejected by market structure filter"),
                "rejected": True,
            })
        elif sig:
            signals.append(sig)
    except Exception as exc:
        log.error("[%s] MeanRev error: %s", symbol, exc)

    # Momentum
    try:
        sig = analyze_momentum(symbol, timeframes)
        if sig and sig.get("rejected"):
            log.info(
                "[%s] REJECTED | strategy=Momentum | Reason: %s",
                symbol,
                str(sig.get("reason", "Rejected by market structure filter")),
            )
            structural_rejections.append({
                "symbol": symbol,
                "strategy": sig.get("strategy", "Momentum"),
                "reason": sig.get("reason", "Rejected by market structure filter"),
                "rejected": True,
            })
        elif sig:
            signals.append(sig)
    except Exception as exc:
        log.error("[%s] Momentum error: %s", symbol, exc)

    if not signals:
        if structural_rejections:
            for rej in structural_rejections:
                log.info(
                    "[%s] REJECTED | strategy=%s | Reason: %s",
                    symbol, rej.get("strategy", "Unknown"), rej.get("reason", "Rejected"),
                )
            # Keep structural rejections as internal logs only in this path.
            return None
        return None

    # Pick the signal with the highest score; filter by confidence
    best = max(signals, key=lambda s: s["score"])

    if best["confidence"] < ACTIVE_MIN_CONFIDENCE:
        log.debug(
            "[%s] Best signal confidence %.1f%% below MIN_CONFIDENCE %.1f%% — discarded",
            symbol, best["confidence"], ACTIVE_MIN_CONFIDENCE,
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

    # Parallel ticker scanning — IO-bound (provider HTTP calls)
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


# ── Parallel scanner (no execution) ───────────────────────────────────────────

def _analyze_one_from_timeframes(symbol: str, timeframes: dict, min_confidence: float) -> dict[str, Any] | None:
    """Strategy stack (sync) given pre-fetched timeframes — MeanRev uses FAST profile by default."""
    candidates: list[
        tuple[
            str,
            float,
            str,
            str,
            float | None,
            float | None,
            float | None,
            bool,
            float | None,
            float | None,
            bool,
        ]
    ] = []
    raw_confs: list[float] = []

    try:
        action, conf, reason = analyze_multi_timeframe(timeframes, symbol=symbol)
        try:
            raw_confs.append(float(conf))
        except Exception:
            pass
        if action and conf >= float(min_confidence):
            candidates.append((action, float(conf), "RF", str(reason), None, None, None, False, None, None, False))
    except Exception:
        pass

    try:
        mr = analyze_meanrev(symbol, timeframes)
        try:
            raw_confs.append(float((mr or {}).get("confidence", 0)))
        except Exception:
            pass
        if mr and mr.get("rejected"):
            print(
                f"[{symbol}] REJECTED | strategy=MeanRev | Reason: "
                f"{str(mr.get('reason', 'Rejected by market structure filter'))}"
            )
            candidates.append((
                "__REJECTED__",
                -1.0,
                str(mr.get("strategy", "MeanRev")),
                str(mr.get("reason", "Rejected by market structure filter")),
                None,
                None,
                None,
                False,
                None,
                None,
                False,
            ))
        elif mr and float(mr.get("confidence", 0)) >= float(min_confidence):
            rsi_v = mr.get("rsi_15m")
            candidates.append((
                str(mr["action"]),
                float(mr["confidence"]),
                "MeanRev",
                str(mr.get("reason", "")),
                mr.get("stop_loss_pct"),
                (float(mr.get("ms_score")) if mr.get("ms_score") is not None else None),
                (float(mr.get("score")) if mr.get("score") is not None else None),
                bool(mr.get("mr_fast_bypass")),
                (float(rsi_v) if rsi_v is not None else None),
                None,
                False,
            ))
    except Exception:
        pass

    try:
        mo = analyze_momentum(symbol, timeframes)
        try:
            raw_confs.append(float((mo or {}).get("confidence", 0)))
        except Exception:
            pass
        if mo and mo.get("rejected"):
            print(
                f"[{symbol}] REJECTED | strategy=Momentum | Reason: "
                f"{str(mo.get('reason', 'Rejected by market structure filter'))}"
            )
            candidates.append((
                "__REJECTED__",
                -1.0,
                str(mo.get("strategy", "Momentum")),
                str(mo.get("reason", "Rejected by market structure filter")),
                None,
                None,
                None,
                False,
                None,
                None,
                False,
            ))
        elif mo and float(mo.get("confidence", 0)) >= float(min_confidence):
            mrsi = mo.get("mom_rsi_15m")
            mvr = mo.get("mom_vol_ratio")
            candidates.append((
                str(mo["action"]),
                float(mo["confidence"]),
                "Momentum",
                str(mo.get("reason", "")),
                mo.get("stop_loss_pct"),
                (float(mo.get("ms_score")) if mo.get("ms_score") is not None else None),
                (float(mo.get("score")) if mo.get("score") is not None else None),
                False,
                (float(mrsi) if mrsi is not None else None),
                (float(mvr) if mvr is not None else None),
                bool(mo.get("mom_low_vol_entry")),
            ))
    except Exception:
        pass

    if not candidates:
        best_conf = max(raw_confs) if raw_confs else 0.0
        print(f"[NO SIGNAL] {symbol} | best_conf={best_conf:.1f}")
        return None

    accepted = [c for c in candidates if c[0] != "__REJECTED__"]
    if not accepted:
        rejected_rows = [c for c in candidates if c[0] == "__REJECTED__"]
        labels = [str(r[2]) for r in rejected_rows] if rejected_rows else ["Unknown"]
        reasons = [f"{str(r[2])}: {str(r[3])}" for r in rejected_rows] if rejected_rows else ["Unknown rejection"]
        rej_label = "+".join(sorted(set(labels)))
        rej_reason = " || ".join(reasons)
        return {
            "symbol": symbol,
            "action": None,
            "confidence": 0.0,
            "strategy_label": rej_label,
            "reason": rej_reason,
            "stop_loss_pct": None,
            "ms_score": None,
            "score": None,
            "timeframes": timeframes,
            "rejected": True,
        }

    best_action, best_conf, best_label, best_reason, best_sl_pct, best_ms_score, best_score, best_mr_fast_bypass, best_rsi_aux, best_mom_vol, best_mom_low_vol = max(
        accepted, key=lambda x: x[1]
    )
    print(f"[CANDIDATES] {symbol} | count={len(candidates)} | best_conf={best_conf:.1f}")

    out: dict[str, Any] = {
        "symbol": symbol,
        "action": best_action,
        "confidence": best_conf,
        "strategy_label": best_label,
        "reason": best_reason,
        "stop_loss_pct": best_sl_pct,
        "ms_score": best_ms_score,
        "score": best_score,
        "timeframes": timeframes,
        "mr_fast_bypass": bool(best_mr_fast_bypass),
        "rsi_15m": float(best_rsi_aux) if best_label == "MeanRev" and best_rsi_aux is not None else None,
        "mom_rsi_15m": float(best_rsi_aux) if best_label == "Momentum" and best_rsi_aux is not None else None,
        "mom_vol_ratio": float(best_mom_vol) if best_label == "Momentum" and best_mom_vol is not None else None,
        "mom_low_vol_entry": bool(best_mom_low_vol) if best_label == "Momentum" else False,
    }
    return out


async def _analyze_one_async(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    symbol: str,
    min_confidence: float,
) -> dict[str, Any] | None:
    timeframes = await scan_multi_timeframe_async(symbol, session_context=None, session=session, semaphore=sem)
    if not timeframes:
        return None
    return _analyze_one_from_timeframes(symbol, timeframes, min_confidence)


async def scan_watchlist_parallel_async(
    watchlist: list[str],
    *,
    min_confidence: float = ACTIVE_MIN_CONFIDENCE,
    max_workers: int = 8,
) -> list[dict[str, Any]]:
    """
    Async batch scan: one aiohttp session, global semaphore (default 3 concurrent Capital requests).
    Processes symbols in groups of three for clean progress logs.
    """
    del max_workers  # retained for API compatibility; concurrency is HTTP-only

    wl = [str(s).strip().upper() for s in (watchlist or []) if str(s).strip()]
    if not wl:
        return []

    results: list[dict[str, Any]] = []
    sem = asyncio.Semaphore(CAPITAL_HTTP_CONCURRENCY)
    n_batches = (len(wl) + 2) // 3

    async with aiohttp.ClientSession(timeout=CAPITAL_CLIENT_TIMEOUT) as session:
        for bi in range(0, len(wl), 3):
            batch = wl[bi : bi + 3]
            bnum = bi // 3 + 1
            print(
                f"[SCAN BATCH] {bnum}/{n_batches} tickers={batch} "
                f"(Capital HTTP concurrency≤{CAPITAL_HTTP_CONCURRENCY})",
                flush=True,
            )
            tasks = [_analyze_one_async(session, sem, sym, float(min_confidence)) for sym in batch]
            rows = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, row in zip(batch, rows):
                if isinstance(row, Exception):
                    log.warning("[%s] analyze error: %s", sym, row)
                    continue
                if row:
                    results.append(row)
    return results


def scan_watchlist_parallel(
    watchlist: list[str],
    *,
    min_confidence: float = ACTIVE_MIN_CONFIDENCE,
    max_workers: int = 8,
) -> list[dict[str, Any]]:
    """
    High-throughput scanner for main.py.

    - Fetches multi-timeframe data via aiohttp (async) with bounded concurrency.
    - Runs RF + MeanRev + Momentum to build candidates.
    - Returns best-per-symbol signals (does NOT execute, does NOT apply tiers/risk).

    Output rows contain:
      symbol, action, confidence, strategy_label, reason, stop_loss_pct, timeframes
    """
    return asyncio.run(
        scan_watchlist_parallel_async(
            watchlist,
            min_confidence=min_confidence,
            max_workers=max_workers,
        )
    )
