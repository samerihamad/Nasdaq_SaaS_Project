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
import pandas as pd

from utils.market_hours import utc_today, synchronized_utc_now, ET, is_nyse_trading_day
from config import (
    WATCHLIST,
    SIGNAL_MIN_CONFIDENCE,
    FAST_MIN_CONFIDENCE,
    GLOBAL_MIN_AI_CONFIDENCE,
    MAX_DAILY_TRADES,
    SCAN_INTERVAL_SEC,
    GLOBAL_MAX_OPEN_TRADES,
)
from core.strategy_meanrev  import analyze as analyze_meanrev
from core.strategy_momentum import analyze as analyze_momentum
from core.strategy_momentum import _adx, _rsi, _flatten as _mr_flatten
from core.risk_manager      import can_open_trade
from core.executor          import place_trade_for_user
from database.db_manager import DB_PATH
from utils.market_scanner import (
    scan_multi_timeframe,
    scan_multi_timeframe_async,
    chunked_parallel_gather,
    CAPITAL_CLIENT_TIMEOUT,
    CAPITAL_HTTP_CONCURRENCY,
    CHUNKED_SCAN_BATCH_SIZE,
    CHUNKED_SCAN_INTER_BATCH_SLEEP_SEC,
)
from utils.ai_model         import analyze_multi_timeframe
from utils.success_tracker import (
    boost_confidence_for_pattern,
    is_hot_symbol,
    get_hot_symbols,
)

# Phase 1-A: Decision Agent integration (Shadow Mode)
# The agent provides AI opinions but does NOT block trades
try:
    from core.decision_agent import DecisionAgent, analyze_signal_shadow, AgentOpinion
    _DECISION_AGENT_AVAILABLE = True
except Exception as _decision_agent_err:
    _DECISION_AGENT_AVAILABLE = False
    log.warning(f"[DecisionAgent] Import failed: {_decision_agent_err}. Agent will not be available.")

log = logging.getLogger(__name__)
ACTIVE_MIN_CONFIDENCE = float(SIGNAL_MIN_CONFIDENCE if SIGNAL_MIN_CONFIDENCE is not None else FAST_MIN_CONFIDENCE)

# Dynamic gate (15m ADX/RSI): bonus / choppy targets; neutral uses FAST_MIN_CONFIDENCE from .env.
_DYNAMIC_TREND_THRESHOLD = 52.0
_DYNAMIC_CHOPPY_THRESHOLD = 62.0


def _dynamic_confidence_threshold(timeframes: dict, _symbol: str) -> float:
    """
    Per-symbol minimum confidence: FAST_MIN_CONFIDENCE baseline, 52% if strong trend
    (bullish: ADX > 25 and RSI > 70; bearish: ADX > 25 and RSI < 30), 62% if choppy (ADX < 12).
    Applies to both Long (BUY) and Short (SELL) — threshold is on confidence %, not direction.
    """
    base = float(FAST_MIN_CONFIDENCE)
    # Global alignment: never require more than the global floor to *consider* a signal.
    base = min(base, float(GLOBAL_MIN_AI_CONFIDENCE))
    df_15m = timeframes.get("15m")
    if df_15m is None or getattr(df_15m, "empty", True):
        return base
    try:
        df = _mr_flatten(df_15m)
        # STRICT: Require at least 2 bars to use closed candle (iloc[-2])
        if len(df) < 30 or not all(c in df.columns for c in ("High", "Low", "Close")):
            return base
        adx_series, _, _ = _adx(df)
        # STRICT: Use last CLOSED candle (iloc[-2]), never live forming candle
        adx_val = float(adx_series.iloc[-2])
        close = df["Close"].squeeze().astype(float)
        rsi_val = float(_rsi(close).iloc[-2])
    except Exception:
        return base

    if adx_val < 12:
        thr = _DYNAMIC_CHOPPY_THRESHOLD
        log.info(
            "[DYNAMIC] Adjusted threshold to %.1f%% due to Choppy conditions.",
            thr,
        )
        return min(thr, float(GLOBAL_MIN_AI_CONFIDENCE))
    if adx_val > 25 and rsi_val > 70:
        thr = _DYNAMIC_TREND_THRESHOLD
        log.info(
            "[DYNAMIC] Adjusted threshold to %.1f%% due to Trend (bullish) conditions.",
            thr,
        )
        return min(thr, float(GLOBAL_MIN_AI_CONFIDENCE))
    if adx_val > 25 and rsi_val < 30:
        thr = _DYNAMIC_TREND_THRESHOLD
        log.info(
            "[DYNAMIC] Adjusted threshold to %.1f%% due to Trend (bearish) conditions.",
            thr,
        )
        return min(thr, float(GLOBAL_MIN_AI_CONFIDENCE))
    return min(base, float(GLOBAL_MIN_AI_CONFIDENCE))


# ── QUANT OPTIMIZATION: Dynamic Volatility Adaptation ─────────────────────────

def _calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    Calculate Average True Range (ATR) for volatility measurement.
    Returns ATR value as float. Higher ATR = higher volatility.
    """
    try:
        high  = df["High"].squeeze().astype(float)
        low   = df["Low"].squeeze().astype(float)
        close = df["Close"].squeeze().astype(float)

        prev_close = close.shift(1)

        # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean().iloc[-1]
        return float(atr) if not pd.isna(atr) else 0.0
    except Exception as exc:
        log.warning(f"[ATR] Calculation failed: {exc}. Defaulting to ATR=0 (conservative)")
        return 0.0


def _get_volatility_adjusted_thresholds(
    df_15m: pd.DataFrame,
    base_rsi_buy: float = 30.0,
    base_rsi_sell: float = 70.0,
    base_confidence: float = 0.65,
) -> dict:
    """
    QUANT OPTIMIZATION: Hedge Fund Approach - Dynamic thresholds based on ATR.
    
    For high-volatility stocks (e.g., TSLA): Wider RSI bands, higher confidence threshold
    For low-volatility stocks (e.g., AAPL): Standard settings
    
    Returns adjusted thresholds dictionary.
    """
    try:
        # Calculate ATR as % of price (normalized volatility)
        current_price = float(df_15m["Close"].iloc[-1])
        atr_value = _calculate_atr(df_15m, period=14)
        atr_pct = (atr_value / current_price * 100) if current_price > 0 else 0.0

        # Categorize volatility (typical ranges for 15m timeframe)
        # Low: < 0.3%, Normal: 0.3-0.8%, High: > 0.8%
        if atr_pct < 0.3:
            volatility_tier = "low"
            rsi_adjustment = 0.0
            confidence_adjustment = 0.0
        elif atr_pct < 0.8:
            volatility_tier = "normal"
            rsi_adjustment = 2.5
            confidence_adjustment = 0.02
        else:
            volatility_tier = "high"
            rsi_adjustment = 5.0
            confidence_adjustment = 0.05

        # Adjust RSI thresholds (wider bands for high volatility)
        # For Mean Rev: Lower buy threshold, higher sell threshold
        adjusted_rsi_buy = max(20.0, base_rsi_buy - rsi_adjustment)
        adjusted_rsi_sell = min(80.0, base_rsi_sell + rsi_adjustment)

        # Adjust confidence threshold (higher bar for high volatility)
        adjusted_confidence = min(0.85, base_confidence + confidence_adjustment)

        log.info(
            f"[DYNAMIC] Volatility: {volatility_tier.upper()} (ATR={atr_pct:.2f}%). "
            f"RSI: {adjusted_rsi_buy:.0f}/{adjusted_rsi_sell:.0f}, "
            f"Conf: {adjusted_confidence:.0%}"
        )

        return {
            "volatility_tier": volatility_tier,
            "atr_pct": atr_pct,
            "rsi_oversold": adjusted_rsi_buy,
            "rsi_overbought": adjusted_rsi_sell,
            "min_confidence": adjusted_confidence,
        }
    except Exception as exc:
        log.warning(f"[DYNAMIC] Volatility adaptation failed: {exc}. Using conservative defaults.")
        # FALLBACK: Conservative defaults
        return {
            "volatility_tier": "unknown",
            "atr_pct": 0.0,
            "rsi_oversold": base_rsi_buy,
            "rsi_overbought": base_rsi_sell,
            "min_confidence": base_confidence + 0.05,  # More conservative
        }


def _side_label(action: str | None) -> str:
    if not action:
        return "Neutral"
    a = str(action).upper().strip()
    if a == "BUY":
        return "Long"
    if a == "SELL":
        return "Short"
    return a


def _latest_signal_price(timeframes: dict) -> float | None:
    """Use latest 15m close as signal reference price for execution slippage guard."""
    try:
        df_15m = (timeframes or {}).get("15m")
        if df_15m is None or getattr(df_15m, "empty", True):
            return None
        px = float(df_15m["Close"].iloc[-1])
        return px if px > 0 else None
    except Exception:
        return None


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

    eff_min_conf = _dynamic_confidence_threshold(timeframes, symbol)

    signals = []
    structural_rejections = []

    # Mean Reversion
    try:
        sig = analyze_meanrev(symbol, timeframes, min_confidence_floor=eff_min_conf)
        if sig and sig.get("rejected"):
            structural_rejections.append({
                "symbol": symbol,
                "strategy": sig.get("strategy", "MeanRev"),
                "reason": sig.get("reason", "Rejected by market structure filter"),
                "rejected": True,
                "action": sig.get("action"),
            })
        elif sig:
            signals.append(sig)
    except Exception as exc:
        log.error("[%s] MeanRev error: %s", symbol, exc)

    # Momentum
    try:
        sig = analyze_momentum(symbol, timeframes, min_confidence_floor=eff_min_conf)
        if sig and sig.get("rejected"):
            _sl = _side_label(sig.get("action"))
            log.info(
                "[%s] REJECTED | Side: %s | strategy=Momentum | Reason: %s",
                symbol,
                _sl,
                str(sig.get("reason", "Rejected by market structure filter")),
            )
            structural_rejections.append({
                "symbol": symbol,
                "strategy": sig.get("strategy", "Momentum"),
                "reason": sig.get("reason", "Rejected by market structure filter"),
                "rejected": True,
                "action": sig.get("action"),
            })
        elif sig:
            signals.append(sig)
    except Exception as exc:
        log.error("[%s] Momentum error: %s", symbol, exc)

    if not signals:
        if structural_rejections:
            for rej in structural_rejections:
                _sl = _side_label(rej.get("action"))
                log.info(
                    "[%s] REJECTED | Side: %s | strategy=%s | Reason: %s",
                    symbol, _sl, rej.get("strategy", "Unknown"), rej.get("reason", "Rejected"),
                )
            # Keep structural rejections as internal logs only in this path.
            return None
        return None

    # Pick the signal with the highest score; filter by confidence
    best = max(signals, key=lambda s: s["score"])

    if best["confidence"] < eff_min_conf:
        log.debug(
            "[%s] REJECTED | Side: %s | Reason: Below dynamic threshold "
            "(conf %.1f%% < %.1f%%) | strategy=%s",
            symbol,
            _side_label(best.get("action")),
            best["confidence"],
            eff_min_conf,
            best.get("strategy", "?"),
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

        # ── FIX: Cancel oldest pending limit order if at max trades and high-confidence ──
        try:
            from core.risk_manager import _get_global_open_trades_count
            current_open = _get_global_open_trades_count()
            
            # If we're at capacity and this is a high-confidence signal, make room
            if current_open >= GLOBAL_MAX_OPEN_TRADES and confidence >= 0.75:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                
                # Find oldest pending limit order (by created_at timestamp)
                c.execute(
                    "SELECT id, symbol, created_at FROM pending_limit_orders "
                    "WHERE status='PENDING' ORDER BY created_at ASC LIMIT 1"
                )
                oldest = c.fetchone()
                
                if oldest:
                    order_id, order_symbol, created_at = oldest
                    log.info(
                        f"[CAPACITY PRUNING] At max trades ({current_open}/{GLOBAL_MAX_OPEN_TRADES}). "
                        f"Cancelling oldest pending order #{order_id} for {order_symbol} (created {created_at}) "
                        f"to make room for high-confidence {symbol} signal ({confidence:.1%})"
                    )
                    c.execute(
                        "UPDATE pending_limit_orders SET status='CANCELLED', reason='capacity_high_conf_priority' "
                        "WHERE id=?",
                        (order_id,),
                    )
                    conn.commit()
                    log.info(f"[CAPACITY PRUNING] Successfully cancelled order #{order_id}")
                
                conn.close()
        except Exception as exc:
            log.warning(f"[CAPACITY PRUNING] Error checking/cancelling pending orders: {exc}")

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
                signal_price=signal.get("signal_price"),
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
    if not is_nyse_trading_day(synchronized_utc_now().astimezone(ET)):
        log.info("[SCAN SAFETY] NYSE closed day — run_scan skipped.")
        return []
    
    # ── HOT SYMBOL PRIORITIZATION ───────────────────────────────────────────
    # Get hot symbols and prioritize them in the scan order
    hot_symbols = get_hot_symbols()
    if hot_symbols:
        log.info(f"[HOT SYMBOLS] Prioritizing hot symbols: {', '.join(hot_symbols)}")
    
    # Reorder WATCHLIST: hot symbols first, then others
    prioritized_watchlist = list(dict.fromkeys(hot_symbols + [sym for sym in WATCHLIST if sym not in hot_symbols]))
    
    log.info("=== Market scan started — %d tickers ===", len(prioritized_watchlist))

    subscribers = _get_active_subscribers()
    if not subscribers:
        log.warning("No active subscribers — scan aborted")
        return []

    triggered = []

    # Parallel ticker scanning — IO-bound (provider HTTP calls)
    with ThreadPoolExecutor(max_workers=min(8, len(prioritized_watchlist))) as pool:
        futures = {pool.submit(_analyze_ticker, sym): sym for sym in prioritized_watchlist}
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

def _analyze_one_from_timeframes(
    symbol: str, timeframes: dict, _min_confidence: float
) -> dict[str, Any] | None:
    """Strategy stack (sync) given pre-fetched timeframes — MeanRev uses FAST profile by default."""
    
    # --- Avoidance Logic Hook ---
    try:
        from database.behavioral_db import is_toxic_habit
        from utils.ai_model import regime_type as _ai_regime_type, _adx, _flatten
        from datetime import datetime
        
        df_15m = timeframes.get("15m")
        if df_15m is not None and not df_15m.empty:
            regime = _ai_regime_type(df_15m)
            adx_val = float(_adx(_flatten(df_15m)).iloc[-1])
            adx_band = "LOW" if adx_val < 25 else "HIGH"
            
            # Simple extraction for 'time_of_day' from local time (as a proxy for scan time)
            from utils.market_hours import synchronized_utc_now
            time_of_day = synchronized_utc_now().strftime("%H:00 UTC")
            
            # Symbol Signature Check: NVDA hard rule
            if symbol == 'NVDA' and regime == 'VOLATILE' and adx_band == 'LOW':
                print(f"[BEHAVIORAL BLOCK] Avoided entry for NVDA due to Signature Check (VOLATILE, ADX < 25).")
                return None
                
            if is_toxic_habit(symbol, regime, adx_band, time_of_day):
                print(f"[BEHAVIORAL BLOCK] Avoided entry for {symbol} due to historical toxic habit in {regime} at {time_of_day}.")
                return None
    except Exception as e:
        print(f"[Behavioral Memory] Error checking toxic habit: {e}")
        pass
    
    # Confidence gate uses FAST_MIN_CONFIDENCE-based dynamic threshold (see _dynamic_confidence_threshold).
    eff_min_conf = _dynamic_confidence_threshold(timeframes, symbol)
    
    # ── QUANT OPTIMIZATION: Apply dynamic volatility adaptation ─────────────────
    # Adjust effective confidence based on ATR-derived volatility tier
    df_15m = timeframes.get("15m")
    if df_15m is not None and not df_15m.empty:
        vol_adjustments = _get_volatility_adjusted_thresholds(
            df_15m,
            base_rsi_buy=30.0,
            base_rsi_sell=70.0,
            base_confidence=eff_min_conf,
        )
        # Use the higher of the two confidence thresholds
        eff_min_conf = max(eff_min_conf, vol_adjustments["min_confidence"])
        log.info(f"[SignalEngine {symbol}] Final confidence threshold: {eff_min_conf:.0%}")
    
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
        if action and conf >= float(eff_min_conf):
            candidates.append((action, float(conf), "RF", str(reason), None, None, None, False, None, None, False, False))
        elif action and conf < float(eff_min_conf):
            print(
                f"[{symbol}] REJECTED | Side: {_side_label(action)} | strategy=RF | "
                f"Reason: Below dynamic threshold (conf {float(conf):.1f}% < {eff_min_conf:.1f}%)"
            )
    except Exception:
        pass

    try:
        mr = analyze_meanrev(symbol, timeframes, min_confidence_floor=eff_min_conf)
        try:
            raw_confs.append(float((mr or {}).get("confidence", 0)))
        except Exception:
            pass
        if mr and mr.get("rejected"):
            print(
                f"[{symbol}] REJECTED | Side: {_side_label(mr.get('action'))} | strategy=MeanRev | Reason: "
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
                False,
            ))
        elif mr and not mr.get("rejected") and float(mr.get("confidence", 0)) < float(eff_min_conf):
            print(
                f"[{symbol}] REJECTED | Side: {_side_label(mr.get('action'))} | strategy=MeanRev | "
                f"Reason: Below dynamic threshold (conf {float(mr.get('confidence', 0)):.1f}% < {eff_min_conf:.1f}%)"
            )
        elif mr and float(mr.get("confidence", 0)) >= float(eff_min_conf):
            rsi_v = mr.get("rsi_15m")
            candidates.append((
                str(mr["action"]),
                float(mr["confidence"]),
                "MeanRev",
                str(mr.get("reason", "")),
                mr.get("stop_loss_pct"),
                (float(mr.get("ms_score")) if mr.get("ms_score") is not None else None),
                (float(mr.get("score")) if mr.get("score") is not None else None),
                (float(rsi_v) if rsi_v is not None else None),
                None,  # mom_vol not used for MeanRev
                False,  # mom_low_vol_entry not used for MeanRev
            ))
    except Exception:
        pass

    try:
        mo = analyze_momentum(symbol, timeframes, min_confidence_floor=eff_min_conf)
        try:
            raw_confs.append(float((mo or {}).get("confidence", 0)))
        except Exception:
            pass
        if mo and mo.get("rejected"):
            print(
                f"[{symbol}] REJECTED | Side: {_side_label(mo.get('action'))} | strategy=Momentum | Reason: "
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
                None,  # rsi_aux placeholder
                None,  # mom_vol placeholder
                False,  # mom_low_vol_entry placeholder
            ))
        elif mo and not mo.get("rejected") and float(mo.get("confidence", 0)) < float(eff_min_conf):
            print(
                f"[{symbol}] REJECTED | Side: {_side_label(mo.get('action'))} | strategy=Momentum | "
                f"Reason: Below dynamic threshold (conf {float(mo.get('confidence', 0)):.1f}% < {eff_min_conf:.1f}%)"
            )
        elif mo and float(mo.get("confidence", 0)) >= float(eff_min_conf):
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

    best_action, best_conf, best_label, best_reason, best_sl_pct, best_ms_score, best_score, best_rsi_aux, best_mom_vol, best_mom_low_vol = max(
        accepted, key=lambda x: x[1]
    )
    print(f"[CANDIDATES] {symbol} | count={len(candidates)} | best_conf={best_conf:.1f}")

    # ── SUCCESS REINFORCEMENT: Pattern Matching ──────────────────────────────
    # Boost confidence if this signal matches a recent successful setup
    original_conf = best_conf
    try:
        from utils.success_tracker import boost_confidence_for_pattern
        
        # Get current ADX and RSI for pattern matching
        df_15m = timeframes.get("15m")
        adx_val = 0.0
        rsi_val = 0.0
        if df_15m is not None and not df_15m.empty:
            from utils.ai_model import _adx, _rsi, _flatten
            flat_df = _flatten(df_15m)
            adx_series = _adx(flat_df)
            rsi_series = _rsi(flat_df["Close"].squeeze())
            if len(adx_series) >= 2:
                adx_val = float(adx_series.iloc[-2])
            if len(rsi_series) >= 2:
                rsi_val = float(rsi_series.iloc[-2])
        
        boosted_conf, was_boosted, matching_pattern = boost_confidence_for_pattern(
            base_confidence=best_conf,
            symbol=symbol,
            direction=best_action,
            rsi=rsi_val,
            adx=adx_val,
            ai_confidence=best_conf,
            timeframe="15m",
            strategy=best_label,
        )
        
        if was_boosted and matching_pattern:
            best_conf = boosted_conf
            best_reason = f"{best_reason} | REINFORCED (+10% from win pattern)"
            log.info(
                f"[SUCCESS REINFORCEMENT] Pattern match found for {symbol}. "
                f"Boosting confidence {original_conf:.1%} → {best_conf:.1%} due to recent win."
            )
            
    except Exception as exc:
        log.warning(f"[SUCCESS REINFORCEMENT] Pattern boost failed for {symbol}: {exc}")
        pass

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
        # REMOVED: "mr_fast_bypass" — eliminated per strict confirmation policy
        "rsi_15m": float(best_rsi_aux) if best_label == "MeanRev" and best_rsi_aux is not None else None,
        "mom_rsi_15m": float(best_rsi_aux) if best_label == "Momentum" and best_rsi_aux is not None else None,
        "mom_vol_ratio": float(best_mom_vol) if best_label == "Momentum" and best_mom_vol is not None else None,
        "mom_low_vol_entry": bool(best_mom_low_vol) if best_label == "Momentum" else False,
        # REMOVED: "mom_macd_bypassed" — MACD bypass eliminated per strict policy
        "signal_price": _latest_signal_price(timeframes),
    }

    # Phase 1-A: Decision Agent Shadow Mode Analysis
    # The agent analyzes the signal but does NOT block execution.
    # Its opinion is logged and will be included in Telegram notifications.
    if _DECISION_AGENT_AVAILABLE:
        try:
            signal_data_for_agent = {
                "symbol": symbol,
                "action": best_action,
                "confidence": best_conf,
                "strategy_label": best_label,
                "reason": best_reason,
            }
            
            # Get agent opinion in shadow mode (never blocks)
            agent_opinion = analyze_signal_shadow(
                signal_data=signal_data_for_agent,
                market_data=timeframes,
                chat_id=None,  # Will be sent later with the main signal notification
            )
            
            # Add agent opinion to signal dict for downstream use
            out["agent_opinion"] = agent_opinion.to_dict()
            
            log.info(
                f"[SignalEngine {symbol}] DecisionAgent Shadow Opinion: "
                f"{agent_opinion.verdict} (AI: {agent_opinion.ai_confidence:.1f}%, "
                f"Tech: {agent_opinion.technical_confidence:.1f}%)"
            )
            
        except Exception as agent_exc:
            # CRITICAL: Agent failure must never block trading
            log.warning(
                f"[SignalEngine {symbol}] DecisionAgent analysis failed: {agent_exc}. "
                f"Trade will proceed on technical signal only."
            )
            out["agent_opinion"] = {
                "error": str(agent_exc),
                "verdict": "ERROR",
                "shadow_mode": True,
            }
    else:
        # DecisionAgent not available - mark in signal for transparency
        out["agent_opinion"] = {
            "verdict": "UNAVAILABLE",
            "reasoning": "DecisionAgent module not loaded",
            "shadow_mode": True,
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
    Async batch scan: one aiohttp session, bounded Capital HTTP concurrency.
    Symbols run in chunks of CHUNKED_SCAN_BATCH_SIZE (default 2) via chunked_parallel_gather
    in utils.market_scanner — gather only per chunk, micro-pause between chunks.
    """
    del max_workers  # retained for API compatibility; concurrency is HTTP-only
    if not is_nyse_trading_day(synchronized_utc_now().astimezone(ET)):
        log.info("[SCAN SAFETY] NYSE closed day — async watchlist scan skipped.")
        return []

    wl = [str(s).strip().upper() for s in (watchlist or []) if str(s).strip()]
    if not wl:
        return []
    log.info(
        "[SCAN SAFETY] HTTP concurrency=%d | chunk_size=%d",
        int(CAPITAL_HTTP_CONCURRENCY),
        int(CHUNKED_SCAN_BATCH_SIZE),
    )

    results: list[dict[str, Any]] = []
    sem = asyncio.Semaphore(CAPITAL_HTTP_CONCURRENCY)

    async with aiohttp.ClientSession(timeout=CAPITAL_CLIENT_TIMEOUT) as session:

        def _task_factory(sym: str):
            async def _run():
                return await _analyze_one_async(session, sem, sym, float(min_confidence))

            return _run

        factories = [_task_factory(s) for s in wl]
        rows = await chunked_parallel_gather(
            factories,
            chunk_labels=wl,
            chunk_size=CHUNKED_SCAN_BATCH_SIZE,
            inter_chunk_sleep_sec=CHUNKED_SCAN_INTER_BATCH_SLEEP_SEC,
            return_exceptions=True,
        )
        for sym, row in zip(wl, rows):
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
    if not is_nyse_trading_day(synchronized_utc_now().astimezone(ET)):
        log.info("[SCAN SAFETY] NYSE closed day — watchlist scan skipped.")
        return []
    return asyncio.run(
        scan_watchlist_parallel_async(
            watchlist,
            min_confidence=min_confidence,
            max_workers=max_workers,
        )
    )
