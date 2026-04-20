"""
Success Memory System — NATB v2.0
Reinforcement Learning for Trading Patterns

Tracks successful trade setups and boosts confidence for matching patterns.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import logging

log = logging.getLogger(__name__)

# HARDCODED ABSOLUTE PATH (consistent with autonomous_training.py)
ABSOLUTE_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ABSOLUTE_PROJECT_ROOT, "data")
SUCCESS_SETUPS_PATH = os.path.join(DATA_DIR, "successful_setups.json")
HOT_SYMBOLS_PATH = os.path.join(DATA_DIR, "hot_symbols.json")

# Ensure directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# ── Constants ──────────────────────────────────────────────────────────────

SUCCESS_PATTERN_WINDOW_HOURS = 24  # Pattern match window
HOT_SYMBOL_WINDOW_HOURS = 4        # How long a symbol stays "hot"
CONSECUTIVE_WINS_THRESHOLD = 2     # Wins needed to mark as "hot"
CONFIDENCE_BOOST_PCT = 0.10        # +10% confidence boost for pattern matches


def _now_ts() -> float:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc).timestamp()


def _read_json(path: str, default: Any = None) -> Any:
    """Read JSON file safely."""
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        log.warning(f"[SuccessTracker] Failed to read {path}: {exc}")
        return default


def _write_json(path: str, data: Any) -> bool:
    """Write JSON file atomically."""
    try:
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, indent=2)
        os.replace(tmp, path)
        return True
    except Exception as exc:
        log.error(f"[SuccessTracker] Failed to write {path}: {exc}")
        return False


# ── Success Pattern Logging ────────────────────────────────────────────────

def log_successful_setup(
    symbol: str,
    direction: str,
    rsi: float,
    adx: float,
    ai_confidence: float,
    timeframe: str = "15m",
    strategy: str = "unknown",
    extra_features: dict | None = None,
) -> bool:
    """
    Log a successful trade setup when Take Profit is hit.
    
    Args:
        symbol: Ticker symbol (e.g., "AAPL")
        direction: "BUY" or "SELL"
        rsi: RSI value at entry
        adx: ADX value at entry  
        ai_confidence: AI model confidence (0-1)
        timeframe: Entry timeframe (default "15m")
        strategy: Strategy name ("Momentum", "MeanRev", etc.)
        extra_features: Additional features to store
    
    Returns:
        True if logged successfully
    """
    try:
        setups = _read_json(SUCCESS_SETUPS_PATH, default=[])
        
        # Keep only last 24 hours of patterns
        cutoff_ts = _now_ts() - (SUCCESS_PATTERN_WINDOW_HOURS * 3600)
        setups = [s for s in setups if s.get("timestamp", 0) > cutoff_ts]
        
        # Add new success
        success_record = {
            "timestamp": _now_ts(),
            "symbol": symbol.upper(),
            "direction": direction.upper(),
            "rsi": round(rsi, 2),
            "adx": round(adx, 2),
            "ai_confidence": round(ai_confidence, 4),
            "timeframe": timeframe,
            "strategy": strategy,
            "features": extra_features or {},
        }
        setups.append(success_record)
        
        success = _write_json(SUCCESS_SETUPS_PATH, setups)
        if success:
            log.info(f"[SuccessTracker] Logged winning setup for {symbol} {direction}: RSI={rsi:.1f}, ADX={adx:.1f}, Conf={ai_confidence:.1%}")
        return success
        
    except Exception as exc:
        log.error(f"[SuccessTracker] Error logging success: {exc}")
        return False


# ── Pattern Matching ─────────────────────────────────────────────────────

def find_pattern_match(
    symbol: str,
    direction: str,
    rsi: float,
    adx: float,
    ai_confidence: float,
    timeframe: str = "15m",
    strategy: str = "unknown",
    rsi_tolerance: float = 5.0,
    adx_tolerance: float = 3.0,
) -> dict | None:
    """
    Check if current signal matches a recent successful pattern.
    
    Args:
        symbol: Ticker symbol
        direction: "BUY" or "SELL"
        rsi: Current RSI value
        adx: Current ADX value
        ai_confidence: Current AI confidence
        timeframe: Signal timeframe
        strategy: Strategy name
        rsi_tolerance: +/- tolerance for RSI match
        adx_tolerance: +/- tolerance for ADX match
    
    Returns:
        Matching success record or None
    """
    try:
        setups = _read_json(SUCCESS_SETUPS_PATH, default=[])
        if not setups:
            return None
        
        # Only look at last 24 hours
        cutoff_ts = _now_ts() - (SUCCESS_PATTERN_WINDOW_HOURS * 3600)
        recent_setups = [s for s in setups if s.get("timestamp", 0) > cutoff_ts]
        
        for setup in recent_setups:
            # Must match symbol, direction, timeframe, strategy
            if setup.get("symbol") != symbol.upper():
                continue
            if setup.get("direction") != direction.upper():
                continue
            if setup.get("timeframe") != timeframe:
                continue
            if setup.get("strategy") != strategy:
                continue
            
            # Check feature similarity within tolerance
            rsi_match = abs(setup.get("rsi", 0) - rsi) <= rsi_tolerance
            adx_match = abs(setup.get("adx", 0) - adx) <= adx_tolerance
            
            if rsi_match and adx_match:
                return setup
        
        return None
        
    except Exception as exc:
        log.warning(f"[SuccessTracker] Pattern match error: {exc}")
        return None


def boost_confidence_for_pattern(
    base_confidence: float,
    symbol: str,
    direction: str,
    rsi: float,
    adx: float,
    ai_confidence: float,
    timeframe: str = "15m",
    strategy: str = "unknown",
) -> tuple[float, bool, dict | None]:
    """
    Boost confidence if signal matches a successful pattern.
    
    Args:
        base_confidence: Original confidence value
        ... (other signal features)
    
    Returns:
        Tuple of (adjusted_confidence, was_boosted, matching_pattern)
    """
    match = find_pattern_match(symbol, direction, rsi, adx, ai_confidence, timeframe, strategy)
    
    if match is None:
        return base_confidence, False, None
    
    # Apply +10% boost, capped at 95%
    boosted = min(0.95, base_confidence + CONFIDENCE_BOOST_PCT)
    
    log.info(
        f"[SUCCESS REINFORCEMENT] Pattern match found for {symbol}. "
        f"Boosting confidence {base_confidence:.1%} → {boosted:.1%} due to recent win."
    )
    
    return boosted, True, match


# ── Hot Symbol Tracking ───────────────────────────────────────────────────

def log_win_for_symbol(symbol: str) -> bool:
    """
    Log a winning trade for a symbol to track "hot" status.
    Called when TP is hit.
    """
    try:
        hot_data = _read_json(HOT_SYMBOLS_PATH, default={})
        symbol_upper = symbol.upper()
        
        now_ts = _now_ts()
        
        if symbol_upper not in hot_data:
            hot_data[symbol_upper] = {
                "consecutive_wins": 1,
                "last_win_ts": now_ts,
                "total_wins": 1,
                "is_hot": False,
            }
        else:
            record = hot_data[symbol_upper]
            # Check if win is within hot window (reset if too old)
            if now_ts - record.get("last_win_ts", 0) < HOT_SYMBOL_WINDOW_HOURS * 3600:
                record["consecutive_wins"] = record.get("consecutive_wins", 0) + 1
            else:
                # Reset consecutive count if too much time passed
                record["consecutive_wins"] = 1
            
            record["last_win_ts"] = now_ts
            record["total_wins"] = record.get("total_wins", 0) + 1
            
            # Mark as hot if threshold reached
            if record["consecutive_wins"] >= CONSECUTIVE_WINS_THRESHOLD:
                if not record.get("is_hot", False):
                    log.info(f"[HOT SYMBOL] {symbol} is now HOT after {record['consecutive_wins']} consecutive wins!")
                record["is_hot"] = True
            
            hot_data[symbol_upper] = record
        
        return _write_json(HOT_SYMBOLS_PATH, hot_data)
        
    except Exception as exc:
        log.error(f"[SuccessTracker] Error logging win: {exc}")
        return False


def is_hot_symbol(symbol: str) -> bool:
    """Check if symbol is currently marked as 'hot'."""
    try:
        hot_data = _read_json(HOT_SYMBOLS_PATH, default={})
        record = hot_data.get(symbol.upper())
        
        if not record:
            return False
        
        # Check if hot status expired
        now_ts = _now_ts()
        last_win = record.get("last_win_ts", 0)
        
        if now_ts - last_win > HOT_SYMBOL_WINDOW_HOURS * 3600:
            # Expired - reset hot status
            if record.get("is_hot", False):
                record["is_hot"] = False
                record["consecutive_wins"] = 0
                _write_json(HOT_SYMBOLS_PATH, hot_data)
            return False
        
        return record.get("is_hot", False)
        
    except Exception as exc:
        log.warning(f"[SuccessTracker] Error checking hot status: {exc}")
        return False


def get_hot_symbols() -> list[str]:
    """Get list of all currently hot symbols."""
    try:
        hot_data = _read_json(HOT_SYMBOLS_PATH, default={})
        now_ts = _now_ts()
        hot_symbols = []
        
        for symbol, record in hot_data.items():
            last_win = record.get("last_win_ts", 0)
            if record.get("is_hot", False) and (now_ts - last_win <= HOT_SYMBOL_WINDOW_HOURS * 3600):
                hot_symbols.append(symbol)
        
        return hot_symbols
        
    except Exception as exc:
        log.warning(f"[SuccessTracker] Error getting hot symbols: {exc}")
        return []


def reset_symbol_status(symbol: str) -> bool:
    """Reset hot/consecutive status for a symbol (e.g., after SL hit)."""
    try:
        hot_data = _read_json(HOT_SYMBOLS_PATH, default={})
        symbol_upper = symbol.upper()
        
        if symbol_upper in hot_data:
            hot_data[symbol_upper]["consecutive_wins"] = 0
            hot_data[symbol_upper]["is_hot"] = False
            return _write_json(HOT_SYMBOLS_PATH, hot_data)
        
        return True
        
    except Exception as exc:
        log.error(f"[SuccessTracker] Error resetting symbol: {exc}")
        return False


# ── Debug / Admin Functions ───────────────────────────────────────────────

def get_success_stats() -> dict:
    """Get statistics about successful patterns."""
    try:
        setups = _read_json(SUCCESS_SETUPS_PATH, default=[])
        hot_data = _read_json(HOT_SYMBOLS_PATH, default={})
        
        now_ts = _now_ts()
        cutoff_24h = now_ts - (24 * 3600)
        
        recent_setups = [s for s in setups if s.get("timestamp", 0) > cutoff_24h]
        
        # Group by symbol
        symbol_wins = {}
        for s in recent_setups:
            sym = s.get("symbol", "UNKNOWN")
            symbol_wins[sym] = symbol_wins.get(sym, 0) + 1
        
        # Get current hot symbols
        hot_list = get_hot_symbols()
        
        return {
            "total_success_patterns_24h": len(recent_setups),
            "unique_symbols_with_wins": len(symbol_wins),
            "hot_symbols_now": hot_list,
            "hot_symbols_count": len(hot_list),
            "symbol_win_counts": symbol_wins,
        }
        
    except Exception as exc:
        log.error(f"[SuccessTracker] Stats error: {exc}")
        return {}
