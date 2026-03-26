"""
Mean Reversion Strategy — NATB v2.0

Entry logic (multi-timeframe):
  1D  — master trend: NOT in an extreme RSI condition (40–65 range); confirms
         the market is range-bound or mean-reverting at a macro level.
  4H  — zone confirmation: price is trading near a key VWAP deviation zone.
  15M — trigger: RSI oversold/overbought + Reversal Candle + Liquidity Sweep.

Filters that REJECT a signal:
  • News Trap  — entry bar gapped > MR_NEWS_TRAP_GAP_PCT (news-driven move,
                 reversal is unreliable).
  • No Candle  — neither a Hammer nor a Bullish/Bearish Engulfing on the 15m
                 trigger bar.

Signal score breakdown (max 100):
  RSI extreme (15m)       → 25 pts
  VWAP deviation (15m)    → 20 pts
  Reversal candle (15m)   → 20 pts
  Liquidity sweep (15m)   → 15 pts
  4H zone alignment       → 10 pts
  1D trend alignment      → 10 pts
"""

import logging
import numpy as np
import pandas as pd

from config import (
    MR_RSI_OVERSOLD,
    MR_RSI_OVERBOUGHT,
    MR_VWAP_DEV_PCT,
    MR_NEWS_TRAP_GAP_PCT,
    MR_SWEEP_LOOKBACK,
    MR_MIN_SCORE,
    ENABLE_MARKET_STRUCTURE_FILTERS,
    ENABLE_PREMIUM_DISCOUNT_FILTER,
    ENABLE_LIQUIDITY_MAP_FILTER,
    MARKET_STRUCTURE_NO_TRADE_ZONE_PCT,
    MARKET_STRUCTURE_HTF_LOOKBACK,
    LIQUIDITY_OPENING_RANGE_BARS,
)
from core.market_structure import (
    compute_htf_range,
    allow_direction_by_pd,
    in_no_trade_zone,
    build_liquidity_map,
    liquidity_sweep_score,
)

log = logging.getLogger(__name__)


# ── Indicator helpers ─────────────────────────────────────────────────────────

def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _vwap(df: pd.DataFrame) -> pd.Series:
    """Session-reset VWAP for intraday; rolling-20 for daily."""
    typical = (df["High"] + df["Low"] + df["Close"]).squeeze() / 3
    volume  = df["Volume"].squeeze()

    is_intraday = len(df) > 1 and (df.index[1] - df.index[0]).total_seconds() < 86400

    if is_intraday:
        dates  = pd.Series([ts.date() for ts in df.index], index=df.index)
        result = pd.Series(np.nan, index=df.index, dtype=float)
        for d in dates.unique():
            mask = dates == d
            result[mask] = (typical[mask] * volume[mask]).cumsum() / volume[mask].cumsum()
        return result
    return (typical * volume).rolling(20).sum() / volume.rolling(20).sum()


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


# ── Candle pattern detectors ──────────────────────────────────────────────────

def _is_hammer(df: pd.DataFrame, idx: int) -> bool:
    """
    Hammer (bullish reversal):
      - Lower wick ≥ 2× body
      - Upper wick ≤ body
      - Close > Open (green body preferred, but doji hammers are also valid)
    """
    row   = df.iloc[idx]
    body  = abs(float(row["Close"]) - float(row["Open"]))
    upper = float(row["High"]) - max(float(row["Close"]), float(row["Open"]))
    lower = min(float(row["Close"]), float(row["Open"])) - float(row["Low"])

    if body == 0:
        body = 0.0001  # doji guard

    return lower >= 2 * body and upper <= body


def _is_bullish_engulfing(df: pd.DataFrame, idx: int) -> bool:
    """Current bar fully engulfs the previous bearish bar."""
    if idx < 1:
        return False
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]
    prev_bearish = float(prev["Close"]) < float(prev["Open"])
    curr_bullish = float(curr["Close"]) > float(curr["Open"])
    engulfs = (
        float(curr["Open"])  <= float(prev["Close"]) and
        float(curr["Close"]) >= float(prev["Open"])
    )
    return prev_bearish and curr_bullish and engulfs


def _is_bearish_engulfing(df: pd.DataFrame, idx: int) -> bool:
    """Current bar fully engulfs the previous bullish bar."""
    if idx < 1:
        return False
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1]
    prev_bullish = float(prev["Close"]) > float(prev["Open"])
    curr_bearish = float(curr["Close"]) < float(curr["Open"])
    engulfs = (
        float(curr["Open"])  >= float(prev["Close"]) and
        float(curr["Close"]) <= float(prev["Open"])
    )
    return prev_bullish and curr_bearish and engulfs


def _is_shooting_star(df: pd.DataFrame, idx: int) -> bool:
    """
    Shooting Star (bearish reversal — mirror of hammer):
      - Upper wick ≥ 2× body
      - Lower wick ≤ body
    """
    row   = df.iloc[idx]
    body  = abs(float(row["Close"]) - float(row["Open"]))
    upper = float(row["High"]) - max(float(row["Close"]), float(row["Open"]))
    lower = min(float(row["Close"]), float(row["Open"])) - float(row["Low"])

    if body == 0:
        body = 0.0001

    return upper >= 2 * body and lower <= body


# ── Core analysis ─────────────────────────────────────────────────────────────

def _news_trap(df_15m: pd.DataFrame) -> bool:
    """
    True if the most recent bar opened with a gap > MR_NEWS_TRAP_GAP_PCT.
    A large gap indicates a news-driven move where reversals are unreliable.
    """
    if len(df_15m) < 2:
        return False
    prev_close = float(df_15m["Close"].iloc[-2])
    curr_open  = float(df_15m["Open"].iloc[-1])
    if prev_close == 0:
        return False
    gap_pct = abs(curr_open - prev_close) / prev_close * 100
    return gap_pct >= MR_NEWS_TRAP_GAP_PCT


def _liquidity_sweep(df_15m: pd.DataFrame, direction: str) -> bool:
    """
    Detects a Liquidity Sweep:
      BUY : Price briefly broke below a recent swing low then closed ABOVE it
            → trapped shorts, likely reversal up.
      SELL: Price briefly broke above a recent swing high then closed BELOW it
            → trapped longs, likely reversal down.

    We look at the last MR_SWEEP_LOOKBACK bars (excluding the trigger bar).
    """
    lookback = MR_SWEEP_LOOKBACK
    if len(df_15m) < lookback + 2:
        return False

    reference = df_15m.iloc[-(lookback + 1):-1]  # bars before trigger
    trigger   = df_15m.iloc[-1]

    if direction == "BUY":
        swing_low = float(reference["Low"].min())
        # Trigger bar's low dipped below swing_low (sweep) but closed above it
        swept  = float(trigger["Low"])   < swing_low
        closed = float(trigger["Close"]) > swing_low
        return swept and closed

    else:  # SELL
        swing_high = float(reference["High"].max())
        swept  = float(trigger["High"])  > swing_high
        closed = float(trigger["Close"]) < swing_high
        return swept and closed


def _check_1d_alignment(df_1d: pd.DataFrame, direction: str) -> bool:
    """
    1D alignment check — daily RSI must not be sharply contrary to our direction.
    BUY : daily RSI <= 68 (allows oversold and mid-range; rejects only overbought)
    SELL: daily RSI >= 32 (allows overbought and mid-range; rejects only oversold)
    """
    if df_1d is None or df_1d.empty:
        return False
    df_1d = _flatten(df_1d)
    rsi_1d = _rsi(df_1d["Close"].squeeze())
    val    = float(rsi_1d.iloc[-1])

    if direction == "BUY":
        # Allow oversold (RSI < 35) — those are prime mean-reversion candidates
        return val <= 68
    else:
        # Allow overbought (RSI > 65) — those are prime short candidates
        return val >= 32


def _check_4h_zone(df_4h: pd.DataFrame, direction: str) -> bool:
    """
    4H: Price must be meaningfully below (BUY) or above (SELL) the 4H VWAP,
    confirming it is in a zone where a reversal is credible.
    """
    if df_4h is None or df_4h.empty:
        return False
    df_4h  = _flatten(df_4h)
    vwap   = _vwap(df_4h)
    close  = float(df_4h["Close"].squeeze().iloc[-1])
    vwap_v = float(vwap.iloc[-1])
    if vwap_v == 0:
        return False
    dev_pct = (close - vwap_v) / vwap_v * 100

    if direction == "BUY":
        return dev_pct <= -MR_VWAP_DEV_PCT   # price below VWAP
    else:
        return dev_pct >= MR_VWAP_DEV_PCT    # price above VWAP


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(symbol: str, timeframes: dict) -> dict | None:
    """
    Run the full Mean Reversion analysis.

    Parameters
    ----------
    symbol     : ticker string (for logging)
    timeframes : dict with keys '1d', '4h', '15m' — each a DataFrame

    Returns
    -------
    dict  with keys: action, confidence, score, strategy, stop_loss_pct, reason
    None  if no valid signal found
    """
    df_1d  = _flatten(timeframes.get("1d",  pd.DataFrame()))
    df_4h  = _flatten(timeframes.get("4h",  pd.DataFrame()))
    df_15m = _flatten(timeframes.get("15m", pd.DataFrame()))

    if df_15m.empty or len(df_15m) < 20:
        log.debug("[MeanRev %s] Insufficient 15m data", symbol)
        return None

    close_15m = df_15m["Close"].squeeze()
    rsi_15m   = _rsi(close_15m)
    vwap_15m  = _vwap(df_15m)

    rsi_val   = float(rsi_15m.iloc[-1])
    close_val = float(close_15m.iloc[-1])
    vwap_val  = float(vwap_15m.iloc[-1])

    # ── Determine candidate direction from RSI ────────────────────────────────
    if rsi_val <= MR_RSI_OVERSOLD:
        direction = "BUY"
    elif rsi_val >= MR_RSI_OVERBOUGHT:
        direction = "SELL"
    else:
        log.debug("[MeanRev %s] RSI %.1f — no extreme; skip", symbol, rsi_val)
        return None

    # ── Sprint 1: Market structure gates (feature-flagged) ──────────────────
    htf = None
    liq = None
    if ENABLE_MARKET_STRUCTURE_FILTERS:
        htf = compute_htf_range(df_4h, close_val, lookback=MARKET_STRUCTURE_HTF_LOOKBACK)
        if htf and in_no_trade_zone(close_val, htf, width_pct=MARKET_STRUCTURE_NO_TRADE_ZONE_PCT):
            rej = f"Rejected: No-Trade Zone | zone={htf.zone} eq={htf.eq:.2f} close={close_val:.2f}"
            log.info("[MeanRev %s] %s", symbol, rej)
            return {"rejected": True, "strategy": "MeanRev", "reason": rej}

        if ENABLE_PREMIUM_DISCOUNT_FILTER and not allow_direction_by_pd(direction, htf):
            zone = htf.zone if htf else "unknown"
            rej = f"Rejected: Price in {zone.title()} Zone for {direction}"
            log.info("[MeanRev %s] %s", symbol, rej)
            return {"rejected": True, "strategy": "MeanRev", "reason": rej}

        if ENABLE_LIQUIDITY_MAP_FILTER:
            liq = build_liquidity_map(df_1d, df_15m, opening_bars=LIQUIDITY_OPENING_RANGE_BARS)

    # ── News Trap filter ──────────────────────────────────────────────────────
    if _news_trap(df_15m):
        log.info("[MeanRev %s] News Trap detected — signal skipped", symbol)
        return None

    # ── Reversal candle check (preferred but not mandatory) ──────────────────
    # A reversal candle OR a liquidity sweep is sufficient to proceed.
    # If neither is present the signal is rejected (low-quality setup).
    last_idx = len(df_15m) - 1
    if direction == "BUY":
        has_reversal_candle = (
            _is_hammer(df_15m, last_idx) or
            _is_bullish_engulfing(df_15m, last_idx)
        )
        candle_label = "Hammer / Bullish Engulfing"
    else:
        has_reversal_candle = (
            _is_shooting_star(df_15m, last_idx) or
            _is_bearish_engulfing(df_15m, last_idx)
        )
        candle_label = "Shooting Star / Bearish Engulfing"

    has_sweep = _liquidity_sweep(df_15m, direction)

    if not has_reversal_candle and not has_sweep:
        log.debug(
            "[MeanRev %s] No reversal candle (%s) and no liquidity sweep — signal rejected",
            symbol, candle_label,
        )
        return None

    # ── Composite scoring ─────────────────────────────────────────────────────
    score = 0
    reasons = []

    # 1. RSI extreme (25 pts)
    rsi_dist = (MR_RSI_OVERSOLD - rsi_val) if direction == "BUY" else (rsi_val - MR_RSI_OVERBOUGHT)
    rsi_pts  = min(25, int(25 * rsi_dist / 15))   # full 25 pts at 15-unit extreme
    score += rsi_pts
    reasons.append(f"RSI={rsi_val:.1f} (+{rsi_pts})")

    # 2. VWAP deviation (20 pts)
    if vwap_val > 0:
        dev_pct = (close_val - vwap_val) / vwap_val * 100
        if (direction == "BUY" and dev_pct <= -MR_VWAP_DEV_PCT) or \
           (direction == "SELL" and dev_pct >= MR_VWAP_DEV_PCT):
            vwap_pts = min(20, int(20 * abs(dev_pct) / 3.0))
            score   += vwap_pts
            reasons.append(f"VWAP_dev={dev_pct:.1f}% (+{vwap_pts})")

    # 3. Reversal candle (20 pts — binary)
    if has_reversal_candle:
        score += 20
        reasons.append(f"{candle_label} (+20)")

    # 4. Liquidity Sweep (15 pts — pre-computed above)
    if has_sweep:
        score += 15
        reasons.append("LiqSweep (+15)")

    # 5. 4H zone alignment (10 pts)
    if _check_4h_zone(df_4h, direction):
        score += 10
        reasons.append("4H_zone (+10)")

    # 6. 1D trend alignment (10 pts)
    if _check_1d_alignment(df_1d, direction):
        score += 10
        reasons.append("1D_align (+10)")

    # 7. Structural liquidity confluence (up to 10 pts)
    if ENABLE_MARKET_STRUCTURE_FILTERS and ENABLE_LIQUIDITY_MAP_FILTER and liq is not None:
        liq_pts = liquidity_sweep_score(direction, close_val, liq)
        if liq_pts > 0:
            score += liq_pts
            reasons.append(f"LiqMap (+{liq_pts})")

    if ENABLE_MARKET_STRUCTURE_FILTERS and htf is not None:
        reasons.append(
            f"MS zone={htf.zone} range=[{htf.low:.2f}-{htf.high:.2f}] eq={htf.eq:.2f}"
        )
        if ENABLE_LIQUIDITY_MAP_FILTER and liq is not None:
            reasons.append(
                "LiqLvls "
                f"PDH={liq.pdh if liq.pdh is not None else 'NA'} "
                f"PDL={liq.pdl if liq.pdl is not None else 'NA'} "
                f"ORH={liq.orh if liq.orh is not None else 'NA'} "
                f"ORL={liq.orl if liq.orl is not None else 'NA'}"
            )

    log.info(
        "[MeanRev %s] %s | score=%d | %s",
        symbol, direction, score, " | ".join(reasons),
    )

    if score < MR_MIN_SCORE:
        log.debug("[MeanRev %s] Score %d below threshold %d", symbol, score, MR_MIN_SCORE)
        return None

    # Map score (55–100) to confidence (65–95 %)
    confidence = round(65.0 + (score - MR_MIN_SCORE) / (100 - MR_MIN_SCORE) * 30.0, 1)
    confidence = min(95.0, confidence)

    # ATR-based stop distance as a % of entry price (used by calculate_position_size)
    # We target 1× ATR from entry; the executor will refine with the live ATR.
    # Here we estimate via the 15m range as a proxy.
    avg_range   = float((df_15m["High"] - df_15m["Low"]).tail(14).mean())
    stop_pct    = round(avg_range / close_val, 4) if close_val > 0 else 0.01
    stop_pct    = max(0.005, min(0.05, stop_pct))   # clamp to 0.5 % – 5 %

    return {
        "action":       direction,
        "confidence":   confidence,
        "score":        score,
        "strategy":     "MeanReversion",
        "stop_loss_pct": stop_pct,
        "reason":       " | ".join(reasons),
    }
