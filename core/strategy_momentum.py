"""
Momentum Strategy — NATB v2.0

Entry logic (primarily 15m, confirmed by 1D trend):
  1D  — master trend must align: 1D RSI > 50 for BUY, < 50 for SELL.
         EMA20 > EMA50 for BUY, EMA20 < EMA50 for SELL.
  15M — trigger conditions:
         • ADX > MOM_ADX_THRESHOLD (trend is real, not chop)
         • MACD crossover: macd_line crosses above/below signal_line
         • Volume spike: current vol ≥ MOM_VOL_RATIO × 20-bar average
         • Optional gap: gap > MOM_GAP_PCT boosts score

News quality boost (optional, requires NEWS_API_KEY):
  Checks NewsAPI for recent headlines mentioning the ticker.
  A positive/negative headline adds NEWS_QUALITY_SCORE bonus points.

Signal score breakdown (max 100):
  ADX strength        → 20 pts  (scaled; +5 bonus if ADX > MOM_ADX_STRONG)
  MACD crossover      → 20 pts
  Volume spike        → 20 pts  (scaled by ratio)
  1D trend alignment  → 15 pts
  Gap confirmation    → 10 pts
  News quality        → 15 pts  (optional)
"""

import logging
import requests
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from config import (
    MOM_ADX_STRONG,
    MOM_VOL_RATIO,
    MOM_GAP_PCT,
    MOM_MACD_CONFIRM,
    SIGNAL_MOM_MIN_SCORE,
    SIGNAL_PROFILE,
    MIN_15M_BARS,
    FAST_MOM_MIN_SCORE,
    FAST_MOM_ADX_THRESHOLD,
    FAST_MOM_VOL_RATIO,
    FAST_MOM_VOL_RATIO_FLOOR,
    FAST_MOM_VOL_RATIO_HIGH_RSI,
    FAST_MOM_RSI_VOL_TIER_HIGH,
    FAST_MOM_RSI_BUY_MAX,
    FAST_MOM_RSI_SELL_MIN,
    NEWS_API_KEY,
    NEWS_LOOKBACK_HOURS,
    NEWS_QUALITY_SCORE,
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
    apply_market_structure_policy,
)

log = logging.getLogger(__name__)

# FAST ADX gate + vol floor: FAST_MOM_ADX_THRESHOLD and FAST_MOM_VOL_RATIO_FLOOR from config / .env only.


# ── Indicator helpers ─────────────────────────────────────────────────────────

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """Returns (macd_line, signal_line)."""
    ema_fast    = _ema(series, fast)
    ema_slow    = _ema(series, slow)
    macd_line   = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    return macd_line, signal_line


def _adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Wilder's ADX implementation.
    Returns a Series of ADX values (same index as df).
    """
    high  = df["High"].squeeze().astype(float)
    low   = df["Low"].squeeze().astype(float)
    close = df["Close"].squeeze().astype(float)

    prev_high  = high.shift(1)
    prev_low   = low.shift(1)
    prev_close = close.shift(1)

    # True Range
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Directional Movements
    up_move   = high - prev_high
    down_move = prev_low - low

    plus_dm  = np.where((up_move > down_move) & (up_move > 0),  up_move,  0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    plus_dm  = pd.Series(plus_dm,  index=df.index, dtype=float)
    minus_dm = pd.Series(minus_dm, index=df.index, dtype=float)

    # Wilder smoothing (equivalent to EWM with alpha = 1/period)
    atr_s       = tr.ewm(alpha=1 / period, adjust=False).mean()
    plus_di     = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean()  / atr_s.replace(0, np.nan)
    minus_di    = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_s.replace(0, np.nan)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx_series  = dx.ewm(alpha=1 / period, adjust=False).mean()

    return adx_series, plus_di, minus_di


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


# ── MACD crossover detector ───────────────────────────────────────────────────

def _macd_crossover(macd_line: pd.Series, signal_line: pd.Series, direction: str) -> bool:
    """
    Returns True if a fresh crossover occurred on the LAST CLOSED candle (iloc[-2]).
    STRICT POLICY: Only evaluate on fully closed candles, never live forming candles.
    BUY : macd_line crossed ABOVE signal_line (prev closed bar was below, last closed is above)
    SELL: macd_line crossed BELOW signal_line
    """
    if len(macd_line) < 3:
        return False
    # Use iloc[-3] vs iloc[-2] to detect crossover on the last CLOSED candle
    prior_diff = float(macd_line.iloc[-3]) - float(signal_line.iloc[-3])
    last_closed_diff = float(macd_line.iloc[-2]) - float(signal_line.iloc[-2])

    if direction == "BUY":
        return prior_diff < 0 < last_closed_diff          # crossed from below to above on closed candle
    else:
        return prior_diff > 0 > last_closed_diff          # crossed from above to below on closed candle


# ── Volume spike ─────────────────────────────────────────────────────────────

def _volume_ratio(df: pd.DataFrame) -> float:
    """Last CLOSED bar volume / 20-bar rolling average volume. STRICT: use iloc[-2]."""
    vol = df["Volume"].squeeze().astype(float)
    if len(vol) < 2:
        return 0.0
    avg = float(vol.rolling(20).mean().iloc[-2])
    if avg == 0:
        return 0.0
    return float(vol.iloc[-2]) / avg


# ── Gap detector ─────────────────────────────────────────────────────────────

def _gap_pct(df: pd.DataFrame) -> float:
    """
    Overnight/session gap % based on last two CLOSED candles.
    STRICT: Uses iloc[-2] (last closed) vs iloc[-3] (prior closed).
    Returns (Open[last_closed] - Close[prev_closed]) / Close[prev_closed] * 100
    """
    if len(df) < 3:
        return 0.0
    prev_close = float(df["Close"].iloc[-3])  # Candle before last closed
    curr_open  = float(df["Open"].iloc[-2])   # Last closed candle's open
    if prev_close == 0:
        return 0.0
    return (curr_open - prev_close) / prev_close * 100


# ── 1D trend alignment ────────────────────────────────────────────────────────

def _check_1d_trend(df_1d: pd.DataFrame, direction: str) -> bool:
    """
    1D must show a clear trend (evaluated on CLOSED candle only).
    STRICT: Uses iloc[-2] for last closed candle.
      BUY : RSI > 55 AND EMA20 > EMA50  (Changed from 50 to avoid chop)
      SELL: RSI < 45 AND EMA20 < EMA50  (Changed from 50 to avoid chop)
    """
    if df_1d is None or df_1d.empty or len(df_1d) < 2:
        return False
    df_1d  = _flatten(df_1d)
    close  = df_1d["Close"].squeeze().astype(float)
    rsi    = _rsi(close)
    ema20  = _ema(close, 20)
    ema50  = _ema(close, 50)

    # STRICT: Use last CLOSED candle (iloc[-2]), never live forming candle
    rsi_v   = float(rsi.iloc[-2])
    ema20_v = float(ema20.iloc[-2])
    ema50_v = float(ema50.iloc[-2])

    # QUANT OPTIMIZATION: Aggressive RSI thresholds to avoid choppy 50-zone
    if direction == "BUY":
        return rsi_v > 55 and ema20_v > ema50_v
    else:
        return rsi_v < 45 and ema20_v < ema50_v


# ── Price Action Confirmation ────────────────────────────────────────────────

def _price_action_confirmed(df: pd.DataFrame, direction: str) -> bool:
    """
    STRICT Price Action Confirmation Filter.
    
    BUY: Current price (live candle) MUST break above the High of the setup candle (iloc[-2]).
    SELL: Current price (live candle) MUST break below the Low of the setup candle (iloc[-2]).
    
    This prevents premature entries by requiring the market to CONFIRM the signal
    by breaking the setup candle's extreme before we enter.
    """
    if df is None or len(df) < 2:
        return False
    
    df = _flatten(df)
    
    # Setup candle is the last CLOSED candle (iloc[-2])
    setup_high = float(df["High"].iloc[-2])
    setup_low = float(df["Low"].iloc[-2])
    
    # Current/live candle is iloc[-1] - we check if it has broken the setup candle's extreme
    current_high = float(df["High"].iloc[-1])
    current_low = float(df["Low"].iloc[-1])
    
    if direction == "BUY":
        # For BUY: current candle must break above setup candle's high
        confirmed = current_high > setup_high
        if not confirmed:
            log.info("[PA Confirm] BUY rejected: current High %.3f <= setup High %.3f", current_high, setup_high)
        return confirmed
    else:
        # For SELL: current candle must break below setup candle's low
        confirmed = current_low < setup_low
        if not confirmed:
            log.info("[PA Confirm] SELL rejected: current Low %.3f >= setup Low %.3f", current_low, setup_low)
        return confirmed


# ── News quality check ────────────────────────────────────────────────────────

def _news_quality_score(symbol: str, direction: str) -> int:
    """
    Fetch recent headlines from NewsAPI for `symbol`.
    Returns NEWS_QUALITY_SCORE if relevant positive/negative news found, else 0.
    Fails silently — news is a bonus, never a blocker.
    """
    if not NEWS_API_KEY:
        return 0

    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS)).isoformat()
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q":        symbol,
                "from":     since,
                "sortBy":   "publishedAt",
                "pageSize": 5,
                "apiKey":   NEWS_API_KEY,
            },
            timeout=5,
        )
        if resp.status_code != 200:
            return 0

        articles = resp.json().get("articles", [])
        if not articles:
            return 0

        # Keyword scoring — very basic sentiment proxy
        positive_kw = {"beat", "upgrade", "record", "surge", "growth", "profit", "revenue"}
        negative_kw = {"miss", "downgrade", "loss", "cut", "decline", "lawsuit", "recall"}

        for article in articles:
            title = (article.get("title") or "").lower()
            if direction == "BUY"  and any(w in title for w in positive_kw):
                log.info("[Momentum %s] Positive news detected: %s", symbol, article["title"])
                return NEWS_QUALITY_SCORE
            if direction == "SELL" and any(w in title for w in negative_kw):
                log.info("[Momentum %s] Negative news detected: %s", symbol, article["title"])
                return NEWS_QUALITY_SCORE

    except Exception as exc:
        log.debug("[Momentum %s] News API error: %s", symbol, exc)

    return 0


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    timeframes: dict,
    *,
    signal_profile: str | None = None,
    min_confidence_floor: float | None = None,
) -> dict | None:
    """
    Run the full Momentum analysis.

    Parameters
    ----------
    symbol     : ticker string
    timeframes : dict with keys '1d', '4h', '15m' — each a DataFrame

    Returns
    -------
    dict  with keys: action, confidence, score, strategy, stop_loss_pct, reason
    None  if no valid signal found
    """
    logging.info(f"Checking Momentum for {symbol}")
    df_1d  = _flatten(timeframes.get("1d",  pd.DataFrame()))
    df_4h  = _flatten(timeframes.get("4h",  pd.DataFrame()))
    df_15m = _flatten(timeframes.get("15m", pd.DataFrame()))

    # STRICT POLICY: Require at least 3 bars to have 2 closed candles (iloc[-3] and iloc[-2])
    min_bars = max(55, int(MIN_15M_BARS))
    if df_15m.empty or len(df_15m) < max(3, min_bars):
        log.debug("[Momentum %s] Insufficient 15m data (need %d bars)", symbol, min_bars)
        return None

    close_15m = df_15m["Close"].squeeze().astype(float)
    rsi_15m_series = _rsi(close_15m)
    # STRICT: Use last CLOSED candle (iloc[-2]), never live forming candle (iloc[-1])
    rsi_15m_val = float(rsi_15m_series.iloc[-2])
    # Mirror of bullish high-RSI volume tier: very low RSI → relaxed vol floor on SELL.
    bearish_rsi_vol_line = 100.0 - float(FAST_MOM_RSI_VOL_TIER_HIGH)

    # ── ADX ───────────────────────────────────────────────────────────────────
    # STRICT: All indicators evaluated on CLOSED candle (iloc[-2])
    adx_series, plus_di, minus_di = _adx(df_15m)
    adx_val      = float(adx_series.iloc[-2])
    plus_di_val  = float(plus_di.iloc[-2])
    minus_di_val = float(minus_di.iloc[-2])

    # Direction first so every rejection can log Long/Short consistently.
    if plus_di_val > minus_di_val:
        direction = "BUY"
    else:
        direction = "SELL"

    active_adx_min = float(FAST_MOM_ADX_THRESHOLD)
    if adx_val < active_adx_min:
        rej = f"Rejected: Weak Trend (ADX {adx_val:.1f} < {float(active_adx_min):.1f})"
        log.info("[Momentum %s] %s", symbol, rej)
        return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}

    gap_now = _gap_pct(df_15m)
    # Downward gap / open pressure: allow slightly higher RSI on shorts (still bearish tape).
    sell_rsi_ceiling = 55.0 if gap_now <= -float(MOM_GAP_PCT) else 50.0

    # ── 15m RSI band (FAST tier in-engine; GOLDEN re-validated in dispatch_signal) ─
    if direction == "BUY":
        if not (50.0 <= rsi_15m_val <= float(FAST_MOM_RSI_BUY_MAX)):
            rej = (
                f"Rejected: 15m RSI {rsi_15m_val:.1f} outside FAST buy band "
                f"50–{float(FAST_MOM_RSI_BUY_MAX):.0f}"
            )
            log.info("[Momentum %s] %s", symbol, rej)
            return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}
    else:
        if not (float(FAST_MOM_RSI_SELL_MIN) <= rsi_15m_val <= sell_rsi_ceiling):
            rej = (
                f"Rejected: 15m RSI {rsi_15m_val:.1f} outside FAST sell band "
                f"{float(FAST_MOM_RSI_SELL_MIN):.0f}–{sell_rsi_ceiling:.0f}"
                + (f" (gap-down relax)" if sell_rsi_ceiling > 50.0 else "")
            )
            log.info("[Momentum %s] %s", symbol, rej)
            return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}

    # STRICT: Use last CLOSED candle price (iloc[-2])
    close_val = float(close_15m.iloc[-2])

    # FAST SMA flexibility: require SMA20 alignment only (no SMA50 hard gate).
    sma20_15m = close_15m.rolling(20).mean()
    sma50_15m = close_15m.rolling(50).mean()
    # STRICT: SMA evaluated on CLOSED candle (iloc[-2])
    sma20_v = float(sma20_15m.iloc[-2])
    sma50_v = float(sma50_15m.iloc[-2])
    
    # CHANGED FOR MORE SIGNALS — FAST MODE ONLY — CONSERVATIVE BALANCED VERSION — based on April 7-8 logs
    tier = str(signal_profile or SIGNAL_PROFILE or "FAST").strip().upper()
    # April 7-8 structural logs show frequent SMA20 near-miss rejections; use a *tiny* tolerance in FAST only.
    sma20_tolerance = 0.999 if tier == "FAST" else 1.000
    sma20_tolerance_sell = 1.001 if tier == "FAST" else 1.000

    if direction == "BUY" and close_val <= sma20_v * sma20_tolerance:
        rej = f"Rejected: Price not above SMA20 ({close_val:.2f} <= {sma20_v * sma20_tolerance:.2f})"
        log.info("[Momentum %s] %s", symbol, rej)
        return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}
    if direction == "SELL" and close_val >= sma20_v * sma20_tolerance_sell:
        rej = f"Rejected: Price not below SMA20 ({close_val:.2f} >= {sma20_v * sma20_tolerance_sell:.2f})"
        log.info("[Momentum %s] %s", symbol, rej)
        return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}
    if direction == "BUY" and close_val > sma20_v * sma20_tolerance and close_val <= sma50_v:
        log.info("[FAST OPTIMIZATION] Trade triggered by Momentum SMA20-only Filter | %s", symbol)

    # ── Market structure policy v2 (soft-scoring, no structural hard reject) ─
    htf = None
    liq = None
    ms_ctx = None
    if ENABLE_MARKET_STRUCTURE_FILTERS:
        # CHANGED FOR MORE SIGNALS — FAST MODE ONLY — CONSERVATIVE BALANCED VERSION — based on April 7-8 logs
        tier = str(signal_profile or SIGNAL_PROFILE or "FAST").strip().upper()
        ntz_pct = MARKET_STRUCTURE_NO_TRADE_ZONE_PCT
        if tier == "FAST":
            ntz_pct = 0.12  # modest relax from 0.14 (do NOT use 0.08)

        ms_ctx = apply_market_structure_policy(
            direction=direction,
            close_price=close_val,
            df_1d=df_1d,
            df_4h=df_4h,
            df_15m=df_15m,
            htf_lookback=MARKET_STRUCTURE_HTF_LOOKBACK,
            no_trade_zone_pct=ntz_pct,
            enable_pd_filter=ENABLE_PREMIUM_DISCOUNT_FILTER,
            enable_liquidity_map_filter=ENABLE_LIQUIDITY_MAP_FILTER,
            opening_bars=LIQUIDITY_OPENING_RANGE_BARS,
        )
        htf = ms_ctx.htf
        liq = ms_ctx.liq

    # ── MACD crossover ────────────────────────────────────────────────────────
    # STRICT POLICY: MACD confirmation is REQUIRED. No bypasses allowed.
    # REMOVED: macd_bypass_rsi logic — all MACD bypasses eliminated.
    macd_line, signal_line = _macd(close_15m)
    has_macd_confirm = _macd_crossover(macd_line, signal_line, direction)
    if MOM_MACD_CONFIRM and not has_macd_confirm:
        # MACD required but not present — signal rejected
        log.info("[Momentum %s] MACD confirmation required but missing — signal rejected", symbol)
        return {"action": "NONE", "confidence": 0, "score": 0, "strategy": "Momentum", "reason": "MACD confirmation required"}

    # ── Volume vs MA20 (FAST tiered; floor from FAST_MOM_VOL_RATIO_FLOOR in .env) ─
    vol_ratio = _volume_ratio(df_15m)
    _vf = float(FAST_MOM_VOL_RATIO_FLOOR)
    if direction == "SELL":
        if rsi_15m_val <= bearish_rsi_vol_line:
            vol_min = max(_vf, float(FAST_MOM_VOL_RATIO_HIGH_RSI))
        else:
            vol_min = max(_vf, float(FAST_MOM_VOL_RATIO))
    elif rsi_15m_val > float(FAST_MOM_RSI_VOL_TIER_HIGH):
        vol_min = max(_vf, float(FAST_MOM_VOL_RATIO_HIGH_RSI))
    else:
        vol_min = max(_vf, float(FAST_MOM_VOL_RATIO))
    mom_low_vol_entry = (
        (
            direction == "BUY"
            and rsi_15m_val > float(FAST_MOM_RSI_VOL_TIER_HIGH)
            and vol_ratio >= vol_min
            and vol_ratio < float(FAST_MOM_VOL_RATIO)
        )
        or (
            direction == "SELL"
            and rsi_15m_val < bearish_rsi_vol_line
            and vol_ratio >= vol_min
            and vol_ratio < float(FAST_MOM_VOL_RATIO)
        )
    )
    if mom_low_vol_entry:
        log.info(
            "[FAST MOMENTUM] High RSI (%.1f) - Low Vol Entry | %s",
            rsi_15m_val,
            symbol,
        )
        log.info("[FAST OPTIMIZATION] Trade triggered by Momentum High RSI Low Volume | %s", symbol)
    if vol_ratio < vol_min:
        rej = f"Rejected: Low Volume ({vol_ratio:.1f}x < {vol_min:.1f}x)"
        log.info("[Momentum %s] %s", symbol, rej)
        return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}

    # ── Price Action Confirmation ─────────────────────────────────────────────
    # STRICT: Signal candle (iloc[-2]) must be confirmed by live candle breaking its extreme
    if not _price_action_confirmed(df_15m, direction):
        rej = f"Rejected: Price Action Confirmation failed (current candle did not break setup candle extreme)"
        log.info("[Momentum %s] %s", symbol, rej)
        return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}
    log.info("[Momentum %s] Price Action Confirmation passed", symbol)

    # ── Composite scoring ─────────────────────────────────────────────────────
    score   = 0
    reasons = []

    # 1. ADX (20 pts base + 5 bonus for strong trend)
    adx_den = max(1e-9, float(MOM_ADX_STRONG) - float(active_adx_min))
    adx_pts = min(20, int(20 * (adx_val - active_adx_min) / adx_den))
    score  += adx_pts
    reasons.append(f"ADX={adx_val:.1f} (+{adx_pts})")
    if adx_val >= MOM_ADX_STRONG:
        score += 5
        reasons.append("StrongTrend (+5)")

    # 2. MACD crossover (20 pts) — STRICT: must have confirmation, no bypasses
    if has_macd_confirm:
        score += 20
        reasons.append("MACD_cross (+20)")
    else:
        # No MACD confirmation — signal rejected (should not reach here due to early return)
        return {"action": "NONE", "confidence": 0, "score": 0, "strategy": "Momentum", "reason": "MACD confirmation required"}

    # 3. Volume spike (20 pts, scaled vs FAST floor)
    vol_pts = min(20, int(20 * (vol_ratio - vol_min) / 2.0 + 10))
    score  += vol_pts
    reasons.append(f"VolRatio={vol_ratio:.1f}x (+{vol_pts})")

    # 4. 1D trend alignment (15 pts)
    if _check_1d_trend(df_1d, direction):
        score += 15
        reasons.append("1D_trend (+15)")

    # 5. Gap confirmation (10 pts)
    gap = gap_now
    gap_confirms = (direction == "BUY" and gap >= MOM_GAP_PCT) or \
                   (direction == "SELL" and gap <= -MOM_GAP_PCT)
    if gap_confirms:
        score += 10
        reasons.append(f"Gap={gap:.1f}% (+10)")

    # 6. News quality (up to NEWS_QUALITY_SCORE pts — 15 default)
    news_pts = _news_quality_score(symbol, direction)
    if news_pts:
        score  += news_pts
        reasons.append(f"News (+{news_pts})")

    # 7. Structural liquidity confluence (up to 10 pts)
    if ENABLE_MARKET_STRUCTURE_FILTERS and ENABLE_LIQUIDITY_MAP_FILTER and liq is not None:
        liq_pts = liquidity_sweep_score(direction, close_val, liq)
        if liq_pts > 0:
            score += liq_pts
            reasons.append(f"LiqMap (+{liq_pts})")

    # 8. Market-structure policy modifier (soft override path)
    if ENABLE_MARKET_STRUCTURE_FILTERS and ms_ctx is not None:
        ms_mod = int(round((float(ms_ctx.ms_score) - 50.0) / 5.0))
        score += ms_mod
        reasons.append(
            f"MS_score={int(ms_ctx.ms_score)} ({'+' if ms_mod >= 0 else ''}{ms_mod})"
        )

    if ENABLE_MARKET_STRUCTURE_FILTERS and htf is not None:
        reasons.append(
            f"MS zone={htf.zone} range=[{htf.low:.2f}-{htf.high:.2f}] eq={htf.eq:.2f}"
        )
        if ms_ctx is not None:
            reasons.append(
                f"MS regime={ms_ctx.regime} bias={ms_ctx.htf_bias} ntz={int(ms_ctx.in_no_trade_zone)} sweep={int(ms_ctx.sweep_detected)}"
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
        "[Momentum %s] %s | score=%d | %s",
        symbol, direction, score, " | ".join(reasons),
    )

    active_min_score = int(SIGNAL_MOM_MIN_SCORE if SIGNAL_MOM_MIN_SCORE is not None else FAST_MOM_MIN_SCORE)
    if score < active_min_score:
        rej = f"Rejected: Low Confidence (Score {int(score)} < {int(active_min_score)})"
        log.info("[Momentum %s] %s", symbol, rej)
        return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}

    # Map score (60–100) to confidence (65–95 %)
    confidence = round(65.0 + (score - active_min_score) / max(1, (100 - active_min_score)) * 30.0, 1)
    confidence = min(95.0, confidence)

    if min_confidence_floor is not None and float(confidence) < float(min_confidence_floor):
        rej = (
            f"Rejected: Mapped confidence {float(confidence):.1f}% below dynamic floor "
            f"{float(min_confidence_floor):.1f}%"
        )
        log.info("[Momentum %s] %s", symbol, rej)
        return {"rejected": True, "strategy": "Momentum", "reason": rej, "action": direction}

    # Estimate stop distance from recent ATR proxy (15m range)
    avg_range   = float((df_15m["High"] - df_15m["Low"]).tail(14).mean())
    stop_pct    = round(avg_range / close_val, 4) if close_val > 0 else 0.01
    stop_pct    = max(0.005, min(0.05, stop_pct))

    return {
        "action":        direction,
        "confidence":    confidence,
        "score":         score,
        "strategy":      "Momentum",
        "stop_loss_pct": stop_pct,
        "ms_score":      (int(ms_ctx.ms_score) if ms_ctx is not None else None),
        "reason":        " | ".join(reasons),
        "mom_rsi_15m":   rsi_15m_val,
        "mom_vol_ratio": vol_ratio,
        "mom_low_vol_entry": bool(mom_low_vol_entry),
        # REMOVED: "mom_macd_bypassed" — MACD bypass eliminated per strict policy
    }
