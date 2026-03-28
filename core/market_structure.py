"""
Market Structure Foundation (Sprint 1)

Provides lightweight, strategy-safe structural filters:
- HTF range + premium/discount classification
- Daily liquidity levels (PDH/PDL)
- Intraday opening range levels
- Mid-range no-trade-zone gate
"""

from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass
class HtfRange:
    low: float
    high: float
    eq: float
    zone: str  # "premium" | "discount" | "equilibrium"


@dataclass
class LiquidityMap:
    pdh: float | None
    pdl: float | None
    orh: float | None
    orl: float | None


@dataclass
class MarketStructureContext:
    """
    Strategy-facing structural context for soft policy scoring.
    """
    htf_bias: str                 # bullish | bearish | neutral
    zone: str                     # premium | discount | equilibrium | unknown
    regime: str                   # trending | ranging | volatile | unknown
    in_no_trade_zone: bool
    sweep_detected: bool
    ms_score: int                 # 0..100
    hard_block: bool
    block_reason: str
    htf: HtfRange | None
    liq: LiquidityMap | None


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        out = df.copy()
        out.columns = out.columns.get_level_values(0)
        return out
    return df


def compute_htf_range(df_4h: pd.DataFrame, close_price: float, lookback: int = 60) -> HtfRange | None:
    """
    Build a higher-timeframe range using recent 4H candles.
    """
    if df_4h is None or df_4h.empty or close_price <= 0:
        return None

    frame = _flatten(df_4h).tail(max(5, int(lookback)))
    if frame.empty:
        return None

    low = float(frame["Low"].min())
    high = float(frame["High"].max())
    if high <= low:
        return None

    eq = (high + low) / 2.0
    # Mid bucket keeps entries away from "no man's land".
    band = (high - low) * 0.05

    if close_price > (eq + band):
        zone = "premium"
    elif close_price < (eq - band):
        zone = "discount"
    else:
        zone = "equilibrium"

    return HtfRange(low=low, high=high, eq=eq, zone=zone)


def allow_direction_by_pd(direction: str, htf: HtfRange | None) -> bool:
    """
    Longs only in discount, shorts only in premium.
    """
    if htf is None:
        return False
    if direction == "BUY":
        return htf.zone == "discount"
    return htf.zone == "premium"


def in_no_trade_zone(close_price: float, htf: HtfRange | None, width_pct: float = 0.1) -> bool:
    """
    Reject entries around HTF equilibrium.
    width_pct is the center-band percentage of total range (default 10%).
    """
    if htf is None or close_price <= 0:
        return False
    width = max(0.0, min(0.45, float(width_pct))) * (htf.high - htf.low)
    return (htf.eq - width) <= close_price <= (htf.eq + width)


def build_liquidity_map(df_1d: pd.DataFrame, df_15m: pd.DataFrame, opening_bars: int = 4) -> LiquidityMap:
    """
    PDH/PDL from previous daily candle.
    Opening Range from current session's first `opening_bars` 15m candles.
    """
    pdh = None
    pdl = None
    orh = None
    orl = None

    if df_1d is not None and not df_1d.empty and len(df_1d) >= 2:
        daily = _flatten(df_1d)
        prev = daily.iloc[-2]
        pdh = float(prev["High"])
        pdl = float(prev["Low"])

    if df_15m is not None and not df_15m.empty:
        intraday = _flatten(df_15m)
        day = intraday.index[-1].date()
        day_bars = intraday[intraday.index.date == day]
        if len(day_bars) >= 1:
            window = day_bars.head(max(1, int(opening_bars)))
            orh = float(window["High"].max())
            orl = float(window["Low"].min())

    return LiquidityMap(pdh=pdh, pdl=pdl, orh=orh, orl=orl)


def liquidity_sweep_score(direction: str, close_price: float, liq: LiquidityMap) -> int:
    """
    Structural confluence score (0-10) for sweep/reclaim behavior around
    key liquidity levels.
    """
    score = 0
    levels = [liq.pdh, liq.pdl, liq.orh, liq.orl]
    for level in levels:
        if level is None or level == 0:
            continue
        dist = abs(close_price - level) / abs(level)
        if dist <= 0.003:  # within 0.30%
            score += 3
        elif dist <= 0.008:  # within 0.80%
            score += 1

    # Directional nudge.
    if direction == "BUY" and liq.pdl is not None and close_price > liq.pdl:
        score += 1
    if direction == "SELL" and liq.pdh is not None and close_price < liq.pdh:
        score += 1

    return min(10, score)


def _classify_regime(df_15m: pd.DataFrame) -> str:
    """
    Lightweight regime classification from intraday volatility/range behavior.
    """
    if df_15m is None or df_15m.empty or len(df_15m) < 20:
        return "unknown"
    frame = _flatten(df_15m).tail(40)
    close = frame["Close"].astype(float)
    high = frame["High"].astype(float)
    low = frame["Low"].astype(float)
    hl = (high - low) / close.replace(0, pd.NA) * 100
    atr_pct = float(hl.tail(14).mean()) if not hl.empty else 0.0
    trend_proxy = abs(float(close.iloc[-1] - close.iloc[-20]) / max(close.iloc[-20], 1e-9) * 100.0)
    if atr_pct >= 2.8:
        return "volatile"
    if trend_proxy >= 2.0:
        return "trending"
    return "ranging"


def _htf_bias(df_4h: pd.DataFrame) -> str:
    if df_4h is None or df_4h.empty or len(df_4h) < 20:
        return "neutral"
    frame = _flatten(df_4h)
    c = frame["Close"].astype(float)
    ema20 = c.ewm(span=20, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()
    if float(ema20.iloc[-1]) > float(ema50.iloc[-1]):
        return "bullish"
    if float(ema20.iloc[-1]) < float(ema50.iloc[-1]):
        return "bearish"
    return "neutral"


def _detect_sweep(direction: str, df_15m: pd.DataFrame, liq: LiquidityMap | None) -> bool:
    if df_15m is None or df_15m.empty or len(df_15m) < 4:
        return False
    if liq is None:
        return False
    frame = _flatten(df_15m)
    last = frame.iloc[-1]
    prev = frame.iloc[-2]
    if direction == "BUY":
        levels = [x for x in (liq.pdl, liq.orl) if x is not None]
        if not levels:
            return False
        level = max(levels)
        return float(prev["Low"]) < float(level) and float(last["Close"]) > float(level)
    levels = [x for x in (liq.pdh, liq.orh) if x is not None]
    if not levels:
        return False
    level = min(levels)
    return float(prev["High"]) > float(level) and float(last["Close"]) < float(level)


def apply_market_structure_policy(
    direction: str,
    close_price: float,
    df_1d: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_15m: pd.DataFrame,
    *,
    htf_lookback: int = 60,
    no_trade_zone_pct: float = 0.1,
    enable_pd_filter: bool = True,
    enable_liquidity_map_filter: bool = True,
    opening_bars: int = 4,
) -> MarketStructureContext:
    """
    Build unified market-structure context and convert it to a soft score.
    Keeps hard_block available for future strict policies; current v2 policy is
    intentionally soft (score modifiers) to avoid over-rejection.
    """
    htf = compute_htf_range(df_4h, close_price, lookback=htf_lookback)
    liq = build_liquidity_map(df_1d, df_15m, opening_bars=opening_bars) if enable_liquidity_map_filter else None
    zone = htf.zone if htf else "unknown"
    regime = _classify_regime(df_15m)
    bias = _htf_bias(df_4h)
    in_ntz = in_no_trade_zone(close_price, htf, width_pct=no_trade_zone_pct) if htf else False
    sweep = _detect_sweep(direction, df_15m, liq) if liq is not None else False

    # Base neutral score.
    score = 50

    # Premium/discount directional fit.
    if enable_pd_filter and htf is not None:
        if allow_direction_by_pd(direction, htf):
            score += 16
        elif zone == "equilibrium":
            score -= 6
        else:
            score -= 18

    # No-trade zone is a soft penalty (previously hard reject).
    if in_ntz:
        score -= 14

    # HTF trend bias alignment.
    if (direction == "BUY" and bias == "bullish") or (direction == "SELL" and bias == "bearish"):
        score += 8
    elif bias in ("bullish", "bearish"):
        score -= 6

    # Liquidity map and sweep confluence.
    if liq is not None:
        score += min(10, liquidity_sweep_score(direction, close_price, liq))
    if sweep:
        score += 10

    # Regime moderation.
    if regime == "volatile":
        score -= 4
    elif regime == "trending":
        score += 4

    score = int(max(0, min(100, score)))

    return MarketStructureContext(
        htf_bias=bias,
        zone=zone,
        regime=regime,
        in_no_trade_zone=bool(in_ntz),
        sweep_detected=bool(sweep),
        ms_score=score,
        hard_block=False,  # soft policy default
        block_reason="",
        htf=htf,
        liq=liq,
    )
