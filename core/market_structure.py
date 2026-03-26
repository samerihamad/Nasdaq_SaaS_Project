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
