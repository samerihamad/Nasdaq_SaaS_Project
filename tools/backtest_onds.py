#!/usr/bin/env python3
"""
Standalone backtest for ONDS only.

- Does not modify production code or use the production SQLite DB.
- Loads OHLCV via yfinance (default) or optional CSV files.
- Patches ai_model.load_or_train_model to rule-only inference (no Capital training / model dir writes).
- Per 15m bar: proposes direction, runs utils.ai_model.evaluate_symbol, applies slippage + weighted ATR SL + RR gate from config/risk_manager.

Usage (from repo root):
  python tools/backtest_onds.py
  python tools/backtest_onds.py --csv-15m path/to/onds_15m.csv --csv-1h path/to/onds_1h.csv

Optional: pip install yfinance pandas numpy
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# Repo root on sys.path
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# -----------------------------------------------------------------------------
# Config / risk (from project .env via config + risk_manager)
# -----------------------------------------------------------------------------
from config import TP1_PCT, TP2_PCT, TP1_SPLIT_PCT, TP2_SPLIT_PCT  # noqa: E402
from core.risk_manager import MIN_RR_RATIO, check_rr_ratio  # noqa: E402

SYMBOL = "ONDS"
ENTRY_SLIPPAGE_PCT = float(os.getenv("MAX_SLIPPAGE_PCT", "0.003"))
ATR_PERIOD = 14
W_15M = 0.60
W_1H = 0.40
STOP_ATR_MULT = 2.0  # matches default institutional distance (2 * ATR / entry) pattern
WARMUP_15M = 120


@dataclass
class TradeResult:
    entry_date: str
    direction: str
    entry_price: float
    exit_price: float
    result: str  # Win / Loss
    pnl_pct: float
    reason: str


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = out.columns.get_level_values(0)
    colmap = {c: str(c).replace(" ", "") for c in out.columns}
    out = out.rename(columns={**{c: c.title() for c in out.columns}, **colmap})
    for need in ("Open", "High", "Low", "Close"):
        if need not in out.columns:
            alt = need.lower()
            if alt in out.columns:
                out[need] = out[alt]
    if "Volume" not in out.columns:
        out["Volume"] = 0.0
    out = out[["Open", "High", "Low", "Close", "Volume"]].astype(float)
    out.index = pd.to_datetime(out.index, utc=True)
    out = out.sort_index()
    return out


def _load_yfinance(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        import yfinance as yf
    except ImportError as e:
        raise RuntimeError("Install yfinance: pip install yfinance") from e

    # Yahoo caps 15m history (~60d). Request 3mo for coarser TFs; use max 15m window here.
    d15 = yf.download(symbol, period="59d", interval="15m", progress=False, auto_adjust=False)
    d1h = yf.download(symbol, period="3mo", interval="1h", progress=False, auto_adjust=False)
    d1d = yf.download(symbol, period="2y", interval="1d", progress=False, auto_adjust=False)
    return (
        _normalize_ohlcv(d15),
        _normalize_ohlcv(d1h),
        _normalize_ohlcv(d1d),
    )


def _load_csv(path: str) -> pd.DataFrame:
    raw = pd.read_csv(path)
    if "Date" in raw.columns:
        raw["Date"] = pd.to_datetime(raw["Date"], utc=True)
        raw = raw.set_index("Date")
    else:
        raw.index = pd.to_datetime(raw.index, utc=True)
    return _normalize_ohlcv(raw)


def _resample_4h_from_1h(df_1h: pd.DataFrame) -> pd.DataFrame:
    if df_1h is None or df_1h.empty:
        return pd.DataFrame()
    o = df_1h["Open"].resample("4h").first()
    h = df_1h["High"].resample("4h").max()
    l = df_1h["Low"].resample("4h").min()
    c = df_1h["Close"].resample("4h").last()
    v = df_1h["Volume"].resample("4h").sum()
    out = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c, "Volume": v}).dropna(how="any")
    return out


def _atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> float | None:
    if df is None or len(df) < period + 1:
        return None
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    close = df["Close"].astype(float)
    prev = close.shift(1)
    tr = pd.concat(
        [high - low, (high - prev).abs(), (low - prev).abs()],
        axis=1,
    ).max(axis=1)
    val = tr.rolling(period).mean().iloc[-1]
    if pd.isna(val):
        return None
    return float(val)


def weighted_atr(df_15: pd.DataFrame, df_1h: pd.DataFrame) -> float | None:
    a15 = _atr(df_15)
    a1h = _atr(df_1h)
    if a15 is not None and a1h is not None:
        return W_15M * a15 + W_1H * a1h
    if a15 is not None:
        return a15
    if a1h is not None:
        return a1h
    return None


def _propose_direction(tf: dict, symbol: str) -> tuple[str | None, str]:
    """Alignment if possible; else 15m rule-based side."""
    from utils.ai_model import analyze_multi_timeframe, _flatten, _rule_based_probability

    aligned, _conf, reason = analyze_multi_timeframe(tf, symbol=symbol)
    if aligned in ("BUY", "SELL"):
        return aligned, f"align:{reason}"
    df15 = tf.get("15m")
    if df15 is None or df15.empty:
        return None, "no_15m"
    flat = _flatten(df15)
    pb = float(_rule_based_probability(flat, "BUY"))
    return ("BUY" if pb >= 50.0 else "SELL"), f"rule:{pb:.1f}"


def _apply_entry_slippage(direction: str, signal_close: float) -> float:
    s = float(signal_close)
    if direction == "BUY":
        return s * (1.0 + ENTRY_SLIPPAGE_PCT)
    return s * (1.0 - ENTRY_SLIPPAGE_PCT)


def _stop_level(direction: str, entry: float, w_atr: float) -> float:
    dist = STOP_ATR_MULT * float(w_atr)
    if direction == "BUY":
        return float(entry) - dist
    return float(entry) + dist


def _pnl_frac(direction: str, entry: float, exit_px: float) -> float:
    """Return (exit - entry) / entry signed so profit is positive for both sides."""
    if direction == "BUY":
        return (exit_px - entry) / entry
    return (entry - exit_px) / entry


def _simulate_trade(
    direction: str,
    entry: float,
    stop: float,
    df_15m: pd.DataFrame,
    start_idx: int,
) -> tuple[float, str, str, int, float]:
    """
    Walk forward on 15m bars. Intraday priority: stop before targets (conservative).
    70% / 30% split at TP1 / TP2.
    Returns (reporting_exit_price, Win/Loss, detail, last_bar_index_consumed, pnl_frac_blended).
    """
    tp1 = entry * (1 + TP1_PCT) if direction == "BUY" else entry * (1 - TP1_PCT)
    tp2 = entry * (1 + TP2_PCT) if direction == "BUY" else entry * (1 - TP2_PCT)

    leg1_open = TP1_SPLIT_PCT
    leg2_open = TP2_SPLIT_PCT
    pnl_accum = 0.0

    for j in range(start_idx + 1, len(df_15m)):
        row = df_15m.iloc[j]
        hi = float(row["High"])
        lo = float(row["Low"])

        if direction == "BUY":
            hit_sl = lo <= stop
            hit_tp1 = hi >= tp1
            hit_tp2 = hi >= tp2
        else:
            hit_sl = hi >= stop
            hit_tp1 = lo <= tp1
            hit_tp2 = lo <= tp2

        rem = leg1_open + leg2_open
        if rem > 0 and hit_sl:
            pnl_accum += _pnl_frac(direction, entry, stop) * rem
            return stop, ("Win" if pnl_accum >= 0 else "Loss"), f"stopped rem={rem:.2f}", j, pnl_accum

        if leg1_open > 0 and hit_tp1:
            pnl_accum += _pnl_frac(direction, entry, tp1) * leg1_open
            leg1_open = 0.0

        if leg1_open == 0 and leg2_open > 0:
            if hit_sl:
                pnl_accum += _pnl_frac(direction, entry, stop) * leg2_open
                leg2_open = 0.0
                return stop, ("Win" if pnl_accum >= 0 else "Loss"), "leg2_stop", j, pnl_accum
            if hit_tp2:
                pnl_accum += _pnl_frac(direction, entry, tp2) * leg2_open
                leg2_open = 0.0
                ex = entry * (1 + pnl_accum) if direction == "BUY" else entry * (1 - pnl_accum)
                return float(ex), ("Win" if pnl_accum >= 0 else "Loss"), "tp2", j, pnl_accum

        if leg1_open == 0 and leg2_open == 0:
            ex = entry * (1 + pnl_accum) if direction == "BUY" else entry * (1 - pnl_accum)
            return float(ex), ("Win" if pnl_accum >= 0 else "Loss"), "flat", j, pnl_accum

    # End of data — mark at last close
    last = float(df_15m["Close"].iloc[-1])
    rem = leg1_open + leg2_open
    pnl_accum += _pnl_frac(direction, entry, last) * rem
    return last, ("Win" if pnl_accum >= 0 else "Loss"), "eod", len(df_15m) - 1, pnl_accum


def run_backtest(args: argparse.Namespace) -> list[TradeResult]:
    logging.basicConfig(level=logging.WARNING)

    if args.csv_15m and args.csv_1h:
        df_15 = _load_csv(args.csv_15m)
        df_1h = _load_csv(args.csv_1h)
        df_1d = _load_csv(args.csv_1d) if args.csv_1d else _load_yfinance(SYMBOL)[2]
    else:
        df_15, df_1h, df_1d = _load_yfinance(SYMBOL)

    if df_15 is None or df_15.empty:
        raise RuntimeError("No 15m data for ONDS. Use --csv-15m or check yfinance.")

    df_4h = _resample_4h_from_1h(df_1h)
    if df_4h.empty and df_1h is not None and not df_1h.empty:
        df_4h = df_1h  # fallback: treat 1h as coarse proxy if resample empty

    # Patch AI to rule-only (no Capital scan / no writes to ./models)
    import utils.ai_model as am

    def _load_no_train(symbol: str, timeframe: str = "1d"):
        return None, None

    am.load_or_train_model = _load_no_train  # type: ignore[assignment]
    am._model_cache.clear()

    from utils.ai_model import evaluate_symbol

    open_trade_until: int | None = None
    results: list[TradeResult] = []

    for i in range(WARMUP_15M, len(df_15) - 1):
        if open_trade_until is not None and i <= open_trade_until:
            continue

        ts = df_15.index[i]
        df15_s = _normalize_ohlcv(df_15.loc[:ts])
        df1h_s = _normalize_ohlcv(df_1h.loc[: ts]) if df_1h is not None and not df_1h.empty else pd.DataFrame()
        df4h_s = _normalize_ohlcv(df_4h.loc[: ts]) if df_4h is not None and not df_4h.empty else pd.DataFrame()
        df1d_s = _normalize_ohlcv(df_1d.loc[: ts]) if df_1d is not None and not df_1d.empty else pd.DataFrame()

        tf = {"15m": df15_s, "4h": df4h_s if not df4h_s.empty else df1h_s, "1d": df1d_s}
        # evaluate_symbol expects 4h key; ensure we have something for 4h
        if tf["4h"] is None or tf["4h"].empty:
            if df4h_s.empty and not df1h_s.empty:
                tf["4h"] = df1h_s

        direction, prop_reason = _propose_direction(tf, SYMBOL)
        if direction not in ("BUY", "SELL"):
            continue

        gate = evaluate_symbol(
            SYMBOL,
            {"1d": tf["1d"], "4h": tf["4h"], "15m": tf["15m"], "direction": direction},
            min_probability=None,
            ms_score=None,
        )
        if not gate.get("is_approved"):
            continue

        signal_close = float(df15_s["Close"].iloc[-1])
        w_atr = weighted_atr(df15_s, df1h_s if not df1h_s.empty else df15_s)
        if w_atr is None or w_atr <= 0:
            continue

        entry_exec = _apply_entry_slippage(direction, signal_close)
        stop = _stop_level(direction, entry_exec, w_atr)
        ok_rr, rr_val, rr_reason = check_rr_ratio(
            float(entry_exec),
            float(stop),
            direction,
            float(w_atr),
            min_rr=float(MIN_RR_RATIO),
        )
        if not ok_rr:
            continue

        exit_px, outcome, detail, end_j, pnl_frac = _simulate_trade(
            direction, entry_exec, stop, df_15, i
        )
        pnl_pct = pnl_frac * 100.0

        results.append(
            TradeResult(
                entry_date=str(ts)[:19],
                direction=direction,
                entry_price=round(entry_exec, 4),
                exit_price=round(exit_px, 4),
                result=outcome,
                pnl_pct=round(pnl_pct, 4),
                reason=f"{prop_reason}|{detail}|rr={rr_val}",
            )
        )
        open_trade_until = end_j

    return results


def _max_drawdown(pnls: list[float]) -> float:
    eq = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        eq += p
        peak = max(peak, eq)
        max_dd = max(max_dd, peak - eq)
    return max_dd


def main() -> None:
    p = argparse.ArgumentParser(description="Standalone ONDS backtest (no production DB).")
    p.add_argument("--csv-15m", help="Optional CSV with OHLCV for 15m")
    p.add_argument("--csv-1h", help="Optional CSV with OHLCV for 1h")
    p.add_argument("--csv-1d", help="Optional CSV for daily (else yfinance 2y)")
    p.add_argument("--json-out", help="Write trades JSON to this path (default: temp file)")
    args = p.parse_args()

    trades = run_backtest(args)
    pnls = [t.pnl_pct for t in trades]
    wins = sum(1 for t in trades if t.result == "Win")
    n = len(trades)
    win_rate = (wins / n * 100.0) if n else 0.0
    net = sum(pnls)
    mdd = _max_drawdown(pnls)

    print("\n=== ONDS Backtest Summary (yfinance: ~59d 15m max; 3mo 1h/1d | or CSV) ===")
    print(f"Symbol: {SYMBOL}")
    print(f"Entry slippage: {ENTRY_SLIPPAGE_PCT * 100:.2f}%")
    print(
        f"Weighted ATR SL: {W_15M:.0%}*15m + {W_1H:.0%}*1h, stop mult: {STOP_ATR_MULT}*ATR"
    )
    print(f"TP1/TP2 from config: {TP1_PCT*100:.2f}% / {TP2_PCT*100:.2f}% | split {TP1_SPLIT_PCT:.0%}/{TP2_SPLIT_PCT:.0%}")
    print(f"Min R:R from risk_manager: 1:{MIN_RR_RATIO:.1f}")
    print("-" * 60)
    print(f"Total trades: {n}")
    print(f"Win rate: {win_rate:.2f}%")
    print(f"Net P/L (% sum of per-trade %%): {net:.4f}%")
    print(f"Max drawdown (%% equity curve): {mdd:.4f}%")
    print("-" * 60)
    for t in trades:
        print(
            f"[{t.entry_date}] {t.direction} entry={t.entry_price} exit={t.exit_price} "
            f"=> {t.result} (pnl%={t.pnl_pct}) | {t.reason}"
        )

    out_path = args.json_out or os.path.join(tempfile.gettempdir(), "backtest_onds_trades.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump([asdict(t) for t in trades], f, indent=2)
    print(f"\nTrades JSON: {out_path}")


if __name__ == "__main__":
    main()
