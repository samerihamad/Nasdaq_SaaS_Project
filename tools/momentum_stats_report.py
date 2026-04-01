"""
Temporary diagnostic: Momentum Statistical Opportunity Report (15m).

Fetches the same multi-timeframe data as the live scanner (aiohttp + Semaphore 3),
aggregates RSI buckets, volume vs MA20, SMA alignment, and two simulation scenarios.

Does not modify trading logic. From repo root:

  python tools/momentum_stats_report.py
  python -m tools.momentum_stats_report

Optional:
  python -m tools.momentum_stats_report --watchlist path/to/symbols.txt
  python -m tools.momentum_stats_report --use-config-watchlist
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

# Repo root on path (supports running as script or -m)
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import aiohttp
import numpy as np
import pandas as pd

from config import MAX_WATCHLIST, MIN_15M_BARS, WATCHLIST as CONFIG_WATCHLIST
from core.strategy_momentum import _rsi
from utils.market_scanner import (
    CAPITAL_CLIENT_TIMEOUT,
    CAPITAL_HTTP_CONCURRENCY,
    fetch_15m_ohlcv_async,
)

log = logging.getLogger(__name__)

# Report thresholds (scenario labels per product request; live MOM_VOL_RATIO may differ.)
VOL_STRICT = 1.2
VOL_RELAXED = 1.0
RSI_SCENARIO_A_HI = 65.0
RSI_SCENARIO_B_HI = 75.0


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def compute_15m_metrics(df_15m: pd.DataFrame) -> dict[str, Any] | None:
    """RSI(14), volume/MA20, SMA20/SMA50 alignment on last 15m bar — mirrors momentum data prep."""
    need = max(55, int(MIN_15M_BARS))
    if df_15m is None or df_15m.empty or len(df_15m) < need:
        return None
    df = _flatten(df_15m)
    for col in ("Close", "Volume"):
        if col not in df.columns:
            return None
    close = df["Close"].squeeze().astype(float)
    vol = df["Volume"].squeeze().astype(float)
    rsi_s = _rsi(close, 14)
    rsi_val = float(rsi_s.iloc[-1])
    if not np.isfinite(rsi_val):
        return None
    ma20v = vol.rolling(20).mean()
    av = float(ma20v.iloc[-1])
    v_last = float(vol.iloc[-1])
    vol_ratio = (v_last / av) if av and np.isfinite(av) else float("nan")
    sma20 = close.rolling(20).mean()
    sma50 = close.rolling(50).mean()
    c = float(close.iloc[-1])
    s20 = float(sma20.iloc[-1])
    s50 = float(sma50.iloc[-1])
    if not all(np.isfinite(x) for x in (c, s20, s50)):
        return None
    sma_align = c > s20 and c > s50
    return {
        "rsi": rsi_val,
        "vol_ratio": vol_ratio,
        "sma_align": sma_align,
    }


def build_engine_watchlist() -> list[str]:
    """Same construction as main.run_daily_scan (up to MAX_WATCHLIST symbols)."""
    from utils.filters import get_nasdaq_tickers, level1_filter, level2_filter, level3_filter

    tickers = get_nasdaq_tickers()
    if not tickers:
        return []
    level1 = level1_filter(tickers, top_n=300)
    level2 = level2_filter(level1)
    level3 = level3_filter(level2)
    seen: set[str] = set()
    out: list[str] = []
    for sym in level3:
        if sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
        if len(out) >= MAX_WATCHLIST:
            return out
    if len(out) < MAX_WATCHLIST:
        for sym in level2:
            if sym in seen:
                continue
            seen.add(sym)
            out.append(sym)
            if len(out) >= MAX_WATCHLIST:
                break
    return out


def load_symbols_from_file(path: str) -> list[str]:
    rows: list[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip().upper()
            if s and not s.startswith("#"):
                rows.append(s)
    return rows


async def _one_symbol(
    symbol: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
) -> tuple[str, dict[str, Any] | None, str | None]:
    try:
        df_15m, reason = await fetch_15m_ohlcv_async(
            symbol, session_context=None, session=session, semaphore=sem
        )
        if reason:
            return symbol, None, reason
        if df_15m is None or df_15m.empty:
            return symbol, None, "15m_unavailable"
        m = compute_15m_metrics(df_15m)
        if m is None:
            return symbol, None, "insufficient_bars"
        return symbol, m, None
    except Exception as exc:
        return symbol, None, f"error:{exc!s}"


async def run_momentum_stats_report_async(symbols: list[str]) -> dict[str, Any]:
    """
    Fetch all symbols with shared aiohttp session and CAPITAL_HTTP_CONCURRENCY semaphore.
    Returns aggregate counts (not per-symbol lists).
    """
    sem = asyncio.Semaphore(CAPITAL_HTTP_CONCURRENCY)
    async with aiohttp.ClientSession(timeout=CAPITAL_CLIENT_TIMEOUT) as session:
        tasks = [_one_symbol(sym, session, sem) for sym in symbols]
        rows = await asyncio.gather(*tasks)

    counts: dict[str, int] = {
        "rsi_50_60": 0,
        "rsi_60_70": 0,
        "rsi_70_75": 0,
        "rsi_gt_75": 0,
        "rsi_lt_50": 0,
        "vol_gt_1_2": 0,
        "vol_gt_1_0": 0,
        "sma_align": 0,
        "scenario_a": 0,
        "scenario_b": 0,
        "ok": 0,
        "failed": 0,
    }
    fail_reasons: dict[str, int] = {}

    for sym, m, err in rows:
        if err or m is None:
            counts["failed"] += 1
            key = err or "unknown"
            fail_reasons[key] = fail_reasons.get(key, 0) + 1
            continue
        counts["ok"] += 1
        rsi = m["rsi"]
        vr = m["vol_ratio"]
        align = m["sma_align"]

        if rsi < 50:
            counts["rsi_lt_50"] += 1
        elif 50 <= rsi < 60:
            counts["rsi_50_60"] += 1
        elif 60 <= rsi < 70:
            counts["rsi_60_70"] += 1
        elif 70 <= rsi <= 75:
            counts["rsi_70_75"] += 1
        else:  # rsi > 75
            counts["rsi_gt_75"] += 1

        if np.isfinite(vr):
            if vr > VOL_STRICT:
                counts["vol_gt_1_2"] += 1
            if vr > VOL_RELAXED:
                counts["vol_gt_1_0"] += 1

        if align:
            counts["sma_align"] += 1

        if (
            align
            and np.isfinite(vr)
            and vr > VOL_STRICT
            and 50 <= rsi <= RSI_SCENARIO_A_HI
        ):
            counts["scenario_a"] += 1
        if (
            align
            and np.isfinite(vr)
            and vr > VOL_RELAXED
            and 50 <= rsi <= RSI_SCENARIO_B_HI
        ):
            counts["scenario_b"] += 1

    return {
        "symbols_requested": len(symbols),
        "counts": counts,
        "fail_reasons": fail_reasons,
    }


def format_report_table(result: dict[str, Any]) -> str:
    n = int(result["symbols_requested"])
    c = result["counts"]
    fr = result.get("fail_reasons") or {}
    fail_hint = ""
    if fr:
        top = sorted(fr.items(), key=lambda x: -x[1])[:5]
        fail_hint = "  (fail breakdown: " + ", ".join(f"{k}={v}" for k, v in top) + ")"
    lines = [
        "",
        "=" * 78,
        f"MOMENTUM STATS REPORT (15m) - {n} symbols | http concurrency <= {CAPITAL_HTTP_CONCURRENCY}",
        "=" * 78,
        "RSI BUCKETS (15m, Wilder RSI-14 - same as Momentum strategy)",
        f"  50-60   Current Start Zone          : {c['rsi_50_60']:4d}",
        f"  60-70   Exit Zone / Danger           : {c['rsi_60_70']:4d}",
        f"  70-75   Proposed New Zone            : {c['rsi_70_75']:4d}",
        f"  >75     Extreme Momentum             : {c['rsi_gt_75']:4d}",
        f"  <50     (outside listed zones)       : {c['rsi_lt_50']:4d}",
        "",
        "VOLUME vs MA20 (last bar / 20-bar mean volume)",
        f"  Vol > {VOL_STRICT}x MA20 (strict, diagnostic)    : {c['vol_gt_1_2']:4d}",
        f"  Vol > {VOL_RELAXED}x MA20 (relaxed, diagnostic)  : {c['vol_gt_1_0']:4d}",
        "",
        "SMA ALIGNMENT (15m close > SMA20 and close > SMA50)",
        f"  Count                               : {c['sma_align']:4d}",
        "",
        "SIMULATION SCENARIOS (SMA alignment required)",
        f"  Scenario A (current-style)  RSI 50-{RSI_SCENARIO_A_HI:.0f} & Vol>{VOL_STRICT}x & SMA align : {c['scenario_a']:4d}",
        f"  Scenario B (aggressive)     RSI 50-{RSI_SCENARIO_B_HI:.0f} & Vol>{VOL_RELAXED}x & SMA align : {c['scenario_b']:4d}",
        "",
        f"  Parsed OK: {c['ok']} | Failed / skipped: {c['failed']}{fail_hint}",
        "=" * 78,
        "",
    ]
    return "\n".join(lines)


def run_momentum_stats_report(symbols: list[str]) -> dict[str, Any]:
    """Sync entry: runs asyncio report."""
    return asyncio.run(run_momentum_stats_report_async(symbols))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(description="Momentum Stats Report (diagnostic)")
    p.add_argument(
        "--watchlist",
        type=str,
        default=None,
        help="Path to text file: one ticker per line",
    )
    p.add_argument(
        "--use-config-watchlist",
        action="store_true",
        help=f"Use config.WATCHLIST only (~{len(CONFIG_WATCHLIST)} symbols)",
    )
    p.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help=f"Cap symbol count (default: all from source, engine uses {MAX_WATCHLIST})",
    )
    args = p.parse_args(argv)

    if args.use_config_watchlist:
        symbols = list(CONFIG_WATCHLIST)
    elif args.watchlist:
        symbols = load_symbols_from_file(args.watchlist)
    else:
        log.info("Building engine watchlist (Nasdaq filters, up to %s symbols)...", MAX_WATCHLIST)
        symbols = build_engine_watchlist()

    if args.max_symbols is not None:
        symbols = symbols[: max(0, args.max_symbols)]

    if not symbols:
        log.error("No symbols to scan.")
        return 1

    log.info("Running Momentum Stats Report on %d symbol(s)...", len(symbols))
    result = run_momentum_stats_report(symbols)
    table = format_report_table(result)
    print(table, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
