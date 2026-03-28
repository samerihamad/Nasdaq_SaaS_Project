"""
Backtesting Engine — NATB v2.0

Walk-forward simulation on historical daily bars.
Supports both MeanReversion and Momentum strategies.

Metrics produced:
  win_rate        — % of trades that were profitable
  profit_factor   — gross_profit / gross_loss (> 1.5 is good, > 2 is excellent)
  max_drawdown    — peak-to-trough % drawdown of the equity curve
  expectancy      — average dollar gain per trade (must be > 0 to go live)
  sharpe_ratio    — annualised return / annualised volatility
  total_return    — cumulative % return over the test period

Deployment gate: the bot prints a hard WARNING and does not recommend going live
if expectancy <= 0 or profit_factor < 1.2 or win_rate < 35%.

Usage:
    # Programmatic
    from core.backtest import run_backtest
    result = run_backtest("AAPL", strategy="meanrev", period="2y")
    result.print_summary()

    # CLI
    python -m core.backtest AAPL --strategy momentum --period 2y
    python -m core.backtest NVDA --strategy all --period 1y
"""

import argparse
import logging
from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import pandas as pd
from utils.market_scanner import scan_market

log = logging.getLogger(__name__)

# ── Simulation constants ──────────────────────────────────────────────────────

ATR_PERIOD       = 14
STOP_ATR_MULT    = 2.0    # stop  = entry ± ATR × 2
TARGET_ATR_MULT  = 4.0    # target = entry ± ATR × 4  (guarantees 1:2 RR)
MAX_HOLD_BARS    = 20     # force-exit after N bars if neither stop nor target hit
COMMISSION_PCT   = 0.001  # 0.1% per side (round-trip = 0.2%)
INITIAL_BALANCE  = 10_000
RISK_PER_TRADE   = 0.015  # 1.5% of balance per trade

# Deployment gates
MIN_WIN_RATE     = 35.0   # %
MIN_PROFIT_FACTOR = 1.20
MIN_EXPECTANCY   = 0.0    # dollars (> 0 required)


# ── Result container ──────────────────────────────────────────────────────────

@dataclass
class BacktestResult:
    symbol:         str
    strategy:       str
    period:         str
    n_trades:       int
    win_rate:       float          # %
    profit_factor:  float
    max_drawdown:   float          # % (negative number)
    expectancy:     float          # $ per trade
    sharpe_ratio:   float
    total_return:   float          # %
    equity_curve:   list = field(default_factory=list, repr=False)
    trades:         list = field(default_factory=list, repr=False)

    @property
    def passes_deployment_gate(self) -> bool:
        return (
            self.win_rate      >= MIN_WIN_RATE      and
            self.profit_factor >= MIN_PROFIT_FACTOR  and
            self.expectancy    >  MIN_EXPECTANCY
        )

    def print_summary(self):
        bar  = "=" * 52
        gate = "PASS — ready for live deployment" if self.passes_deployment_gate else "FAIL — DO NOT deploy live"
        print(bar)
        print(f"  Backtest: {self.symbol} | {self.strategy.upper()} | {self.period}")
        print(bar)
        print(f"  Trades         : {self.n_trades}")
        print(f"  Win Rate       : {self.win_rate:.1f}%    (min {MIN_WIN_RATE}%)")
        print(f"  Profit Factor  : {self.profit_factor:.2f}    (min {MIN_PROFIT_FACTOR})")
        print(f"  Max Drawdown   : {self.max_drawdown:.1f}%")
        print(f"  Expectancy     : ${self.expectancy:.2f}/trade  (must be > $0)")
        print(f"  Sharpe Ratio   : {self.sharpe_ratio:.2f}")
        print(f"  Total Return   : {self.total_return:.1f}%")
        print(bar)
        print(f"  Deployment Gate: {gate}")
        print(bar)

        if not self.passes_deployment_gate:
            print()
            print("  ISSUES:")
            if self.win_rate < MIN_WIN_RATE:
                print(f"    - Win rate {self.win_rate:.1f}% < minimum {MIN_WIN_RATE}%")
            if self.profit_factor < MIN_PROFIT_FACTOR:
                print(f"    - Profit factor {self.profit_factor:.2f} < minimum {MIN_PROFIT_FACTOR}")
            if self.expectancy <= MIN_EXPECTANCY:
                print(f"    - Expectancy ${self.expectancy:.2f} ≤ $0 (system has no edge)")
            print()


# ── Indicator helpers ─────────────────────────────────────────────────────────

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def _rsi(s: pd.Series, n: int = 14) -> pd.Series:
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, np.nan)))


def _atr(df: pd.DataFrame, n: int = ATR_PERIOD) -> pd.Series:
    h, lo, c = df['High'].astype(float), df['Low'].astype(float), df['Close'].astype(float)
    pc = c.shift(1)
    tr = pd.concat([(h-lo), (h-pc).abs(), (lo-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def _adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, lo, c = df['High'].astype(float), df['Low'].astype(float), df['Close'].astype(float)
    up, dn = h - h.shift(1), lo.shift(1) - lo
    plus_dm  = np.where((up > dn) & (up > 0),  up.values,  0.0)
    minus_dm = np.where((dn > up) & (dn > 0),  dn.values,  0.0)
    atr_s    = _atr(df, n)
    alpha    = 1 / n
    pdi = 100 * pd.Series(plus_dm,  index=df.index).ewm(alpha=alpha, adjust=False).mean() / atr_s.replace(0, np.nan)
    mdi = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=alpha, adjust=False).mean() / atr_s.replace(0, np.nan)
    dx  = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return dx.ewm(alpha=alpha, adjust=False).mean(), pdi, mdi


def _macd(s: pd.Series):
    ml = _ema(s, 12) - _ema(s, 26)
    sl = _ema(ml, 9)
    return ml, sl


def _flatten(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


# ── Signal generators (daily-bar versions) ────────────────────────────────────

def _signals_meanrev(df: pd.DataFrame) -> pd.Series:
    """
    Generate BUY/SELL/None signals for each daily bar using MeanRev logic.
    No lookahead: signal at bar i uses only data up to bar i.
    """
    df    = _flatten(df).copy()
    close = df['Close'].astype(float)
    high  = df['High'].astype(float)
    low   = df['Low'].astype(float)
    open_ = df['Open'].astype(float)
    vol   = df['Volume'].astype(float)

    rsi   = _rsi(close)
    vwap  = (close * vol).rolling(20).sum() / vol.rolling(20).sum()
    atr_v = _atr(df)

    signals = pd.Series(None, index=df.index, dtype=object)

    for i in range(20, len(df)):
        r      = float(rsi.iloc[i])
        c      = float(close.iloc[i])
        v      = float(vwap.iloc[i])
        prev_c = float(close.iloc[i - 1])
        prev_o = float(open_.iloc[i - 1])
        curr_o = float(open_.iloc[i])
        curr_c = float(close.iloc[i])
        curr_h = float(high.iloc[i])
        curr_l = float(low.iloc[i])

        # News trap: gap > 3%
        if prev_c > 0 and abs(curr_o - prev_c) / prev_c * 100 >= 3.0:
            continue

        # VWAP deviation
        if v <= 0:
            continue
        dev = (c - v) / v * 100

        if r <= 30 and dev <= -1.5:
            # Hammer check
            body  = abs(curr_c - curr_o)
            lower = min(curr_c, curr_o) - curr_l
            upper = curr_h - max(curr_c, curr_o)
            is_hammer = lower >= 2 * (body or 0.0001) and upper <= (body or 0.0001)
            # Bullish engulfing
            is_eng = (prev_c < prev_o) and (curr_c > curr_o) and (curr_o <= prev_c) and (curr_c >= prev_o)
            if is_hammer or is_eng:
                signals.iloc[i] = 'BUY'

        elif r >= 70 and dev >= 1.5:
            body  = abs(curr_c - curr_o)
            upper = curr_h - max(curr_c, curr_o)
            lower = min(curr_c, curr_o) - curr_l
            is_star = upper >= 2 * (body or 0.0001) and lower <= (body or 0.0001)
            is_eng  = (prev_c > prev_o) and (curr_c < curr_o) and (curr_o >= prev_c) and (curr_c <= prev_o)
            if is_star or is_eng:
                signals.iloc[i] = 'SELL'

    return signals


def _signals_momentum(df: pd.DataFrame) -> pd.Series:
    """Generate BUY/SELL/None signals using Momentum logic on daily bars."""
    df    = _flatten(df).copy()
    close = df['Close'].astype(float)
    vol   = df['Volume'].astype(float)

    adx_s, pdi, mdi = _adx(df)
    ml, sl          = _macd(close)
    vol_ratio       = vol / vol.rolling(20).mean()

    signals = pd.Series(None, index=df.index, dtype=object)

    for i in range(30, len(df)):
        adx_v  = float(adx_s.iloc[i])
        vr     = float(vol_ratio.iloc[i])
        pdi_v  = float(pdi.iloc[i])
        mdi_v  = float(mdi.iloc[i])
        ml_cur = float(ml.iloc[i])
        sl_cur = float(sl.iloc[i])
        ml_prv = float(ml.iloc[i-1])
        sl_prv = float(sl.iloc[i-1])

        if adx_v < 25 or vr < 1.8:
            continue

        if pdi_v > mdi_v:
            direction = 'BUY'
        else:
            direction = 'SELL'

        # Require MACD crossover
        if direction == 'BUY' and not (ml_prv < sl_prv < 0 or ml_prv < sl_prv) :
            # fresh cross above signal
            if not (ml_prv < sl_prv and ml_cur > sl_cur):
                continue
        if direction == 'SELL':
            if not (ml_prv > sl_prv and ml_cur < sl_cur):
                continue

        signals.iloc[i] = direction

    return signals


# ── Trade simulator ───────────────────────────────────────────────────────────

def _simulate_trades(df: pd.DataFrame, signals: pd.Series,
                      initial_balance: float = INITIAL_BALANCE) -> tuple:
    """
    Walk-forward trade simulation using ATR-based stop and target.

    Entry  : next bar's open price (avoid lookahead).
    Stop   : entry ± ATR × STOP_ATR_MULT
    Target : entry ± ATR × TARGET_ATR_MULT  (guarantees 1:2 RR)
    Exit   : whichever is hit first; or MAX_HOLD_BARS force-exit.

    Returns (trades: list[dict], equity_curve: list[float])
    """
    df    = _flatten(df).reset_index(drop=False)
    atr_v = _atr(df.set_index(df.columns[0]) if 'Date' in df.columns else df)

    trades       = []
    equity       = initial_balance
    equity_curve = [equity]
    i = 0

    while i < len(df) - 1:
        sig = signals.iloc[i] if i < len(signals) else None
        if sig not in ('BUY', 'SELL'):
            equity_curve.append(equity)
            i += 1
            continue

        entry_idx = i + 1                          # next bar open (no lookahead)
        if entry_idx >= len(df):
            break

        entry  = float(df['Open'].iloc[entry_idx])
        atr_at = float(atr_v.iloc[i]) if i < len(atr_v) else 0.0

        if atr_at <= 0 or entry <= 0:
            equity_curve.append(equity)
            i += 1
            continue

        stop   = entry - atr_at * STOP_ATR_MULT  if sig == 'BUY' else entry + atr_at * STOP_ATR_MULT
        target = entry + atr_at * TARGET_ATR_MULT if sig == 'BUY' else entry - atr_at * TARGET_ATR_MULT

        # Risk-based position size
        risk_amount = equity * RISK_PER_TRADE
        stop_dist   = abs(entry - stop)
        shares      = risk_amount / stop_dist if stop_dist > 0 else 0.0

        if shares <= 0:
            equity_curve.append(equity)
            i += 1
            continue

        # Scan subsequent bars for exit
        outcome = 'TIMEOUT'
        exit_price = float(df['Close'].iloc[min(entry_idx + MAX_HOLD_BARS - 1, len(df)-1)])

        for j in range(entry_idx, min(entry_idx + MAX_HOLD_BARS, len(df))):
            bar_high = float(df['High'].iloc[j])
            bar_low  = float(df['Low'].iloc[j])

            if sig == 'BUY':
                if bar_low  <= stop:   exit_price = stop;   outcome = 'STOP';   break
                if bar_high >= target: exit_price = target; outcome = 'TARGET'; break
            else:
                if bar_high >= stop:   exit_price = stop;   outcome = 'STOP';   break
                if bar_low  <= target: exit_price = target; outcome = 'TARGET'; break

        # P&L (accounting for direction and commission)
        raw_pnl  = (exit_price - entry) * shares if sig == 'BUY' else (entry - exit_price) * shares
        commission = entry * shares * COMMISSION_PCT * 2
        net_pnl  = raw_pnl - commission

        equity = max(0.0, equity + net_pnl)
        equity_curve.append(equity)

        trades.append({
            'entry_bar': entry_idx,
            'direction': sig,
            'entry':     round(entry, 4),
            'exit':      round(exit_price, 4),
            'outcome':   outcome,
            'pnl':       round(net_pnl, 2),
            'shares':    round(shares, 4),
        })

        # Skip to first bar after the exit
        i = entry_idx + (j - entry_idx) + 1

    return trades, equity_curve


# ── Metric calculations ───────────────────────────────────────────────────────

def _compute_metrics(trades: list, equity_curve: list, initial_balance: float) -> dict:
    if not trades:
        return dict(
            n_trades=0, win_rate=0.0, profit_factor=0.0,
            max_drawdown=0.0, expectancy=0.0, sharpe_ratio=0.0,
            total_return=0.0,
        )

    pnls     = [t['pnl'] for t in trades]
    wins     = [p for p in pnls if p > 0]
    losses   = [p for p in pnls if p <= 0]

    win_rate      = len(wins) / len(pnls) * 100
    gross_profit  = sum(wins)
    gross_loss    = abs(sum(losses)) if losses else 1e-9
    profit_factor = gross_profit / gross_loss
    expectancy    = sum(pnls) / len(pnls)
    total_return  = (equity_curve[-1] - initial_balance) / initial_balance * 100

    # Max drawdown
    eq     = np.array(equity_curve, dtype=float)
    peak   = np.maximum.accumulate(eq)
    dd     = (eq - peak) / peak * 100
    max_dd = float(dd.min())

    # Sharpe ratio (daily returns, annualised × √252)
    if len(equity_curve) > 2:
        daily_rets = np.diff(equity_curve) / np.array(equity_curve[:-1], dtype=float)
        mean_r     = daily_rets.mean()
        std_r      = daily_rets.std()
        sharpe     = (mean_r / std_r * np.sqrt(252)) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    return dict(
        n_trades      = len(trades),
        win_rate      = round(win_rate, 1),
        profit_factor = round(profit_factor, 2),
        max_drawdown  = round(max_dd, 1),
        expectancy    = round(expectancy, 2),
        sharpe_ratio  = round(sharpe, 2),
        total_return  = round(total_return, 1),
    )


# ── Public API ────────────────────────────────────────────────────────────────

def run_backtest(
    symbol:           str,
    strategy:         Literal['meanrev', 'momentum'] = 'meanrev',
    period:           str  = '2y',
    initial_balance:  float = INITIAL_BALANCE,
) -> BacktestResult:
    """
    Run a walk-forward backtest for `symbol` using the specified strategy.

    Parameters
    ----------
    symbol          : ticker (e.g. 'AAPL')
    strategy        : 'meanrev' | 'momentum'
    period          : period string ('6mo', '1y', '2y', '5y', …)
    initial_balance : starting portfolio value in USD

    Returns
    -------
    BacktestResult — call .print_summary() for a formatted report.
    """
    log.info("Fetching %s data for %s backtest...", period, symbol)
    df = scan_market(symbol, period=period, interval="1d")

    if df is None or len(df) < 60:
        log.error("Insufficient data for backtest (%s bars)", len(df) if df is not None else 0)
        return BacktestResult(
            symbol=symbol, strategy=strategy, period=period,
            n_trades=0, win_rate=0.0, profit_factor=0.0,
            max_drawdown=0.0, expectancy=0.0, sharpe_ratio=0.0, total_return=0.0,
        )

    df = _flatten(df)
    log.info("Loaded %d daily bars for %s", len(df), symbol)

    # Generate signals (no lookahead)
    if strategy == 'meanrev':
        signals = _signals_meanrev(df)
    elif strategy == 'momentum':
        signals = _signals_momentum(df)
    else:
        raise ValueError(f"Unknown strategy: {strategy!r}. Use 'meanrev' or 'momentum'.")

    signal_count = signals.notna().sum()
    log.info("Signals generated: %d", signal_count)

    # Simulate trades
    trades, equity_curve = _simulate_trades(df, signals, initial_balance)

    # Compute metrics
    metrics = _compute_metrics(trades, equity_curve, initial_balance)

    return BacktestResult(
        symbol        = symbol,
        strategy      = strategy,
        period        = period,
        equity_curve  = equity_curve,
        trades        = trades,
        **metrics,
    )


def run_backtest_all(symbol: str, period: str = '2y',
                     initial_balance: float = INITIAL_BALANCE) -> dict:
    """Run both strategies and return a dict of BacktestResult objects."""
    return {
        'meanrev':  run_backtest(symbol, 'meanrev',  period, initial_balance),
        'momentum': run_backtest(symbol, 'momentum', period, initial_balance),
    }


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="NATB Backtesting Engine")
    parser.add_argument("symbol",     type=str, help="Ticker symbol (e.g. AAPL)")
    parser.add_argument("--strategy", type=str, default="all",
                        choices=["meanrev", "momentum", "all"],
                        help="Strategy to backtest (default: all)")
    parser.add_argument("--period",   type=str, default="2y",
                        help="historical period string (default: 2y)")
    parser.add_argument("--balance",  type=float, default=INITIAL_BALANCE,
                        help=f"Starting balance USD (default: {INITIAL_BALANCE})")
    args = parser.parse_args()

    if args.strategy == "all":
        results = run_backtest_all(args.symbol.upper(), args.period, args.balance)
        for name, res in results.items():
            res.print_summary()
    else:
        res = run_backtest(args.symbol.upper(), args.strategy, args.period, args.balance)
        res.print_summary()
