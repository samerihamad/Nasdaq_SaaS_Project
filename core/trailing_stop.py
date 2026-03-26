"""
ATR-Based Dynamic Trailing Stop — NATB v2.0

Logic:
  BUY  position: trailing_stop = max(prev_stop, current_price - ATR × multiplier)
                 close when current_price <= trailing_stop
  SELL position: trailing_stop = min(prev_stop, current_price + ATR × multiplier)
                 close when current_price >= trailing_stop

The stop only ever moves in the direction of the trade — never against it.
"""

import pandas as pd
import numpy as np
import sqlite3
import yfinance as yf

ATR_PERIOD     = 14
ATR_MULTIPLIER = 2.0
DB_PATH        = 'database/trading_saas.db'


# ── ATR calculation ───────────────────────────────────────────────────────────

def calculate_atr(symbol, period=ATR_PERIOD):
    """
    Fetch 1-month of daily bars for `symbol` and return the latest ATR value.
    Uses Wilder's smoothed ATR (rolling mean over True Range).
    Returns None on failure so the caller can fall back gracefully.
    """
    try:
        df = yf.download(symbol, period="1mo", interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or len(df) < period + 1:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        high  = df['High'].squeeze()
        low   = df['Low'].squeeze()
        close = df['Close'].squeeze()

        prev_close = close.shift(1)
        tr = pd.concat([
            (high - low),
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)

        atr = tr.rolling(period).mean()
        return float(atr.iloc[-1])

    except Exception as e:
        print(f"⚠️  ATR [{symbol}]: {e}")
        return None


# ── Stop level computation ────────────────────────────────────────────────────

def compute_stop_candidate(direction, current_price, atr, multiplier=ATR_MULTIPLIER):
    """Return the raw ATR-based stop level for the current bar."""
    offset = multiplier * atr
    if direction == 'BUY':
        return round(current_price - offset, 6)
    else:
        return round(current_price + offset, 6)


def advance_trailing_stop(prev_stop, candidate, direction):
    """
    Ratchet the trailing stop — only moves in the profitable direction.
    BUY : stop is the higher of prev and candidate (moves up only).
    SELL: stop is the lower  of prev and candidate (moves down only).
    """
    if direction == 'BUY':
        return max(prev_stop, candidate)
    else:
        return min(prev_stop, candidate)


def is_stop_hit(current_price, trailing_stop, direction):
    """True if the price has reached or crossed the trailing stop."""
    if direction == 'BUY':
        return current_price <= trailing_stop
    else:
        return current_price >= trailing_stop


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_open_trades(chat_id):
    """Return all locally tracked OPEN trades for this user."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT trade_id, deal_id, symbol, direction, entry_price, size, trailing_stop, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), stop_distance, COALESCE(target_reached,'') "
        "FROM trades WHERE chat_id=? AND status='OPEN'",
        (chat_id,)
    )
    rows = c.fetchall()
    conn.close()
    return [
        {
            'trade_id':       r[0],
            'deal_id':        r[1],
            'symbol':         r[2],
            'direction':      r[3],
            'entry_price':    r[4],
            'size':           r[5],
            'trailing_stop':  r[6],
            'leg_role':       r[7] or None,
            'parent_session': r[8] or None,
            'stop_distance':  r[9],
            'target_reached': r[10] or None,
        }
        for r in rows
    ]


def update_trade_stop(trade_id, new_stop):
    """Persist the updated trailing stop level to the DB."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE trades SET trailing_stop=? WHERE trade_id=?",
        (new_stop, trade_id)
    )
    conn.commit()
    conn.close()


def update_trade_target_reached(trade_id: int, target_reached: str) -> None:
    """
    Persist the highest target milestone reached so far.
    Examples: 'TARGET_1_HIT', 'TARGET_2_HIT'
    """
    if not target_reached:
        return
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE trades SET target_reached=? WHERE trade_id=?",
        (str(target_reached), int(trade_id)),
    )
    conn.commit()
    conn.close()


def close_trade_in_db(
    trade_id: int,
    *,
    actual_pnl: float,
    exit_price: float | None = None,
    target_reached: str | None = None,
    close_reason: str | None = None,
):
    """
    Mark a locally tracked trade as CLOSED using broker-synced final data.

    Notes:
    - `pnl` is set to `actual_pnl` for backward compatibility with reports.
    - Do NOT pass unrealized PnL here; call Capital.com history sync first.
    """
    from utils.market_hours import utc_now
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE trades SET status='CLOSED', pnl=?, actual_pnl=?, exit_price=?, "
        "target_reached=COALESCE(?, target_reached), close_reason=?, closed_at=? "
        "WHERE trade_id=?",
        (
            float(actual_pnl),
            float(actual_pnl),
            float(exit_price) if exit_price is not None else None,
            str(target_reached) if target_reached else None,
            str(close_reason) if close_reason else None,
            utc_now().isoformat(),
            int(trade_id),
        ),
    )
    conn.commit()
    conn.close()


def record_open_trade(
    chat_id,
    symbol,
    direction,
    entry_price,
    size,
    deal_id,
    initial_stop,
    leg_role=None,
    parent_session=None,
    stop_distance=None,
):
    """Insert a new trade into the local DB when a position is opened (UTC timestamp)."""
    from utils.market_hours import utc_now
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        '''INSERT INTO trades
           (chat_id, symbol, direction, entry_price, size, deal_id, trailing_stop, status, opened_at,
            leg_role, parent_session, stop_distance)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?)''',
        (
            chat_id,
            symbol,
            direction,
            entry_price,
            size,
            deal_id,
            initial_stop,
            utc_now().isoformat(),
            leg_role,
            parent_session,
            stop_distance,
        ),
    )
    trade_id = cur.lastrowid
    conn.commit()
    conn.close()
    return trade_id
