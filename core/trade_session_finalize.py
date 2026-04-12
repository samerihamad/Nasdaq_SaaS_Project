"""
Multi-leg session accounting (TP1 + TP2 share one parent_session).

- Risk engine (record_trade_result) and win/loss analytics must use *session* total P&L,
  not each leg separately — otherwise a winning session (TP1 profit + TP2 stopped out)
  can be counted as one win + one loss and trip the circuit breaker incorrectly.

- When all legs for a session are CLOSED, we insert one row into `trade_sessions`
  with outcome WIN/LOSS/BREAKEVEN and tp1_hit / tp2_hit flags for reporting
  (e.g. only TP1 vs both targets).
"""

from __future__ import annotations

import sqlite3

from core.risk_manager import record_trade_result
from database.db_manager import DB_PATH


def after_trade_leg_closed(
    chat_id: str,
    parent_session: str | None,
    leg_pnl: float,
) -> None:
    """
    Call after a trade row is marked CLOSED in the DB.

    - Single-leg / legacy (no parent_session): feed leg P&L to risk immediately.
    - Multi-leg: only when *all* legs in the session are closed, compute total P&L,
      persist `trade_sessions`, then call record_trade_result once with the total.
    """
    ps = (parent_session or "").strip()
    if not ps:
        if float(leg_pnl) < 0:
            # Single-leg loss hook
            try:
                from database.behavioral_db import record_negative_habit
                record_negative_habit(
                    symbol="LEGACY_SINGLE_LEG", # Would need full row lookup here for symbol
                    direction="UNKNOWN",
                    regime="UNKNOWN_REGIME",
                    adx_band="UNKNOWN_ADX",
                    time_of_day="UNKNOWN_TIME",
                    sector_sentiment="UNKNOWN_SECTOR"
                )
            except Exception as e:
                pass
        record_trade_result(chat_id, float(leg_pnl))
        return
    finalize_session_if_complete(chat_id, ps)


def finalize_session_if_complete(chat_id: str, parent_session: str) -> None:
    """
    If no OPEN rows remain for this session, aggregate P&L, store trade_sessions,
    and call record_trade_result(total_pnl) exactly once.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT COUNT(*) FROM trades WHERE parent_session=? AND status='OPEN'",
        (parent_session,),
    )
    if c.fetchone()[0] > 0:
        conn.close()
        return

    c.execute(
        "SELECT 1 FROM trade_sessions WHERE session_id=?",
        (parent_session,),
    )
    if c.fetchone():
        conn.close()
        return

    c.execute(
        "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE parent_session=? AND status='CLOSED'",
        (parent_session,),
    )
    total = float(c.fetchone()[0] or 0.0)

    c.execute(
        "SELECT pnl FROM trades WHERE parent_session=? AND COALESCE(leg_role,'')='TP1' "
        "AND status='CLOSED' ORDER BY trade_id LIMIT 1",
        (parent_session,),
    )
    r1 = c.fetchone()
    tp1_pnl = float(r1[0]) if r1 and r1[0] is not None else None

    c.execute(
        "SELECT pnl FROM trades WHERE parent_session=? AND COALESCE(leg_role,'')='TP2' "
        "AND status='CLOSED' ORDER BY trade_id LIMIT 1",
        (parent_session,),
    )
    r2 = c.fetchone()
    tp2_pnl = float(r2[0]) if r2 and r2[0] is not None else None

    tp1_hit = 1 if tp1_pnl is not None and tp1_pnl > 0 else 0
    tp2_hit = 1 if tp2_pnl is not None and tp2_pnl > 0 else 0

    c.execute(
        "SELECT symbol, direction, opened_at FROM trades WHERE parent_session=? "
        "ORDER BY trade_id LIMIT 1",
        (parent_session,),
    )
    row = c.fetchone()
    if not row:
        conn.close()
        return
    symbol, direction, opened_at = row[0], row[1], row[2]

    c.execute(
        "SELECT COUNT(*) FROM trades WHERE parent_session=? AND status='CLOSED'",
        (parent_session,),
    )
    leg_count = int(c.fetchone()[0] or 0)

    c.execute(
        "SELECT MAX(closed_at) FROM trades WHERE parent_session=? AND status='CLOSED'",
        (parent_session,),
    )
    closed_at = c.fetchone()[0]

    if total > 0:
        outcome = "WIN"
    elif total < 0:
        outcome = "LOSS"
        
        # --- Memory Layer Hook: Record the Negative Habit Fingerprint ---
        try:
            from database.behavioral_db import record_negative_habit
            from datetime import datetime
            
            # Simple extraction for 'time_of_day' from 'opened_at'
            time_of_day = "UNKNOWN"
            if opened_at:
                try:
                    dt = datetime.fromisoformat(opened_at)
                    time_of_day = dt.strftime("%H:00 UTC") # Bucket by hour
                except:
                    pass
            
            # Record fingerprint. Real implementation would fetch these from DB or scanner cache.
            record_negative_habit(
                symbol=symbol,
                direction=direction,
                regime="UNKNOWN_REGIME_CACHE", # Placeholder for actual historical DB mapping
                adx_band="UNKNOWN_ADX_CACHE", 
                time_of_day=time_of_day,
                sector_sentiment="UNKNOWN_SECTOR_CACHE"
            )
        except Exception as e:
            print(f"[Behavioral Memory] Error recording negative habit: {e}")
            
    else:
        outcome = "BREAKEVEN"

    c.execute(
        """INSERT INTO trade_sessions
           (session_id, chat_id, symbol, direction, opened_at, closed_at, total_pnl,
            outcome, tp1_hit, tp2_hit, leg_count)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            parent_session,
            chat_id,
            symbol,
            direction,
            opened_at,
            closed_at,
            total,
            outcome,
            tp1_hit,
            tp2_hit,
            leg_count,
        ),
    )
    conn.commit()
    conn.close()

    record_trade_result(chat_id, total)


def session_group_key(parent_session: str | None, trade_id: int) -> str:
    """Stable key for grouping leg rows into one session for daily stats."""
    ps = (parent_session or "").strip()
    return ps if ps else f"single:{trade_id}"
