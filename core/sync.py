"""
DB ↔ Broker Reconciliation — NATB v2.0

Runs at the start of every monitoring cycle to fix any discrepancies:

  Case 1 — Locally OPEN, not on broker:
    Position was closed externally (TP hit, manual close, margin call).
    → Mark CLOSED in DB, fetch PnL from broker history, feed into
      Circuit Breaker state machine.

  Case 2 — On broker, not tracked locally:
    Position opened manually via the platform UI.
    → Insert into DB so the trailing stop engine starts tracking it.
"""

import sqlite3
import requests
import re
import time
from datetime import datetime as _dt

from core.trade_session_finalize import after_trade_leg_closed
from core.trade_close_messages import (
    send_reconcile_generic_external,
    send_reconcile_tp1_hit,
    send_reconcile_tp2_final,
)
from database.db_manager import is_maintenance_mode

DB_PATH = 'database/trading_saas.db'


def reconcile(chat_id, base_url, headers, *, notify: bool = True):
    """
    Sync local DB with live Capital.com /positions.
    Should be called once per monitoring cycle before any stop checks.
    """
    pos_res = requests.get(f"{base_url}/positions", headers=headers)
    if pos_res.status_code != 200:
        return

    live_positions = pos_res.json().get('positions', [])
    # Normalize dealId types: broker may return int while DB stores str (or vice versa).
    # If we don't normalize, membership checks will fail and we keep "re-syncing"
    # the same position every monitoring cycle.
    live_deal_ids = {
        str(p['position']['dealId'])
        for p in live_positions
        if p.get('position', {}).get('dealId') is not None
    }

    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    # Fetch locally tracked open trades
    c.execute(
        "SELECT trade_id, deal_id, symbol, direction, entry_price, size, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), stop_distance, trailing_stop "
        "FROM trades WHERE chat_id=? AND status='OPEN'",
        (chat_id,),
    )
    local_open = c.fetchall()
    # If TP1 and TP2 both closed on the broker in the same cycle, process TP1 first
    # so TP1 is CLOSED in DB before TP2 final aggregates P&L.
    local_open = sorted(
        local_open,
        key=lambda r: (
            0 if (r[6] or "").strip() == "TP1" else 1 if (r[6] or "").strip() == "TP2" else 2,
            r[0],
        ),
    )
    local_deal_ids = {str(row[1]) for row in local_open if row[1]}

    # ── Case 1: closed externally ─────────────────────────────────────────────
    for (
        trade_id,
        deal_id,
        symbol,
        direction,
        entry_price,
        size,
        leg_role,
        parent_session,
        stop_distance,
        trailing_stop,
    ) in local_open:
        if deal_id and str(deal_id) not in live_deal_ids:
            # Broker realizedPnL can be delayed right after a manual close.
            # If we read too early, we may incorrectly get 0.0 and send
            # "Breakeven" to Telegram even though the final realized P&L is > 0.
            pnl = _fetch_closed_pnl(
                base_url,
                headers,
                str(deal_id),
                wait_for_realized=True,
                fallback_calc={
                    "direction": direction,
                    "entry_price": entry_price,
                    "size": size,
                },
            )
            cur = c.execute(
                "UPDATE trades SET status='CLOSED', pnl=?, closed_at=? "
                "WHERE trade_id=? AND status='OPEN'",
                (pnl, _dt.now().isoformat(), trade_id),
            )
            # If rowcount is 0, another process updated it before this cycle.
            if cur.rowcount != 1:
                continue

            # Persist immediately so Telegram sync messages don't repeat
            # if anything fails after updating this row.
            conn.commit()

            ps = (parent_session or "").strip()
            pnl_f = float(pnl)
            # One risk outcome per session (not per leg) for TP1+TP2 splits.
            after_trade_leg_closed(chat_id, ps, pnl_f)

            # During maintenance, we never send Telegram messages from reconcile.
            # Also allow callers (e.g., dashboard UI) to run reconcile silently
            # to clean up stale DB rows without spamming the user.
            if is_maintenance_mode() or not notify:
                continue

            ep = float(entry_price or 0)
            sz = float(size or 0)
            ts = float(trailing_stop) if trailing_stop is not None else None
            sd = stop_distance
            if sd is None and ts is not None and ep:
                sd = abs(ep - ts)
            lr = (leg_role or "").strip()

            if lr == "TP1" and ps:
                tp2_open = c.execute(
                    "SELECT 1 FROM trades WHERE parent_session=? AND status='OPEN' "
                    "AND COALESCE(leg_role,'')='TP2' LIMIT 1",
                    (ps,),
                ).fetchone()
                send_reconcile_tp1_hit(
                    chat_id,
                    symbol=symbol,
                    direction=direction,
                    entry_price=ep,
                    size=sz,
                    pnl=pnl_f,
                    stop_distance=sd,
                    trailing_stop=ts,
                    tp2_still_open=bool(tp2_open),
                )
            elif lr == "TP2" and ps:
                c.execute(
                    "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE parent_session=? AND status='CLOSED'",
                    (ps,),
                )
                total_pnl = float(c.fetchone()[0])
                c.execute(
                    "SELECT COALESCE(SUM(size),0) FROM trades WHERE parent_session=? AND status='CLOSED'",
                    (ps,),
                )
                total_qty = float(c.fetchone()[0])
                c.execute(
                    "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE parent_session=? "
                    "AND COALESCE(leg_role,'')='TP1' AND status='CLOSED'",
                    (ps,),
                )
                tp1_pnl = float(c.fetchone()[0])
                tp2_pnl = pnl_f
                c.execute(
                    "SELECT symbol, direction, entry_price, stop_distance, trailing_stop "
                    "FROM trades WHERE parent_session=? ORDER BY trade_id LIMIT 1",
                    (ps,),
                )
                row0 = c.fetchone()
                sym0 = row0[0] if row0 else symbol
                dir0 = row0[1] if row0 else direction
                ep0 = float(row0[2]) if row0 and row0[2] is not None else ep
                sd0 = row0[3] if row0 else None
                ts0 = float(row0[4]) if row0 and row0[4] is not None else ts
                sd_use = sd0 if sd0 is not None else sd
                if sd_use is None and ts0 is not None and ep0:
                    sd_use = abs(ep0 - ts0)
                send_reconcile_tp2_final(
                    chat_id,
                    symbol=sym0,
                    direction=dir0,
                    entry_price=ep0,
                    total_qty=total_qty,
                    tp1_pnl=tp1_pnl,
                    tp2_pnl=tp2_pnl,
                    total_pnl=total_pnl,
                    stop_distance=sd_use,
                    trailing_stop=ts0,
                )
            else:
                send_reconcile_generic_external(
                    chat_id,
                    symbol=symbol,
                    direction=direction,
                    entry_price=ep,
                    size=sz,
                    pnl=pnl_f,
                    stop_distance=sd,
                    trailing_stop=ts,
                    reason_hint="sync",
                )

    # ── Case 2: manually opened, not tracked ─────────────────────────────────
    for p in live_positions:
        deal_id = p.get('position', {}).get('dealId')
        if deal_id is None:
            continue
        deal_id_str = str(deal_id)
        if deal_id_str not in local_deal_ids:
            symbol    = p['market'].get('epic', p['market'].get('instrumentName', ''))
            direction = p['position']['direction']
            entry     = float(p['position'].get('level', 0))
            size      = float(p['position'].get('size', 1))

            c.execute(
                '''INSERT INTO trades
                   (chat_id, symbol, direction, entry_price, size, deal_id, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'OPEN')''',
                (chat_id, symbol, direction, entry, size, deal_id_str)
            )
            print(f"🔄 مزامنة: صفقة يدوية رُصدت في {symbol} ({direction}) — تم إضافتها للتتبع.")

    conn.commit()
    conn.close()


def _fetch_closed_pnl(
    base_url,
    headers,
    deal_id,
    *,
    wait_for_realized: bool = False,
    fallback_calc: dict | None = None,
):
    """
    Attempt to retrieve the realized PnL of a closed deal from broker history.
    Returns 0.0 if the endpoint is unavailable or the deal isn't found.
    """
    def _parse_pnl(tx: dict) -> float | None:
        # Capital payloads may vary by account/region.
        # Try several known keys and parse numeric strings safely.
        for k in ("profitAndLoss", "profitAndLossValue", "pnl", "realisedPnl", "realizedPnl"):
            if k not in tx or tx.get(k) is None:
                continue
            v = tx.get(k)
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            m = re.search(r"[-+]?\d+(?:\.\d+)?", s.replace(",", ""))
            if m:
                try:
                    return float(m.group(0))
                except Exception:
                    continue
        return None

    def _parse_price(tx: dict, keys: tuple[str, ...]) -> float | None:
        for k in keys:
            if k not in tx or tx.get(k) is None:
                continue
            v = tx.get(k)
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).strip()
            m = re.search(r"[-+]?\d+(?:\.\d+)?", s.replace(",", ""))
            if m:
                try:
                    return float(m.group(0))
                except Exception:
                    continue
        return None

    def _fallback_compute(tx: dict) -> float | None:
        """
        If broker doesn't return realised PnL yet (or ever), attempt to compute it.
        Uses entry_price/size/direction from the DB row and a close price from tx.
        """
        if not fallback_calc:
            return None
        try:
            direction = str(fallback_calc.get("direction") or "").strip().upper()
            entry = float(fallback_calc.get("entry_price") or 0)
            qty = float(fallback_calc.get("size") or 0)
            if not direction or entry <= 0 or qty <= 0:
                return None
        except Exception:
            return None

        # Try to locate an execution/close price in the transaction payload.
        close_px = _parse_price(
            tx,
            (
                "level",
                "price",
                "closeLevel",
                "closingLevel",
                "closePrice",
                "executionPrice",
            ),
        )
        if close_px is None or close_px <= 0:
            return None

        diff = (close_px - entry) if direction == "BUY" else (entry - close_px)
        pnl_est = diff * qty
        # Avoid rounding noise being treated as "real".
        if abs(pnl_est) < 0.005:
            return 0.0
        return float(pnl_est)

    def _fetch(params: dict | None) -> list:
        res = requests.get(
            f"{base_url}/history/transactions",
            params=params or {},
            headers=headers,
        )
        if res.status_code != 200:
            return []
        return (res.json() or {}).get("transactions", []) or []

    # When called from sync for externally-closed deals, the broker may not
    # have the realized PnL populated yet. We can safely retry briefly.
    attempts = 1
    delay_s = 0.0
    if wait_for_realized:
        attempts = 5
        delay_s = 0.7

    pnl_zero_candidate: float | None = None

    for attempt in range(attempts):
        try:
            # Try direct dealId filter first.
            txs = _fetch({"dealId": str(deal_id)})
            # Fallback: fetch recent transactions window and match manually.
            if not txs:
                txs = _fetch({"max": 200})

            for tx in txs:
                tx_deal = tx.get("dealId")
                tx_ref  = tx.get("dealReference")
                if str(tx_deal) == str(deal_id) or str(tx_ref) == str(deal_id):
                    p = _parse_pnl(tx)
                    if p is None:
                        # Try fallback compute when realisedPnL is absent.
                        f = _fallback_compute(tx)
                        if f is None:
                            continue
                        p = f
                    p_f = float(p)

                    # If realizedPnL is still not ready, broker may temporarily
                    # return 0.0. Retry when we got 0.0.
                    if p_f == 0.0:
                        pnl_zero_candidate = 0.0
                        continue
                    return p_f
        except Exception:
            # On transient API/network issues we retry (when enabled).
            pass

        if delay_s and attempt < attempts - 1:
            time.sleep(delay_s)

    return float(pnl_zero_candidate) if pnl_zero_candidate is not None else 0.0


def backfill_closed_pnls(chat_id, base_url, headers, lookback: int = 200) -> int:
    """
    Backfill missing/zero PnL values for already CLOSED trades from broker history.
    Useful when a deal was closed manually and initial reconciliation stored 0.0.
    Returns number of rows updated.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT trade_id, deal_id, pnl, direction, entry_price, size FROM trades "
        "WHERE chat_id=? AND status='CLOSED' "
        "AND deal_id IS NOT NULL AND TRIM(deal_id) != '' "
        "ORDER BY trade_id DESC LIMIT ?",
        (chat_id, int(lookback)),
    )
    rows = c.fetchall()
    if not rows:
        conn.close()
        return 0

    updated = 0
    for trade_id, deal_id, pnl, direction, entry_price, size in rows:
        # Backfill only unknown/zero rows.
        if pnl is not None and float(pnl) != 0.0:
            continue
        fetched = _fetch_closed_pnl(base_url, headers, str(deal_id))
        if fetched == 0.0:
            fetched = _fetch_closed_pnl(
                base_url,
                headers,
                str(deal_id),
                wait_for_realized=True,
                fallback_calc={
                    "direction": direction,
                    "entry_price": entry_price,
                    "size": size,
                },
            )
        # Skip when broker still returns zero/unknown.
        if fetched == 0.0:
            continue
        c.execute("UPDATE trades SET pnl=? WHERE trade_id=?", (float(fetched), int(trade_id)))
        if c.rowcount == 1:
            updated += 1

    if updated:
        conn.commit()
    conn.close()
    return updated
