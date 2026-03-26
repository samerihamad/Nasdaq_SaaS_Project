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

import os
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
from database.db_manager import get_subscriber_lang

DB_PATH = 'database/trading_saas.db'
ENABLE_CLOSE_PENDING_NOTIFY = (os.getenv("ENABLE_CLOSE_PENDING_NOTIFY", "false").strip().lower() == "true")
SYNC_RETRY_COOLDOWN_SEC = int(os.getenv("CLOSE_SYNC_RETRY_COOLDOWN_SEC", "30"))
SYNC_MAX_ATTEMPTS = int(os.getenv("CLOSE_SYNC_MAX_ATTEMPTS", "120"))


def fetch_closed_deal_final_data(
    base_url: str,
    headers: dict,
    deal_id: str,
    *,
    wait_for_realized: bool = True,
    identifiers: list[str] | None = None,
    lookback_max: int = 2000,
) -> dict | None:
    """
    Fetch broker-truth final close data for a dealId from Capital.com history.

    Returns dict:
      { 'actual_pnl': float, 'exit_price': float|None }

    Stop-condition:
    - If history endpoint fails or the deal cannot be found (after retries),
      return None and the caller MUST stop the close/report sequence.

    Important:
    - We do NOT calculate PnL manually. We only read realized PnL from broker history.
    """

    def _parse_float(v) -> float | None:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        m = re.search(r"[-+]?\d+(?:\.\d+)?", s.replace(",", ""))
        if not m:
            return None
        try:
            return float(m.group(0))
        except Exception:
            return None

    def _parse_pnl(tx: dict) -> float | None:
        for k in (
            "profitAndLoss",
            "profitAndLossValue",
            "pnl",
            "realisedPnl",
            "realizedPnl",
        ):
            if k in tx and tx.get(k) is not None:
                p = _parse_float(tx.get(k))
                if p is not None:
                    return p
        return None

    def _parse_exit_price(tx: dict) -> float | None:
        for k in (
            "level",
            "price",
            "closeLevel",
            "closingLevel",
            "closePrice",
            "executionPrice",
        ):
            if k in tx and tx.get(k) is not None:
                px = _parse_float(tx.get(k))
                if px is not None and px > 0:
                    return px
        return None

    def _fetch(params: dict | None) -> tuple[bool, list, int, str]:
        try:
            res = requests.get(
                f"{base_url}/history/transactions",
                params=params or {},
                headers=headers,
                timeout=20,
            )
        except Exception as exc:
            return False, [], 0, f"exception: {exc}"
        if res.status_code != 200:
            txt = (res.text or "").strip()
            return False, [], int(res.status_code), txt[:300]
        return True, (res.json() or {}).get("transactions", []) or [], int(res.status_code), ""

    # Capital history can lag after close (especially around session transitions).
    # Use more attempts + a slightly longer delay to reduce "transaction not found yet".
    attempts = 12 if wait_for_realized else 3
    delay_s = 1.2 if wait_for_realized else 0.0

    ids = [str(deal_id)]
    if identifiers:
        ids.extend([str(x).strip() for x in identifiers if str(x).strip()])
    # De-dup while preserving order
    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    for attempt in range(attempts):
        ok, txs, st, info = _fetch({"dealId": str(deal_id)})
        if not ok:
            # Stop-condition: endpoint failure (do not proceed with close reporting).
            print("Error: Could not sync final data from Capital.com", flush=True)
            print(f"[Capital Sync] history/transactions failed status={st} info={info}", flush=True)
            return None
        if not txs:
            ok2, txs2, st2, info2 = _fetch({"max": int(lookback_max)})
            if not ok2:
                print("Error: Could not sync final data from Capital.com", flush=True)
                print(f"[Capital Sync] history/transactions failed status={st2} info={info2}", flush=True)
                return None
            txs = txs2

        for tx in txs:
            tx_deal = tx.get("dealId")
            tx_ref = tx.get("dealReference")
            tx_related = tx.get("relatedDealId") or tx.get("relatedDealReference")
            tx_reference = tx.get("reference") or tx.get("transactionReference")
            match = False
            for x in ids:
                if (
                    str(tx_deal) == x
                    or str(tx_ref) == x
                    or str(tx_related) == x
                    or str(tx_reference) == x
                ):
                    match = True
                    break
            if not match:
                continue
            pnl = _parse_pnl(tx)
            if pnl is None:
                continue
            # Capital sometimes returns 0.0 briefly right after close; allow retries.
            if wait_for_realized and float(pnl) == 0.0 and attempt < attempts - 1:
                break
            return {
                "actual_pnl": float(pnl),
                "exit_price": _parse_exit_price(tx),
            }

        if delay_s and attempt < attempts - 1:
            # Gentle linear backoff; keeps API pressure reasonable.
            time.sleep(delay_s + (0.25 * attempt))

    # Stop-condition: not found / not ready.
    print("Error: Could not sync final data from Capital.com", flush=True)
    print(f"[Capital Sync] transaction not found yet ids={ids}", flush=True)
    return None


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
        "SELECT trade_id, deal_id, COALESCE(deal_reference,''), symbol, direction, entry_price, size, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), stop_distance, trailing_stop, "
        "COALESCE(close_sync_notified,0), COALESCE(close_sync_attempts,0), close_sync_last_try_at "
        "FROM trades WHERE chat_id=? AND status='OPEN'",
        (chat_id,),
    )
    local_open = c.fetchall()
    # If TP1 and TP2 both closed on the broker in the same cycle, process TP1 first
    # so TP1 is CLOSED in DB before TP2 final aggregates P&L.
    # local_open tuple layout:
    # (trade_id, deal_id, deal_reference, symbol, direction, entry_price, size,
    #  leg_role, parent_session, stop_distance, trailing_stop, close_sync_notified,
    #  close_sync_attempts, close_sync_last_try_at)
    # NOTE: `size` (r[6]) is numeric and must NEVER be `.strip()`'d.
    local_open = sorted(
        local_open,
        key=lambda r: (
            0 if (r[7] or "").strip() == "TP1" else 1 if (r[7] or "").strip() == "TP2" else 2,
            r[0],
        ),
    )
    local_deal_ids = {str(row[1]) for row in local_open if row[1]}

    # ── Case 1: closed externally ─────────────────────────────────────────────
    for (
        trade_id,
        deal_id,
        deal_reference,
        symbol,
        direction,
        entry_price,
        size,
        leg_role,
        parent_session,
        stop_distance,
        trailing_stop,
        close_sync_notified,
        close_sync_attempts,
        close_sync_last_try_at,
    ) in local_open:
        if deal_id and str(deal_id) not in live_deal_ids:
            # Prevent hot-loop hammering when Capital history lags.
            # Retry with cooldown and bounded attempt tracking per trade.
            try:
                attempts_i = int(close_sync_attempts or 0)
            except Exception:
                attempts_i = 0
            if attempts_i >= SYNC_MAX_ATTEMPTS:
                if attempts_i == SYNC_MAX_ATTEMPTS:
                    print(
                        f"[Capital Sync] max attempts reached trade_id={trade_id} deal_id={deal_id}",
                        flush=True,
                    )
                    c.execute(
                        "UPDATE trades SET close_sync_attempts=?, close_sync_last_error=? WHERE trade_id=?",
                        (attempts_i + 1, "max_attempts_reached", int(trade_id)),
                    )
                    conn.commit()
                continue
            if close_sync_last_try_at:
                try:
                    last_try = _dt.fromisoformat(str(close_sync_last_try_at))
                    delta = (_dt.now() - last_try).total_seconds()
                    if delta < max(1, SYNC_RETRY_COOLDOWN_SEC):
                        continue
                except Exception:
                    pass

            c.execute(
                "UPDATE trades SET close_sync_attempts=COALESCE(close_sync_attempts,0)+1, "
                "close_sync_last_try_at=? WHERE trade_id=? AND status='OPEN'",
                (_dt.now().isoformat(), int(trade_id)),
            )
            conn.commit()

            # Broker realizedPnL can be delayed right after a manual close.
            # If we read too early, we may incorrectly get 0.0 and send
            # "Breakeven" to Telegram even though the final realized P&L is > 0.
            dr = str(deal_reference or "").strip()
            final = fetch_closed_deal_final_data(
                base_url,
                headers,
                str(deal_id),
                wait_for_realized=True,
                identifiers=[dr] if dr else None,
            )
            if not final:
                # Optional UX ping while waiting for broker history sync.
                # Default is OFF to avoid notification floods.
                try:
                    if ENABLE_CLOSE_PENDING_NOTIFY and notify and int(close_sync_notified or 0) == 0:
                        lang = get_subscriber_lang(chat_id)
                        msg = (
                            "⏳ *Trade closed on platform*\n\n"
                            f"📌 Asset: *{symbol}* ({direction})\n"
                            f"🆔 Trade ID: *{int(trade_id)}*\n"
                            "Syncing final P&L from Capital.com history…"
                            if lang == "en"
                            else
                            "⏳ *تم إغلاق الصفقة على المنصة*\n\n"
                            f"📌 الأداة: *{symbol}* ({'شراء' if direction=='BUY' else 'بيع'})\n"
                            f"🆔 رقم الصفقة: *{int(trade_id)}*\n"
                            "جارٍ مزامنة الربح/الخسارة النهائية من سجل Capital.com…"
                        )
                        from bot.notifier import send_telegram_message
                        send_telegram_message(chat_id, msg)
                        c.execute(
                            "UPDATE trades SET close_sync_notified=1 WHERE trade_id=? AND status='OPEN'",
                            (int(trade_id),),
                        )
                        conn.commit()
                except Exception:
                    pass
                c.execute(
                    "UPDATE trades SET close_sync_last_error=? WHERE trade_id=? AND status='OPEN'",
                    ("transaction_not_found_or_not_realized", int(trade_id)),
                )
                conn.commit()
                continue

            pnl = float(final["actual_pnl"])
            exit_price = final.get("exit_price")
            cur = c.execute(
                "UPDATE trades SET status='CLOSED', pnl=?, actual_pnl=?, exit_price=?, closed_at=?, "
                "close_sync_last_error=NULL "
                "WHERE trade_id=? AND status='OPEN'",
                (
                    pnl,
                    pnl,
                    float(exit_price) if exit_price is not None else None,
                    _dt.now().isoformat(),
                    trade_id,
                ),
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

            # Allow callers (e.g., dashboard UI) to run reconcile silently
            # to clean up stale DB rows without spamming the user.
            #
            # NOTE: Even in maintenance mode we still send close notifications,
            # because maintenance should block NEW entries, not hide trade exits.
            if not notify:
                continue

            ep = float(entry_price or 0)
            sz = float(size or 0)
            ts = float(trailing_stop) if trailing_stop is not None else None
            sd = stop_distance
            if sd is None and ts is not None and ep:
                sd = abs(ep - ts)
            lr = (leg_role or "").strip()

            if lr == "TP1" and ps:
                # Persist milestone for reporting.
                try:
                    c.execute(
                        "UPDATE trades SET target_reached=COALESCE(target_reached,'TARGET_1_HIT') "
                        "WHERE trade_id=?",
                        (trade_id,),
                    )
                    conn.commit()
                except Exception:
                    pass
                tp2_open = c.execute(
                    "SELECT 1 FROM trades WHERE parent_session=? AND status='OPEN' "
                    "AND COALESCE(leg_role,'')='TP2' LIMIT 1",
                    (ps,),
                ).fetchone()
                send_reconcile_tp1_hit(
                    chat_id,
                    trade_id=int(trade_id),
                    symbol=symbol,
                    direction=direction,
                    entry_price=ep,
                    exit_price=float(exit_price) if exit_price is not None else None,
                    size=sz,
                    pnl=pnl_f,
                    stop_distance=sd,
                    trailing_stop=ts,
                    tp2_still_open=bool(tp2_open),
                )
            elif lr == "TP2" and ps:
                try:
                    c.execute(
                        "UPDATE trades SET target_reached=COALESCE(target_reached,'TRAILING_STOP_EXIT') "
                        "WHERE trade_id=?",
                        (trade_id,),
                    )
                    conn.commit()
                except Exception:
                    pass
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
                    trade_id=int(trade_id),
                    symbol=sym0,
                    direction=dir0,
                    entry_price=ep0,
                    exit_price=float(exit_price) if exit_price is not None else None,
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
                    trade_id=int(trade_id),
                    symbol=symbol,
                    direction=direction,
                    entry_price=ep,
                    exit_price=float(exit_price) if exit_price is not None else None,
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
        final = fetch_closed_deal_final_data(base_url, headers, str(deal_id), wait_for_realized=True)
        if not final:
            continue
        fetched = float(final["actual_pnl"])
        exit_price = final.get("exit_price")
        c.execute(
            "UPDATE trades SET pnl=?, actual_pnl=?, exit_price=? WHERE trade_id=?",
            (float(fetched), float(fetched), float(exit_price) if exit_price is not None else None, int(trade_id)),
        )
        if c.rowcount == 1:
            updated += 1

    if updated:
        conn.commit()
    conn.close()
    return updated
