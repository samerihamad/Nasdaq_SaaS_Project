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
from bot.notifier import send_telegram_message
from core.risk_manager import record_trade_result

DB_PATH = 'database/trading_saas.db'


def reconcile(chat_id, base_url, headers):
    """
    Sync local DB with live Capital.com /positions.
    Should be called once per monitoring cycle before any stop checks.
    """
    pos_res = requests.get(f"{base_url}/positions", headers=headers)
    if pos_res.status_code != 200:
        return

    live_positions = pos_res.json().get('positions', [])
    live_deal_ids  = {p['position']['dealId'] for p in live_positions}

    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    # Fetch locally tracked open trades
    c.execute(
        "SELECT trade_id, deal_id, symbol, direction FROM trades "
        "WHERE chat_id=? AND status='OPEN'",
        (chat_id,)
    )
    local_open = c.fetchall()
    local_deal_ids = {row[1] for row in local_open if row[1]}

    # ── Case 1: closed externally ─────────────────────────────────────────────
    for trade_id, deal_id, symbol, direction in local_open:
        if deal_id and deal_id not in live_deal_ids:
            pnl = _fetch_closed_pnl(base_url, headers, deal_id)
            from datetime import datetime as _dt
            c.execute(
                "UPDATE trades SET status='CLOSED', pnl=?, closed_at=? WHERE trade_id=?",
                (pnl, _dt.now().isoformat(), trade_id)
            )

            label  = "ربح" if pnl > 0 else "خسارة"
            send_telegram_message(
                chat_id,
                f"📋 *مزامنة — صفقة مغلقة تلقائياً*\n"
                f"الأداة: {symbol} ({direction})\n"
                f"{'الربح' if pnl > 0 else 'الخسارة'}: ${abs(pnl):.2f} ({label})\n"
                f"تم تحديث السجل المحلي."
            )
            # Feed into Circuit Breaker state machine
            record_trade_result(chat_id, pnl)

    # ── Case 2: manually opened, not tracked ─────────────────────────────────
    for p in live_positions:
        deal_id = p['position']['dealId']
        if deal_id not in local_deal_ids:
            symbol    = p['market'].get('epic', p['market'].get('instrumentName', ''))
            direction = p['position']['direction']
            entry     = float(p['position'].get('level', 0))
            size      = float(p['position'].get('size', 1))

            c.execute(
                '''INSERT INTO trades
                   (chat_id, symbol, direction, entry_price, size, deal_id, status)
                   VALUES (?, ?, ?, ?, ?, ?, 'OPEN')''',
                (chat_id, symbol, direction, entry, size, deal_id)
            )
            print(f"🔄 مزامنة: صفقة يدوية رُصدت في {symbol} ({direction}) — تم إضافتها للتتبع.")

    conn.commit()
    conn.close()


def _fetch_closed_pnl(base_url, headers, deal_id):
    """
    Attempt to retrieve the realized PnL of a closed deal from broker history.
    Returns 0.0 if the endpoint is unavailable or the deal isn't found.
    """
    try:
        res = requests.get(
            f"{base_url}/history/transactions",
            params={"dealId": deal_id},
            headers=headers
        )
        if res.status_code == 200:
            for tx in res.json().get('transactions', []):
                if tx.get('dealId') == deal_id:
                    return float(tx.get('profitAndLoss', 0))
    except Exception:
        pass
    return 0.0
