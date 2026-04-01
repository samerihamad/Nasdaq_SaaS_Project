"""
Global Watcher — NATB v2.0

Responsibilities:
  1. Iterate ALL active subscribers and run position monitoring
     (trailing stops + reconciliation) regardless of maintenance state.
  2. Detect and auto-recover orphaned trades:
     — position exists on Capital.com but is absent from local DB
       (caused by crashes, DB wipes, or manual platform entries).
  3. Alert the admin for every orphan recovered.

During maintenance mode the trading engine skips new signal processing
and delegates entirely to run_watcher().
"""

import os
import sqlite3
import requests
import traceback
from bot.notifier import send_telegram_message
from bot.licensing import safe_decrypt
from core.trailing_stop import record_open_trade

DB_PATH       = 'database/trading_saas.db'
ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID', '')


# ── Subscriber helpers ────────────────────────────────────────────────────────

def get_all_active_subscribers():
    """
    Return all subscribers who are active and have registered credentials.
    Used for position monitoring — runs regardless of trading_enabled so
    that open positions are always watched even when the user has paused trading.
    """
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT chat_id, api_key, api_password, is_demo, email "
        "FROM subscribers WHERE is_active=1 AND email IS NOT NULL"
    )
    rows = c.fetchall()
    conn.close()
    return rows


def get_trading_subscribers():
    """
    Return subscribers who have explicitly enabled their trading engine.
    Used by dispatch_signal — only users who pressed "Start Trading"
    receive new trade signals.
    """
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT chat_id, api_key, api_password, is_demo, email "
        "FROM subscribers WHERE is_active=1 AND email IS NOT NULL AND trading_enabled=1"
    )
    rows = c.fetchall()
    conn.close()
    return rows


def broadcast_to_all(message: str) -> int:
    """Send `message` to every active subscriber. Returns the count sent."""
    count = 0
    for row in get_all_active_subscribers():
        try:
            send_telegram_message(row[0], message)
            count += 1
        except Exception:
            pass
    return count


# ── Orphan detection & recovery ───────────────────────────────────────────────

def detect_and_recover_orphans(chat_id: str, base_url: str, headers: dict) -> int:
    """
    Compare live broker positions against the local trades DB for `chat_id`.
    For every position that is live but absent from DB:
      1. Fetch full trade details (epic, direction, entry price, size, stop level).
      2. Insert the trade into the local DB (status='OPEN').
      3. Send an alert to the admin.

    Returns the number of orphans recovered.
    """
    pos_res = requests.get(f"{base_url}/positions", headers=headers)
    if pos_res.status_code != 200:
        return 0

    live_positions = pos_res.json().get('positions', [])
    if not live_positions:
        return 0

    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT deal_id FROM trades WHERE chat_id=? AND status='OPEN'",
        (chat_id,)
    )
    local_deal_ids = {row[0] for row in c.fetchall() if row[0]}
    conn.close()

    recovered = 0
    for p in live_positions:
        deal_id = p['position']['dealId']
        if deal_id in local_deal_ids:
            continue  # already tracked — not an orphan

        # ── Extract full details from Capital.com response ────────────────────
        symbol      = p['market'].get('epic') or p['market'].get('instrumentName', 'UNKNOWN')
        direction   = p['position']['direction']
        entry_price = float(p['position'].get('level', 0))
        size        = float(p['position'].get('size', 1))
        stop_level  = p['position'].get('stopLevel')
        sl_init = float(stop_level) if stop_level else None
        sd_or = None
        if sl_init is not None and entry_price:
            sd_or = abs(float(entry_price) - float(sl_init))

        pos = p.get("position") or {}
        oid = pos.get("orderId") or pos.get("order_id") or p.get("orderId")
        capital_oid = str(oid).strip() if oid else None

        # ── Force-sync: register in local DB ─────────────────────────────────
        record_open_trade(
            chat_id     = chat_id,
            symbol      = symbol,
            direction   = direction,
            entry_price = entry_price,
            size        = size,
            deal_id     = deal_id,
            initial_stop= sl_init,
            capital_order_id=capital_oid,
            leg_role="SINGLE",
            parent_session=None,
            stop_distance=sd_or,
        )
        recovered += 1

        # ── Admin alert ───────────────────────────────────────────────────────
        alert = (
            f"🔍 *صفقة يتيمة — تم الاسترداد التلقائي*\n\n"
            f"المشترك: `{chat_id}`\n"
            f"الأداة: *{symbol}* ({direction})\n"
            f"سعر الدخول: `{entry_price}`\n"
            f"الحجم: `{size}`\n"
            f"وقف الخسارة المسجل: `{stop_level or 'غير محدد'}`\n"
            f"الرقم المرجعي: `{deal_id}`\n\n"
            f"✅ تم تسجيلها في قاعدة البيانات."
        )
        if ADMIN_CHAT_ID:
            send_telegram_message(ADMIN_CHAT_ID, alert)
        send_telegram_message(
            chat_id,
            f"🔄 *تزامن تلقائي*\n"
            f"تم رصد صفقة غير مسجلة في {symbol} ({direction}) "
            f"وإضافتها للمتابعة التلقائية."
        )
        print(f"🔍 استرداد صفقة يتيمة: {symbol} ({direction}) | مشترك: {chat_id}")

    return recovered


# ── Main watcher loop ─────────────────────────────────────────────────────────

def run_watcher() -> int:
    """
    Run one full pass over ALL active subscribers:
      - Call monitor_and_close() → handles trailing stops + per-user sync.
      - detect_and_recover_orphans() is called inside sync.reconcile()
        which is called by monitor_and_close(), so orphans are caught automatically.

    Used both during normal operation (once per cycle) and as the
    sole active component during maintenance mode.

    Returns total orphans recovered across all subscribers.
    """
    # Lazy import to avoid circular dependency
    from core.executor import monitor_and_close

    subscribers   = get_all_active_subscribers()
    total_orphans = 0

    for row in subscribers:
        # SQLite can return numeric types if legacy rows were inserted without
        # enforcing TEXT affinity for chat_id. Normalize to a safe string early.
        chat_id = str(row[0]).strip()
        try:
            monitor_and_close(chat_id)
        except Exception as e:
            print(f"⚠️  Watcher error for {chat_id}: {e}")
            try:
                print(traceback.format_exc())
            except Exception:
                pass

    # Standalone orphan scan (in case monitor_and_close failed or was skipped)
    for row in subscribers:
        raw_chat_id, enc_key, enc_pass, is_demo, enc_email = row
        chat_id = str(raw_chat_id).strip()
        try:
            api_key    = safe_decrypt(enc_key)
            password   = safe_decrypt(enc_pass)
            user_email = safe_decrypt(enc_email)

            base_url = (
                "https://demo-api-capital.backend-capital.com/api/v1"
                if is_demo else
                "https://api-capital.backend-capital.com/api/v1"
            )
            headers = {
                "X-CAP-API-KEY":  api_key,
                "Content-Type":   "application/json",
                "Accept":         "application/json",
            }
            auth = requests.post(
                f"{base_url}/session",
                json={"identifier": user_email, "password": password},
                headers=headers,
            )
            if auth.status_code != 200:
                continue

            headers["CST"]              = auth.headers.get("CST")
            headers["X-SECURITY-TOKEN"] = auth.headers.get("X-SECURITY-TOKEN")

            total_orphans += detect_and_recover_orphans(chat_id, base_url, headers)

        except Exception as e:
            print(f"⚠️  Orphan scan error for {chat_id}: {e}")
            try:
                print(traceback.format_exc())
            except Exception:
                pass

    return total_orphans
