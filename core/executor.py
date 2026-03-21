import requests
import sqlite3
from bot.notifier import send_telegram_message
from bot.licensing import safe_decrypt
from database.db_manager import is_maintenance_mode
from core.risk_manager import (
    can_open_trade, record_trade_result,
    calculate_position_size, STATE_MANUAL_OVERRIDE,
    check_daily_drawdown, check_rr_ratio,
)
from core.trailing_stop import (
    calculate_atr, compute_stop_candidate, advance_trailing_stop,
    is_stop_hit, get_open_trades, update_trade_stop,
    close_trade_in_db, record_open_trade,
)
from core.sync import reconcile


# ── Credentials & session ─────────────────────────────────────────────────────

def get_user_credentials(chat_id):
    """Fetch and decrypt user credentials from DB."""
    conn = sqlite3.connect('database/trading_saas.db')
    c = conn.cursor()
    c.execute(
        "SELECT api_key, api_password, is_demo, email FROM subscribers WHERE chat_id=?",
        (chat_id,)
    )
    data = c.fetchone()
    conn.close()
    if not data:
        return None
    # Decrypt stored credentials (encrypted since Step 6)
    return (safe_decrypt(data[0]), safe_decrypt(data[1]), data[2], safe_decrypt(data[3]))


def get_session(creds):
    api_key, password, is_demo, user_email = (
        str(creds[0]).strip(), str(creds[1]).strip(), creds[2], str(creds[3]).strip()
    )
    base_url = (
        "https://demo-api-capital.backend-capital.com/api/v1"
        if is_demo else
        "https://api-capital.backend-capital.com/api/v1"
    )
    headers = {
        "X-CAP-API-KEY": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    auth_res = requests.post(
        f"{base_url}/session",
        json={"identifier": user_email, "password": password},
        headers=headers,
    )
    if auth_res.status_code == 200:
        return base_url, {
            **headers,
            "CST": auth_res.headers.get("CST"),
            "X-SECURITY-TOKEN": auth_res.headers.get("X-SECURITY-TOKEN"),
        }
    # Print useful debugging info (kept simple; avoid dumping full secrets).
    try:
        err_text = (auth_res.text or "").strip()
        err_text = err_text[:400] + ("..." if len(err_text) > 400 else "")
    except Exception:
        err_text = ""
    print(
        f"[Capital Auth Failed] status={auth_res.status_code} "
        f"is_demo={is_demo} base_url={base_url}\n"
        f"response={err_text}"
    )
    return None, None


def _get_balance(base_url, headers):
    try:
        res = requests.get(f"{base_url}/accounts", headers=headers)
        if res.status_code == 200:
            return float(res.json()['accounts'][0]['balance']['balance'])
    except Exception:
        pass
    return 1000.0


def _get_current_price(base_url, headers, epic):
    """Fetch latest bid price for an instrument."""
    try:
        res = requests.get(f"{base_url}/markets/{epic}", headers=headers)
        if res.status_code == 200:
            return float(res.json().get('snapshot', {}).get('bid', 0))
    except Exception:
        pass
    return 0.0


# ── Position monitoring — trailing stop + hard fallback ───────────────────────

def monitor_and_close(chat_id):
    """
    Full monitoring cycle per user:

    1. Reconcile local DB with live broker positions (detect external closes,
       register manual trades).
    2. For each locally tracked OPEN trade:
         a. Get current price from the live position feed.
         b. Calculate ATR for the symbol.
         c. Compute new trailing stop candidate.
         d. Advance the stop (only in the trade's favour).
         e. Persist the updated stop to DB.
         f. Close the position if stop is hit.
    3. Hard fallback: close any position whose UPL breaches -3% of balance
       (safety net for gap events or ATR fetch failures).

    Open positions are NEVER affected by the Circuit Breaker / Hard Block —
    those gates only block NEW entries.
    """
    creds = get_user_credentials(chat_id)
    if not creds:
        return False
    base_url, headers = get_session(creds)
    if not headers:
        return False

    # ── Step 1: DB ↔ broker reconciliation ────────────────────────────────────
    reconcile(chat_id, base_url, headers)

    # Fetch live positions for price data and hard-stop checks
    pos_res = requests.get(f"{base_url}/positions", headers=headers)
    if pos_res.status_code != 200:
        return False

    live_positions  = pos_res.json().get('positions', [])
    balance         = _get_balance(base_url, headers)
    hard_stop_limit = -(balance * 0.03)   # -3% of balance

    # Index live positions by dealId for O(1) lookup
    live_by_id = {p['position']['dealId']: p for p in live_positions}

    closed_any = False

    # ── Step 2: ATR trailing stop logic ──────────────────────────────────────
    for trade in get_open_trades(chat_id):
        deal_id   = trade['deal_id']
        symbol    = trade['symbol']
        direction = trade['direction']

        live = live_by_id.get(deal_id)
        if not live:
            continue   # already handled by reconcile above

        # Current mid price (use bid for BUY exits, offer for SELL exits)
        current_price = float(
            live['market']['bid'] if direction == 'BUY'
            else live['market']['offer']
        )
        upl = float(live['position']['upl'])

        # Calculate ATR (try yfinance with the epic as ticker symbol)
        atr = calculate_atr(symbol)

        if atr and atr > 0:
            candidate    = compute_stop_candidate(direction, current_price, atr)
            prev_stop    = trade['trailing_stop'] or candidate  # initialise on first run
            new_stop     = advance_trailing_stop(prev_stop, candidate, direction)
            update_trade_stop(trade['trade_id'], new_stop)

            stop_triggered = is_stop_hit(current_price, new_stop, direction)
            stop_label = f"ATR trailing stop @ {new_stop:.4f}"
        else:
            # Fallback: use hard 3% portfolio limit as the stop trigger
            stop_triggered = upl <= hard_stop_limit
            new_stop = None
            stop_label = f"وقف خسارة احتياطي (3%) @ UPL={upl:.2f}"

        print(
            f"📊 [{symbol} {direction}] "
            f"السعر: {current_price:.4f} | "
            f"الوقف: {new_stop or 'N/A'} | "
            f"UPL: ${upl:.2f}"
        )

        if stop_triggered:
            del_res = requests.delete(
                f"{base_url}/positions/{deal_id}", headers=headers
            )
            if del_res.status_code == 200:
                close_trade_in_db(trade['trade_id'], upl)
                record_trade_result(chat_id, upl)

                send_telegram_message(
                    chat_id,
                    f"🔔 *إغلاق آلي — {symbol}*\n"
                    f"الاتجاه: {direction}\n"
                    f"السبب: {stop_label}\n"
                    f"{'الربح' if upl >= 0 else 'الخسارة'}: ${upl:.2f}"
                )
                closed_any = True

    return closed_any


# ── Trade execution ───────────────────────────────────────────────────────────

def place_trade_for_user(chat_id, symbol, action, confidence=75.0, stop_loss_pct=None):
    """
    Open a new position.

    Flow:
      1. Maintenance gate.
      2. Circuit Breaker / Hard Block state gate.
      3. Daily drawdown guard  (-5% → hard stop, new in v2).
      4. Duplicate position check.
      5. R:R ratio gate         (minimum 1:2, new in v2).
      6. Dynamic position sizing (1–2% risk, tightened in v2).
      7. Place order via Capital.com API.
      8. Record the initial ATR-based trailing stop in local DB.
    """
    # ── Maintenance gate ──────────────────────────────────────────────────────
    if is_maintenance_mode():
        return "🔧 System in maintenance — new entries suspended."

    # ── Circuit Breaker / Hard Block gate ─────────────────────────────────────
    allowed, reason = can_open_trade(chat_id)
    if not allowed:
        return f"🚫 {reason}"

    creds = get_user_credentials(chat_id)
    if not creds:
        msg = "❌ User not registered"
        send_telegram_message(chat_id, msg)
        return msg
    base_url, headers = get_session(creds)
    if not headers:
        msg = "❌ Capital.com authentication failed"
        send_telegram_message(chat_id, msg)
        return msg

    # ── Daily drawdown gate ───────────────────────────────────────────────────
    balance = _get_balance(base_url, headers)
    within_dd, dd_pct = check_daily_drawdown(chat_id, balance)
    if not within_dd:
        return f"🔴 Daily drawdown limit reached ({dd_pct:.1f}%) — no new entries today."

    # ── Duplicate check ───────────────────────────────────────────────────────
    pos_res = requests.get(f"{base_url}/positions", headers=headers)
    if pos_res.status_code == 200:
        for p in pos_res.json().get('positions', []):
            if symbol.upper() in p['market']['instrumentName'].upper():
                return f"⚠️ Position already open for {symbol} — monitoring."

    # ── ATR fetch (used for both RR check and initial stop) ───────────────────
    entry_price = _get_current_price(base_url, headers, symbol)
    atr         = calculate_atr(symbol)

    # ── R:R ratio gate ────────────────────────────────────────────────────────
    if atr and entry_price > 0:
        # Compute stop price from stop_loss_pct (strategy-supplied) or ATR fallback
        effective_sl_pct = stop_loss_pct if stop_loss_pct else (atr * 2.0 / entry_price)
        stop_price = (
            entry_price * (1 - effective_sl_pct) if action == 'BUY'
            else entry_price * (1 + effective_sl_pct)
        )
        rr_ok, rr_ratio = check_rr_ratio(entry_price, stop_price, action, atr)
        if not rr_ok:
            msg = (
                f"❌ R:R {rr_ratio:.1f}:1 does not meet minimum 1:2 — "
                f"setup discarded ({symbol} {action})"
            )
            send_telegram_message(chat_id, msg)
            return msg
    else:
        effective_sl_pct = stop_loss_pct or 0.01
        rr_ratio = 0.0

    # ── Position size (1–2% risk, confidence-scaled) ──────────────────────────
    size = calculate_position_size(balance, confidence, entry_price, effective_sl_pct, chat_id)

    # ── Place order ───────────────────────────────────────────────────────────
    trade_res = requests.post(
        f"{base_url}/positions",
        json={"epic": symbol, "direction": action, "size": size},
        headers=headers,
    )

    if trade_res.status_code != 200:
        msg = f"❌ Order failed ({symbol} {action}): {trade_res.text[:300]}"
        send_telegram_message(chat_id, msg)
        return msg

    deal_id = trade_res.json().get('dealId') or trade_res.json().get('dealReference', '')

    # ── Initial ATR trailing stop ─────────────────────────────────────────────
    initial_stop = None
    if atr and entry_price > 0:
        initial_stop = compute_stop_candidate(action, entry_price, atr)

    # ── Record in local DB ────────────────────────────────────────────────────
    record_open_trade(chat_id, symbol, action, entry_price, size, deal_id, initial_stop)

    # ── Notify ────────────────────────────────────────────────────────────────
    is_override  = (reason == STATE_MANUAL_OVERRIDE)
    risk_used    = 1.0 + 1.0 * ((min(max(confidence, 70), 100) - 70) / 30)
    stop_info    = f"{initial_stop:.4f}" if initial_stop else "computed next cycle"

    send_telegram_message(
        chat_id,
        f"{'⚠️ Manual Override' if is_override else '✅'} *{action} — {symbol}*\n"
        f"Size: {size} units\n"
        f"Confidence: {confidence}%  |  Risk: {risk_used:.1f}%\n"
        f"R:R ratio: {rr_ratio:.1f}:1\n"
        f"Initial ATR stop: {stop_info}"
    )
    return f"✅ Opened — size: {size} | stop: {stop_info} | RR: {rr_ratio:.1f}"
