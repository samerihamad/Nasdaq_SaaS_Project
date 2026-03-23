import requests
import sqlite3
from datetime import datetime
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


def _resolve_epic(base_url, headers, symbol):
    """
    Resolve a Nasdaq ticker symbol (e.g. PAYP) to a Capital.com epic.
    Returns epic string, or None if no tradable market is found.
    """
    s = str(symbol or "").strip().upper()
    if not s:
        return None

    # 1) Fast path: assume provided symbol is already an epic.
    try:
        direct = requests.get(f"{base_url}/markets/{s}", headers=headers)
        if direct.status_code == 200:
            return s
    except Exception:
        pass

    # 2) Try common US-share epic patterns.
    candidates = [f"US.{s}.CASH", f"US.{s}.CFD", f"{s}.CASH", f"{s}.CFD"]
    for epic in candidates:
        try:
            res = requests.get(f"{base_url}/markets/{epic}", headers=headers)
            if res.status_code == 200:
                return epic
        except Exception:
            continue

    # 3) Search endpoint fallback.
    try:
        res = requests.get(
            f"{base_url}/markets",
            params={"searchTerm": s},
            headers=headers,
        )
        if res.status_code == 200:
            markets = res.json().get("markets", [])
            # Prefer exact ticker token matches in instrumentName,
            # then any US stock market as fallback.
            for m in markets:
                inst = str(m.get("instrumentName", "")).upper()
                epic = m.get("epic")
                if epic and f" {s}" in f" {inst} ":
                    return epic
            for m in markets:
                epic = m.get("epic")
                if epic and str(m.get("marketId", "")).upper() == "SHARES":
                    return epic
            if markets and markets[0].get("epic"):
                return markets[0]["epic"]
    except Exception:
        pass

    return None


def _open_position_with_protection(base_url, headers, epic, action, size, stop_level, target_level):
    """
    Open one position with attached stop-loss and take-profit levels.
    Returns (ok: bool, payload_or_error: dict|str).
    """
    payloads = [
        {
            "epic": epic,
            "direction": action,
            "size": size,
            "orderType": "MARKET",
            "forceOpen": True,
            "stopLevel": stop_level,
            "profitLevel": target_level,
        },
        {
            "epic": epic,
            "direction": action,
            "size": size,
            "stopLevel": stop_level,
            "profitLevel": target_level,
        },
    ]
    last_err = ""
    for payload in payloads:
        try:
            res = requests.post(f"{base_url}/positions", json=payload, headers=headers)
            if res.status_code == 200:
                return True, res.json()
            last_err = (res.text or "")[:300]
        except Exception as exc:
            last_err = str(exc)
    return False, last_err or "unknown error"


def _resolve_confirmed_deal_id(base_url, headers, order_response):
    """
    Resolve a real dealId from order response.
    Capital may return dealReference first, while /positions uses dealId.
    """
    deal_id = order_response.get("dealId")
    if deal_id:
        return str(deal_id)

    deal_ref = order_response.get("dealReference")
    if not deal_ref:
        return ""

    # Confirmation can lag briefly after order placement.
    import time as _time
    for _ in range(5):
        try:
            res = requests.get(f"{base_url}/confirms/{deal_ref}", headers=headers)
            if res.status_code == 200:
                data = res.json() or {}
                did = data.get("dealId")
                if did:
                    return str(did)
        except Exception:
            pass
        _time.sleep(0.4)

    return str(deal_ref)


def _sync_stop_to_broker(base_url, headers, deal_id, stop_level):
    """
    Push updated trailing-stop level to Capital for an open position.
    Returns (ok: bool, info: str).
    """
    if not deal_id:
        return False, "missing deal_id"

    payloads = [
        {"stopLevel": stop_level},
        {"stopLevel": stop_level, "trailingStop": False},
    ]
    last_err = ""
    for payload in payloads:
        try:
            res = requests.put(
                f"{base_url}/positions/{deal_id}",
                json=payload,
                headers=headers,
            )
            if res.status_code == 200:
                return True, "ok"
            last_err = (res.text or "")[:300]
        except Exception as exc:
            last_err = str(exc)
    return False, last_err or "unknown error"


def _sync_protection_to_broker(base_url, headers, deal_id, stop_level, profit_level):
    """
    Ensure both SL and TP are set on broker for a live position.
    Returns (ok: bool, info: str).
    """
    if not deal_id:
        return False, "missing deal_id"

    payloads = [
        {"stopLevel": stop_level, "profitLevel": profit_level},
        {"stopLevel": stop_level, "profitLevel": profit_level, "trailingStop": False},
        {"profitLevel": profit_level},
    ]
    last_err = ""
    for payload in payloads:
        try:
            res = requests.put(
                f"{base_url}/positions/{deal_id}",
                json=payload,
                headers=headers,
            )
            if res.status_code == 200:
                return True, "ok"
            last_err = (res.text or "")[:300]
        except Exception as exc:
            last_err = str(exc)
    return False, last_err or "unknown error"


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

    # Index live positions by normalized dealId for O(1) lookup.
    live_by_id = {
        str(p.get('position', {}).get('dealId')): p
        for p in live_positions
        if p.get('position', {}).get('dealId') is not None
    }

    closed_any = False

    # ── Step 2: ATR trailing stop logic ──────────────────────────────────────
    for trade in get_open_trades(chat_id):
        deal_id   = trade['deal_id']
        symbol    = trade['symbol']
        direction = trade['direction']

        live = live_by_id.get(str(deal_id))
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
            # Keep broker-side SL synced with local trailing stop.
            if prev_stop != new_stop:
                ok, info = _sync_stop_to_broker(base_url, headers, deal_id, new_stop)
                if not ok:
                    print(f"⚠️  Broker stop sync failed [{symbol} {deal_id}]: {info}")

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

def place_trade_for_user(chat_id, symbol, action, confidence=75.0, stop_loss_pct=None, strategy_label=None):
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

    # ── Resolve broker epic from scanner ticker ────────────────────────────────
    order_epic = _resolve_epic(base_url, headers, symbol)
    if not order_epic:
        msg = f"❌ Order failed ({symbol} {action}): unable to resolve epic on broker"
        send_telegram_message(chat_id, msg)
        return msg

    # ── Duplicate check (epic-first, robust) ─────────────────────────────────
    pos_res = requests.get(f"{base_url}/positions", headers=headers)
    if pos_res.status_code == 200:
        for p in pos_res.json().get('positions', []):
            m = p.get('market', {})
            live_epic = str(m.get('epic', '')).upper()
            live_name = str(m.get('instrumentName', '')).upper()
            if live_epic == str(order_epic).upper() or symbol.upper() in live_name:
                return (
                    f"⚠️ Position already open for {symbol} "
                    f"({order_epic}) — monitoring."
                )

    # ── ATR fetch (used for both RR check and initial stop) ───────────────────
    entry_price = _get_current_price(base_url, headers, order_epic)
    atr         = calculate_atr(symbol)

    # ── R:R ratio gate ────────────────────────────────────────────────────────
    if atr and entry_price > 0:
        # Compute stop price from stop_loss_pct (strategy-supplied) or ATR fallback
        effective_sl_pct = stop_loss_pct if stop_loss_pct else (atr * 2.0 / entry_price)
        stop_price = (
            entry_price * (1 - effective_sl_pct) if action == 'BUY'
            else entry_price * (1 + effective_sl_pct)
        )
        rr_ok, rr_ratio, rr_reason = check_rr_ratio(entry_price, stop_price, action, atr)
        if not rr_ok:
            if rr_reason == "target_beyond_atr_limit":
                msg = (
                    f"❌ R:R {rr_ratio:.1f}:1 rejected — target not achievable "
                    f"within ATR limit ({symbol} {action})"
                )
            else:
                msg = (
                    f"❌ R:R {rr_ratio:.1f}:1 does not meet minimum 1:2 — "
                    f"setup discarded ({symbol} {action})"
                )
            send_telegram_message(chat_id, msg)
            return msg
    else:
        effective_sl_pct = stop_loss_pct or 0.01
        rr_ratio = 0.0
        stop_price = (
            entry_price * (1 - effective_sl_pct) if action == 'BUY'
            else entry_price * (1 + effective_sl_pct)
        )

    # ── Position size (1–2% risk, confidence-scaled) ──────────────────────────
    size = calculate_position_size(balance, confidence, entry_price, effective_sl_pct, chat_id)
    # Protection levels used BOTH for broker order and Telegram report.
    # Use strategy stop when available, otherwise ATR fallback.
    if atr and entry_price > 0:
        initial_stop = compute_stop_candidate(action, entry_price, atr)
    else:
        initial_stop = stop_price

    stop_level = initial_stop if initial_stop is not None else stop_price
    stop_dist  = abs(entry_price - stop_level)
    if stop_dist <= 0:
        msg = f"❌ Order failed ({symbol} {action}): invalid stop distance"
        send_telegram_message(chat_id, msg)
        return msg

    if action == 'BUY':
        target1 = entry_price + stop_dist * 1.00
        target2 = entry_price + stop_dist * 1.33
        dir_ar   = 'شراء'
        sq_color = '🟩'
    else:
        target1 = entry_price - stop_dist * 1.00
        target2 = entry_price - stop_dist * 1.33
        dir_ar   = 'بيع'
        sq_color = '🟥'

    # Split into two positions to support TP1 + TP2 on broker.
    # Capital applies one TP per position, so we split size intentionally.
    qty_total = max(1, int(round(size)))
    qty1 = max(1, qty_total // 2)
    qty2 = qty_total - qty1
    if qty2 < 1:
        qty2 = 0

    legs = [(qty1, target1)]
    if qty2 > 0:
        legs.append((qty2, target2))

    opened_legs = []
    for leg_size, leg_target in legs:
        ok, out = _open_position_with_protection(
            base_url, headers, order_epic, action, leg_size, stop_level, leg_target
        )
        if not ok:
            # If first leg fails, abort immediately. If second fails, keep first
            # and warn user to avoid hidden exposure.
            if not opened_legs:
                msg = f"❌ Order failed ({symbol} {action}): {out}"
                send_telegram_message(chat_id, msg)
                return msg
            send_telegram_message(
                chat_id,
                f"⚠️ {symbol}: first leg opened, second TP leg failed.\n"
                f"Please review position manually.\n"
                f"Error: {out}"
            )
            break

        deal_id = _resolve_confirmed_deal_id(base_url, headers, out)
        opened_legs.append((leg_size, leg_target, deal_id))
        record_open_trade(chat_id, symbol, action, entry_price, leg_size, deal_id, stop_level)
        ok, info = _sync_protection_to_broker(
            base_url, headers, deal_id, stop_level, leg_target
        )
        if not ok:
            print(
                f"⚠️  Broker TP/SL sync failed "
                f"[{symbol} {deal_id}] sl={stop_level:.4f} tp={leg_target:.4f} :: {info}"
            )

    # ── Notify (localized detailed trade report) ─────────────────────────────
    is_override  = (reason == STATE_MANUAL_OVERRIDE)
    _fmt_money = lambda v: f"${v:,.2f}"

    total_amount = entry_price * qty_total
    risk_amount  = stop_dist * qty_total

    # Daily trade counter (for the "#N" label).
    utc_today = datetime.utcnow().date().isoformat()
    conn = sqlite3.connect('database/trading_saas.db')
    c = conn.cursor()
    c.execute(
        "SELECT COUNT(*) FROM trades WHERE chat_id=? AND DATE(opened_at)=?",
        (chat_id, utc_today),
    )
    row = c.fetchone()
    trade_index = row[0] if row else 0
    conn.close()

    # Strategy label mapping (main.py passes best_label into this function).
    strategy_map = {
        'MeanRev': 'Mean Reversion',
        'MeanReversion': 'Mean Reversion',
        'Momentum': 'Momentum',
        'RF': 'RF',
    }
    strategy_name = strategy_map.get(strategy_label or '', None) or (strategy_label or 'Mean Reversion')

    from utils.market_hours import _now_et
    now_et_str = _now_et().strftime('%Y-%m-%d %H:%M %Z')

    override_line = "⚠️ تجاوز يدوي — جاري فتح صفقة جديدة\n" if is_override else ""
    header = (
        f"🇦🇪 العربية\n"
        f"{sq_color} {symbol}  اسم السهم\n"
        f"📅 {now_et_str}\n"
        f"------------------------------\n"
        f"{override_line}"
    )

    msg = (
        f"{header}"
        f"🔔 صفقة جديدة #{trade_index} 🔔\n"
        f"📊 الاستراتيجية  : {strategy_name}\n"
        f"▶️  الاتجاه       : {dir_ar}\n"
        f"💰 الدخول        : {_fmt_money(entry_price)}\n"
        f"🔢 الكمية         : {qty_total} سهم\n"
        f"💵 إجمالي المبلغ : {_fmt_money(total_amount)}\n"
        f"🔴 وقف الخسارة   : {_fmt_money(stop_level)}\n"
        f"----------\n"
        f"🎯 الهدف 1 ({qty1} سهم) : {_fmt_money(target1)} (1R)\n"
        f"🏆 الهدف 2 ({qty2} سهم) : {_fmt_money(target2)} (1.33R)\n"
        f"---------\n"
        f"⚠️  المخاطرة      : {_fmt_money(risk_amount)}"
    )

    send_telegram_message(chat_id, msg)

    stop_info = f"{stop_level:.4f}" if stop_level is not None else "N/A"
    return f"✅ Opened — legs: {len(opened_legs)} | size: {qty_total} | stop: {stop_info} | RR: {rr_ratio:.1f}"
