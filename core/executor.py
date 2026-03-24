import requests
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from bot.notifier import send_telegram_message
from bot.licensing import safe_decrypt
from database.db_manager import is_maintenance_mode, get_subscriber_lang
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
from core.trade_close_messages import (
    send_bot_automated_close,
    count_open_sibling_same_session,
)

# Daily per-user symbol→epic cache (and unsupported symbols) to avoid
# repeating broker lookups for every signal cycle.
_EPIC_CACHE = {}
_SESSION_CACHE = {}
SESSION_TTL_SECONDS = 45


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

    cache_key = f"{api_key}|{is_demo}|{user_email}"
    now = datetime.now(timezone.utc)
    cached = _SESSION_CACHE.get(cache_key)
    if cached and cached.get("expires_at") and cached["expires_at"] > now:
        return cached["base_url"], cached["headers"]

    auth_res = None
    # Capital auth can fail transiently (network edge / temporary backend errors).
    # Retry briefly before surfacing an authentication failure to the user.
    for attempt in range(3):
        try:
            auth_res = requests.post(
                f"{base_url}/session",
                json={"identifier": user_email, "password": password},
                headers=headers,
                timeout=20,
            )
        except Exception as exc:
            auth_res = None
            print(f"[Capital Auth Exception] attempt={attempt + 1}/3 error={exc}")
        if auth_res is not None and auth_res.status_code == 200:
            session_headers = {
                **headers,
                "CST": auth_res.headers.get("CST"),
                "X-SECURITY-TOKEN": auth_res.headers.get("X-SECURITY-TOKEN"),
            }
            _SESSION_CACHE[cache_key] = {
                "base_url": base_url,
                "headers": session_headers,
                "expires_at": now + timedelta(seconds=SESSION_TTL_SECONDS),
            }
            return base_url, session_headers
        if attempt < 2:
            time.sleep(0.7 * (attempt + 1))

    # Print useful debugging info (kept simple; avoid dumping full secrets).
    try:
        err_text = (auth_res.text or "").strip() if auth_res is not None else "no response"
        err_text = err_text[:400] + ("..." if len(err_text) > 400 else "")
        status = auth_res.status_code if auth_res is not None else "N/A"
    except Exception:
        err_text = ""
        status = "N/A"
    print(
        f"[Capital Auth Failed] status={status} "
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


def _epic_cache_key(chat_id, is_demo):
    day = datetime.utcnow().strftime('%Y-%m-%d')
    return f"{chat_id}|{'DEMO' if is_demo else 'LIVE'}|{day}"


def _get_cache_bucket(chat_id, is_demo):
    key = _epic_cache_key(chat_id, is_demo)
    if key not in _EPIC_CACHE:
        _EPIC_CACHE[key] = {"symbol_to_epic": {}, "unsupported": set()}
    return _EPIC_CACHE[key]


def _prune_old_epic_cache():
    """Keep only today's cache buckets."""
    today = datetime.utcnow().strftime('%Y-%m-%d')
    stale = [k for k in _EPIC_CACHE.keys() if not k.endswith(f"|{today}")]
    for k in stale:
        _EPIC_CACHE.pop(k, None)


def _resolve_epic(base_url, headers, symbol):
    """
    Resolve a Nasdaq ticker symbol (e.g. PAYP) to a Capital.com epic.
    Returns epic string, or None if no tradable market is found.
    """
    s = str(symbol or "").strip().upper()
    # Common ticker aliases / typo normalization
    # PAYP frequently appears in screener output while broker uses PYPL.
    symbol_aliases = {
        "PAYP": "PYPL",
    }
    s = symbol_aliases.get(s, s)
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

    def _tokens(text):
        import re
        return {t for t in re.split(r"[^A-Z0-9]+", str(text or "").upper()) if t}

    # 3) Search endpoint fallback (strict matching to avoid wrong markets like CURY.L for PURR).
    try:
        res = requests.get(
            f"{base_url}/markets",
            params={"searchTerm": s},
            headers=headers,
        )
        if res.status_code == 200:
            markets = res.json().get("markets", [])
            scored = []
            for m in markets:
                epic = str(m.get("epic", "")).upper()
                if not epic:
                    continue
                inst = str(m.get("instrumentName", "")).upper()
                market_id = str(m.get("marketId", "")).upper()
                status = str(m.get("marketStatus", "")).upper()
                country = str(m.get("countryCode", "")).upper()
                currency = str(m.get("currency", "")).upper()

                inst_tokens = _tokens(inst)
                epic_tokens = _tokens(epic)

                exact_token = (s in inst_tokens) or (s in epic_tokens)
                exact_epic = (epic == s)
                us_hint = (
                    ".US." in f".{epic}." or
                    country == "US" or
                    currency == "USD"
                )
                share_hint = market_id in ("SHARES", "SHARE")
                open_hint = status in ("TRADEABLE", "OPEN")

                # Require at least an exact token match to avoid false positives.
                if not (exact_token or exact_epic):
                    continue

                score = 0
                score += 100 if exact_epic else 0
                score += 60 if exact_token else 0
                score += 25 if us_hint else 0
                score += 10 if share_hint else 0
                score += 8 if open_hint else 0
                scored.append((score, epic))

            if scored:
                scored.sort(reverse=True)
                return scored[0][1]
    except Exception:
        pass

    return None


def _get_min_deal_size(base_url, headers, epic) -> float:
    """
    Fetch broker minimum deal size for an epic.
    Returns 1.0 if unavailable.
    """
    try:
        res = requests.get(f"{base_url}/markets/{epic}", headers=headers, timeout=20)
        if res.status_code != 200:
            return 1.0
        data = res.json() or {}
        rules = data.get("dealingRules", {}) or {}
        md = rules.get("minDealSize")
        if isinstance(md, dict):
            val = md.get("value")
        else:
            val = md
        if val is None:
            val = rules.get("minStepDistance")
        v = float(val) if val is not None else 1.0
        return max(1.0, v)
    except Exception:
        return 1.0


def resolve_epic_for_user(chat_id, symbol, base_url=None, headers=None, is_demo=None):
    """
    Resolve and cache broker epic for (chat_id, symbol) per day.
    Returns epic or None if unsupported.
    """
    _prune_old_epic_cache()
    s = str(symbol or "").strip().upper()
    if not s:
        return None

    # If caller did not provide an authenticated session, create one.
    local_session = False
    if base_url is None or headers is None or is_demo is None:
        creds = get_user_credentials(chat_id)
        if not creds:
            return None
        is_demo = bool(creds[2])
        base_url, headers = get_session(creds)
        if not headers:
            return None
        local_session = True

    bucket = _get_cache_bucket(chat_id, is_demo)
    if s in bucket["symbol_to_epic"]:
        return bucket["symbol_to_epic"][s]
    if s in bucket["unsupported"]:
        return None

    epic = _resolve_epic(base_url, headers, s)
    if epic:
        bucket["symbol_to_epic"][s] = epic
    else:
        bucket["unsupported"].add(s)

    if local_session:
        # Nothing explicit to close; tokens are per-request headers.
        pass
    return epic


def is_symbol_supported_for_user(chat_id, symbol) -> bool:
    """True if we can resolve a valid broker epic for this user today."""
    return bool(resolve_epic_for_user(chat_id, symbol))


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
                data = res.json() or {}
                # Capital sometimes returns HTTP 200 with an error payload.
                if data.get("errorCode"):
                    last_err = f"{data.get('errorCode')}: {str(data.get('message', ''))[:200]}"
                    continue
                return True, data
            last_err = (res.text or "")[:300]
        except Exception as exc:
            last_err = str(exc)
    return False, last_err or "unknown error"


def _position_has_deal(base_url, headers, deal_id: str) -> bool:
    """True if GET /positions lists this dealId (real open position)."""
    if not deal_id:
        return False
    try:
        res = requests.get(f"{base_url}/positions", headers=headers, timeout=20)
        if res.status_code != 200:
            return False
        for p in res.json().get("positions", []):
            if str(p.get("position", {}).get("dealId")) == str(deal_id):
                return True
    except Exception:
        pass
    return False


def _confirm_deal_and_visibility(
    base_url, headers, order_response: dict, timeout_sec: float = 22.0
):
    """
    Resolve a real dealId (via /confirms when needed) and ensure the position
    appears in GET /positions. Never treats dealReference alone as a valid dealId.

    Returns (ok, deal_id, error_message).
    """
    if not isinstance(order_response, dict):
        return False, "", "Invalid order response"

    ec = order_response.get("errorCode")
    if ec:
        return False, "", f"{ec}: {str(order_response.get('message', ''))[:200]}"

    import time as _time

    deadline = _time.time() + timeout_sec
    deal_id = order_response.get("dealId")
    deal_ref = order_response.get("dealReference")

    if deal_id:
        deal_id = str(deal_id)
        while _time.time() < deadline:
            if _position_has_deal(base_url, headers, deal_id):
                return True, deal_id, ""
            _time.sleep(0.35)

    # Poll /confirms for dealReference until we get dealId or rejection.
    while deal_ref and _time.time() < deadline:
        try:
            res = requests.get(
                f"{base_url}/confirms/{deal_ref}", headers=headers, timeout=20
            )
            if res.status_code == 200:
                data = res.json() or {}
                if data.get("errorCode"):
                    return (
                        False,
                        "",
                        f"{data.get('errorCode')}: {str(data.get('message', ''))[:160]}",
                    )
                did = data.get("dealId")
                if did:
                    deal_id = str(did)
                    if _position_has_deal(base_url, headers, deal_id):
                        return True, deal_id, ""
                st = str(
                    data.get("dealStatus") or data.get("status") or ""
                ).upper()
                if st in ("REJECTED", "FAILED"):
                    return False, "", f"deal rejected ({st})"
        except Exception:
            pass
        _time.sleep(0.4)

    if deal_id and _position_has_deal(base_url, headers, str(deal_id)):
        return True, str(deal_id), ""

    return (
        False,
        "",
        "Could not verify an open position on the broker (check demo vs live account, "
        "or that the instrument is enabled).",
    )


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


def _sanitize_protection_levels(action, entry_price, stop_level, target1, target2):
    """
    Ensure broker protection levels are valid and strictly ordered:
      BUY  -> stop < entry < targets
      SELL -> targets < entry < stop
    Also ensures all levels are strictly positive.
    """
    eps = max(entry_price * 0.0005, 0.01)  # 0.05% or 1 cent minimum buffer

    stop = float(stop_level or 0.0)
    t1 = float(target1 or 0.0)
    t2 = float(target2 or 0.0)

    if action == "BUY":
        if stop <= 0 or stop >= entry_price:
            stop = max(0.01, entry_price - eps)
        if t1 <= entry_price:
            t1 = entry_price + eps
        if t2 <= t1:
            t2 = t1 + eps
    else:
        if stop <= entry_price:
            stop = entry_price + eps
        if t1 <= 0 or t1 >= entry_price:
            t1 = max(0.01, entry_price - eps)
        if t2 <= 0 or t2 >= t1:
            t2 = max(0.01, t1 - eps)

    return round(stop, 6), round(t1, 6), round(t2, 6)


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
        trade_id  = trade['trade_id']
        leg_role  = trade.get('leg_role') or ''
        parent_session = trade.get('parent_session')
        size_leg  = float(trade.get('size') or 0)
        sd_stored = trade.get('stop_distance')

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
        entry_price = float(trade.get('entry_price') or current_price)
        base_stop = trade.get('trailing_stop')

        # TP1 gate:
        # Do NOT start trailing from the initial SL.
        # Only after TP1 is reached, move SL to at least breakeven, then trail.
        tp1_hit = False
        tp1_price = None
        if base_stop is not None:
            stop_dist = abs(entry_price - float(base_stop))
            if stop_dist > 0:
                if direction == 'BUY':
                    tp1_price = entry_price + stop_dist
                    tp1_hit = current_price >= tp1_price
                else:
                    tp1_price = entry_price - stop_dist
                    tp1_hit = current_price <= tp1_price

        if atr and atr > 0:
            if tp1_hit:
                candidate = compute_stop_candidate(direction, current_price, atr)
                prev_stop = float(base_stop if base_stop is not None else candidate)
                breakeven = entry_price
                if direction == 'BUY':
                    prev_stop = max(prev_stop, breakeven)
                else:
                    prev_stop = min(prev_stop, breakeven)

                new_stop = advance_trailing_stop(prev_stop, candidate, direction)
                if base_stop != new_stop:
                    update_trade_stop(trade['trade_id'], new_stop)
                    ok, info = _sync_stop_to_broker(base_url, headers, deal_id, new_stop)
                    if not ok:
                        print(f"⚠️  Broker stop sync failed [{symbol} {deal_id}]: {info}")
                stop_triggered = is_stop_hit(current_price, new_stop, direction)
                stop_label = f"ATR trailing stop @ {new_stop:.4f} (TP1 passed)"
            else:
                # Before TP1: keep initial SL unchanged.
                new_stop = float(base_stop) if base_stop is not None else None
                if new_stop is not None:
                    stop_triggered = is_stop_hit(current_price, new_stop, direction)
                    stop_label = (
                        f"Initial SL @ {new_stop:.4f} "
                        f"(waiting TP1{f' {tp1_price:.4f}' if tp1_price else ''})"
                    )
                else:
                    # Fallback only when SL is missing
                    stop_triggered = upl <= hard_stop_limit
                    stop_label = f"وقف خسارة احتياطي (3%) @ UPL={upl:.2f}"
        else:
            # ATR unavailable: respect existing SL if present, else hard fallback.
            new_stop = float(base_stop) if base_stop is not None else None
            if new_stop is not None:
                stop_triggered = is_stop_hit(current_price, new_stop, direction)
                stop_label = f"Initial SL @ {new_stop:.4f} (ATR unavailable)"
            else:
                stop_triggered = upl <= hard_stop_limit
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

                sd_eff = sd_stored
                if sd_eff is None and base_stop is not None and entry_price:
                    sd_eff = abs(float(entry_price) - float(base_stop))
                sibling_tp2_open = False
                if leg_role == "TP1" and parent_session:
                    sibling_tp2_open = (
                        count_open_sibling_same_session(
                            parent_session, trade_id, "TP2"
                        )
                        > 0
                    )
                if not is_maintenance_mode():
                    send_bot_automated_close(
                        chat_id,
                        symbol=symbol,
                        direction=direction,
                        entry_price=entry_price,
                        size=size_leg or float(live["position"].get("size") or 0),
                        pnl=upl,
                        stop_distance=sd_eff,
                        trailing_stop=float(base_stop) if base_stop is not None else None,
                        stop_label=stop_label,
                        sibling_tp2_open=sibling_tp2_open,
                        leg_role=leg_role or "SINGLE",
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
    order_epic = resolve_epic_for_user(
        chat_id, symbol, base_url=base_url, headers=headers, is_demo=bool(creds[2])
    )
    if not order_epic:
        # Symbol is not tradable on this broker account/region.
        # Skip quietly for subscribers to avoid noisy false "errors".
        msg = f"⏭️ Skipped ({symbol} {action}): symbol not available on broker"
        print(f"[BROKER SKIP] chat={chat_id} {msg}")
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

    # Capital requires valid absolute levels; sanitize defensively.
    stop_level, target1, target2 = _sanitize_protection_levels(
        action, entry_price, stop_level, target1, target2
    )

    # Split into two positions to support TP1 + TP2 on broker.
    # Capital applies one TP per position, so we split size intentionally.
    min_deal_size = _get_min_deal_size(base_url, headers, order_epic)
    required_for_two_legs = max(2.0, 2.0 * min_deal_size)
    qty_total = max(required_for_two_legs, float(size))
    # Keep integer-style execution to match current reporting semantics.
    qty_total = float(int(round(qty_total)))
    if qty_total < required_for_two_legs:
        qty_total = required_for_two_legs

    qty1 = float(int(round(qty_total / 2)))
    qty2 = float(int(round(qty_total - qty1)))
    if qty1 < min_deal_size:
        qty1 = min_deal_size
        qty2 = max(min_deal_size, qty_total - qty1)
    if qty2 < min_deal_size:
        qty2 = min_deal_size
        qty1 = max(min_deal_size, qty_total - qty2)

    qty_total = qty1 + qty2
    legs = [(qty1, target1), (qty2, target2)]
    auto_upsized = qty_total > float(size)

    opened_legs = []
    parent_session = str(uuid.uuid4())
    for idx, (leg_size, leg_target) in enumerate(legs):
        leg_role = "TP1" if idx == 0 else "TP2"
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

        payload = out if isinstance(out, dict) else {}
        ok_deal, deal_id, cerr = _confirm_deal_and_visibility(
            base_url, headers, payload
        )
        if not ok_deal or not deal_id:
            if not opened_legs:
                msg = f"❌ Order failed ({symbol} {action}): {cerr}"
                send_telegram_message(chat_id, msg)
                return msg
            send_telegram_message(
                chat_id,
                f"⚠️ {symbol}: first leg is open, second leg not confirmed on broker.\n"
                f"{cerr}\n"
                f"Please verify positions manually."
            )
            break

        opened_legs.append((leg_size, leg_target, deal_id))
        record_open_trade(
            chat_id,
            symbol,
            action,
            entry_price,
            leg_size,
            deal_id,
            stop_level,
            leg_role=leg_role,
            parent_session=parent_session,
            stop_distance=stop_dist,
        )
        ok, info = _sync_protection_to_broker(
            base_url, headers, deal_id, stop_level, leg_target
        )
        if not ok:
            print(
                f"⚠️  Broker TP/SL sync failed "
                f"[{symbol} {deal_id}] sl={stop_level:.4f} tp={leg_target:.4f} :: {info}"
            )

    if not opened_legs:
        err = f"❌ Order failed ({symbol} {action}): no verified open position on broker"
        send_telegram_message(chat_id, err)
        return err

    verified_qty = float(sum(l[0] for l in opened_legs))
    partial_only_one_leg = len(opened_legs) < len(legs)
    if partial_only_one_leg:
        total_amount = entry_price * verified_qty
        risk_amount = stop_dist * verified_qty
    else:
        total_amount = entry_price * qty_total
        risk_amount = stop_dist * qty_total

    qty_display = int(verified_qty)

    # ── Notify (localized detailed trade report) ─────────────────────────────
    is_override  = (reason == STATE_MANUAL_OVERRIDE)
    _fmt_money   = lambda v: f"${v:,.2f}"

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

    from utils.market_hours import _now_et
    now_et_str = _now_et().strftime('%Y-%m-%d %H:%M ET')

    lang = get_subscriber_lang(chat_id)
    partial_note_ar = (
        "\n⚠️ *تنبيه:* فُتح حد طلب واحد فقط على الوسيط — راجع الصفقات المفتوحة."
        if partial_only_one_leg
        else ""
    )
    partial_note_en = (
        "\n⚠️ *Note:* Only one TP leg opened on the broker — check open positions."
        if partial_only_one_leg
        else ""
    )

    if lang == 'en':
        dir_label   = 'BUY' if action == 'BUY' else 'SELL'
        override_line = "⚠️ Manual Override — New trade initiated\n\n" if is_override else ""
        msg = (
            f"{sq_color} *New Trade #{trade_index} — {symbol}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{override_line}"
            f"▶️  Direction    :  *{dir_label}*\n"
            f"💰  Entry Price  :  *{_fmt_money(entry_price)}*\n"
            f"🔢  Quantity     :  *{qty_display} shares*\n"
            f"💵  Total Value  :  *{_fmt_money(total_amount)}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔴  Stop Loss    :  *{_fmt_money(stop_level)}*\n"
            f"🎯  Target 1     :  *{_fmt_money(target1)}*  ({int(qty1)} shares — 1R)\n"
            f"🏆  Target 2     :  *{_fmt_money(target2)}*  ({int(qty2)} shares — 1.33R)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️  Risk Amount  :  *{_fmt_money(risk_amount)}*\n"
            f"🕐  Time (ET)    :  {now_et_str}"
            f"{partial_note_en}"
            f"{f'\\nℹ️  Size auto-adjusted to broker minimum for TP1/TP2: {int(qty_total)} shares' if auto_upsized else ''}"
        )
    else:
        override_line = "⚠️ تجاوز يدوي — جاري فتح صفقة جديدة\n\n" if is_override else ""
        msg = (
            f"{sq_color} *صفقة جديدة #{trade_index} — {symbol}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{override_line}"
            f"▶️  الاتجاه        :  *{dir_ar}*\n"
            f"💰  سعر الدخول    :  *{_fmt_money(entry_price)}*\n"
            f"🔢  الكمية         :  *{qty_display} سهم*\n"
            f"💵  إجمالي المبلغ  :  *{_fmt_money(total_amount)}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔴  وقف الخسارة   :  *{_fmt_money(stop_level)}*\n"
            f"🎯  الهدف 1        :  *{_fmt_money(target1)}*  ({int(qty1)} سهم — 1R)\n"
            f"🏆  الهدف 2        :  *{_fmt_money(target2)}*  ({int(qty2)} سهم — 1.33R)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️  المخاطرة       :  *{_fmt_money(risk_amount)}*\n"
            f"🕐  التوقيت (ET)   :  {now_et_str}"
            f"{partial_note_ar}"
            f"{f'\\nℹ️  تم تعديل الكمية تلقائيًا للحد الأدنى لدى الوسيط للحفاظ على الهدفين: {int(qty_total)} سهم' if auto_upsized else ''}"
        )

    send_telegram_message(chat_id, msg)

    stop_info = f"{stop_level:.4f}" if stop_level is not None else "N/A"
    return (
        f"✅ Opened — legs: {len(opened_legs)} | size: {verified_qty} | "
        f"stop: {stop_info} | RR: {rr_ratio:.1f}"
    )
