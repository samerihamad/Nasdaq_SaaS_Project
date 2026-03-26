import requests
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from bot.notifier import send_telegram_message
from bot.licensing import safe_decrypt
from config import (
    TP1_PCT,
    TP2_PCT,
    TP1_SPLIT_PCT,
    TP2_SPLIT_PCT,
    TP2_MIN_DISTANCE_BUFFER_MULT,
    BE_LOCK_BUFFER_PCT,
)
from database.db_manager import is_maintenance_mode, get_subscriber_lang
from core.risk_manager import (
    can_open_trade,
    calculate_position_size, STATE_MANUAL_OVERRIDE,
    check_daily_drawdown, check_rr_ratio,
)
from core.trade_session_finalize import after_trade_leg_closed
from core.trailing_stop import (
    calculate_atr, compute_stop_candidate, advance_trailing_stop,
    is_stop_hit, get_open_trades, update_trade_stop, update_trade_target_reached,
    close_trade_in_db, record_open_trade,
)
from core.sync import reconcile
from core.sync import fetch_closed_deal_final_data
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


def _get_min_stop_profit_distance(base_url, headers, epic) -> float | None:
    """
    Fetch broker minimum stop/profit distance for an epic (in absolute price units).

    Capital.com dealing rules vary by instrument/region. We try multiple known keys.
    Returns None if unavailable.
    """
    try:
        res = requests.get(f"{base_url}/markets/{epic}", headers=headers, timeout=20)
        if res.status_code != 200:
            return None
        data = res.json() or {}
        rules = data.get("dealingRules", {}) or {}

        # Known keys seen in Capital payloads (may be dicts with "value")
        candidates = (
            "minStopOrProfitDistance",
            "minStopOrLimitDistance",
            "minStopDistance",
            "minLimitDistance",
        )
        for k in candidates:
            v = rules.get(k)
            if isinstance(v, dict):
                v = v.get("value")
            if v is None:
                continue
            try:
                f = float(v)
                if f > 0:
                    return f
            except Exception:
                continue
    except Exception:
        return None
    return None


def _get_max_stop_profit_distance(base_url, headers, epic) -> float | None:
    """
    Fetch broker maximum stop/profit distance for an epic (absolute price units).
    Used to avoid: error.invalid.stoploss.maxvalue
    Returns None if unavailable.
    """
    try:
        res = requests.get(f"{base_url}/markets/{epic}", headers=headers, timeout=20)
        if res.status_code != 200:
            return None
        data = res.json() or {}
        rules = data.get("dealingRules", {}) or {}
        candidates = (
            "maxStopOrProfitDistance",
            "maxStopOrLimitDistance",
            "maxStopDistance",
            "maxLimitDistance",
        )
        for k in candidates:
            v = rules.get(k)
            if isinstance(v, dict):
                v = v.get("value")
            if v is None:
                continue
            try:
                f = float(v)
                if f > 0:
                    return f
            except Exception:
                continue
    except Exception:
        return None
    return None


def _cap_stop_to_max_distance(
    action: str,
    entry_price: float,
    stop_level: float,
    max_dist: float | None,
) -> tuple[float, bool]:
    """
    If broker enforces a maximum stop distance, cap stop to that distance.
    Returns (new_stop_level, adjusted_flag).
    """
    if max_dist is None or max_dist <= 0 or entry_price <= 0:
        return stop_level, False
    d = abs(float(entry_price) - float(stop_level))
    if d <= float(max_dist):
        return stop_level, False
    md = float(max_dist)
    if action == "BUY":
        return float(entry_price) - md, True
    return float(entry_price) + md, True


def _market_tradeability(base_url: str, headers: dict, epic: str) -> tuple[bool, str]:
    """
    Check if an instrument is currently tradable on Capital.com.
    Returns (is_tradeable, status_string).
    """
    if not epic:
        return False, "UNKNOWN"
    try:
        res = requests.get(f"{base_url}/markets/{epic}", headers=headers, timeout=20)
        if res.status_code != 200:
            return False, f"HTTP_{res.status_code}"
        data = res.json() or {}
        status = str(data.get("marketStatus") or data.get("snapshot", {}).get("marketStatus") or "").upper()
        if not status:
            status = str((data.get("instrument") or {}).get("marketStatus") or "").upper()
        return status in ("TRADEABLE", "OPEN"), (status or "UNKNOWN")
    except Exception:
        return False, "ERROR"


def _split_qty_70_30(*, qty_total: float, min_deal_size: float) -> tuple[bool, float, float, str]:
    """
    Compute a robust 70/30 split and enforce broker min lot size.
    Stop-condition: if either leg is below min_deal_size, caller must abort.
    """
    try:
        q = int(round(float(qty_total)))
    except Exception:
        return False, 0.0, 0.0, "invalid total quantity"
    if q <= 0:
        return False, 0.0, 0.0, "total quantity <= 0"

    md = float(min_deal_size or 1.0)
    q1 = int(q * float(TP1_SPLIT_PCT))
    q1 = max(0, q1)
    q2 = int(q - q1)
    if q1 <= 0 or q2 <= 0:
        return False, float(q1), float(q2), "quantity split produced zero-size leg"
    if float(q1) < md or float(q2) < md:
        return (
            False,
            float(q1),
            float(q2),
            f"split below broker minimum lot size (min={md}, tp1={q1}, tp2={q2})",
        )
    return True, float(q1), float(q2), ""


def _delete_position(base_url: str, headers: dict, deal_id: str) -> tuple[bool, str]:
    """Attempt to close an open broker position by dealId."""
    if not deal_id:
        return False, "missing deal_id"
    try:
        res = requests.delete(f"{base_url}/positions/{deal_id}", headers=headers, timeout=20)
        if res.status_code == 200:
            return True, "ok"
        t = (res.text or "").strip()
        t = t[:240] + ("..." if len(t) > 240 else "")
        return False, f"HTTP_{res.status_code} {t}"
    except Exception as exc:
        return False, str(exc)


def _apply_min_distance_to_protection(
    action: str,
    entry_price: float,
    stop_level: float,
    profit_level: float | None,
    min_dist: float | None,
) -> tuple[float, float | None]:
    """
    Ensure stop/profit are at least `min_dist` away from entry.
    Adjusts levels outward only (more conservative), preserving direction ordering.
    """
    if min_dist is None or min_dist <= 0 or entry_price <= 0:
        return stop_level, profit_level
    md = float(min_dist)
    if action == "BUY":
        stop_level = min(float(stop_level), float(entry_price) - md)
        if profit_level is not None:
            profit_level = max(float(profit_level), float(entry_price) + md)
    else:
        stop_level = max(float(stop_level), float(entry_price) + md)
        if profit_level is not None:
            profit_level = min(float(profit_level), float(entry_price) - md)
    return stop_level, profit_level


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
    # If target_level is None, we omit profitLevel so the broker won't close
    # the TP2 leg at a fixed price; trailing/SL management will decide later.
    payloads = []
    base = {
        "epic": epic,
        "direction": action,
        "size": size,
        "orderType": "MARKET",
        "forceOpen": True,
        "stopLevel": stop_level,
    }
    base2 = {
        "epic": epic,
        "direction": action,
        "size": size,
        "orderType": "MARKET",
        "forceOpen": True,
        "stopLevel": stop_level,
    }
    if target_level is not None:
        base["profitLevel"] = target_level
        base2["profitLevel"] = target_level
    payloads = [base, base2]
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


def _deal_id_from_position_row(p: dict) -> str | None:
    """Capital payloads may nest dealId under position or at top level."""
    if not isinstance(p, dict):
        return None
    pos = p.get("position") or {}
    did = pos.get("dealId") if isinstance(pos, dict) else None
    if did is None:
        did = p.get("dealId")
    return str(did) if did is not None else None


def _position_has_deal(base_url, headers, deal_id: str) -> bool:
    """True if GET /positions lists this dealId (real open position)."""
    if not deal_id:
        return False
    try:
        res = requests.get(f"{base_url}/positions", headers=headers, timeout=20)
        if res.status_code != 200:
            return False
        for p in res.json().get("positions", []):
            row_id = _deal_id_from_position_row(p)
            if row_id and str(row_id) == str(deal_id):
                return True
    except Exception:
        pass
    return False


def _position_get_by_deal_id(base_url, headers, deal_id: str) -> bool:
    """Some regions return 200 on GET /positions/{dealId} before the list updates."""
    if not deal_id:
        return False
    try:
        res = requests.get(
            f"{base_url}/positions/{deal_id}", headers=headers, timeout=15
        )
        if res.status_code == 200:
            data = res.json() or {}
            if data.get("errorCode"):
                return False
            return bool(
                _deal_id_from_position_row(data)
                or (data.get("position") or {}).get("dealId")
            )
    except Exception:
        pass
    return False


def _epic_last_token(ep: str) -> str:
    """Compare ADMA, ADMA.US, NASDAQ:ADMA, etc."""
    e = str(ep or "").upper().strip()
    return e.split(".")[-1].split(":")[-1]


def _find_open_deal_by_epic_size(
    base_url,
    headers,
    epic: str,
    direction: str,
    leg_size: float,
    *,
    exclude_deal_ids: set[str] | None = None,
) -> str | None:
    """
    Last-resort: match one open row by epic + direction + size (Capital list lag).
    Returns dealId only if exactly one row matches (avoids ambiguity).
    """
    if not epic or not direction or leg_size is None:
        return None
    try:
        res = requests.get(f"{base_url}/positions", headers=headers, timeout=20)
        if res.status_code != 200:
            return None
        want_tok = _epic_last_token(epic)
        ex = {str(d).strip() for d in (exclude_deal_ids or set()) if str(d).strip()}
        matches: list[str] = []
        for p in res.json().get("positions", []):
            m = p.get("market") or {}
            pos = p.get("position") or {}
            row_epic = str(m.get("epic", "") or "").upper()
            if not row_epic:
                continue
            row_tok = _epic_last_token(row_epic)
            epic_ok = want_tok == row_tok or want_tok in row_epic or row_tok in epic
            if not epic_ok:
                continue
            if str(pos.get("direction")) != str(direction):
                continue
            try:
                sz = float(pos.get("size") or 0)
            except (TypeError, ValueError):
                continue
            if abs(sz - float(leg_size)) > 0.51:
                continue
            did = _deal_id_from_position_row(p)
            if did:
                if did in ex:
                    continue
                matches.append(did)
        if len(matches) == 1:
            return matches[0]
    except Exception:
        pass
    return None


def _confirm_deal_and_visibility(
    base_url,
    headers,
    order_response: dict,
    timeout_sec: float = 45.0,
    *,
    order_epic: str | None = None,
    direction: str | None = None,
    leg_size: float | None = None,
    exclude_deal_ids: set[str] | None = None,
):
    """
    Resolve a real dealId (via /confirms when needed) and ensure the position
    exists on the broker (list, GET-by-id, or epic/size match).

    Returns (ok, deal_id, deal_reference, error_message).
    """
    if not isinstance(order_response, dict):
        return False, "", "", "Invalid order response"

    ec = order_response.get("errorCode")
    if ec:
        return False, "", "", f"{ec}: {str(order_response.get('message', ''))[:200]}"

    import time as _time

    deadline = _time.time() + timeout_sec
    deal_id = order_response.get("dealId")
    deal_ref = order_response.get("dealReference")
    deal_ref_str = str(deal_ref).strip() if deal_ref is not None else ""

    def _visible(did: str) -> bool:
        if not did:
            return False
        if _position_has_deal(base_url, headers, str(did)):
            return True
        return _position_get_by_deal_id(base_url, headers, str(did))

    if deal_id:
        deal_id = str(deal_id)
        while _time.time() < deadline:
            if _visible(deal_id):
                return True, deal_id, deal_ref_str, ""
            _time.sleep(0.45)

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
                        deal_ref_str,
                        f"{data.get('errorCode')}: {str(data.get('message', ''))[:160]}",
                    )
                did = data.get("dealId")
                if did:
                    deal_id = str(did)
                    if _visible(deal_id):
                        return True, deal_id, deal_ref_str, ""
                st = str(
                    data.get("dealStatus") or data.get("status") or ""
                ).upper()
                if st in ("REJECTED", "FAILED"):
                    # Broker usually includes a human-readable rejection reason.
                    # Expose it so Telegram shows the actual cause instead of a generic label.
                    msg = (
                        data.get("message")
                        or data.get("errorMessage")
                        or data.get("reason")
                        or data.get("rejectionReason")
                        or data.get("dealError")
                        or ""
                    )
                    msg = str(msg).strip()
                    err = f"deal rejected ({st})"
                    if msg:
                        err += f": {msg[:200]}"
                    return False, "", deal_ref_str, err
        except Exception:
            pass
        _time.sleep(0.45)

    if deal_id and _visible(str(deal_id)):
        return True, str(deal_id), deal_ref_str, ""

    # Epic + direction + size (single match) when list/API lags after a 200 POST.
    if order_epic and direction and leg_size is not None:
        alt = _find_open_deal_by_epic_size(
            base_url,
            headers,
            order_epic,
            direction,
            float(leg_size),
            exclude_deal_ids=exclude_deal_ids,
        )
        if alt:
            if deal_id and str(deal_id) != str(alt):
                print(
                    f"[VERIFY] Using epic/size match dealId={alt} (POST had {deal_id})"
                )
            return True, str(alt), deal_ref_str, ""

    return (
        False,
        "",
        deal_ref_str,
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

        # Fixed TP milestones (entry-relative):
        # - TP1  = entry +/- 1%
        # - TP2  = entry +/- 1.5%
        # Trailing starts only after TP2 is hit.
        tp1_price = (
            entry_price * (1 + TP1_PCT)
            if direction == "BUY"
            else entry_price * (1 - TP1_PCT)
        )
        tp2_price = (
            entry_price * (1 + TP2_PCT)
            if direction == "BUY"
            else entry_price * (1 - TP2_PCT)
        )
        tp1_hit = (
            current_price >= tp1_price
            if direction == "BUY"
            else current_price <= tp1_price
        )
        tp2_hit = (
            current_price >= tp2_price
            if direction == "BUY"
            else current_price <= tp2_price
        )

        if atr and atr > 0:
            # We only apply "lock after TP1" and "trailing after TP2"
            # to the TP2 leg, because TP1 leg is closed by broker at TP1.
            manage_leg = (leg_role or "").strip().upper() == "TP2"

            if tp2_hit and manage_leg:
                # Persist milestone once TP2 threshold is first reached.
                try:
                    if (trade.get("target_reached") or "").strip() != "TARGET_2_HIT":
                        update_trade_target_reached(trade_id, "TARGET_2_HIT")
                except Exception:
                    pass
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
                stop_label = f"ATR trailing stop @ {new_stop:.4f} (TP2 passed)"
            elif tp1_hit and manage_leg:
                # Persist milestone once TP1 threshold is first reached (for TP2 leg).
                try:
                    if (trade.get("target_reached") or "").strip() not in ("TARGET_1_HIT", "TARGET_2_HIT"):
                        update_trade_target_reached(trade_id, "TARGET_1_HIT")
                except Exception:
                    pass
                # After TP1: lock TP2 leg beyond breakeven.
                # This prevents the trade from reverting back to entry.
                breakeven_lock = (
                    entry_price * (1 + BE_LOCK_BUFFER_PCT)
                    if direction == "BUY"
                    else entry_price * (1 - BE_LOCK_BUFFER_PCT)
                )
                if base_stop is not None:
                    # Ratchet in the profitable direction only.
                    new_stop = (
                        max(float(base_stop), breakeven_lock)
                        if direction == "BUY"
                        else min(float(base_stop), breakeven_lock)
                    )
                else:
                    new_stop = breakeven_lock

                if base_stop != new_stop:
                    update_trade_stop(trade['trade_id'], new_stop)
                    ok, info = _sync_stop_to_broker(base_url, headers, deal_id, new_stop)
                    if not ok:
                        print(f"⚠️  Broker stop sync failed [{symbol} {deal_id}]: {info}")
                stop_triggered = is_stop_hit(current_price, new_stop, direction)
                stop_label = f"SL locked @ {new_stop:.4f} (TP1 passed)"
            else:
                # Before TP1: keep initial SL unchanged.
                new_stop = float(base_stop) if base_stop is not None else None
                if new_stop is not None:
                    stop_triggered = is_stop_hit(current_price, new_stop, direction)
                    stop_label = (
                        f"Initial SL @ {new_stop:.4f} "
                        f"(waiting TP1 {tp1_price:.4f} / TP2 {tp2_price:.4f})"
                    )
                else:
                    # Fallback only when SL is missing
                    stop_triggered = upl <= hard_stop_limit
                    stop_label = f"وقف خسارة احتياطي (3%) @ UPL={upl:.2f}"
        else:
            # ATR unavailable: respect existing SL if present, else hard fallback.
            if base_stop is not None:
                manage_leg = (leg_role or "").strip().upper() == "TP2"
                new_stop = float(base_stop)
                if manage_leg and tp1_hit:
                    breakeven_lock = (
                        entry_price * (1 + BE_LOCK_BUFFER_PCT)
                        if direction == "BUY"
                        else entry_price * (1 - BE_LOCK_BUFFER_PCT)
                    )
                    # Ratchet only in profitable direction.
                    new_stop = (
                        max(new_stop, breakeven_lock)
                        if direction == "BUY"
                        else min(new_stop, breakeven_lock)
                    )
                    if float(base_stop) != new_stop:
                        update_trade_stop(trade['trade_id'], new_stop)
                        ok, info = _sync_stop_to_broker(base_url, headers, deal_id, new_stop)
                        if not ok:
                            print(f"⚠️  Broker stop sync failed [{symbol} {deal_id}]: {info}")

                stop_triggered = is_stop_hit(current_price, new_stop, direction)
                stop_label = (
                    f"Initial SL @ {new_stop:.4f} "
                    f"(ATR unavailable, tp1_hit={tp1_hit})"
                )
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
                # Capture dealReference from close response when present (Capital often keys history by it).
                try:
                    close_payload = del_res.json() or {}
                except Exception:
                    close_payload = {}
                deal_ref = (
                    close_payload.get("dealReference")
                    or close_payload.get("dealRef")
                    or close_payload.get("reference")
                )
                # Broker-truth sync (realized PnL + exit price) MUST happen after close.
                final = fetch_closed_deal_final_data(
                    base_url,
                    headers,
                    str(deal_id),
                    wait_for_realized=True,
                    identifiers=[str(deal_ref)] if deal_ref else None,
                )
                if not final:
                    # fetch_closed_deal_final_data already logged the root cause details.
                    # Leave DB row OPEN; reconcile() will retry next cycle.
                    continue

                actual_pnl = float(final["actual_pnl"])
                exit_price = final.get("exit_price")

                # Derive final label for reporting (milestone vs exit condition).
                leg = (leg_role or "").strip().upper()
                if leg == "TP2" and ("trailing" in str(stop_label).lower() or "tp2 passed" in str(stop_label).lower()):
                    target_label = "TRAILING_STOP_EXIT"
                elif leg == "TP1" and tp1_hit:
                    target_label = "TARGET_1_HIT"
                elif leg == "TP2" and tp2_hit:
                    target_label = "TARGET_2_HIT"
                elif leg == "TP2" and tp1_hit:
                    target_label = "STOP_AFTER_TP1"
                else:
                    target_label = "STOP_LOSS"

                close_trade_in_db(
                    int(trade['trade_id']),
                    actual_pnl=actual_pnl,
                    exit_price=float(exit_price) if exit_price is not None else None,
                    target_reached=target_label,
                    close_reason=stop_label,
                )
                after_trade_leg_closed(chat_id, parent_session, actual_pnl)

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
                # Even in maintenance mode we still send close notifications,
                # because maintenance should block NEW entries, not hide trade exits.
                send_bot_automated_close(
                    chat_id,
                    trade_id=int(trade_id),
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    exit_price=float(exit_price) if exit_price is not None else None,
                    size=size_leg or float(live["position"].get("size") or 0),
                    pnl=actual_pnl,
                    stop_distance=sd_eff,
                    trailing_stop=float(base_stop) if base_stop is not None else None,
                    stop_label=stop_label,
                    sibling_tp2_open=sibling_tp2_open,
                    leg_role=leg_role or "SINGLE",
                    target_reached=target_label,
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

    lang = get_subscriber_lang(chat_id)

    # ── Session Gatekeeper (UTC) — do not call broker outside window ─────────
    try:
        from utils.market_hours import is_within_us_cash_session_utc
        ok_sess, sess_reason = is_within_us_cash_session_utc()
    except Exception:
        ok_sess, sess_reason = True, "OK"
    if not ok_sess:
        if lang == "en":
            msg = f"⏭️ Skipped: Outside Market Hours ({sess_reason}) — {symbol}"
        else:
            msg = f"⏭️ تم التخطي: خارج ساعات السوق ({sess_reason}) — {symbol}"
        print(msg)
        if not is_maintenance_mode():
            send_telegram_message(chat_id, msg)
        return msg

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

    # ── Market hours / tradability gate (before opening ANY leg) ─────────────
    is_tradeable, m_status = _market_tradeability(base_url, headers, order_epic)
    if not is_tradeable:
        print(f"Trade skipped: Market Closed for {symbol} ({order_epic}) status={m_status}")
        if lang == "en":
            return f"⏭️ Trade skipped: Market Closed for {symbol} ({m_status})"
        return f"⏭️ تم التخطي: السوق مغلق لـ {symbol} ({m_status})"

    # ── R:R ratio gate ────────────────────────────────────────────────────────
    if atr and entry_price > 0:
        # Compute stop price from stop_loss_pct (strategy-supplied) or ATR fallback
        effective_sl_pct = (
            stop_loss_pct
            if stop_loss_pct is not None
            else (atr * 2.0 / entry_price)
        )
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
        effective_sl_pct = stop_loss_pct if stop_loss_pct is not None else 0.01
        rr_ratio = 0.0
        stop_price = (
            entry_price * (1 - effective_sl_pct) if action == 'BUY'
            else entry_price * (1 + effective_sl_pct)
        )

    # ── Position size (1–2% risk, confidence-scaled) ──────────────────────────
    size = calculate_position_size(balance, confidence, entry_price, effective_sl_pct, chat_id)
    # Protection levels used BOTH for broker order and Telegram report.
    # If the strategy provided stop_loss_pct, use it directly.
    # Otherwise fall back to ATR candidate (legacy behavior).
    if stop_loss_pct is not None:
        initial_stop = stop_price
    elif atr and entry_price > 0:
        initial_stop = compute_stop_candidate(action, entry_price, atr)
    else:
        initial_stop = stop_price

    stop_level = initial_stop if initial_stop is not None else stop_price

    # ── Stop-loss max-distance cap (avoid invalid.stoploss.maxvalue) ─────────
    sl_adjust_note = ""
    max_dist = _get_max_stop_profit_distance(base_url, headers, order_epic)
    stop_level_capped, sl_adjusted = _cap_stop_to_max_distance(action, entry_price, stop_level, max_dist)
    if sl_adjusted:
        stop_level = stop_level_capped
        if lang == "en":
            sl_adjust_note = (
                f"\nℹ️ Adjusted: SL too far → capped to broker max distance ({float(max_dist):.4f})."
            )
        else:
            sl_adjust_note = (
                f"\nℹ️ تم التعديل: وقف الخسارة بعيد جداً → تم تحديده عند الحد الأقصى المسموح من الوسيط ({float(max_dist):.4f})."
            )
    stop_dist  = abs(entry_price - stop_level)
    if stop_dist <= 0:
        msg = f"❌ Order failed ({symbol} {action}): invalid stop distance"
        send_telegram_message(chat_id, msg)
        return msg

    # Fixed targets as % distance from entry (independent of ATR/stop_dist).
    if action == 'BUY':
        target1 = entry_price * (1 + TP1_PCT)
        target2 = entry_price * (1 + TP2_PCT)
        dir_ar   = 'شراء'
        sq_color = '🟩'
    else:
        target1 = entry_price * (1 - TP1_PCT)
        target2 = entry_price * (1 - TP2_PCT)
        dir_ar   = 'بيع'
        sq_color = '🟥'

    # Capital requires valid absolute levels; sanitize defensively.
    stop_level, target1, target2 = _sanitize_protection_levels(
        action, entry_price, stop_level, target1, target2
    )

    # Broker dealing rules: validate minimum stop/profit distance.
    # Requirement:
    # - TP1 must remain 1%. If it violates min distance, skip the trade (do not open any leg).
    # - TP2 can be widened slightly if too tight.
    min_dist = _get_min_stop_profit_distance(base_url, headers, order_epic)
    if min_dist and float(min_dist) > 0 and entry_price > 0:
        md = float(min_dist)
        tp1_dist = abs(float(target1) - float(entry_price))
        if tp1_dist < md:
            if lang == "en":
                msg = (
                    f"⏭️ Trade skipped ({symbol} {action}): "
                    f"Target 1 distance too tight for broker rules (min={md:.4f}, got={tp1_dist:.4f})"
                )
            else:
                msg = (
                    f"⏭️ تم التخطي ({symbol} {action}): "
                    f"مسافة الهدف الأول صغيرة جداً حسب شروط الوسيط (الحد الأدنى={md:.4f}، الحالي={tp1_dist:.4f})"
                )
            print(msg)
            return msg
        tp2_dist = abs(float(target2) - float(entry_price))
        if tp2_dist < md:
            widen = md * float(TP2_MIN_DISTANCE_BUFFER_MULT)
            target2 = (
                float(entry_price) + widen
                if action == "BUY"
                else float(entry_price) - widen
            )
            stop_level, target1, target2 = _sanitize_protection_levels(
                action, entry_price, stop_level, target1, target2
            )

    # Split into two positions to support TP1 + TP2 on broker.
    # Capital applies one TP per position, so we split size intentionally.
    min_deal_size = _get_min_deal_size(base_url, headers, order_epic)
    qty_total = float(int(round(float(size))))
    ok_split, qty1, qty2, split_err = _split_qty_70_30(qty_total=qty_total, min_deal_size=min_deal_size)
    if not ok_split:
        if lang == "en":
            msg = f"❌ Order blocked ({symbol} {action}): {split_err}"
        else:
            msg = f"❌ تم منع الأمر ({symbol} {action}): {split_err}"
        print(msg)
        return msg

    qty_total = qty1 + qty2

    # Both legs have fixed TPs now (70% at TP1, 30% at TP2).
    legs = [
        {"role": "TP1", "size": float(qty1), "tp": float(target1)},
        {"role": "TP2", "size": float(qty2), "tp": float(target2)},
    ]

    # Atomic execution: open/confirm BOTH legs first, then write to DB.
    opened_broker_legs: list[dict] = []
    parent_session = str(uuid.uuid4())

    for idx, leg in enumerate(legs):
        leg_role = str(leg["role"])
        leg_size = float(leg["size"])
        leg_target = float(leg["tp"])

        # Retry TP2 once by widening TP2 if needed.
        attempts = 2 if leg_role == "TP2" else 1
        last_err = ""
        for attempt in range(attempts):
            ok, out = _open_position_with_protection(
                base_url, headers, order_epic, action, leg_size, stop_level, leg_target
            )
            if not ok:
                last_err = str(out)
                if leg_role == "TP2" and attempt == 0 and min_dist and float(min_dist) > 0 and entry_price > 0:
                    widen = float(min_dist) * float(TP2_MIN_DISTANCE_BUFFER_MULT)
                    leg_target = (float(entry_price) + widen) if action == "BUY" else (float(entry_price) - widen)
                    stop_level, target1, leg_target = _sanitize_protection_levels(
                        action, entry_price, stop_level, target1, leg_target
                    )
                    continue
                break

            payload = out if isinstance(out, dict) else {}
            ok_deal, deal_id, deal_ref, cerr = _confirm_deal_and_visibility(
                base_url,
                headers,
                payload,
                order_epic=order_epic,
                direction=action,
                leg_size=float(leg_size),
                exclude_deal_ids={str(x.get("deal_id")).strip() for x in opened_broker_legs if str(x.get("deal_id")).strip()},
            )
            if not ok_deal or not deal_id:
                last_err = str(cerr)
                break

            opened_broker_legs.append(
                {
                    "role": leg_role,
                    "size": float(leg_size),
                    "tp": float(leg_target),
                    "deal_id": str(deal_id),
                    "deal_reference": (str(deal_ref).strip() if deal_ref else None),
                }
            )
            ok_sync, info = _sync_protection_to_broker(
                base_url, headers, str(deal_id), stop_level, float(leg_target)
            )
            if not ok_sync:
                print(
                    f"⚠️  Broker TP/SL sync failed "
                    f"[{symbol} {deal_id}] sl={stop_level:.4f} tp={float(leg_target):.4f} :: {info}"
                )
            break

        # If this leg failed, rollback any opened legs and abort (no half-configured trade).
        if len(opened_broker_legs) != (idx + 1):
            for ob in reversed(opened_broker_legs):
                ok_del, info = _delete_position(base_url, headers, str(ob.get("deal_id") or ""))
                if not ok_del:
                    print(f"⚠️  Rollback failed for {symbol} deal={ob.get('deal_id')}: {info}")
            reason = last_err or "unknown rejection"
            msg = (
                f"❌ Order rejected ({symbol} {action}) — {leg_role} failed.\n"
                f"Reason: {reason}"
            )
            send_telegram_message(chat_id, msg)
            return msg

    opened_trade_ids: list[int] = []
    for ob in opened_broker_legs:
        tid = record_open_trade(
            chat_id,
            symbol,
            action,
            entry_price,
            float(ob["size"]),
            str(ob["deal_id"]),
            stop_level,
            deal_reference=ob.get("deal_reference"),
            leg_role=str(ob["role"]),
            parent_session=parent_session,
            stop_distance=stop_dist,
        )
        try:
            if tid is not None:
                opened_trade_ids.append(int(tid))
        except Exception:
            pass

    opened_legs = [(float(ob["size"]), float(ob["tp"]), str(ob["deal_id"])) for ob in opened_broker_legs]
    partial_only_one_leg = False

    if not opened_legs:
        err = f"❌ Order failed ({symbol} {action}): no verified open position on broker"
        send_telegram_message(chat_id, err)
        return err

    verified_qty = float(sum(l[0] for l in opened_legs))
    total_amount = entry_price * verified_qty
    risk_amount = stop_dist * verified_qty

    qty_display = int(verified_qty)

    # ── Notify (localized detailed trade report) ─────────────────────────────
    is_override  = (reason == STATE_MANUAL_OVERRIDE)
    _fmt_money   = lambda v: f"${v:,.2f}"

    # Stable trade identifier:
    # Use the smallest trade_id opened in this session (TP1/TP2) as the display number.
    trade_index = min(opened_trade_ids) if opened_trade_ids else 0

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
            f"🎯  Target 1     :  *{_fmt_money(target1)}*  ({int(qty1)} shares — +{TP1_PCT * 100:.2f}%)\n"
            f"🏆  Target 2     :  *{_fmt_money(target2)}*  ({int(qty2)} shares — +{TP2_PCT * 100:.2f}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️  Risk Amount  :  *{_fmt_money(risk_amount)}*\n"
            f"🕐  Time (ET)    :  {now_et_str}"
            f"{partial_note_en}"
            f"{sl_adjust_note}"
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
            f"🎯  الهدف 1        :  *{_fmt_money(target1)}*  ({int(qty1)} سهم — +{TP1_PCT * 100:.2f}%)\n"
            f"🏆  الهدف 2        :  *{_fmt_money(target2)}*  ({int(qty2)} سهم — +{TP2_PCT * 100:.2f}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️  المخاطرة       :  *{_fmt_money(risk_amount)}*\n"
            f"🕐  التوقيت (ET)   :  {now_et_str}"
            f"{partial_note_ar}"
            f"{sl_adjust_note}"
        )

    tg_res = send_telegram_message(chat_id, msg)
    # Always surface Telegram delivery failures in engine logs.
    try:
        if isinstance(tg_res, dict) and tg_res.get("ok") is False:
            desc = str(tg_res.get("description") or "").strip()
            print(f"[Telegram] Send failed chat_id={chat_id} desc={desc[:220]}", flush=True)
    except Exception:
        pass

    stop_info = f"{stop_level:.4f}" if stop_level is not None else "N/A"
    return (
        f"✅ Opened — legs: {len(opened_legs)} | size: {verified_qty} | "
        f"stop: {stop_info} | RR: {rr_ratio:.1f}"
    )
