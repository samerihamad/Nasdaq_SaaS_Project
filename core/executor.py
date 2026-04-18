import requests
import sqlite3
import time
import uuid
import math
import os
import logging
import threading
import hashlib
from datetime import datetime, timedelta, timezone
import pandas as pd
from bot.notifier import send_telegram_message, notify_admin_alert
from bot.licensing import safe_decrypt
from config import (
    TP1_PCT,
    TP2_PCT,
    TP1_SPLIT_PCT,
    TP2_SPLIT_PCT,
    TP2_MIN_DISTANCE_BUFFER_MULT,
    BE_LOCK_BUFFER_PCT,
    SUPPRESS_EXPECTED_REJECTION_TELEGRAM,
    ENABLE_LIMIT_ORDER_MODE,
    LIMIT_ORDER_TTL_BARS,
    LIMIT_ORDER_BAR_MINUTES,
    LIMIT_ORDER_MOMENTUM_RETRACE,
    LIMIT_ORDER_MEANREV_ATR_OFFSET,
    LIMIT_ORDER_ALLOW_MARKET_FALLBACK,
    EXECUTION_REJECTION_NOTIFY_COOLDOWN_SEC,
)
from utils.market_scanner import (
    HTTP_429_COOLDOWN_SEC,
    respect_capital_http_interval_sync,
    scan_multi_timeframe,
    scan_market,
    _SESSION_CACHE as _SHARED_SESSION_CACHE,
)
from utils.market_hours import is_trading_required, is_nyse_trading_day, ET
from database.db_manager import (
    DB_PATH,
    is_maintenance_mode,
    get_subscriber_lang,
    get_user_signal_profile,
    touch_signal_delivered,
)
from core.risk_manager import (
    can_open_trade,
    calculate_position_size, STATE_MANUAL_OVERRIDE,
    check_daily_drawdown, check_rr_ratio,
    get_effective_leverage, validate_pre_trade, generate_institutional_stop_loss,
    compute_last_rsi,
)
from core.trade_session_finalize import after_trade_leg_closed
from core.trailing_stop import (
    calculate_atr, compute_stop_candidate, advance_trailing_stop,
    is_stop_hit, get_open_trades, update_trade_stop, update_trade_target_reached,
    close_trade_in_db, record_open_trade,
)
from core.sync import reconcile
from core.sync import mark_trade_closed_pending
from core.sync import spawn_background_final_sync
from core.sync import capital_verify_deal_closed_after_close_request
from core.sync import capital_deal_still_open
from core.sync import _capital_all_ids_from_row
from utils.market_hours import synchronized_utc_now, sync_utc_with_ntp

log = logging.getLogger(__name__)

# Daily per-user symbol→epic cache (and unsupported symbols) to avoid
# repeating broker lookups for every signal cycle.
_EPIC_CACHE = {}
SESSION_TTL_SECONDS = 45
LOG_ROOT = os.getenv("ENGINE_LOG_ROOT", "logs")
_REJECTION_NOTIFY_CACHE: dict[str, float] = {}
MAX_SLIPPAGE_PCT = float(os.getenv("MAX_SLIPPAGE_PCT", "0.003"))
_EXEC_ATR_PERIOD = 14
_EXEC_ATR_W_15M = 0.60
_EXEC_ATR_W_1H = 0.40
_CORRELATED_GROUP_MAX_EXPOSURE_PCT = 0.20
_CORRELATED_GROUPS: dict[str, set[str]] = {
    "TECH": {"AAPL", "MSFT", "GOOGL", "NVDA", "QQQ"},
}
_VOLATILE_LIMIT_ATR_FRAC = 0.12
_ADX_STRONG_TREND = 25.0
_ADX_RANGING = 20.0
_TRAIL_MULT_STRONG = 1.6
_TRAIL_MULT_BASE = 2.0
_TRAIL_MULT_RANGING = 2.5
_EXEC_TELEMETRY_LOCK = threading.Lock()
_EXEC_TELEMETRY_DAY = synchronized_utc_now().date().isoformat()
_EXEC_TELEMETRY = {
    "slippage_aborts": 0,
    "local_guard_blocks": 0,
    "weekend_blocks": 0,
}

_TIME_SHIFT_ERROR_MARKERS = (
    "time shift",
    "session expired",
    "invalid timestamp",
    "request expired",
)


def _atr_from_ohlcv(df: pd.DataFrame | None, period: int = _EXEC_ATR_PERIOD) -> float | None:
    try:
        if df is None or len(df) < int(period) + 1:
            return None
        work = df.copy()
        if isinstance(work.columns, pd.MultiIndex):
            work.columns = work.columns.get_level_values(0)
        if not all(c in work.columns for c in ("High", "Low", "Close")):
            return None
        high = work["High"].astype(float)
        low = work["Low"].astype(float)
        close = work["Close"].astype(float)
        prev_close = close.shift(1)
        tr = pd.concat(
            [
                (high - low),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.rolling(int(period)).mean().iloc[-1]
        if pd.isna(atr):
            return None
        return float(atr)
    except Exception:
        return None


def _bump_execution_shield_counter(
    metric: str,
    *,
    chat_id: str | None = None,
    symbol: str | None = None,
    action: str | None = None,
    details: str = "",
) -> None:
    """Daily-reset telemetry counters for execution shield guards."""
    global _EXEC_TELEMETRY_DAY
    today = synchronized_utc_now().date().isoformat()
    with _EXEC_TELEMETRY_LOCK:
        if _EXEC_TELEMETRY_DAY != today:
            _EXEC_TELEMETRY_DAY = today
            _EXEC_TELEMETRY["slippage_aborts"] = 0
            _EXEC_TELEMETRY["local_guard_blocks"] = 0
            _EXEC_TELEMETRY["weekend_blocks"] = 0
        if metric not in _EXEC_TELEMETRY:
            return
        _EXEC_TELEMETRY[metric] = int(_EXEC_TELEMETRY.get(metric, 0)) + 1
        snapshot = (
            f"day={_EXEC_TELEMETRY_DAY} "
            f"slippage_aborts={int(_EXEC_TELEMETRY['slippage_aborts'])} "
            f"local_guard_blocks={int(_EXEC_TELEMETRY['local_guard_blocks'])} "
            f"weekend_blocks={int(_EXEC_TELEMETRY['weekend_blocks'])}"
        )
    _audit_exec_event(
        stage=f"telemetry_{metric}",
        chat_id=chat_id,
        symbol=symbol,
        action=action,
        details=f"{snapshot} {details}".strip(),
    )
    log.info("[EXEC_TELEMETRY] %s", snapshot)


def _weighted_multi_tf_atr(symbol: str, *, session_context: dict | None = None) -> tuple[float | None, float | None, float | None]:
    """Weighted ATR from 15m (60%) + 1h (40%) with safe fallback."""
    atr_15m = _atr_from_ohlcv(scan_market(symbol, period="7d", interval="15m", session_context=session_context))
    atr_1h = _atr_from_ohlcv(scan_market(symbol, period="14d", interval="1h", session_context=session_context))
    if atr_15m is not None and atr_1h is not None:
        return (float(atr_15m) * _EXEC_ATR_W_15M) + (float(atr_1h) * _EXEC_ATR_W_1H), atr_15m, atr_1h
    if atr_15m is not None:
        return float(atr_15m), atr_15m, atr_1h
    if atr_1h is not None:
        return float(atr_1h), atr_15m, atr_1h
    return None, None, None


def _symbol_group(symbol: str) -> str | None:
    s = str(symbol or "").upper().strip()
    for g, members in _CORRELATED_GROUPS.items():
        if s in members:
            return g
    return None


def _local_group_notional(chat_id: str, group: str) -> float:
    members = _CORRELATED_GROUPS.get(str(group), set())
    if not members:
        return 0.0
    try:
        placeholders = ",".join(["?"] * len(members))
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                f"""
                SELECT COALESCE(SUM(ABS(COALESCE(entry_price,0) * COALESCE(size,0))), 0)
                FROM trades
                WHERE chat_id=?
                  AND UPPER(status)='OPEN'
                  AND UPPER(symbol) IN ({placeholders})
                """,
                [str(chat_id), *[str(x).upper() for x in members]],
            )
            row = c.fetchone()
            return float(row[0] or 0.0)
    except Exception:
        return 0.0


def _adaptive_trailing_multiplier(symbol: str, *, session_context: dict | None = None) -> float:
    """
    ADX-aware trailing multiplier:
    - ADX > 25: tighter trail (smaller multiplier)
    - ADX < 20: looser trail (larger multiplier)
    """
    try:
        from core.strategy_momentum import _adx as _mom_adx, _flatten as _mom_flatten

        df = scan_market(str(symbol), period="5d", interval="15m", session_context=session_context)
        if df is None or len(df) < 30:
            return float(_TRAIL_MULT_BASE)
        flat = _mom_flatten(df)
        adx_s, _, _ = _mom_adx(flat)
        adx_val = float(adx_s.iloc[-1])
        if not math.isfinite(adx_val):
            return float(_TRAIL_MULT_BASE)
        if adx_val > float(_ADX_STRONG_TREND):
            return float(_TRAIL_MULT_STRONG)
        if adx_val < float(_ADX_RANGING):
            return float(_TRAIL_MULT_RANGING)
        return float(_TRAIL_MULT_BASE)
    except Exception:
        return float(_TRAIL_MULT_BASE)


def _has_local_pending_or_open_trade(chat_id: str, symbol: str) -> bool:
    """Execution state guard against duplicate local entries during broker lag."""
    try:
        s = str(symbol or "").upper().strip()
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute(
                "SELECT 1 FROM trades WHERE chat_id=? AND UPPER(symbol)=? AND UPPER(status) IN ('OPEN','PENDING') LIMIT 1",
                (str(chat_id), s),
            )
            if c.fetchone():
                return True
            c.execute(
                "SELECT 1 FROM pending_limit_orders WHERE chat_id=? AND UPPER(symbol)=? AND UPPER(status)='PENDING' LIMIT 1",
                (str(chat_id), s),
            )
            return bool(c.fetchone())
    except Exception:
        return False


def _append_daily_exec_log(filename: str, message: str):
    """Write execution-layer events into daily logs/<date>/ files."""
    try:
        day_dir = os.path.join(LOG_ROOT, datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        os.makedirs(day_dir, exist_ok=True)
        path = os.path.join(day_dir, filename)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception:
        pass


def _audit_exec_event(stage: str, chat_id: str | None, symbol: str | None, action: str | None, details: str):
    _append_daily_exec_log(
        "execution_events.txt",
        (
            f"stage={stage} chat_id={chat_id or 'n/a'} symbol={symbol or 'n/a'} "
            f"action={action or 'n/a'} details={details}"
        ),
    )


def _maybe_notify_rejection(chat_id: str, message: str, *, symbol: str = "", action: str = "", stage: str = "generic"):
    """
    Cooldown-throttled rejection notification to reduce repetitive user spam.
    """
    try:
        now_ts = time.time()
        key = f"{chat_id}|{str(symbol).upper()}|{str(action).upper()}|{stage}"
        last_ts = float(_REJECTION_NOTIFY_CACHE.get(key, 0.0))
        if (now_ts - last_ts) < float(EXECUTION_REJECTION_NOTIFY_COOLDOWN_SEC):
            return False
        _REJECTION_NOTIFY_CACHE[key] = now_ts
        send_telegram_message(chat_id, message)
        return True
    except Exception:
        return False


def _log_trade_rejection(
    chat_id,
    symbol,
    action,
    stage: str,
    reason: str,
    details: str = "",
    *,
    reason_code: str | None = None,
):
    """
    Persist expected rejections for audit without spamming Telegram.
    """
    try:
        code = (reason_code or stage or "UNKNOWN").strip().upper().replace(" ", "_")
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.execute(
                "INSERT INTO trade_rejections (created_at, chat_id, symbol, action, stage, reason, details, reason_code) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    datetime.now(timezone.utc).isoformat(),
                    str(chat_id),
                    str(symbol),
                    str(action),
                    str(stage),
                    str(reason),
                    str(details or ""),
                    str(code),
                ),
            )
        except Exception:
            conn.execute(
                "INSERT INTO trade_rejections (created_at, chat_id, symbol, action, stage, reason, details) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    datetime.now(timezone.utc).isoformat(),
                    str(chat_id),
                    str(symbol),
                    str(action),
                    str(stage),
                    str(reason),
                    str(details or ""),
                ),
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _is_expected_rejection(reason: str) -> bool:
    r = str(reason or "").lower()
    markers = [
        "error.invalid.takeprofit.minvalue",
        "error.invalid.stoploss.maxvalue",
        "deal rejected (rejected)",
        "target not achievable within atr",
        "does not meet minimum 1:2",
        "symbol not available on broker",
        "distance too tight for broker rules",
        "min distance",
        "max distance",
    ]
    return any(m in r for m in markers)


def _extract_min_tp_value(err: str) -> float | None:
    """
    Parse Capital error like: error.invalid.takeprofit.minvalue: 14.38
    Returns the numeric minimum TP level, if present.
    """
    try:
        import re
        m = re.search(r"takeprofit\.minvalue\s*:\s*([0-9]+(?:\.[0-9]+)?)", str(err or ""), re.I)
        if not m:
            return None
        return float(m.group(1))
    except Exception:
        return None


def _safe_json_response(res: requests.Response) -> dict:
    try:
        data = res.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _utc_timestamp_ms() -> str:
    return str(int(synchronized_utc_now().timestamp() * 1000))


def _with_broker_timestamp_headers(headers: dict | None) -> dict:
    out = dict(headers or {})
    ts_ms = _utc_timestamp_ms()
    out["X-CAP-TIMESTAMP"] = ts_ms
    out["X-TIMESTAMP"] = ts_ms
    return out


def _is_time_or_session_error(status_code: int, error_message: str) -> bool:
    if int(status_code) in (401, 403):
        return True
    msg = str(error_message or "").lower()
    return any(marker in msg for marker in _TIME_SHIFT_ERROR_MARKERS)


def _force_resync_and_relogin(
    *,
    creds=None,
    chat_id: str | None = None,
    reason: str = "",
) -> tuple[bool, str, dict | None]:
    """
    Self-healing path:
    1) sync process UTC offset from NTP
    2) force fresh Capital session tokens
    """
    sync_diag = sync_utc_with_ntp()
    _audit_exec_event(
        stage="clock_resync",
        chat_id=(str(chat_id) if chat_id is not None else None),
        symbol=None,
        action=None,
        details=f"reason={reason[:120]} ntp_ok={int(bool(sync_diag.get('ok')))} diag={sync_diag}",
    )
    if creds is None:
        return False, "missing creds for relogin", None
    base_url, headers = get_session(
        creds,
        chat_id=(str(chat_id) if chat_id is not None else None),
        force_refresh=True,
    )
    if not headers:
        return False, "relogin_failed", None
    return True, "recovered", {"base_url": base_url, "headers": headers}


def _resilient_capital_get(
    url: str,
    *,
    headers: dict,
    timeout: int = 20,
    chat_id: str | None = None,
    creds=None,
) -> tuple[requests.Response | None, dict]:
    """
    GET with broker timestamp + one-shot self-heal for time/session errors.
    Returns (response, active_headers).
    """
    respect_capital_http_interval_sync()
    stamped = _with_broker_timestamp_headers(headers)
    try:
        res = requests.get(url, headers=stamped, timeout=timeout)
    except Exception:
        return None, headers

    if res.status_code in (401, 403):
        err = _normalize_broker_error(res=res, payload=_safe_json_response(res))
        recovered, _, payload = _force_resync_and_relogin(
            creds=creds,
            chat_id=chat_id,
            reason=err,
        )
        if recovered and payload and payload.get("headers"):
            new_headers = payload["headers"]
            try:
                respect_capital_http_interval_sync()
                res2 = requests.get(
                    url,
                    headers=_with_broker_timestamp_headers(new_headers),
                    timeout=timeout,
                )
                return res2, new_headers
            except Exception:
                return res, headers
    return res, headers


def _normalize_broker_error(res: requests.Response | None = None, payload: dict | None = None) -> str:
    """
    Unified Capital.com error parser to avoid scattered string handling.
    """
    data = payload or {}
    if (not data) and res is not None:
        data = _safe_json_response(res)

    fields = (
        "errorCode",
        "message",
        "errorMessage",
        "reason",
        "rejectionReason",
        "dealError",
        "details",
    )
    pieces: list[str] = []
    for k in fields:
        v = data.get(k) if isinstance(data, dict) else None
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if k == "errorCode":
            pieces.insert(0, s)
        else:
            pieces.append(s)
    if pieces:
        merged = ": ".join([pieces[0], " | ".join(pieces[1:])]) if len(pieces) > 1 else pieces[0]
        return merged[:320]

    if res is None:
        return "unknown broker error"
    txt = (res.text or "").strip()
    if txt:
        return f"HTTP_{res.status_code} {txt[:280]}"
    return f"HTTP_{res.status_code}"


def _broker_request(
    method: str,
    url: str,
    *,
    headers: dict,
    json_payload: dict | None = None,
    params: dict | None = None,
    timeout: int = 20,
    creds=None,
    chat_id: str | None = None,
) -> tuple[bool, dict, str, int]:
    """
    Unified broker request wrapper.
    Returns: (ok, payload, error_msg, status_code)
    """
    respect_capital_http_interval_sync()
    req_headers = _with_broker_timestamp_headers(headers)
    try:
        res = requests.request(
            method=method.upper(),
            url=url,
            headers=req_headers,
            json=json_payload,
            params=params,
            timeout=timeout,
        )
    except Exception as exc:
        return False, {}, str(exc), 0

    data = _safe_json_response(res)
    if res.status_code != 200:
        err = _normalize_broker_error(res=res, payload=data)
        # Self-heal on session expiry/time drift and retry once with fresh session.
        if _is_time_or_session_error(int(res.status_code), err):
            recovered, _, payload = _force_resync_and_relogin(
                creds=creds,
                chat_id=chat_id,
                reason=err,
            )
            if recovered and payload and payload.get("headers"):
                try:
                    headers.clear()
                    headers.update(payload["headers"])
                except Exception:
                    pass
                retry_headers = _with_broker_timestamp_headers(payload["headers"])
                try:
                    respect_capital_http_interval_sync()
                    res2 = requests.request(
                        method=method.upper(),
                        url=url,
                        headers=retry_headers,
                        json=json_payload,
                        params=params,
                        timeout=timeout,
                    )
                    data2 = _safe_json_response(res2)
                    if res2.status_code == 200 and not (isinstance(data2, dict) and data2.get("errorCode")):
                        return True, data2, "", int(res2.status_code)
                    return False, data2, _normalize_broker_error(res=res2, payload=data2), int(res2.status_code)
                except Exception as exc2:
                    return False, {}, str(exc2), 0
        return False, data, err, int(res.status_code)
    if isinstance(data, dict) and data.get("errorCode"):
        return False, data, _normalize_broker_error(res=res, payload=data), int(res.status_code)
    return True, data, "", int(res.status_code)


def _calculate_limit_price(
    symbol: str,
    action: str,
    strategy_label: str,
    entry_price: float,
    atr: float | None,
    *,
    session_context: dict | None = None,
    regime_type: str | None = None,
) -> float:
    """
    Sprint 2 policy map:
    - Momentum: 0.618 retrace of last 15m candle range.
    - MeanRev: ATR offset from current extreme/reclaimed area proxy.
    """
    s = (strategy_label or "").strip().lower()
    tf = scan_multi_timeframe(symbol, session_context=session_context) or {}
    df_15m = tf.get("15m")
    if df_15m is None or len(df_15m) < 2:
        return float(entry_price)

    try:
        last = df_15m.iloc[-1]
        rng = abs(float(last["High"]) - float(last["Low"]))
    except Exception:
        rng = 0.0
    if rng <= 0:
        rng = max(0.01, float(entry_price) * 0.002)

    if s == "momentum":
        retr = max(0.1, min(0.9, float(LIMIT_ORDER_MOMENTUM_RETRACE)))
        if action == "BUY":
            px = float(entry_price) - (retr * rng)
        else:
            px = float(entry_price) + (retr * rng)
    else:
        # Mean reversion: use conservative ATR offset from current price proxy.
        atr_off = float(atr or 0.0) * max(0.05, float(LIMIT_ORDER_MEANREV_ATR_OFFSET))
        if atr_off <= 0:
            atr_off = max(0.01, float(entry_price) * 0.0015)
        if action == "BUY":
            px = float(entry_price) - atr_off
        else:
            px = float(entry_price) + atr_off

    # VOLATILE regime: use slightly more aggressive limit (closer to market) for better fill.
    if str(regime_type or "").upper() == "VOLATILE" and atr is not None and math.isfinite(float(atr)) and float(atr) > 0:
        tighten = float(atr) * float(_VOLATILE_LIMIT_ATR_FRAC)
        if action == "BUY":
            px = max(float(px), float(entry_price) - float(tighten))
        else:
            px = min(float(px), float(entry_price) + float(tighten))

    return max(0.01, round(px, 4))


def _place_pending_limit_order(
    chat_id: str,
    symbol: str,
    action: str,
    strategy_label: str,
    confidence: float,
    ai_prob: float | None,
    stop_loss_pct: float | None,
    limit_price: float,
):
    ttl_sec = max(60, int(LIMIT_ORDER_TTL_BARS) * int(LIMIT_ORDER_BAR_MINUTES) * 60)
    now = datetime.now(timezone.utc)
    exp = now + timedelta(seconds=ttl_sec)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # one active pending limit per symbol+direction per user
    c.execute(
        "SELECT id FROM pending_limit_orders WHERE chat_id=? AND symbol=? AND action=? "
        "AND status='PENDING' LIMIT 1",
        (str(chat_id), str(symbol), str(action)),
    )
    row = c.fetchone()
    if row:
        conn.close()
        return False, int(row[0]), "already_pending"
    c.execute(
        "INSERT INTO pending_limit_orders "
        "(created_at, expires_at, chat_id, symbol, action, strategy_label, confidence, ai_prob, stop_loss_pct, limit_price, status, reason) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)",
        (
            now.isoformat(),
            exp.isoformat(),
            str(chat_id),
            str(symbol),
            str(action),
            str(strategy_label or ""),
            float(confidence),
            float(ai_prob) if ai_prob is not None else None,
            float(stop_loss_pct) if stop_loss_pct is not None else None,
            float(limit_price),
            "limit_policy",
        ),
    )
    oid = int(c.lastrowid)
    conn.commit()
    conn.close()
    return True, oid, ""


def process_pending_limit_orders():
    """
    Worker: track pending limits, trigger execution on touch, auto-cancel on TTL.
    TTL expiry uses DB/Telegram only (runs on weekends). Touch/execute uses Capital only when is_trading_required().
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT id, chat_id, symbol, action, strategy_label, confidence, stop_loss_pct, limit_price, "
        "created_at, expires_at FROM pending_limit_orders WHERE status='PENDING' ORDER BY id ASC"
    )
    rows = c.fetchall()
    conn.close()
    if not rows:
        return 0

    processed = 0
    now = datetime.now(timezone.utc)
    for row in rows:
        (
            oid,
            chat_id,
            symbol,
            action,
            strategy_label,
            confidence,
            stop_loss_pct,
            limit_price,
            _created_at,
            expires_at,
        ) = row
        exp_dt = None
        try:
            exp_dt = datetime.fromisoformat(str(expires_at))
        except Exception:
            exp_dt = None
        if exp_dt and now >= exp_dt:
            with sqlite3.connect(DB_PATH) as cx:
                cx.execute(
                    "UPDATE pending_limit_orders SET status='CANCELLED', cancelled_at=?, reason=? WHERE id=? AND status='PENDING'",
                    (now.isoformat(), "ttl_expired", int(oid)),
                )
            _audit_exec_event(
                stage="pending_limit_cancelled",
                chat_id=str(chat_id),
                symbol=str(symbol),
                action=str(action),
                details=f"oid={int(oid)} reason=ttl_expired limit={float(limit_price):.4f}",
            )
            lang = get_subscriber_lang(str(chat_id))
            msg = (
                f"⏱️ *Limit order expired*\n\n📌 {symbol} {action}\n💰 Limit: {float(limit_price):.4f}\nReason: TTL reached ({int(LIMIT_ORDER_TTL_BARS)} bars)."
                if lang == "en"
                else
                f"⏱️ *انتهت صلاحية أمر ليمت*\n\n📌 {symbol} {('شراء' if action=='BUY' else 'بيع')}\n💰 سعر الليمِت: {float(limit_price):.4f}\nالسبب: انتهاء المهلة ({int(LIMIT_ORDER_TTL_BARS)} شموع)."
            )
            send_telegram_message(str(chat_id), msg)
            processed += 1
            continue

        if not is_nyse_trading_day(synchronized_utc_now().astimezone(ET)):
            _bump_execution_shield_counter(
                "weekend_blocks",
                chat_id=str(chat_id),
                symbol=str(symbol),
                action=str(action),
                details="pending_limit gate",
            )
            continue
        if not is_trading_required():
            continue

        creds = get_user_credentials(str(chat_id))
        if not creds:
            continue
        base_url, headers = get_session(creds, chat_id=str(chat_id))
        if not headers:
            continue
        order_epic = resolve_epic_for_user(str(chat_id), str(symbol), base_url=base_url, headers=headers, is_demo=bool(creds[2]))
        if not order_epic:
            continue
        px = _get_current_price(base_url, headers, order_epic)
        touched = (float(px) <= float(limit_price)) if str(action) == "BUY" else (float(px) >= float(limit_price))
        if not touched:
            continue

        with sqlite3.connect(DB_PATH) as cx:
            cx.execute(
                "UPDATE pending_limit_orders SET status='TRIGGERED', triggered_at=? WHERE id=? AND status='PENDING'",
                (now.isoformat(), int(oid)),
            )
        lang = get_subscriber_lang(str(chat_id))
        trig_msg = (
            f"🎯 *Limit touched* — executing\n{symbol} {action}\nLimit: {float(limit_price):.4f}\nNow: {float(px):.4f}"
            if lang == "en"
            else
            f"🎯 *تم لمس سعر الليمِت* — جارٍ التنفيذ\n{symbol} {('شراء' if action=='BUY' else 'بيع')}\nسعر الليمِت: {float(limit_price):.4f}\nالسعر الحالي: {float(px):.4f}"
        )
        send_telegram_message(str(chat_id), trig_msg)
        result = place_trade_for_user(
            str(chat_id),
            str(symbol),
            str(action),
            confidence=float(confidence or 75.0),
            stop_loss_pct=float(stop_loss_pct) if stop_loss_pct is not None else None,
            strategy_label=str(strategy_label or ""),
            force_market=True,
            is_pending_trigger=True,
        )
        final_status = "FILLED" if isinstance(result, str) and result.startswith("✅ Opened") else "FAILED"
        with sqlite3.connect(DB_PATH) as cx:
            cx.execute(
                "UPDATE pending_limit_orders SET status=?, last_error=? WHERE id=?",
                (final_status, "" if final_status == "FILLED" else str(result or "")[:400], int(oid)),
            )
        _audit_exec_event(
            stage="pending_limit_triggered",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details=f"oid={int(oid)} status={final_status} limit={float(limit_price):.4f} now={float(px):.4f}",
        )
        if final_status != "FILLED":
            # Ensure the user gets an explicit outcome after "executing".
            lang = get_subscriber_lang(str(chat_id))
            err = str(result or "").strip()
            err = err[:700] + ("..." if len(err) > 700 else "")
            fail_msg = (
                f"❌ *Limit execution failed*\n{symbol} {action}\nReason: {err}"
                if lang == "en"
                else
                f"❌ *فشل تنفيذ أمر الليمِت*\n{symbol} {('شراء' if action=='BUY' else 'بيع')}\nالسبب: {err}"
            )
            send_telegram_message(str(chat_id), fail_msg)
        processed += 1
    return processed


# ── Credentials & session ─────────────────────────────────────────────────────

def get_user_credentials(chat_id):
    """Fetch and decrypt user credentials from DB."""
    conn = sqlite3.connect(DB_PATH)
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


def get_session(creds, chat_id: str | None = None, force_refresh: bool = False):
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

    # Use the same cache_key scheme as utils.market_scanner so scanner + executor share sessions.
    cache_key = f"{api_key[:8]}|{user_email}|{int(bool(is_demo))}"
    now = datetime.now(timezone.utc)
    cached = _SHARED_SESSION_CACHE.get(cache_key, {})
    try:
        expires_ts = float(cached.get("expires_ts") or 0.0)
    except Exception:
        expires_ts = 0.0
    cached_headers = cached.get("headers")
    cached_base = cached.get("base_url")
    if (not force_refresh) and cached_headers and cached_base and now.timestamp() < expires_ts:
        return str(cached_base), dict(cached_headers)

    auth_res = None
    # Capital auth can fail transiently (network edge / temporary backend errors).
    # Retry briefly before surfacing an authentication failure to the user.
    for attempt in range(3):
        try:
            respect_capital_http_interval_sync()
            auth_headers = _with_broker_timestamp_headers(headers)
            auth_res = requests.post(
                f"{base_url}/session",
                json={"identifier": user_email, "password": password},
                headers=auth_headers,
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
            _SHARED_SESSION_CACHE[cache_key] = {
                "base_url": base_url,
                "headers": session_headers,
                "expires_ts": datetime.now(timezone.utc).timestamp() + float(SESSION_TTL_SECONDS),
            }
            return base_url, session_headers
        if auth_res is not None and int(auth_res.status_code) == 429:
            print(
                f"[Capital Auth] HTTP 429 — cooling down {float(HTTP_429_COOLDOWN_SEC):.0f}s "
                f"before retry (attempt {attempt + 1}/3).",
                flush=True,
            )
            if attempt < 2:
                time.sleep(float(HTTP_429_COOLDOWN_SEC))
            continue
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
    _audit_exec_event(
        stage="capital_auth_failed",
        chat_id=(str(chat_id).strip() if chat_id is not None else None),
        symbol=None,
        action=None,
        details=f"status={status} is_demo={is_demo} response={err_text[:180]}",
    )
    return None, None


def _scanner_context_from_creds(chat_id: str, creds) -> dict:
    """
    Build scanner session context from decrypted user credentials.
    """
    return {
        "api_key": str(creds[0]).strip(),
        "password": str(creds[1]).strip(),
        "email": str(creds[3]).strip(),
        "is_demo": bool(creds[2]),
        "label": f"user:{chat_id}",
    }


def _get_balance(base_url, headers):
    try:
        res = requests.get(f"{base_url}/accounts", headers=_with_broker_timestamp_headers(headers))
        if res.status_code == 200:
            return float(res.json()['accounts'][0]['balance']['balance'])
    except Exception:
        pass
    return 1000.0


def _get_balance_and_free_margin(base_url, headers) -> tuple[float, float]:
    """
    Return (balance, free_margin) with robust fallback across Capital payload shapes.
    """
    try:
        res = requests.get(f"{base_url}/accounts", headers=_with_broker_timestamp_headers(headers), timeout=20)
        if res.status_code != 200:
            bal = _get_balance(base_url, headers)
            return float(bal), float(bal)
        data = res.json() or {}
        accs = data.get("accounts", []) or []
        if not accs:
            bal = _get_balance(base_url, headers)
            return float(bal), float(bal)
        acc = accs[0] or {}
        b = acc.get("balance", {}) or {}
        balance = (
            b.get("balance")
            if b.get("balance") is not None
            else acc.get("balance")
        )
        free_margin = (
            b.get("available")
            if b.get("available") is not None
            else b.get("availableFunds")
        )
        if free_margin is None:
            # Conservative fallback if broker payload omits a dedicated free-margin field.
            free_margin = balance
        return float(balance or 0.0), float(free_margin or 0.0)
    except Exception:
        bal = _get_balance(base_url, headers)
        return float(bal), float(bal)


def _current_exposure_notional(base_url, headers, symbol: str, order_epic: str) -> tuple[float, float, dict]:
    """
    Return (symbol_exposure_notional, total_exposure_notional) from open positions.
    """
    sym_token = str(symbol or "").upper().strip()
    epic_u = str(order_epic or "").upper().strip()
    sym_exposure = 0.0
    total_exposure = 0.0
    by_symbol: dict[str, float] = {}
    try:
        res = requests.get(f"{base_url}/positions", headers=_with_broker_timestamp_headers(headers), timeout=20)
        if res.status_code != 200:
            return 0.0, 0.0, {}
        for p in (res.json() or {}).get("positions", []):
            m = p.get("market", {}) or {}
            pos = p.get("position", {}) or {}
            row_epic = str(m.get("epic") or "").upper()
            row_name = str(m.get("instrumentName") or "").upper()
            try:
                level = float(pos.get("level") or 0.0)
                size = abs(float(pos.get("size") or 0.0))
            except Exception:
                continue
            notional = level * size
            if notional <= 0:
                continue
            total_exposure += notional
            token = ""
            if row_epic:
                token = row_epic.split(".")[-1].split(":")[-1]
            if not token and row_name:
                token = row_name.split()[0]
            if token:
                by_symbol[token] = float(by_symbol.get(token, 0.0) + notional)
            if epic_u and row_epic == epic_u:
                sym_exposure += notional
            elif sym_token and sym_token in row_name:
                sym_exposure += notional
    except Exception:
        return 0.0, 0.0, {}
    return float(sym_exposure), float(total_exposure), by_symbol


def _get_current_price(base_url, headers, epic):
    """Fetch latest bid price for an instrument."""
    try:
        res = requests.get(f"{base_url}/markets/{epic}", headers=_with_broker_timestamp_headers(headers))
        if res.status_code == 200:
            return float(res.json().get('snapshot', {}).get('bid', 0))
    except Exception:
        pass
    return 0.0


def _epic_cache_key(chat_id, is_demo):
    day = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    return f"{chat_id}|{'DEMO' if is_demo else 'LIVE'}|{day}"


def _get_cache_bucket(chat_id, is_demo):
    key = _epic_cache_key(chat_id, is_demo)
    if key not in _EPIC_CACHE:
        _EPIC_CACHE[key] = {"symbol_to_epic": {}, "unsupported": set()}
    return _EPIC_CACHE[key]


def _prune_old_epic_cache():
    """Keep only today's cache buckets."""
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
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
        direct = requests.get(f"{base_url}/markets/{s}", headers=_with_broker_timestamp_headers(headers))
        if direct.status_code == 200:
            return s
    except Exception:
        pass

    # 2) Try common US-share epic patterns.
    candidates = [f"US.{s}.CASH", f"US.{s}.CFD", f"{s}.CASH", f"{s}.CFD"]
    for epic in candidates:
        try:
            res = requests.get(f"{base_url}/markets/{epic}", headers=_with_broker_timestamp_headers(headers))
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
            headers=_with_broker_timestamp_headers(headers),
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
        res = requests.get(f"{base_url}/markets/{epic}", headers=_with_broker_timestamp_headers(headers), timeout=20)
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
        res = requests.get(f"{base_url}/markets/{epic}", headers=_with_broker_timestamp_headers(headers), timeout=20)
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

        # Fallback: scan for any plausible min-*-distance key variants.
        # Capital payloads can vary (e.g. minNormalStopOrLimitDistance, etc.).
        vals: list[float] = []
        for k, v in (rules or {}).items():
            ks = str(k or "")
            kl = ks.lower()
            if "min" not in kl:
                continue
            if "distance" not in kl:
                continue
            if not (("stop" in kl) or ("profit" in kl) or ("limit" in kl)):
                continue
            if isinstance(v, dict):
                v = v.get("value")
            if v is None:
                continue
            try:
                f = float(v)
                if f > 0:
                    vals.append(f)
            except Exception:
                continue
        if vals:
            return min(vals)
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
        res = requests.get(f"{base_url}/markets/{epic}", headers=_with_broker_timestamp_headers(headers), timeout=20)
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
        res = requests.get(f"{base_url}/markets/{epic}", headers=_with_broker_timestamp_headers(headers), timeout=20)
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


def _delete_position(
    base_url: str,
    headers: dict,
    deal_id: str,
    *,
    creds=None,
    chat_id: str | None = None,
) -> tuple[bool, str]:
    """Attempt to close an open broker position by dealId."""
    if not deal_id:
        return False, "missing deal_id"
    ok, data, err, _ = _broker_request(
        "DELETE",
        f"{base_url}/positions/{deal_id}",
        headers=headers,
        timeout=20,
        creds=creds,
        chat_id=chat_id,
    )
    if ok:
        return True, "ok"
    return False, err or _normalize_broker_error(payload=data)


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
        base_url, headers = get_session(creds, chat_id=str(chat_id))
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


def _open_position_with_protection(
    base_url,
    headers,
    epic,
    action,
    size,
    stop_level,
    target_level,
    *,
    creds=None,
    chat_id: str | None = None,
):
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
        ok, data, err, _ = _broker_request(
            "POST",
            f"{base_url}/positions",
            headers=headers,
            json_payload=payload,
            timeout=20,
            creds=creds,
            chat_id=chat_id,
        )
        if ok:
            return True, data
        last_err = err or _normalize_broker_error(payload=data)
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
        res = requests.get(f"{base_url}/positions", headers=_with_broker_timestamp_headers(headers), timeout=20)
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
            f"{base_url}/positions/{deal_id}", headers=_with_broker_timestamp_headers(headers), timeout=15
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
        res = requests.get(f"{base_url}/positions", headers=_with_broker_timestamp_headers(headers), timeout=20)
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
                f"{base_url}/confirms/{deal_ref}", headers=_with_broker_timestamp_headers(headers), timeout=20
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
                    # Capital can accept an order but lag in /positions visibility.
                    # If /confirms produced a dealId and it was not rejected, treat this
                    # as opened-pending-visibility and let reconcile() confirm later.
                    st = str(
                        data.get("dealStatus") or data.get("status") or ""
                    ).upper()
                    if st and st not in ("REJECTED", "FAILED"):
                        return True, deal_id, deal_ref_str, "pending_visibility"
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
    if deal_id and deal_ref_str:
        # Same rationale as above: we have a dealId but visibility checks didn't pass
        # within the verification window. Proceed and rely on reconcile().
        return True, str(deal_id), deal_ref_str, "pending_visibility"

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


def _sync_stop_to_broker(
    base_url,
    headers,
    deal_id,
    stop_level,
    *,
    creds=None,
    chat_id: str | None = None,
):
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
        ok, data, err, _ = _broker_request(
            "PUT",
            f"{base_url}/positions/{deal_id}",
            headers=headers,
            json_payload=payload,
            timeout=20,
            creds=creds,
            chat_id=chat_id,
        )
        if ok:
            return True, "ok"
        last_err = err or _normalize_broker_error(payload=data)
    return False, last_err or "unknown error"


def _sync_protection_to_broker(
    base_url,
    headers,
    deal_id,
    stop_level,
    profit_level,
    *,
    creds=None,
    chat_id: str | None = None,
):
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
        ok, data, err, _ = _broker_request(
            "PUT",
            f"{base_url}/positions/{deal_id}",
            headers=headers,
            json_payload=payload,
            timeout=20,
            creds=creds,
            chat_id=chat_id,
        )
        if ok:
            return True, "ok"
        last_err = err or _normalize_broker_error(payload=data)
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


def _local_positions_state_hash(chat_id: str) -> str:
    rows = get_open_trades(chat_id) or []
    parts: list[str] = []
    for r in rows:
        parts.append(
            "|".join(
                [
                    str(r.get("deal_id") or "").strip(),
                    str(r.get("symbol") or "").upper().strip(),
                    str(r.get("direction") or "").upper().strip(),
                    f"{float(r.get('size') or 0.0):.4f}",
                ]
            )
        )
    payload = "||".join(sorted(parts))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _broker_positions_state_hash(live_positions: list[dict]) -> str:
    parts: list[str] = []
    for p in live_positions or []:
        m = p.get("market") or {}
        pos = p.get("position") or {}
        parts.append(
            "|".join(
                [
                    str(pos.get("dealId") or p.get("dealId") or "").strip(),
                    str(m.get("epic") or "").upper().strip(),
                    str(pos.get("direction") or "").upper().strip(),
                    f"{float(pos.get('size') or 0.0):.4f}",
                ]
            )
        )
    payload = "||".join(sorted(parts))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
    base_url, headers = get_session(creds, chat_id=str(chat_id))
    if not headers:
        return False

    # ── Step 1: DB ↔ broker reconciliation ────────────────────────────────────
    reconcile(chat_id, base_url, headers)

    # Fetch live positions for price data and hard-stop checks
    pos_res, headers = _resilient_capital_get(
        f"{base_url}/positions",
        headers=headers,
        timeout=20,
        chat_id=str(chat_id),
        creds=creds,
    )
    if pos_res is None:
        return False
    if pos_res.status_code != 200:
        return False

    live_positions  = pos_res.json().get('positions', [])
    balance         = _get_balance(base_url, headers)
    hard_stop_limit = -(balance * 0.03)   # -3% of balance
    # Optional state-sync guard: skip close attempts on hard local/broker divergence.
    local_hash = _local_positions_state_hash(str(chat_id))
    broker_hash = _broker_positions_state_hash(live_positions)
    if local_hash != broker_hash:
        _audit_exec_event(
            stage="state_hash_mismatch",
            chat_id=str(chat_id),
            symbol=None,
            action=None,
            details=f"local_hash={local_hash[:12]} broker_hash={broker_hash[:12]} skip_close_cycle=1",
        )
        return False

    # Index by every dealId/positionId Capital exposes (DB may store either).
    live_by_id = {}
    for p in live_positions:
        ids = _capital_all_ids_from_row(p)
        if not ids and p.get("position", {}).get("dealId") is not None:
            ids = {str(p["position"]["dealId"]).strip()}
        for rid in ids:
            live_by_id[str(rid).strip()] = p

    closed_any = False

    # ── Step 2: ATR trailing stop logic ──────────────────────────────────────
    for trade in get_open_trades(chat_id):
        deal_id   = trade['deal_id']
        symbol    = trade['symbol']
        direction = trade['direction']
        trade_id  = trade['trade_id']
        leg_role = str(trade.get("leg_role") or "").strip()
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

        # Calculate ATR from unified scanner provider data
        atr = calculate_atr(
            symbol,
            session_context=_scanner_context_from_creds(str(chat_id), creds),
        )
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
            manage_leg = leg_role.upper() == "TP2"
            trail_mult = _adaptive_trailing_multiplier(
                symbol,
                session_context=_scanner_context_from_creds(str(chat_id), creds),
            )

            if tp2_hit and manage_leg:
                # Persist milestone once TP2 threshold is first reached.
                try:
                    if (trade.get("target_reached") or "").strip() != "TARGET_2_HIT":
                        update_trade_target_reached(trade_id, "TARGET_2_HIT")
                except Exception:
                    pass
                candidate = compute_stop_candidate(direction, current_price, atr, multiplier=float(trail_mult))
                prev_stop = float(base_stop if base_stop is not None else candidate)
                breakeven = entry_price
                if direction == 'BUY':
                    prev_stop = max(prev_stop, breakeven)
                else:
                    prev_stop = min(prev_stop, breakeven)

                new_stop = advance_trailing_stop(prev_stop, candidate, direction)
                if base_stop != new_stop:
                    update_trade_stop(trade['trade_id'], new_stop)
                    ok, info = _sync_stop_to_broker(
                        base_url,
                        headers,
                        deal_id,
                        new_stop,
                        creds=creds,
                        chat_id=str(chat_id),
                    )
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
                    ok, info = _sync_stop_to_broker(
                        base_url,
                        headers,
                        deal_id,
                        new_stop,
                        creds=creds,
                        chat_id=str(chat_id),
                    )
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
                manage_leg = leg_role.upper() == "TP2"
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
                        ok, info = _sync_stop_to_broker(
                            base_url,
                            headers,
                            deal_id,
                            new_stop,
                            creds=creds,
                            chat_id=str(chat_id),
                        )
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
            ok_del, close_payload, close_err, close_status = _broker_request(
                "DELETE",
                f"{base_url}/positions/{deal_id}",
                headers=headers,
                timeout=20,
                creds=creds,
                chat_id=str(chat_id),
            )
            if ok_del:
                # Capture dealReference from close response when present (Capital often keys history by it).
                deal_ref = (
                    close_payload.get("dealReference")
                    or close_payload.get("dealRef")
                    or close_payload.get("reference")
                )
                # CRITICAL FIX: Capital.com Two-Leg Architecture
                # The DELETE response returns a NEW dealId for the closing leg.
                # The P/L is stored under this NEW dealId in history, NOT the original open dealId.
                close_deal_id = (
                    close_payload.get("dealId")
                    or close_payload.get("deal_id")
                    or close_payload.get("position", {}).get("dealId")
                )
                # Gold rule: never trust HTTP 200 alone — confirm GET /positions no longer lists this deal.
                try:
                    _sz_v = float(size_leg) if size_leg is not None else float(live["position"].get("size") or 0)
                except (TypeError, ValueError):
                    _sz_v = None
                if not capital_verify_deal_closed_after_close_request(
                    base_url,
                    headers,
                    str(deal_id),
                    symbol=str(symbol or ""),
                    direction=str(direction or ""),
                    size=_sz_v,
                ):
                    try:
                        notify_admin_alert(
                            "Desync: Close request returned HTTP 200 but position still OPEN on broker.\n"
                            f"chat_id={chat_id} trade_id={trade_id} {symbol} deal_id={deal_id}"
                        )
                    except Exception:
                        pass
                    continue

                # Broker-truth PnL: non-blocking — mark CLOSED + PENDING_SYNC, daemon retries with
                # exponential backoff up to ~5m; Telegram after successful sync (admin only if all retries fail).
                if capital_deal_still_open(
                    base_url,
                    headers,
                    str(deal_id),
                    symbol=str(symbol or ""),
                    direction=str(direction or ""),
                    size=_sz_v,
                ):
                    try:
                        notify_admin_alert(
                            "ABORT mark_trade_closed_pending: DELETE ok but position still on broker "
                            f"(pre-sync check). chat_id={chat_id} trade_id={trade_id} {symbol} deal_id={deal_id}"
                        )
                    except Exception:
                        pass
                    continue

                leg = leg_role.upper()
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

                conn_u = sqlite3.connect(DB_PATH)
                conn_u.execute(
                    "UPDATE trades SET target_reached=COALESCE(?, target_reached), close_reason=COALESCE(?, close_reason) "
                    "WHERE trade_id=? AND status='OPEN'",
                    (target_label, str(stop_label) if stop_label else None, int(trade["trade_id"])),
                )
                conn_u.commit()
                conn_u.close()

                mark_trade_closed_pending(
                    chat_id,
                    int(trade["trade_id"]),
                    symbol=symbol,
                    direction=direction,
                    deal_reference=str(deal_ref).strip() if deal_ref else None,
                    close_deal_id=str(close_deal_id).strip() if close_deal_id else None,
                    reason="awaiting_background_final_sync",
                    notify=False,
                )
                spawn_background_final_sync(int(trade["trade_id"]))
                closed_any = True
            else:
                _audit_exec_event(
                    stage="close_request_failed",
                    chat_id=str(chat_id),
                    symbol=str(symbol),
                    action=str(direction),
                    details=f"deal_id={deal_id} status={close_status} err={str(close_err)[:220]}",
                )

    return closed_any


# ── Trade execution ───────────────────────────────────────────────────────────


def _validate_execution_gate(
    chat_id: str,
    symbol: str,
    action: str,
    is_pending_trigger: bool = False,
) -> tuple[bool, str, dict]:
    """Pre-trade checks: market/session/account/local-duplicate/broker-duplicate."""
    if is_maintenance_mode():
        return False, "🔧 System in maintenance — new entries suspended.", {}

    lang = get_subscriber_lang(chat_id)
    if not is_nyse_trading_day(synchronized_utc_now().astimezone(ET)):
        msg = "[GUARD] Execution blocked: Market is closed."
        _bump_execution_shield_counter(
            "weekend_blocks",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details="place_trade_for_user gate",
        )
        _audit_exec_event(
            stage="execution_guard_closed_day",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details="is_nyse_trading_day=0",
        )
        return False, msg, {"lang": lang}

    if _has_local_pending_or_open_trade(str(chat_id), str(symbol)):
        msg = f"⏭️ Local guard: existing OPEN/PENDING trade for {symbol}."
        _bump_execution_shield_counter(
            "local_guard_blocks",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details="duplicate local OPEN/PENDING found",
        )
        _audit_exec_event(
            stage="local_execution_guard_duplicate",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details="duplicate local OPEN/PENDING found",
        )
        return False, msg, {"lang": lang}

    try:
        from utils.market_hours import is_within_us_cash_session_utc
        ok_sess, sess_reason = is_within_us_cash_session_utc()
    except Exception:
        ok_sess, sess_reason = True, "OK"
    if not ok_sess:
        msg = (
            f"⏭️ Skipped: Outside Market Hours ({sess_reason}) — {symbol}"
            if lang == "en"
            else f"⏭️ تم التخطي: خارج ساعات السوق ({sess_reason}) — {symbol}"
        )
        if not is_maintenance_mode():
            send_telegram_message(chat_id, msg)
        return False, msg, {"lang": lang}

    allowed, reason = can_open_trade(chat_id, is_pending_trigger=is_pending_trigger)
    if not allowed:
        return False, f"🚫 {reason}", {"lang": lang}

    creds = get_user_credentials(chat_id)
    if not creds:
        msg = "❌ User not registered"
        _maybe_notify_rejection(chat_id, msg, symbol=symbol, action=action, stage="user_not_registered")
        return False, msg, {"lang": lang}

    base_url, headers = get_session(creds, chat_id=str(chat_id))
    if not headers:
        msg = "❌ Capital.com authentication failed"
        _maybe_notify_rejection(chat_id, msg, symbol=symbol, action=action, stage="capital_auth_failed")
        return False, msg, {"lang": lang}

    balance, free_margin = _get_balance_and_free_margin(base_url, headers)
    within_dd, dd_pct = check_daily_drawdown(chat_id, balance)
    if not within_dd:
        msg = f"🔴 Daily drawdown limit reached ({dd_pct:.1f}%) — no new entries today."
        _maybe_notify_rejection(chat_id, msg, symbol=symbol, action=action, stage="daily_drawdown")
        return False, msg, {"lang": lang}

    # Correlation/Sector guard (local DB only: no extra broker API calls here).
    group = _symbol_group(symbol)
    if group and float(balance) > 0:
        group_notional = _local_group_notional(str(chat_id), group)
        exposure_pct = float(group_notional) / max(float(balance), 1e-9)
        if exposure_pct > float(_CORRELATED_GROUP_MAX_EXPOSURE_PCT):
            msg = (
                f"🚫 Correlation guard: {group} exposure {exposure_pct * 100.0:.1f}% "
                f"> {float(_CORRELATED_GROUP_MAX_EXPOSURE_PCT) * 100.0:.1f}%."
            )
            _audit_exec_event(
                stage="correlation_guard_reject",
                chat_id=str(chat_id),
                symbol=str(symbol),
                action=str(action),
                details=(
                    f"group={group} group_notional={group_notional:.2f} "
                    f"equity={float(balance):.2f} exposure_pct={exposure_pct * 100.0:.4f}"
                ),
            )
            return False, msg, {"lang": lang}

    order_epic = resolve_epic_for_user(
        chat_id, symbol, base_url=base_url, headers=headers, is_demo=bool(creds[2])
    )
    if not order_epic:
        msg = f"⏭️ Skipped ({symbol} {action}): symbol not available on broker"
        return False, msg, {"lang": lang}

    pos_res, headers = _resilient_capital_get(
        f"{base_url}/positions",
        headers=headers,
        timeout=20,
        chat_id=str(chat_id),
        creds=creds,
    )
    if pos_res is None:
        msg = f"❌ Order failed ({symbol} {action}): broker request failed"
        _maybe_notify_rejection(chat_id, msg, symbol=symbol, action=action, stage="broker_positions_request")
        return False, msg, {"lang": lang}
    if pos_res.status_code == 200:
        for p in pos_res.json().get("positions", []):
            m = p.get("market", {})
            live_epic = str(m.get("epic", "")).upper()
            live_name = str(m.get("instrumentName", "")).upper()
            if live_epic == str(order_epic).upper() or symbol.upper() in live_name:
                return False, f"⚠️ Position already open for {symbol} ({order_epic}) — monitoring.", {"lang": lang}

    return True, "", {
        "lang": lang,
        "open_reason": str(reason or ""),
        "creds": creds,
        "base_url": base_url,
        "headers": headers,
        "balance": float(balance),
        "free_margin": float(free_margin),
        "order_epic": str(order_epic),
        "scanner_ctx": _scanner_context_from_creds(str(chat_id), creds),
    }


def _generate_protection_levels(
    *,
    chat_id: str,
    symbol: str,
    action: str,
    confidence: float,
    stop_loss_pct: float | None,
    strategy_label: str | None,
    base_url: str,
    headers: dict,
    order_epic: str,
    scanner_ctx: dict,
    lang: str,
) -> tuple[bool, str, dict]:
    """Build entry/ATR/SL and enforce RR gate."""
    entry_price = _get_current_price(base_url, headers, order_epic)
    multi_tf_atr_value, _, _ = _weighted_multi_tf_atr(str(symbol), session_context=scanner_ctx)
    atr = (
        float(multi_tf_atr_value)
        if multi_tf_atr_value is not None and math.isfinite(float(multi_tf_atr_value))
        else calculate_atr(symbol, session_context=scanner_ctx)
    )
    if not isinstance(entry_price, (int, float)) or not math.isfinite(float(entry_price)) or float(entry_price) <= 0:
        msg = f"❌ Order failed ({symbol} {action}): invalid broker entry price"
        _log_trade_rejection(chat_id, symbol, action, "entry_price_validation", msg, f"entry={entry_price}")
        return False, msg, {}

    is_tradeable, m_status = _market_tradeability(base_url, headers, order_epic)
    if not is_tradeable:
        msg = (
            f"⏭️ Trade skipped: Market Closed for {symbol} ({m_status})"
            if lang == "en"
            else f"⏭️ تم التخطي: السوق مغلق لـ {symbol} ({m_status})"
        )
        return False, msg, {}

    effective_sl_pct = stop_loss_pct if stop_loss_pct is not None else (
        (atr * 2.0 / entry_price) if (atr and entry_price > 0) else 0.01
    )
    try:
        effective_sl_pct = float(effective_sl_pct)
    except Exception:
        effective_sl_pct = 0.01
    if (not math.isfinite(effective_sl_pct)) or effective_sl_pct <= 0:
        effective_sl_pct = 0.01

    base_stop_price = (
        entry_price * (1 - effective_sl_pct) if action == "BUY"
        else entry_price * (1 + effective_sl_pct)
    )
    min_dist = _get_min_stop_profit_distance(base_url, headers, order_epic)
    max_dist = _get_max_stop_profit_distance(base_url, headers, order_epic)

    tf: dict = {}
    df_15m_sl = None
    rsi_15m_gate = None
    regime_type = "UNKNOWN"
    try:
        tf = scan_multi_timeframe(str(symbol), session_context=scanner_ctx) or {}
        df_15m_sl = tf.get("15m")
        if df_15m_sl is not None and len(df_15m_sl) > 0:
            rsi_15m_gate = compute_last_rsi(df_15m_sl["Close"])
            try:
                from utils.ai_model import regime_type as _ai_regime_type
                regime_type = _ai_regime_type(df_15m_sl)
            except Exception:
                regime_type = "UNKNOWN"
    except Exception:
        tf = {}
        df_15m_sl = None
        rsi_15m_gate = None
        regime_type = "UNKNOWN"

    try:
        if df_15m_sl is not None and len(df_15m_sl) > 0:
            from core.market_structure import build_liquidity_map
            liq = build_liquidity_map(df_15m_sl)
            liq_levels = {
                "pdh": getattr(liq, "pdh", None),
                "pdl": getattr(liq, "pdl", None),
                "orh": getattr(liq, "orh", None),
                "orl": getattr(liq, "orl", None),
            }
            stop_price, stop_reason, stop_meta = generate_institutional_stop_loss(
                direction=str(action),
                entry_price=float(entry_price),
                df_15m=df_15m_sl,
                liquidity_levels=liq_levels,
                atr_value=float(atr) if atr is not None else None,
                min_stop_distance=min_dist,
                max_stop_distance=max_dist,
            )
        else:
            stop_price, stop_reason, stop_meta = base_stop_price, "fallback_base_stop", {}
    except Exception:
        stop_price, stop_reason, stop_meta = base_stop_price, "fallback_base_stop_exception", {}

    rr_ok, rr_ratio, rr_reason = check_rr_ratio(
        float(entry_price),
        float(stop_price),
        str(action),
        float(atr) if (atr is not None and math.isfinite(float(atr))) else 0.0,
    )
    if isinstance(rr_ratio, float) and (math.isnan(rr_ratio) or math.isinf(rr_ratio)):
        rr_ok = False
        rr_reason = "invalid_rr_nan"
    if not rr_ok:
        if rr_reason == "target_beyond_atr_limit":
            msg = f"❌ R:R {rr_ratio:.1f}:1 rejected — target not achievable within ATR limit ({symbol} {action})"
        elif rr_reason == "invalid_rr_nan":
            msg = f"❌ R:R nan:1 does not meet minimum 1:2 — setup discarded ({symbol} {action})"
        else:
            msg = f"❌ R:R {rr_ratio:.1f}:1 does not meet minimum 1:2 — setup discarded ({symbol} {action})"
        _log_trade_rejection(chat_id, symbol, action, "rr_gate", msg, f"{rr_reason or ''} stop_reason={stop_reason} stop_meta={stop_meta}")
        return False, msg, {}

    return True, "", {
        "entry_price": float(entry_price),
        "atr": (float(atr) if atr is not None and math.isfinite(float(atr)) else None),
        "effective_sl_pct": float(effective_sl_pct),
        "stop_price": float(stop_price),
        "min_dist": min_dist,
        "max_dist": max_dist,
        "rsi_15m_gate": rsi_15m_gate,
        "rr_ratio": float(rr_ratio),
        "stop_reason": stop_reason,
        "stop_meta": stop_meta,
        "regime_type": str(regime_type),
        "strategy_label": str(strategy_label or ""),
        "confidence": float(confidence),
    }


def _calculate_position_sizing(
    *,
    chat_id: str,
    symbol: str,
    action: str,
    confidence: float,
    strategy_label: str | None,
    base_url: str,
    headers: dict,
    order_epic: str,
    balance: float,
    free_margin: float,
    entry_price: float,
    stop_price: float,
    effective_sl_pct: float,
    rsi_15m_gate,
    regime_type: str = "UNKNOWN",
) -> tuple[bool, str, dict]:
    """Validate exposure/margin then produce pre-trade position size."""
    lev = get_effective_leverage(str(chat_id))
    sym_exposure, total_exposure, exposure_by_symbol = _current_exposure_notional(
        base_url=base_url,
        headers=headers,
        symbol=str(symbol),
        order_epic=str(order_epic),
    )
    approved_pre, pre_reason, pre_details = validate_pre_trade(
        symbol=str(symbol),
        entry_price=float(entry_price),
        stop_loss=float(stop_price),
        leverage=float(lev),
        account_balance=float(balance),
        free_margin=float(free_margin),
        confidence=float(confidence),
        chat_id=str(chat_id),
        current_symbol_exposure=float(sym_exposure),
        current_total_exposure=float(total_exposure),
        exposure_by_symbol=exposure_by_symbol,
        action=str(action),
        strategy_label=str(strategy_label or ""),
        rsi_15m=rsi_15m_gate,
        regime_type=regime_type,
    )
    if not approved_pre:
        msg = str(pre_reason or "Trade rejected: pre-trade validation failed")
        return False, msg, {}
    pretrade_size = float(pre_details.get("position_size") or 0.0)
    size = pretrade_size if pretrade_size >= 1.0 else calculate_position_size(
        float(balance), float(confidence), float(entry_price), float(effective_sl_pct), str(chat_id), regime_type
    )
    return True, "", {
        "leverage": float(lev),
        "pre_details": pre_details,
        "pretrade_size": pretrade_size,
        "size": float(size),
    }


def _check_slippage_safety(
    *,
    chat_id: str,
    symbol: str,
    action: str,
    signal_price: float | None,
    base_url: str,
    headers: dict,
    order_epic: str,
) -> tuple[bool, str, float]:
    """Abort if signal price drift exceeds MAX_SLIPPAGE_PCT."""
    signal_ref_price = None
    try:
        if signal_price is not None and math.isfinite(float(signal_price)) and float(signal_price) > 0:
            signal_ref_price = float(signal_price)
    except Exception:
        signal_ref_price = None
    if signal_ref_price is None:
        return True, "", 0.0
    latest_px = _get_current_price(base_url, headers, order_epic)
    if not isinstance(latest_px, (int, float)) or not math.isfinite(float(latest_px)) or float(latest_px) <= 0:
        return True, "", 0.0
    calculated_slippage_pct = abs(float(latest_px) - float(signal_ref_price)) / float(signal_ref_price)
    if calculated_slippage_pct > float(MAX_SLIPPAGE_PCT):
        msg = (
            f"[SLIPPAGE] Trade aborted: Current price move "
            f"({calculated_slippage_pct * 100.0:.2f}%) exceeds max slippage "
            f"({float(MAX_SLIPPAGE_PCT) * 100.0:.1f}%)."
        )
        _bump_execution_shield_counter(
            "slippage_aborts",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details=(
                f"signal={float(signal_ref_price):.4f} latest={float(latest_px):.4f} "
                f"slippage_pct={calculated_slippage_pct * 100.0:.4f}"
            ),
        )
        return False, msg, calculated_slippage_pct
    return True, "", calculated_slippage_pct


def _execute_broker_order(
    *,
    base_url: str,
    headers: dict,
    order_epic: str,
    action: str,
    leg_size: float,
    stop_level: float,
    leg_target: float,
    entry_price: float,
    min_dist,
    target1: float,
    creds,
    chat_id: str,
    opened_broker_legs: list[dict],
) -> tuple[bool, str, float]:
    """
    Final layer touching Capital.com API for one execution leg.
    Returns (ok, error_message, effective_target).
    """
    attempts = 2 if str(len(opened_broker_legs)) == "1" else 1
    last_err = ""
    current_target = float(leg_target)
    for attempt in range(attempts):
        ok, out = _open_position_with_protection(
            base_url,
            headers,
            order_epic,
            action,
            leg_size,
            stop_level,
            current_target,
            creds=creds,
            chat_id=str(chat_id),
        )
        if not ok:
            last_err = str(out)
            if attempt == 0 and min_dist and float(min_dist) > 0 and entry_price > 0:
                widen = float(min_dist) * float(TP2_MIN_DISTANCE_BUFFER_MULT)
                current_target = (float(entry_price) + widen) if action == "BUY" else (float(entry_price) - widen)
                stop_level, target1, current_target = _sanitize_protection_levels(
                    action, entry_price, stop_level, target1, current_target
                )
                continue
            return False, last_err or "unknown rejection", float(current_target)

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
            return False, str(cerr), float(current_target)

        capital_oid = (
            payload.get("orderId")
            or payload.get("order_id")
            or (payload.get("position") or {}).get("orderId")
        )
        if capital_oid is not None:
            capital_oid = str(capital_oid).strip() or None
        opened_broker_legs.append(
            {
                "size": float(leg_size),
                "tp": float(current_target),
                "deal_id": str(deal_id),
                "deal_reference": (str(deal_ref).strip() if deal_ref else None),
                "capital_order_id": capital_oid,
            }
        )
        return True, "", float(current_target)
    return False, last_err or "unknown rejection", float(current_target)

def place_trade_for_user(
    chat_id,
    symbol,
    action,
    confidence=75.0,
    stop_loss_pct=None,
    strategy_label=None,
    force_market: bool = False,
    ai_prob: float | None = None,
    signal_price: float | None = None,
    is_pending_trigger: bool = False,
):
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
    gate_ok, gate_msg, gate_ctx = _validate_execution_gate(
        str(chat_id), str(symbol), str(action), is_pending_trigger=is_pending_trigger
    )
    if not gate_ok:
        _audit_exec_event(
            stage="pipeline_abort_validate_execution_gate",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details=str(gate_msg)[:240],
        )
        return gate_msg
    lang = str(gate_ctx.get("lang") or get_subscriber_lang(chat_id))
    reason = str(gate_ctx.get("open_reason") or "")
    creds = gate_ctx["creds"]
    base_url = gate_ctx["base_url"]
    headers = gate_ctx["headers"]
    balance = float(gate_ctx["balance"])
    free_margin = float(gate_ctx["free_margin"])
    order_epic = str(gate_ctx["order_epic"])
    scanner_ctx = gate_ctx["scanner_ctx"]
    
    # Self-Correction logic: If this is an actual placement (not just checking gates),
    # and we are at the open trades limit, cancel all remaining pending limit orders.
    from config import GLOBAL_MAX_OPEN_TRADES
    from core.risk_manager import _get_global_open_trades_count
    if _get_global_open_trades_count() >= int(GLOBAL_MAX_OPEN_TRADES) - 1:
        with sqlite3.connect(DB_PATH) as cx:
            # Cancel all remaining pending orders since we are at capacity
            cx.execute(
                "UPDATE pending_limit_orders SET status='CANCELLED', reason='capacity_pruning' "
                "WHERE status='PENDING'"
            )
            cx.commit()

    prot_ok, prot_msg, prot_ctx = _generate_protection_levels(
        chat_id=str(chat_id),
        symbol=str(symbol),
        action=str(action),
        confidence=float(confidence),
        stop_loss_pct=(float(stop_loss_pct) if stop_loss_pct is not None else None),
        strategy_label=str(strategy_label or ""),
        base_url=base_url,
        headers=headers,
        order_epic=order_epic,
        scanner_ctx=scanner_ctx,
        lang=lang,
    )
    if not prot_ok:
        _audit_exec_event(
            stage="pipeline_abort_generate_protection_levels",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details=str(prot_msg)[:240],
        )
        return prot_msg
    entry_price = float(prot_ctx["entry_price"])
    atr = prot_ctx["atr"]
    effective_sl_pct = float(prot_ctx["effective_sl_pct"])
    stop_price = float(prot_ctx["stop_price"])
    min_dist = prot_ctx["min_dist"]
    max_dist = prot_ctx["max_dist"]
    rsi_15m_gate = prot_ctx["rsi_15m_gate"]
    rr_ratio = float(prot_ctx["rr_ratio"])
    regime_type = str(prot_ctx.get("regime_type") or "UNKNOWN")

    size_ok, size_msg, size_ctx = _calculate_position_sizing(
        chat_id=str(chat_id),
        symbol=str(symbol),
        action=str(action),
        confidence=float(confidence),
        strategy_label=str(strategy_label or ""),
        base_url=base_url,
        headers=headers,
        order_epic=order_epic,
        balance=float(balance),
        free_margin=float(free_margin),
        entry_price=float(entry_price),
        stop_price=float(stop_price),
        effective_sl_pct=float(effective_sl_pct),
        rsi_15m_gate=rsi_15m_gate,
        regime_type=regime_type,
    )
    if not size_ok:
        _audit_exec_event(
            stage="pipeline_abort_calculate_position_sizing",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details=str(size_msg)[:240],
        )
        _maybe_notify_rejection(chat_id, size_msg, symbol=symbol, action=action, stage="pre_trade_validation")
        return size_msg
    pre_details = size_ctx["pre_details"]
    pretrade_size = float(size_ctx["pretrade_size"])

    slip_ok, slip_msg, calculated_slippage_pct = _check_slippage_safety(
        chat_id=str(chat_id),
        symbol=str(symbol),
        action=str(action),
        signal_price=signal_price,
        base_url=base_url,
        headers=headers,
        order_epic=order_epic,
    )
    if not slip_ok:
        _audit_exec_event(
            stage="pipeline_abort_check_slippage_safety",
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            details=str(slip_msg)[:240],
        )
        return slip_msg

    # ── Sprint 2: limit-first entry policy ───────────────────────────────────
    if ENABLE_LIMIT_ORDER_MODE and not force_market:
        limit_px = _calculate_limit_price(
            symbol=str(symbol),
            action=str(action),
            strategy_label=str(strategy_label or ""),
            entry_price=float(entry_price),
            atr=float(atr) if atr is not None else None,
            session_context=scanner_ctx,
            regime_type=regime_type,
        )
        ok_lim, oid, lim_reason = _place_pending_limit_order(
            chat_id=str(chat_id),
            symbol=str(symbol),
            action=str(action),
            strategy_label=str(strategy_label or ""),
            confidence=float(confidence),
            ai_prob=float(ai_prob) if ai_prob is not None else float(confidence),
            stop_loss_pct=float(stop_loss_pct) if stop_loss_pct is not None else None,
            limit_price=float(limit_px),
        )
        if not ok_lim and lim_reason == "already_pending":
            msg = f"⏭️ Limit already pending ({symbol} {action})"
            return msg
        lang = get_subscriber_lang(chat_id)
        ttl_bars = int(LIMIT_ORDER_TTL_BARS)
        _prof = get_user_signal_profile(str(chat_id))
        _lim_pol_en = (
            "⚙️ *Fast Entry Executed*"
            if _prof != "GOLDEN"
            else "⚙️ *Gold Discipline Applied*"
        )
        _lim_pol_ar = (
            "⚙️ *تنفيذ Fast — Fast Entry Executed*"
            if _prof != "GOLDEN"
            else "⚙️ *معيار الذهب — Gold Discipline Applied*"
        )
        if lang == "en":
            msg = (
                f"🧾 *Limit order placed* #{oid}\n"
                f"📌 {symbol} {action}\n"
                f"💰 Limit price: *{float(limit_px):.4f}*\n"
                f"⏱️ TTL: *{ttl_bars} bars* ({int(LIMIT_ORDER_BAR_MINUTES)}m)\n"
                f"{_lim_pol_en}"
            )
        else:
            msg = (
                f"🧾 *تم وضع أمر ليمِت* #{oid}\n"
                f"📌 {symbol} {('شراء' if action=='BUY' else 'بيع')}\n"
                f"💰 سعر الليمِت: *{float(limit_px):.4f}*\n"
                f"⏱️ الصلاحية: *{ttl_bars} شموع* ({int(LIMIT_ORDER_BAR_MINUTES)}م)\n"
                f"{_lim_pol_ar}"
            )
        send_telegram_message(chat_id, msg)
        return f"🧾 Limit placed ({symbol} {action}) @ {float(limit_px):.4f}"

    # ── Position size (1–2% risk, confidence-scaled) ──────────────────────────
    size = pretrade_size if pretrade_size >= 1.0 else calculate_position_size(
        balance, confidence, entry_price, effective_sl_pct, chat_id
    )
    # Protection levels used BOTH for broker order and Telegram report.
    # Stop comes from institutional SL synthesis done pre-trade.
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
        _log_trade_rejection(chat_id, symbol, action, "stop_validation", msg)
        if not SUPPRESS_EXPECTED_REJECTION_TELEGRAM:
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

    # Broker dealing rules: enforce minimum stop/profit distance.
    # Policy (institutional, broker-compliant):
    # - If TP1/TP2 are too tight, widen them to broker minimum distance (with a small buffer)
    #   instead of skipping strong signals.
    min_dist = _get_min_stop_profit_distance(base_url, headers, order_epic)
    tp_adjust_note = ""
    if min_dist and float(min_dist) > 0 and entry_price > 0:
        md = float(min_dist)
        # Enforce broker minimum stop distance as well (not only TP widening).
        stop_level, _ = _apply_min_distance_to_protection(
            action=action,
            entry_price=float(entry_price),
            stop_level=float(stop_level),
            profit_level=None,
            min_dist=md,
        )
        tp1_dist = abs(float(target1) - float(entry_price))
        if tp1_dist < md:
            widen = md * float(TP2_MIN_DISTANCE_BUFFER_MULT)
            target1 = (
                float(entry_price) + widen
                if action == "BUY"
                else float(entry_price) - widen
            )
            if lang == "en":
                tp_adjust_note += (
                    f"\nℹ️ Adjusted: TP1 widened to broker minimum distance ({md:.4f})."
                )
            else:
                tp_adjust_note += (
                    f"\nℹ️ تم التعديل: توسيع الهدف 1 ليتوافق مع الحد الأدنى للوسيط ({md:.4f})."
                )
        tp2_dist = abs(float(target2) - float(entry_price))
        widen2 = md * float(TP2_MIN_DISTANCE_BUFFER_MULT)
        if tp2_dist < md:
            target2 = (
                float(entry_price) + widen2
                if action == "BUY"
                else float(entry_price) - widen2
            )
            if lang == "en":
                tp_adjust_note += (
                    f"\nℹ️ Adjusted: TP2 widened to broker minimum distance ({md:.4f})."
                )
            else:
                tp_adjust_note += (
                    f"\nℹ️ تم التعديل: توسيع الهدف 2 ليتوافق مع الحد الأدنى للوسيط ({md:.4f})."
                )

        # Re-sanitize after any TP adjustments.
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
        _log_trade_rejection(chat_id, symbol, action, "split_qty", msg, split_err or "")
        _maybe_notify_rejection(chat_id, msg, symbol=symbol, action=action, stage="split_qty")
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
    calculated_slippage_pct = float(calculated_slippage_pct or 0.0)

    for idx, leg in enumerate(legs):
        leg_role = str(leg["role"])
        leg_size = float(leg["size"])
        leg_target = float(leg["tp"])

        ok_leg, last_err, leg_target = _execute_broker_order(
            base_url=base_url,
            headers=headers,
            order_epic=order_epic,
            action=action,
            leg_size=float(leg_size),
            stop_level=float(stop_level),
            leg_target=float(leg_target),
            entry_price=float(entry_price),
            min_dist=min_dist,
            target1=float(target1),
            creds=creds,
            chat_id=str(chat_id),
            opened_broker_legs=opened_broker_legs,
        )
        if ok_leg and opened_broker_legs:
            opened_broker_legs[-1]["role"] = leg_role
            deal_id = str(opened_broker_legs[-1].get("deal_id") or "")
            ok_sync, info = _sync_protection_to_broker(
                base_url,
                headers,
                deal_id,
                stop_level,
                float(leg_target),
                creds=creds,
                chat_id=str(chat_id),
            )
            if not ok_sync:
                print(
                    f"⚠️  Broker TP/SL sync failed "
                    f"[{symbol} {deal_id}] sl={stop_level:.4f} tp={float(leg_target):.4f} :: {info}"
                )

        # If this leg failed, rollback any opened legs and abort (no half-configured trade).
        if len(opened_broker_legs) != (idx + 1):
            for ob in reversed(opened_broker_legs):
                ok_del, info = _delete_position(
                    base_url,
                    headers,
                    str(ob.get("deal_id") or ""),
                    creds=creds,
                    chat_id=str(chat_id),
                )
                if not ok_del:
                    print(f"⚠️  Rollback failed for {symbol} deal={ob.get('deal_id')}: {info}")
            reason = last_err or "unknown rejection"
            min_tp = _extract_min_tp_value(reason)
            broker_note = ""
            if min_tp is not None and entry_price and float(entry_price) > 0:
                # Explain common broker rule: TP is too close for this instrument.
                try:
                    if lang == "en":
                        broker_note = f"\nBroker rule: minimum take-profit level is {float(min_tp):.4f}."
                    else:
                        broker_note = f"\nشرط الوسيط: الحد الأدنى للهدف (TP) هو {float(min_tp):.4f}."
                except Exception:
                    broker_note = ""
            msg = (
                f"❌ Order rejected ({symbol} {action}) — {leg_role} failed.\n"
                f"Reason: {reason}{broker_note}"
            )
            _audit_exec_event(
                stage="order_leg_failed",
                chat_id=str(chat_id),
                symbol=str(symbol),
                action=str(action),
                details=f"leg={leg_role} reason={reason[:220]}",
            )
            _log_trade_rejection(chat_id, symbol, action, f"{leg_role}_execution", msg, reason)
            if (not SUPPRESS_EXPECTED_REJECTION_TELEGRAM) or (not _is_expected_rejection(reason)):
                _maybe_notify_rejection(chat_id, msg, symbol=symbol, action=action, stage=f"{leg_role}_execution")
            return msg

    opened_trade_ids: list[int] = []
    
    # We want to pull adx_band and sector_sentiment if available.
    # In executor, we already know regime_type from prot_ctx, but we might need to recalculate the others or just store them.
    # We can default to "UNKNOWN" if not available immediately to keep execution fast.
    adx_band = "UNKNOWN"
    sector_sentiment = "UNKNOWN"
    try:
        from utils.ai_model import _adx, _flatten
        from utils.market_scanner import _SHARED_SESSION_CACHE
        df_15m = _SHARED_SESSION_CACHE.get(str(symbol), {}).get("15m")
        if df_15m is not None and not df_15m.empty:
            adx_val = float(_adx(_flatten(df_15m)).iloc[-1])
            adx_band = "LOW" if adx_val < 25 else "HIGH"
    except Exception:
        pass

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
            capital_order_id=ob.get("capital_order_id"),
            leg_role=str(ob["role"]),
            parent_session=parent_session,
            stop_distance=stop_dist,
            regime=regime_type,
            adx_band=adx_band,
            sector_sentiment=sector_sentiment,
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
        _maybe_notify_rejection(chat_id, err, symbol=symbol, action=action, stage="verify_open_position")
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
    _policy_en = str(pre_details.get("execution_policy_en") or "").strip()
    _policy_ar = str(pre_details.get("execution_policy_ar") or "").strip()
    _policy_block_en = f"\n{_policy_en}" if _policy_en else ""
    _policy_block_ar = f"\n{_policy_ar}" if _policy_ar else ""
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
            f"{_policy_block_en}\n"
            f"🕐  Time (ET)    :  {now_et_str}"
            f"{partial_note_en}"
            f"{sl_adjust_note}"
            f"{tp_adjust_note}"
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
            f"{_policy_block_ar}\n"
            f"🕐  التوقيت (ET)   :  {now_et_str}"
            f"{partial_note_ar}"
            f"{sl_adjust_note}"
            f"{tp_adjust_note}"
        )

    tg_res = send_telegram_message(chat_id, msg)
    # Always surface Telegram delivery failures in engine logs.
    try:
        if isinstance(tg_res, dict) and tg_res.get("ok") is False:
            desc = str(tg_res.get("description") or "").strip()
            print(f"[Telegram] Send failed chat_id={chat_id} desc={desc[:220]}", flush=True)
        elif isinstance(tg_res, dict) and tg_res.get("ok"):
            try:
                touch_signal_delivered(str(chat_id))
            except Exception:
                pass
    except Exception:
        pass

    stop_info = f"{stop_level:.4f}" if stop_level is not None else "N/A"
    _audit_exec_event(
        stage="order_opened",
        chat_id=str(chat_id),
        symbol=str(symbol),
        action=str(action),
        details=(
            f"legs={len(opened_legs)} qty={verified_qty:.2f} entry={entry_price:.4f} "
            f"sl={stop_info} rr={float(rr_ratio):.2f} "
            f"tier={pre_details.get('subscription_tier', 'n/a')} "
            f"calculated_slippage_pct={calculated_slippage_pct * 100.0:.4f} "
            f"multi_tf_atr_value={(float(atr) if atr is not None else 0.0):.6f}"
        ),
    )
    log.info(
        "[TRADE_EXECUTION] symbol=%s action=%s entry=%.4f calculated_slippage=%.4f%% multi_tf_atr_value=%.6f",
        str(symbol),
        str(action),
        float(entry_price),
        float(calculated_slippage_pct * 100.0),
        float(atr) if atr is not None and math.isfinite(float(atr)) else 0.0,
    )
    return (
        f"✅ Opened — legs: {len(opened_legs)} | size: {verified_qty} | "
        f"stop: {stop_info} | RR: {rr_ratio:.1f}"
    )
