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
from datetime import timezone

from core.trade_session_finalize import after_trade_leg_closed
from core.trade_close_messages import (
    send_reconcile_generic_external,
    send_reconcile_tp1_hit,
    send_reconcile_tp2_final,
)
from database.db_manager import get_subscriber_lang
from config import FINAL_SYNC_FALLBACK_ENABLED
from utils.market_hours import utc_now

DB_PATH = 'database/trading_saas.db'
ENABLE_CLOSE_PENDING_NOTIFY = (os.getenv("ENABLE_CLOSE_PENDING_NOTIFY", "false").strip().lower() == "true")
SYNC_RETRY_COOLDOWN_SEC = int(os.getenv("CLOSE_SYNC_RETRY_COOLDOWN_SEC", "30"))
SYNC_MAX_ATTEMPTS = int(os.getenv("CLOSE_SYNC_MAX_ATTEMPTS", "120"))
PENDING_FINAL = "PENDING_FINAL"
SYNCED = "SYNCED"
FINALIZED_NO_PNL = "FINALIZED_NO_PNL"

# After DELETE / manual close, poll /positions before trusting "closed" (2s, 5s, 10s gaps in fetch_closed_deal_final_data).
VERIFY_CLOSE_MAX_POLLS = int(os.getenv("VERIFY_CLOSE_MAX_POLLS", "10"))
VERIFY_CLOSE_POLL_SEC = float(os.getenv("VERIFY_CLOSE_POLL_SEC", "0.6"))
VERIFY_CLOSE_CONSECUTIVE_ABSENT = max(1, int(os.getenv("VERIFY_CLOSE_CONSECUTIVE_ABSENT", "2")))


def _capital_norm_id(v) -> str:
    if v is None:
        return ""
    return re.sub(r"[^a-zA-Z0-9]", "", str(v)).lower()


def _capital_ids_match(a: str, b: str) -> bool:
    """Same deal can appear as dealId vs positionId or with formatting drift."""
    na = _capital_norm_id(a)
    nb = _capital_norm_id(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if len(na) >= 8 and nb.endswith(na):
        return True
    if len(nb) >= 8 and na.endswith(nb):
        return True
    return False


def _capital_all_ids_from_row(p: dict) -> set[str]:
    """Collect dealId + positionId from both top-level and nested position (Capital varies)."""
    out: set[str] = set()
    if not isinstance(p, dict):
        return out
    pos = p.get("position") or {}
    if not isinstance(pos, dict):
        pos = {}
    for obj in (pos, p):
        if not isinstance(obj, dict):
            continue
        for k in ("dealId", "positionId"):
            v = obj.get(k)
            if v is not None:
                out.add(str(v).strip())
    return out


def _capital_deal_id_from_row(p: dict) -> str | None:
    """First available id (legacy callers)."""
    ids = _capital_all_ids_from_row(p)
    return next(iter(ids)) if ids else None


def _now_utc() -> _dt:
    return utc_now()


def _parse_iso_utc(value: str | None) -> _dt | None:
    if not value:
        return None
    try:
        dt = _dt.fromisoformat(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _capital_row_matches_symbol_direction_size(
    p: dict,
    *,
    symbol: str | None,
    direction: str | None,
    size: float | None,
) -> bool:
    """Last-resort match when dealId/positionId in DB drifted from API strings."""
    if not symbol or not direction:
        return False
    m = p.get("market") or {}
    pos = p.get("position") or {}
    if not isinstance(pos, dict):
        return False
    epic = str(m.get("epic", "") or m.get("instrumentName", "") or "").upper()
    sym = str(symbol).strip().upper()
    sym_tok = sym.split(".")[-1].split(":")[-1]
    epic_ok = sym in epic or sym_tok and sym_tok in epic
    if not epic_ok:
        return False
    if str(pos.get("direction")) != str(direction):
        return False
    if size is None:
        return True
    try:
        sz = float(pos.get("size") or 0)
    except (TypeError, ValueError):
        return False
    return abs(sz - float(size)) <= 0.51


def capital_deal_still_open(
    base_url: str,
    headers: dict,
    deal_id: str,
    *,
    symbol: str | None = None,
    direction: str | None = None,
    size: float | None = None,
) -> bool:
    """
    True if the broker still lists this deal/position as OPEN.

    Conservative rules (false "closed" is unacceptable):
    - Non-200 on GET /positions → assume OPEN (cannot verify absence).
    - Any row in /positions whose dealId OR positionId matches (normalized) → OPEN.
    - If /positions returns at least one open row but NO id match for our deal_id,
      assume OPEN anyway (ID drift / TP split / API shape) — log once.
    - GET /positions/{deal_id} returning 404 while the list is non-empty and did not
      match is treated as ambiguous id, not as proof of absence.
    """
    if not deal_id:
        return False
    try:
        res = requests.get(f"{base_url}/positions", headers=headers, timeout=20)
        if res.status_code != 200:
            return True
        positions = (res.json() or {}).get("positions", []) or []
        for p in positions:
            for rid in _capital_all_ids_from_row(p):
                if _capital_ids_match(deal_id, rid):
                    return True
            if symbol and direction and _capital_row_matches_symbol_direction_size(
                p, symbol=symbol, direction=direction, size=size
            ):
                print(
                    f"[Capital sync] Matched open row by epic+direction+size for deal_id={deal_id} "
                    f"symbol={symbol} (id drift safety)",
                    flush=True,
                )
                return True
        if len(positions) > 0 and symbol and str(symbol).strip():
            sym_u = str(symbol).strip().upper()
            sym_tok = sym_u.split(".")[-1].split(":")[-1]
            for p in positions:
                m = p.get("market") or {}
                epic = str(m.get("epic", "") or m.get("instrumentName", "") or "").upper()
                if sym_u in epic or (sym_tok and sym_tok in epic):
                    print(
                        f"[Capital sync] CONSERVATIVE: open row(s) on same symbol {sym_u} but no id match "
                        f"for deal_id={deal_id} — treating as STILL OPEN",
                        flush=True,
                    )
                    return True

        res2 = requests.get(
            f"{base_url}/positions/{deal_id}", headers=headers, timeout=15
        )
        if res2.status_code == 200:
            data = res2.json() or {}
            if data.get("errorCode"):
                return True
            if _capital_all_ids_from_row(data):
                return True
            pos = data.get("position") or {}
            if isinstance(pos, dict) and (
                pos.get("dealId") is not None or pos.get("positionId") is not None
            ):
                return True
        # List empty + detail not 200 with position: treat as flat only when list was empty.
        if res2.status_code == 404 and len(positions) == 0:
            return False
        if res2.status_code == 404 and len(positions) > 0:
            # If no id/symbol/direction/size match was found in /positions, a 404 on
            # /positions/{deal_id} is strong evidence this exact deal is no longer open.
            # Other open rows may exist for different instruments/users, so keep them
            # untouched while allowing this deal to reconcile to CLOSED.
            print(
                f"[Capital sync] detail 404 with non-matching open list for deal_id={deal_id} — CLOSED",
                flush=True,
            )
            return False
    except Exception:
        return True
    return False


def capital_verify_deal_closed_after_close_request(
    base_url: str,
    headers: dict,
    deal_id: str,
    *,
    symbol: str | None = None,
    direction: str | None = None,
    size: float | None = None,
) -> bool:
    """
    After DELETE /positions/{id}, poll until GET /positions shows the deal absent.
    Requires VERIFY_CLOSE_CONSECUTIVE_ABSENT consecutive "absent" reads (default 2) so a
    single flaky empty response cannot confirm a close.
    """
    if not deal_id:
        return False
    n = max(1, VERIFY_CLOSE_MAX_POLLS)
    need_absent = max(1, VERIFY_CLOSE_CONSECUTIVE_ABSENT)
    absent_streak = 0
    for i in range(n):
        still = capital_deal_still_open(
            base_url,
            headers,
            str(deal_id),
            symbol=symbol,
            direction=direction,
            size=size,
        )
        if not still:
            absent_streak += 1
            print(
                f"[Capital verify] deal {deal_id} absent poll streak {absent_streak}/{need_absent} "
                f"(iteration {i + 1}/{n})",
                flush=True,
            )
            if absent_streak >= need_absent:
                return True
        else:
            absent_streak = 0
        time.sleep(max(0.1, VERIFY_CLOSE_POLL_SEC))
    print(
        f"[Capital verify] deal {deal_id} STILL OPEN on broker after {n} poll(s) — close not confirmed",
        flush=True,
    )
    return False


def _send_pending_close_notice(chat_id, symbol, direction, trade_id):
    try:
        # Last chance before sending placeholder text: force one quick broker sync.
        emergency = try_sync_final_for_trade_id(int(trade_id), attempts=1, delay_sec=0.0)
        if emergency is not None:
            from core.trade_close_messages import send_bot_automated_close_from_db
            send_bot_automated_close_from_db(int(trade_id))
            return

        from bot.notifier import send_telegram_message
        lang = get_subscriber_lang(chat_id)
        msg = (
            "⏳ *Trade close detected*\n\n"
            f"📌 Asset: *{symbol}* ({direction})\n"
            f"🆔 Trade ID: *{int(trade_id)}*\n"
            "Final P&L is pending broker history sync."
            if lang == "en"
            else
            "⏳ *تم رصد إغلاق الصفقة*\n\n"
            f"📌 الأداة: *{symbol}* ({'شراء' if direction=='BUY' else 'بيع'})\n"
            f"🆔 رقم الصفقة: *{int(trade_id)}*\n"
            "الربح/الخسارة النهائية قيد مزامنة سجل الوسيط."
        )
        send_telegram_message(chat_id, msg)
    except Exception:
        pass


def _send_final_no_pnl_notice(chat_id, symbol, direction, trade_id):
    try:
        from bot.notifier import send_telegram_message
        lang = get_subscriber_lang(chat_id)
        if lang == "en":
            msg = (
                "⚠️ *Trade Closed — Final P&L unavailable*\n\n"
                f"📌 Asset: *{symbol}* ({direction})\n"
                f"🆔 Trade ID: *{int(trade_id)}*\n"
                "The broker closed this trade, but Capital history did not return realized P&L after retries."
            )
        else:
            msg = (
                "⚠️ *تم إغلاق الصفقة — الربح/الخسارة النهائية غير متاحة*\n\n"
                f"📌 الأداة: *{symbol}* ({'شراء' if direction=='BUY' else 'بيع'})\n"
                f"🆔 رقم الصفقة: *{int(trade_id)}*\n"
                "تم تأكيد إغلاق الصفقة لدى الوسيط، لكن سجل Capital لم يُرجع الربح/الخسارة بعد عدة محاولات."
            )
        send_telegram_message(chat_id, msg)
    except Exception:
        pass


def mark_trade_closed_pending(
    chat_id,
    trade_id: int,
    *,
    symbol: str,
    direction: str,
    deal_reference: str | None = None,
    reason: str = "awaiting_broker_history",
    notify: bool = True,
):
    """
    Mark as CLOSED immediately when broker no longer has the position, even if
    final realized PnL is delayed in history.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "UPDATE trades SET status='CLOSED', closed_at=?, sync_status=?, "
        "close_sync_last_error=?, close_reason=COALESCE(close_reason, ?), "
        "deal_reference=COALESCE(NULLIF(?,''), deal_reference), close_sync_notified=1 "
        "WHERE trade_id=? AND status='OPEN'",
        (
            _now_utc().isoformat(),
            PENDING_FINAL,
            str(reason),
            "pending_final_sync",
            str(deal_reference or "").strip(),
            int(trade_id),
        ),
    )
    changed = c.rowcount == 1
    if changed:
        conn.commit()
    conn.close()
    # User-facing "pending sync" close notices are optional and disabled by default.
    if changed and notify and ENABLE_CLOSE_PENDING_NOTIFY:
        _send_pending_close_notice(chat_id, symbol, direction, trade_id)
    return changed


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

    def _parse_commission_total(tx: dict) -> float:
        """
        Sum known cost fields when provided by broker payload.
        Returned value is positive (absolute cost in account currency units).
        """
        total = 0.0
        for k in (
            "commission",
            "commissions",
            "fee",
            "fees",
            "charge",
            "charges",
            "spreadCost",
            "financing",
            "financingCharge",
            "rollover",
            "swap",
        ):
            if k in tx and tx.get(k) is not None:
                v = _parse_float(tx.get(k))
                if v is not None:
                    total += abs(float(v))
        return float(total)

    def _parse_pnl(tx: dict) -> float | None:
        # Prefer net/realized fields first (already include broker costs).
        for k in (
            "netProfitAndLoss",
            "netPnl",
            "realisedPnl",
            "realizedPnl",
            "profitAndLoss",
            "profitAndLossValue",
            "pnl",
        ):
            if k in tx and tx.get(k) is not None:
                p = _parse_float(tx.get(k))
                if p is not None:
                    return float(p)
        # Fallback: gross PnL minus explicit costs when present.
        for k in (
            "grossProfitAndLoss",
            "grossPnl",
            "profitAndLossBeforeCharges",
        ):
            if k in tx and tx.get(k) is not None:
                gross = _parse_float(tx.get(k))
                if gross is not None:
                    return float(gross) - _parse_commission_total(tx)
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

    # Capital history can lag after close. Retry with 2s, 5s, 10s (then 10s) between attempts.
    attempts = 12 if wait_for_realized else 3
    post_fail_delays = [2.0, 5.0, 10.0]

    ids = [str(deal_id)]
    if identifiers:
        ids.extend([str(x).strip() for x in identifiers if str(x).strip()])
    # De-dup while preserving order
    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    def _norm_id(v) -> str:
        if v is None:
            return ""
        return re.sub(r"[^a-zA-Z0-9]", "", str(v)).lower()

    def _id_match(a, b) -> bool:
        na = _norm_id(a)
        nb = _norm_id(b)
        if not na or not nb:
            return False
        if na == nb:
            return True
        # Capital may return shorter/alternate formatted references in history.
        # Accept suffix matches when enough entropy exists.
        if len(na) >= 8 and nb.endswith(na):
            return True
        if len(nb) >= 8 and na.endswith(nb):
            return True
        return False

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
            tx_opening = tx.get("openingDealId") or tx.get("openDealId") or tx.get("positionId")
            match = False
            for x in ids:
                if (
                    _id_match(tx_deal, x)
                    or _id_match(tx_ref, x)
                    or _id_match(tx_related, x)
                    or _id_match(tx_reference, x)
                    or _id_match(tx_opening, x)
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

        if wait_for_realized and attempt < attempts - 1:
            idx = min(attempt, len(post_fail_delays) - 1)
            time.sleep(post_fail_delays[idx])

    # Stop-condition: not found / not ready.
    print("Error: Could not sync final data from Capital.com", flush=True)
    print(f"[Capital Sync] transaction not found yet ids={ids}", flush=True)
    return None


def try_sync_final_for_trade_id(
    trade_id: int,
    *,
    attempts: int = 3,
    delay_sec: float = 2.0,
) -> dict | None:
    """
    Emergency one-trade sync:
    - Build a broker session from subscriber creds
    - Query Capital history for realized close data
    - Persist pnl/actual_pnl/exit_price before user-facing notifications
    """
    try:
        tid = int(trade_id)
    except Exception:
        return None

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    row = c.execute(
        "SELECT chat_id, deal_id, COALESCE(deal_reference,''), symbol, direction, "
        "entry_price, size FROM trades WHERE trade_id=?",
        (tid,),
    ).fetchone()
    if not row:
        conn.close()
        return None
    chat_id, deal_id, deal_reference, symbol, direction, entry_price, size = row
    if not deal_id:
        conn.close()
        return None

    creds = c.execute(
        "SELECT api_key, api_password, is_demo, email FROM subscribers WHERE chat_id=?",
        (str(chat_id),),
    ).fetchone()
    conn.close()
    if not creds:
        return None

    try:
        from bot.licensing import safe_decrypt

        api_key = str(safe_decrypt(creds[0] or "")).strip()
        password = str(safe_decrypt(creds[1] or "")).strip()
        is_demo = bool(creds[2])
        email = str(safe_decrypt(creds[3] or "")).strip()
    except Exception:
        return None
    if not api_key or not password or not email:
        return None

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
    try:
        auth_res = requests.post(
            f"{base_url}/session",
            json={"identifier": email, "password": password},
            headers=headers,
            timeout=20,
        )
        if auth_res.status_code != 200:
            return None
        session_headers = {
            **headers,
            "CST": auth_res.headers.get("CST"),
            "X-SECURITY-TOKEN": auth_res.headers.get("X-SECURITY-TOKEN"),
        }
    except Exception:
        return None

    final = None
    tries = max(1, int(attempts))
    for idx in range(tries):
        final = fetch_closed_deal_final_data(
            base_url,
            session_headers,
            str(deal_id),
            wait_for_realized=False,
            identifiers=[str(deal_reference).strip()] if str(deal_reference or "").strip() else None,
        )
        if final is not None:
            break
        if idx < tries - 1:
            time.sleep(max(0.0, float(delay_sec)))
    if not final:
        return None

    pnl_val = float(final.get("actual_pnl", 0.0))
    exit_price = final.get("exit_price")
    try:
        ep = float(entry_price or 0.0)
        qty = float(size or 0.0)
        side = 1.0 if str(direction).upper() == "BUY" else -1.0
    except Exception:
        ep = 0.0
        qty = 0.0
        side = 1.0

    # Fallback calculation when broker returned exit but pnl is still missing/zero.
    if float(pnl_val) == 0.0 and exit_price is not None and ep > 0 and qty > 0:
        pnl_val = (float(exit_price) - ep) * qty * side

    conn2 = sqlite3.connect(DB_PATH)
    conn2.execute(
        "UPDATE trades SET status='CLOSED', pnl=?, actual_pnl=?, exit_price=COALESCE(?, exit_price), "
        "sync_status=?, close_sync_last_error=NULL, closed_at=COALESCE(closed_at, ?) "
        "WHERE trade_id=?",
        (
            float(pnl_val),
            float(pnl_val),
            float(exit_price) if exit_price is not None else None,
            SYNCED,
            _now_utc().isoformat(),
            int(tid),
        ),
    )
    conn2.commit()
    conn2.close()
    print(
        f"[PROFIT_FIX] Trade {int(tid)} closed at "
        f"{('n/a' if exit_price is None else float(exit_price))}. Realized PnL: {float(pnl_val):.2f}",
        flush=True,
    )
    return {"actual_pnl": float(pnl_val), "exit_price": exit_price}


def reconcile(chat_id, base_url, headers, *, notify: bool = True):
    """
    Sync local DB with live Capital.com /positions.
    Should be called once per monitoring cycle before any stop checks.
    """
    pos_res = requests.get(f"{base_url}/positions", headers=headers)
    if pos_res.status_code != 200:
        return

    live_positions = pos_res.json().get('positions', [])
    # Every dealId + positionId from each row (DB may store either).
    live_deal_ids = set()
    for p in live_positions:
        for pid in _capital_all_ids_from_row(p):
            live_deal_ids.add(str(pid).strip())

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
            # Gold rule: list membership can race; confirm the broker truly has no open position.
            try:
                sz_f = float(size) if size is not None else None
            except (TypeError, ValueError):
                sz_f = None
            if capital_deal_still_open(
                base_url,
                headers,
                str(deal_id),
                symbol=str(symbol or ""),
                direction=str(direction or ""),
                size=sz_f,
            ):
                continue
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
                    last_try = _parse_iso_utc(str(close_sync_last_try_at))
                    if last_try is None:
                        raise ValueError("invalid close_sync_last_try_at")
                    delta = (_now_utc() - last_try).total_seconds()
                    if delta < max(1, SYNC_RETRY_COOLDOWN_SEC):
                        continue
                except Exception:
                    pass

            c.execute(
                "UPDATE trades SET close_sync_attempts=COALESCE(close_sync_attempts,0)+1, "
                "close_sync_last_try_at=? WHERE trade_id=? AND status='OPEN'",
                (_now_utc().isoformat(), int(trade_id)),
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
                # Hard check: never mark CLOSED / notify if the broker still lists this leg.
                if capital_deal_still_open(
                    base_url,
                    headers,
                    str(deal_id),
                    symbol=str(symbol or ""),
                    direction=str(direction or ""),
                    size=sz_f,
                ):
                    print(
                        f"[Capital Sync] ABORT mark_trade_closed_pending: trade_id={trade_id} "
                        f"deal_id={deal_id} still OPEN at broker (history miss, position present)",
                        flush=True,
                    )
                    continue
                mark_trade_closed_pending(
                    chat_id,
                    int(trade_id),
                    symbol=symbol,
                    direction=direction,
                    deal_reference=dr,
                    reason="transaction_not_found_or_not_realized",
                    # Close events are critical and must always notify.
                    notify=True,
                )
                continue

            pnl = float(final["actual_pnl"])
            exit_price = final.get("exit_price")
            cur = c.execute(
                "UPDATE trades SET status='CLOSED', pnl=?, actual_pnl=?, exit_price=?, closed_at=?, "
                "close_sync_last_error=NULL, sync_status=? "
                "WHERE trade_id=? AND status='OPEN'",
                (
                    pnl,
                    pnl,
                    float(exit_price) if exit_price is not None else None,
                    _now_utc().isoformat(),
                    SYNCED,
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
            # Close notifications are mandatory; do not silence with notify=False.

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

    # ── Case 1b: closed pending final sync ───────────────────────────────────
    c.execute(
        "SELECT trade_id, deal_id, COALESCE(deal_reference,''), symbol, direction, entry_price, size, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), stop_distance, trailing_stop, "
        "COALESCE(close_sync_attempts,0), close_sync_last_try_at "
        "FROM trades WHERE chat_id=? AND status='CLOSED' AND COALESCE(sync_status,'')=?",
        (chat_id, PENDING_FINAL),
    )
    pending_rows = c.fetchall()
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
        close_sync_attempts,
        close_sync_last_try_at,
    ) in pending_rows:
        if not deal_id:
            continue
        try:
            attempts_i = int(close_sync_attempts or 0)
        except Exception:
            attempts_i = 0
        if attempts_i >= SYNC_MAX_ATTEMPTS:
            if FINAL_SYNC_FALLBACK_ENABLED:
                try:
                    sz_pf = float(size) if size is not None else None
                except (TypeError, ValueError):
                    sz_pf = None
                # Never tell the user "closed / no PnL" if the position is still open at the broker.
                if deal_id and capital_deal_still_open(
                    base_url,
                    headers,
                    str(deal_id),
                    symbol=str(symbol or ""),
                    direction=str(direction or ""),
                    size=sz_pf,
                ):
                    try:
                        from bot.notifier import notify_admin_alert

                        notify_admin_alert(
                            "Desync (PENDING_FINAL): DB CLOSED but broker still shows OPEN position.\n"
                            f"chat_id={chat_id} trade_id={trade_id} deal_id={deal_id} {symbol} {direction}\n"
                            "Skipped misleading 'PnL unavailable' user message."
                        )
                    except Exception:
                        pass
                    continue
                c.execute(
                    "SELECT COALESCE(sync_status,''), COALESCE(close_sync_last_error,'') FROM trades WHERE trade_id=?",
                    (int(trade_id),),
                )
                st_row = c.fetchone() or ("", "")
                if st_row[0] != FINALIZED_NO_PNL:
                    c.execute(
                        "UPDATE trades SET sync_status=?, close_sync_last_error=? WHERE trade_id=?",
                        (FINALIZED_NO_PNL, "finalized_without_broker_pnl", int(trade_id)),
                    )
                    conn.commit()
                    if notify:
                        _send_final_no_pnl_notice(chat_id, symbol, direction, int(trade_id))
            continue
        if close_sync_last_try_at:
            try:
                last_try = _parse_iso_utc(str(close_sync_last_try_at))
                if last_try is None:
                    raise ValueError("invalid close_sync_last_try_at")
                if (_now_utc() - last_try).total_seconds() < max(1, SYNC_RETRY_COOLDOWN_SEC):
                    continue
            except Exception:
                pass
        c.execute(
            "UPDATE trades SET close_sync_attempts=COALESCE(close_sync_attempts,0)+1, close_sync_last_try_at=? "
            "WHERE trade_id=?",
            (_now_utc().isoformat(), int(trade_id)),
        )
        conn.commit()

        dr = str(deal_reference or "").strip()
        final = fetch_closed_deal_final_data(
            base_url,
            headers,
            str(deal_id),
            wait_for_realized=True,
            identifiers=[dr] if dr else None,
        )
        if not final:
            c.execute(
                "UPDATE trades SET close_sync_last_error=? WHERE trade_id=?",
                ("transaction_not_found_or_not_realized", int(trade_id)),
            )
            conn.commit()
            continue

        pnl = float(final["actual_pnl"])
        exit_price = final.get("exit_price")
        c.execute(
            "UPDATE trades SET pnl=?, actual_pnl=?, exit_price=?, close_sync_last_error=NULL, sync_status=? "
            "WHERE trade_id=?",
            (
                pnl,
                pnl,
                float(exit_price) if exit_price is not None else None,
                SYNCED,
                int(trade_id),
            ),
        )
        conn.commit()

        ps = (parent_session or "").strip()
        pnl_f = float(pnl)
        after_trade_leg_closed(chat_id, ps, pnl_f)

        # Close notifications are mandatory; do not silence with notify=False.
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
        deal_id_str = _capital_deal_id_from_row(p)
        if not deal_id_str:
            continue
        if deal_id_str not in local_deal_ids:
            exists = c.execute(
                "SELECT 1 FROM trades WHERE chat_id=? AND deal_id=? AND status='OPEN' LIMIT 1",
                (chat_id, deal_id_str),
            ).fetchone()
            if exists:
                continue
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


def run_zombie_trade_audit(
    chat_id_filter: str | None = None,
    *,
    fix: bool = False,
) -> str:
    """
    Admin tool: compare DB rows to GET /positions.
    - OPEN locally but flat at broker → optional `reconcile()` to mark closed + PnL.
    - CLOSED locally but still OPEN at broker → alert; optional fix sets row back to OPEN.
    """
    from core.executor import get_user_credentials, get_session
    from bot.notifier import notify_admin_alert

    lines: list[str] = []
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT DISTINCT chat_id FROM trades WHERE status IN ('OPEN','CLOSED')"
    )
    chats = [str(r[0]) for r in c.fetchall()]
    conn.close()

    if not chats:
        return "No trades in database."

    for cid in chats:
        if chat_id_filter and str(chat_id_filter).strip() != str(cid).strip():
            continue
        creds = get_user_credentials(cid)
        if not creds:
            lines.append(f"[{cid}] skip — no broker credentials")
            continue
        base_url, headers = get_session(creds)
        if not headers:
            lines.append(f"[{cid}] skip — session failed")
            continue

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT trade_id, deal_id, symbol, direction, size, status, COALESCE(sync_status,'') FROM trades "
            "WHERE chat_id=? AND deal_id IS NOT NULL AND TRIM(deal_id) != ''",
            (cid,),
        )
        rows = c.fetchall()
        conn.close()

        need_reconcile = False
        for trade_id, deal_id, symbol, direction, size, status, sync_st in rows:
            did = str(deal_id).strip()
            try:
                sz_a = float(size) if size is not None else None
            except (TypeError, ValueError):
                sz_a = None
            broker_open = capital_deal_still_open(
                base_url,
                headers,
                did,
                symbol=str(symbol or ""),
                direction=str(direction or ""),
                size=sz_a,
            )

            if status == "OPEN" and not broker_open:
                lines.append(
                    f"[{cid}] trade_id={trade_id} {symbol} deal={did}: DB OPEN, flat at broker → reconcile"
                )
                need_reconcile = True

            if status == "CLOSED" and broker_open:
                lines.append(
                    f"[{cid}] trade_id={trade_id} {symbol} deal={did} sync={sync_st}: "
                    f"DB CLOSED but position STILL OPEN at broker"
                )
                if fix:
                    notify_admin_alert(
                        "audit_sync: re-opening local row (CLOSED in DB, OPEN at broker).\n"
                        f"chat_id={cid} trade_id={trade_id} {symbol} deal_id={did}"
                    )
                    conn2 = sqlite3.connect(DB_PATH)
                    conn2.execute(
                        "UPDATE trades SET status='OPEN', closed_at=NULL, "
                        "sync_status=NULL, close_sync_last_error=? WHERE trade_id=?",
                        ("audit_reopen_desync", int(trade_id)),
                    )
                    conn2.commit()
                    conn2.close()

        if fix and need_reconcile:
            reconcile(cid, base_url, headers, notify=True)

    if not lines:
        return "OK — no DB vs broker mismatches detected for scanned rows."
    out = "\n".join(lines)
    if len(out) > 3500:
        return out[:3500] + "\n…(truncated)"
    return out
