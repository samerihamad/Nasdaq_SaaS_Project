"""
DB ↔ Broker Reconciliation — NATB v2.0

Runs at the start of every monitoring cycle to fix any discrepancies:

  Case 1 — Locally OPEN, not on broker:
    Position was closed externally (TP hit, manual close, margin call).
    → Mark CLOSED + PENDING_SYNC immediately; broker PnL fetch runs on a
      daemon thread (does not block the market scanner).

  Case 2 — On broker, not tracked locally:
    Position opened manually via the platform UI.
    → Insert into DB so the trailing stop engine starts tracking it.
"""

import queue
import sqlite3
import requests
import re
import threading
import time
from datetime import datetime as _dt
from datetime import timezone, timedelta

from core.trade_session_finalize import after_trade_leg_closed
from core.trade_close_messages import (
    send_reconcile_generic_external,
    send_reconcile_tp1_hit,
    send_reconcile_tp2_final,
)
from database.db_manager import get_subscriber_lang, DB_PATH
from config import (
    FINAL_SYNC_FALLBACK_ENABLED,
    ENABLE_CLOSE_PENDING_NOTIFY,
    SYNC_RETRY_COOLDOWN_SEC,
    SYNC_MAX_ATTEMPTS,
    FINAL_SYNC_MAX_WALL_SEC,
    RECONCILE_FINAL_SYNC_WALL_SEC,
    STALLED_PNL_RETRY_INTERVAL_SEC,
    STALLED_PNL_RETRY_MAX_ROUNDS,
    VERIFY_CLOSE_MAX_POLLS,
    VERIFY_CLOSE_POLL_SEC,
    VERIFY_CLOSE_CONSECUTIVE_ABSENT,
    FINAL_SYNC_HOURLY_RETRY_SEC,
    FINAL_SYNC_CONSOLE_WARN_AFTER_SEC,
)
from utils.market_hours import utc_now

PENDING_FINAL = "PENDING_FINAL"
# New canonical: waiting for broker history (preferred on write).
PENDING_SYNC = "PENDING_SYNC"
SYNCED = "SYNCED"
FINALIZED_NO_PNL = "FINALIZED_NO_PNL"

# Capital can take ~60s to populate realized rpl in history after close — space retries out.
FINAL_SYNC_BACKOFF_SEC = [15.0, 30.0, 45.0, 60.0, 60.0, 60.0]

# Background P&L sync: reconcile enqueues trade_ids; worker runs fetch_closed_deal_final_data
# (including internal backoff sleeps) off the main / watcher / scanner thread.
_pnl_sync_queue: queue.Queue = queue.Queue()
_pnl_sync_lock = threading.Lock()
_pnl_sync_scheduled: set[int] = set()
_pnl_sync_worker_started = False


def pnl_sync_background_active() -> bool:
    """True if P&L jobs are queued or a worker run is in progress."""
    with _pnl_sync_lock:
        return (not _pnl_sync_queue.empty()) or (len(_pnl_sync_scheduled) > 0)


def schedule_trade_pnl_sync(trade_id: int, *, notify: bool = True) -> None:
    """
    Queue broker history fetch + finalize for this trade on the P&L daemon thread.
    Safe to call from reconcile; does not block on Capital history backoff.
    """
    tid = int(trade_id)
    with _pnl_sync_lock:
        if tid in _pnl_sync_scheduled:
            return
        _pnl_sync_scheduled.add(tid)
    _pnl_sync_queue.put((tid, bool(notify)))
    _ensure_pnl_sync_worker()


def _ensure_pnl_sync_worker() -> None:
    global _pnl_sync_worker_started
    with _pnl_sync_lock:
        if _pnl_sync_worker_started:
            return
        _pnl_sync_worker_started = True
    threading.Thread(target=_pnl_sync_worker_loop, daemon=True, name="pnl_sync_worker").start()


def _pnl_sync_worker_loop() -> None:
    while True:
        try:
            item = _pnl_sync_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        tid, notify = item
        try:
            _broker_fetch_and_finalize_trade(int(tid), notify=notify)
        except Exception as exc:
            print(f"[P&L sync worker] trade_id={tid} error: {exc!s}", flush=True)
        finally:
            with _pnl_sync_lock:
                _pnl_sync_scheduled.discard(int(tid))


def _emit_reconcile_close_messages(chat_id: str, trade_id: int) -> None:
    """Send TP1 / TP2 / generic reconcile Telegram after broker P&L is persisted."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    row = c.execute(
        "SELECT symbol, direction, entry_price, exit_price, size, pnl, trailing_stop, stop_distance, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), COALESCE(pnl_trade_parts,0) "
        "FROM trades WHERE trade_id=? AND status='CLOSED'",
        (int(trade_id),),
    ).fetchone()
    if not row:
        conn.close()
        return
    (
        symbol,
        direction,
        entry_price,
        exit_price,
        size,
        pnl,
        trailing_stop,
        stop_distance,
        leg_role,
        parent_session,
        pnl_trade_parts,
    ) = row
    ep = float(entry_price or 0)
    sz = float(size or 0)
    ts = float(trailing_stop) if trailing_stop is not None else None
    sd = stop_distance
    if sd is None and ts is not None and ep:
        sd = abs(ep - ts)
    lr = str(leg_role or "").strip()
    ps = str(parent_session or "").strip()
    pnl_f = float(pnl or 0)
    part_n = int(pnl_trade_parts or 0)
    agg_parts = int(part_n) if part_n > 1 else None

    if lr == "TP1" and ps:
        try:
            c.execute(
                "UPDATE trades SET target_reached=COALESCE(target_reached,'TARGET_1_HIT') WHERE trade_id=?",
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
            aggregated_parts=agg_parts,
        )
    elif lr == "TP2" and ps:
        try:
            c.execute(
                "UPDATE trades SET target_reached=COALESCE(target_reached,'TRAILING_STOP_EXIT') WHERE trade_id=?",
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
            aggregated_parts=agg_parts,
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
            aggregated_parts=agg_parts,
        )
    conn.close()


def _broker_fetch_and_finalize_trade(trade_id: int, *, notify: bool = True) -> bool:
    """
    Auth to Capital, fetch realized P&L from history (may block with internal backoff),
    persist SYNCED row, risk hooks, and reconcile Telegram. Runs only on the P&L worker thread.
    """
    tid = int(trade_id)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    row = c.execute(
        "SELECT chat_id, deal_id, COALESCE(deal_reference,''), COALESCE(capital_order_id,''), "
        "symbol, direction, entry_price, size, closed_at, COALESCE(sync_status,''), "
        "COALESCE(risk_outcome_recorded,0) "
        "FROM trades WHERE trade_id=?",
        (tid,),
    ).fetchone()
    conn.close()
    if not row:
        return False
    (
        chat_id,
        deal_id,
        deal_reference,
        capital_order_id,
        symbol,
        direction,
        entry_price,
        size,
        closed_at,
        sync_st,
        risk_already,
    ) = row
    if not deal_id:
        return False
    st = str(sync_st or "").strip()
    if st == SYNCED:
        return True

    try:
        from bot.licensing import safe_decrypt

        conn2 = sqlite3.connect(DB_PATH)
        creds = conn2.execute(
            "SELECT api_key, api_password, is_demo, email FROM subscribers WHERE chat_id=?",
            (str(chat_id),),
        ).fetchone()
        conn2.close()
        if not creds:
            return False
        api_key = str(safe_decrypt(creds[0] or "")).strip()
        password = str(safe_decrypt(creds[1] or "")).strip()
        is_demo = bool(creds[2])
        email = str(safe_decrypt(creds[3] or "")).strip()
    except Exception:
        return False
    if not api_key or not password or not email:
        return False

    base_url = (
        "https://demo-api-capital.backend-capital.com/api/v1"
        if is_demo
        else "https://api-capital.backend-capital.com/api/v1"
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
            return False
        session_headers = {
            **headers,
            "CST": auth_res.headers.get("CST"),
            "X-SECURITY-TOKEN": auth_res.headers.get("X-SECURITY-TOKEN"),
        }
    except Exception:
        return False

    dr = str(deal_reference or "").strip()
    co = str(capital_order_id or "").strip()
    try:
        size_f = float(size) if size is not None else None
    except (TypeError, ValueError):
        size_f = None
    final = fetch_closed_deal_final_data(
        base_url,
        session_headers,
        str(deal_id),
        symbol=str(symbol or ""),
        direction=str(direction or ""),
        size=size_f,
        closed_at=str(closed_at or "").strip() or None,
        wait_for_realized=True,
        identifiers=[dr] if dr else None,
        capital_order_id=co or None,
        max_wall_sec=RECONCILE_FINAL_SYNC_WALL_SEC,
        quiet=True,
        db_trade_id=int(tid),
    )
    if not final:
        try:
            conn_e = sqlite3.connect(DB_PATH)
            conn_e.execute(
                "UPDATE trades SET close_sync_last_error=? WHERE trade_id=?",
                ("transaction_not_found_or_not_realized", tid),
            )
            conn_e.commit()
            conn_e.close()
        except Exception:
            pass
        return False

    pnl = float(final["actual_pnl"])
    exit_price = final.get("exit_price")
    part_n = int(final.get("part_count") or 1)
    try:
        ep = float(entry_price or 0.0)
        qty = float(size or 0.0)
        side = 1.0 if str(direction).upper() == "BUY" else -1.0
    except Exception:
        ep = 0.0
        qty = 0.0
        side = 1.0
    if float(pnl) == 0.0 and exit_price is not None and ep > 0 and qty > 0:
        pnl = (float(exit_price) - ep) * qty * side

    conn3 = sqlite3.connect(DB_PATH)
    cur = conn3.execute(
        "UPDATE trades SET pnl=?, actual_pnl=?, exit_price=?, close_sync_last_error=NULL, sync_status=?, "
        "pnl_trade_parts=? "
        "WHERE trade_id=? AND COALESCE(sync_status,'') IN (?, ?)",
        (
            float(pnl),
            float(pnl),
            float(exit_price) if exit_price is not None else None,
            SYNCED,
            int(part_n) if part_n > 1 else None,
            tid,
            PENDING_SYNC,
            PENDING_FINAL,
        ),
    )
    if cur.rowcount != 1:
        conn3.close()
        return False
    conn3.commit()
    conn3.close()

    conn_ps = sqlite3.connect(DB_PATH)
    ps_row = conn_ps.execute(
        "SELECT COALESCE(parent_session,'') FROM trades WHERE trade_id=?",
        (tid,),
    ).fetchone()
    conn_ps.close()
    ps = str(ps_row[0] or "").strip() if ps_row else ""
    if not int(risk_already or 0):
        after_trade_leg_closed(str(chat_id), ps, float(pnl))

    print(
        f"[PROFIT_FIX] Trade {int(tid)} closed at "
        f"{('n/a' if exit_price is None else float(exit_price))}. Realized PnL: {float(pnl):.2f}",
        flush=True,
    )
    if notify:
        _emit_reconcile_close_messages(str(chat_id), tid)
    return True


def spawn_stalled_pnl_retry(trade_id: int) -> None:
    """
    After a close, if P/L is still null or zero, retry broker history on a fixed interval
    (default 5 min) for up to 1 hour, then stop.
    """
    tid = int(trade_id)

    def _run() -> None:
        import importlib
        import time as _time

        sync = importlib.import_module("core.sync")
        rounds = max(1, int(STALLED_PNL_RETRY_MAX_ROUNDS))
        interval = max(60.0, float(STALLED_PNL_RETRY_INTERVAL_SEC))
        for _ in range(rounds):
            _time.sleep(interval)
            try:
                result = sync.try_sync_final_for_trade_id(
                    tid,
                    max_wall_sec=120.0,
                    with_notifications=False,
                    quiet=True,
                )
                if result is not None and float(result.get("actual_pnl", 0.0)) != 0.0:
                    sync._finalize_trade_close_notifications(tid)
                    return
                conn = sqlite3.connect(DB_PATH)
                row = conn.execute(
                    "SELECT COALESCE(actual_pnl, pnl, 0) FROM trades WHERE trade_id=?",
                    (tid,),
                ).fetchone()
                conn.close()
                if row and float(row[0] or 0) != 0.0:
                    sync._finalize_trade_close_notifications(tid)
                    return
            except Exception:
                pass

    threading.Thread(target=_run, daemon=True, name=f"stalled_pnl_{tid}").start()


def _pending_sync_retry_cooldown_sec() -> float:
    """
    Closed trades pending broker P&L should retry slowly (Capital can take time to finalize rpl).
    Default: 5 minutes.
    """
    return max(60.0, float(STALLED_PNL_RETRY_INTERVAL_SEC))


def _pending_sync_max_attempts() -> int:
    """Retry for ~1 hour (5 min × 12 by default)."""
    return max(1, int(STALLED_PNL_RETRY_MAX_ROUNDS))


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


def _broker_tx_deal_id_for_dedup(tx: dict) -> str:
    """Primary broker id on a history transaction (for dedup against trades.deal_id)."""
    if not isinstance(tx, dict):
        return ""
    for k in ("dealId", "positionId", "orderId"):
        v = tx.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    pos = tx.get("position")
    if isinstance(pos, dict):
        for k in ("dealId", "positionId", "orderId"):
            v = pos.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def _fuzzy_tx_deal_claimed_by_other_row(tx: dict, db_trade_id: int | None) -> bool:
    """True if this transaction's deal id is already linked to a different DB trade row."""
    if db_trade_id is None:
        return False
    cand = _broker_tx_deal_id_for_dedup(tx)
    if not cand:
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT deal_id FROM trades WHERE trade_id!=? AND deal_id IS NOT NULL AND TRIM(deal_id)!=''",
            (int(db_trade_id),),
        ).fetchall()
        conn.close()
    except Exception:
        return False
    for (did,) in rows:
        if _capital_ids_match(cand, str(did or "")):
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
    provisional_pnl: float | None = None,
    apply_fast_risk: bool = True,
    risk_outcome_hint: str | None = None,
):
    """
    Mark as CLOSED immediately when broker no longer has the position, even if
    final realized PnL is delayed in history.

    When apply_fast_risk is True, writes provisional P&L (from last broker UPL or
    caller) and runs trade_session_finalize / record_trade_result immediately so
    the circuit breaker does not wait for history sync.

    risk_outcome_hint: optional 'loss'/'win' for single-leg trades when outcome must
    be inferred before broker rpl (e.g. stop-loss with flat provisional UPL).
    """
    tid = int(trade_id)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    if not apply_fast_risk and provisional_pnl is None:
        c.execute(
            "UPDATE trades SET status='CLOSED', closed_at=?, sync_status=?, "
            "close_sync_last_error=?, close_reason=COALESCE(close_reason, ?), "
            "deal_reference=COALESCE(NULLIF(?,''), deal_reference), close_sync_notified=1 "
            "WHERE trade_id=? AND status='OPEN'",
            (
                _now_utc().isoformat(),
                PENDING_SYNC,
                str(reason),
                "pending_final_sync",
                str(deal_reference or "").strip(),
                tid,
            ),
        )
        changed = c.rowcount == 1
        if changed:
            conn.commit()
        conn.close()
        if changed and notify and ENABLE_CLOSE_PENDING_NOTIFY:
            _send_pending_close_notice(chat_id, symbol, direction, trade_id)
        return changed

    eff_pnl: float | None = provisional_pnl
    if eff_pnl is None:
        row_lb = c.execute(
            "SELECT COALESCE(last_broker_upl, pnl) FROM trades WHERE trade_id=?",
            (tid,),
        ).fetchone()
        if row_lb and row_lb[0] is not None:
            try:
                eff_pnl = float(row_lb[0])
            except (TypeError, ValueError):
                eff_pnl = None
    if eff_pnl is None:
        eff_pnl = 0.0

    risk_flag = 1 if apply_fast_risk else 0
    c.execute(
        "UPDATE trades SET status='CLOSED', closed_at=?, sync_status=?, "
        "close_sync_last_error=?, close_reason=COALESCE(close_reason, ?), "
        "deal_reference=COALESCE(NULLIF(?,''), deal_reference), close_sync_notified=1, "
        "pnl=?, actual_pnl=?, risk_outcome_recorded=? "
        "WHERE trade_id=? AND status='OPEN'",
        (
            _now_utc().isoformat(),
            PENDING_SYNC,
            str(reason),
            "pending_final_sync",
            str(deal_reference or "").strip(),
            float(eff_pnl),
            float(eff_pnl),
            int(risk_flag),
            tid,
        ),
    )
    changed = c.rowcount == 1
    ps_row = None
    if changed:
        ps_row = c.execute(
            "SELECT COALESCE(parent_session,'') FROM trades WHERE trade_id=?",
            (tid,),
        ).fetchone()
        conn.commit()
    conn.close()

    if changed and apply_fast_risk:
        ps = str((ps_row or ("",))[0] or "").strip()
        try:
            after_trade_leg_closed(
                str(chat_id),
                ps,
                float(eff_pnl),
                outcome_hint=risk_outcome_hint,
            )
        except Exception:
            pass

    # User-facing "pending sync" close notices are optional and disabled by default.
    if changed and notify and ENABLE_CLOSE_PENDING_NOTIFY:
        _send_pending_close_notice(chat_id, symbol, direction, trade_id)
    return changed


def fetch_closed_deal_final_data(
    base_url: str,
    headers: dict,
    deal_id: str,
    *,
    symbol: str | None = None,
    direction: str | None = None,
    size: float | None = None,
    closed_at: str | None = None,
    wait_for_realized: bool = True,
    identifiers: list[str] | None = None,
    capital_order_id: str | None = None,
    lookback_max: int = 2000,
    max_wall_sec: float | None = None,
    quiet: bool = True,
    db_trade_id: int | None = None,
) -> dict | None:
    """
    Fetch broker-truth final close data from Capital.com /history/transactions.

    One logical order can split into multiple trade rows (same Order Id, different Trade Id).
    We match *all* rows whose dealId or orderId matches any stored hook (dealId,
    dealReference, capital order id, etc.), sum realized P&L per row (CSV `rpl` / API
    equivalent), and return one aggregate.

    Returns dict:
      { 'actual_pnl': float, 'exit_price': float|None, 'part_count': int }

    Retries with backoff until max_wall_sec (default FINAL_SYNC_MAX_WALL_SEC, e.g. 300s).

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
        # Broker CSV uses `rpl` — treat as primary source of truth when present.
        for k in (
            "rpl",
            "Rpl",
            "RPL",
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

    def _to_utc_iso_z(dt: _dt) -> str:
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _norm_symbol(v: str | None) -> str:
        return str(v or "").strip().upper()

    def _symbol_tokens(v: str | None) -> set[str]:
        s = _norm_symbol(v)
        if not s:
            return set()
        toks = {s}
        for sep in (".", ":", "/"):
            parts = [p for p in s.split(sep) if p]
            if parts:
                toks.add(parts[-1])
        return {t for t in toks if t}

    def _tx_symbol(tx: dict) -> str:
        for k in ("instrumentName", "epic", "symbol", "market"):
            v = tx.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
            if isinstance(v, dict):
                vv = v.get("instrumentName") or v.get("epic") or v.get("symbol")
                if vv is not None:
                    return str(vv).strip()
        return ""

    def _tx_direction(tx: dict) -> str:
        d = tx.get("direction") or tx.get("side")
        if d is None and isinstance(tx.get("position"), dict):
            d = (tx.get("position") or {}).get("direction")
        return str(d or "").strip().upper()

    def _expected_close_side(open_side: str | None) -> str:
        s = str(open_side or "").strip().upper()
        if s == "BUY":
            return "SELL"
        if s == "SELL":
            return "BUY"
        return ""

    def _tx_size(tx: dict) -> float | None:
        return _parse_float(tx.get("size") or tx.get("quantity") or tx.get("dealSize"))

    def _tx_has_valid_rpl(tx: dict) -> bool:
        for k in ("rpl", "Rpl", "RPL"):
            if k in tx and tx.get(k) is not None:
                return _parse_float(tx.get(k)) is not None
        return False

    def _tx_fuzzy_matches(tx: dict) -> bool:
        # Fuzzy fallback uses Symbol + opposite side + volume + valid realized PnL.
        tx_sym = _norm_symbol(_tx_symbol(tx))
        want_sym_tokens = _symbol_tokens(symbol)
        if not tx_sym or not want_sym_tokens:
            return False
        if not any(tok in tx_sym for tok in want_sym_tokens):
            return False

        if size is None:
            return False
        tx_sz = _tx_size(tx)
        if tx_sz is None or abs(float(tx_sz) - float(size)) >= 0.0001:
            return False

        if not _tx_has_valid_rpl(tx):
            return False

        expected_close = _expected_close_side(direction)
        tx_side = _tx_direction(tx)
        if expected_close and tx_side and tx_side != expected_close:
            return False
        return True

    def _tx_datetime_utc(tx: dict) -> _dt | None:
        for k in ("date", "timestamp", "createdDate", "createdTime", "time"):
            raw = tx.get(k)
            if raw is None:
                continue
            if isinstance(raw, (int, float)):
                try:
                    val = float(raw)
                    if val > 1e12:  # epoch milliseconds
                        val = val / 1000.0
                    return _dt.fromtimestamp(val, tz=timezone.utc)
                except Exception:
                    continue
            try:
                s = str(raw).strip()
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                parsed = _parse_iso_utc(s)
                if parsed is not None:
                    return parsed
            except Exception:
                continue
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

    wall = float(max_wall_sec) if max_wall_sec is not None else float(FINAL_SYNC_MAX_WALL_SEC)
    if not wait_for_realized:
        wall = min(wall, 45.0)

    ids = [str(deal_id)]
    if capital_order_id and str(capital_order_id).strip():
        ids.append(str(capital_order_id).strip())
    if identifiers:
        ids.extend([str(x).strip() for x in identifiers if str(x).strip()])
    # De-dup while preserving order
    seen_ids: set[str] = set()
    ids = [x for x in ids if not (x in seen_ids or seen_ids.add(x))]

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

    def _sleep_for_attempt(attempt_idx: int) -> float:
        if attempt_idx < len(FINAL_SYNC_BACKOFF_SEC):
            return float(FINAL_SYNC_BACKOFF_SEC[attempt_idx])
        return 60.0

    def _tx_candidate_strings(tx: dict) -> list[str]:
        out: list[str] = []
        if not isinstance(tx, dict):
            return out
        for k in (
            "dealId",
            "orderId",
            "order_id",
            "dealReference",
            "dealRef",
            "relatedDealId",
            "relatedDealReference",
            "reference",
            "transactionReference",
            "openingDealId",
            "openDealId",
            "positionId",
        ):
            v = tx.get(k)
            if v is not None:
                out.append(str(v).strip())
        pos = tx.get("position")
        if isinstance(pos, dict):
            for k in ("dealId", "orderId", "positionId"):
                v = pos.get(k)
                if v is not None:
                    out.append(str(v).strip())
        return out

    def _tx_matches_any_hook(tx: dict) -> bool:
        for c in _tx_candidate_strings(tx):
            for hid in ids:
                if _id_match(c, hid):
                    return True
        tx_sym = _norm_symbol(_tx_symbol(tx))
        want_sym_tokens = _symbol_tokens(symbol)
        if not tx_sym or not want_sym_tokens:
            return False
        if not any(tok in tx_sym for tok in want_sym_tokens):
            return False
        if size is None:
            return False
        tx_sz = _tx_size(tx)
        if tx_sz is None or abs(float(tx_sz) - float(size)) >= 0.0001:
            return False
        # Closing transactions are the opposite side of the original open trade.
        expected_close = _expected_close_side(direction)
        tx_side = _tx_direction(tx)
        if expected_close and tx_side and tx_side != expected_close:
            return False
        if closed_at_dt is not None:
            tx_dt = _tx_datetime_utc(tx)
            if tx_dt is not None:
                from_dt = closed_at_dt - timedelta(hours=24)
                to_dt = closed_at_dt + timedelta(hours=24)
                if tx_dt < from_dt or tx_dt > to_dt:
                    return False
        return True

    closed_at_dt = _parse_iso_utc(closed_at)
    fuzzy_window_from = None
    fuzzy_window_to = None
    if closed_at_dt is not None:
        fuzzy_window_from = _to_utc_iso_z(closed_at_dt - timedelta(hours=24))
        fuzzy_window_to = _to_utc_iso_z(closed_at_dt + timedelta(hours=24))

    def _refine_fuzzy_manual_matches(raw: list[dict]) -> list[dict]:
        """
        Drop broker rows whose deal id is already bound to another trade, then pick the
        transaction whose timestamp is closest to our DB closed_at (manual-close path).
        """
        if not raw:
            return []
        deduped = [
            tx
            for tx in raw
            if isinstance(tx, dict) and not _fuzzy_tx_deal_claimed_by_other_row(tx, db_trade_id)
        ]
        if not deduped:
            return []
        if closed_at_dt is None:
            return [deduped[0]]
        best_tx = None
        best_sec = float("inf")
        undated: list[dict] = []
        for tx in deduped:
            tdt = _tx_datetime_utc(tx)
            if tdt is None:
                undated.append(tx)
                continue
            delta = abs((tdt - closed_at_dt).total_seconds())
            if delta < best_sec:
                best_sec = delta
                best_tx = tx
        if best_tx is not None:
            return [best_tx]
        return [undated[0]] if undated else [deduped[0]]

    def _collect_merged_transactions() -> tuple[list[dict], str, list[dict]]:
        seen_keys: set[str] = set()
        merged: list[dict] = []
        last_err = ""
        fuzzy_matches: list[dict] = []

        def _add_batch(raw: list) -> None:
            for tx in raw or []:
                if not isinstance(tx, dict):
                    continue
                key = "|".join(
                    (
                        str(tx.get("dealId") or ""),
                        str(tx.get("orderId") or tx.get("order_id") or ""),
                        str(tx.get("date") or tx.get("timestamp") or ""),
                        str(tx.get("level") or tx.get("price") or ""),
                    )
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                merged.append(tx)

        ok, txs, st, info = _fetch({"dealId": str(deal_id)})
        if not ok:
            last_err = f"history/transactions failed status={st} info={info}"
        elif txs:
            _add_batch(txs)
        elif fuzzy_window_from and fuzzy_window_to:
            ok_f, txs_f, st_f, info_f = _fetch(
                {
                    "from": str(fuzzy_window_from),
                    "to": str(fuzzy_window_to),
                    "max": int(lookback_max),
                }
            )
            if not ok_f:
                last_err = str(
                    last_err
                    or f"history/transactions(fuzzy-window) failed status={st_f} info={info_f}"
                )
            elif txs_f:
                fuzzy_matches = [tx for tx in txs_f if isinstance(tx, dict) and _tx_fuzzy_matches(tx)]
                if fuzzy_matches:
                    _add_batch(fuzzy_matches)

        for oid in ids:
            if not oid or oid == str(deal_id):
                continue
            ok2, txs2, st2, info2 = _fetch({"dealId": str(oid)})
            if not ok2:
                last_err = str(last_err or f"history/transactions failed status={st2} info={info2}")
            elif txs2:
                _add_batch(txs2)
            ok3, txs3, _, _ = _fetch({"orderId": str(oid)})
            if ok3 and txs3:
                _add_batch(txs3)

        okm, txs_max, st2, info2 = _fetch({"max": int(lookback_max)})
        if not okm:
            last_err = str(last_err or f"history/transactions(max) failed status={st2} info={info2}")
        elif txs_max:
            _add_batch(txs_max)

        return merged, last_err, fuzzy_matches

    t0 = time.monotonic()
    attempt_idx = 0
    last_issue = ""

    while True:
        elapsed = time.monotonic() - t0
        if elapsed >= wall:
            break

        merged, fetch_err, fuzzy_matches = _collect_merged_transactions()
        if fetch_err and not merged:
            last_issue = fetch_err
            if not quiet:
                print("Warning: Could not sync final data from Capital.com", flush=True)
                print(f"[Capital Sync] {last_issue}", flush=True)
        elif fetch_err and merged:
            last_issue = fetch_err

        matched = [tx for tx in merged if _tx_matches_any_hook(tx)]
        matched_via_fuzzy = False
        if not matched:
            if fuzzy_matches:
                refined = _refine_fuzzy_manual_matches(fuzzy_matches)
                if refined:
                    matched = refined
                    matched_via_fuzzy = True
                else:
                    last_issue = last_issue or "fuzzy_deduped_empty"
            else:
                last_issue = last_issue or "no_matching_transactions"
        if matched:
            total_pnl = 0.0
            parsed_rows = 0
            for tx in matched:
                pnl = _parse_pnl(tx)
                if pnl is None:
                    continue
                total_pnl += float(pnl)
                parsed_rows += 1

            if parsed_rows == 0:
                last_issue = "matched_rows_but_no_pnl"
            else:
                qty_sum = 0.0
                px_weighted = 0.0
                for tx in matched:
                    q = _parse_float(tx.get("size") or tx.get("quantity") or tx.get("dealSize"))
                    ep = _parse_exit_price(tx)
                    if q is not None and q > 0 and ep is not None:
                        px_weighted += float(q) * float(ep)
                        qty_sum += float(q)
                exit_price: float | None = None
                if qty_sum > 0 and px_weighted > 0:
                    exit_price = px_weighted / qty_sum
                else:
                    for tx in reversed(matched):
                        ep = _parse_exit_price(tx)
                        if ep is not None:
                            exit_price = ep
                            break

                part_count = len(matched)
                found_zero_hold = False
                if wait_for_realized and float(total_pnl) == 0.0:
                    found_zero_hold = True
                if not found_zero_hold:
                    if matched_via_fuzzy:
                        print(
                            f"[MANUAL-SYNC-SUCCESS] Found manual closure for "
                            f"{str(symbol or '').strip() or 'UNKNOWN'} in history. P&L updated.",
                            flush=True,
                        )
                    return {
                        "actual_pnl": float(total_pnl),
                        "exit_price": exit_price,
                        "part_count": int(part_count),
                        "matched_via_fuzzy": bool(matched_via_fuzzy),
                    }
                last_issue = "pnl_zero_placeholder_retry"

        if time.monotonic() - t0 >= wall:
            break
        sl = _sleep_for_attempt(attempt_idx)
        remaining = wall - (time.monotonic() - t0)
        if remaining <= 0:
            break
        sl = min(sl, max(0.0, remaining))
        time.sleep(sl)
        attempt_idx += 1

    if not quiet:
        print("Warning: Could not sync final data from Capital.com", flush=True)
        print(
            f"[Capital Sync] transaction not found yet ids={ids} "
            f"after {wall:.0f}s wall last_issue={last_issue or 'no_match'}",
            flush=True,
        )
    return None


def _finalize_trade_close_notifications(trade_id: int) -> None:
    """After DB row has final PnL: circuit breaker hook + Telegram from persisted row."""
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT chat_id, COALESCE(parent_session,''), COALESCE(actual_pnl, pnl, 0), "
            "COALESCE(risk_outcome_recorded,0) "
            "FROM trades WHERE trade_id=?",
            (int(trade_id),),
        ).fetchone()
        conn.close()
        if not row:
            return
        chat_id, parent_session, pnl_v, risk_done = row
        if not int(risk_done or 0):
            after_trade_leg_closed(str(chat_id), str(parent_session or "").strip(), float(pnl_v or 0.0))
    except Exception:
        pass
    try:
        from core.trade_close_messages import send_bot_automated_close_from_db

        send_bot_automated_close_from_db(int(trade_id))
    except Exception:
        pass


def spawn_background_final_sync(
    trade_id: int,
    *,
    max_wall_sec: float | None = None,
) -> None:
    """
    Non-blocking: daemon thread retries broker history sync on a schedule:
    - Each attempt uses max_wall_sec (default FINAL_SYNC_MAX_WALL_SEC) with quiet logs.
    - Waits FINAL_SYNC_HOURLY_RETRY_SEC between attempts (default 1h), indefinitely.
    - No console Warning spam; first console + admin alert only after
      FINAL_SYNC_CONSOLE_WARN_AFTER_SEC (default 24h) if still unsynced.
    """
    wall = float(max_wall_sec) if max_wall_sec is not None else float(FINAL_SYNC_MAX_WALL_SEC)
    tid = int(trade_id)

    def _run() -> None:
        t0 = time.time()
        warned = False
        while True:
            try:
                result = try_sync_final_for_trade_id(
                    tid,
                    max_wall_sec=wall,
                    with_notifications=True,
                    quiet=True,
                )
                if result is not None:
                    return
            except Exception as exc:
                try:
                    from bot.notifier import notify_admin_alert

                    notify_admin_alert(
                        f"Background final_sync exception trade_id={tid}: {exc!s}"
                    )
                except Exception:
                    pass

            elapsed = time.time() - t0
            if elapsed >= FINAL_SYNC_CONSOLE_WARN_AFTER_SEC and not warned:
                print(
                    "Warning: Final Capital.com history still missing after "
                    f"{int(FINAL_SYNC_CONSOLE_WARN_AFTER_SEC // 3600)}h for trade_id={tid} "
                    "(hourly retries continue).",
                    flush=True,
                )
                try:
                    from bot.notifier import notify_admin_alert

                    notify_admin_alert(
                        "Final Capital.com history sync still failing after "
                        f"{int(FINAL_SYNC_CONSOLE_WARN_AFTER_SEC // 3600)}h. trade_id={tid}\n"
                        "Check sync_status / broker history; background worker continues hourly."
                    )
                except Exception:
                    pass
                warned = True

            time.sleep(max(60.0, float(FINAL_SYNC_HOURLY_RETRY_SEC)))

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"final_sync_{tid}",
    ).start()


def try_sync_final_for_trade_id(
    trade_id: int,
    *,
    attempts: int = 3,
    delay_sec: float = 2.0,
    max_wall_sec: float | None = None,
    with_notifications: bool = False,
    quiet: bool = True,
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
        "SELECT chat_id, deal_id, COALESCE(deal_reference,''), COALESCE(capital_order_id,''), "
        "symbol, direction, entry_price, size, closed_at FROM trades WHERE trade_id=?",
        (tid,),
    ).fetchone()
    if not row:
        conn.close()
        return None
    chat_id, deal_id, deal_reference, capital_order_id, symbol, direction, entry_price, size, closed_at = row
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

    tries = max(1, int(attempts))
    if max_wall_sec is not None:
        wall = float(max_wall_sec)
    else:
        wall = min(120.0, max(12.0, float(tries) * max(0.5, float(delay_sec)) * 20.0))
    wait_realized = float(wall) >= 90.0

    dr = str(deal_reference or "").strip()
    co = str(capital_order_id or "").strip()
    try:
        size_f = float(size) if size is not None else None
    except (TypeError, ValueError):
        size_f = None
    final = fetch_closed_deal_final_data(
        base_url,
        session_headers,
        str(deal_id),
        symbol=str(symbol or ""),
        direction=str(direction or ""),
        size=size_f,
        closed_at=str(closed_at or "").strip() or None,
        wait_for_realized=wait_realized,
        identifiers=[dr] if dr else None,
        capital_order_id=co or None,
        max_wall_sec=wall,
        quiet=quiet,
        db_trade_id=int(tid),
    )
    if not final:
        return None

    pnl_val = float(final.get("actual_pnl", 0.0))
    exit_price = final.get("exit_price")
    part_n = int(final.get("part_count") or 1)
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
        "sync_status=?, close_sync_last_error=NULL, closed_at=COALESCE(closed_at, ?), pnl_trade_parts=? "
        "WHERE trade_id=?",
        (
            float(pnl_val),
            float(pnl_val),
            float(exit_price) if exit_price is not None else None,
            SYNCED,
            _now_utc().isoformat(),
            int(part_n) if part_n > 1 else None,
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
    if with_notifications:
        _finalize_trade_close_notifications(int(tid))
    return {"actual_pnl": float(pnl_val), "exit_price": exit_price}


def reconcile(chat_id, base_url, headers, *, notify: bool = True):
    """
    Sync local DB with live Capital.com /positions.
    Should be called once per monitoring cycle before any stop checks.

    Broker history P&L fetch never blocks this function: closed trades are marked
    PENDING_SYNC and finalized on the P&L daemon thread (see schedule_trade_pnl_sync).
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
        "SELECT trade_id, deal_id, COALESCE(deal_reference,''), COALESCE(capital_order_id,''), "
        "symbol, direction, entry_price, size, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), stop_distance, trailing_stop, "
        "COALESCE(close_sync_notified,0), COALESCE(close_sync_attempts,0), close_sync_last_try_at "
        "FROM trades WHERE chat_id=? AND status='OPEN'",
        (chat_id,),
    )
    local_open = c.fetchall()
    # If TP1 and TP2 both closed on the broker in the same cycle, process TP1 first
    # so TP1 is CLOSED in DB before TP2 final aggregates P&L.
    # local_open tuple layout:
    # (trade_id, deal_id, deal_reference, capital_order_id, symbol, direction, entry_price, size,
    #  leg_role, parent_session, stop_distance, trailing_stop, close_sync_notified,
    #  close_sync_attempts, close_sync_last_try_at)
    # NOTE: `entry_price` r[6], `size` r[7] are numeric; `leg_role` is r[8].
    local_open = sorted(
        local_open,
        key=lambda r: (
            0
            if str(r[8] or "").strip() == "TP1"
            else 1
            if str(r[8] or "").strip() == "TP2"
            else 2,
            r[0],
        ),
    )
    local_deal_ids = {str(row[1]) for row in local_open if row[1]}

    # ── Case 1: closed externally ─────────────────────────────────────────────
    for (
        trade_id,
        deal_id,
        deal_reference,
        capital_order_id,
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
            # Mark CLOSED + PENDING_SYNC immediately; broker P&L fetch (with internal backoff
            # sleeps) runs on the P&L daemon thread so the scanner / main loop is never blocked.
            dr = str(deal_reference or "").strip()
            changed = mark_trade_closed_pending(
                chat_id,
                int(trade_id),
                symbol=symbol,
                direction=direction,
                deal_reference=dr,
                reason="awaiting_background_pnl_sync",
                notify=True,
            )
            if changed:
                schedule_trade_pnl_sync(int(trade_id), notify=notify)

    # ── Case 1b: closed pending final sync ───────────────────────────────────
    c.execute(
        "SELECT trade_id, deal_id, COALESCE(deal_reference,''), COALESCE(capital_order_id,''), "
        "symbol, direction, entry_price, size, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), stop_distance, trailing_stop, "
        "COALESCE(close_sync_attempts,0), close_sync_last_try_at "
        "FROM trades WHERE chat_id=? AND status='CLOSED' AND COALESCE(sync_status,'') IN (?, ?)",
        (chat_id, PENDING_FINAL, PENDING_SYNC),
    )
    pending_rows = c.fetchall()
    for (
        trade_id,
        deal_id,
        deal_reference,
        capital_order_id,
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
        # Do NOT finalize "P&L unavailable" until the 1-hour pending-sync retry budget is exhausted.
        max_attempts_pending = _pending_sync_max_attempts()
        if attempts_i >= max_attempts_pending:
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
                            "Desync (PENDING_FINAL/PENDING_SYNC): DB CLOSED but broker still shows OPEN position.\n"
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
                if (_now_utc() - last_try).total_seconds() < _pending_sync_retry_cooldown_sec():
                    continue
            except Exception:
                pass
        c.execute(
            "UPDATE trades SET close_sync_attempts=COALESCE(close_sync_attempts,0)+1, close_sync_last_try_at=? "
            "WHERE trade_id=?",
            (_now_utc().isoformat(), int(trade_id)),
        )
        conn.commit()

        schedule_trade_pnl_sync(int(trade_id), notify=notify)

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


def force_reconcile(chat_id, base_url, headers, *, notify: bool = True) -> None:
    """
    Explicit reconciliation pass used to recover from local/broker state hash drift.
    Never raises to caller.
    """
    try:
        reconcile(chat_id, base_url, headers, notify=notify)
    except Exception as exc:
        print(f"[FORCE-RECONCILE] chat_id={chat_id} failed: {exc!s}", flush=True)


def backfill_closed_pnls(chat_id, base_url, headers, lookback: int = 200) -> int:
    """
    Backfill missing/zero PnL values for already CLOSED trades from broker history.
    Useful when a deal was closed manually and initial reconciliation stored 0.0.
    Returns number of rows updated.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT trade_id, deal_id, COALESCE(deal_reference,''), COALESCE(capital_order_id,''), "
        "pnl, direction, entry_price, size, symbol, closed_at FROM trades "
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
    for trade_id, deal_id, deal_reference, capital_order_id, pnl, direction, entry_price, size, symbol, closed_at in rows:
        # Backfill only unknown/zero rows.
        if pnl is not None and float(pnl) != 0.0:
            continue
        dr = str(deal_reference or "").strip()
        co = str(capital_order_id or "").strip()
        try:
            size_f = float(size) if size is not None else None
        except (TypeError, ValueError):
            size_f = None
        final = fetch_closed_deal_final_data(
            base_url,
            headers,
            str(deal_id),
            symbol=str(symbol or ""),
            direction=str(direction or ""),
            size=size_f,
            closed_at=str(closed_at or "").strip() or None,
            wait_for_realized=True,
            identifiers=[dr] if dr else None,
            capital_order_id=co or None,
            db_trade_id=int(trade_id),
        )
        if not final:
            continue
        fetched = float(final["actual_pnl"])
        exit_price = final.get("exit_price")
        part_n = int(final.get("part_count") or 1)
        c.execute(
            "UPDATE trades SET pnl=?, actual_pnl=?, exit_price=?, pnl_trade_parts=? WHERE trade_id=?",
            (
                float(fetched),
                float(fetched),
                float(exit_price) if exit_price is not None else None,
                int(part_n) if part_n > 1 else None,
                int(trade_id),
            ),
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
