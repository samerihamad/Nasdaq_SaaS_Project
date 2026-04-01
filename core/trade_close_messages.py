"""
Rich Telegram templates for trade closes (broker sync + bot-managed).
Uses P&L from the platform when provided; R = P&L / (stop_distance × quantity).
"""

from __future__ import annotations

import sqlite3
from typing import Any

from bot.notifier import send_telegram_message
from database.db_manager import get_subscriber_lang

DB_PATH = "database/trading_saas.db"


def _emergency_refresh_final_close(
    *,
    trade_id: int,
    entry_price: float,
    exit_price: float | None,
    size: float,
    direction: str,
    pnl: float | None,
) -> tuple[float | None, float]:
    """
    Last-chance refresh before Telegram close message:
    1) Ask sync layer for broker-final data.
    2) If still missing, compute fallback pnl from exit-entry.
    """
    need_refresh = (
        exit_price is None
        or pnl is None
        or float(pnl) == 0.0
    )
    if need_refresh:
        try:
            from core.sync import try_sync_final_for_trade_id
            try_sync_final_for_trade_id(int(trade_id), attempts=1, delay_sec=0.0)
        except Exception:
            pass

    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT exit_price, pnl FROM trades WHERE trade_id=?",
        (int(trade_id),),
    ).fetchone()
    if row:
        exit_price = float(row[0]) if row[0] is not None else exit_price
        pnl = float(row[1]) if row[1] is not None else pnl

    # Final fallback formula requested by ops when broker pnl still not present.
    if (pnl is None or float(pnl) == 0.0) and exit_price is not None and entry_price > 0 and size > 0:
        side = 1.0 if str(direction).upper() == "BUY" else -1.0
        pnl_calc = (float(exit_price) - float(entry_price)) * float(size) * side
        conn.execute(
            "UPDATE trades SET pnl=?, actual_pnl=COALESCE(actual_pnl, ?), "
            "exit_price=COALESCE(exit_price, ?) WHERE trade_id=?",
            (float(pnl_calc), float(pnl_calc), float(exit_price), int(trade_id)),
        )
        conn.commit()
        print(
            f"[PROFIT_FIX] Trade {int(trade_id)} closed at {float(exit_price)}. "
            f"Realized PnL: {float(pnl_calc):.2f}",
            flush=True,
        )
        pnl = float(pnl_calc)
    conn.close()
    return exit_price, float(pnl or 0.0)


def _resolve_stop_distance(
    stop_distance: float | None,
    entry_price: float | None,
    trailing_stop: float | None,
) -> float | None:
    if stop_distance is not None and float(stop_distance) > 0:
        return float(stop_distance)
    if entry_price is None or trailing_stop is None:
        return None
    d = abs(float(entry_price) - float(trailing_stop))
    return d if d > 0 else None


def pnl_to_r_multiple(pnl: float, stop_distance: float | None, qty: float | None) -> float | None:
    if stop_distance is None or qty is None:
        return None
    risk = abs(float(stop_distance)) * float(qty)
    if risk <= 1e-12:
        return None
    return pnl / risk


def _fmt_money_signed(v: float) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}${abs(v):,.2f}"


def _fmt_r(r: float | None) -> str:
    if r is None:
        return "—"
    sign = "+" if r >= 0 else ""
    return f"{sign}{r:.2f}R"


def _dir_ar(direction: str) -> str:
    return "شراء" if direction == "BUY" else "بيع"


def _outcome_title_ar(pnl: float, partial: bool = False) -> str:
    if pnl > 0:
        return "✅ *صفقة ناجحة*" + (" — جزئي" if partial else "") + "\n"
    if pnl < 0:
        return "❌ *صفقة خاسرة*" + (" — جزئي" if partial else "") + "\n"
    return "⚖️ *صفقة متعادلة*\n"


def _outcome_title_en(pnl: float, partial: bool = False) -> str:
    if pnl > 0:
        return "✅ *Profit Trade*" + (" — partial" if partial else "") + "\n"
    if pnl < 0:
        return "❌ *Loss Trade*" + (" — partial" if partial else "") + "\n"
    return "⚖️ *Break-even*\n"


def _is_profit_exception_tp1_then_be(
    *,
    pnl: float,
    tp1_was_hit: bool,
) -> bool:
    """
    Exception rule:
    If TP1 was hit, and later SL is hit around entry (or slight profit),
    we must classify as Profit Trade.
    """
    if not tp1_was_hit:
        return False
    # Treat tiny negatives / zero as profit under this exception.
    return float(pnl) >= -0.01


def _outcome_title_en_with_exception(pnl: float, *, tp1_was_hit: bool, partial: bool = False) -> str:
    if pnl > 0 or _is_profit_exception_tp1_then_be(pnl=pnl, tp1_was_hit=tp1_was_hit):
        return "✅ *Profit Trade*" + (" — partial" if partial else "") + "\n"
    if pnl < 0:
        return "❌ *Loss Trade*" + (" — partial" if partial else "") + "\n"
    return "⚖️ *Break-even*\n"


def _aggregated_pnl_note(aggregated_parts: int | None, lang: str) -> str:
    if aggregated_parts is None or aggregated_parts <= 1:
        return ""
    if lang == "en":
        return f"\n_(Aggregated from {int(aggregated_parts)} trade parts)._"
    return f"\n_(مجمّع من {int(aggregated_parts)} أجزاء تنفيذ)._"


def send_reconcile_tp1_hit(
    chat_id: str,
    *,
    trade_id: int,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float | None,
    size: float,
    pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
    tp2_still_open: bool,
    aggregated_parts: int | None = None,
) -> None:
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    r_leg = pnl_to_r_multiple(pnl, sd, size)
    lang = get_subscriber_lang(chat_id)

    if lang == "en":
        body = (
            _outcome_title_en_with_exception(pnl, tp1_was_hit=True, partial=True)
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *Asset*      : *{symbol}*\n"
            + f"🆔 *Trade ID*   : *{trade_id}*\n"
            + f"▶️ *Direction*  : *{direction}*\n"
            + f"💰 *Entry*      : *${entry_price:,.2f}*\n"
            + (f"🏁 *Exit*       : *${exit_price:,.2f}*\n" if exit_price is not None else "")
            + f"🔢 *Qty (leg 1)*: *{int(size)}* shares\n"
            + f"💵 *Realized P&L*: *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R (this leg)*: *{_fmt_r(r_leg)}*\n"
            + f"📍 *Target*     : *Target 1 Hit*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + _aggregated_pnl_note(aggregated_parts, "en")
        )
        if tp2_still_open:
            body += (
                "⏳ *TP2 (2.5%)* is still open — managed as a separate leg.\n"
            )
        else:
            body += "ℹ️ No second leg tracked as open.\n"
    else:
        body = (
            _outcome_title_ar(pnl, partial=True)
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *الرمز*           : *{symbol}*\n"
            + f"▶️ *الاتجاه*        : *{_dir_ar(direction)}*\n"
            + f"💰 *سعر الدخول*     : *${entry_price:,.2f}*\n"
            + f"🔢 *الكمية (الحد 1)*: *{int(size)}* سهم\n"
            + f"💵 *الربح/الخسارة (من المنصة)*: *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R لهذا الحد*    : *{_fmt_r(r_leg)}*\n"
            + f"📍 *تم تحقيق*      : *الهدف الأول (1R)*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + _aggregated_pnl_note(aggregated_parts, "ar")
        )
        if tp2_still_open:
            body += (
                "⏳ *الهدف الثاني (2.5%)* لا يزال مفتوحاً — يُدار كحد منفصل.\n"
            )
        else:
            body += "ℹ️ لا يوجد حد ثاني مسجّل كمفتوح.\n"

    send_telegram_message(chat_id, body)


def send_reconcile_tp2_final(
    chat_id: str,
    *,
    trade_id: int,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float | None,
    total_qty: float,
    tp1_pnl: float,
    tp2_pnl: float,
    total_pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
    aggregated_parts: int | None = None,
) -> None:
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    total_r = pnl_to_r_multiple(total_pnl, sd, total_qty)
    lang = get_subscriber_lang(chat_id)

    if lang == "en":
        title = _outcome_title_en_with_exception(total_pnl, tp1_was_hit=(tp1_pnl > 0), partial=False)
        body = (
            title
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *Asset*         : *{symbol}*\n"
            + f"🆔 *Trade ID*      : *{trade_id}*\n"
            + f"▶️ *Direction*     : *{direction}*\n"
            + f"💰 *Entry*         : *${entry_price:,.2f}*\n"
            + (f"🏁 *Exit*          : *${exit_price:,.2f}*\n" if exit_price is not None else "")
            + f"🔢 *Total quantity*: *{int(total_qty)}* shares\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"💰 *TP1 (1R) P&L*  : *{_fmt_money_signed(tp1_pnl)}*\n"
            + f"💰 *TP2 (2.5%) P&L*: *{_fmt_money_signed(tp2_pnl)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Final Realized P&L*: *{_fmt_money_signed(total_pnl)}*\n"
            + f"🎯 *R*             : *{_fmt_r(total_r)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📍 *Target*        : *Target 2 Hit*\n"
            + _aggregated_pnl_note(aggregated_parts, "en")
        )
    else:
        title = _outcome_title_ar(total_pnl, partial=False)
        body = (
            title
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *الرمز*              : *{symbol}*\n"
            + f"▶️ *الاتجاه*           : *{_dir_ar(direction)}*\n"
            + f"💰 *سعر الدخول*        : *${entry_price:,.2f}*\n"
            + f"🔢 *الكمية الإجمالية*  : *{int(total_qty)}* سهم\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"💰 *ربح/خسارة الهدف 1* : *{_fmt_money_signed(tp1_pnl)}*\n"
            + f"💰 *ربح/خسارة الهدف 2* : *{_fmt_money_signed(tp2_pnl)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Total P&L*            : *{_fmt_money_signed(total_pnl)}*\n"
            + f"🎯 *R*                  : *{_fmt_r(total_r)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📍 *النتيجة*            : *تحقق الهدف الثاني (2.5%)*\n"
            + _aggregated_pnl_note(aggregated_parts, "ar")
        )

    send_telegram_message(chat_id, body)


def send_reconcile_generic_external(
    chat_id: str,
    *,
    trade_id: int,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float | None,
    size: float,
    pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
    reason_hint: str = "external_close",
    aggregated_parts: int | None = None,
) -> None:
    """Single-leg / legacy row closed on broker (TP, SL, manual)."""
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    r_mult = pnl_to_r_multiple(pnl, sd, size)
    lang = get_subscriber_lang(chat_id)

    if lang == "en":
        title = _outcome_title_en_with_exception(pnl, tp1_was_hit=False, partial=False)
        body = (
            title
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *Asset*      : *{symbol}*\n"
            + f"🆔 *Trade ID*   : *{trade_id}*\n"
            + f"▶️ *Direction*  : *{direction}*\n"
            + f"💰 *Entry*      : *${entry_price:,.2f}*\n"
            + (f"🏁 *Exit*       : *${exit_price:,.2f}*\n" if exit_price is not None else "")
            + f"🔢 *Quantity*   : *{int(size)}* shares\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Final Realized P&L*: *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R*         : *{_fmt_r(r_mult)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"_Close synced from platform ({reason_hint})._\n"
            + _aggregated_pnl_note(aggregated_parts, "en")
        )
    else:
        title = _outcome_title_ar(pnl, partial=False)
        body = (
            title
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *الرمز*           : *{symbol}*\n"
            + f"▶️ *الاتجاه*        : *{_dir_ar(direction)}*\n"
            + f"💰 *سعر الدخول*     : *${entry_price:,.2f}*\n"
            + f"🔢 *الكمية*         : *{int(size)}* سهم\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Total P&L*         : *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R*               : *{_fmt_r(r_mult)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"_إغلاق مزامن من المنصة ({reason_hint})._\n"
            + _aggregated_pnl_note(aggregated_parts, "ar")
        )

    send_telegram_message(chat_id, body)


def send_bot_automated_close(
    chat_id: str,
    *,
    trade_id: int | None = None,
    symbol: str,
    direction: str,
    entry_price: float,
    exit_price: float | None = None,
    size: float,
    pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
    stop_label: str,
    sibling_tp2_open: bool,
    leg_role: str,
    target_reached: str | None = None,
    aggregated_parts: int | None = None,
) -> None:
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    r_mult = pnl_to_r_multiple(pnl, sd, size)
    lang = get_subscriber_lang(chat_id)
    partial = bool(leg_role == "TP1" and sibling_tp2_open)

    if lang == "en":
        # Exception only applies when TP1 was hit earlier for the session; for bot-close
        # we infer it from the target label if provided.
        tp1_was_hit = str(target_reached or "").upper() in ("STOP_AFTER_TP1", "TARGET_1_HIT", "TARGET_2_HIT", "TRAILING_STOP_EXIT")
        title = _outcome_title_en_with_exception(pnl, tp1_was_hit=tp1_was_hit, partial=partial)
        body = (
            title
            + f"🔔 *Auto close (bot)* — *{symbol}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + (f"🆔 *Trade ID*   : *{trade_id}*\n" if trade_id is not None else "")
            + f"▶️ *Direction*  : *{direction}*\n"
            + f"💰 *Entry*      : *${entry_price:,.2f}*\n"
            + (f"🏁 *Exit*       : *${exit_price:,.2f}*\n" if exit_price is not None else "")
            + f"🔢 *Qty (leg)*  : *{int(size)}* shares\n"
            + f"📍 *Reason*     : _{stop_label}_\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Final Realized P&L*: *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R*         : *{_fmt_r(r_mult)}*\n"
            + (f"📍 *Target*     : *{target_reached}*\n" if target_reached else "")
            + _aggregated_pnl_note(aggregated_parts, "en")
        )
        if sibling_tp2_open and leg_role == "TP1":
            body += (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                "⏳ *TP2 leg* is still open — managed separately.\n"
            )
    else:
        title = _outcome_title_ar(pnl, partial=partial)
        body = (
            title
            + f"🔔 *إغلاق آلي (البوت)* — *{symbol}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"▶️ *الاتجاه*    : *{_dir_ar(direction)}*\n"
            + f"💰 *سعر الدخول*: *${entry_price:,.2f}*\n"
            + (f"🏁 *سعر الخروج*: *${exit_price:,.2f}*\n" if exit_price is not None else "")
            + f"🔢 *كمية الحد* : *{int(size)}* سهم\n"
            + f"📍 *السبب*     : _{stop_label}_\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*الربح/الخسارة النهائي*: *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R*         : *{_fmt_r(r_mult)}*\n"
            + (f"📍 *الهدف*     : *{target_reached}*\n" if target_reached else "")
            + _aggregated_pnl_note(aggregated_parts, "ar")
        )
        if sibling_tp2_open and leg_role == "TP1":
            body += (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                "⏳ *حد الهدف الثاني* لا يزال مفتوحاً — يُدار بشكل منفصل.\n"
            )

    send_telegram_message(chat_id, body)


def send_bot_automated_close_from_db(trade_id: int) -> bool:
    """
    Send the automated close Telegram using persisted rows only (DB-first reporting).
    Call after close_trade_in_db() with status CLOSED and synced PnL.
    """
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT chat_id, symbol, direction, entry_price, exit_price, size, pnl, "
        "trailing_stop, stop_distance, COALESCE(leg_role,''), COALESCE(parent_session,''), "
        "COALESCE(target_reached,''), COALESCE(close_reason,''), COALESCE(pnl_trade_parts,0) "
        "FROM trades WHERE trade_id=? AND status='CLOSED'",
        (int(trade_id),),
    ).fetchone()
    conn.close()
    if not row:
        return False
    (
        chat_id,
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
        target_reached,
        close_reason,
        pnl_trade_parts,
    ) = row
    tid = int(trade_id)
    agg_parts = int(pnl_trade_parts) if pnl_trade_parts and int(pnl_trade_parts) > 1 else None
    sibling_tp2_open = False
    if (leg_role or "").strip().upper() == "TP1" and (parent_session or "").strip():
        sibling_tp2_open = (
            count_open_sibling_same_session(
                str(parent_session).strip(), tid, "TP2"
            )
            > 0
        )
    exit_price_v, pnl_v = _emergency_refresh_final_close(
        trade_id=tid,
        entry_price=float(entry_price or 0.0),
        exit_price=float(exit_price) if exit_price is not None else None,
        size=float(size or 0.0),
        direction=str(direction),
        pnl=float(pnl) if pnl is not None else None,
    )

    send_bot_automated_close(
        str(chat_id),
        trade_id=tid,
        symbol=str(symbol),
        direction=str(direction),
        entry_price=float(entry_price or 0),
        exit_price=float(exit_price_v) if exit_price_v is not None else None,
        size=float(size or 0),
        pnl=float(pnl_v or 0),
        stop_distance=float(stop_distance) if stop_distance is not None else None,
        trailing_stop=float(trailing_stop) if trailing_stop is not None else None,
        stop_label=str(close_reason or "—"),
        sibling_tp2_open=sibling_tp2_open,
        leg_role=str(leg_role or "SINGLE"),
        target_reached=str(target_reached or "") or None,
        aggregated_parts=agg_parts,
    )
    return True


def count_open_sibling_same_session(
    parent_session: str | None,
    exclude_trade_id: int | None,
    leg_filter: str | None = None,
) -> int:
    if not parent_session:
        return 0
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    q = (
        "SELECT COUNT(*) FROM trades WHERE parent_session=? AND status='OPEN'"
    )
    args: list[Any] = [parent_session]
    if exclude_trade_id is not None:
        q += " AND trade_id != ?"
        args.append(exclude_trade_id)
    if leg_filter:
        q += " AND COALESCE(leg_role,'')=?"
        args.append(leg_filter)
    c.execute(q, tuple(args))
    n = c.fetchone()[0]
    conn.close()
    return int(n)
