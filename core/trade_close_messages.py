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
        return "✅ *Winning trade*" + (" — partial" if partial else "") + "\n"
    if pnl < 0:
        return "❌ *Losing trade*" + (" — partial" if partial else "") + "\n"
    return "⚖️ *Breakeven*\n"


def send_reconcile_tp1_hit(
    chat_id: str,
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    size: float,
    pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
    tp2_still_open: bool,
) -> None:
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    r_leg = pnl_to_r_multiple(pnl, sd, size)
    lang = get_subscriber_lang(chat_id)

    if lang == "en":
        body = (
            _outcome_title_en(pnl, partial=True)
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *Symbol*     : *{symbol}*\n"
            + f"▶️ *Direction*  : *{direction}*\n"
            + f"💰 *Entry*      : *${entry_price:,.2f}*\n"
            + f"🔢 *Qty (leg 1)*: *{int(size)}* shares\n"
            + f"💵 *P&L (broker)*: *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R (this leg)*: *{_fmt_r(r_leg)}*\n"
            + f"📍 *Hit*        : *TP1 (1R target)*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
        )
        if tp2_still_open:
            body += (
                "⏳ *TP2 (1.5%)* is still open — waiting for the trailing phase "
                "or stop management.\n"
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
        )
        if tp2_still_open:
            body += (
                "⏳ *الهدف الثاني (1.5%)* لا يزال مفتوحاً — ننتظر تفعيل التريلينج أو إدارة الوقف.\n"
            )
        else:
            body += "ℹ️ لا يوجد حد ثاني مسجّل كمفتوح.\n"

    send_telegram_message(chat_id, body)


def send_reconcile_tp2_final(
    chat_id: str,
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    total_qty: float,
    tp1_pnl: float,
    tp2_pnl: float,
    total_pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
) -> None:
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    total_r = pnl_to_r_multiple(total_pnl, sd, total_qty)
    lang = get_subscriber_lang(chat_id)

    if lang == "en":
        title = _outcome_title_en(total_pnl, partial=False)
        body = (
            title
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *Symbol*        : *{symbol}*\n"
            + f"▶️ *Direction*     : *{direction}*\n"
            + f"💰 *Entry*         : *${entry_price:,.2f}*\n"
            + f"🔢 *Total quantity*: *{int(total_qty)}* shares\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"💰 *TP1 (1R) P&L*  : *{_fmt_money_signed(tp1_pnl)}*\n"
            + f"💰 *TP2 (1.5%) P&L*: *{_fmt_money_signed(tp2_pnl)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Total P&L*       : *{_fmt_money_signed(total_pnl)}*\n"
            + f"🎯 *R*             : *{_fmt_r(total_r)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"_P&L from broker history where available._\n"
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
            + f"_القيم محسوبة من بيانات المنصة حيث توفرت._\n"
        )

    send_telegram_message(chat_id, body)


def send_reconcile_generic_external(
    chat_id: str,
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    size: float,
    pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
    reason_hint: str = "external_close",
) -> None:
    """Single-leg / legacy row closed on broker (TP, SL, manual)."""
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    r_mult = pnl_to_r_multiple(pnl, sd, size)
    lang = get_subscriber_lang(chat_id)

    if lang == "en":
        title = _outcome_title_en(pnl, partial=False)
        body = (
            title
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"📌 *Symbol*     : *{symbol}*\n"
            + f"▶️ *Direction*  : *{direction}*\n"
            + f"💰 *Entry*      : *${entry_price:,.2f}*\n"
            + f"🔢 *Quantity*   : *{int(size)}* shares\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Total P&L*    : *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R*         : *{_fmt_r(r_mult)}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"_Close synced from platform ({reason_hint})._\n"
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
        )

    send_telegram_message(chat_id, body)


def send_bot_automated_close(
    chat_id: str,
    *,
    symbol: str,
    direction: str,
    entry_price: float,
    size: float,
    pnl: float,
    stop_distance: float | None,
    trailing_stop: float | None,
    stop_label: str,
    sibling_tp2_open: bool,
    leg_role: str,
) -> None:
    sd = _resolve_stop_distance(stop_distance, entry_price, trailing_stop)
    r_mult = pnl_to_r_multiple(pnl, sd, size)
    lang = get_subscriber_lang(chat_id)
    partial = bool(leg_role == "TP1" and sibling_tp2_open)

    if lang == "en":
        title = _outcome_title_en(pnl, partial=partial)
        body = (
            title
            + f"🔔 *Auto close (bot)* — *{symbol}*\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"▶️ *Direction*  : *{direction}*\n"
            + f"💰 *Entry*      : *${entry_price:,.2f}*\n"
            + f"🔢 *Qty (leg)*  : *{int(size)}* shares\n"
            + f"📍 *Reason*     : _{stop_label}_\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Total P&L*    : *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R*         : *{_fmt_r(r_mult)}*\n"
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
            + f"🔢 *كمية الحد* : *{int(size)}* سهم\n"
            + f"📍 *السبب*     : _{stop_label}_\n"
            + f"━━━━━━━━━━━━━━━━━━━━\n"
            + f"*Total P&L*    : *{_fmt_money_signed(pnl)}*\n"
            + f"🎯 *R*         : *{_fmt_r(r_mult)}*\n"
        )
        if sibling_tp2_open and leg_role == "TP1":
            body += (
                f"━━━━━━━━━━━━━━━━━━━━\n"
                "⏳ *حد الهدف الثاني* لا يزال مفتوحاً — يُدار بشكل منفصل.\n"
            )

    send_telegram_message(chat_id, body)


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
