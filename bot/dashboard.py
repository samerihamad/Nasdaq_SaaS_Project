"""
NATB v2.0 — Telegram Dashboard & Onboarding Bot

New subscriber flow:
  /start -> Language -> First Name -> Last Name -> Continue -> Bank Details
         -> Upload Payment Proof -> (Admin approves) -> Enter License Key
         -> Capital.com Email -> API Password -> API Key
         -> Connected + Balance -> Main Dashboard

Existing subscriber:
  /start -> license check -> Main Dashboard
"""

import os
import io
import csv
import asyncio
import sqlite3
import requests
import sys
import re
from datetime import date, timedelta, datetime
from dotenv import load_dotenv

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes, ConversationHandler,
    PicklePersistence,
)
from telegram.request import HTTPXRequest
from telegram.error import BadRequest

from bot.i18n import t
from bot.licensing import (
    check_license, store_credentials, generate_license_key,
    safe_decrypt, validate_license_key,
)
from bot.notifier import send_telegram_message, notify_admin_payment
from core.risk_manager import (
    apply_manual_override, apply_day_halt, resume_day_halt,
    get_effective_leverage, get_user_max_leverage,
    get_risk_state, STATE_USER_DAY_HALT,
)
from core.executor import get_user_credentials, get_session
from core.sync import reconcile, backfill_closed_pnls
from core.trailing_stop import close_trade_in_db
from core.trade_session_finalize import after_trade_leg_closed
from bot.admin import admin_handler, limits_handler, monitor_handler, audit_sync_handler
from database.db_manager import (
    create_db, get_bank_details,
    get_trading_enabled, set_trading_enabled,
    apply_subscription_cancellation, get_preferred_leverage,
    set_preferred_leverage, infer_subscription_start, is_maintenance_mode,
    get_user_signal_profile, set_user_signal_profile,
    touch_bot_activity,
)
from utils.market_hours import (
    get_market_status,
    STATUS_OPEN, STATUS_PRE_MARKET, STATUS_AFTER_HRS,
    market_open_in_uae, market_close_in_uae, next_market_open_in_uae,
    uae_offset_hours, utc_today,
)

load_dotenv()

# Always route support actions to the official company support bot.
SUPPORT_URL = 'https://t.me/NATBSupport_bot'

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

DB_PATH = 'database/trading_saas.db'

# Ensure schema exists/was migrated before any SQL queries.
create_db()

# ── Conversation states ────────────────────────────────────────────────────────
(
    LANG_SELECT,
    GET_FIRSTNAME,
    GET_LASTNAME,
    GET_PHONE,
    AWAIT_BANK_ACK,
    UPLOAD_PROOF,
    ENTER_LICENSE,
    GET_EMAIL,
    GET_PASS,
    GET_APIKEY,
) = range(10)

SUBSCRIPTION_DAYS = 30   # single-plan validity per approval


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _db():
    return sqlite3.connect(DB_PATH)


def _get_subscriber(chat_id: str) -> dict:
    conn = _db()
    c    = conn.cursor()
    c.execute(
        """SELECT lang, first_name, last_name, phone, payment_status,
                  payment_proof, license_key, email, expiry_date, mode
           FROM subscribers WHERE chat_id=?""",
        (str(chat_id),)
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return {}
    keys = [
        'lang', 'first_name', 'last_name', 'phone', 'payment_status',
        'payment_proof', 'license_key', 'email', 'expiry_date', 'mode'
    ]
    return dict(zip(keys, row))


def _normalize_spaces(s: str) -> str:
    return ' '.join((s or '').split()).strip()


def _is_valid_name_input(s: str) -> bool:
    """
    Allow letters + spaces + common name separators (hyphen/apostrophe).
    Reject digits and all other symbols to protect stored customer data.
    """
    s = _normalize_spaces(s)
    if len(s) < 2:
        return False
    if any(ch.isdigit() for ch in s):
        return False
    allowed_separators = {" ", "-", "'"}
    for ch in s:
        if ch in allowed_separators:
            continue
        # .isalpha() covers Arabic and Latin letters.
        if not ch.isalpha():
            return False
    return True


def _get_lang(chat_id: str) -> str:
    conn = _db()
    c    = conn.cursor()
    c.execute("SELECT lang FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row  = c.fetchone()
    conn.close()
    return (row[0] if row and row[0] else 'ar')


def _get_mode(chat_id: str) -> str:
    conn = _db()
    c    = conn.cursor()
    c.execute("SELECT mode FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row  = c.fetchone()
    conn.close()
    return (row[0] if row and row[0] else 'AUTO')


def _set_lang(chat_id: str, lang: str):
    conn = _db()
    conn.execute("UPDATE subscribers SET lang=? WHERE chat_id=?", (lang, str(chat_id)))
    conn.commit()
    conn.close()


def _set_mode(chat_id: str, mode: str):
    conn = _db()
    conn.execute("UPDATE subscribers SET mode=? WHERE chat_id=?", (mode, str(chat_id)))
    conn.commit()
    conn.close()


def _ensure_subscriber_row(chat_id: str):
    conn = _db()
    conn.execute(
        "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (str(chat_id),)
    )
    conn.commit()
    conn.close()
    try:
        touch_bot_activity(chat_id)
    except Exception:
        pass


def _update_subscriber(chat_id: str, **kwargs):
    if not kwargs:
        return
    sets   = ', '.join(f"{k}=?" for k in kwargs)
    values = list(kwargs.values()) + [str(chat_id)]
    conn   = _db()
    conn.execute(f"UPDATE subscribers SET {sets} WHERE chat_id=?", values)
    conn.commit()
    conn.close()


def _get_user_onboarding_state(chat_id: str) -> str:
    """
    Returns one of:
      FULLY_ACTIVE     — has API credentials + valid license
      NEEDS_RENEWAL    — has credentials + stale license row (expired); clear and renew
      APPROVED_MISSING_LICENSE — paid (APPROVED) but license_key missing; ask for key, not bank
      LICENSE_READY    — admin approved, license issued, waiting for API creds
      PAYMENT_PENDING  — proof uploaded, awaiting admin review
      PAYMENT_REJECTED — admin rejected the payment
      NEEDS_PHONE     — provided names but missing phone number
      HAS_LAST_NAME    — has both names, ready to proceed to payment
      HAS_FIRST_NAME   — has first name, needs last name
      NEW              — brand new, show language picker
    """
    sub = _get_subscriber(chat_id)
    if not sub:
        return 'NEW'

    if sub.get('email'):
        valid, _ = check_license(chat_id)
        if valid:
            return 'FULLY_ACTIVE'
        ps0 = sub.get('payment_status', 'NONE')
        if ps0 != 'PENDING' and sub.get('license_key'):
            return 'NEEDS_RENEWAL'
        if ps0 == 'APPROVED' and not sub.get('license_key'):
            return 'APPROVED_MISSING_LICENSE'

    ps = sub.get('payment_status', 'NONE')

    if sub.get('license_key') and ps == 'APPROVED' and not sub.get('email'):
        return 'LICENSE_READY'

    if ps == 'PENDING':
        return 'PAYMENT_PENDING'

    if ps == 'REJECTED':
        return 'PAYMENT_REJECTED'

    if sub.get('last_name') and not sub.get('phone'):
        return 'NEEDS_PHONE'

    if sub.get('last_name'):
        return 'HAS_LAST_NAME'

    if sub.get('first_name'):
        return 'HAS_FIRST_NAME'

    return 'NEW'


# ── Keyboard builders ──────────────────────────────────────────────────────────

def _lang_keyboard():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton('🇸🇦 العربية', callback_data='onboard_lang_ar'),
        InlineKeyboardButton('🇬🇧 English', callback_data='onboard_lang_en'),
    ]])


def _continue_keyboard(lang: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t('btn_continue', lang), callback_data='onboard_continue')],
    ])


def _paid_keyboard(lang: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(t('btn_i_paid', lang), callback_data='onboard_paid'),
    ]])


def _main_keyboard(lang: str, mode: str, trading: bool):
    """Main menu: Start/Stop = pause only. Full-day block lives under Settings."""
    mode_btn = (
        t('btn_mode_auto',    lang) if mode == 'AUTO'
        else t('btn_mode_hybrid', lang)
    )
    switch_btn = (
        t('btn_switch_hybrid', lang) if mode == 'AUTO'
        else t('btn_switch_auto', lang)
    )
    engine_btn = (
        InlineKeyboardButton(t('btn_engine_stop',  lang), callback_data='toggle_engine')
        if trading else
        InlineKeyboardButton(t('btn_engine_start', lang), callback_data='toggle_engine')
    )
    row_engine = [engine_btn]
    row_settings = [
        InlineKeyboardButton(t('btn_settings', lang), callback_data='settings'),
    ]

    return InlineKeyboardMarkup([
        row_engine,
        [InlineKeyboardButton(t('btn_balance',  lang), callback_data='balance'),
         InlineKeyboardButton(t('btn_report',   lang), callback_data='report')],
        [InlineKeyboardButton(t('btn_manage_trades', lang), callback_data='manage_trades')],
        [InlineKeyboardButton(mode_btn,                callback_data='mode_info'),
         InlineKeyboardButton(switch_btn,              callback_data='toggle_mode')],
        [InlineKeyboardButton(t('btn_license',  lang), callback_data='license'),
         InlineKeyboardButton(t('btn_lang',     lang), callback_data='toggle_lang')],
        row_settings,
        [InlineKeyboardButton(t('btn_support',      lang), url=SUPPORT_URL)],
    ])


def _fetch_open_trades(chat_id: str) -> list[dict]:
    """
    Dashboard source-of-truth for open trades: local DB.
    This avoids false "no positions" when broker /positions is temporarily empty/failing.
    """
    conn = _db()
    c = conn.cursor()
    c.execute(
        "SELECT trade_id, symbol, direction, entry_price, size, deal_id, trailing_stop, "
        "COALESCE(leg_role,''), COALESCE(parent_session,''), stop_distance "
        "FROM trades WHERE chat_id=? AND status='OPEN' ORDER BY trade_id ASC",
        (str(chat_id),),
    )
    rows = c.fetchall()
    conn.close()

    out: list[dict] = []
    for r in rows:
        out.append({
            "trade_id": int(r[0]),
            "symbol": r[1],
            "direction": r[2],
            "entry_price": float(r[3] or 0),
            "size": float(r[4] or 0),
            "deal_id": str(r[5] or ""),
            "trailing_stop": float(r[6]) if r[6] is not None else None,
            "leg_role": (r[7] or "").strip(),
            "parent_session": (r[8] or "").strip(),
            "stop_distance": float(r[9]) if r[9] is not None else None,
        })
    return out


def _group_open_trades(rows: list[dict]) -> list[dict]:
    """
    Group TP1/TP2 legs into a single display card by parent_session.
    Single-leg trades are keyed by their own trade_id.
    """
    groups: dict[str, list[dict]] = {}
    for r in rows:
        key = r["parent_session"] if r.get("parent_session") else f"single:{r['trade_id']}"
        groups.setdefault(key, []).append(r)

    cards: list[dict] = []
    for key, legs in groups.items():
        legs = sorted(legs, key=lambda x: x["trade_id"])
        first = legs[0]
        symbol = first["symbol"]
        direction = first["direction"]
        entry = float(first["entry_price"] or 0)
        qty_total = float(sum(l.get("size", 0) for l in legs))

        stops = [l.get("trailing_stop") for l in legs if l.get("trailing_stop") is not None]
        stop = None
        if stops:
            stop = min(stops) if direction == "BUY" else max(stops)

        cards.append({
            "key": key,
            "trade_id": int(first["trade_id"]),
            "symbol": symbol,
            "direction": direction,
            "entry_price": entry,
            "qty": qty_total,
            "stop": stop,
        })

    return sorted(cards, key=lambda c: int(c["trade_id"]))


def _format_manage_trade_card(card: dict, lang: str) -> str:
    from config import TP1_PCT, TP2_PCT
    symbol = card["symbol"]
    direction = card["direction"]
    entry = float(card["entry_price"] or 0)
    qty = float(card["qty"] or 0)
    stop = card.get("stop")
    total_amount = entry * qty if entry and qty else 0.0

    if direction == "BUY":
        tp1 = entry * (1 + TP1_PCT)
        tp2 = entry * (1 + TP2_PCT)
        dir_ar = "شراء"
        dir_label = "BUY"
    else:
        tp1 = entry * (1 - TP1_PCT)
        tp2 = entry * (1 - TP2_PCT)
        dir_ar = "بيع"
        dir_label = "SELL"

    stop_s = f"${stop:,.2f}" if isinstance(stop, (int, float)) else "—"
    if lang == "en":
        return (
            f"📋 *Trade #{card['trade_id']} — {symbol}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"▶️ *Direction*   : *{dir_label}*\n"
            f"💰 *Entry*       : *${entry:,.2f}*\n"
            f"💵 *Amount*      : *${total_amount:,.2f}*\n"
            f"🔴 *Stop*        : *{stop_s}*\n"
            f"🎯 *TP1*         : *${tp1:,.2f}*  (+1.00%)\n"
            f"🏆 *TP2*         : *${tp2:,.2f}*  (+1.50%)\n"
        )
    return (
        f"📋 *صفقة #{card['trade_id']} — {symbol}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"▶️ *الاتجاه*      : *{dir_ar}*\n"
        f"💰 *سعر الدخول*   : *${entry:,.2f}*\n"
        f"💵 *إجمالي المبلغ*: *${total_amount:,.2f}*\n"
        f"🔴 *وقف الخسارة*  : *{stop_s}*\n"
        f"🎯 *الهدف 1*      : *${tp1:,.2f}*  (+1.00%)\n"
        f"🏆 *الهدف 2*      : *${tp2:,.2f}*  (+1.50%)\n"
    )


def _fetch_live_positions(base_url: str, headers: dict) -> list[dict]:
    """Fetch live broker positions. Returns [] on failure."""
    try:
        res = requests.get(f"{base_url}/positions", headers=headers, timeout=15)
        if res.status_code != 200:
            return []
        payload = res.json() if res.content else {}
        return (payload or {}).get("positions", []) or []
    except Exception:
        return []


def _cards_from_live_positions(chat_id: str, positions: list[dict]) -> list[dict]:
    """
    Build manage-trade cards from broker positions, enriched by DB (symbol + parent_session).
    Cards are grouped by parent_session when available; otherwise single by deal_id.
    """
    # Map deal_id -> (trade_id, symbol, parent_session, trailing_stop)
    deal_ids = []
    for p in positions:
        try:
            did = str(p.get("position", {}).get("dealId") or "")
        except Exception:
            did = ""
        if did:
            deal_ids.append(did)

    db_map: dict[str, dict] = {}
    if deal_ids:
        conn = _db()
        c = conn.cursor()
        q = (
            "SELECT trade_id, symbol, deal_id, COALESCE(parent_session,''), trailing_stop "
            "FROM trades WHERE chat_id=? AND status='OPEN' AND deal_id IN ({})"
        ).format(",".join(["?"] * len(deal_ids)))
        c.execute(q, (str(chat_id), *deal_ids))
        for trade_id, symbol, deal_id, parent_session, trailing_stop in c.fetchall():
            db_map[str(deal_id)] = {
                "trade_id": int(trade_id),
                "symbol": symbol,
                "parent_session": (parent_session or "").strip(),
                "trailing_stop": float(trailing_stop) if trailing_stop is not None else None,
            }
        conn.close()

    # Group positions by parent_session when possible
    groups: dict[str, list[dict]] = {}
    for p in positions:
        pos = p.get("position") or {}
        mkt = p.get("market") or {}
        deal_id = str(pos.get("dealId") or "")
        if not deal_id:
            continue

        direction = str(pos.get("direction") or "")
        entry = float(pos.get("level") or 0.0)
        size = float(pos.get("size") or 0.0)
        stop_level = pos.get("stopLevel")
        stop = float(stop_level) if stop_level is not None else None

        meta = db_map.get(deal_id) or {}
        symbol = meta.get("symbol") or mkt.get("instrumentName") or mkt.get("epic") or "UNKNOWN"
        trade_id = int(meta.get("trade_id") or 0)
        parent_session = str(meta.get("parent_session") or "").strip()
        if stop is None:
            stop = meta.get("trailing_stop")

        key = parent_session if parent_session else f"deal:{deal_id}"
        groups.setdefault(key, []).append({
            "deal_id": deal_id,
            "trade_id": trade_id,
            "symbol": symbol,
            "direction": direction,
            "entry_price": entry,
            "qty": size,
            "stop": stop,
        })

    cards: list[dict] = []
    for key, legs in groups.items():
        legs = sorted(legs, key=lambda x: int(x.get("trade_id") or 0))
        first = legs[0]
        direction = first["direction"]
        entry = float(first["entry_price"] or 0)
        qty_total = float(sum(l.get("qty", 0) for l in legs))
        stops = [l.get("stop") for l in legs if isinstance(l.get("stop"), (int, float))]
        stop = None
        if stops:
            stop = min(stops) if direction == "BUY" else max(stops)
        trade_id = min([int(l.get("trade_id") or 0) for l in legs if int(l.get("trade_id") or 0) > 0] or [0])
        cards.append({
            "key": key,
            "trade_id": trade_id,
            "symbol": first["symbol"],
            "direction": direction,
            "entry_price": entry,
            "qty": qty_total,
            "stop": stop,
            "deal_ids": [l["deal_id"] for l in legs],
        })

    # Sort stable by trade_id (fallback to symbol)
    return sorted(cards, key=lambda c: (int(c.get("trade_id") or 0), str(c.get("symbol") or "")))


def _back_keyboard(lang: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(t('btn_back', lang), callback_data='main_menu')
    ]])


def _can_cancel_subscription(chat_id: str) -> bool:
    valid, _ = check_license(chat_id)
    if not valid:
        return False
    conn = _db()
    c    = conn.cursor()
    c.execute("SELECT payment_status FROM subscribers WHERE chat_id=?", (chat_id,))
    row = c.fetchone()
    conn.close()
    if not row or row[0] != 'APPROVED':
        return False
    start = infer_subscription_start(chat_id)
    if not start:
        return False
    return (utc_today() - start).days <= 30


def _settings_menu_keyboard(lang: str, chat_id: str):
    rows = []
    current_profile = get_user_signal_profile(chat_id)
    rows.append([InlineKeyboardButton(
        t('btn_signal_profile', lang, profile=_profile_label(current_profile, lang)),
        callback_data='settings_signal_profile')])
    if _can_cancel_subscription(chat_id):
        rows.append([InlineKeyboardButton(
            t('btn_cancel_subscription', lang), callback_data='settings_cancel_warn')])
    risk = get_risk_state(chat_id)
    if risk == STATE_USER_DAY_HALT:
        rows.append([InlineKeyboardButton(
            t('btn_resume_day_trading', lang), callback_data='settings_resume_day_halt')])
    else:
        rows.append([InlineKeyboardButton(
            t('btn_day_halt', lang), callback_data='settings_day_halt')])
    rows.append([InlineKeyboardButton(
        t('btn_leverage_settings', lang), callback_data='settings_leverage')])
    rows.append([InlineKeyboardButton(
        t('btn_update_broker', lang), callback_data='settings_broker_warn')])
    rows.append([InlineKeyboardButton(t('btn_back', lang), callback_data='main_menu')])
    return InlineKeyboardMarkup(rows)


def _leverage_choice_keyboard(lang: str, cap: int):
    nums = list(range(1, cap + 1))
    rows = []
    row = []
    for n in nums:
        row.append(InlineKeyboardButton(f'{n}x', callback_data=f'lev_set_{n}'))
        if len(row) >= 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(t('btn_back', lang), callback_data='settings')])
    return InlineKeyboardMarkup(rows)


def _contact_support_keyboard(lang: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(t('btn_contact_support', lang), url=SUPPORT_URL),
    ]])


def _profile_label(profile: str, lang: str) -> str:
    p = str(profile or "FAST").strip().upper()
    return t('profile_golden', lang) if p == 'GOLDEN' else t('profile_fast', lang)


def _signal_profile_keyboard(lang: str, chat_id: str):
    current = get_user_signal_profile(chat_id)
    fast_label = f"✅ {t('profile_fast', lang)}" if current == "FAST" else t('profile_fast', lang)
    golden_label = f"✅ {t('profile_golden', lang)}" if current == "GOLDEN" else t('profile_golden', lang)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(fast_label, callback_data='settings_profile_fast')],
        [InlineKeyboardButton(golden_label, callback_data='settings_profile_golden')],
        [InlineKeyboardButton(t('btn_back', lang), callback_data='settings')],
    ])


# ── Bank details helper ────────────────────────────────────────────────────────

def _build_bank_text(lang: str) -> str:
    b    = get_bank_details()
    body = t('bank_details_body', lang,
             bank_name      = b.get('BANK_NAME', '—'),
             account_name   = b.get('BANK_ACCOUNT_NAME', '—'),
             account_number = b.get('BANK_ACCOUNT_NUMBER', '—'),
             iban           = b.get('BANK_IBAN', '—'),
             swift          = b.get('BANK_SWIFT', '—'),
             currency       = b.get('BANK_CURRENCY', 'AED'),
             branch         = b.get('BANK_BRANCH', '—'),
             instructions   = b.get('BANK_INSTRUCTIONS', ''))
    return t('bank_details_title', lang) + '\n\n' + body


def _fetch_live_equity(base_url: str, headers: dict) -> tuple[float | None, str]:
    """
    Return live account equity (preferred) and currency.
    Falls back to: balance + sum(open UPL) when equity is not provided.
    """
    res = requests.get(f"{base_url}/accounts", headers=headers, timeout=15)
    if res.status_code != 200:
        return None, 'USD'

    accounts = (res.json() or {}).get('accounts', []) or []
    if not accounts:
        return None, 'USD'

    acc = accounts[0]
    bal = acc.get('balance', {}) or {}

    currency = (
        bal.get('currency')
        or acc.get('currency')
        or 'USD'
    )

    # 1) Prefer native equity field if present.
    equity = bal.get('equity', acc.get('equity'))
    if isinstance(equity, (int, float)):
        return float(equity), currency

    # 2) Fallback to balance + open UPL.
    base_balance = bal.get('balance', acc.get('balance'))
    if not isinstance(base_balance, (int, float)):
        return None, currency

    upl_sum = 0.0
    try:
        p_res = requests.get(f"{base_url}/positions", headers=headers, timeout=15)
        if p_res.status_code == 200:
            for p in (p_res.json() or {}).get('positions', []):
                upl_sum += float(p.get('position', {}).get('upl', 0.0) or 0.0)
    except Exception:
        pass

    return float(base_balance) + upl_sum, currency


# ── Main menu display ──────────────────────────────────────────────────────────

def _engine_status_line(chat_id: str, lang: str) -> str:
    """Return the one-line engine status shown at the top of the dashboard."""
    if is_maintenance_mode():
        return t('engine_status_maintenance', lang)
    risk = get_risk_state(chat_id)
    if risk == STATE_USER_DAY_HALT:
        return t('engine_status_day_block', lang)
    trading = get_trading_enabled(chat_id)
    if not trading:
        return t('engine_status_off', lang)
    # Engine on: show market activity. Risk limits (circuit breaker, etc.) do not
    # replace "system running" here — that is pause vs run only.
    market = get_market_status()
    if market == STATUS_OPEN:
        return t('engine_status_on_open', lang)
    if market == STATUS_PRE_MARKET:
        return t('engine_status_on_premarket', lang)
    return t('engine_status_on_closed', lang)


async def _show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          edit: bool = False):
    chat_id = str(update.effective_chat.id)
    lang    = _get_lang(chat_id)
    mode    = _get_mode(chat_id)
    trading = get_trading_enabled(chat_id)
    name    = update.effective_user.first_name or ''
    name    = name.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')
    text    = t('main_menu_title', lang, name=name) + '\n\n' + _engine_status_line(chat_id, lang)
    markup  = _main_keyboard(lang, mode, trading)

    if edit and update.callback_query:
        await update.callback_query.edit_message_text(
            text, reply_markup=markup, parse_mode='Markdown'
        )
    else:
        msg = update.message or update.callback_query.message
        await msg.reply_text(text, reply_markup=markup, parse_mode='Markdown')


# ── Credential helpers ────────────────────────────────────────────────────────

def _clear_credentials(chat_id: str):
    """Wipe stored broker credentials so the user must re-enter them."""
    conn = _db()
    conn.execute(
        "UPDATE subscribers SET email=NULL, api_password=NULL, api_key=NULL "
        "WHERE chat_id=?",
        (chat_id,)
    )
    conn.commit()
    conn.close()


# ── Periodic system-status job ─────────────────────────────────────────────────

async def _send_status_update(context: ContextTypes.DEFAULT_TYPE):
    """Hourly job: send market / bot status to the subscriber."""
    job     = context.job
    chat_id = job.data['chat_id']
    lang    = _get_lang(chat_id)

    # Stop the job if the license has expired
    valid, _ = check_license(chat_id)
    if not valid:
        job.schedule_removal()
        return

    market    = get_market_status()
    open_gst  = market_open_in_uae()
    close_gst = market_close_in_uae()
    next_gst  = next_market_open_in_uae()
    offset    = uae_offset_hours()          # 8h (summer/EDT) or 9h (winter/EST)

    if market == STATUS_OPEN:
        msg = (t('status_running', lang, close_gst=close_gst, offset=offset)
               + '\n\n' + t('status_no_opportunities', lang))
    elif market == STATUS_PRE_MARKET:
        msg = t('status_premarket', lang, open_gst=open_gst, offset=offset)
    elif market == STATUS_AFTER_HRS:
        msg = t('status_afterhours', lang, next_gst=next_gst, offset=offset)
    else:
        msg = t('status_market_closed', lang, next_gst=next_gst, offset=offset)

    await context.bot.send_message(
        chat_id    = int(chat_id),
        text       = msg,
        parse_mode = 'Markdown',
    )


def _schedule_status_job(context: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """Start (or restart) the hourly status broadcast for a subscriber."""
    jq = context.job_queue
    if jq is None:
        # Without extras, PTB has no JobQueue — do not block the main menu.
        print(
            "[dashboard] JobQueue unavailable (pip install "
            '"python-telegram-bot[job-queue]"). Hourly status jobs skipped.'
        )
        return
    for job in jq.get_jobs_by_name(f'status_{chat_id}'):
        job.schedule_removal()
    jq.run_repeating(
        _send_status_update,
        interval = 3600,
        first    = 10,          # first message 10 s after login
        chat_id  = int(chat_id),
        name     = f'status_{chat_id}',
        data     = {'chat_id': chat_id},
    )


# ── /start entry point ─────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    _ensure_subscriber_row(chat_id)
    try:
        touch_bot_activity(chat_id)
    except Exception:
        pass

    state = _get_user_onboarding_state(chat_id)
    lang  = _get_lang(chat_id)

    # ── Expired license row but broker credentials still stored ─────────────
    if state == 'NEEDS_RENEWAL':
        _update_subscriber(chat_id,
                           payment_status='NONE',
                           license_key=None,
                           expiry_date=None)
        await update.message.reply_text(
            t('license_expired', lang), parse_mode='Markdown'
        )
        await update.message.reply_text(
            t('select_plan_title', lang),
            reply_markup=_continue_keyboard(lang),
            parse_mode='Markdown',
        )
        return AWAIT_BANK_ACK

    # ── Paid but license row missing (corruption / failed write) ────────────
    if state == 'APPROVED_MISSING_LICENSE':
        await update.message.reply_text(
            t('approved_missing_license', lang),
            reply_markup=_contact_support_keyboard(lang),
            parse_mode='Markdown',
        )
        return ENTER_LICENSE

    # ── Already fully registered ───────────────────────────────────────────
    if state == 'FULLY_ACTIVE':
        valid, days = check_license(chat_id)
        if not valid:
            # Clear expired license and restart the payment/renewal flow
            _update_subscriber(chat_id,
                               payment_status='NONE',
                               license_key=None,
                               expiry_date=None)
            await update.message.reply_text(
                t('license_expired', lang), parse_mode='Markdown'
            )
            await update.message.reply_text(
                t('select_plan_title', lang),
                reply_markup=_continue_keyboard(lang),
                parse_mode='Markdown',
            )
            return AWAIT_BANK_ACK
        _schedule_status_job(context, chat_id)
        await _show_main_menu(update, context)
        return ConversationHandler.END

    # ── License issued, needs API creds ───────────────────────────────────
    if state == 'LICENSE_READY':
        sub  = _get_subscriber(chat_id)
        name = sub.get('first_name') or ''
        name = name.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')
        await update.message.reply_text(
            t('enter_license_prompt', lang, name=name), parse_mode='Markdown'
        )
        return ENTER_LICENSE

    # ── Payment under review ───────────────────────────────────────────────
    if state == 'PAYMENT_PENDING':
        await update.message.reply_text(
            t('payment_pending', lang), parse_mode='Markdown'
        )
        return ConversationHandler.END

    # ── Payment was rejected ───────────────────────────────────────────────
    if state == 'PAYMENT_REJECTED':
        _update_subscriber(chat_id, payment_status='NONE', payment_proof=None)
        await update.message.reply_text(
            t('payment_rejected_user', lang), parse_mode='Markdown'
        )
        await update.message.reply_text(t('lang_select'), reply_markup=_lang_keyboard())
        return LANG_SELECT

    # ── Has last name, proceed to payment ──────────────────────────────────
    if state == 'NEEDS_PHONE':
        await update.message.reply_text(t('ask_phone', lang), parse_mode='Markdown')
        return GET_PHONE

    # ── Has last name, proceed to payment ──────────────────────────────────
    if state == 'HAS_LAST_NAME':
        await update.message.reply_text(
            t('select_plan_title', lang),
            reply_markup=_continue_keyboard(lang),
            parse_mode='Markdown',
        )
        return AWAIT_BANK_ACK

    # ── Has first name, ask last name ──────────────────────────────────────
    if state == 'HAS_FIRST_NAME':
        await update.message.reply_text(t('ask_lastname', lang), parse_mode='Markdown')
        return GET_LASTNAME

    # ── Brand new user: language selection ────────────────────────────────
    await update.message.reply_text(t('lang_select'), reply_markup=_lang_keyboard())
    return LANG_SELECT


# ── Onboarding: language selection ────────────────────────────────────────────

async def lang_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat_id)
    lang    = 'ar' if query.data == 'onboard_lang_ar' else 'en'
    _update_subscriber(chat_id, lang=lang)

    await query.edit_message_text(
        t('ask_firstname', lang), parse_mode='Markdown'
    )
    return GET_FIRSTNAME


# ── Onboarding: first name ─────────────────────────────────────────────────────

async def get_firstname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id    = str(update.message.chat_id)
    lang       = _get_lang(chat_id)
    first_name = update.message.text.strip()

    if not first_name:
        await update.message.reply_text(t('ask_firstname', lang), parse_mode='Markdown')
        return GET_FIRSTNAME

    if not _is_valid_name_input(first_name):
        await update.message.reply_text(t('invalid_name', lang), parse_mode='Markdown')
        return GET_FIRSTNAME

    _update_subscriber(chat_id, first_name=first_name)
    await update.message.reply_text(t('ask_lastname', lang), parse_mode='Markdown')
    return GET_LASTNAME


# ── Onboarding: last name ──────────────────────────────────────────────────────

async def get_lastname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id   = str(update.message.chat_id)
    lang      = _get_lang(chat_id)
    last_name = update.message.text.strip()

    if not last_name:
        await update.message.reply_text(t('ask_lastname', lang), parse_mode='Markdown')
        return GET_LASTNAME

    if not _is_valid_name_input(last_name):
        await update.message.reply_text(t('invalid_name', lang), parse_mode='Markdown')
        return GET_LASTNAME

    _update_subscriber(chat_id, last_name=last_name)
    sub  = _get_subscriber(chat_id)
    name = sub.get('first_name', '')
    name = name.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')

    await update.message.reply_text(
        t('welcome_name', lang, name=name), parse_mode='Markdown'
    )
    await update.message.reply_text(t('ask_phone', lang), parse_mode='Markdown')
    return GET_PHONE


# ── Onboarding: phone number ─────────────────────────────────────────────

async def get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    lang    = _get_lang(chat_id)
    phone   = update.message.text.strip()

    # Phone: digits only (no +, no symbols).
    if '+' in phone:
        await update.message.reply_text(t('invalid_phone', lang), parse_mode='Markdown')
        return GET_PHONE

    digits = phone

    digits = digits.replace(' ', '')
    if not digits.isdigit() or len(digits) < 6:
        await update.message.reply_text(t('invalid_phone', lang), parse_mode='Markdown')
        return GET_PHONE

    phone_norm = digits
    _update_subscriber(chat_id, phone=phone_norm)

    await update.message.reply_text(
        t('select_plan_title', lang),
        reply_markup=_continue_keyboard(lang),
        parse_mode='Markdown',
    )
    return AWAIT_BANK_ACK


# ── Onboarding: continue ──────────────────────────────────────────────────────

async def continue_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat_id)
    lang    = _get_lang(chat_id)
    # Single-plan system: treat selection as continue.

    bank_text = _build_bank_text(lang)
    await query.edit_message_text(
        bank_text,
        reply_markup=_paid_keyboard(lang),
        parse_mode='Markdown',
    )
    return AWAIT_BANK_ACK


# ── Onboarding: bank acknowledged (paid button clicked) ───────────────────────

async def bank_acked(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat_id)
    lang    = _get_lang(chat_id)

    # Loading state — reassure user before transitioning
    await query.edit_message_text(t('bank_loading', lang), parse_mode='Markdown')
    await asyncio.sleep(1.5)

    await query.edit_message_text(t('ask_payment_proof', lang), parse_mode='Markdown')
    return UPLOAD_PROOF


# ── Onboarding: payment proof upload ──────────────────────────────────────────

async def upload_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    lang    = _get_lang(chat_id)

    if not update.message.photo:
        await update.message.reply_text(
            t('ask_payment_proof', lang), parse_mode='Markdown'
        )
        return UPLOAD_PROOF

    # Get largest photo size
    file_id = update.message.photo[-1].file_id
    sub     = _get_subscriber(chat_id)

    # Immediate feedback so the user knows their upload was received
    processing_msg = await update.message.reply_text(
        t('processing', lang), parse_mode='Markdown'
    )

    _update_subscriber(chat_id,
                       payment_proof=file_id,
                       payment_status='PENDING')

    # Notify admin
    full_name = f"{sub.get('first_name', '')} {sub.get('last_name', '')}".strip()
    notify_admin_payment(chat_id, full_name, file_id)

    await processing_msg.edit_text(
        t('payment_received', lang), parse_mode='Markdown'
    )
    return ConversationHandler.END


# ── Onboarding: enter license key ─────────────────────────────────────────────

async def enter_license(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id     = str(update.message.chat_id)
    lang        = _get_lang(chat_id)
    license_key = update.message.text.strip()

    had_broker = bool(_get_subscriber(chat_id).get('email'))

    owner_id, expiry = validate_license_key(license_key)
    if not owner_id:
        await update.message.reply_text(
            t('reg_invalid_license', lang),
            reply_markup=_contact_support_keyboard(lang),
            parse_mode='Markdown',
        )
        return ENTER_LICENSE

    # Bind license to this chat_id (may already be bound from admin approval)
    conn = _db()
    conn.execute(
        "UPDATE subscribers SET license_key=?, expiry_date=? WHERE chat_id=?",
        (license_key, expiry, chat_id)
    )
    conn.commit()
    conn.close()

    if had_broker:
        await update.message.reply_text(
            t('license_linked_resume', lang), parse_mode='Markdown'
        )
        _schedule_status_job(context, chat_id)
        await _show_main_menu(update, context)
        return ConversationHandler.END

    await update.message.reply_text(t('broker_intro', lang), parse_mode='Markdown')
    return GET_EMAIL


# ── Onboarding: broker email ───────────────────────────────────────────────────

async def reg_get_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['email'] = update.message.text.strip()
    lang = _get_lang(str(update.message.chat_id))
    await update.message.reply_text(t('reg_ask_pass', lang), parse_mode='Markdown')
    return GET_PASS


# ── Onboarding: broker password ───────────────────────────────────────────────

async def reg_get_pass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['pass'] = update.message.text.strip()
    lang = _get_lang(str(update.message.chat_id))
    await update.message.reply_text(t('reg_ask_key', lang), parse_mode='Markdown')
    return GET_APIKEY


# ── Onboarding: API key + final connection ─────────────────────────────────────

async def reg_get_apikey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    lang    = _get_lang(chat_id)
    api_key = update.message.text.strip()

    # Store encrypted credentials
    store_credentials(
        chat_id,
        context.user_data['email'],
        context.user_data['pass'],
        api_key,
    )

    await update.message.reply_text(t('connecting', lang), parse_mode='Markdown')

    connected    = False
    balance_text = ''
    try:
        creds = get_user_credentials(chat_id)
        if creds:
            base_url, headers = get_session(creds)
            if headers:
                res = requests.get(f"{base_url}/accounts", headers=headers)
                if res.status_code == 200:
                    acc          = res.json()['accounts'][0]
                    bal          = acc['balance']['balance']
                    curr         = acc['balance'].get('currency', 'USD')
                    balance_text = t('connected_success', lang, balance=bal, currency=curr)
                    connected    = True
                else:
                    try:
                        err_text = (res.text or "").strip()[:400]
                    except Exception:
                        err_text = ""
                    print(f"[Capital Accounts Failed] status={res.status_code} response={err_text}")
    except Exception:
        pass

    # ── Connection failed: wipe credentials, re-prompt ──────────────────────
    if not connected:
        _clear_credentials(chat_id)
        await update.message.reply_text(t('connected_error', lang), parse_mode='Markdown')
        await update.message.reply_text(t('broker_intro', lang), parse_mode='Markdown')
        return GET_EMAIL

    # ── Connection confirmed: open dashboard ─────────────────────────────────
    await update.message.reply_text(balance_text, parse_mode='Markdown')
    _schedule_status_job(context, chat_id)
    await _show_main_menu(update, context)
    return ConversationHandler.END


# ── Admin: payment approval callbacks ─────────────────────────────────────────

async def _handle_approve_payment(query, chat_id: str, user_chat_id: str):
    """Admin pressed Approve for a user's payment proof."""
    sub  = _get_subscriber(user_chat_id)
    lang = sub.get('lang', 'ar')
    days = int(SUBSCRIPTION_DAYS)

    license_key = generate_license_key()
    today_utc = utc_today()
    expiry = str(today_utc + timedelta(days=days))

    conn = _db()
    conn.execute(
        """UPDATE subscribers
           SET license_key=?, expiry_date=?, payment_status='APPROVED',
               subscription_started_at=?
           WHERE chat_id=?""",
        (license_key, expiry, str(today_utc), user_chat_id)
    )
    conn.commit()
    conn.close()

    # Notify the subscriber
    send_telegram_message(
        user_chat_id,
        t('payment_approved_user', lang, license_key=license_key, days=days)
    )

    await query.edit_message_caption(
        caption='✅ Approved — License key sent to subscriber.',
        parse_mode='Markdown',
    )


async def _handle_reject_payment(query, chat_id: str, user_chat_id: str):
    """Admin pressed Reject for a user's payment proof."""
    sub  = _get_subscriber(user_chat_id)
    lang = sub.get('lang', 'ar')

    conn = _db()
    conn.execute(
        "UPDATE subscribers SET payment_status='REJECTED' WHERE chat_id=?",
        (user_chat_id,)
    )
    conn.commit()
    conn.close()

    send_telegram_message(user_chat_id, t('payment_rejected_user', lang))

    await query.edit_message_caption(
        caption='❌ Rejected — Subscriber has been notified.',
        parse_mode='Markdown',
    )


# ── Main dashboard callback handler ───────────────────────────────────────────

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    chat_id = str(query.message.chat_id)
    lang    = _get_lang(chat_id)
    data    = query.data
    try:
        touch_bot_activity(chat_id)
    except Exception:
        pass

    # ── Session guard ───────────────────────────────────────────────────────
    # Admin and onboarding callbacks bypass the guard; everything else requires
    # the user to be FULLY_ACTIVE (license valid + credentials stored).
    _bypass = (
        data.startswith('approve_payment_') or
        data.startswith('reject_payment_')  or
        data.startswith('onboard_')
    )
    if not _bypass and _get_user_onboarding_state(chat_id) != 'FULLY_ACTIVE':
        await query.answer(
            '⛔ يرجى إرسال /start للمتابعة.' if lang == 'ar'
            else '⛔ Please send /start to continue.',
            show_alert=True,
        )
        return

    await query.answer()

    # ── Payment approval (admin only) ──────────────────────────────────────
    if data.startswith('approve_payment_'):
        user_chat_id = data.split('approve_payment_', 1)[1]
        await _handle_approve_payment(query, chat_id, user_chat_id)
        return

    if data.startswith('reject_payment_'):
        user_chat_id = data.split('reject_payment_', 1)[1]
        await _handle_reject_payment(query, chat_id, user_chat_id)
        return

    # ── Onboarding fallbacks (bot-restart edge case: save choice + prompt /start)
    if data.startswith('onboard_lang_'):
        chosen_lang = 'ar' if data == 'onboard_lang_ar' else 'en'
        _update_subscriber(chat_id, lang=chosen_lang)
        await query.edit_message_text(
            ('تم اختيار اللغة. أرسل /start للمتابعة.'
             if chosen_lang == 'ar' else
             'Language saved. Send /start to continue.'),
            parse_mode='Markdown',
        )
        return

    if data == 'onboard_continue':
        # Enforce phone registration before continuing.
        sub = _get_subscriber(chat_id)
        if not sub.get('phone'):
            await query.edit_message_text(
                t('onboard_phone_missing_restart', lang),
                parse_mode='Markdown',
            )
            return

        await query.edit_message_text(
            ('تمت المتابعة. أرسل /start للمتابعة.'
             if lang == 'ar' else
             'Continue saved. Send /start to continue.'),
            parse_mode='Markdown',
        )
        return

    if data == 'onboard_paid':
        await query.edit_message_text(
            ('أرسل /start ثم أرفق صورة الإيصال.'
             if lang == 'ar' else
             'Send /start then upload your payment screenshot.'),
            parse_mode='Markdown',
        )
        return

    # ── Balance ────────────────────────────────────────────────────────────
    if data == 'balance':
        await query.edit_message_text(t('balance_loading', lang), parse_mode='Markdown')
        creds = get_user_credentials(chat_id)
        if creds:
            base_url, headers = get_session(creds)
            if headers:
                try:
                    live_equity, curr = _fetch_live_equity(base_url, headers)
                    if live_equity is not None:
                        text = t('balance_result', lang, amount=round(live_equity, 2), currency=curr)
                    else:
                        text = t('balance_error', lang)
                except Exception:
                    text = t('balance_error', lang)
            else:
                text = t('balance_error', lang)
        else:
            text = t('balance_error', lang)
        await asyncio.sleep(1.5)
        await query.edit_message_text(
            text, reply_markup=_back_keyboard(lang), parse_mode='Markdown'
        )

    # ── Performance report sub-menu ────────────────────────────────────────
    elif data == 'report':
        await query.edit_message_text(t('report_loading', lang), parse_mode='Markdown')
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(t('btn_report_daily', lang), callback_data='report_daily')],
            [InlineKeyboardButton(t('btn_report_csv',   lang), callback_data='report_csv')],
            [InlineKeyboardButton(t('btn_back',         lang), callback_data='main_menu')],
        ])
        await query.edit_message_text(
            t('report_menu_title', lang),
            reply_markup=keyboard, parse_mode='Markdown'
        )

    # ── Daily report: ask for date ─────────────────────────────────────────
    elif data == 'report_daily':
        await query.edit_message_text(t('report_loading', lang), parse_mode='Markdown')
        context.user_data['report_state'] = 'AWAIT_DAILY_DATE'
        await query.edit_message_text(
            t('report_ask_date', lang),
            reply_markup=_back_keyboard(lang), parse_mode='Markdown'
        )

    # ── CSV export: ask for start date ────────────────────────────────────
    elif data == 'report_csv':
        await query.edit_message_text(t('report_loading', lang), parse_mode='Markdown')
        context.user_data['report_state'] = 'AWAIT_CSV_START'
        await query.edit_message_text(
            t('report_ask_start_date', lang),
            reply_markup=_back_keyboard(lang), parse_mode='Markdown'
        )

    # ── Manage trades (replaces Open Positions) ────────────────────────────
    elif data in ('positions', 'manage_trades'):
        # Backward-compat: old callback 'positions' now routes to Manage Trades.
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(t('btn_show_open_trades', lang), callback_data='manage_show_open')],
            [InlineKeyboardButton(t('btn_close_all_trades', lang), callback_data='manage_close_all')],
            [InlineKeyboardButton(t('btn_view_trade_by_id', lang), callback_data='manage_view_trade')],
            [InlineKeyboardButton(t('btn_back', lang), callback_data='main_menu')],
        ])
        await query.edit_message_text(
            t('manage_menu_title', lang),
            reply_markup=keyboard,
            parse_mode='Markdown',
        )

    elif data == 'manage_show_open':
        # Source of truth: broker live positions.
        # Step 1: reconcile so stale DB "OPEN" rows are closed if broker has none.
        creds = get_user_credentials(chat_id)
        if not creds:
            await query.edit_message_text(
                t('balance_error', lang),
                reply_markup=_back_keyboard(lang),
                parse_mode='Markdown',
            )
            return
        base_url, headers = get_session(creds)
        if not headers:
            await query.edit_message_text(
                t('balance_error', lang),
                reply_markup=_back_keyboard(lang),
                parse_mode='Markdown',
            )
            return
        try:
            reconcile(chat_id, base_url, headers, notify=False)
        except Exception:
            pass

        positions = _fetch_live_positions(base_url, headers)
        cards = _cards_from_live_positions(chat_id, positions)
        if not cards:
            await query.edit_message_text(
                t('manage_no_open_trades', lang),
                reply_markup=_back_keyboard(lang),
                parse_mode='Markdown',
            )
            return
        context.user_data['manage_cards'] = cards
        context.user_data['manage_idx'] = 0
        card = cards[0]
        nav = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(t('btn_prev', lang), callback_data='manage_prev'),
                InlineKeyboardButton(t('btn_next', lang), callback_data='manage_next'),
            ],
            [InlineKeyboardButton(t('btn_back', lang), callback_data='manage_trades')],
        ])
        await query.edit_message_text(
            _format_manage_trade_card(card, lang),
            reply_markup=nav,
            parse_mode='Markdown',
        )

    elif data in ('manage_prev', 'manage_next'):
        cards = context.user_data.get('manage_cards') or []
        if not cards:
            await query.edit_message_text(
                t('manage_no_open_trades', lang),
                reply_markup=_back_keyboard(lang),
                parse_mode='Markdown',
            )
            return
        idx = int(context.user_data.get('manage_idx') or 0)
        idx = max(0, idx - 1) if data == 'manage_prev' else min(len(cards) - 1, idx + 1)
        context.user_data['manage_idx'] = idx
        card = cards[idx]
        nav = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(t('btn_prev', lang), callback_data='manage_prev'),
                InlineKeyboardButton(t('btn_next', lang), callback_data='manage_next'),
            ],
            [InlineKeyboardButton(t('btn_back', lang), callback_data='manage_trades')],
        ])
        await query.edit_message_text(
            _format_manage_trade_card(card, lang),
            reply_markup=nav,
            parse_mode='Markdown',
        )

    elif data == 'manage_close_all':
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(t('btn_confirm_yes', lang), callback_data='manage_close_all_yes')],
            [InlineKeyboardButton(t('btn_confirm_no',  lang), callback_data='manage_close_all_no')],
        ])
        await query.edit_message_text(
            t('manage_close_all_confirm', lang),
            reply_markup=keyboard,
            parse_mode='Markdown',
        )

    elif data == 'manage_close_all_no':
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(t('btn_show_open_trades', lang), callback_data='manage_show_open')],
            [InlineKeyboardButton(t('btn_close_all_trades', lang), callback_data='manage_close_all')],
            [InlineKeyboardButton(t('btn_view_trade_by_id', lang), callback_data='manage_view_trade')],
            [InlineKeyboardButton(t('btn_back', lang), callback_data='main_menu')],
        ])
        await query.edit_message_text(
            t('manage_menu_title', lang),
            reply_markup=keyboard,
            parse_mode='Markdown',
        )

    elif data == 'manage_close_all_yes':
        # Close ALL broker live positions (source of truth), then persist to DB.
        creds = get_user_credentials(chat_id)
        if not creds:
            await query.edit_message_text(
                t('balance_error', lang),
                reply_markup=_back_keyboard(lang),
                parse_mode='Markdown',
            )
            return
        base_url, headers = get_session(creds)
        if not headers:
            await query.edit_message_text(
                t('balance_error', lang),
                reply_markup=_back_keyboard(lang),
                parse_mode='Markdown',
            )
            return

        positions = _fetch_live_positions(base_url, headers)
        if not positions:
            await query.edit_message_text(
                t('manage_no_open_trades', lang),
                reply_markup=_back_keyboard(lang),
                parse_mode='Markdown',
            )
            return

        upl_by_id: dict[str, float] = {}
        for p in positions:
            did = str((p.get("position") or {}).get("dealId") or "")
            if not did:
                continue
            try:
                upl_by_id[did] = float((p.get("position") or {}).get("upl") or 0.0)
            except Exception:
                upl_by_id[did] = 0.0

        closed = 0
        failed = 0
        # Work on unique deal IDs only; duplicated rows can otherwise inflate "failed".
        unique_deal_ids = []
        seen_deals = set()
        for p in positions:
            did = str((p.get("position") or {}).get("dealId") or "")
            if not did or did in seen_deals:
                continue
            seen_deals.add(did)
            unique_deal_ids.append(did)

        for deal_id in unique_deal_ids:
            try:
                del_res = requests.delete(f"{base_url}/positions/{deal_id}", headers=headers, timeout=20)
                if del_res.status_code != 200:
                    # Some broker responses are non-200 even when the position is already gone.
                    # Verify from live positions before counting as failed.
                    verify_res = requests.get(f"{base_url}/positions", headers=headers, timeout=15)
                    still_open = True
                    if verify_res.status_code == 200:
                        verify_positions = (verify_res.json() or {}).get("positions", []) or []
                        live_ids = {
                            str((vp.get("position") or {}).get("dealId") or "")
                            for vp in verify_positions
                        }
                        still_open = deal_id in live_ids
                    if still_open:
                        failed += 1
                        continue

                upl = float(upl_by_id.get(deal_id, 0.0))
                # Close local DB rows that match this deal_id (if present).
                conn = _db()
                c = conn.cursor()
                c.execute(
                    "SELECT trade_id, COALESCE(parent_session,'') FROM trades "
                    "WHERE chat_id=? AND deal_id=? AND status='OPEN'",
                    (chat_id, deal_id),
                )
                rows = c.fetchall()
                conn.close()
                for trade_id, ps in rows:
                    close_trade_in_db(int(trade_id), actual_pnl=float(upl))
                    after_trade_leg_closed(chat_id, (ps or "").strip(), float(upl))
                closed += 1
            except Exception:
                failed += 1

        await query.edit_message_text(
            t('manage_close_all_result', lang, closed=closed, failed=failed),
            reply_markup=_back_keyboard(lang),
            parse_mode='Markdown',
        )

    elif data == 'manage_view_trade':
        # Ensure this doesn't collide with the report date input flow.
        context.user_data.pop('report_state', None)
        context.user_data['manage_trade_state'] = 'AWAIT_TRADE_ID'
        await query.edit_message_text(
            t('manage_trade_prompt_id', lang),
            reply_markup=_back_keyboard(lang),
            parse_mode='Markdown',
        )

    # ── Open positions (legacy) ────────────────────────────────────────────
    elif data == 'positions_legacy':
        positions = []
        try:
            creds = get_user_credentials(chat_id)
            if not creds:
                await query.edit_message_text(
                    t('no_positions', lang),
                    reply_markup=_back_keyboard(lang), parse_mode='Markdown'
                )
                return

            base_url, headers = get_session(creds)
            if not headers:
                await query.edit_message_text(
                    t('balance_error', lang),
                    reply_markup=_back_keyboard(lang), parse_mode='Markdown'
                )
                return

            # Keep callback responsive; avoid hanging "silent" button presses.
            res = requests.get(f"{base_url}/positions", headers=headers, timeout=15)
            if res.status_code == 200:
                payload = res.json() if res.content else {}
                positions = payload.get('positions', []) or []
        except Exception:
            positions = []

        if not positions:
            await query.edit_message_text(
                t('no_positions', lang),
                reply_markup=_back_keyboard(lang), parse_mode='Markdown'
            )
            return

        lines     = [t('positions_title', lang)]
        btns      = []
        for p in positions:
            sym  = p['market']['instrumentName']
            dir_ = p['position']['direction']
            upl  = round(float(p['position']['upl']), 2)

            conn = _db()
            c    = conn.cursor()
            c.execute(
                "SELECT trailing_stop FROM trades "
                "WHERE chat_id=? AND deal_id=? AND status='OPEN'",
                (chat_id, p['position']['dealId'])
            )
            row  = c.fetchone()
            conn.close()
            stop = f"{row[0]:.4f}" if row and row[0] else '—'

            lines.append(t('position_row', lang, symbol=sym, dir=dir_, upl=upl, stop=stop))
            btns.append([InlineKeyboardButton(
                t('btn_close_pos', lang, symbol=sym),
                callback_data=f"close_{p['position']['dealId']}_{sym}"
            )])

        btns.append([InlineKeyboardButton(t('btn_back', lang), callback_data='main_menu')])
        await query.edit_message_text(
            '\n'.join(lines),
            reply_markup=InlineKeyboardMarkup(btns),
            parse_mode='Markdown',
        )

    # ── Manual close ───────────────────────────────────────────────────────
    elif data.startswith('close_'):
        parts   = data.split('_', 2)
        deal_id = parts[1]
        symbol  = parts[2] if len(parts) > 2 else '?'

        creds    = get_user_credentials(chat_id)
        base_url, headers = get_session(creds)
        ok = False
        if headers:
            del_res = requests.delete(f"{base_url}/positions/{deal_id}", headers=headers)
            ok = del_res.status_code == 200

        msg = t('close_success', lang, symbol=symbol) if ok else t('close_fail', lang, symbol=symbol)
        await query.edit_message_text(
            msg, reply_markup=_back_keyboard(lang), parse_mode='Markdown'
        )

    # ── Toggle trading engine on / off ────────────────────────────────────
    elif data == 'toggle_engine':
        if is_maintenance_mode():
            await query.answer(t('maintenance_start_blocked', lang), show_alert=True)
            return
        if get_risk_state(chat_id) == STATE_USER_DAY_HALT:
            await query.answer(t('use_resume_in_settings', lang), show_alert=True)
            return
        currently_on = get_trading_enabled(chat_id)
        new_state    = not currently_on
        set_trading_enabled(chat_id, new_state)
        if new_state:
            await query.edit_message_text(
                t('trading_engine_started', lang), parse_mode='Markdown'
            )
            # Restart hourly status job so it reflects the new running state
            _schedule_status_job(context, chat_id)
        else:
            await query.edit_message_text(
                t('trading_engine_stopped', lang), parse_mode='Markdown'
            )
        await asyncio.sleep(2)
        await _show_main_menu(update, context, edit=True)

    # ── Toggle mode ────────────────────────────────────────────────────────
    elif data == 'toggle_mode':
        current  = _get_mode(chat_id)
        new_mode = 'HYBRID' if current == 'AUTO' else 'AUTO'
        _set_mode(chat_id, new_mode)
        msg = (
            t('mode_switched_hybrid', lang) if new_mode == 'HYBRID'
            else t('mode_switched_auto', lang)
        )
        await query.edit_message_text(
            msg, reply_markup=_back_keyboard(lang), parse_mode='Markdown'
        )

    # ── License info ───────────────────────────────────────────────────────
    elif data == 'license':
        valid, days = check_license(chat_id)
        conn = _db()
        c    = conn.cursor()
        c.execute("SELECT expiry_date FROM subscribers WHERE chat_id=?", (chat_id,))
        row  = c.fetchone()
        conn.close()
        expiry = row[0] if row and row[0] else '—'

        if days is None:
            await query.edit_message_text(
                t('license_none', lang),
                reply_markup=_back_keyboard(lang), parse_mode='Markdown',
            )
        elif valid:
            await query.edit_message_text(
                t('license_active', lang, expiry=expiry, days=days),
                reply_markup=_back_keyboard(lang), parse_mode='Markdown',
            )
        else:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(t('btn_contact_support', lang), url=SUPPORT_URL)],
                [InlineKeyboardButton(t('btn_back', lang), callback_data='main_menu')],
            ])
            await query.edit_message_text(
                t('license_expired', lang),
                reply_markup=keyboard, parse_mode='Markdown',
            )

    # ── Toggle language ────────────────────────────────────────────────────
    elif data == 'toggle_lang':
        new_lang = 'en' if lang == 'ar' else 'ar'
        _set_lang(chat_id, new_lang)
        await _show_main_menu(update, context, edit=True)

    # ── Full-day halt (Settings, /stop_today, or legacy callback stop_today) ──
    elif data in ('stop_today', 'settings_day_halt'):
        apply_day_halt(chat_id)
        await query.edit_message_text(
            t('day_halt_applied', lang), parse_mode='Markdown'
        )
        await asyncio.sleep(2)
        await _show_main_menu(update, context, edit=True)

    elif data in ('resume_day_halt', 'settings_resume_day_halt'):
        ok = resume_day_halt(chat_id)
        if not ok:
            await query.answer(t('resume_day_noop', lang), show_alert=True)
            return
        await query.edit_message_text(
            t('day_halt_resumed', lang), parse_mode='Markdown'
        )
        _schedule_status_job(context, chat_id)
        await asyncio.sleep(2)
        await _show_main_menu(update, context, edit=True)

    # ── Settings (cancel sub, leverage, broker update) ─────────────────────
    elif data == 'settings':
        await query.edit_message_text(
            t('settings_menu_intro', lang),
            reply_markup=_settings_menu_keyboard(lang, chat_id),
            parse_mode='Markdown',
        )

    elif data == 'settings_signal_profile':
        current = get_user_signal_profile(chat_id)
        await query.edit_message_text(
            t('signal_profile_menu_title', lang, profile=_profile_label(current, lang)),
            reply_markup=_signal_profile_keyboard(lang, chat_id),
            parse_mode='Markdown',
        )

    elif data in ('settings_profile_fast', 'settings_profile_golden'):
        selected = 'GOLDEN' if data == 'settings_profile_golden' else 'FAST'
        set_user_signal_profile(chat_id, selected)
        await query.edit_message_text(
            t('signal_profile_saved', lang, profile=_profile_label(selected, lang)),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(t('btn_back', lang), callback_data='settings'),
            ]]),
            parse_mode='Markdown',
        )

    elif data == 'settings_cancel_warn':
        if not _can_cancel_subscription(chat_id):
            await query.answer(t('cancel_sub_not_available', lang), show_alert=True)
            return
        await query.edit_message_text(
            t('cancel_sub_warning', lang),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    t('btn_cancel_confirm', lang), callback_data='settings_cancel_do')],
                [InlineKeyboardButton(t('btn_back', lang), callback_data='settings')],
            ]),
            parse_mode='Markdown',
        )

    elif data == 'settings_cancel_do':
        if not _can_cancel_subscription(chat_id):
            await query.answer(t('cancel_sub_not_available', lang), show_alert=True)
            return
        apply_subscription_cancellation(chat_id)
        await query.edit_message_text(
            t('cancel_sub_done', lang), parse_mode='Markdown'
        )
        await asyncio.sleep(2.5)
        await _show_main_menu(update, context, edit=True)

    elif data == 'settings_leverage':
        cap  = get_user_max_leverage(chat_id)
        eff  = get_effective_leverage(chat_id)
        await query.edit_message_text(
            t('leverage_settings_body', lang, cap=cap, current=eff)
            + '\n\n' + t('leverage_disclaimer', lang),
            reply_markup=_leverage_choice_keyboard(lang, cap),
            parse_mode='Markdown',
        )

    elif data.startswith('lev_set_'):
        try:
            n = int(data.split('_', 2)[2])
        except (ValueError, IndexError):
            await query.answer()
            return
        cap = get_user_max_leverage(chat_id)
        if n < 1 or n > cap:
            await query.answer(t('leverage_invalid', lang), show_alert=True)
            return
        set_preferred_leverage(chat_id, n)
        await query.edit_message_text(
            t('leverage_saved', lang, leverage=n, cap=cap)
            + '\n\n' + t('leverage_disclaimer', lang),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(t('btn_back', lang), callback_data='settings')]]),
            parse_mode='Markdown',
        )

    elif data == 'settings_broker_warn':
        await query.edit_message_text(
            t('settings_broker_warn_text', lang),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    t('btn_broker_clear_confirm', lang), callback_data='settings_broker_do')],
                [InlineKeyboardButton(t('btn_back', lang), callback_data='settings')],
            ]),
            parse_mode='Markdown',
        )

    elif data == 'settings_broker_do':
        _clear_credentials(chat_id)
        await query.edit_message_text(
            t('settings_broker_cleared', lang), parse_mode='Markdown'
        )

    # ── Hybrid signal: approve ─────────────────────────────────────────────
    elif data.startswith('approve_'):
        signal_id = int(data.split('_')[1])
        _update_signal_status(signal_id, 'APPROVED')
        await query.edit_message_text(
            t('signal_approved', lang), parse_mode='Markdown'
        )

    # ── Hybrid signal: reject ──────────────────────────────────────────────
    elif data.startswith('reject_'):
        signal_id = int(data.split('_')[1])
        _update_signal_status(signal_id, 'REJECTED')
        await query.edit_message_text(
            t('signal_rejected', lang), parse_mode='Markdown'
        )

    # ── Technical support session ──────────────────────────────────────────
    elif data == 'support':
        # Cancel any existing timeout job for this user
        for job in context.job_queue.get_jobs_by_name(f'support_{chat_id}'):
            job.schedule_removal()

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(t('btn_contact_support',   lang), url=SUPPORT_URL),
            InlineKeyboardButton(t('btn_support_resolved',  lang), callback_data='support_resolved'),
        ]])
        await query.edit_message_text(
            t('support_greeting', lang),
            reply_markup=keyboard, parse_mode='Markdown',
        )
        # Auto-close after 3 minutes of no response
        context.job_queue.run_once(
            support_timeout_cb,
            180,
            chat_id = int(chat_id),
            name    = f'support_{chat_id}',
            data    = {'lang': lang},
        )

    elif data == 'support_resolved':
        # User confirmed issue resolved — cancel the timeout and close
        for job in context.job_queue.get_jobs_by_name(f'support_{chat_id}'):
            job.schedule_removal()
        await query.edit_message_text(
            t('support_resolved_msg', lang), parse_mode='Markdown',
        )
        await asyncio.sleep(1.5)
        await _show_main_menu(update, context, edit=True)

    # ── Back to main menu ──────────────────────────────────────────────────
    elif data == 'main_menu':
        await _show_main_menu(update, context, edit=True)

    elif data == 'mode_info':
        pass  # tapping the current-mode label does nothing


# ── Report text-input handler ──────────────────────────────────────────────────

def _get_subscriber_name(chat_id: str) -> str:
    conn = _db()
    c    = conn.cursor()
    c.execute("SELECT first_name, last_name FROM subscribers WHERE chat_id=?", (chat_id,))
    row  = c.fetchone()
    conn.close()
    if row:
        name = f"{row[0] or ''} {row[1] or ''}".strip()
        return name or chat_id
    return chat_id


def _build_daily_report_text(chat_id: str, lang: str, date_str: str) -> str:
    """Build a daily P&L report for a specific date and return it as a Telegram message."""
    conn = _db()
    c    = conn.cursor()
    c.execute(
        "SELECT pnl FROM trades WHERE chat_id=? AND status='CLOSED' AND closed_at LIKE ?",
        (chat_id, f"{date_str}%")
    )
    closed_pnls = [row[0] for row in c.fetchall() if row[0] is not None]
    conn.close()

    title = t('report_title', lang, date=date_str) + '\n\n'

    if not closed_pnls:
        return title + t('report_empty', lang)

    wins   = sum(1 for p in closed_pnls if p > 0)
    losses = sum(1 for p in closed_pnls if p <= 0)
    net    = round(sum(closed_pnls), 2)
    return (
        title +
        t('report_body', lang,
          total=len(closed_pnls),
          wins=wins,
          win_rate=round(wins / len(closed_pnls) * 100, 1),
          losses=losses,
          loss_rate=round(losses / len(closed_pnls) * 100, 1),
          net_pnl=net,
          best=round(max(closed_pnls), 2),
          worst=round(min(closed_pnls), 2))
    )


async def report_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text messages for the report date-collection flow and manage-trade flows."""
    state = context.user_data.get('report_state')
    mstate = context.user_data.get('manage_trade_state')
    if not state and not mstate:
        return  # not in a handled text flow — let other handlers process

    chat_id = str(update.message.chat_id)
    lang    = _get_lang(chat_id)
    text    = (update.message.text or '').strip()

    def _valid_date(s: str) -> bool:
        try:
            datetime.strptime(s, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    # ── Manage trades: view specific trade by id ────────────────────────────
    # Handle this FIRST to avoid collisions with report date flows.
    if mstate == 'AWAIT_TRADE_ID':
        context.user_data.pop('manage_trade_state', None)
        # Also clear report state if it was left dangling.
        context.user_data.pop('report_state', None)
        m = re.search(r"\d+", text)
        if not m:
            await update.message.reply_text(t('manage_trade_not_found', lang), parse_mode='Markdown')
            return
        tid = int(m.group(0))

        conn = _db()
        c = conn.cursor()
        c.execute(
            "SELECT trade_id, symbol, direction, entry_price, size, trailing_stop, "
            "COALESCE(parent_session,'') "
            "FROM trades WHERE chat_id=? AND trade_id=?",
            (chat_id, tid),
        )
        row = c.fetchone()
        if not row:
            conn.close()
            await update.message.reply_text(t('manage_trade_not_found', lang), parse_mode='Markdown')
            return
        parent_session = (row[6] or "").strip()
        if parent_session:
            c.execute(
                "SELECT trade_id, symbol, direction, entry_price, size, trailing_stop, parent_session "
                "FROM trades WHERE chat_id=? AND parent_session=? AND status='OPEN' ORDER BY trade_id ASC",
                (chat_id, parent_session),
            )
            legs = c.fetchall() or []
        else:
            legs = []
        conn.close()

        if legs:
            rows = []
            for lr in legs:
                rows.append({
                    "trade_id": int(lr[0]),
                    "symbol": lr[1],
                    "direction": lr[2],
                    "entry_price": float(lr[3] or 0),
                    "qty": float(lr[4] or 0),
                    "stop": float(lr[5]) if lr[5] is not None else None,
                    "parent_session": (lr[6] or "").strip(),
                })
            card = _group_open_trades(rows)[0]
        else:
            card = {
                "trade_id": int(row[0]),
                "symbol": row[1],
                "direction": row[2],
                "entry_price": float(row[3] or 0),
                "qty": float(row[4] or 0),
                "stop": float(row[5]) if row[5] is not None else None,
            }
        await update.message.reply_text(
            _format_manage_trade_card(card, lang),
            reply_markup=_back_keyboard(lang),
            parse_mode='Markdown',
        )
        return

    # ── Waiting for date of daily report ────────────────────────────────────
    if state == 'AWAIT_DAILY_DATE':
        if not _valid_date(text):
            await update.message.reply_text(
                t('report_invalid_date', lang), parse_mode='Markdown'
            )
            return

        context.user_data.pop('report_state', None)
        loading_msg = await update.message.reply_text(
            t('report_loading', lang), parse_mode='Markdown'
        )

        # On-demand sync so recent broker closes appear
        try:
            from core.executor import get_user_credentials, get_session
            from core.sync import reconcile, backfill_closed_pnls
            creds = get_user_credentials(chat_id)
            if creds:
                base_url, headers = get_session(creds)
                if headers:
                    reconcile(chat_id, base_url, headers)
                    # Use a wider lookback to avoid "all zeros" when requested date
                    # has older closed trades whose PnL was not backfilled before.
                    backfill_closed_pnls(chat_id, base_url, headers, lookback=5000)
        except Exception:
            pass

        report = _build_daily_report_text(chat_id, lang, text)
        await loading_msg.edit_text(
            report,
            reply_markup=_back_keyboard(lang),
            parse_mode='Markdown',
        )

    # ── Waiting for CSV start date ───────────────────────────────────────────
    elif state == 'AWAIT_CSV_START':
        if not _valid_date(text):
            await update.message.reply_text(
                t('report_invalid_date', lang), parse_mode='Markdown'
            )
            return

        context.user_data['report_state']     = 'AWAIT_CSV_END'
        context.user_data['report_csv_start'] = text
        await update.message.reply_text(
            t('report_ask_end_date', lang), parse_mode='Markdown'
        )

    # ── Waiting for CSV end date — generate and send file ───────────────────
    elif state == 'AWAIT_CSV_END':
        if not _valid_date(text):
            await update.message.reply_text(
                t('report_invalid_date', lang), parse_mode='Markdown'
            )
            return

        start_date = context.user_data.pop('report_csv_start', '')
        end_date   = text
        context.user_data.pop('report_state', None)

        # Ensure start <= end
        if start_date > end_date:
            start_date, end_date = end_date, start_date

        loading_msg = await update.message.reply_text(
            t('report_loading', lang), parse_mode='Markdown'
        )

        conn = _db()
        c    = conn.cursor()
        c.execute(
            """SELECT trade_id, symbol, direction, entry_price, size, deal_id,
                      pnl, status, opened_at, closed_at
               FROM trades
               WHERE chat_id=?
                 AND (
                       (closed_at IS NOT NULL AND closed_at BETWEEN ? AND ?)
                    OR (status='OPEN' AND opened_at BETWEEN ? AND ?)
                 )
               ORDER BY COALESCE(closed_at, opened_at)""",
            (chat_id, f"{start_date} 00:00:00", f"{end_date} 23:59:59",
                      f"{start_date} 00:00:00", f"{end_date} 23:59:59")
        )
        rows = c.fetchall()
        conn.close()

        if not rows:
            await loading_msg.edit_text(
                t('report_csv_empty', lang), parse_mode='Markdown'
            )
            return

        # Build CSV in memory
        output   = io.StringIO()
        writer   = csv.writer(output)
        writer.writerow(['Trade ID', 'Symbol', 'Direction', 'Entry Price',
                         'Size', 'Deal ID', 'P&L', 'Status',
                         'Opened At', 'Closed At'])
        for row in rows:
            writer.writerow(row)

        csv_bytes = output.getvalue().encode('utf-8-sig')  # BOM for Excel
        buf       = io.BytesIO(csv_bytes)

        client_name = _get_subscriber_name(chat_id).replace(' ', '_') or chat_id
        filename    = f"{client_name}_{start_date}_to_{end_date}.csv"

        await update.message.reply_document(
            document=buf,
            filename=filename,
            caption=f"Report: {start_date} → {end_date}  |  {len(rows)} trade(s)",
        )
        await loading_msg.edit_text(
            t('report_csv_ready', lang),
            reply_markup=_back_keyboard(lang),
            parse_mode='Markdown',
        )



# ── Circuit Breaker commands ───────────────────────────────────────────────────

async def override_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    lang    = _get_lang(chat_id)
    _, msg  = apply_manual_override(chat_id)
    await update.message.reply_text(msg, parse_mode='Markdown')


async def stop_today_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    apply_day_halt(chat_id)
    lang = _get_lang(chat_id)
    await update.message.reply_text(t('day_halt_applied', lang), parse_mode='Markdown')


# ── Hybrid signal helpers ──────────────────────────────────────────────────────

def _update_signal_status(signal_id: int, status: str):
    conn = _db()
    conn.execute(
        "UPDATE pending_signals SET status=? WHERE signal_id=?",
        (status, signal_id)
    )
    conn.commit()
    conn.close()


def post_pending_signal(chat_id: str, symbol: str, action: str,
                        confidence: float, reason: str,
                        strategy_label: str = '', stop_loss_pct=None) -> int:
    conn = _db()
    c    = conn.cursor()
    c.execute(
        '''INSERT INTO pending_signals
              (chat_id, symbol, action, confidence, reason, strategy_label, stop_loss_pct, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', datetime('now'))''',
        (chat_id, symbol, action, confidence, reason, strategy_label, stop_loss_pct)
    )
    signal_id = c.lastrowid
    conn.commit()
    conn.close()

    lang    = _get_lang(chat_id)
    text    = t('signal_prompt', lang,
                symbol=symbol, action=action,
                confidence=confidence, reason=reason)
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(t('btn_approve', lang), callback_data=f'approve_{signal_id}'),
        InlineKeyboardButton(t('btn_reject',  lang), callback_data=f'reject_{signal_id}'),
    ]])

    token = os.getenv('TELEGRAM_BOT_TOKEN', '')
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id":      chat_id,
                "text":         text,
                "parse_mode":   "Markdown",
                "reply_markup": keyboard.to_dict(),
            },
            timeout=20,
        )
        if resp.ok and resp.json().get("ok"):
            try:
                from database.db_manager import touch_signal_delivered
                touch_signal_delivered(str(chat_id))
            except Exception:
                pass
    except Exception:
        pass
    return signal_id


def get_signal_status(signal_id: int) -> str:
    conn = _db()
    c    = conn.cursor()
    c.execute("SELECT status FROM pending_signals WHERE signal_id=?", (signal_id,))
    row  = c.fetchone()
    conn.close()
    return row[0] if row else 'PENDING'


# ── Support session timeout ────────────────────────────────────────────────────

async def support_timeout_cb(context: ContextTypes.DEFAULT_TYPE):
    job  = context.job
    lang = job.data['lang']
    await context.bot.send_message(
        chat_id    = job.chat_id,
        text       = t('support_timeout', lang),
        parse_mode = 'Markdown',
    )


# ── Bot entry point ────────────────────────────────────────────────────────────

async def _post_init(app):
    await app.bot.set_my_commands([
        BotCommand('start',      'Main dashboard / onboarding'),
        BotCommand('override',   'Manual override after Circuit Breaker'),
        BotCommand('stop_today', 'Block trading for the rest of today (stops engine)'),
        BotCommand('limits',     'Admin: active pending limit orders'),
        BotCommand('orders',     'Admin alias for /limits'),
        BotCommand('monitor',    'Admin: live subscriber monitor'),
        BotCommand('admin',      'Admin controls (/admin ai, /admin status, ...)'),
        BotCommand('audit_sync', 'Admin: DB vs broker position audit'),
    ])


async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    """
    Suppress noisy, harmless Telegram edit collisions:
    "Message is not modified".
    Keep logging all other errors.
    """
    err = getattr(context, "error", None)
    if isinstance(err, BadRequest) and "Message is not modified" in str(err):
        return
    try:
        print(f"[dashboard] Unhandled error: {err}", flush=True)
    except Exception:
        pass


if __name__ == "__main__":
    persistence = PicklePersistence(filepath='conversation_states.pkl')

    app = (
        ApplicationBuilder()
        .token(os.getenv("TELEGRAM_BOT_TOKEN"))
        .persistence(persistence)
        # Make polling more tolerant to temporary network hiccups.
        .request(HTTPXRequest(
            connect_timeout=20.0,
            read_timeout=20.0,
            write_timeout=20.0,
            http_version="1.1",
        ))
        .post_init(_post_init)
        .build()
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            LANG_SELECT:    [CallbackQueryHandler(lang_selected,  pattern='^onboard_lang_')],
            GET_FIRSTNAME:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_firstname)],
            GET_LASTNAME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_lastname)],
            GET_PHONE:      [MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone)],
            AWAIT_BANK_ACK: [
                CallbackQueryHandler(continue_selected,  pattern='^onboard_continue$'),
                CallbackQueryHandler(bank_acked,     pattern='^onboard_paid$'),
            ],
            UPLOAD_PROOF:   [MessageHandler(filters.PHOTO, upload_proof)],
            ENTER_LICENSE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_license)],
            GET_EMAIL:      [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_get_email)],
            GET_PASS:       [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_get_pass)],
            GET_APIKEY:     [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_get_apikey)],
        },
        fallbacks=[CommandHandler('start', start)],
        persistent=True,
        name='onboarding_conv',
    )

    app.add_handler(conv)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, report_input_handler
    ))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(CommandHandler('override',   override_handler))
    app.add_handler(CommandHandler('stop_today', stop_today_handler))
    app.add_handler(CommandHandler('admin',      admin_handler))
    app.add_handler(CommandHandler('limits',     limits_handler))
    app.add_handler(CommandHandler('orders',     limits_handler))
    app.add_handler(CommandHandler('monitor',    monitor_handler))
    app.add_handler(CommandHandler('audit_sync', audit_sync_handler))
    app.add_error_handler(_on_error)

    print("NATB Dashboard Bot running...")
    app.run_polling()
