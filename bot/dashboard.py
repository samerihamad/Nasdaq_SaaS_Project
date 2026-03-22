"""
NATB v2.0 — Telegram Dashboard & Onboarding Bot

New subscriber flow:
  /start -> Language -> First Name -> Last Name -> Tier -> Bank Details
         -> Upload Payment Proof -> (Admin approves) -> Enter License Key
         -> Capital.com Email -> API Password -> API Key
         -> Connected + Balance -> Main Dashboard

Existing subscriber:
  /start -> license check -> Main Dashboard
"""

import os
import asyncio
import sqlite3
import requests
import sys
from datetime import date, timedelta
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

from bot.i18n import t
from bot.licensing import (
    check_license, store_credentials, generate_license_key,
    safe_decrypt, validate_license_key,
)
from bot.notifier import send_telegram_message, notify_admin_payment
from core.risk_manager import (
    apply_manual_override, apply_day_halt, resume_day_halt,
    get_effective_leverage, get_user_max_leverage,
    get_risk_state,
    STATE_CIRCUIT_BREAKER, STATE_HARD_BLOCK, STATE_USER_DAY_HALT,
)
from core.executor import get_user_credentials, get_session
from bot.admin import admin_handler
from database.db_manager import (
    create_db, get_bank_details, get_user_tier,
    get_trading_enabled, set_trading_enabled,
    apply_subscription_cancellation, get_preferred_leverage,
    set_preferred_leverage, infer_subscription_start,
)
from utils.market_hours import (
    get_market_status,
    STATUS_OPEN, STATUS_PRE_MARKET, STATUS_AFTER_HRS,
    market_open_in_uae, market_close_in_uae, next_market_open_in_uae,
    uae_offset_hours,
)

load_dotenv()

_support_username = (os.getenv('SUPPORT_USERNAME') or 'NATBSupport').strip().lstrip('@')
SUPPORT_URL = 'https://t.me/' + _support_username

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
    SELECT_TIER,
    AWAIT_BANK_ACK,
    UPLOAD_PROOF,
    ENTER_LICENSE,
    GET_EMAIL,
    GET_PASS,
    GET_APIKEY,
) = range(10)

TIER_DAYS = {1: 30, 2: 30}   # days of validity per tier on approval


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _db():
    return sqlite3.connect(DB_PATH)


def _get_subscriber(chat_id: str) -> dict:
    conn = _db()
    c    = conn.cursor()
    c.execute(
        """SELECT lang, first_name, last_name, tier, payment_status,
                  payment_proof, license_key, email, expiry_date, mode
           FROM subscribers WHERE chat_id=?""",
        (str(chat_id),)
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return {}
    keys = ['lang', 'first_name', 'last_name', 'tier', 'payment_status',
            'payment_proof', 'license_key', 'email', 'expiry_date', 'mode']
    return dict(zip(keys, row))


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
      HAS_TIER         — chose tier, bank shown, needs to upload proof
      HAS_LAST_NAME    — has both names, needs to pick tier
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

    if sub.get('tier'):
        return 'HAS_TIER'

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


def _tier_keyboard(lang: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t('btn_tier1', lang), callback_data='onboard_tier_1')],
        [InlineKeyboardButton(t('btn_tier2', lang), callback_data='onboard_tier_2')],
    ])


def _paid_keyboard(lang: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(t('btn_i_paid', lang), callback_data='onboard_paid'),
    ]])


def _main_keyboard(lang: str, mode: str, trading: bool, chat_id: str):
    risk = get_risk_state(chat_id)
    mode_btn = (
        t('btn_mode_auto',    lang) if mode == 'AUTO'
        else t('btn_mode_hybrid', lang)
    )
    switch_btn = (
        t('btn_switch_hybrid', lang) if mode == 'AUTO'
        else t('btn_switch_auto', lang)
    )
    # Row 1: while full-day halt is active, only "resume" (restarts engine + clears halt)
    if risk == STATE_USER_DAY_HALT:
        row_engine = [
            InlineKeyboardButton(
                t('btn_resume_day_trading', lang), callback_data='resume_day_halt'
            ),
        ]
    else:
        engine_btn = (
            InlineKeyboardButton(t('btn_engine_stop',  lang), callback_data='toggle_engine')
            if trading else
            InlineKeyboardButton(t('btn_engine_start', lang), callback_data='toggle_engine')
        )
        row_engine = [engine_btn]

    if risk in (STATE_USER_DAY_HALT, STATE_CIRCUIT_BREAKER, STATE_HARD_BLOCK):
        row_halt_settings = [
            InlineKeyboardButton(t('btn_settings', lang), callback_data='settings'),
        ]
    else:
        row_halt_settings = [
            InlineKeyboardButton(t('btn_day_halt', lang), callback_data='stop_today'),
            InlineKeyboardButton(t('btn_settings',     lang), callback_data='settings'),
        ]

    return InlineKeyboardMarkup([
        row_engine,
        [InlineKeyboardButton(t('btn_balance',  lang), callback_data='balance'),
         InlineKeyboardButton(t('btn_report',   lang), callback_data='report')],
        [InlineKeyboardButton(t('btn_positions', lang), callback_data='positions')],
        [InlineKeyboardButton(mode_btn,                callback_data='mode_info'),
         InlineKeyboardButton(switch_btn,              callback_data='toggle_mode')],
        [InlineKeyboardButton(t('btn_license',  lang), callback_data='license'),
         InlineKeyboardButton(t('btn_lang',     lang), callback_data='toggle_lang')],
        [InlineKeyboardButton(t('btn_tier_info', lang), callback_data='tier_info')],
        row_halt_settings,
        [InlineKeyboardButton(t('btn_support',      lang), callback_data='support')],
    ])


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
    return (date.today() - start).days <= 30


def _settings_menu_keyboard(lang: str, chat_id: str):
    rows = []
    if _can_cancel_subscription(chat_id):
        rows.append([InlineKeyboardButton(
            t('btn_cancel_subscription', lang), callback_data='settings_cancel_warn')])
    rows.append([InlineKeyboardButton(
        t('btn_leverage_settings', lang), callback_data='settings_leverage')])
    rows.append([InlineKeyboardButton(
        t('btn_update_broker', lang), callback_data='settings_broker_warn')])
    rows.append([InlineKeyboardButton(t('btn_back', lang), callback_data='main_menu')])
    return InlineKeyboardMarkup(rows)


def _leverage_choice_keyboard(lang: str, tier: int, cap: int):
    if tier <= 1:
        nums = [1, 2]
    else:
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


# ── Main menu display ──────────────────────────────────────────────────────────

def _engine_status_line(chat_id: str, lang: str) -> str:
    """Return the one-line engine status shown at the top of the dashboard."""
    risk = get_risk_state(chat_id)
    if risk == STATE_USER_DAY_HALT:
        return t('engine_status_day_block', lang)
    trading = get_trading_enabled(chat_id)
    if not trading:
        return t('engine_status_off', lang)
    if risk in (STATE_CIRCUIT_BREAKER, STATE_HARD_BLOCK):
        return t('engine_status_halted_day', lang)
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
    markup  = _main_keyboard(lang, mode, trading, chat_id)

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
            t('select_tier_title', lang),
            reply_markup=_tier_keyboard(lang),
            parse_mode='Markdown',
        )
        return SELECT_TIER

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
                t('select_tier_title', lang),
                reply_markup=_tier_keyboard(lang),
                parse_mode='Markdown',
            )
            return SELECT_TIER
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
        _update_subscriber(chat_id, payment_status='NONE', payment_proof=None, tier=0)
        await update.message.reply_text(
            t('payment_rejected_user', lang), parse_mode='Markdown'
        )
        await update.message.reply_text(t('lang_select'), reply_markup=_lang_keyboard())
        return LANG_SELECT

    # ── Has tier, show bank details ────────────────────────────────────────
    if state == 'HAS_TIER':
        await update.message.reply_text(
            _build_bank_text(lang),
            reply_markup=_paid_keyboard(lang),
            parse_mode='Markdown',
        )
        return AWAIT_BANK_ACK

    # ── Has last name, show tier selection ─────────────────────────────────
    if state == 'HAS_LAST_NAME':
        await update.message.reply_text(
            t('select_tier_title', lang),
            reply_markup=_tier_keyboard(lang),
            parse_mode='Markdown',
        )
        return SELECT_TIER

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

    _update_subscriber(chat_id, last_name=last_name)
    sub  = _get_subscriber(chat_id)
    name = sub.get('first_name', '')
    name = name.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')

    await update.message.reply_text(
        t('welcome_name', lang, name=name), parse_mode='Markdown'
    )
    await update.message.reply_text(
        t('select_tier_title', lang),
        reply_markup=_tier_keyboard(lang),
        parse_mode='Markdown',
    )
    return SELECT_TIER


# ── Onboarding: tier selection ─────────────────────────────────────────────────

async def tier_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat_id)
    lang    = _get_lang(chat_id)
    tier    = 1 if query.data == 'onboard_tier_1' else 2

    _update_subscriber(chat_id, tier=tier)

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
    tier    = sub.get('tier', 1)

    # Immediate feedback so the user knows their upload was received
    processing_msg = await update.message.reply_text(
        t('processing', lang), parse_mode='Markdown'
    )

    _update_subscriber(chat_id,
                       payment_proof=file_id,
                       payment_status='PENDING')

    # Notify admin
    full_name = f"{sub.get('first_name', '')} {sub.get('last_name', '')}".strip()
    notify_admin_payment(chat_id, full_name, tier, file_id)

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
    tier = sub.get('tier', 1)
    lang = sub.get('lang', 'ar')
    days = TIER_DAYS.get(tier, 30)

    license_key = generate_license_key()
    expiry = str(date.today() + timedelta(days=days))

    conn = _db()
    conn.execute(
        """UPDATE subscribers
           SET license_key=?, expiry_date=?, payment_status='APPROVED',
               subscription_started_at=?
           WHERE chat_id=?""",
        (license_key, expiry, str(date.today()), user_chat_id)
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

    if data.startswith('onboard_tier_'):
        tier = 1 if data == 'onboard_tier_1' else 2
        _update_subscriber(chat_id, tier=tier)
        await query.edit_message_text(
            ('تم اختيار الباقة. أرسل /start للمتابعة.'
             if lang == 'ar' else
             'Plan saved. Send /start to continue.'),
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
                    res = requests.get(f"{base_url}/accounts", headers=headers)
                    if res.status_code == 200:
                        acc  = res.json()['accounts'][0]
                        bal  = acc['balance']['balance']
                        curr = acc['balance'].get('currency', 'USD')
                        text = t('balance_result', lang, amount=bal, currency=curr)
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

    # ── Performance report ─────────────────────────────────────────────────
    elif data == 'report':
        conn = _db()
        c    = conn.cursor()
        c.execute(
            "SELECT pnl FROM trades WHERE chat_id=? AND status='CLOSED'",
            (chat_id,)
        )
        rows = [r[0] for r in c.fetchall() if r[0] is not None]
        conn.close()

        if not rows:
            await query.edit_message_text(
                t('report_empty', lang),
                reply_markup=_back_keyboard(lang), parse_mode='Markdown'
            )
        else:
            from datetime import datetime
            wins   = sum(1 for p in rows if p > 0)
            losses = len(rows) - wins
            text   = (
                t('report_title', lang, date=datetime.now().strftime('%Y-%m-%d')) + '\n\n' +
                t('report_body', lang,
                  total=len(rows), wins=wins,
                  win_rate=round(wins / len(rows) * 100, 1),
                  losses=losses,
                  loss_rate=round(losses / len(rows) * 100, 1),
                  net_pnl=round(sum(rows), 2),
                  best=round(max(rows), 2),
                  worst=round(min(rows), 2))
            )
            await query.edit_message_text(
                text, reply_markup=_back_keyboard(lang), parse_mode='Markdown'
            )

    # ── Open positions ─────────────────────────────────────────────────────
    elif data == 'positions':
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

        res = requests.get(f"{base_url}/positions", headers=headers)
        if res.status_code != 200 or not res.json().get('positions'):
            await query.edit_message_text(
                t('no_positions', lang),
                reply_markup=_back_keyboard(lang), parse_mode='Markdown'
            )
            return

        positions = res.json()['positions']
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
        if get_risk_state(chat_id) == STATE_USER_DAY_HALT:
            await query.answer(t('use_resume_to_lift_halt', lang), show_alert=True)
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

    # ── Tier info ──────────────────────────────────────────────────────────
    elif data == 'tier_info':
        from core.risk_manager import get_tier_limits
        tier   = get_user_tier(chat_id)
        limits = get_tier_limits(chat_id)
        max_t  = str(limits['max_trades']) if limits['max_trades'] != float('inf') else ('غير محدود' if lang == 'ar' else 'Unlimited')
        max_w  = str(limits['max_watchlist']) if limits['max_watchlist'] != float('inf') else ('غير محدودة' if lang == 'ar' else 'Unlimited')
        label  = (
            (t('btn_tier1', lang) if tier == 1 else t('btn_tier2', lang))
            if tier in (1, 2) else ('—')
        )
        msg = t('tier_info', lang,
                tier_label   = label,
                max_trades   = max_t,
                max_leverage = limits['max_leverage'],
                max_watchlist= max_w)
        await query.edit_message_text(
            msg, reply_markup=_back_keyboard(lang), parse_mode='Markdown'
        )

    # ── Toggle language ────────────────────────────────────────────────────
    elif data == 'toggle_lang':
        new_lang = 'en' if lang == 'ar' else 'ar'
        _set_lang(chat_id, new_lang)
        await _show_main_menu(update, context, edit=True)

    # ── Full-day halt (engine off + USER_DAY_HALT) ──────────────────────────
    elif data == 'stop_today':
        apply_day_halt(chat_id)
        await query.edit_message_text(
            t('day_halt_applied', lang), parse_mode='Markdown'
        )
        await asyncio.sleep(2)
        await _show_main_menu(update, context, edit=True)

    elif data == 'resume_day_halt':
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
        tier = get_user_tier(chat_id)
        cap  = get_user_max_leverage(chat_id)
        eff  = get_effective_leverage(chat_id)
        await query.edit_message_text(
            t('leverage_settings_body', lang, cap=cap, current=eff)
            + '\n\n' + t('leverage_disclaimer', lang),
            reply_markup=_leverage_choice_keyboard(lang, tier, cap),
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
                        confidence: float, reason: str) -> int:
    conn = _db()
    c    = conn.cursor()
    c.execute(
        '''INSERT INTO pending_signals (chat_id, symbol, action, confidence, reason, status, created_at)
           VALUES (?, ?, ?, ?, ?, 'PENDING', datetime('now'))''',
        (chat_id, symbol, action, confidence, reason)
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
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id":      chat_id,
            "text":         text,
            "parse_mode":   "Markdown",
            "reply_markup": keyboard.to_dict(),
        }
    )
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
    ])


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
            SELECT_TIER:    [CallbackQueryHandler(tier_selected,  pattern='^onboard_tier_')],
            AWAIT_BANK_ACK: [CallbackQueryHandler(bank_acked,     pattern='^onboard_paid$')],
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
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(CommandHandler('override',   override_handler))
    app.add_handler(CommandHandler('stop_today', stop_today_handler))
    app.add_handler(CommandHandler('admin',      admin_handler))

    print("NATB Dashboard Bot running...")
    app.run_polling()
