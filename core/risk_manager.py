"""
Risk Management Engine — NATB v2.0

State machine per user per day:
  NORMAL          → trading allowed
  USER_DAY_HALT   → user blocked the rest of the day; engine off; resume from dashboard
  CIRCUIT_BREAKER → 2 consecutive losses; no new entries; Telegram prompt sent
  MANUAL_OVERRIDE → user approved one extra trade after Circuit Breaker
  HARD_BLOCK      → manual override trade was a loss; fully locked until next day

Open positions are NEVER touched during a Circuit Breaker or Hard Block.
They stay active until they hit TP or SL.
"""

import logging
import sqlite3
import math
import requests
import numpy as np
import pandas as pd
from bot.notifier import send_telegram_message
from bot.licensing import safe_decrypt
from utils.market_hours import utc_today
from config import (
    MAX_DAILY_TRADES,
    GLOBAL_MAX_OPEN_TRADES,
    MAX_DAILY_LOSS_PCT,
    CB_LOSS_LIMIT,
    MAX_RISK_PCT_VOLATILE,
    WEEKLY_DRAWDOWN_LIMIT,
    MIN_RR_RATIO,
    MAX_SYMBOL_EXPOSURE_FRACTION,
    MAX_SECTOR_EXPOSURE_FRACTION,
    MAX_TOTAL_EXPOSURE_MULT,
    MAX_TRADES_PER_SYMBOL_DAY,
    MAX_TRADES_PER_DAY_RISK,
    MARGIN_CALL_BUFFER_PCT,
    MAX_STOP_LOSS_PCT,
    ATR_SL_MULT_LOW_VOL,
    ATR_SL_MULT_HIGH_VOL,
    VOL_BAND_MULT,
    LIQUIDITY_SL_BUFFER_ATR,
    SWING_LOOKBACK_BARS,
    FAST_RSI_LIMITS,
    GOLD_RSI_LIMITS,
    FAST_SL_RELAX_CONFIDENCE_THRESHOLD,
    FAST_SL_RELAX_MULTIPLIER,
    GOLD_SL_RELAX_CONFIDENCE_THRESHOLD,
    GOLD_SL_RELAX_MULTIPLIER,
)
from database.db_manager import (
    DB_PATH,
    is_master_kill_switch, get_user_kill_switch, get_user_risk_params,
    get_preferred_leverage, set_trading_enabled, get_user_signal_profile,
)

log = logging.getLogger(__name__)

STATE_NORMAL          = 'NORMAL'
STATE_CIRCUIT_BREAKER = 'CIRCUIT_BREAKER'
STATE_MANUAL_OVERRIDE = 'MANUAL_OVERRIDE'
STATE_HARD_BLOCK      = 'HARD_BLOCK'
STATE_USER_DAY_HALT   = 'USER_DAY_HALT'

CONSECUTIVE_LOSS_LIMIT = int(CB_LOSS_LIMIT)

# Risk scaling: confidence 70% → 1.0% risk, 100% → 2.0% risk  (hard cap at 2%)
MIN_CONF, MAX_CONF = 70.0, 100.0
MIN_RISK, MAX_RISK = 1.0,  2.0
# In VOLATILE regime, effective risk_pct is capped (see config MAX_RISK_PCT_VOLATILE).

# Institutional risk controls (values from config — single source of truth)
DAILY_DRAWDOWN_LIMIT = float(MAX_DAILY_LOSS_PCT)   # % drawdown from session start → hard stop

def get_user_max_leverage(chat_id: str) -> int:
    """Return the maximum leverage allowed (single-plan system)."""
    return 10


def get_effective_leverage(chat_id: str) -> int:
    """
    User-chosen leverage capped by global max (1–10). If unset, uses the max.

    Used with max leverage to form (effective / cap): scales risk budget for
    position sizing — not a multiplier on size after risk is computed.
    """
    cap = get_user_max_leverage(chat_id)
    pref = get_preferred_leverage(chat_id)
    if pref is None:
        return cap
    return max(1, min(int(pref), cap))


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn():
    return sqlite3.connect(DB_PATH)


def _get_or_reset_state(cursor, chat_id):
    """
    Return today's state row for chat_id.
    Automatically resets to NORMAL at the start of each new trading day.
    """
    today = str(utc_today())
    cursor.execute(
        "SELECT date, consecutive_losses, state FROM daily_risk_state WHERE chat_id=?",
        (chat_id,)
    )
    row = cursor.fetchone()

    if row is None or row[0] != today:
        cursor.execute(
            '''INSERT OR REPLACE INTO daily_risk_state
               (chat_id, date, consecutive_losses, state)
               VALUES (?, ?, 0, ?)''',
            (chat_id, today, STATE_NORMAL)
        )
        return {'date': today, 'consecutive_losses': 0, 'state': STATE_NORMAL}

    return {'date': row[0], 'consecutive_losses': row[1], 'state': row[2]}


# ── Public read API ───────────────────────────────────────────────────────────

def get_risk_state(chat_id):
    """Return the current state string for chat_id (resets daily)."""
    db = _conn()
    c  = db.cursor()
    state = _get_or_reset_state(c, chat_id)['state']
    db.commit()
    db.close()
    return state


def _get_daily_trade_count(chat_id: str) -> int:
    """Count trades opened today for this user."""
    today = str(utc_today())
    conn  = sqlite3.connect(DB_PATH)
    c     = conn.cursor()
    c.execute(
        "SELECT COUNT(*) FROM trades WHERE chat_id=? AND DATE(opened_at)=?",
        (chat_id, today),
    )
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0


def _get_global_open_trades_count() -> int:
    """Count currently open positions across all users."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'")
    row = c.fetchone()
    conn.close()
    return int(row[0] or 0) if row else 0


def _get_live_balance(chat_id: str) -> float | None:
    """
    Fetch live account equity/balance from Capital.com for daily loss guard.
    Returns None when balance cannot be resolved.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT api_key, api_password, is_demo, email FROM subscribers WHERE chat_id=?",
            (str(chat_id),),
        )
        row = c.fetchone()
        conn.close()
        if not row:
            return None

        api_key = safe_decrypt(row[0] or "")
        password = safe_decrypt(row[1] or "")
        is_demo = bool(row[2])
        email = safe_decrypt(row[3] or "")
        if not api_key or not password or not email:
            return None

        base_url = (
            "https://demo-api-capital.backend-capital.com/api/v1"
            if is_demo else
            "https://api-capital.backend-capital.com/api/v1"
        )
        headers = {
            "X-CAP-API-KEY": str(api_key).strip(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        auth = requests.post(
            f"{base_url}/session",
            json={"identifier": str(email).strip(), "password": str(password).strip()},
            headers=headers,
            timeout=20,
        )
        if auth.status_code != 200:
            return None

        session_headers = {
            **headers,
            "CST": auth.headers.get("CST"),
            "X-SECURITY-TOKEN": auth.headers.get("X-SECURITY-TOKEN"),
        }
        acc_res = requests.get(f"{base_url}/accounts", headers=session_headers, timeout=20)
        if acc_res.status_code != 200:
            return None
        accounts = (acc_res.json() or {}).get("accounts", []) or []
        if not accounts:
            return None
        acc = accounts[0] or {}
        bal = acc.get("balance", {}) or {}
        equity = bal.get("equity", acc.get("equity"))
        if isinstance(equity, (int, float)) and equity > 0:
            return float(equity)
        balance = bal.get("balance", acc.get("balance"))
        if isinstance(balance, (int, float)) and balance > 0:
            return float(balance)
        return None
    except Exception:
        return None


def can_open_trade(chat_id):
    """
    Returns (allowed: bool, reason: str).

    Gate order (checked in priority):
      1. Master kill switch       — admin halted ALL trading globally
      2. User kill switch         — this specific user is halted
      3. Circuit Breaker / Hard Block state machine
    """
    # ── 1. Master kill switch ─────────────────────────────────────────────────
    if is_master_kill_switch():
        return False, "Master kill switch active — all trading halted by admin"

    # ── 2. Per-user kill switch ───────────────────────────────────────────────
    if get_user_kill_switch(chat_id):
        return False, "Your trading session has been halted by the admin"

    # ── 3. Circuit Breaker / Hard Block ───────────────────────────────────────
    state = get_risk_state(chat_id)
    if state == STATE_USER_DAY_HALT:
        return False, "Full-day halt active — resume from dashboard to trade again"
    if state == STATE_CIRCUIT_BREAKER:
        return False, "Circuit Breaker active — awaiting your Telegram approval"
    if state == STATE_HARD_BLOCK:
        return False, "Hard Block active — lifts automatically at next trading day open"
    if state not in (STATE_NORMAL, STATE_MANUAL_OVERRIDE):
        return False, "Unknown risk state"

    # ── 4. Per-user max daily trade count ─────────────────────────────────────
    trades_today = _get_daily_trade_count(str(chat_id))
    if trades_today >= int(MAX_DAILY_TRADES):
        return False, f"Max daily trades reached ({trades_today}/{int(MAX_DAILY_TRADES)})"

    # ── 5. Global max open positions ──────────────────────────────────────────
    open_positions = _get_global_open_trades_count()
    if open_positions >= int(GLOBAL_MAX_OPEN_TRADES):
        return False, (
            f"Global max open trades reached "
            f"({open_positions}/{int(GLOBAL_MAX_OPEN_TRADES)})"
        )

    # ── 6. Daily realized loss % guard ────────────────────────────────────────
    live_balance = _get_live_balance(str(chat_id))
    if live_balance and live_balance > 0:
        daily_pnl = get_daily_pnl(str(chat_id))
        daily_loss_pct = abs(float(daily_pnl)) / float(live_balance) * 100 if daily_pnl < 0 else 0.0
        if daily_loss_pct >= float(MAX_DAILY_LOSS_PCT):
            return False, (
                f"Max daily loss reached ({daily_loss_pct:.2f}% / "
                f"{float(MAX_DAILY_LOSS_PCT):.2f}%)"
            )

    return True, state


# ── Trade outcome recording ───────────────────────────────────────────────────

def record_trade_result(chat_id, pnl: float, *, outcome_hint: str | None = None):
    """
    Call when a *risk outcome* is final for the user.

    Fast path: `mark_trade_closed_pending(..., apply_fast_risk=True)` may invoke
    `after_trade_leg_closed` with provisional UPL before broker history sync; the
    P&L sync worker skips duplicate risk updates when `risk_outcome_recorded` is set.

    For split TP1/TP2 positions sharing `parent_session`, call this **once** with
    the session total P&L (after all legs are closed), not per leg — see
    `trade_session_finalize.after_trade_leg_closed`.

    outcome_hint: when PnL is unknown or stale (e.g. SL close before broker rpl),
    pass 'loss' or 'win' to drive the circuit-breaker counter without waiting for sync.

    Transitions:
      NORMAL          + 2nd consecutive loss  → CIRCUIT_BREAKER (sends Telegram prompt)
      MANUAL_OVERRIDE + loss                  → HARD_BLOCK
      MANUAL_OVERRIDE + win                   → NORMAL
      Any state       + win                   → resets consecutive_losses to 0
    """
    db = _conn()
    c  = db.cursor()
    data = _get_or_reset_state(c, chat_id)
    prev_state   = data['state']
    consecutive  = data['consecutive_losses']

    oh = (outcome_hint or "").strip().lower()
    if oh == "loss":
        is_loss = True
    elif oh == "win":
        is_loss = False
    else:
        is_loss = pnl < 0
    consecutive = (consecutive + 1) if is_loss else 0

    # Determine new state
    if prev_state == STATE_MANUAL_OVERRIDE:
        new_state = STATE_HARD_BLOCK if is_loss else STATE_NORMAL
    elif prev_state == STATE_USER_DAY_HALT:
        # Stay in user day-halt until they resume from the dashboard
        new_state = STATE_USER_DAY_HALT
    elif consecutive >= CONSECUTIVE_LOSS_LIMIT and prev_state == STATE_NORMAL:
        new_state = STATE_CIRCUIT_BREAKER
    else:
        new_state = prev_state

    c.execute(
        "UPDATE daily_risk_state SET consecutive_losses=?, state=? WHERE chat_id=?",
        (consecutive, new_state, chat_id)
    )
    db.commit()
    db.close()

    # Notify on state transitions
    if new_state == STATE_CIRCUIT_BREAKER and prev_state != STATE_CIRCUIT_BREAKER:
        _send_circuit_breaker_prompt(chat_id)
    elif new_state == STATE_HARD_BLOCK:
        send_telegram_message(
            chat_id,
            "🔒 *قفل كامل حتى الغد*\n"
            "خسرت الصفقة اليدوية بعد تفعيل قاطع الدارة.\n"
            "لن تُفتح صفقات جديدة حتى بداية يوم التداول القادم.\n"
            "الصفقات المفتوحة تبقى نشطة حتى TP / SL."
        )

    return new_state


# ── Telegram prompts ──────────────────────────────────────────────────────────

def _send_circuit_breaker_prompt(chat_id):
    send_telegram_message(
        chat_id,
        "⚡ *قاطع الدارة — Circuit Breaker*\n\n"
        "تم رصد خسارتين متتاليتين اليوم.\n"
        "🚫 تم إيقاف فتح صفقات جديدة تلقائياً.\n\n"
        "✅ الصفقات المفتوحة تبقى نشطة حتى TP أو SL.\n\n"
        "الخيارات المتاحة:\n"
        "• /override — السماح بصفقة يدوية واحدة ⚠️\n"
        "• /stop\\_today — إيقاف التداول حتى الغد 🛑"
    )


# ── User commands ─────────────────────────────────────────────────────────────

def apply_manual_override(chat_id):
    """Process /override command. Returns (success: bool, message: str)."""
    db = _conn()
    c  = db.cursor()
    data = _get_or_reset_state(c, chat_id)
    if data['state'] != STATE_CIRCUIT_BREAKER:
        db.close()
        return False, "لا يوجد قاطع دارة نشط حالياً."

    c.execute(
        "UPDATE daily_risk_state SET state=? WHERE chat_id=?",
        (STATE_MANUAL_OVERRIDE, chat_id)
    )
    db.commit()
    db.close()

    send_telegram_message(
        chat_id,
        "⚠️ *تجاوز يدوي مفعّل — صفقة واحدة فقط*\n"
        "تم السماح بصفقة إضافية واحدة.\n"
        "⚠️ إذا خسرت هذه الصفقة ← قفل كامل حتى الغد."
    )
    return True, "تم تفعيل التجاوز اليدوي."


def apply_day_halt(chat_id: str):
    """
    User-requested full-day block: no new entries, trading engine off.
    Distinct from automatic CIRCUIT_BREAKER (two losses).
    """
    db = _conn()
    c = db.cursor()
    _get_or_reset_state(c, chat_id)
    c.execute(
        "UPDATE daily_risk_state SET state=? WHERE chat_id=?",
        (STATE_USER_DAY_HALT, chat_id),
    )
    db.commit()
    db.close()
    set_trading_enabled(chat_id, False)


def resume_day_halt(chat_id: str) -> bool:
    """Clear USER_DAY_HALT and turn the trading engine back on."""
    db = _conn()
    c = db.cursor()
    data = _get_or_reset_state(c, chat_id)
    if data['state'] != STATE_USER_DAY_HALT:
        db.close()
        return False
    c.execute(
        "UPDATE daily_risk_state SET consecutive_losses=0, state=? WHERE chat_id=?",
        (STATE_NORMAL, chat_id),
    )
    db.commit()
    db.close()
    set_trading_enabled(chat_id, True)
    return True


def apply_stop_today(chat_id):
    """Alias: /stop_today — same as full-day block (engine off + USER_DAY_HALT)."""
    apply_day_halt(chat_id)


# ── Position sizing ───────────────────────────────────────────────────────────

def calculate_position_size(balance: float, confidence: float,
                             entry_price: float, stop_loss_pct: float = 0.01,
                             chat_id: str = None,
                             regime: str | None = None):
    """
    Per-user dynamic risk sizing. Tunables come from ``config`` (imported names).

    Regime handling (Sizing Shield):
      If ``regime`` normalizes to ``VOLATILE``, ``risk_pct`` is capped by
      ``MAX_RISK_PCT_VOLATILE`` from config before computing dollar risk.

    Convex confidence mapping → [user_min, user_max] risk %, then leverage scaling.

    Size = risk_budget / (entry_price * stop_loss_pct). Broker min is typically 1 unit;
    if the formula yields < 1.0, we clamp to 1.0 and emit a **critical** log because
    effective exposure exceeds the intended risk budget at that stop distance.
    """
    if chat_id:
        user_min, user_max = get_user_risk_params(chat_id)
    else:
        user_min, user_max = MIN_RISK, MAX_RISK

    clamped = max(MIN_CONF, min(MAX_CONF, float(confidence)))
    conf_norm = (clamped - MIN_CONF) / (MAX_CONF - MIN_CONF)
    conf_weight = conf_norm ** 1.8
    risk_pct = float(user_min) + (float(user_max) - float(user_min)) * conf_weight

    reg = str(regime or "").strip().upper()
    if reg == "VOLATILE":
        risk_pct = min(risk_pct, float(MAX_RISK_PCT_VOLATILE))

    risk_budget = float(balance) * (risk_pct / 100.0)
    if chat_id:
        cap = get_user_max_leverage(chat_id)
        lev = get_effective_leverage(chat_id)
        if cap > 0:
            risk_budget *= float(lev) / float(cap)

    ep = float(entry_price)
    slp = float(stop_loss_pct)
    if ep <= 0 or slp <= 0 or not math.isfinite(risk_budget):
        return 1.0

    denom = ep * slp
    raw_size = risk_budget / denom
    rounded = round(float(raw_size), 2)

    if rounded < 1.0:
        log.critical(
            "POSITION_SIZE_RISK_CLAMP: raw_computed_size=%.8f < 1.0 — clamped to 1.0; "
            "intended_risk_budget=%.6f implies risk above configured cap at this stop "
            "(balance=%.6f risk_pct=%.6f regime=%r chat_id=%s entry=%.8f stop_loss_pct=%.8f)",
            float(raw_size),
            float(risk_budget),
            float(balance),
            float(risk_pct),
            regime,
            str(chat_id or ""),
            ep,
            slp,
        )
        return 1.0

    return float(rounded)


# ── Daily drawdown guard ──────────────────────────────────────────────────────

def get_daily_pnl(chat_id: str) -> float:
    """Sum of today's closed P&L in dollar terms."""
    try:
        today = str(utc_today())
        conn  = sqlite3.connect(DB_PATH)
        c     = conn.cursor()
        c.execute(
            "SELECT SUM(pnl) FROM trades "
            "WHERE chat_id=? AND DATE(closed_at)=? AND status='CLOSED'",
            (chat_id, today),
        )
        row = c.fetchone()
        conn.close()
        return float(row[0] or 0.0)
    except Exception:
        return 0.0


def check_daily_drawdown(chat_id: str, current_balance: float) -> tuple:
    """
    Returns (within_limit: bool, drawdown_pct: float).

    Computes today's total closed P&L as a % of current balance.
    If drawdown exceeds DAILY_DRAWDOWN_LIMIT (-5%), triggers CIRCUIT_BREAKER
    and returns False.
    """
    if current_balance <= 0:
        return True, 0.0

    daily_pnl     = get_daily_pnl(chat_id)
    drawdown_pct  = daily_pnl / current_balance * 100   # negative = loss

    if drawdown_pct <= -DAILY_DRAWDOWN_LIMIT:
        # Force circuit breaker state
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "UPDATE daily_risk_state SET state=? WHERE chat_id=?",
                (STATE_CIRCUIT_BREAKER, chat_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

        send_telegram_message(
            chat_id,
            f"🔴 *Daily Drawdown Limit Hit*\n"
            f"Daily P&L: ${daily_pnl:.2f} ({drawdown_pct:.1f}%)\n"
            f"Limit: -{DAILY_DRAWDOWN_LIMIT}%\n"
            f"All new entries blocked until tomorrow.\n"
            f"Open positions remain active until TP / SL."
        )
        return False, round(drawdown_pct, 2)

    return True, round(drawdown_pct, 2)


# ── Risk:Reward enforcement ───────────────────────────────────────────────────

def check_rr_ratio(entry: float, stop: float, direction: str,
                    atr: float, min_rr: float = MIN_RR_RATIO) -> tuple:
    """
    Returns (passes: bool, rr_ratio: float, reason: str).

    Computes the natural RR of the setup:
      stop_distance = |entry - stop|
      target        = entry ± (stop_distance × min_rr)

    The target is considered achievable if it falls within 4 × ATR
    (a move the market can realistically make in 1–3 sessions).
    A setup that cannot offer 1:2 is discarded before any order is sent.
    """
    eps = 1e-9
    stop_dist = abs(entry - stop)
    if stop_dist <= eps or atr <= eps:
        return False, 0.0, "invalid_stop_or_atr"

    target_dist = stop_dist * min_rr
    rr_ratio    = round(target_dist / stop_dist, 2)

    # Target must be within 4 × ATR — beyond this it's unrealistic.
    # Add tiny epsilon to avoid float-boundary false negatives.
    achievable = target_dist <= (atr * 4 + eps)
    rr_ok = rr_ratio + eps >= min_rr

    if not rr_ok:
        return False, rr_ratio, "rr_below_minimum"
    if not achievable:
        return False, rr_ratio, "target_beyond_atr_limit"
    return True, rr_ratio, "ok"


def _daily_symbol_trade_count(chat_id: str, symbol: str) -> int:
    try:
        today = str(utc_today())
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM trades WHERE chat_id=? AND symbol=? AND DATE(opened_at)=?",
            (str(chat_id), str(symbol), today),
        )
        row = c.fetchone()
        conn.close()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _weekly_pnl(chat_id: str) -> float:
    """
    Last 7 calendar days closed PnL.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM trades "
            "WHERE chat_id=? AND status='CLOSED' "
            "AND DATE(closed_at) >= DATE('now', '-6 day')",
            (str(chat_id),),
        )
        row = c.fetchone()
        conn.close()
        return float(row[0] or 0.0) if row else 0.0
    except Exception:
        return 0.0


def _sector_for_symbol(symbol: str) -> str:
    s = str(symbol or "").upper().strip()
    groups = {
        "TECH": {"AAPL", "MSFT", "NVDA", "AMD", "AVGO", "QCOM", "ORCL", "INTC", "SMCI", "ARM", "META", "GOOGL", "NFLX"},
        "FIN": {"JPM", "BAC", "GS", "MS", "V", "MA", "XLF"},
        "ENERGY": {"XOM", "CVX", "USO"},
        "HEALTH": {"JNJ", "PFE"},
        "CONSUMER": {"AMZN", "WMT", "COST", "HD", "RBLX", "UBER"},
        "CRYPTO_BETA": {"COIN", "MSTR"},
        "INDEX": {"SPY", "QQQ", "IWM", "XLK"},
        "METALS": {"GLD", "SLV"},
    }
    for sector, names in groups.items():
        if s in names:
            return sector
    return "OTHER"


def generate_institutional_stop_loss(
    *,
    direction: str,
    entry_price: float,
    df_15m,
    liquidity_levels: dict | None = None,
    atr_value: float | None = None,
    atr_mult_override: float | None = None,
    min_stop_distance: float | None = None,
    max_stop_distance: float | None = None,
) -> tuple[float, str, dict]:
    """
    Compute institutional SL candidates and choose the safest one:
      - ATR-based
      - volatility-band based (stdev)
      - structure-based (swing)
      - liquidity-based (nearest pool beyond price)
    Enforces min/max distance and returns final stop + reason + details.
    """
    entry = float(entry_price)
    if entry <= 0:
        return entry, "invalid_entry", {}

    atr = float(atr_value or 0.0)
    high_vol = bool(atr > 0 and (atr / entry) >= 0.02)
    atr_mult = ATR_SL_MULT_HIGH_VOL if high_vol else ATR_SL_MULT_LOW_VOL
    if atr_mult_override is not None:
        try:
            ov = float(atr_mult_override)
            if math.isfinite(ov) and ov > 0:
                atr_mult = ov
        except Exception:
            pass
    atr_dist = atr * atr_mult if atr > 0 else 0.0
    if atr_dist <= 0:
        atr_dist = entry * 0.01

    if direction == "BUY":
        stop_atr = entry - atr_dist
    else:
        stop_atr = entry + atr_dist

    # Volatility band candidate from rolling return std.
    stop_vol = stop_atr
    try:
        close = df_15m["Close"].astype(float)
        vol = float(close.pct_change().rolling(14).std().iloc[-1])
        if math.isfinite(vol) and vol > 0:
            vol_dist = max(entry * vol * VOL_BAND_MULT, entry * 0.0035)
            stop_vol = (entry - vol_dist) if direction == "BUY" else (entry + vol_dist)
    except Exception:
        pass

    # Structure candidate: previous swing low/high
    stop_structure = stop_atr
    try:
        h = df_15m["High"].astype(float)
        l = df_15m["Low"].astype(float)
        look = max(5, int(SWING_LOOKBACK_BARS))
        if direction == "BUY":
            swing = float(l.tail(look).min())
            buf = max(entry * 0.0015, atr * 0.15 if atr > 0 else entry * 0.0015)
            stop_structure = swing - buf
        else:
            swing = float(h.tail(look).max())
            buf = max(entry * 0.0015, atr * 0.15 if atr > 0 else entry * 0.0015)
            stop_structure = swing + buf
    except Exception:
        pass

    # Liquidity candidate: beyond nearest liquidity pool with ATR buffer.
    stop_liquidity = stop_atr
    if isinstance(liquidity_levels, dict):
        lvls = []
        for k in ("pdh", "pdl", "orh", "orl"):
            try:
                v = liquidity_levels.get(k)
                if v is not None:
                    fv = float(v)
                    if math.isfinite(fv):
                        lvls.append(fv)
            except Exception:
                continue
        if lvls:
            if direction == "BUY":
                downside = [x for x in lvls if x < entry]
                if downside:
                    nearest = max(downside)
                    stop_liquidity = nearest - max(entry * 0.001, atr * LIQUIDITY_SL_BUFFER_ATR if atr > 0 else entry * 0.001)
            else:
                upside = [x for x in lvls if x > entry]
                if upside:
                    nearest = min(upside)
                    stop_liquidity = nearest + max(entry * 0.001, atr * LIQUIDITY_SL_BUFFER_ATR if atr > 0 else entry * 0.001)

    candidates = {
        "atr": float(stop_atr),
        "volatility_band": float(stop_vol),
        "structure_swing": float(stop_structure),
        "liquidity": float(stop_liquidity),
    }
    # "Safest": most protective against stop hunts (furthest valid stop).
    if direction == "BUY":
        selected_reason, selected_stop = min(candidates.items(), key=lambda x: x[1])
    else:
        selected_reason, selected_stop = max(candidates.items(), key=lambda x: x[1])

    raw_dist = abs(entry - selected_stop)
    # Enforce distance floors/ceilings.
    if min_stop_distance is not None and min_stop_distance > 0 and raw_dist < float(min_stop_distance):
        selected_stop = (entry - float(min_stop_distance)) if direction == "BUY" else (entry + float(min_stop_distance))
        selected_reason = f"{selected_reason}+min_distance"
    if max_stop_distance is not None and max_stop_distance > 0:
        if abs(entry - selected_stop) > float(max_stop_distance):
            selected_stop = (entry - float(max_stop_distance)) if direction == "BUY" else (entry + float(max_stop_distance))
            selected_reason = f"{selected_reason}+max_distance"

    # Risk hard cap by pct (institutional sanity).
    cap_dist = entry * float(MAX_STOP_LOSS_PCT)
    if abs(entry - selected_stop) > cap_dist:
        selected_stop = (entry - cap_dist) if direction == "BUY" else (entry + cap_dist)
        selected_reason = f"{selected_reason}+risk_cap"

    return float(selected_stop), selected_reason, {
        "selected_reason": selected_reason,
        "high_volatility": int(high_vol),
        "candidates": {k: round(v, 6) for k, v in candidates.items()},
    }


def compute_last_rsi(close_series, period: int = 14) -> float | None:
    """Last RSI value from a Close series (matches strategy_meanrev RSI math)."""
    try:
        s = pd.Series(close_series).squeeze()
        if s is None or len(s) < period + 1:
            return None
        delta = s.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        v = float(rsi.iloc[-1])
        return v if math.isfinite(v) else None
    except Exception:
        return None


def _subscription_tier_for_chat(chat_id: str | None) -> str:
    """FAST or GOLDEN (Gold) — from subscribers.signal_profile."""
    if not chat_id:
        return "FAST"
    try:
        return "GOLDEN" if get_user_signal_profile(str(chat_id)) == "GOLDEN" else "FAST"
    except Exception:
        return "FAST"


def _is_mean_reversion_strategy(strategy_label: str | None) -> bool:
    s = (strategy_label or "").strip().lower()
    return (
        "meanrev" in s
        or "meanreversion" in s
        or "mean_rev" in s
        or s in ("mean reversion", "meanrev")
    )


def _execution_policy_labels(
    tier: str,
    *,
    sl_relax_active: bool,
    relax_pct_label: int | None,
) -> tuple[str, str]:
    if tier == "GOLDEN":
        base_en = "⚙️ *Gold Discipline Applied*"
        base_ar = "⚙️ *معيار الذهب — Gold Discipline Applied*"
    else:
        base_en = "⚙️ *Fast Entry Executed*"
        base_ar = "⚙️ *تنفيذ Fast — Fast Entry Executed*"
    if sl_relax_active and relax_pct_label:
        return (
            f"{base_en} (+{relax_pct_label}% max risk cap)",
            f"{base_ar} (+{relax_pct_label}% سقف مخاطرة)",
        )
    return base_en, base_ar


def validate_pre_trade(
    symbol,
    entry_price,
    stop_loss,
    leverage,
    account_balance,
    free_margin,
    *,
    confidence: float = 75.0,
    chat_id: str | None = None,
    current_symbol_exposure: float = 0.0,
    current_total_exposure: float = 0.0,
    exposure_by_symbol: dict | None = None,
    action: str | None = None,
    strategy_label: str | None = None,
    rsi_15m: float | None = None,
    regime: str | None = None,
) -> tuple[bool, str, dict]:
    """
    Mandatory pre-trade validation.

    Tier (Fast vs Gold) affects:
      - Mean Reversion RSI limits at execution (FAST_RSI_LIMITS vs GOLD_RSI_LIMITS)
      - Max stop-loss % relaxation (Fast: >80% conf → +15%; Gold: >90% conf → +10%)

    Returns:
      (approved, reason, details)
    """
    try:
        entry = float(entry_price)
        stop = float(stop_loss)
        lev = float(leverage)
        bal = float(account_balance)
        margin_free = float(free_margin)
    except Exception:
        return False, "Trade rejected: invalid numeric pre-trade inputs", {}

    if entry <= 0 or not (entry == entry):
        return False, "Trade rejected: invalid entry price", {}
    if lev <= 0 or not (lev == lev):
        return False, "Trade rejected: invalid leverage", {}
    if bal <= 0 or not (bal == bal):
        return False, "Trade rejected: invalid account balance", {}
    if margin_free < 0 or not (margin_free == margin_free):
        return False, "Trade rejected: invalid free margin", {}

    stop_dist = abs(entry - stop)
    if stop_dist <= 0:
        return False, "Trade rejected: invalid stop-loss distance", {}

    stop_loss_pct = stop_dist / entry
    if stop_loss_pct <= 0 or stop_loss_pct >= 0.5:
        return False, "Trade rejected: invalid stop-loss ratio", {}

    conf_val = float(confidence)
    tier = _subscription_tier_for_chat(chat_id)

    # Tier RSI gate — Mean Reversion only (re-check at execution for Gold vs Fast).
    if (
        chat_id
        and _is_mean_reversion_strategy(strategy_label)
        and rsi_15m is not None
        and action in ("BUY", "SELL", "buy", "sell")
    ):
        os_lim, ob_lim = (GOLD_RSI_LIMITS if tier == "GOLDEN" else FAST_RSI_LIMITS)
        rv = float(rsi_15m)
        au = str(action).upper()
        if au == "BUY" and rv > float(os_lim):
            return False, "Trade rejected: RSI outside tier limits (Mean Reversion)", {
                "rsi_15m": round(rv, 4),
                "subscription_tier": tier,
                "rsi_oversold_max": float(os_lim),
                "rsi_overbought_min": float(ob_lim),
                "action": au,
            }
        if au == "SELL" and rv < float(ob_lim):
            return False, "Trade rejected: RSI outside tier limits (Mean Reversion)", {
                "rsi_15m": round(rv, 4),
                "subscription_tier": tier,
                "rsi_oversold_max": float(os_lim),
                "rsi_overbought_min": float(ob_lim),
                "action": au,
            }

    max_stop_loss_pct_allowed = float(MAX_STOP_LOSS_PCT)
    sl_relax_active = False
    relax_pct_label: int | None = None
    if tier == "GOLDEN":
        if conf_val > float(GOLD_SL_RELAX_CONFIDENCE_THRESHOLD):
            max_stop_loss_pct_allowed = float(MAX_STOP_LOSS_PCT) * float(GOLD_SL_RELAX_MULTIPLIER)
            sl_relax_active = True
            relax_pct_label = 10
    else:
        if conf_val > float(FAST_SL_RELAX_CONFIDENCE_THRESHOLD):
            max_stop_loss_pct_allowed = float(MAX_STOP_LOSS_PCT) * float(FAST_SL_RELAX_MULTIPLIER)
            sl_relax_active = True
            relax_pct_label = 15

    if (stop_loss_pct - max_stop_loss_pct_allowed) > 1e-9:
        return False, "Trade rejected: stop-loss exceeds max risk distance", {
            "stop_loss_pct": round(stop_loss_pct, 8),
            "max_stop_loss_pct": round(max_stop_loss_pct_allowed, 8),
            "base_max_stop_loss_pct": float(MAX_STOP_LOSS_PCT),
            "confidence": round(conf_val, 2),
            "subscription_tier": tier,
        }

    # Trade count constraints
    if chat_id:
        trades_today = _get_daily_trade_count(str(chat_id))
        if trades_today >= int(MAX_TRADES_PER_DAY_RISK):
            return False, "Trade rejected: max trades per day exceeded", {
                "max_trades_per_day": int(MAX_TRADES_PER_DAY_RISK),
                "trades_today": int(trades_today),
            }
        sym_trades_today = _daily_symbol_trade_count(str(chat_id), str(symbol))
        if sym_trades_today >= int(MAX_TRADES_PER_SYMBOL_DAY):
            return False, "Trade rejected: max trades per symbol exceeded", {
                "max_trades_per_symbol_day": int(MAX_TRADES_PER_SYMBOL_DAY),
                "symbol_trades_today": int(sym_trades_today),
            }

        weekly_pnl = _weekly_pnl(str(chat_id))
        weekly_dd_pct = (weekly_pnl / bal) * 100.0 if bal > 0 else 0.0
        if weekly_dd_pct <= -float(WEEKLY_DRAWDOWN_LIMIT):
            return False, "Trade rejected: max weekly loss exceeded", {
                "weekly_pnl": round(weekly_pnl, 4),
                "weekly_drawdown_pct": round(weekly_dd_pct, 4),
                "weekly_limit_pct": float(WEEKLY_DRAWDOWN_LIMIT),
            }

    proposed_size = float(
        calculate_position_size(
            balance=bal,
            confidence=float(confidence),
            entry_price=entry,
            stop_loss_pct=stop_loss_pct,
            chat_id=chat_id,
            regime=regime,
        )
    )
    if proposed_size < 1.0 or not (proposed_size == proposed_size):
        return False, "Trade rejected: invalid position size", {}

    required_margin = (entry * proposed_size) / lev
    if required_margin > margin_free:
        return False, "Trade rejected: insufficient free margin", {
            "required_margin": round(required_margin, 4),
            "free_margin": round(margin_free, 4),
            "position_size": round(proposed_size, 4),
        }
    remaining_free_margin = margin_free - required_margin
    min_free_buffer = bal * float(MARGIN_CALL_BUFFER_PCT)
    if remaining_free_margin < min_free_buffer:
        return False, "Trade rejected: margin call protection buffer breached", {
            "remaining_free_margin": round(remaining_free_margin, 4),
            "min_required_buffer": round(min_free_buffer, 4),
        }

    # Exposure caps: projected notional must stay under symbol/portfolio limits.
    total_limit = max(0.0, bal * lev * float(MAX_TOTAL_EXPOSURE_MULT))
    symbol_limit = max(0.0, total_limit * float(MAX_SYMBOL_EXPOSURE_FRACTION))
    sector_limit = max(0.0, total_limit * float(MAX_SECTOR_EXPOSURE_FRACTION))
    projected_notional = entry * proposed_size
    projected_symbol = float(current_symbol_exposure) + projected_notional
    projected_total = float(current_total_exposure) + projected_notional

    if symbol_limit > 0 and projected_symbol > symbol_limit:
        return False, "Trade rejected: max exposure per symbol exceeded", {
            "projected_symbol_exposure": round(projected_symbol, 4),
            "symbol_limit": round(symbol_limit, 4),
        }
    if total_limit > 0 and projected_total > total_limit:
        return False, "Trade rejected: max total exposure exceeded", {
            "projected_total_exposure": round(projected_total, 4),
            "total_limit": round(total_limit, 4),
        }

    # Correlated/cluster exposure: sector bucket limit.
    ex_map = dict(exposure_by_symbol or {})
    curr_sector = _sector_for_symbol(str(symbol))
    sector_exposure = 0.0
    for sym, notion in ex_map.items():
        try:
            if _sector_for_symbol(str(sym)) == curr_sector:
                sector_exposure += float(notion or 0.0)
        except Exception:
            continue
    projected_sector = sector_exposure + projected_notional
    if sector_limit > 0 and projected_sector > sector_limit:
        return False, "Trade rejected: correlated exposure limit exceeded", {
            "sector": curr_sector,
            "projected_sector_exposure": round(projected_sector, 4),
            "sector_limit": round(sector_limit, 4),
        }

    pol_en, pol_ar = _execution_policy_labels(
        tier,
        sl_relax_active=bool(sl_relax_active),
        relax_pct_label=relax_pct_label,
    )

    return True, "approved", {
        "position_size": round(proposed_size, 4),
        "required_margin": round(required_margin, 4),
        "stop_distance": round(stop_dist, 6),
        "stop_loss_pct": round(stop_loss_pct, 8),
        "projected_symbol_exposure": round(projected_symbol, 4),
        "projected_total_exposure": round(projected_total, 4),
        "remaining_free_margin": round(remaining_free_margin, 4),
        "sector": curr_sector,
        "projected_sector_exposure": round(projected_sector, 4),
        "subscription_tier": tier,
        "sl_relaxation_active": bool(sl_relax_active),
        "execution_policy_en": pol_en,
        "execution_policy_ar": pol_ar,
    }
