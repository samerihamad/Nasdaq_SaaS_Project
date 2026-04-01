"""
NATB v2.0 — Trading Engine

Run alongside the Telegram bot (bot/dashboard.py) as a separate process.
Both processes share the SQLite database.

Pre-market alert:  sent ~30 minutes before market open (ET, DST-aware).
Daily scan:        runs before open when alert window starts, with open fallback.
Live cycle:        every CHECK_INTERVAL seconds during market hours.
Heartbeat:         written every HEARTBEAT_INTERVAL seconds for watchdog.py.
Backup:            hourly encrypted cloud backup in a background thread.
"""

import os
import sys
import json
import time
import sqlite3
import threading
import traceback
from datetime import datetime, timezone
from collections import Counter
from typing import Optional

from database.db_manager import create_db, is_maintenance_mode
from utils.filters import get_nasdaq_tickers, level1_filter, level2_filter, level3_filter
from utils.market_scanner import scan_multi_timeframe, scan_market, clear_local_price_caches
from utils.ai_model import (
    analyze_multi_timeframe,
    load_or_train_model,
    validate_signal,
    AI_PROBABILITY_THRESHOLD,
)
from utils.autonomous_training import AutonomousTrainingManager
from utils.market_hours import (
    get_market_status,
    minutes_to_open,
    STATUS_OPEN,
    STATUS_CLOSED,
    utc_today,
    get_current_timezones,
    ET,
    UAE,
    synchronized_utc_now,
    sync_utc_with_ntp,
)
from utils.daily_report import send_daily_reports
from core.executor import place_trade_for_user, monitor_and_close, process_pending_limit_orders
from core.watcher import run_watcher, get_all_active_subscribers, get_trading_subscribers
from core.signal_engine import scan_watchlist_parallel
from core.strategy_meanrev  import analyze as analyze_meanrev
from core.strategy_momentum import analyze as analyze_momentum
from core.risk_manager import get_risk_state, STATE_USER_DAY_HALT
from bot.notifier import send_telegram_message
from bot.dashboard import post_pending_signal, get_signal_status
from bot.i18n import t
from database.db_manager import set_trading_enabled
from database.db_manager import touch_engine_activity
from database.db_manager import get_user_signal_profile
from config import (
    MIN_CONFIDENCE,
    SIGNAL_MIN_CONFIDENCE,
    FAST_MIN_CONFIDENCE,
    GOLDEN_MIN_CONFIDENCE,
    FAST_MR_MIN_SCORE,
    GOLDEN_MR_MIN_SCORE,
    FAST_MOM_MIN_SCORE,
    GOLDEN_MOM_MIN_SCORE,
    GOLDEN_MOM_VOL_RATIO,
    GOLDEN_MOM_RSI_BUY_MAX,
    GOLDEN_MOM_RSI_SELL_MIN,
    CHECK_INTERVAL,
    MAX_WATCHLIST,
    HYBRID_SIGNAL_TTL,
    HEARTBEAT_INTERVAL,
    BACKUP_INTERVAL,
    HEARTBEAT_FILE,
    PREMARKET_ALERT_WINDOW_MIN,
    WATCHLIST_REFRESH_SECONDS,
    AI_MIN_PROB_RF,
    AI_MIN_PROB_MOMENTUM,
    AI_MIN_PROB_MEANREV,
    AI_SOFT_OVERRIDE_CONFIDENCE,
    AI_SOFT_OVERRIDE_MIN_PROB,
    ENABLE_AI_SOFT_OVERRIDE,
    ENABLE_STRUCTURAL_REJECTION_NOTIFY,
    STRUCTURAL_REJECTION_NOTIFY_COOLDOWN_SEC,
    STRUCTURAL_REJECTION_NOTIFY_MAX_PER_CYCLE,
    ENABLE_AUTONOMOUS_TRAINING,
)

# ── Single-instance lock ──────────────────────────────────────────────────────
_LOCK_FILE = "main.pid"

def _acquire_lock():
    """Write PID file. Exit if another instance is already running."""
    if os.path.exists(_LOCK_FILE):
        try:
            with open(_LOCK_FILE) as f:
                old_pid = int(f.read().strip())
            # Check if that PID is actually alive (Windows + Unix)
            import psutil
            if psutil.pid_exists(old_pid):
                print(f"❌ Another instance is already running (PID {old_pid}). Exiting.")
                sys.exit(1)
        except Exception:
            pass   # stale lock file — overwrite it
    with open(_LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def _release_lock():
    try:
        os.remove(_LOCK_FILE)
    except Exception:
        pass

_acquire_lock()
import atexit
atexit.register(_release_lock)

# Ensure all DB tables exist before anything else runs
create_db()
clear_local_price_caches()

# --- Configuration ---
ADMIN_CHAT_ID      = os.getenv("ADMIN_CHAT_ID", "")   # admin-only notifications

# Daily log folders (Phase 6 baseline).
LOG_ROOT = os.getenv("ENGINE_LOG_ROOT", "logs")
REJECTION_SUPPRESSION_NOTICE_COOLDOWN_SEC = int(
    os.getenv("REJECTION_SUPPRESSION_NOTICE_COOLDOWN_SEC", "1800")
)

# --- State ---
_watchlist          = []
_last_scan_date     = None
_premarket_sent     = None   # date of last pre-market alert
_daily_report_sent  = None   # date of last daily report
_prev_market_status = None   # detect OPEN→CLOSED transition
_closed_notified    = False  # sent "market closed" msg this session
_last_watchlist_refresh_at = None  # UTC datetime of last in-session refresh
_unsupported_all_day: set[str] = set()  # symbols unsupported for all users today
_last_structural_rejection_sent_at: dict[str, float] = {}
_last_structural_suppression_notice_at: float = 0.0
_autotrain_manager: Optional[AutonomousTrainingManager] = None
_market_open_last_alert_date: str | None = None

# File paths
_market_open_state_file = os.path.join(LOG_ROOT, "market_open_state.json")


def _ensure_daily_log_dir() -> str:
    """Create daily log directory and return its path."""
    day_dir = os.path.join(LOG_ROOT, synchronized_utc_now().strftime("%Y-%m-%d"))
    os.makedirs(day_dir, exist_ok=True)
    return day_dir


def _append_daily_log(filename: str, message: str):
    """Append a timestamped line into a daily log file."""
    try:
        log_dir = _ensure_daily_log_dir()
        path = os.path.join(log_dir, filename)
        ts = synchronized_utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception as exc:
        print(f"[LOG] Failed writing {filename}: {exc}")


def _append_market_open_log(message: str):
    """Append market-open alert entries into logs/market_open.log."""
    try:
        os.makedirs(LOG_ROOT, exist_ok=True)
        path = os.path.join(LOG_ROOT, "market_open.log")
        ts = synchronized_utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {message}\n")
    except Exception as exc:
        print(f"[MARKET OPEN LOG] write failed: {exc}")


def _load_market_open_last_alert_date() -> str | None:
    try:
        if not os.path.exists(_market_open_state_file):
            return None
        with open(_market_open_state_file, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        val = str(data.get("last_alert_date") or "").strip()
        return val or None
    except Exception:
        return None


def _save_market_open_last_alert_date(ny_date: str):
    try:
        os.makedirs(LOG_ROOT, exist_ok=True)
        with open(_market_open_state_file, "w", encoding="utf-8") as f:
            json.dump({"last_alert_date": str(ny_date)}, f)
    except Exception as exc:
        _append_market_open_log(f"state_write_error={exc}")


def _log_structural_rejection(symbol: str, strategy: str, reason: str, notified: bool = False):
    _append_daily_log(
        "structural_rejections.txt",
        f"symbol={symbol} strategy={strategy} notified={int(bool(notified))} reason={reason}",
    )
    # Keep structural rejections auditable in DB without Telegram noise.
    try:
        conn = sqlite3.connect("database/trading_saas.db")
        conn.execute(
            "INSERT INTO trade_rejections (created_at, chat_id, symbol, action, stage, reason, details) "
            "VALUES (datetime('now'), ?, ?, ?, ?, ?, ?)",
            (
                str(ADMIN_CHAT_ID or "n/a"),
                str(symbol or ""),
                "",
                "structural_filter",
                str(reason or ""),
                f"strategy={strategy} notified={int(bool(notified))}",
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _log_ai_gatekeeper(
    symbol: str,
    action: str,
    strategy: str,
    confidence: float,
    ms_score: Optional[float],
    probability: Optional[float],
    approved: bool,
    override: bool,
    regime: Optional[str],
):
    prob_str = "n/a" if probability is None else f"{float(probability):.2f}"
    _append_daily_log(
        "ai_telemetry.txt",
        (
            f"symbol={symbol} action={action} strategy={strategy} conf={float(confidence):.2f} "
            f"ms_score={('n/a' if ms_score is None else f'{float(ms_score):.1f}')} "
            f"prob={prob_str} approved={int(bool(approved))} override={int(bool(override))} "
            f"regime={regime or 'n/a'}"
        ),
    )


def _log_execution_audit(
    symbol: str,
    action: str,
    strategy: Optional[str],
    attempted: int,
    opened: int,
    skipped: int,
    failed: int,
    status: str,
):
    _append_daily_log(
        "execution_audit.txt",
        (
            f"status={status} symbol={symbol} action={action} strategy={strategy or 'n/a'} "
            f"attempted={attempted} opened={opened} skipped={skipped} failed={failed}"
        ),
    )


def _log_scan_cycle_summary(
    scanned_symbols: int,
    total_signals: int,
    rejected_signals: int,
    accepted_signals: int,
    ai_blocked: int,
):
    acceptance_ratio = (accepted_signals / total_signals * 100.0) if total_signals else 0.0
    rejection_ratio = (rejected_signals / total_signals * 100.0) if total_signals else 0.0
    _append_daily_log(
        "engine_cycle.txt",
        (
            f"scanned={scanned_symbols} candidates={total_signals} accepted={accepted_signals} "
            f"rejected={rejected_signals} ai_blocked={ai_blocked} "
            f"acceptance_ratio={acceptance_ratio:.2f}% rejection_ratio={rejection_ratio:.2f}%"
        ),
    )


# ── Heartbeat & backup threads ────────────────────────────────────────────────

def _heartbeat_loop():
    """Daemon thread: writes heartbeat.json every HEARTBEAT_INTERVAL seconds."""
    while True:
        try:
            with open(HEARTBEAT_FILE, 'w') as f:
                json.dump({'timestamp': synchronized_utc_now().isoformat()}, f)
        except Exception as e:
            print(f"[HEARTBEAT] write error: {e}")
        time.sleep(HEARTBEAT_INTERVAL)


def _backup_loop():
    """Daemon thread: runs an encrypted cloud backup every BACKUP_INTERVAL seconds.
    Runs only when the market is OPEN (no new trades during closed/off-hours)."""
    time.sleep(60)  # let main process stabilise before first backup
    while True:
        try:
            if is_maintenance_mode():
                print("[BACKUP] Skipped — maintenance mode is active.")
                time.sleep(BACKUP_INTERVAL)
                continue
            from utils.market_hours import get_market_status, STATUS_OPEN
            if get_market_status() != STATUS_OPEN:
                print("[BACKUP] Skipped — market is not OPEN (no new data to snapshot).")
            else:
                from utils.backup import run_backup
                print("[BACKUP] Starting hourly backup...")
                run_backup()
        except Exception as e:
            print(f"[BACKUP] Error: {e}")
        time.sleep(BACKUP_INTERVAL)


def _hybrid_approval_loop():
    """
    Background thread — non-blocking multi-user hybrid signal executor.

    Every 5 seconds:
      1. Execute any APPROVED signals (mark PROCESSING first to prevent duplicates).
      2. Expire PENDING signals older than HYBRID_SIGNAL_TTL.

    This decouples HYBRID mode from the main scan loop so multiple users'
    approval decisions are handled independently and concurrently.
    """
    while True:
        try:
            db_path = 'database/trading_saas.db'

            # ── Execute approved signals ──────────────────────────────────────
            conn = sqlite3.connect(db_path)
            c    = conn.cursor()
            c.execute(
                "SELECT signal_id, chat_id, symbol, action, confidence, strategy_label, stop_loss_pct "
                "FROM pending_signals WHERE status='APPROVED'"
            )
            approved = c.fetchall()
            conn.close()

            for signal_id, chat_id, symbol, action, confidence, strategy_label, stop_loss_pct in approved:
                # Mark PROCESSING atomically to prevent duplicate execution
                with sqlite3.connect(db_path) as cx:
                    cx.execute(
                        "UPDATE pending_signals SET status='PROCESSING' WHERE signal_id=? AND status='APPROVED'",
                        (signal_id,),
                    )
                try:
                    result = place_trade_for_user(
                        chat_id,
                        symbol,
                        action,
                        confidence=float(confidence),
                        stop_loss_pct=stop_loss_pct,
                        strategy_label=strategy_label,
                    )
                    print(f"   [HYBRID {chat_id}] Signal #{signal_id} → {result}")
                    # place_trade_for_user() sends its own rich message on success.
                    # For non-open outcomes, send explicit feedback to the user here
                    # so Hybrid mode never looks "silent" after approval.
                    with sqlite3.connect(db_path) as cx:
                        if isinstance(result, str) and result.startswith("✅ Opened"):
                            cx.execute(
                                "UPDATE pending_signals SET status='EXECUTED' WHERE signal_id=?",
                                (signal_id,),
                            )
                        elif isinstance(result, str) and result.startswith("🧾 Limit placed"):
                            cx.execute(
                                "UPDATE pending_signals SET status='EXECUTED' WHERE signal_id=?",
                                (signal_id,),
                            )
                        elif isinstance(result, str) and result.startswith("⏭️"):
                            cx.execute(
                                "UPDATE pending_signals SET status='SKIPPED' WHERE signal_id=?",
                                (signal_id,),
                            )
                            send_telegram_message(chat_id, result)
                        else:
                            cx.execute(
                                "UPDATE pending_signals SET status='FAILED' WHERE signal_id=?",
                                (signal_id,),
                            )
                            if isinstance(result, str) and result.strip():
                                send_telegram_message(chat_id, result)
                except Exception as exc:
                    print(f"   [HYBRID {chat_id}] Signal #{signal_id} execution error: {exc}")
                    with sqlite3.connect(db_path) as cx:
                        cx.execute(
                            "UPDATE pending_signals SET status='ERROR' WHERE signal_id=?",
                            (signal_id,),
                        )
                    try:
                        send_telegram_message(
                            chat_id,
                            f"❌ تعذر تنفيذ الصفقة بعد الموافقة.\n{symbol} {action}\n{exc}"
                        )
                    except Exception:
                        pass

            # ── Expire stale pending signals ──────────────────────────────────
            with sqlite3.connect(db_path) as cx:
                cx.execute(
                    "UPDATE pending_signals SET status='EXPIRED' "
                    "WHERE status='PENDING' "
                    "AND created_at < datetime('now', ? || ' seconds')",
                    (f"-{HYBRID_SIGNAL_TTL}",),
                )

        except Exception as exc:
            print(f"[HYBRID LOOP] Error: {exc}")

        time.sleep(5)


def _limit_order_worker_loop():
    """Daemon worker: tracks pending limits and handles TTL cancels."""
    while True:
        try:
            n = process_pending_limit_orders()
            if n:
                print(f"[LIMIT WORKER] processed={n}")
        except Exception as exc:
            print(f"[LIMIT WORKER] Error: {exc}")
        time.sleep(5)


def _market_open_alert_loop():
    """
    Daemon loop: sends one admin alert exactly at NYSE/NASDAQ regular open (09:30 ET).
    Checks every 30 seconds and enforces one alert per New York trading date.
    """
    global _market_open_last_alert_date

    def _fmt_float(v: float | None, digits: int = 2) -> str:
        if v is None:
            return "N/A"
        try:
            return f"{float(v):.{int(digits)}f}"
        except Exception:
            return "N/A"

    def _compute_open_snapshot(now_utc: datetime) -> dict:
        out = {
            "spy_open": None,
            "gap_pct": None,
            "gap_dir_en": "Flat",
            "gap_dir_ar": "محايد",
            "premarket_summary_en": "N/A",
            "premarket_summary_ar": "غير متاح",
        }
        try:
            d1 = scan_market("SPY", period="5d", interval="1d")
            if d1 is not None and len(d1) >= 2:
                prev_close = float(d1["Close"].iloc[-2])
                curr_open = float(d1["Open"].iloc[-1])
                out["spy_open"] = curr_open
                if prev_close > 0:
                    gp = ((curr_open - prev_close) / prev_close) * 100.0
                    out["gap_pct"] = gp
                    if gp > 0.02:
                        out["gap_dir_en"] = "Up"
                        out["gap_dir_ar"] = "صاعد"
                    elif gp < -0.02:
                        out["gap_dir_en"] = "Down"
                        out["gap_dir_ar"] = "هابط"

            m15 = scan_market("SPY", period="1d", interval="15m")
            if m15 is not None and not m15.empty:
                w = m15.copy()
                idx = w.index
                if getattr(idx, "tz", None) is None:
                    idx = idx.tz_localize(timezone.utc)
                else:
                    idx = idx.tz_convert(timezone.utc)
                w.index = idx.tz_convert(ET)
                pm = w.between_time("09:00", "09:29")
                if not pm.empty:
                    pm_h = float(pm["High"].max())
                    pm_l = float(pm["Low"].min())
                    pm_v = float(pm["Volume"].sum())
                    out["premarket_summary_en"] = (
                        f"Range {pm_l:.2f}-{pm_h:.2f} | Vol {pm_v:,.0f}"
                    )
                    out["premarket_summary_ar"] = (
                        f"المدى {pm_l:.2f}-{pm_h:.2f} | الحجم {pm_v:,.0f}"
                    )
        except Exception as exc:
            _append_market_open_log(f"snapshot_error={exc}")
        return out

    if _market_open_last_alert_date is None:
        _market_open_last_alert_date = _load_market_open_last_alert_date()
    while True:
        try:
            now_utc = synchronized_utc_now()
            now_ny = now_utc.astimezone(ET)
            ny_date = now_ny.date().isoformat()
            now_dubai = now_utc.astimezone(UAE)
            # Wide-enough window to avoid missing 09:30 tick if loop is delayed.
            is_open_window = (
                now_ny.hour == 9
                and 30 <= now_ny.minute <= 35
            )
            if is_open_window and _market_open_last_alert_date != ny_date:
                tz_map = get_current_timezones(now_utc)
                snapshot = _compute_open_snapshot(now_utc)
                ny_clock = now_ny.strftime("%Y-%m-%d %H:%M:%S ET")
                dubai_clock = now_dubai.strftime("%Y-%m-%d %H:%M:%S GST")
                watchlist_size = len(_watchlist)
                spy_open_txt = _fmt_float(snapshot.get("spy_open"), 2)
                gap_pct_val = snapshot.get("gap_pct")
                gap_pct_txt = _fmt_float(gap_pct_val, 2)
                lang = os.getenv("ADMIN_LANG", "ar").strip().lower()
                system_status = (
                    f"Engine=ACTIVE | Market={get_market_status()} | Watchlist={watchlist_size}"
                )

                english_template = (
                    "🚀 Market Open — NYSE/NASDAQ just opened.\n"
                    "🕘 New York time: {ny_time}\n"
                    "🕓 Dubai time: {dubai_time}\n"
                    "🟢 System status: {system_status}\n"
                    "📈 SPY price at open: {spy_open}\n"
                    "📊 Gap: {gap_dir} ({gap_pct}%)\n"
                    "🌅 Pre-market summary: {premarket_summary}\n"
                    "— NATB v2.0"
                )
                arabic_template = (
                    "🚀 افتتاح السوق — NYSE/NASDAQ بدأ الآن.\n"
                    "🕘 توقيت نيويورك: {ny_time}\n"
                    "🕓 توقيت دبي: {dubai_time}\n"
                    "🟢 حالة النظام: {system_status}\n"
                    "📈 سعر SPY عند الافتتاح: {spy_open}\n"
                    "📊 الفجوة: {gap_dir} ({gap_pct}%)\n"
                    "🌅 ملخص ما قبل الافتتاح: {premarket_summary}\n"
                    "— NATB v2.0"
                )

                if lang == "en":
                    msg = english_template.format(
                        ny_time=ny_clock,
                        dubai_time=dubai_clock,
                        system_status=system_status,
                        spy_open=spy_open_txt,
                        gap_dir=snapshot.get("gap_dir_en", "Flat"),
                        gap_pct=gap_pct_txt,
                        premarket_summary=snapshot.get("premarket_summary_en", "N/A"),
                    )
                else:
                    msg = arabic_template.format(
                        ny_time=ny_clock,
                        dubai_time=dubai_clock,
                        system_status=system_status,
                        spy_open=spy_open_txt,
                        gap_dir=snapshot.get("gap_dir_ar", "محايد"),
                        gap_pct=gap_pct_txt,
                        premarket_summary=snapshot.get("premarket_summary_ar", "غير متاح"),
                    )
                if ADMIN_CHAT_ID:
                    try:
                        send_telegram_message(ADMIN_CHAT_ID, msg)
                    except Exception as exc:
                        _append_market_open_log(f"notify_error={exc}")
                _append_market_open_log(
                    f"alert_sent=1 ny_date={ny_date} lang={('en' if lang == 'en' else 'ar')} "
                    f"utc={tz_map.get('utc')} new_york={tz_map.get('new_york')} dubai={tz_map.get('dubai')}\n"
                    f"{msg}"
                )
                _market_open_last_alert_date = ny_date
                _save_market_open_last_alert_date(ny_date)
        except Exception as exc:
            _append_market_open_log(f"loop_error={exc}")
        time.sleep(30)


def _start_background_threads():
    global _autotrain_manager
    for target, name in [
        (_heartbeat_loop,       "heartbeat"),
        (_backup_loop,          "backup"),
        (_hybrid_approval_loop, "hybrid-approvals"),
        (_limit_order_worker_loop, "limit-orders"),
        (_market_open_alert_loop, "market-open-alert"),
    ]:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        print(f"   Thread '{name}' started.")
    if _autotrain_manager is None:
        _autotrain_manager = AutonomousTrainingManager(
            admin_chat_id=ADMIN_CHAT_ID,
            watchlist_provider=lambda: list(_watchlist),
        )
        _autotrain_manager.start()
        if ENABLE_AUTONOMOUS_TRAINING:
            print("   Thread 'autonomous-training-scheduler' started.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_user_lang(chat_id: str) -> str:
    conn = sqlite3.connect('database/trading_saas.db')
    c    = conn.cursor()
    c.execute("SELECT lang FROM subscribers WHERE chat_id=?", (chat_id,))
    row  = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else 'ar'


def _get_user_mode(chat_id: str) -> str:
    conn = sqlite3.connect('database/trading_saas.db')
    c    = conn.cursor()
    c.execute("SELECT mode FROM subscribers WHERE chat_id=?", (chat_id,))
    row  = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else 'AUTO'


def _profile_thresholds(profile: str) -> tuple[float, int, int]:
    """Resolve confidence/score thresholds for FAST vs GOLDEN profile."""
    p = str(profile or "FAST").strip().upper()
    if p == "GOLDEN":
        return float(GOLDEN_MIN_CONFIDENCE), int(GOLDEN_MR_MIN_SCORE), int(GOLDEN_MOM_MIN_SCORE)
    return float(FAST_MIN_CONFIDENCE), int(FAST_MR_MIN_SCORE), int(FAST_MOM_MIN_SCORE)


def _is_mean_reversion_strategy_label(strategy_label: str | None) -> bool:
    s = (strategy_label or "").strip().lower()
    return "meanrev" in s or "meanreversion" in s


def _is_momentum_strategy_label(strategy_label: str | None) -> bool:
    return "momentum" in (strategy_label or "").strip().lower()


def _passes_profile_gate(
    *,
    profile: str,
    strategy_label: str | None,
    confidence: float,
    signal_score: float | None,
    mr_fast_bypass: bool = False,
    action: str | None = None,
    mom_rsi_15m: float | None = None,
    mom_vol_ratio: float | None = None,
) -> tuple[bool, str]:
    """Per-user profile gate used before AUTO/HYBRID dispatch."""
    p = str(profile or "FAST").strip().upper()
    if p == "GOLDEN" and mr_fast_bypass and _is_mean_reversion_strategy_label(strategy_label):
        return (
            False,
            "GOLDEN requires full Mean Reversion confirmation (FAST RSI-extreme bypass not allowed)",
        )

    if (
        p == "GOLDEN"
        and _is_momentum_strategy_label(strategy_label)
        and mom_rsi_15m is not None
        and mom_vol_ratio is not None
    ):
        rsi_v = float(mom_rsi_15m)
        vr = float(mom_vol_ratio)
        act = str(action or "").strip().upper()
        gvr = float(GOLDEN_MOM_VOL_RATIO)
        if act == "BUY":
            if not (50.0 <= rsi_v <= float(GOLDEN_MOM_RSI_BUY_MAX)) or vr < gvr:
                return (
                    False,
                    f"GOLDEN momentum buy needs RSI 50–{float(GOLDEN_MOM_RSI_BUY_MAX):.0f} "
                    f"and Vol≥{gvr:.1f}x (got RSI={rsi_v:.1f}, Vol={vr:.2f}x)",
                )
        elif act == "SELL":
            if not (float(GOLDEN_MOM_RSI_SELL_MIN) <= rsi_v <= 50.0) or vr < gvr:
                return (
                    False,
                    f"GOLDEN momentum sell needs RSI {float(GOLDEN_MOM_RSI_SELL_MIN):.0f}–50 "
                    f"and Vol≥{gvr:.1f}x (got RSI={rsi_v:.1f}, Vol={vr:.2f}x)",
                )

    min_conf, mr_min, mom_min = _profile_thresholds(profile)
    if float(confidence) < float(min_conf):
        return False, f"confidence {float(confidence):.1f} < {float(min_conf):.1f}"

    label = str(strategy_label or "").strip().lower()
    if label == "meanrev" and signal_score is not None and float(signal_score) < float(mr_min):
        return False, f"meanrev score {float(signal_score):.1f} < {float(mr_min):.1f}"
    if label == "momentum" and signal_score is not None and float(signal_score) < float(mom_min):
        return False, f"momentum score {float(signal_score):.1f} < {float(mom_min):.1f}"
    return True, "ok"


def _is_license_valid_for_user(chat_id: str) -> bool:
    """Lightweight license check for engine auto-enable decisions."""
    conn = sqlite3.connect('database/trading_saas.db')
    c    = conn.cursor()
    c.execute(
        "SELECT payment_status, expiry_date FROM subscribers WHERE chat_id=?",
        (chat_id,),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return False
    payment_status, expiry_date = row
    if payment_status != 'APPROVED' or not expiry_date:
        return False
    try:
        return datetime.fromisoformat(f"{expiry_date}T00:00:00+00:00").date() >= utc_today()
    except Exception:
        return False


def _auto_resume_trading_at_open():
    """
    Safety net at market open:
    Re-enable trading for eligible users so an accidental pause does not
    silently waste a full trading day.
    """
    resumed = 0
    for row in get_all_active_subscribers():
        chat_id = str(row[0])
        try:
            # Respect explicit user day-halt; only auto-resume normal users.
            if get_risk_state(chat_id) == STATE_USER_DAY_HALT:
                continue
            if _get_user_mode(chat_id) != 'AUTO':
                continue
            if not _is_license_valid_for_user(chat_id):
                continue

            set_trading_enabled(chat_id, True)
            resumed += 1

        except Exception as exc:
            print(f"[AUTO-RESUME] Skip {chat_id}: {exc}")

    if resumed:
        print(f"[AUTO-RESUME] Enabled trading for {resumed} eligible subscriber(s) at market open.")


# ── Daily scan ────────────────────────────────────────────────────────────────

def run_daily_scan():
    print("=" * 55)
    print(f"🌅 المسح اليومي الشامل — {synchronized_utc_now().strftime('%Y-%m-%d %H:%M UTC')}")

    tickers = get_nasdaq_tickers()
    if not tickers:
        print("❌ فشل جلب قائمة الأسهم.")
        return []

    print(f"📋 إجمالي أسهم ناسداك: {len(tickers)}")
    level1 = level1_filter(tickers, top_n=300)
    level2 = level2_filter(level1)
    level3 = level3_filter(level2)

    # Prefer Level3 (cheap + controlled daily range),
    # but always fill up to MAX_WATCHLIST from Level2 if Level3 returns fewer.
    seen = set()
    watchlist = []
    for sym in level3:
        if sym in seen:
            continue
        seen.add(sym)
        watchlist.append(sym)
        if len(watchlist) >= MAX_WATCHLIST:
            break

    if len(watchlist) < MAX_WATCHLIST:
        for sym in level2:
            if sym in seen:
                continue
            seen.add(sym)
            watchlist.append(sym)
            if len(watchlist) >= MAX_WATCHLIST:
                break

    print(f"✅ القائمة النهائية: {len(watchlist)} سهم")
    return watchlist


def pretrain_models(watchlist):
    global _autotrain_manager
    print(f"\n🤖 تجهيز نماذج RF لـ {len(watchlist)} سهم...")
    if _autotrain_manager is not None:
        try:
            _autotrain_manager.update_watchlist(list(watchlist or []))
        except Exception:
            pass
    for i, symbol in enumerate(watchlist, 1):
        print(f"   [{i}/{len(watchlist)}] {symbol}", end="\r")
        # Warm all inference timeframes to avoid first-signal latency spikes.
        for tf in ("1d", "4h", "15m"):
            try:
                load_or_train_model(symbol, timeframe=tf)
            except Exception:
                pass
    print(f"\n✅ جميع النماذج جاهزة.")
    print("=" * 55)


# ── Pre-market alert ──────────────────────────────────────────────────────────

def send_premarket_alert(watchlist_count: int):
    """Broadcast pre-market alert to all active subscribers (per-user language)."""
    global _premarket_sent
    from core.watcher import get_all_active_subscribers

    for row in get_all_active_subscribers():
        chat_id = str(row[0])
        lang    = _get_user_lang(chat_id)
        mode    = _get_user_mode(chat_id)
        mode_label = (
            t('btn_mode_auto', lang) if mode == 'AUTO' else t('btn_mode_hybrid', lang)
        )
        msg = t(
            'premarket_alert',
            lang,
            watchlist_count=watchlist_count,
            mode=mode_label,
        )
        try:
            send_telegram_message(chat_id, msg)
        except Exception:
            pass

    _premarket_sent = utc_today()
    print("Pre-market alert sent to all subscribers (localized).")


# ── Signal dispatch ───────────────────────────────────────────────────────────

def dispatch_signal(symbol: str, action: str, confidence: float, reason: str,
                    timeframes: dict = None, stop_loss_pct: float = None, strategy_label: str = None,
                    ms_score: Optional[float] = None, signal_score: Optional[float] = None,
                    mr_fast_bypass: bool = False, rsi_15m: Optional[float] = None,
                    mom_rsi_15m: Optional[float] = None, mom_vol_ratio: Optional[float] = None):
    """
    Multi-tenant signal dispatcher.

    Step 1 — AI Gatekeeper: validate_signal() is evaluated ONCE per signal.
             A block here stops execution for ALL users (saves N broker calls).

    Step 2 — Per-user dispatch:
             AUTO   users → place_trade_for_user() called immediately.
             HYBRID users → signal posted to pending_signals table.
                            _hybrid_approval_loop() (background thread) handles
                            execution when the user approves via Telegram.

    Thread safety: each user gets their own broker session and DB row.
    No shared mutable state between users.
    """
    # ── AI Gatekeeper (evaluated once, applies to all users) ──────────────────
    # Consistent decision model:
    # - We rely on validate_signal(..., min_probability=...) to produce the boolean gate.
    # - We allow a soft override only for Momentum/MeanRev when confidence is high.
    ai_prob = None
    ai_approved = True
    ai_override = False
    regime = "UNKNOWN"
    if timeframes:
        strategy_key = (strategy_label or "RF").strip()
        ai_min_by_strategy = {
            "RF": AI_MIN_PROB_RF,
            "Momentum": AI_MIN_PROB_MOMENTUM,
            "MeanRev": AI_MIN_PROB_MEANREV,
        }
        ai_min_prob = ai_min_by_strategy.get(strategy_key, AI_MIN_PROB_RF)
        ai_approved, ai_prob, regime = validate_signal(
            symbol, action, timeframes, min_probability=ai_min_prob, ms_score=ms_score
        )

        # Soft override (Momentum/MeanRev): no VOLATILE block — see config AI_SOFT_OVERRIDE_*
        ai_override = (
            ENABLE_AI_SOFT_OVERRIDE
            and
            (not ai_approved)
            and confidence >= AI_SOFT_OVERRIDE_CONFIDENCE
            and ai_prob >= AI_SOFT_OVERRIDE_MIN_PROB
            and strategy_key in ("Momentum", "MeanRev")
        )

        # ── AI DEBUG (CRITICAL) ───────────────────────────────────────────────
        print(
            f"[AI DEBUG] {symbol} {action} | "
            f"conf={float(confidence):.1f} | prob={float(ai_prob):.1f} | "
            f"approved={bool(ai_approved)} | override={bool(ai_override)} | "
            f"strategy={strategy_key} | regime={regime}"
        )
        _log_ai_gatekeeper(
            symbol=symbol,
            action=action,
            strategy=strategy_key,
            confidence=float(confidence),
            ms_score=ms_score,
            probability=(float(ai_prob) if ai_prob is not None else None),
            approved=bool(ai_approved),
            override=bool(ai_override),
            regime=regime,
        )

        if not (ai_approved or ai_override):
            print(
                f"[AI BLOCK] {symbol} {action} | "
                f"prob={float(ai_prob):.1f} < min={float(ai_min_prob):.1f} | "
                f"conf={float(confidence):.1f} | strategy={strategy_key} | regime={regime}"
            )
            _log_execution_audit(
                symbol=symbol,
                action=action,
                strategy=strategy_label,
                attempted=0,
                opened=0,
                skipped=0,
                failed=0,
                status="ai_blocked",
            )
            return {
                "status": "ai_blocked",
                "attempted": 0,
                "opened": 0,
                "skipped": 0,
                "failed": 0,
            }

        if ai_override:
            print(
                f"   [AI OVERRIDE] {symbol} {action} — "
                f"probability={ai_prob:.1f}% < min({strategy_key})={ai_min_prob:.1f}% "
                f"but confidence={confidence:.1f}% >= {AI_SOFT_OVERRIDE_CONFIDENCE:.1f}% "
                f"| regime={regime}"
            )
        else:
            print(
                f"   [AI OK] {symbol} {action} — probability={ai_prob:.1f}% "
                f"| min({strategy_key})={ai_min_prob:.1f}% | regime={regime}"
            )

    # ── Iterate only subscribers who have started their trading engine ─────────
    subscribers = get_trading_subscribers()
    if not subscribers:
        print(f"   [DISPATCH] No subscribers with trading enabled — skipping signal.")
        _log_execution_audit(
            symbol=symbol,
            action=action,
            strategy=strategy_label,
            attempted=0,
            opened=0,
            skipped=0,
            failed=0,
            status="no_subscribers",
        )
        return {
            "status": "no_subscribers",
            "attempted": 0,
            "opened": 0,
            "skipped": 0,
            "failed": 0,
        }
    if symbol in _unsupported_all_day:
        print(f"   [DISPATCH] {symbol} marked unsupported-for-all today — skipping.")
        _log_execution_audit(
            symbol=symbol,
            action=action,
            strategy=strategy_label,
            attempted=0,
            opened=0,
            skipped=0,
            failed=0,
            status="unsupported_cached",
        )
        return {
            "status": "unsupported_cached",
            "attempted": 0,
            "opened": 0,
            "skipped": 0,
            "failed": 0,
        }

    attempted = 0
    opened = 0
    skipped = 0
    failed = 0
    unsupported_for_all = True
    for row in subscribers:
        chat_id = str(row[0])
        try:
            mode = _get_user_mode(chat_id)
            profile = get_user_signal_profile(chat_id)
            allowed, why_not = _passes_profile_gate(
                profile=profile,
                strategy_label=strategy_label,
                confidence=float(confidence),
                signal_score=(float(signal_score) if signal_score is not None else None),
                mr_fast_bypass=bool(mr_fast_bypass),
                action=action,
                mom_rsi_15m=(float(mom_rsi_15m) if mom_rsi_15m is not None else None),
                mom_vol_ratio=(float(mom_vol_ratio) if mom_vol_ratio is not None else None),
            )
            if not allowed:
                skipped += 1
                unsupported_for_all = False
                print(
                    f"   [PROFILE SKIP {chat_id}] profile={profile} "
                    f"{symbol} {action} | {why_not}"
                )
                continue

            if mode == 'AUTO':
                attempted += 1
                # Heartbeat for admin monitoring: engine is actively processing this user.
                try:
                    touch_engine_activity(chat_id)
                except Exception:
                    pass
                result = place_trade_for_user(
                    chat_id, symbol, action,
                    confidence=confidence, stop_loss_pct=stop_loss_pct,
                    strategy_label=strategy_label,
                    ai_prob=ai_prob,
                )
                print(f"   [AUTO  {chat_id}] {symbol} {action} → {result}")
                if isinstance(result, str) and result.startswith("✅ Opened"):
                    opened += 1
                    unsupported_for_all = False
                    print(f"[TRADE OPENED] {symbol} {action}")
                    if (
                        mr_fast_bypass
                        and profile == "FAST"
                        and _is_mean_reversion_strategy_label(strategy_label)
                        and rsi_15m is not None
                    ):
                        print(
                            f"[FAST EXECUTION] RSI Extreme ({float(rsi_15m):.1f}) - Bypassing Reversal Confirmation"
                        )
                elif isinstance(result, str) and result.startswith("Trade rejected:"):
                    skipped += 1
                    unsupported_for_all = False
                    print(f"[PRETRADE BLOCK] {result}")
                elif isinstance(result, str) and result.startswith("⏭️"):
                    skipped += 1
                    if "symbol not available on broker" not in result:
                        unsupported_for_all = False
                    print(f"[TRADE SKIPPED] {result}")
                else:
                    failed += 1
                    unsupported_for_all = False
                    print(f"[TRADE FAILED] {result}")

            else:
                # HYBRID: post to DB, non-blocking.
                # _hybrid_approval_loop() executes it when user approves.
                sig_id = post_pending_signal(
                    chat_id, symbol, action, confidence, reason,
                    strategy_label=strategy_label,
                    stop_loss_pct=stop_loss_pct,
                )
                print(f"   [HYBRID {chat_id}] Signal #{sig_id} posted — awaiting approval")
                attempted += 1
                # In HYBRID mode this means "queued", not opened yet.
                skipped += 1
                unsupported_for_all = False
                print(f"[TRADE SKIPPED] HYBRID queued signal_id={sig_id} ({symbol} {action})")

        except Exception as exc:
            print(f"   [ERROR  {chat_id}] dispatch failed: {exc}")
            failed += 1
            unsupported_for_all = False
            print(f"[TRADE FAILED] dispatch exception: {exc}")

    print(
        f"   Signal outcome: attempted={attempted}/{len(subscribers)} | "
        f"opened={opened} | skipped={skipped} | failed={failed}"
    )
    if attempted > 0 and unsupported_for_all:
        _unsupported_all_day.add(symbol)
        print(f"   [WATCHLIST PRUNE] {symbol} marked unsupported for all users today.")
    _log_execution_audit(
        symbol=symbol,
        action=action,
        strategy=strategy_label,
        attempted=attempted,
        opened=opened,
        skipped=skipped,
        failed=failed,
        status="executed",
    )
    return {
        "status": "executed",
        "attempted": attempted,
        "opened": opened,
        "skipped": skipped,
        "failed": failed,
        "ai_approved": bool(ai_approved),
        "ai_override": bool(ai_override),
        "ai_probability": ai_prob,
    }


def _notify_structural_rejection(symbol: str, strategy: str, reason: str) -> bool:
    if not ENABLE_STRUCTURAL_REJECTION_NOTIFY:
        _log_structural_rejection(symbol, strategy, reason, notified=False)
        return False
    try:
        if ADMIN_CHAT_ID:
            lang = _get_user_lang(ADMIN_CHAT_ID)
            reason_l = (reason or "").lower()
            if "no-trade zone" in reason_l:
                bucket = "no_trade_zone"
            elif "premium zone" in reason_l and "for buy" in reason_l:
                bucket = "premium_buy_reject"
            elif "discount zone" in reason_l and "for sell" in reason_l:
                bucket = "discount_sell_reject"
            else:
                bucket = "generic_reject"

            cooldown_key = f"{strategy}:{bucket}"
            now_ts = time.time()
            last_ts = _last_structural_rejection_sent_at.get(cooldown_key, 0.0)
            if (now_ts - last_ts) < float(STRUCTURAL_REJECTION_NOTIFY_COOLDOWN_SEC):
                _log_structural_rejection(symbol, strategy, reason, notified=False)
                return False

            ar_reason = reason
            if "no-trade zone" in reason_l:
                ar_reason = "❌ تم الرفض: منطقة لا تداول (No-Trade Zone)."
            elif "premium zone" in reason_l and "for buy" in reason_l:
                ar_reason = "❌ تم رفض الصفقة: السعر في منطقة غالية (Premium Zone) وإشارة الشراء غير مسموحة."
            elif "discount zone" in reason_l and "for sell" in reason_l:
                ar_reason = "❌ تم رفض الصفقة: السعر في منطقة مخفضة (Discount Zone) وإشارة البيع غير مسموحة."
            elif "rejected:" in reason_l:
                ar_reason = f"❌ تم رفض الصفقة: {reason.replace('Rejected: ', '').strip()}"

            if lang == "ar":
                msg = (
                    "⛔ رفض فلتر الهيكل السوقي\n"
                    f"الرمز: {symbol}\n"
                    f"الاستراتيجية: {strategy}\n"
                    f"السبب: {ar_reason}"
                )
            else:
                msg = (
                    "⛔ Structural Filter Rejection\n"
                    f"Symbol: {symbol}\n"
                    f"Strategy: {strategy}\n"
                    f"Reason: {reason}"
                )
            send_telegram_message(ADMIN_CHAT_ID, msg)
            _last_structural_rejection_sent_at[cooldown_key] = now_ts
            _log_structural_rejection(symbol, strategy, reason, notified=True)
            return True
    except Exception:
        _log_structural_rejection(symbol, strategy, reason, notified=False)
        return False
    _log_structural_rejection(symbol, strategy, reason, notified=False)
    return False


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_trading_bot():
    global _watchlist, _last_scan_date, _prev_market_status, _closed_notified, _daily_report_sent, _last_watchlist_refresh_at, _unsupported_all_day, _last_structural_suppression_notice_at

    print("🚀 NATB v2.0 — محرك التداول الذكي")
    # Scan uses the looser floor so FAST-tier candidates are not discarded before per-user gates.
    active_min_conf = min(
        float(FAST_MIN_CONFIDENCE),
        float(GOLDEN_MIN_CONFIDENCE),
    )
    print(
        f"   أدنى ثقة للمسح: {active_min_conf}% "
        f"(FAST≥{float(FAST_MIN_CONFIDENCE):.0f}% GOLDEN≥{float(GOLDEN_MIN_CONFIDENCE):.0f}%) | فحص كل {CHECK_INTERVAL}s"
    )
    print("-" * 55)
    try:
        ntp_diag = sync_utc_with_ntp()
        _append_daily_log("timezone_snapshot.txt", f"startup_ntp_sync={ntp_diag}")
    except Exception:
        pass

    # Timezone snapshot for runtime auditability.
    try:
        tz_map = get_current_timezones()
        _append_daily_log(
            "timezone_snapshot.txt",
            (
                f"utc={tz_map.get('utc')} dubai={tz_map.get('dubai')} "
                f"new_york={tz_map.get('new_york')} ny_dst={int(bool(tz_map.get('new_york_is_dst')))}"
            ),
        )
    except Exception:
        pass

    # Telegram health ping (helps detect missing token / blocked bot early)
    try:
        if not ADMIN_CHAT_ID:
            print("[Telegram] ADMIN_CHAT_ID is not set; startup ping skipped.", flush=True)
        elif not is_maintenance_mode():
            now_utc = synchronized_utc_now().strftime("%Y-%m-%d %H:%M UTC")
            send_telegram_message(
                ADMIN_CHAT_ID,
                f"✅ Engine started\n🕒 {now_utc}",
            )
    except Exception as exc:
        print(f"[Telegram] Startup ping failed: {exc}", flush=True)

    _start_background_threads()

    while True:
        try:
            now_utc       = synchronized_utc_now()
            today         = utc_today()
            market_status = get_market_status()

            if _prev_market_status is not None and _prev_market_status != market_status:
                try:
                    tz_map = get_current_timezones(now_utc)
                    _append_daily_log(
                        "timezone_snapshot.txt",
                        (
                            f"event=market_status_change from={_prev_market_status} to={market_status} "
                            f"utc={tz_map.get('utc')} dubai={tz_map.get('dubai')} "
                            f"new_york={tz_map.get('new_york')} ny_dst={int(bool(tz_map.get('new_york_is_dst')))}"
                        ),
                    )
                except Exception:
                    pass

            # ── Detect OPEN → CLOSED transition → send daily report ───────────
            if _prev_market_status == STATUS_OPEN and market_status != STATUS_OPEN:
                if _daily_report_sent != today:
                    print("[MARKET] السوق أغلق — إرسال التقرير اليومي لجميع المشتركين...")
                    send_daily_reports()
                    _daily_report_sent = today

            # ── Detect CLOSED → OPEN transition → reset closed flag ───────────
            if _prev_market_status != STATUS_OPEN and market_status == STATUS_OPEN:
                _closed_notified = False
                _auto_resume_trading_at_open()

            _prev_market_status = market_status

            # ── Maintenance mode ──────────────────────────────────────────────
            if is_maintenance_mode():
                print("🔧 وضع الصيانة نشط — تشغيل المراقب فقط...")
                orphans = run_watcher()
                if orphans:
                    print(f"   🔍 تم استرداد {orphans} صفقة يتيمة.")
                time.sleep(CHECK_INTERVAL)
                continue

            # ── Pre-open prep/alerts (works in CLOSED and PRE_MARKET) ─────────
            if market_status != STATUS_OPEN:
                mins = minutes_to_open()

                # Build watchlist before open (once daily) so pre-market alert
                # has a real count and models are warm.
                if (_last_scan_date != today
                        and 0 < mins <= PREMARKET_ALERT_WINDOW_MIN):
                    _watchlist = run_daily_scan()
                    _last_scan_date = today
                    _unsupported_all_day.clear()
                    if _watchlist:
                        pretrain_models(_watchlist)

                # If scan failed earlier and we are still inside alert window,
                # one lazy retry allows alert dispatch in PRE_MARKET as well.
                if (_premarket_sent != today
                        and 0 < mins <= PREMARKET_ALERT_WINDOW_MIN
                        and not _watchlist
                        and _last_scan_date != today):
                    _watchlist = run_daily_scan()
                    _last_scan_date = today
                    if _watchlist:
                        pretrain_models(_watchlist)

                # DST-safe pre-market alert window.
                # Ensure the alert is dispatched once even if Level3 produced
                # an empty watchlist (we fill in run_daily_scan()).
                if (_premarket_sent != today
                        and 0 < mins <= PREMARKET_ALERT_WINDOW_MIN
                        and (_watchlist or _last_scan_date == today)):
                    if not _watchlist:
                        # Last-resort: build a watchlist right before sending.
                        _watchlist = run_daily_scan()
                    send_premarket_alert(len(_watchlist))

            # ── Market CLOSED: notify once, run watcher for all users ─────────
            if market_status == STATUS_CLOSED:
                if not _closed_notified:
                    mins = minutes_to_open()
                    hrs  = mins // 60
                    if ADMIN_CHAT_ID:
                        _alang = _get_user_lang(ADMIN_CHAT_ID)
                        send_telegram_message(
                            ADMIN_CHAT_ID,
                            t(
                                'main_market_closed_notify',
                                _alang,
                                hours=hrs,
                                minutes=mins % 60,
                            ),
                        )
                    _closed_notified = True
                    print(f"[MARKET] Closed — opens in ~{mins} min")

                run_watcher()   # monitors all users' open positions
                time.sleep(300)
                continue

            # ── PRE_MARKET / AFTER_HOURS: monitor only, no new signals ────────
            if market_status != STATUS_OPEN:
                print(f"[{market_status}] Monitor-only cycle")
                run_watcher()
                time.sleep(CHECK_INTERVAL)
                continue

            # ── Market is OPEN ────────────────────────────────────────────────

            # Fallback: if pre-open scan did not run for any reason,
            # run once after open.
            if _last_scan_date != today:
                _watchlist      = run_daily_scan()
                _last_scan_date = today
                _unsupported_all_day.clear()
                if _watchlist:
                    pretrain_models(_watchlist)
                _last_watchlist_refresh_at = now_utc

            # Hourly in-session refresh: captures intraday liquidity/volatility shifts.
            if (_last_watchlist_refresh_at is None
                    or (now_utc - _last_watchlist_refresh_at).total_seconds() >= WATCHLIST_REFRESH_SECONDS):
                print("[WATCHLIST] Hourly refresh during open session...")
                _watchlist = run_daily_scan()
                if _watchlist:
                    pretrain_models(_watchlist)
                _last_watchlist_refresh_at = now_utc

            if not _watchlist:
                print("⏳ في انتظار المسح اليومي...")
                time.sleep(CHECK_INTERVAL)
                continue

            # Monitor open positions for ALL active subscribers
            run_watcher()

            print(f"\n[SCAN] {len(_watchlist)} symbols | {len(get_trading_subscribers())} trading / {len(get_all_active_subscribers())} total subscribers")

            scan_started = time.time()
            print(f"[SCAN START] symbols={len(_watchlist)}")

            # Parallel scan: aiohttp + asyncio (bounded Capital HTTP concurrency; see signal_engine).
            # Returns best-per-symbol signals with timeframes attached (no execution).
            signals = scan_watchlist_parallel(
                _watchlist,
                min_confidence=active_min_conf,
                max_workers=8,
            )

            duration = time.time() - scan_started
            print(f"[SCAN END] duration={duration:.1f} sec | signals_found={len(signals)}")

            if not signals:
                print("   [SCAN] No signals found this cycle.")
                _log_scan_cycle_summary(
                    scanned_symbols=len(_watchlist),
                    total_signals=0,
                    rejected_signals=0,
                    accepted_signals=0,
                    ai_blocked=0,
                )
            else:
                rej_sent = 0
                rej_suppressed = 0
                rej_counter = Counter()
                accepted_count = 0
                ai_blocked_count = 0
                for sig in sorted(signals, key=lambda r: float(r.get("confidence", 0)), reverse=True):
                    symbol = sig["symbol"]
                    if sig.get("rejected"):
                        rej_reason = str(sig.get("reason", "Rejected by market structure filter"))
                        rej_strategy = str(sig.get("strategy_label", "Unknown"))
                        rej_counter[rej_reason] += 1
                        print(f"   [{symbol}] REJECTED | strategy={rej_strategy} | {rej_reason}")
                        if rej_sent < int(STRUCTURAL_REJECTION_NOTIFY_MAX_PER_CYCLE):
                            did_send = _notify_structural_rejection(symbol, rej_strategy, rej_reason)
                            if did_send:
                                rej_sent += 1
                            else:
                                rej_suppressed += 1
                        else:
                            rej_suppressed += 1
                        continue

                    best_action = sig["action"]
                    best_conf = float(sig["confidence"])
                    best_label = sig["strategy_label"]
                    best_reason = sig.get("reason", "")
                    best_sl_pct = sig.get("stop_loss_pct")
                    best_ms_score = sig.get("ms_score")
                    best_score = sig.get("score")
                    timeframes = sig.get("timeframes")

                    print(
                        f"   [{symbol}] {best_action} | "
                        f"strategy={best_label} | "
                        f"confidence={best_conf:.1f}% | {best_reason}"
                    )
                    accepted_count += 1
                    dispatch_result = dispatch_signal(
                        symbol, best_action, best_conf, best_reason,
                        timeframes=timeframes, stop_loss_pct=best_sl_pct,
                        strategy_label=best_label, ms_score=best_ms_score,
                        signal_score=best_score,
                        mr_fast_bypass=bool(sig.get("mr_fast_bypass")),
                        rsi_15m=(
                            float(sig["rsi_15m"]) if sig.get("rsi_15m") is not None else None
                        ),
                        mom_rsi_15m=(
                            float(sig["mom_rsi_15m"]) if sig.get("mom_rsi_15m") is not None else None
                        ),
                        mom_vol_ratio=(
                            float(sig["mom_vol_ratio"]) if sig.get("mom_vol_ratio") is not None else None
                        ),
                    )
                    if isinstance(dispatch_result, dict) and dispatch_result.get("status") == "ai_blocked":
                        ai_blocked_count += 1

                total_candidates = len(signals)
                rejected_count = total_candidates - accepted_count
                acceptance_ratio = (accepted_count / total_candidates * 100.0) if total_candidates else 0.0
                print(
                    f"[SCAN METRICS] candidates={total_candidates} accepted={accepted_count} "
                    f"rejected={rejected_count} ai_blocked={ai_blocked_count} "
                    f"acceptance_ratio={acceptance_ratio:.1f}%"
                )
                _log_scan_cycle_summary(
                    scanned_symbols=len(_watchlist),
                    total_signals=total_candidates,
                    rejected_signals=rejected_count,
                    accepted_signals=accepted_count,
                    ai_blocked=ai_blocked_count,
                )
                if rej_suppressed > 0:
                    top_reason, top_count = ("N/A", 0)
                    if rej_counter:
                        top_reason, top_count = rej_counter.most_common(1)[0]
                    print(
                        f"[STRUCTURAL] muted_notifications={rej_suppressed} "
                        f"top_reason={top_reason} x{top_count}"
                    )

        except Exception as e:
            print(f"❌ خطأ في المحرك الرئيسي: {e}")
            try:
                print(traceback.format_exc())
            except Exception:
                pass

        print(f"\n⏰ اكتملت الدورة. الانتظار {CHECK_INTERVAL} ثانية...")
        print("=" * 55)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run_trading_bot()
