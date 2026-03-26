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
from datetime import datetime, date, timezone

from database.db_manager import create_db, is_maintenance_mode
from utils.filters import get_nasdaq_tickers, level1_filter, level2_filter, level3_filter
from utils.market_scanner import scan_multi_timeframe
from utils.ai_model import (
    analyze_multi_timeframe,
    load_or_train_model,
    validate_signal,
    AI_PROBABILITY_THRESHOLD,
)
from utils.market_hours import get_market_status, minutes_to_open, STATUS_OPEN, STATUS_CLOSED
from utils.daily_report import send_daily_reports
from core.executor import place_trade_for_user, monitor_and_close
from core.watcher import run_watcher, get_all_active_subscribers, get_trading_subscribers
from core.signal_engine import scan_watchlist_parallel
from core.strategy_meanrev  import analyze as analyze_meanrev
from core.strategy_momentum import analyze as analyze_momentum
from core.risk_manager import get_risk_state, STATE_USER_DAY_HALT
from bot.notifier import send_telegram_message
from bot.dashboard import post_pending_signal, get_signal_status
from bot.i18n import t
from database.db_manager import set_trading_enabled
from config import (
    MIN_CONFIDENCE,
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
    ENABLE_STRUCTURAL_REJECTION_NOTIFY,
    STRUCTURAL_REJECTION_NOTIFY_COOLDOWN_SEC,
    STRUCTURAL_REJECTION_NOTIFY_MAX_PER_CYCLE,
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

# --- Configuration ---
ADMIN_CHAT_ID      = os.getenv("ADMIN_CHAT_ID", "")   # admin-only notifications

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


# ── Heartbeat & backup threads ────────────────────────────────────────────────

def _heartbeat_loop():
    """Daemon thread: writes heartbeat.json every HEARTBEAT_INTERVAL seconds."""
    while True:
        try:
            with open(HEARTBEAT_FILE, 'w') as f:
                json.dump({'timestamp': datetime.now().isoformat()}, f)
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


def _start_background_threads():
    for target, name in [
        (_heartbeat_loop,       "heartbeat"),
        (_backup_loop,          "backup"),
        (_hybrid_approval_loop, "hybrid-approvals"),
    ]:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        print(f"   Thread '{name}' started.")


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
        return date.fromisoformat(expiry_date) >= date.today()
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
    print(f"🌅 المسح اليومي الشامل — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")

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
    print(f"\n🤖 تجهيز نماذج RF لـ {len(watchlist)} سهم...")
    for i, symbol in enumerate(watchlist, 1):
        print(f"   [{i}/{len(watchlist)}] {symbol}", end="\r")
        load_or_train_model(symbol)
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

    _premarket_sent = date.today()
    print("Pre-market alert sent to all subscribers (localized).")


# ── Signal dispatch ───────────────────────────────────────────────────────────

def dispatch_signal(symbol: str, action: str, confidence: float, reason: str,
                    timeframes: dict = None, stop_loss_pct: float = None, strategy_label: str = None):
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
    if timeframes:
        strategy_key = (strategy_label or "RF").strip()
        ai_min_by_strategy = {
            "RF": AI_MIN_PROB_RF,
            "Momentum": AI_MIN_PROB_MOMENTUM,
            "MeanRev": AI_MIN_PROB_MEANREV,
        }
        ai_min_prob = ai_min_by_strategy.get(strategy_key, AI_MIN_PROB_RF)
        ai_approved, ai_prob, regime = validate_signal(
            symbol, action, timeframes, min_probability=ai_min_prob
        )

        # Soft override (Momentum/MeanRev): no VOLATILE block — see config AI_SOFT_OVERRIDE_*
        ai_override = (
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

        if not (ai_approved or ai_override):
            print(
                f"[AI BLOCK] {symbol} {action} | "
                f"prob={float(ai_prob):.1f} < min={float(ai_min_prob):.1f} | "
                f"conf={float(confidence):.1f} | strategy={strategy_key} | regime={regime}"
            )
            return

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
        return
    if symbol in _unsupported_all_day:
        print(f"   [DISPATCH] {symbol} marked unsupported-for-all today — skipping.")
        return

    attempted = 0
    opened = 0
    skipped = 0
    failed = 0
    unsupported_for_all = True
    for row in subscribers:
        chat_id = str(row[0])
        try:
            mode = _get_user_mode(chat_id)

            if mode == 'AUTO':
                attempted += 1
                result = place_trade_for_user(
                    chat_id, symbol, action,
                    confidence=confidence, stop_loss_pct=stop_loss_pct,
                    strategy_label=strategy_label,
                )
                print(f"   [AUTO  {chat_id}] {symbol} {action} → {result}")
                if isinstance(result, str) and result.startswith("✅ Opened"):
                    opened += 1
                    unsupported_for_all = False
                    print(f"[TRADE OPENED] {symbol} {action}")
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


def _notify_structural_rejection(symbol: str, strategy: str, reason: str) -> bool:
    if not ENABLE_STRUCTURAL_REJECTION_NOTIFY:
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
            return True
    except Exception:
        return False
    return False


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_trading_bot():
    global _watchlist, _last_scan_date, _prev_market_status, _closed_notified, _daily_report_sent, _last_watchlist_refresh_at, _unsupported_all_day

    print("🚀 NATB v2.0 — محرك التداول الذكي")
    print(f"   الثقة الدنيا: {MIN_CONFIDENCE}% | فحص كل {CHECK_INTERVAL}s")
    print("-" * 55)

    # Telegram health ping (helps detect missing token / blocked bot early)
    try:
        if not ADMIN_CHAT_ID:
            print("[Telegram] ADMIN_CHAT_ID is not set; startup ping skipped.", flush=True)
        elif not is_maintenance_mode():
            now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            send_telegram_message(
                ADMIN_CHAT_ID,
                f"✅ Engine started\n🕒 {now_utc}",
            )
    except Exception as exc:
        print(f"[Telegram] Startup ping failed: {exc}", flush=True)

    _start_background_threads()

    while True:
        try:
            now_utc       = datetime.now(timezone.utc)
            today         = date.today()
            market_status = get_market_status()

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

            # Parallel scan: eliminate per-symbol sleeps and IO bottlenecks.
            # Returns best-per-symbol signals with timeframes attached (no execution).
            signals = scan_watchlist_parallel(
                _watchlist,
                min_confidence=MIN_CONFIDENCE,
                max_workers=8,
            )

            duration = time.time() - scan_started
            print(f"[SCAN END] duration={duration:.1f} sec | signals_found={len(signals)}")

            if not signals:
                print("   [SCAN] No signals found this cycle.")
            else:
                rej_sent = 0
                rej_suppressed = 0
                for sig in sorted(signals, key=lambda r: float(r.get("confidence", 0)), reverse=True):
                    symbol = sig["symbol"]
                    if sig.get("rejected"):
                        rej_reason = str(sig.get("reason", "Rejected by market structure filter"))
                        rej_strategy = str(sig.get("strategy_label", "Unknown"))
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
                    timeframes = sig.get("timeframes")

                    print(
                        f"   [{symbol}] {best_action} | "
                        f"strategy={best_label} | "
                        f"confidence={best_conf:.1f}% | {best_reason}"
                    )
                    dispatch_signal(
                        symbol, best_action, best_conf, best_reason,
                        timeframes=timeframes, stop_loss_pct=best_sl_pct,
                        strategy_label=best_label,
                    )
                if rej_suppressed > 0 and ADMIN_CHAT_ID and ENABLE_STRUCTURAL_REJECTION_NOTIFY:
                    try:
                        lang = _get_user_lang(ADMIN_CHAT_ID)
                        if lang == "ar":
                            send_telegram_message(
                                ADMIN_CHAT_ID,
                                f"ℹ️ تم كتم {rej_suppressed} إشعار رفض هيكلي متكرر في هذه الدورة لتقليل الإزعاج.",
                            )
                        else:
                            send_telegram_message(
                                ADMIN_CHAT_ID,
                                f"ℹ️ Suppressed {rej_suppressed} repeated structural rejection alerts in this cycle.",
                            )
                    except Exception:
                        pass

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
