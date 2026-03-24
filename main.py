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
from utils.filters import get_nasdaq_tickers, level1_filter, level2_filter
from utils.market_scanner import scan_multi_timeframe
from utils.ai_model import (
    analyze_multi_timeframe,
    load_or_train_model,
    validate_signal,
    AI_PROBABILITY_THRESHOLD,
)
from utils.market_hours import get_market_status, minutes_to_open, STATUS_OPEN, STATUS_CLOSED
from utils.daily_report import send_daily_reports
from core.executor import place_trade_for_user, monitor_and_close, is_symbol_supported_for_user
from core.watcher import run_watcher, get_all_active_subscribers, get_trading_subscribers
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
    watchlist = level2[:MAX_WATCHLIST]
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
    if timeframes:
        ai_approved, ai_prob, regime = validate_signal(symbol, action, timeframes)
        strategy_key = (strategy_label or "RF").strip()
        ai_min_by_strategy = {
            "RF": AI_MIN_PROB_RF,
            "Momentum": AI_MIN_PROB_MOMENTUM,
            "MeanRev": AI_MIN_PROB_MEANREV,
        }
        ai_min_prob = ai_min_by_strategy.get(strategy_key, AI_MIN_PROB_RF)

        # Soft-gate logic:
        # 1) Normal pass: AI probability meets per-strategy threshold.
        # 2) Soft override: very strong technical confidence can pass despite a
        #    lower AI probability, but never in VOLATILE regime.
        ai_pass_by_threshold = ai_prob >= ai_min_prob
        ai_pass_by_override = (
            confidence >= AI_SOFT_OVERRIDE_CONFIDENCE
            and ai_prob >= AI_SOFT_OVERRIDE_MIN_PROB
            and regime != "VOLATILE"
            and strategy_key in ("Momentum", "MeanRev")
        )

        if not (ai_pass_by_threshold or ai_pass_by_override):
            print(
                f"   [AI BLOCK] {symbol} {action} — "
                f"probability {ai_prob:.1f}% < min({strategy_key})={ai_min_prob:.1f}% "
                f"| regime={regime} | conf={confidence:.1f}%"
            )
            return

        if ai_pass_by_override and not ai_pass_by_threshold:
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

    dispatched = 0
    for row in subscribers:
        chat_id = str(row[0])
        try:
            # Pre-filter: only dispatch symbols tradable on this user's broker account.
            if not is_symbol_supported_for_user(chat_id, symbol):
                print(f"   [SKIP  {chat_id}] {symbol} not supported on broker — filtered before dispatch")
                continue

            mode = _get_user_mode(chat_id)

            if mode == 'AUTO':
                result = place_trade_for_user(
                    chat_id, symbol, action,
                    confidence=confidence, stop_loss_pct=stop_loss_pct,
                    strategy_label=strategy_label,
                )
                print(f"   [AUTO  {chat_id}] {symbol} {action} → {result}")
                dispatched += 1

            else:
                # HYBRID: post to DB, non-blocking.
                # _hybrid_approval_loop() executes it when user approves.
                sig_id = post_pending_signal(
                    chat_id, symbol, action, confidence, reason,
                    strategy_label=strategy_label,
                    stop_loss_pct=stop_loss_pct,
                )
                print(f"   [HYBRID {chat_id}] Signal #{sig_id} posted — awaiting approval")
                dispatched += 1

        except Exception as exc:
            print(f"   [ERROR  {chat_id}] dispatch failed: {exc}")

    print(f"   Signal dispatched to {dispatched}/{len(subscribers)} subscribers")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_trading_bot():
    global _watchlist, _last_scan_date, _prev_market_status, _closed_notified, _daily_report_sent, _last_watchlist_refresh_at

    print("🚀 NATB v2.0 — محرك التداول الذكي")
    print(f"   الثقة الدنيا: {MIN_CONFIDENCE}% | فحص كل {CHECK_INTERVAL}s")
    print("-" * 55)

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
                if (_premarket_sent != today
                        and 0 < mins <= PREMARKET_ALERT_WINDOW_MIN
                        and _watchlist):
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

            for symbol in _watchlist:
                try:
                    timeframes = scan_multi_timeframe(symbol)
                    if timeframes is None:
                        continue

                    # Collect all signals that pass their own thresholds.
                    # Each entry: (action, confidence, strategy_label, reason)
                    candidates = []

                    # 1. RF multi-timeframe model (existing)
                    action, confidence, reason = analyze_multi_timeframe(
                        timeframes, symbol=symbol
                    )
                    if action and confidence >= MIN_CONFIDENCE:
                        candidates.append((action, confidence, "RF", reason))

                    # 2. Mean Reversion strategy
                    mr_sig = analyze_meanrev(symbol, timeframes)
                    if mr_sig and mr_sig["confidence"] >= MIN_CONFIDENCE:
                        candidates.append((
                            mr_sig["action"],
                            mr_sig["confidence"],
                            "MeanRev",
                            mr_sig["reason"],
                        ))

                    # 3. Momentum strategy
                    mo_sig = analyze_momentum(symbol, timeframes)
                    if mo_sig and mo_sig["confidence"] >= MIN_CONFIDENCE:
                        candidates.append((
                            mo_sig["action"],
                            mo_sig["confidence"],
                            "Momentum",
                            mo_sig["reason"],
                        ))

                    if not candidates:
                        best_conf = max(
                            [confidence or 0,
                             (mr_sig or {}).get("confidence", 0),
                             (mo_sig or {}).get("confidence", 0)],
                        )
                        print(f"   [{symbol}] No signal ({best_conf:.0f}%)")
                        time.sleep(1)
                        continue

                    # Pick the single highest-confidence signal to avoid
                    # double-entering the same symbol on the same cycle.
                    # Candidates: (action, confidence, label, reason, stop_loss_pct)
                    best = max(candidates, key=lambda x: x[1])
                    best_action, best_conf, best_label, best_reason = best

                    # Retrieve stop_loss_pct from strategy signals
                    best_sl_pct = None
                    if best_label == "MeanRev" and mr_sig:
                        best_sl_pct = mr_sig.get("stop_loss_pct")
                    elif best_label == "Momentum" and mo_sig:
                        best_sl_pct = mo_sig.get("stop_loss_pct")

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

                    time.sleep(1)

                except Exception as e:
                    print(f"❌ خطأ في {symbol}: {e}")
                    traceback.print_exc()

        except Exception as e:
            print(f"❌ خطأ في المحرك الرئيسي: {e}")

        print(f"\n⏰ اكتملت الدورة. الانتظار {CHECK_INTERVAL} ثانية...")
        print("=" * 55)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run_trading_bot()
