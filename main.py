"""
NATB v2.0 — Trading Engine

Run alongside the Telegram bot (bot/dashboard.py) as a separate process.
Both processes share the SQLite database.

Pre-market alert:  sent at 09:00 AM ET (14:00 UTC) daily.
Daily scan:        runs at 09:00 AM ET after the alert.
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
from utils.ai_model import analyze_multi_timeframe, load_or_train_model, validate_signal
from utils.market_hours import get_market_status, minutes_to_open, STATUS_OPEN, STATUS_CLOSED
from utils.daily_report import send_daily_reports
from core.executor import place_trade_for_user, monitor_and_close
from core.watcher import run_watcher, get_all_active_subscribers, get_trading_subscribers
from core.strategy_meanrev  import analyze as analyze_meanrev
from core.strategy_momentum import analyze as analyze_momentum
from bot.notifier import send_telegram_message
from bot.dashboard import post_pending_signal, get_signal_status
from bot.i18n import t

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
CHECK_INTERVAL     = 60     # seconds between live cycles
DAILY_SCAN_HOUR    = 14     # UTC hour for daily scan + pre-market alert
MIN_CONFIDENCE     = 70     # minimum combined RF confidence
MAX_WATCHLIST      = 180    # top-N stocks into Level 3
HYBRID_SIGNAL_TTL  = 600    # seconds before a pending hybrid signal expires (10 min)
HEARTBEAT_INTERVAL = 30     # seconds between heartbeat writes
BACKUP_INTERVAL    = 3600   # seconds between cloud backups (1 hour)
HEARTBEAT_FILE     = "heartbeat.json"

# --- State ---
_watchlist          = []
_last_scan_date     = None
_premarket_sent     = None   # date of last pre-market alert
_daily_report_sent  = None   # date of last daily report
_prev_market_status = None   # detect OPEN→CLOSED transition
_closed_notified    = False  # sent "market closed" msg this session


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
    Skips when the market is CLOSED (weekends, holidays, off-hours) — no new trades
    means no new data worth snapshotting."""
    time.sleep(60)  # let main process stabilise before first backup
    while True:
        try:
            from utils.market_hours import get_market_status, STATUS_CLOSED
            if get_market_status() == STATUS_CLOSED:
                print("[BACKUP] Skipped — market is CLOSED (no new data to snapshot).")
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
                "SELECT signal_id, chat_id, symbol, action, confidence "
                "FROM pending_signals WHERE status='APPROVED'"
            )
            approved = c.fetchall()
            conn.close()

            for signal_id, chat_id, symbol, action, confidence in approved:
                # Mark PROCESSING atomically to prevent duplicate execution
                with sqlite3.connect(db_path) as cx:
                    cx.execute(
                        "UPDATE pending_signals SET status='PROCESSING' WHERE signal_id=? AND status='APPROVED'",
                        (signal_id,),
                    )
                try:
                    result = place_trade_for_user(chat_id, symbol, action, confidence=float(confidence))
                    print(f"   [HYBRID {chat_id}] Signal #{signal_id} → {result}")
                except Exception as exc:
                    print(f"   [HYBRID {chat_id}] Signal #{signal_id} execution error: {exc}")
                    with sqlite3.connect(db_path) as cx:
                        cx.execute(
                            "UPDATE pending_signals SET status='ERROR' WHERE signal_id=?",
                            (signal_id,),
                        )

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
                    timeframes: dict = None, stop_loss_pct: float = None):
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
        if not ai_approved:
            print(
                f"   [AI BLOCK] {symbol} {action} — "
                f"probability {ai_prob:.1f}% < 70% | regime={regime}"
            )
            return
        print(f"   [AI OK] {symbol} {action} — probability={ai_prob:.1f}% | regime={regime}")

    # ── Iterate only subscribers who have started their trading engine ─────────
    subscribers = get_trading_subscribers()
    if not subscribers:
        print(f"   [DISPATCH] No subscribers with trading enabled — skipping signal.")
        return

    dispatched = 0
    for row in subscribers:
        chat_id = str(row[0])
        try:
            mode = _get_user_mode(chat_id)

            if mode == 'AUTO':
                result = place_trade_for_user(
                    chat_id, symbol, action,
                    confidence=confidence, stop_loss_pct=stop_loss_pct,
                )
                print(f"   [AUTO  {chat_id}] {symbol} {action} → {result}")
                dispatched += 1

            else:
                # HYBRID: post to DB, non-blocking.
                # _hybrid_approval_loop() executes it when user approves.
                sig_id = post_pending_signal(chat_id, symbol, action, confidence, reason)
                print(f"   [HYBRID {chat_id}] Signal #{sig_id} posted — awaiting approval")
                dispatched += 1

        except Exception as exc:
            print(f"   [ERROR  {chat_id}] dispatch failed: {exc}")

    print(f"   Signal dispatched to {dispatched}/{len(subscribers)} subscribers")


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_trading_bot():
    global _watchlist, _last_scan_date, _prev_market_status, _closed_notified, _daily_report_sent

    print("🚀 NATB v2.0 — محرك التداول الذكي")
    print(f"   الثقة الدنيا: {MIN_CONFIDENCE}% | فحص كل {CHECK_INTERVAL}s")
    print("-" * 55)

    _start_background_threads()

    while True:
        try:
            today         = date.today()
            utc_hour      = datetime.now(timezone.utc).hour
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

            _prev_market_status = market_status

            # ── Maintenance mode ──────────────────────────────────────────────
            if is_maintenance_mode():
                print("🔧 وضع الصيانة نشط — تشغيل المراقب فقط...")
                orphans = run_watcher()
                if orphans:
                    print(f"   🔍 تم استرداد {orphans} صفقة يتيمة.")
                time.sleep(CHECK_INTERVAL)
                continue

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

            # Daily scan + pre-market alert (once per day)
            if _last_scan_date != today and utc_hour >= DAILY_SCAN_HOUR:
                _watchlist      = run_daily_scan()
                _last_scan_date = today
                if _watchlist:
                    pretrain_models(_watchlist)

            if (_premarket_sent != today
                    and utc_hour == DAILY_SCAN_HOUR
                    and _watchlist):
                send_premarket_alert(len(_watchlist))

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
