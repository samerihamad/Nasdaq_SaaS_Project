"""
Admin Control Panel — NATB v2.0

All commands require the sender's chat_id to match ADMIN_CHAT_ID in .env.

Commands:
  /admin maintenance on|off      — toggle global maintenance mode
  /admin killswitch on|off       — stop ALL users globally
  /admin killuser <chat_id>      — halt a specific user
  /admin reviveuser <chat_id>    — re-enable a specific user
  /admin riskset <chat_id> <min%> <max%>
  /admin broadcast <message>     — push message to all subscribers
  /admin subscribers             — list all active subscribers
  /admin status                  — system-wide snapshot
  /admin orphans                 — manually trigger orphan scan
  /admin issue <chat_id> <days>  — generate and assign a license key
  /admin setbank <field> <value> — update a bank detail field
  /admin getbank                 — display current bank details
  /admin payments                — list pending payment approvals
  /admin audit_sync [chat_id] [fix] — DB vs Capital position audit (alias: /audit_sync)
"""

import os
import sqlite3
from datetime import datetime, timezone
import requests
from telegram import Update
from telegram.ext import ContextTypes

from database.db_manager import (
    DB_PATH,
    is_maintenance_mode, set_maintenance_mode,
    is_master_kill_switch, set_master_kill_switch,
    get_user_kill_switch, set_user_kill_switch,
    get_user_risk_params, get_bank_details, set_bank_field, BANK_FIELDS,
    touch_bot_activity,
)
from core.watcher import broadcast_to_all, run_watcher, get_all_active_subscribers
from core.executor import get_user_credentials, get_session, resolve_epic_for_user
from utils.market_hours import get_market_status, STATUS_OPEN
from bot.licensing import issue_license
from bot.notifier import send_telegram_message
from bot.i18n import t
from config import (
    LIMIT_ORDER_TTL_BARS,
    LIMIT_ORDER_BAR_MINUTES,
    SIGNAL_PROFILE,
    SIGNAL_MIN_CONFIDENCE,
    SIGNAL_MR_MIN_SCORE,
    SIGNAL_MOM_MIN_SCORE,
    ENABLE_DEEP_DIRECTION_MODEL,
    ENABLE_DEEP_DIRECTION_INFERENCE,
    DEEP_DIRECTION_MODEL_KIND,
    DEEP_DIRECTION_INFERENCE_KIND,
    DEEP_DIRECTION_TIMEFRAME,
    ENABLE_MS_SCORE_AI_INTEGRATION,
    MS_SCORE_AI_SCALE,
    MS_SCORE_AI_MAX_IMPACT,
)

ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID', '')
LOG_ROOT      = os.getenv("ENGINE_LOG_ROOT", "logs")


def _is_admin(chat_id: str) -> bool:
    return bool(ADMIN_CHAT_ID) and str(chat_id) == str(ADMIN_CHAT_ID)


def _fmt_remaining(expires_at: str) -> str:
    try:
        exp = datetime.fromisoformat(str(expires_at))
        now = datetime.now(timezone.utc)
        sec = int((exp - now).total_seconds())
    except Exception:
        return "unknown"
    if sec <= 0:
        return "expired"
    minutes, seconds = divmod(sec, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {seconds}s"


def _bars_remaining(created_at: str, expires_at: str) -> tuple[int, int]:
    try:
        cr = datetime.fromisoformat(str(created_at))
        ex = datetime.fromisoformat(str(expires_at))
        now = datetime.now(timezone.utc)
        total = max(1, int((ex - cr).total_seconds() // max(60, int(LIMIT_ORDER_BAR_MINUTES) * 60)))
        left = int((ex - now).total_seconds() // max(60, int(LIMIT_ORDER_BAR_MINUTES) * 60))
        left = max(0, min(total, left))
        return left, total
    except Exception:
        return 0, int(LIMIT_ORDER_TTL_BARS)


def _safe_strategy_label(strategy_label: str) -> str:
    s = (strategy_label or "").strip().lower()
    if s == "momentum":
        return "⚡ Momentum [0.618 Retrace]"
    if s in ("meanrev", "mean reversion"):
        return "🔄 Mean Reversion"
    return f"📊 {strategy_label or 'Unknown'}"


def _fetch_current_price(uid: str, symbol: str) -> float | None:
    creds = get_user_credentials(str(uid))
    if not creds:
        return None
    base_url, headers = get_session(creds)
    if not headers:
        return None
    epic = resolve_epic_for_user(str(uid), str(symbol), base_url=base_url, headers=headers, is_demo=bool(creds[2]))
    if not epic:
        return None
    try:
        res = requests.get(f"{base_url}/markets/{epic}", headers=headers, timeout=15)
        if res.status_code != 200:
            return None
        snap = (res.json() or {}).get("snapshot") or {}
        bid = snap.get("bid")
        offer = snap.get("offer")
        if bid is not None and offer is not None:
            return (float(bid) + float(offer)) / 2.0
        if bid is not None:
            return float(bid)
        if offer is not None:
            return float(offer)
    except Exception:
        return None
    return None


def _get_lang(chat_id: str) -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT lang FROM subscribers WHERE chat_id=?", (str(chat_id),))
    row = c.fetchone()
    conn.close()
    return row[0] if row and row[0] else 'ar'


def _broadcast_localized(i18n_key: str) -> int:
    """Broadcast a localized i18n message to each active subscriber."""
    count = 0
    for row in get_all_active_subscribers():
        cid = str(row[0])
        lang = _get_lang(cid)
        send_telegram_message(cid, t(i18n_key, lang))
        count += 1
    return count


def _parse_iso_ts(s: str) -> datetime | None:
    try:
        if not s:
            return None
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _build_monitor_panel(window_sec: int = 1800) -> str:
    """
    Admin monitor panel.

    Status logic (all comparisons in UTC):
      - 🟢 Active (Trading) — trading_enabled=1 AND last signal delivered within 1 hour.
      - 🟢 Online           — any activity (bot / engine / signal) within ``window_sec`` (default 30 min).
      - 🔴 Offline          — no recent activity.
    """
    now = datetime.now(timezone.utc)
    signal_active_sec = 3600  # 1 hour window for "Active (Trading)"
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("PRAGMA table_info(subscribers)")
        cols = {str(r[1]) for r in (c.fetchall() or [])}
    except Exception:
        cols = set()

    has_bot = "last_bot_activity_at" in cols
    has_eng = "last_engine_activity_at" in cols
    has_sig = "last_signal_delivered_at" in cols
    has_mode = "mode" in cols

    select = (
        "SELECT chat_id, first_name, last_name, payment_status, trading_enabled, "
        + ("last_bot_activity_at, " if has_bot else "NULL AS last_bot_activity_at, ")
        + ("last_engine_activity_at, " if has_eng else "NULL AS last_engine_activity_at, ")
        + ("last_signal_delivered_at, " if has_sig else "NULL AS last_signal_delivered_at, ")
        + ("mode " if has_mode else "NULL AS mode ")
        + "FROM subscribers "
          "WHERE is_active=1 AND payment_status='APPROVED' "
          "AND email IS NOT NULL AND api_key IS NOT NULL "
          "ORDER BY rowid DESC"
    )
    c.execute(select)
    rows = c.fetchall() or []
    conn.close()

    active_count = 0
    online_count = 0
    offline_count = 0
    lines = []
    for cid, fn, ln, ps, te, bot_ts, eng_ts, sig_ts, user_mode in rows:
        name = f"{fn or ''} {ln or ''}".strip() or "—"
        bot_dt = _parse_iso_ts(bot_ts)
        eng_dt = _parse_iso_ts(eng_ts)
        sig_dt = _parse_iso_ts(sig_ts)

        all_dts = [d for d in (bot_dt, eng_dt, sig_dt) if d is not None]
        last_dt = max(all_dts) if all_dts else None
        src = "—"
        if last_dt:
            if last_dt == sig_dt:
                src = "signal"
            elif last_dt == eng_dt:
                src = "engine"
            else:
                src = "bot"

        trading_on = int(te or 0) == 1
        sig_age = (now - sig_dt).total_seconds() if sig_dt else None
        any_age = (now - last_dt).total_seconds() if last_dt else None

        is_active_trading = (
            trading_on
            and sig_age is not None
            and sig_age <= float(signal_active_sec)
        )
        is_online = (
            any_age is not None
            and any_age <= float(window_sec)
        )

        if is_active_trading:
            active_count += 1
            icon = "🟢"
            tag = "Active (Trading)"
        elif is_online:
            online_count += 1
            icon = "🟢"
            tag = "Online"
        else:
            offline_count += 1
            icon = "🔴"
            tag = "Offline"

        mode_tag = ""
        um = str(user_mode or "AUTO").strip().upper()
        if trading_on:
            mode_tag = " [Auto-Trading On]" if um == "AUTO" else " [Hybrid On]"

        last_s = last_dt.strftime("%H:%M UTC") if last_dt else "—"
        lines.append(
            f"{icon} `{cid}` | {name}{mode_tag} | {tag} | last={last_s} ({src})"
        )

    total_online = active_count + online_count
    return (
        "*Admin Live Monitor*\n\n"
        f"Total: *{len(rows)}* | "
        f"Active: *{active_count}* | Online (<={int(window_sec // 60)}m): *{total_online}* | "
        f"Offline: *{offline_count}*\n\n"
        + ("\n".join(lines) if lines else "_No subscribers._")
    )


def _read_last_log_line(path: str) -> str:
    """Read last non-empty line from a text log file."""
    try:
        if not os.path.exists(path):
            return ""
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [ln.strip() for ln in f.readlines() if ln.strip()]
        return lines[-1] if lines else ""
    except Exception:
        return ""


def _build_ai_runtime_panel() -> str:
    """
    Build admin-facing AI runtime snapshot:
    - active profile and thresholds
    - deep inference state
    - latest cycle/telemetry summary from daily logs
    """
    day_dir = os.path.join(LOG_ROOT, datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    cycle_last = _read_last_log_line(os.path.join(day_dir, "engine_cycle.txt"))
    ai_last = _read_last_log_line(os.path.join(day_dir, "ai_telemetry.txt"))
    rej_last = _read_last_log_line(os.path.join(day_dir, "structural_rejections.txt"))

    infer_mode = (
        "Deep -> RF -> Rule"
        if ENABLE_DEEP_DIRECTION_INFERENCE
        else "RF -> Rule (default)"
    )
    deep_train = "enabled" if ENABLE_DEEP_DIRECTION_MODEL else "disabled"
    deep_inf = "enabled" if ENABLE_DEEP_DIRECTION_INFERENCE else "disabled"
    cycle_txt = cycle_last or "No cycle telemetry yet today."
    ai_txt = ai_last or "No AI telemetry row yet today."
    rej_txt = rej_last or "No structural rejection rows yet today."

    return (
        "*AI Runtime Status*\n\n"
        f"Profile: *{SIGNAL_PROFILE}*\n"
        f"Min Confidence: *{float(SIGNAL_MIN_CONFIDENCE):.1f}%*\n"
        f"MR Min Score: *{int(SIGNAL_MR_MIN_SCORE)}* | MOM Min Score: *{int(SIGNAL_MOM_MIN_SCORE)}*\n\n"
        f"Deep Training: *{deep_train}* ({DEEP_DIRECTION_MODEL_KIND}/{DEEP_DIRECTION_TIMEFRAME})\n"
        f"Deep Inference: *{deep_inf}* ({DEEP_DIRECTION_INFERENCE_KIND})\n"
        f"Inference Order: *{infer_mode}*\n\n"
        f"MS->AI Integration: *{'enabled' if ENABLE_MS_SCORE_AI_INTEGRATION else 'disabled'}* "
        f"(scale={float(MS_SCORE_AI_SCALE):.2f}, cap={float(MS_SCORE_AI_MAX_IMPACT):.1f})\n\n"
        f"*Latest Cycle*\n`{cycle_txt}`\n\n"
        f"*Latest AI Telemetry*\n`{ai_txt}`\n\n"
        f"*Latest Structural Rejection*\n`{rej_txt}`"
    )


async def monitor_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only /monitor command (separate from user UI)."""
    uid = str(update.effective_chat.id)
    if not _is_admin(uid):
        await update.message.reply_text("Access denied.")
        return
    try:
        touch_bot_activity(uid)
    except Exception:
        pass
    await update.message.reply_text(_build_monitor_panel(), parse_mode="Markdown")


async def audit_sync_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only /audit_sync — compare DB trades vs Capital /positions."""
    uid = str(update.effective_chat.id)
    if not _is_admin(uid):
        await update.message.reply_text("Access denied.")
        return
    try:
        touch_bot_activity(uid)
    except Exception:
        pass
    args = context.args or []
    fix = any(str(x).lower() == "fix" for x in args)
    cid_parts = [x for x in args if str(x).lower() != "fix"]
    target = cid_parts[0] if cid_parts else None
    from core.sync import run_zombie_trade_audit

    report = run_zombie_trade_audit(chat_id_filter=target, fix=fix)
    await update.message.reply_text(report[:4000])


async def admin_handler(update: Update, context):
    chat_id = str(update.message.chat_id)

    if not _is_admin(chat_id):
        await update.message.reply_text("Access denied.")
        return
    try:
        touch_bot_activity(chat_id)
    except Exception:
        pass

    args = context.args or []
    if not args:
        await update.message.reply_text(
            "*Admin Commands:*\n"
            "`/admin maintenance on|off`\n"
            "`/admin killswitch on|off`\n"
            "`/admin killuser <chat_id>`\n"
            "`/admin reviveuser <chat_id>`\n"
            "`/admin riskset <chat_id> <min%> <max%>`\n"
            "`/admin broadcast <message>`\n"
            "`/admin monitor`\n"
            "`/admin ai`\n"
            "`/admin subscribers`\n"
            "`/admin status`\n"
            "`/admin orphans`\n"
            "`/admin issue <chat_id> <days>`\n"
            "`/admin setbank <field> <value>`\n"
            "`/admin getbank`\n"
            "`/admin payments`\n"
            "`/admin purgeusers CONFIRM`\n"
            "`/admin audit_sync [chat_id] [fix]`",
            parse_mode='Markdown'
        )
        return

    cmd = args[0].lower()

    # ── maintenance on/off ────────────────────────────────────────────────────
    if cmd == 'maintenance':
        if len(args) < 2 or args[1].lower() not in ('on', 'off'):
            await update.message.reply_text("Usage: /admin maintenance on|off")
            return

        activate = args[1].lower() == 'on'
        set_maintenance_mode(activate)

        if activate:
            # Choose message based on whether the market is currently open
            market_is_open = get_market_status() == STATUS_OPEN
            msg_key = (
                'admin_maintenance_on_msg'        # emergency — market is live
                if market_is_open else
                'admin_maintenance_scheduled_msg' # scheduled — market is closed
            )
            count = _broadcast_localized(msg_key)
            label = "Emergency" if market_is_open else "Scheduled"
            await update.message.reply_text(
                f"*Maintenance mode ON* ({label})\n{count} subscriber(s) notified.",
                parse_mode='Markdown'
            )
        else:
            count = _broadcast_localized('admin_maintenance_off_msg')
            await update.message.reply_text(
                f"*Maintenance mode OFF*\n{count} subscriber(s) notified.",
                parse_mode='Markdown'
            )

    # ── broadcast ─────────────────────────────────────────────────────────────
    elif cmd == 'broadcast':
        if len(args) < 2:
            await update.message.reply_text("Usage: /admin broadcast <message>")
            return
        message = ' '.join(args[1:])
        count   = broadcast_to_all(f"*Message from Admin*\n\n{message}")
        await update.message.reply_text(f"Sent to {count} subscriber(s).")

    # ── purge ALL subscribers (broadcast first) ───────────────────────────────
    elif cmd in ('purgeusers', 'purge', 'resetusers'):
        # Safety: require explicit confirmation token.
        if len(args) < 2 or args[1].strip().upper() != "CONFIRM":
            await update.message.reply_text(
                "Usage: /admin purgeusers CONFIRM\n\n"
                "This will broadcast a final reset notice, then DELETE ALL rows from `subscribers`."
            )
            return

        ar = (
            "⚠️ تنبيه هام: تم تحديث النظام بالكامل إلى النسخة المؤسساتية (Institutional). "
            "تم إعادة ضبط قاعدة البيانات، يرجى إعادة التسجيل وتفعيل اشتراكك الآن للاستفادة من الفلاتر والمميزات الجديدة."
        )
        en = (
            "⚠️ Important: The system has been upgraded to the Institutional Version. "
            "Database reset complete. Please re-register to activate your subscription and access new features."
        )
        msg = f"{ar}\n\n{en}"

        # 1) broadcast
        sent = broadcast_to_all(msg)

        # 2) purge subscribers
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute("DELETE FROM subscribers")
            conn.commit()
            conn.close()
        except Exception as exc:
            await update.message.reply_text(f"Broadcast sent to {sent}. Purge FAILED: {exc}")
            return

        await update.message.reply_text(f"Broadcast sent to {sent}. Purged ALL subscribers successfully.")

    # ── monitor snapshot ──────────────────────────────────────────────────────
    elif cmd in ('monitor', 'panel', 'adminpanel'):
        text = _build_monitor_panel()
        await update.message.reply_text(text, parse_mode='Markdown')

    # ── AI runtime status ─────────────────────────────────────────────────────
    elif cmd in ('ai', 'aistatus', 'ai_status'):
        await update.message.reply_text(_build_ai_runtime_panel(), parse_mode='Markdown')

    # ── DB vs Capital zombie audit ────────────────────────────────────────────
    elif cmd in ('audit_sync', 'auditsync'):
        rest = args[1:]
        fix = any(str(x).lower() == "fix" for x in rest)
        cid_parts = [x for x in rest if str(x).lower() != "fix"]
        target = cid_parts[0] if cid_parts else None
        from core.sync import run_zombie_trade_audit

        report = run_zombie_trade_audit(chat_id_filter=target, fix=fix)
        await update.message.reply_text(report[:4000])

    # ── subscribers list ──────────────────────────────────────────────────────
    elif cmd == 'subscribers':
        subs = get_all_active_subscribers()
        if not subs:
            await update.message.reply_text("No active subscribers.")
            return

        conn  = sqlite3.connect(DB_PATH)
        c     = conn.cursor()
        lines = [f"*Active Subscribers ({len(subs)}):*\n"]
        for row in subs:
            cid = row[0]
            c.execute(
                "SELECT expiry_date, mode, payment_status, first_name, last_name "
                "FROM subscribers WHERE chat_id=?", (cid,)
            )
            info   = c.fetchone()
            expiry = info[0] if info and info[0] else '—'
            mode   = info[1] if info and info[1] else 'AUTO'
            status = info[2] if info and info[2] else 'NONE'
            name   = f"{info[3] or ''} {info[4] or ''}".strip() or '—'
            lines.append(
                f"• `{cid}` | {name} | {status} | exp:{expiry} | {mode}"
            )
        conn.close()
        await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')

    # ── system status ─────────────────────────────────────────────────────────
    elif cmd == 'status':
        maintenance = is_maintenance_mode()

        conn = sqlite3.connect(DB_PATH)
        c    = conn.cursor()
        c.execute("SELECT COUNT(*) FROM subscribers WHERE is_active=1 AND email IS NOT NULL")
        total_subs    = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM subscribers WHERE payment_status='PENDING'")
        pending_pay   = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'")
        open_trades   = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED'")
        closed_trades = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM pending_signals WHERE status='PENDING'")
        pending_sigs  = c.fetchone()[0]
        conn.close()

        status_icon = "MAINTENANCE" if maintenance else "LIVE"
        await update.message.reply_text(
            f"*NATB System Status*\n\n"
            f"Status: *{status_icon}*\n"
            f"Active Subscribers: *{total_subs}*\n"
            f"Pending Payments: *{pending_pay}*\n"
            f"Open Trades: *{open_trades}*\n"
            f"Closed Trades: *{closed_trades}*\n"
            f"Pending Signals: *{pending_sigs}*",
            parse_mode='Markdown'
        )

    # ── orphan scan ───────────────────────────────────────────────────────────
    elif cmd == 'orphans':
        await update.message.reply_text("Scanning for orphaned trades...")
        recovered = run_watcher()
        await update.message.reply_text(
            f"Scan complete.\nRecovered *{recovered}* orphaned trade(s).",
            parse_mode='Markdown'
        )

    # ── issue license ─────────────────────────────────────────────────────────
    elif cmd == 'issue':
        if len(args) < 3:
            await update.message.reply_text(
                "Usage: /admin issue <chat\\_id> <days>", parse_mode='Markdown'
            )
            return
        try:
            target_id = args[1]
            days      = int(args[2])

            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (target_id,)
            )
            conn.commit()
            conn.close()

            key = issue_license(target_id, days)

            # Mark payment as approved so the user proceeds to API creds
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "UPDATE subscribers SET payment_status='APPROVED' WHERE chat_id=?",
                (target_id,)
            )
            conn.commit()
            conn.close()

            await update.message.reply_text(
                f"*License Issued*\n"
                f"Subscriber: `{target_id}`\n"
                f"Validity: {days} days\n"
                f"Key: `{key}`",
                parse_mode='Markdown'
            )
            send_telegram_message(
                target_id,
                f"*Your NATB license is active!*\n\n"
                f"Validity: *{days} days*\n"
                f"License Key: `{key}`\n\n"
                f"Send /start to continue."
            )
        except ValueError:
            await update.message.reply_text("Days must be an integer.")
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")

    # ── Master kill switch ─────────────────────────────────────────────────────
    elif cmd == 'killswitch':
        if len(args) < 2 or args[1].lower() not in ('on', 'off'):
            await update.message.reply_text("Usage: /admin killswitch on|off")
            return

        activate = args[1].lower() == 'on'
        set_master_kill_switch(activate)

        if activate:
            count = broadcast_to_all(
                "*MASTER KILL SWITCH ACTIVATED*\n\n"
                "All new trade entries have been halted by the admin.\n"
                "Open positions remain active until TP / SL."
            )
            await update.message.reply_text(
                f"*Kill Switch: ON*\n{count} subscriber(s) notified.",
                parse_mode='Markdown',
            )
        else:
            count = broadcast_to_all(
                "*Trading Resumed*\n\n"
                "The kill switch has been deactivated.\n"
                "Normal trading has resumed."
            )
            await update.message.reply_text(
                f"*Kill Switch: OFF*\n{count} subscriber(s) notified.",
                parse_mode='Markdown',
            )

    # ── Per-user kill switch ───────────────────────────────────────────────────
    elif cmd == 'killuser':
        if len(args) < 2:
            await update.message.reply_text("Usage: /admin killuser <chat_id>")
            return
        target = args[1]
        set_user_kill_switch(target, True)
        send_telegram_message(
            target,
            "*Your trading session has been halted by the admin.*\n"
            "Open positions remain active. Contact support for details."
        )
        await update.message.reply_text(
            f"User `{target}` halted.", parse_mode='Markdown'
        )

    elif cmd == 'reviveuser':
        if len(args) < 2:
            await update.message.reply_text("Usage: /admin reviveuser <chat_id>")
            return
        target = args[1]
        set_user_kill_switch(target, False)
        send_telegram_message(
            target,
            "*Your trading session has been reactivated.*\n"
            "Normal automated trading has resumed."
        )
        await update.message.reply_text(
            f"User `{target}` reactivated.", parse_mode='Markdown'
        )

    # ── Per-user risk settings ─────────────────────────────────────────────────
    elif cmd == 'riskset':
        if len(args) < 4:
            await update.message.reply_text(
                "Usage: /admin riskset <chat\\_id> <min%> <max%>",
                parse_mode='Markdown',
            )
            return
        try:
            target = args[1]
            min_r  = float(args[2])
            max_r  = float(args[3])
            if not (0 < min_r <= max_r <= 5.0):
                raise ValueError("Invalid range — must be 0 < min <= max <= 5.0")
            with sqlite3.connect(DB_PATH) as cx:
                cx.execute(
                    "UPDATE subscribers SET risk_percent=?, max_risk_percent=? WHERE chat_id=?",
                    (min_r, max_r, target),
                )
            await update.message.reply_text(
                f"Risk updated for `{target}`: {min_r}% – {max_r}%",
                parse_mode='Markdown',
            )
        except ValueError as exc:
            await update.message.reply_text(f"Invalid values: {exc}")

    # ── Bank details: set a field ──────────────────────────────────────────────
    elif cmd == 'setbank':
        if len(args) < 3:
            fields = ', '.join(f.replace('BANK_', '').lower() for f in BANK_FIELDS)
            await update.message.reply_text(
                f"Usage: `/admin setbank <field> <value>`\n"
                f"Fields: `{fields}`\n\n"
                f"Example: `/admin setbank iban AE070260001015434497101`",
                parse_mode='Markdown',
            )
            return

        field_raw = args[1].upper()
        # Accept both 'IBAN' and 'BANK_IBAN'
        field = field_raw if field_raw.startswith('BANK_') else f'BANK_{field_raw}'
        value = ' '.join(args[2:])

        if set_bank_field(field, value):
            await update.message.reply_text(
                f"*{field}* updated to: `{value}`", parse_mode='Markdown'
            )
        else:
            valid = ', '.join(f.replace('BANK_', '').lower() for f in BANK_FIELDS)
            await update.message.reply_text(
                f"Unknown field. Valid fields: `{valid}`", parse_mode='Markdown'
            )

    # ── Bank details: show current values ─────────────────────────────────────
    elif cmd == 'getbank':
        b     = get_bank_details()
        lines = ["*Current Bank Details:*\n"]
        for key, val in b.items():
            short = key.replace('BANK_', '')
            lines.append(f"• *{short}*: `{val}`")
        await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')

    # ── Pending payments list ─────────────────────────────────────────────────
    elif cmd == 'payments':
        conn = sqlite3.connect(DB_PATH)
        c    = conn.cursor()
        c.execute(
            """SELECT chat_id, first_name, last_name, payment_status
               FROM subscribers
               WHERE payment_status IN ('PENDING', 'APPROVED', 'REJECTED')
               ORDER BY rowid DESC LIMIT 20"""
        )
        rows = c.fetchall()
        conn.close()

        if not rows:
            await update.message.reply_text("No payment records found.")
            return

        lines = ["*Recent Payment Records:*\n"]
        for row in rows:
            cid, fn, ln, ps = row
            name = f"{fn or ''} {ln or ''}".strip() or '—'
            lines.append(f"• `{cid}` | {name} | *{ps}*")
        await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')

    else:
        await update.message.reply_text(
            f"Unknown command: `{cmd}`", parse_mode='Markdown'
        )


async def limits_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin-only command:
      /limits
      /orders (alias)
    Show all active pending limit orders with remaining time until expiry.
    """
    chat_id = str(update.message.chat_id)
    if not _is_admin(chat_id):
        await update.message.reply_text("Access denied.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute(
            "SELECT id, chat_id, symbol, action, strategy_label, confidence, ai_prob, "
            "stop_loss_pct, limit_price, created_at, expires_at "
            "FROM pending_limit_orders "
            "WHERE status='PENDING' "
            "ORDER BY expires_at ASC, id ASC"
        )
        rows = c.fetchall()
    except sqlite3.OperationalError:
        conn.close()
        await update.message.reply_text(
            "pending_limit_orders table not found.\n"
            "Restart the engine/bot to apply DB schema.",
        )
        return
    conn.close()

    if not rows:
        await update.message.reply_text("No active pending limit orders at the moment.")
        return

    lines = [f"*Active Pending Limit Orders: {len(rows)}*"]
    for (
        oid, uid, symbol, action, strategy_label, confidence, ai_prob,
        stop_loss_pct, limit_price, created_at, expires_at
    ) in rows[:30]:
        lim = float(limit_price or 0.0)
        cp = _fetch_current_price(str(uid), str(symbol))
        if cp is not None and cp > 0:
            dist_pts = abs(lim - cp)
            dist_pct = (dist_pts / cp) * 100.0
            cp_txt = f"`{cp:.4f}`"
            dist_txt = f"`{dist_pts:.4f}` pts ({dist_pct:.2f}%)"
        else:
            cp_txt = "`N/A`"
            dist_txt = "`N/A`"

        sl_txt = "`N/A`"
        try:
            if stop_loss_pct is not None and lim > 0:
                sl = lim * (1.0 - float(stop_loss_pct)) if str(action) == "BUY" else lim * (1.0 + float(stop_loss_pct))
                sl_txt = f"`{sl:.4f}`"
        except Exception:
            pass

        bars_left, bars_total = _bars_remaining(created_at, expires_at)
        ttl_clock = _fmt_remaining(expires_at)
        side_emoji = "🟢" if str(action) == "BUY" else "🔴"
        ai_val = float(ai_prob) if ai_prob is not None else float(confidence or 0.0)
        lines.append(
            f"\n*#{oid}* `{uid}`\n"
            f"{side_emoji} *{symbol} - {action}*\n"
            f"{_safe_strategy_label(strategy_label)}\n"
            f"• Limit Price: `{lim:.4f}`\n"
            f"• Current Price: {cp_txt}\n"
            f"• Distance to Fill: {dist_txt}\n"
            f"• Confidence: *{float(confidence or 0.0):.1f}%* | AI Prob: *{ai_val:.1f}%*\n"
            f"• Stop Loss: {sl_txt}\n"
            f"• TTL: ⏳ *{bars_left}/{bars_total} Bars remaining* ({ttl_clock})"
        )
    if len(rows) > 30:
        lines.append(f"\n... and {len(rows) - 30} more")

    await update.message.reply_text("\n".join(lines), parse_mode='Markdown')
