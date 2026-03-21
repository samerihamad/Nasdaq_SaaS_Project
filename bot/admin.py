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
"""

import os
import sqlite3
from telegram import Update
from telegram.ext import ContextTypes

from database.db_manager import (
    is_maintenance_mode, set_maintenance_mode,
    is_master_kill_switch, set_master_kill_switch,
    get_user_kill_switch, set_user_kill_switch,
    get_user_risk_params, get_bank_details, set_bank_field, BANK_FIELDS,
)
from core.watcher import broadcast_to_all, run_watcher, get_all_active_subscribers
from bot.licensing import issue_license
from bot.notifier import send_telegram_message

ADMIN_CHAT_ID = os.getenv('ADMIN_CHAT_ID', '')
DB_PATH       = 'database/trading_saas.db'


def _is_admin(chat_id: str) -> bool:
    return bool(ADMIN_CHAT_ID) and str(chat_id) == str(ADMIN_CHAT_ID)


async def admin_handler(update: Update, context):
    chat_id = str(update.message.chat_id)

    if not _is_admin(chat_id):
        await update.message.reply_text("Access denied.")
        return

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
            "`/admin subscribers`\n"
            "`/admin status`\n"
            "`/admin orphans`\n"
            "`/admin issue <chat_id> <days>`\n"
            "`/admin setbank <field> <value>`\n"
            "`/admin getbank`\n"
            "`/admin payments`",
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
            count = broadcast_to_all(
                "*System Alert — Emergency Maintenance*\n\n"
                "The system is now in maintenance mode.\n"
                "New trade entries are temporarily suspended.\n"
                "Open positions remain fully monitored."
            )
            await update.message.reply_text(
                f"*Maintenance mode ON*\n{count} subscriber(s) notified.",
                parse_mode='Markdown'
            )
        else:
            count = broadcast_to_all(
                "*Normal Trading Resumed*\n\n"
                "Maintenance is complete.\n"
                "The system is operating normally."
            )
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
                "SELECT expiry_date, mode, tier, payment_status, first_name, last_name "
                "FROM subscribers WHERE chat_id=?", (cid,)
            )
            info   = c.fetchone()
            expiry = info[0] if info and info[0] else '—'
            mode   = info[1] if info and info[1] else 'AUTO'
            tier   = info[2] if info and info[2] else 0
            status = info[3] if info and info[3] else 'NONE'
            name   = f"{info[4] or ''} {info[5] or ''}".strip() or '—'
            lines.append(
                f"• `{cid}` | {name} | T{tier} | {status} | exp:{expiry} | {mode}"
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
            """SELECT chat_id, first_name, last_name, tier, payment_status
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
            cid, fn, ln, tier, ps = row
            name = f"{fn or ''} {ln or ''}".strip() or '—'
            lines.append(f"• `{cid}` | {name} | T{tier or 0} | *{ps}*")
        await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')

    else:
        await update.message.reply_text(
            f"Unknown command: `{cmd}`", parse_mode='Markdown'
        )
