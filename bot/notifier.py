import os
import smtplib
from email.mime.text import MIMEText
import requests
from dotenv import load_dotenv

load_dotenv()


def send_telegram_message(chat_id, message):
    """Send a Telegram message to a subscriber."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    url   = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id":    chat_id,
        "text":       message,
        "parse_mode": "Markdown",
    }
    try:
        response = requests.post(url, json=payload)
        return response.json()
    except Exception as e:
        print(f"Telegram send error: {e}")
        return None


def notify_admin_payment(chat_id: str, full_name: str, tier: int, proof_file_id: str):
    """
    Notify admin of a new payment via:
      1. Telegram photo with Approve / Reject inline buttons
      2. Email  (requires SMTP_* env vars)
      3. WhatsApp via Twilio  (requires TWILIO_* env vars)
    """
    tier_label = (
        "Basic Plan — $50/month (3 trades/day, 2x leverage)"
        if tier == 1 else
        "Advanced Plan — $100/month (Unlimited, up to 10x leverage)"
    )

    caption = (
        f"*New Subscription Request*\n\n"
        f"Name: *{full_name}*\n"
        f"Package: *{tier_label}*\n"
        f"Chat ID: `{chat_id}`\n\n"
        f"Approve or reject the payment proof below:"
    )

    _send_admin_photo(chat_id, proof_file_id, caption)
    _send_email_alert(full_name, tier_label, chat_id)
    _send_whatsapp_alert(full_name, tier_label)


def _send_admin_photo(user_chat_id: str, file_id: str, caption: str):
    """Forward payment screenshot to admin with approve/reject buttons."""
    token    = os.getenv('TELEGRAM_BOT_TOKEN', '')
    admin_id = os.getenv('ADMIN_CHAT_ID', '')
    if not (token and admin_id):
        return

    url      = f"https://api.telegram.org/bot{token}/sendPhoto"
    keyboard = {
        "inline_keyboard": [[
            {"text": "✅ Approve", "callback_data": f"approve_payment_{user_chat_id}"},
            {"text": "❌ Reject",  "callback_data": f"reject_payment_{user_chat_id}"},
        ]]
    }
    try:
        requests.post(url, json={
            "chat_id":      admin_id,
            "photo":        file_id,
            "caption":      caption,
            "parse_mode":   "Markdown",
            "reply_markup": keyboard,
        })
    except Exception as e:
        print(f"Admin Telegram photo error: {e}")


def _send_email_alert(full_name: str, tier_label: str, chat_id: str):
    """Send email to admin if SMTP credentials are configured."""
    admin_email = (os.getenv('ADMIN_EMAIL') or '').strip()
    smtp_host   = (os.getenv('SMTP_HOST') or '').strip()
    smtp_port   = int(os.getenv('SMTP_PORT', '587'))
    smtp_user   = (os.getenv('SMTP_USER') or '').strip() or admin_email
    smtp_pass   = (os.getenv('SMTP_PASS') or '').strip()

    if not all([admin_email, smtp_host, smtp_pass]):
        print(
            "[EMAIL] Skipped — set ADMIN_EMAIL, SMTP_HOST, and SMTP_PASS in .env "
            "(for Gmail use an App Password, not your normal login password).",
            flush=True,
        )
        return

    body = (
        f"New NATB subscription payment received.\n\n"
        f"Name:       {full_name}\n"
        f"Package:    {tier_label}\n"
        f"Telegram:   {chat_id}\n\n"
        f"Please review the payment screenshot in Telegram and approve or reject."
    )
    msg = MIMEText(body)
    msg['Subject'] = f"NATB: New Payment — {full_name}"
    msg['From']    = smtp_user
    msg['To']      = admin_email

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, admin_email, msg.as_string())
    except Exception as e:
        print(f"Email notification error: {e}")


def _send_whatsapp_alert(full_name: str, tier_label: str):
    """Send WhatsApp via Twilio if configured (optional)."""
    twilio_sid  = os.getenv('TWILIO_ACCOUNT_SID', '')
    twilio_auth = os.getenv('TWILIO_AUTH_TOKEN', '')
    from_num    = os.getenv('TWILIO_WHATSAPP_FROM', '')
    admin_num   = os.getenv('ADMIN_WHATSAPP', '')

    if not all([twilio_sid, twilio_auth, from_num, admin_num]):
        return

    try:
        from twilio.rest import Client
        client = Client(twilio_sid, twilio_auth)
        client.messages.create(
            from_=f"whatsapp:{from_num}",
            body=(
                f"NATB Payment Alert\n"
                f"Name: {full_name}\n"
                f"Package: {tier_label}\n"
                f"Action required: Check Telegram to approve or reject."
            ),
            to=f"whatsapp:{admin_num}",
        )
    except ImportError:
        print("Twilio not installed. WhatsApp notification skipped.")
    except Exception as e:
        print(f"WhatsApp notification error: {e}")
