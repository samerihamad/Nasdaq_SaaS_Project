import os
import smtplib
from email.mime.text import MIMEText
import requests
from dotenv import load_dotenv
from utils.market_hours import now_uae

load_dotenv()


def _dubai_timestamp() -> str:
    return now_uae().strftime("%Y-%m-%d %H:%M:%S GST")


def _with_dubai_footer(message: str) -> str:
    text = str(message or "")
    # Avoid duplicate footer if caller already added Dubai/GST line.
    if "GST" in text and ("Dubai" in text or "دبي" in text):
        return text
    return f"{text}\n\n🕓 Dubai: {_dubai_timestamp()}"


def send_telegram_message(chat_id, message):
    """Send a Telegram message to a subscriber."""
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        print("[Telegram] Missing TELEGRAM_BOT_TOKEN; message not sent.", flush=True)
        return None
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    stamped_message = _with_dubai_footer(str(message))
    payload = {
        "chat_id":    chat_id,
        "text":       stamped_message,
        "parse_mode": "Markdown",
    }
    try:
        response = requests.post(url, json=payload, timeout=20)
        data = response.json()
        # Telegram can return HTTP 200 with ok=false (e.g., Markdown parse errors).
        if not isinstance(data, dict) or not data.get("ok", False):
            desc = ""
            try:
                desc = str((data or {}).get("description") or "").strip()
            except Exception:
                desc = ""
            print(
                f"[Telegram] Send failed chat_id={chat_id} "
                f"status={response.status_code} desc={desc[:220]}",
                flush=True,
            )
            # Fallback: resend without Markdown formatting to avoid parse errors blocking alerts.
            try:
                payload2 = {"chat_id": chat_id, "text": str(message)}
                payload2["text"] = stamped_message
                response2 = requests.post(url, json=payload2, timeout=20)
                data2 = response2.json()
                if isinstance(data2, dict) and data2.get("ok", False):
                    print(
                        f"[Telegram] Fallback plaintext sent chat_id={chat_id}",
                        flush=True,
                    )
                    return data2
                desc2 = str((data2 or {}).get("description") or "").strip()
                print(
                    f"[Telegram] Fallback failed chat_id={chat_id} "
                    f"status={response2.status_code} desc={desc2[:220]}",
                    flush=True,
                )
            except Exception as e2:
                print(f"Telegram fallback send error: {e2}", flush=True)
            return data
        return data
    except Exception as e:
        print(f"Telegram send error: {e}", flush=True)
        return None


def notify_admin_alert(message: str) -> None:
    """
    Plain-text operational alert to ADMIN_CHAT_ID (no Markdown) so deal IDs and symbols
    do not break parsing.
    """
    admin_id = (os.getenv("ADMIN_CHAT_ID") or "").strip()
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    text = _with_dubai_footer(str(message or ""))[:4000]
    if not text:
        return
    if not admin_id or not token:
        print(f"[Admin alert { _dubai_timestamp() }] {text}", flush=True)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(
            url,
            json={"chat_id": admin_id, "text": text},
            timeout=20,
        )
    except Exception as e:
        print(f"notify_admin_alert error: {e}", flush=True)


def notify_admin_payment(chat_id: str, full_name: str, proof_file_id: str):
    """
    Notify admin of a new payment via:
      1. Telegram photo with Approve / Reject inline buttons
      2. Email  (requires SMTP_* env vars)
      3. WhatsApp via Twilio  (requires TWILIO_* env vars)
    """
    plan_label = "Institutional Plan — $100/month"

    caption = (
        f"*New Subscription Request*\n\n"
        f"Name: *{full_name}*\n"
        f"Plan: *{plan_label}*\n"
        f"Chat ID: `{chat_id}`\n\n"
        f"Approve or reject the payment proof below:"
    )

    _send_admin_photo(chat_id, proof_file_id, caption)
    _send_email_alert(full_name, chat_id)
    _send_whatsapp_alert(full_name)


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


def _send_email_alert(full_name: str, chat_id: str):
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
        f"Plan:       Institutional Plan — $100/month\n"
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


def _send_whatsapp_alert(full_name: str):
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
                f"Plan: Institutional Plan — $100/month\n"
                f"Action required: Check Telegram to approve or reject."
            ),
            to=f"whatsapp:{admin_num}",
        )
    except ImportError:
        print("Twilio not installed. WhatsApp notification skipped.")
    except Exception as e:
        print(f"WhatsApp notification error: {e}")
