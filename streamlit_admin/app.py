"""
NATB Admin — Streamlit dashboard (token-gated).

Run from project root:
  streamlit run streamlit_admin/app.py --server.port 8501

Set in .env: ADMIN_TOKEN, TELEGRAM_BOT_TOKEN (for Send Message).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import streamlit as st

st.set_page_config(page_title="NATB Admin", layout="wide")


def _expected_token() -> str:
    return (os.getenv("ADMIN_TOKEN") or "").strip()


def _url_token() -> str:
    try:
        qp = st.query_params
        v = qp.get("token")
    except Exception:
        return ""
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return str(v[0] if v else "")
    return str(v)


def _require_auth() -> None:
    exp = _expected_token()
    if not exp:
        st.error("ADMIN_TOKEN is not set in .env. This dashboard is disabled.")
        st.stop()
    got = _url_token().strip()
    if got != exp:
        st.error("Unauthorized Access")
        st.stop()


_require_auth()

st.title("NATB — Admin")
st.caption("Authenticated via ADMIN_TOKEN")

st.divider()
st.subheader("Send Telegram message")
st.caption("Uses the same `send_telegram_message` path as the bot (Markdown).")

with st.form("tg_send"):
    chat_id_in = st.text_input("User chat_id", placeholder="123456789")
    body = st.text_area("Message", height=160)
    submit = st.form_submit_button("Send")

if submit:
    cid = (chat_id_in or "").strip()
    msg = (body or "").strip()
    if not cid or not msg:
        st.warning("Enter chat_id and message.")
    else:
        try:
            from bot.notifier import send_telegram_message

            send_telegram_message(cid, msg)
            st.success("Message sent.")
        except Exception as exc:
            st.error(f"Send failed: {exc}")

st.divider()
st.subheader("Quick links")
st.markdown(
    "- Telegram bot commands for full admin CLI: `/admin maintenance`, `/admin broadcast`, …\n"
    "- Engine restart from Telegram uses **ENGINE_RESTART_CMD** in `.env`."
)
