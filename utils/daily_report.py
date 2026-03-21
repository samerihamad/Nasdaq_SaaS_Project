"""
Daily P&L Report — NATB v2.0

Generates and sends an end-of-day summary to every active subscriber.
Called automatically when the market transitions from OPEN → AFTER_HOURS.

Report includes:
  - Trades closed today: count, wins, losses, net P&L
  - Currently open positions with live UPL
  - Account balance (fetched from Capital.com)
"""

import sqlite3
import requests
from datetime import date

from bot.notifier import send_telegram_message

DB_PATH = 'database/trading_saas.db'


# ── Bilingual report strings ──────────────────────────────────────────────────

_STRINGS = {
    'title': {
        'ar': '📊 *التقرير اليومي — {date}*',
        'en': '📊 *Daily Report — {date}*',
    },
    'no_activity': {
        'ar': '📭 لا توجد صفقات مغلقة اليوم.',
        'en': '📭 No closed trades today.',
    },
    'closed_section': {
        'ar': (
            '\n*الصفقات المغلقة اليوم:*\n'
            '• العدد الإجمالي: {total}\n'
            '• ✅ رابحة: {wins} | ❌ خاسرة: {losses}\n'
            '• 💵 صافي اليوم: *{sign}{net:.2f}$*'
        ),
        'en': (
            '\n*Closed Trades Today:*\n'
            '• Total: {total}\n'
            '• ✅ Wins: {wins} | ❌ Losses: {losses}\n'
            '• 💵 Net P&L: *{sign}{net:.2f}$*'
        ),
    },
    'open_section_title': {
        'ar': '\n*الصفقات المفتوحة حالياً:*',
        'en': '\n*Currently Open Positions:*',
    },
    'open_row': {
        'ar': '  • {symbol} ({dir}) | UPL: {sign}{upl:.2f}$',
        'en': '  • {symbol} ({dir}) | UPL: {sign}{upl:.2f}$',
    },
    'no_open': {
        'ar': '  لا توجد صفقات مفتوحة.',
        'en': '  No open positions.',
    },
    'balance_row': {
        'ar': '\n💰 رصيد الحساب: *{balance:.2f} {currency}*',
        'en': '\n💰 Account Balance: *{balance:.2f} {currency}*',
    },
    'footer': {
        'ar': '\n_NATB v2.0 — تقرير يومي آلي_',
        'en': '\n_NATB v2.0 — Automated Daily Report_',
    },
}


def _t(key: str, lang: str, **kwargs) -> str:
    text = _STRINGS.get(key, {}).get(lang) or _STRINGS.get(key, {}).get('ar', key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    return text


# ── Balance fetch ─────────────────────────────────────────────────────────────

def _fetch_balance(chat_id: str) -> tuple[float, str]:
    """Returns (balance, currency) or (0.0, 'USD') on failure."""
    try:
        from core.executor import get_user_credentials, get_session
        creds = get_user_credentials(chat_id)
        if not creds:
            return 0.0, 'USD'
        base_url, headers = get_session(creds)
        if not headers:
            return 0.0, 'USD'
        res = requests.get(f"{base_url}/accounts", headers=headers)
        if res.status_code == 200:
            acc  = res.json()['accounts'][0]['balance']
            return float(acc['balance']), acc.get('currency', 'USD')
    except Exception:
        pass
    return 0.0, 'USD'


def _fetch_open_positions(chat_id: str) -> list[dict]:
    """Returns list of {symbol, direction, upl} from Capital.com."""
    try:
        from core.executor import get_user_credentials, get_session
        creds = get_user_credentials(chat_id)
        if not creds:
            return []
        base_url, headers = get_session(creds)
        if not headers:
            return []
        res = requests.get(f"{base_url}/positions", headers=headers)
        if res.status_code == 200:
            return [
                {
                    'symbol': p['market']['instrumentName'],
                    'direction': p['position']['direction'],
                    'upl': float(p['position']['upl']),
                }
                for p in res.json().get('positions', [])
            ]
    except Exception:
        pass
    return []


# ── Report builder ────────────────────────────────────────────────────────────

def build_report(chat_id: str, lang: str) -> str:
    today_str = str(date.today())
    lines     = [_t('title', lang, date=today_str)]

    # ── Closed trades today ───────────────────────────────────────────────────
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT pnl FROM trades WHERE chat_id=? AND status='CLOSED' AND closed_at LIKE ?",
        (chat_id, f"{today_str}%")
    )
    closed_pnls = [row[0] for row in c.fetchall() if row[0] is not None]
    conn.close()

    if closed_pnls:
        wins   = sum(1 for p in closed_pnls if p > 0)
        losses = len(closed_pnls) - wins
        net    = sum(closed_pnls)
        lines.append(_t('closed_section', lang,
                        total=len(closed_pnls),
                        wins=wins,
                        losses=losses,
                        sign='+' if net >= 0 else '',
                        net=net))
    else:
        lines.append(_t('no_activity', lang))

    # ── Open positions ────────────────────────────────────────────────────────
    lines.append(_t('open_section_title', lang))
    positions = _fetch_open_positions(chat_id)
    if positions:
        for p in positions:
            lines.append(_t('open_row', lang,
                            symbol=p['symbol'],
                            dir=p['direction'],
                            sign='+' if p['upl'] >= 0 else '',
                            upl=abs(p['upl'])))
    else:
        lines.append(_t('no_open', lang))

    # ── Account balance ───────────────────────────────────────────────────────
    balance, currency = _fetch_balance(chat_id)
    if balance > 0:
        lines.append(_t('balance_row', lang, balance=balance, currency=currency))

    lines.append(_t('footer', lang))
    return '\n'.join(lines)


# ── Dispatcher ────────────────────────────────────────────────────────────────

def send_daily_reports() -> int:
    """
    Build and send the daily report to every active subscriber.
    Returns the number of reports sent.
    """
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    c.execute(
        "SELECT chat_id, lang FROM subscribers WHERE is_active=1 AND email IS NOT NULL"
    )
    subscribers = c.fetchall()
    conn.close()

    sent = 0
    for chat_id, lang in subscribers:
        try:
            report = build_report(chat_id, lang or 'ar')
            send_telegram_message(chat_id, report)
            sent += 1
        except Exception as e:
            print(f"[DAILY REPORT] Error for {chat_id}: {e}")

    print(f"[DAILY REPORT] Sent to {sent} subscriber(s).")
    return sent
