"""
Unified Reporter — NATB v3.1

Consolidated reporting system replacing:
  - core/strategy_reporter.py (Phase 6-A Strategic Report)
  - utils/daily_report.py (Legacy Daily P&L)

Single Authority for daily summaries.
Schedule: 20:05 UTC (00:05 UAE) — 5 minutes after market close.
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import ADMIN_CHAT_ID, DB_PATH
from database.db_manager import get_subscriber_lang
from bot.notifier import send_telegram_message

logger = logging.getLogger(__name__)


class UnifiedReporter:
    """
    Single authoritative source for daily trading reports.
    
    Combines:
    - Daily Performance: P&L, Win Rate, Account Balance
    - Strategic Insights: Committee Signal Stats
    - Risk Guardian Status: Circuit Breakers, Drawdown alerts
    """
    
    def __init__(self):
        self.report_date = self._get_local_date()
        self.admin_chat_id = ADMIN_CHAT_ID
        
    # ═════════════════════════════════════════════════════════════════════════════
    # TIMEZONE HANDLING (Env-Driven)
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _get_local_date(self) -> str:
        """
        Return the current ET trading-day date (DST-aware, via pytz).

        Why ET and not UAE?
        Market close is 16:00 ET.  At 16:05 ET the UAE clock reads 00:05 next
        day (EDT) or 01:05 next day (EST), so the old UTC+4 offset returned D+1
        while every trade in the DB was stored against UTC date D.  Using the
        ET date aligns the report date with both the DB timestamps and the
        financial calendar.
        """
        from utils.market_hours import _now_et
        return _now_et().strftime("%Y-%m-%d")

    def _get_local_datetime(self) -> datetime:
        """Return current ET datetime (DST-aware, NTP-corrected via market_hours)."""
        from utils.market_hours import _now_et
        return _now_et()
    
    # ═════════════════════════════════════════════════════════════════════════════
    # DATA COLLECTION
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _load_agent_memory(self) -> dict:
        """Load agent memory data for committee analysis."""
        memory_file = os.path.join(PROJECT_ROOT, "data", "agent_memory.json")
        if not os.path.exists(memory_file):
            return {"experiences": {}, "metadata": {}}
        
        try:
            with open(memory_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[UnifiedReporter] Failed to load agent_memory: {e}")
            return {"experiences": {}, "metadata": {}}
    
    def _get_today_experiences(self, memory_data: dict) -> list:
        """Filter experiences to today's date only."""
        today_str = self._get_local_date()
        experiences = memory_data.get("experiences", {})
        
        today_experiences = []
        for symbol, exp_list in experiences.items():
            for exp in exp_list:
                timestamp = exp.get("timestamp", "")
                if timestamp.startswith(today_str):
                    today_experiences.append(exp)
        
        return today_experiences
    
    def _get_today_trades(self) -> list:
        """Fetch today's trades from database."""
        try:
            today = self._get_local_date()
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                "SELECT trade_id, symbol, action, pnl, status FROM trades "
                "WHERE DATE(created_at)=? OR DATE(closed_at)=?",
                (today, today)
            )
            rows = c.fetchall()
            conn.close()
            return [
                {"trade_id": r[0], "symbol": r[1], "action": r[2], "pnl": r[3], "status": r[4]}
                for r in rows
            ]
        except Exception as e:
            logger.error(f"[UnifiedReporter] Failed to fetch trades: {e}")
            return []
    
    def _get_account_balance(self, chat_id: str) -> Tuple[float, str]:
        """Fetch live account balance from Capital.com."""
        try:
            from core.executor import get_user_credentials, get_session
            creds = get_user_credentials(chat_id)
            if not creds:
                return 0.0, "USD"
            base_url, headers = get_session(creds)
            if not headers:
                return 0.0, "USD"
            
            import requests
            res = requests.get(f"{base_url}/accounts", headers=headers, timeout=10)
            if res.status_code == 200:
                acc = res.json()['accounts'][0]['balance']
                return float(acc['balance']), acc.get('currency', 'USD')
        except Exception as e:
            logger.warning(f"[UnifiedReporter] Balance fetch failed for {chat_id}: {e}")
        return 0.0, "USD"
    
    def _get_open_positions(self, chat_id: str) -> List[dict]:
        """Fetch open positions with UPL from Capital.com."""
        try:
            from core.executor import get_user_credentials, get_session
            import requests
            creds = get_user_credentials(chat_id)
            if not creds:
                return []
            base_url, headers = get_session(creds)
            if not headers:
                return []
            
            res = requests.get(f"{base_url}/positions", headers=headers, timeout=10)
            if res.status_code == 200:
                return [
                    {
                        'symbol': p['market']['instrumentName'],
                        'direction': p['position']['direction'],
                        'upl': float(p['position']['upl']),
                    }
                    for p in res.json().get('positions', [])
                ]
        except Exception as e:
            logger.warning(f"[UnifiedReporter] Positions fetch failed for {chat_id}: {e}")
        return []
    
    def _get_risk_guardian_status(self, chat_id: str) -> Dict[str, Any]:
        """Fetch Risk Guardian state for user."""
        try:
            from core.risk_guardian import RiskGuardianAgent, STATE_NORMAL
            guardian = RiskGuardianAgent()
            state = guardian.get_risk_state(chat_id)
            daily_pnl = guardian._get_daily_pnl(chat_id)
            live_balance = guardian._get_live_balance(chat_id)
            
            loss_pct = 0.0
            if live_balance and live_balance > 0 and daily_pnl < 0:
                loss_pct = abs(daily_pnl) / live_balance * 100
            
            return {
                "state": state,
                "daily_pnl": daily_pnl,
                "loss_pct": loss_pct,
                "is_normal": state == STATE_NORMAL
            }
        except Exception as e:
            logger.error(f"[UnifiedReporter] Risk status fetch failed: {e}")
            return {"state": "UNKNOWN", "daily_pnl": 0.0, "loss_pct": 0.0, "is_normal": True}
    
    # ═════════════════════════════════════════════════════════════════════════════
    # ANALYTICS
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _calculate_performance(self, trades: list) -> Dict[str, Any]:
        """Calculate daily performance metrics."""
        closed_trades = [t for t in trades if t.get("status") == "CLOSED" and t.get("pnl") is not None]
        
        if not closed_trades:
            return {
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "net_pnl": 0.0,
                "breakeven": 0
            }
        
        wins = sum(1 for t in closed_trades if float(t.get("pnl", 0)) > 0)
        losses = sum(1 for t in closed_trades if float(t.get("pnl", 0)) < 0)
        breakeven = sum(1 for t in closed_trades if float(t.get("pnl", 0)) == 0)
        net_pnl = sum(float(t.get("pnl", 0)) for t in closed_trades)
        win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0.0
        
        return {
            "total_trades": len(closed_trades),
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate": win_rate,
            "net_pnl": net_pnl
        }
    
    def _calculate_committee_stats(self, experiences: list) -> Dict[str, Any]:
        """Calculate committee signal statistics."""
        if not experiences:
            return {
                "total_signals": 0,
                "approved": 0,
                "blocked": 0,
                "block_rate": 0.0,
                "execution_rate": 0.0,
                "avg_confidence": 0.0,
                "losses_prevented": 0.0
            }
        
        approved = sum(1 for e in experiences if e.get("verdict") == "APPROVE")
        rejected = sum(1 for e in experiences if e.get("verdict") == "REJECT")
        uncertain = sum(1 for e in experiences if e.get("verdict") == "UNCERTAIN")
        blocked = rejected + uncertain
        
        total = len(experiences)
        block_rate = (blocked / total * 100) if total > 0 else 0
        execution_rate = (approved / total * 100) if total > 0 else 0
        
        avg_confidence = sum(e.get("ai_confidence", 0) for e in experiences) / len(experiences)
        
        # Estimate losses prevented (2% per blocked trade, $1000 avg size)
        est_loss_per_blocked = 0.02
        est_trade_size = 1000
        losses_prevented = blocked * est_trade_size * est_loss_per_blocked
        
        return {
            "total_signals": total,
            "approved": approved,
            "rejected": rejected,
            "uncertain": uncertain,
            "blocked": blocked,
            "block_rate": block_rate,
            "execution_rate": execution_rate,
            "avg_confidence": avg_confidence,
            "losses_prevented": losses_prevented
        }
    
    # ═════════════════════════════════════════════════════════════════════════════
    # REPORT GENERATION
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _generate_risk_section(self, risk_status: dict, lang: str) -> str:
        """Generate Risk Guardian status section."""
        state = risk_status.get("state", "NORMAL")
        loss_pct = risk_status.get("loss_pct", 0.0)
        
        if lang == "en":
            if state == "HARD_BLOCK":
                return (
                    "🚨 **RISK GUARDIAN: HARD BLOCK** 🚨\n"
                    "   └─ Trading locked due to override loss.\n"
                    "   └─ Resumes next trading day."
                )
            elif state == "CIRCUIT_BREAKER":
                return (
                    f"🚨 **RISK GUARDIAN: CIRCUIT BREAKER** 🚨\n"
                    f"   └─ Daily loss: {loss_pct:.2f}%\n"
                    f"   └─ Reply /override to resume with caution."
                )
            elif loss_pct > 2.0:
                return (
                    f"⚠️ **RISK GUARDIAN: DRAWDOWN WARNING**\n"
                    f"   └─ Daily loss: {loss_pct:.2f}% (approaching 3% limit)"
                )
            else:
                return "🟢 **Risk Guardian:** All systems operational."
        else:
            # Arabic
            if state == "HARD_BLOCK":
                return (
                    "🚨 **حارس المخاطر: حظر كامل** 🚨\n"
                    "   └─ التداول مغلق بسبب خسارة تجاوز\n"
                    "   └─ يستأنف في يوم التداول التالي"
                )
            elif state == "CIRCUIT_BREAKER":
                return (
                    f"🚨 **حارس المخاطر: قاطع الدائرة** 🚨\n"
                    f"   └─ الخسارة اليومية: {loss_pct:.2f}%\n"
                    f"   └─ أرسل /override للاستئناف بحذر"
                )
            elif loss_pct > 2.0:
                return (
                    f"⚠️ **حارس المخاطر: تنبيه السحب**\n"
                    f"   └─ الخسارة اليومية: {loss_pct:.2f}% (قرب حد 3%)"
                )
            else:
                return "🟢 **حارس المخاطر:** جميع الأنظمة تعمل"
    
    def _generate_admin_report(self, perf: dict, committee: dict, risk: dict, 
                               balance: float, currency: str, open_pos: list) -> str:
        """Generate comprehensive admin-only report."""
        date_str = self._get_local_date()
        open_count = len(open_pos)
        total_upl = sum(p.get("upl", 0) for p in open_pos)
        
        # Blocking reasons
        block_reasons = []
        if committee.get("rejected", 0) > 0:
            block_reasons.append(f"{committee['rejected']} explicit REJECT")
        if committee.get("uncertain", 0) > 0:
            block_reasons.append(f"{committee['uncertain']} insufficient consensus")
        block_reasons_str = " + ".join(block_reasons) if block_reasons else "N/A"
        
        # Health indicator
        avg_conf = committee.get("avg_confidence", 0)
        if avg_conf >= 70:
            health = "🟢 Excellent"
        elif avg_conf >= 55:
            health = "🟡 Good"
        else:
            health = "🔴 Needs Attention"
        
        risk_section = self._generate_risk_section(risk, "en")
        
        report = f"""🧠🛡️ **DAILY INTELLIGENCE REPORT — ADMIN ONLY** 🛡️🧠

📅 Date: {date_str}

═══════════════════════════════════════════════════════════════
💰 DAILY PERFORMANCE SUMMARY
═══════════════════════════════════════════════════════════════

📊 Trading Results:
   ├─ Total Trades: {perf['total_trades']}
   ├─ ✅ Wins: {perf['wins']} | ❌ Losses: {perf['losses']}
   ├─ Win Rate: {perf['win_rate']:.1f}%
   └─ Net P&L: {'+' if perf['net_pnl'] >= 0 else ''}${perf['net_pnl']:,.2f}

💵 Account Status:
   ├─ Balance: ${balance:,.2f} {currency}
   ├─ Open Positions: {open_count}
   └─ Unrealized P&L: {'+' if total_upl >= 0 else ''}${total_upl:,.2f}

═══════════════════════════════════════════════════════════════
🎯 COMMITTEE INTELLIGENCE (Internal)
═══════════════════════════════════════════════════════════════

📈 Signal Flow:
   ├─ Technical Signals Generated: {committee['total_signals']}
   ├─ ✅ Committee Approved: {committee['approved']}
   ├─ 🛡️ Committee Blocked: {committee['blocked']} ({committee['block_rate']:.1f}%)
   └─ ⚖️ Execution Rate: {committee['execution_rate']:.1f}%

💡 Blocking Analysis:
   └─ Reasons: {block_reasons_str}

💰 Capital Protection:
   ├─ Losses Prevented (est.): ${committee['losses_prevented']:,.2f}
   └─ Committee Health: {health} ({avg_conf:.1f}% avg confidence)

═══════════════════════════════════════════════════════════════
🚨 RISK GUARDIAN STATUS
═══════════════════════════════════════════════════════════════

{risk_section}

═══════════════════════════════════════════════════════════════
⚠️ Confidential — Internal Use Only
═══════════════════════════════════════════════════════════════
"""
        return report
    
    def _generate_client_report(self, perf: dict, risk: dict, 
                                balance: float, currency: str, open_pos: list,
                                market_trend: str, lang: str) -> str:
        """Generate client-facing report."""
        date_str = self._get_local_date()
        open_count = len(open_pos)
        total_upl = sum(p.get("upl", 0) for p in open_pos)
        
        risk_section = self._generate_risk_section(risk, lang)
        
        if lang == "en":
            report = f"""📊 **DAILY TRADING REPORT — {date_str}**

═══════════════════════════════════════════════════════════════
💰 Your Performance Today
═══════════════════════════════════════════════════════════════

📊 Trade Summary:
   ├─ Total Trades: {perf['total_trades']}
   ├─ ✅ Wins: {perf['wins']} | ❌ Losses: {perf['losses']}
   ├─ Win Rate: {perf['win_rate']:.1f}%
   └─ Net P&L: {'+' if perf['net_pnl'] >= 0 else ''}${perf['net_pnl']:,.2f}

💵 Account Status:
   ├─ Balance: ${balance:,.2f} {currency}
   ├─ Open Positions: {open_count}
   └─ Unrealized P&L: {'+' if total_upl >= 0 else ''}${total_upl:,.2f}

═══════════════════════════════════════════════════════════════
📈 Market Overview
═══════════════════════════════════════════════════════════════

🌊 Market Trend: {market_trend}

═══════════════════════════════════════════════════════════════
🛡️ Risk Status
═══════════════════════════════════════════════════════════════

{risk_section}

═══════════════════════════════════════════════════════════════
_NATB v3.1 — Unified Daily Report_
═══════════════════════════════════════════════════════════════
"""
        else:
            # Arabic
            report = f"""📊 **التقرير اليومي — {date_str}**

═══════════════════════════════════════════════════════════════
💰 أداؤك اليوم
═══════════════════════════════════════════════════════════════

📊 ملخص الصفقات:
   ├─ إجمالي الصفقات: {perf['total_trades']}
   ├─ ✅ رابحة: {perf['wins']} | ❌ خاسرة: {perf['losses']}
   ├─ نسبة النجاح: {perf['win_rate']:.1f}%
   └─ صافي الربح/الخسارة: {'+' if perf['net_pnl'] >= 0 else ''}${perf['net_pnl']:,.2f}

💵 حالة الحساب:
   ├─ الرصيد: ${balance:,.2f} {currency}
   ├─ الصفقات المفتوحة: {open_count}
   └─ الربح/الخسارة غير المحقق: {'+' if total_upl >= 0 else ''}${total_upl:,.2f}

═══════════════════════════════════════════════════════════════
📈 نظرة عامة على السوق
═══════════════════════════════════════════════════════════════

🌊 اتجاه السوق: {market_trend}

═══════════════════════════════════════════════════════════════
🛡️ حالة المخاطر
═══════════════════════════════════════════════════════════════

{risk_section}

═══════════════════════════════════════════════════════════════
_NATB v3.1 — التقرير اليومي الموحد_
═══════════════════════════════════════════════════════════════
"""
        return report
    
    def _calculate_market_trend(self, trades: list) -> str:
        """Simple market trend calculation."""
        if not trades:
            return "No trading activity today"
        
        buy_signals = sum(1 for t in trades if t.get("action") == "BUY")
        sell_signals = sum(1 for t in trades if t.get("action") == "SELL")
        
        if buy_signals > sell_signals * 1.5:
            return "Bullish bias — more buy signals generated"
        elif sell_signals > buy_signals * 1.5:
            return "Bearish bias — more sell signals generated"
        else:
            return "Mixed signals — balanced market activity"
    
    # ═════════════════════════════════════════════════════════════════════════════
    # PUBLIC API
    # ═════════════════════════════════════════════════════════════════════════════
    
    def send_admin_report(self) -> bool:
        """Send comprehensive report to admin only."""
        if not self.admin_chat_id:
            logger.warning("[UnifiedReporter] ADMIN_CHAT_ID not configured")
            return False
        
        try:
            memory_data = self._load_agent_memory()
            experiences = self._get_today_experiences(memory_data)
            trades = self._get_today_trades()
            perf = self._calculate_performance(trades)
            committee = self._calculate_committee_stats(experiences)
            
            # Use admin's own account data
            balance, currency = self._get_account_balance(self.admin_chat_id)
            open_pos = self._get_open_positions(self.admin_chat_id)
            risk = self._get_risk_guardian_status(self.admin_chat_id)
            
            report = self._generate_admin_report(perf, committee, risk, balance, currency, open_pos)
            
            send_telegram_message(self.admin_chat_id, report)
            logger.info(f"[UnifiedReporter] Admin report sent to {self.admin_chat_id}")
            return True
            
        except Exception as e:
            logger.error(f"[UnifiedReporter] Admin report failed: {e}")
            return False
    
    def send_client_report(self, chat_id: str) -> bool:
        """Send unified report to a single client."""
        try:
            lang = get_subscriber_lang(chat_id)
            trades = self._get_today_trades()
            perf = self._calculate_performance(trades)
            balance, currency = self._get_account_balance(chat_id)
            open_pos = self._get_open_positions(chat_id)
            risk = self._get_risk_guardian_status(chat_id)
            market_trend = self._calculate_market_trend(trades)
            
            report = self._generate_client_report(perf, risk, balance, currency, 
                                                    open_pos, market_trend, lang)
            
            send_telegram_message(chat_id, report)
            logger.info(f"[UnifiedReporter] Client report sent to {chat_id}")
            return True
            
        except Exception as e:
            logger.error(f"[UnifiedReporter] Client report failed for {chat_id}: {e}")
            return False
    
    def send_all_reports(self) -> Dict[str, Any]:
        """Send reports to admin and all active subscribers."""
        logger.info("[UnifiedReporter] Starting unified report generation...")
        
        # Send admin report first
        admin_sent = self.send_admin_report()
        
        # Send client reports
        clients_sent = 0
        try:
            from core.watcher import get_all_active_subscribers
            for row in get_all_active_subscribers():
                chat_id = str(row[0])
                if chat_id == self.admin_chat_id:
                    continue  # Admin already got their report
                if self.send_client_report(chat_id):
                    clients_sent += 1
        except Exception as e:
            logger.error(f"[UnifiedReporter] Failed to send client reports: {e}")
        
        logger.info(f"[UnifiedReporter] Complete: Admin={admin_sent}, Clients={clients_sent}")
        return {
            "admin_sent": admin_sent,
            "clients_sent": clients_sent,
            "date": self._get_local_date()
        }


# ═════════════════════════════════════════════════════════════════════════════════
# SCHEDULING API
# ═════════════════════════════════════════════════════════════════════════════════

def send_unified_reports() -> Dict[str, Any]:
    """Public API: Execute the unified daily report."""
    reporter = UnifiedReporter()
    return reporter.send_all_reports()


def should_run_scheduled_report() -> bool:
    """
    Check if it's time to run the unified report.
    
    Schedule: 20:05 UTC (00:05 UAE) — 5 minutes after market close.
    """
    now = datetime.now(timezone.utc)
    target_hour = 20  # 20:05 UTC = 00:05 UAE (next day)
    target_minute = 5
    
    return now.hour == target_hour and now.minute == target_minute


# ═════════════════════════════════════════════════════════════════════════════════
# TELEGRAM COMMAND HANDLER
# ═════════════════════════════════════════════════════════════════════════════════

async def daily_report_command(update, context):
    """
    Handle /daily_report command (Admin only).
    Security: Only ADMIN_CHAT_ID can trigger this command.
    """
    from telegram import Update
    
    chat_id = str(update.message.chat_id)
    
    if chat_id != ADMIN_CHAT_ID:
        await update.message.reply_text(
            "⛔ This command is restricted to admin users only."
        )
        logger.warning(f"[UnifiedReporter] Unauthorized /daily_report attempt from {chat_id}")
        return
    
    await update.message.reply_text("📊 Generating unified daily reports...")
    
    result = send_unified_reports()
    
    status_msg = f"""✅ Unified Reports Generated

🧠 Admin Report: {'Sent' if result['admin_sent'] else 'Failed'}
📈 Client Reports: {result['clients_sent']} subscribers notified
📅 Date: {result['date']}"""
    
    await update.message.reply_text(status_msg)


if __name__ == "__main__":
    # Test mode
    print("=" * 60)
    print("[TEST] Unified Reporter — NATB v3.1")
    print("=" * 60)
    
    reporter = UnifiedReporter()
    print(f"✓ Report date: {reporter._get_local_date()}")
    print(f"✓ Admin Chat ID: {ADMIN_CHAT_ID}")
    
    # Preview admin report (don't send)
    memory_data = reporter._load_agent_memory()
    experiences = reporter._get_today_experiences(memory_data)
    trades = reporter._get_today_trades()
    perf = reporter._calculate_performance(trades)
    committee = reporter._calculate_committee_stats(experiences)
    
    print(f"✓ Loaded {len(experiences)} experiences, {len(trades)} trades")
    print(f"✓ Performance: {perf['wins']} wins, {perf['losses']} losses")
    print(f"✓ Committee: {committee['approved']} approved, {committee['blocked']} blocked")
    
    print("\n[TEST] Ready for production")
    print("-" * 60)
