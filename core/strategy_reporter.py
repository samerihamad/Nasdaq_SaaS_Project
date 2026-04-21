"""
Strategy Reporter — NATB v2.0 Dual-Channel Nightly Strategic Report

Phase 6-A: Admin vs Client Reporting System

This module implements a dual-channel reporting system that distinguishes between
Admin (Samer) and Client perspectives:
  - Admin Report: Committee performance, blocked signals, losses prevented
  - Client Report: Trading results, P&L, market overview

Security:
  - Admin reports ONLY sent to ADMIN_CHAT_ID from config.py
  - Client reports broadcast to all active subscribers
  - No internal committee logic is exposed to clients

Schedule:
  - Automatic: 23:30 UAE time (19:30 UTC)
  - Manual: /daily_report command (Admin only)
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from pathlib import Path

# Ensure project root is in path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import ADMIN_CHAT_ID
from database.db_manager import DB_PATH
from bot.notifier import send_telegram_message

log = logging.getLogger(__name__)

# Constants
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
MEMORY_FILE = os.path.join(DATA_DIR, "agent_memory.json")

# UAE timezone offset (UTC+4)
UAE_OFFSET_HOURS = 4

# Estimated average trade size for "losses prevented" calculation
ESTIMATED_TRADE_SIZE = 1000  # USD per trade
ESTIMATED_LOSS_PER_BLOCKED = 0.02  # Assume 2% loss on blocked bad signals


class StrategyReporter:
    """
    Dual-Channel Strategy Reporter.
    
    Generates separate reports for Admin (internal) and Clients (external).
    """
    
    def __init__(self):
        self.report_date = datetime.now().strftime("%Y-%m-%d")
        self.admin_chat_id = ADMIN_CHAT_ID
        
    # ═════════════════════════════════════════════════════════════════════════════
    # DATA COLLECTION
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _load_agent_memory(self) -> dict:
        """Load agent memory data for analysis."""
        if not os.path.exists(MEMORY_FILE):
            return {"experiences": {}, "metadata": {}}
        
        try:
            with open(MEMORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            log.error(f"[StrategyReporter] Failed to load agent_memory: {e}")
            return {"experiences": {}, "metadata": {}}
    
    def _get_today_experiences(self, memory_data: dict) -> list:
        """Filter experiences to today's date only."""
        today_str = datetime.now().strftime("%Y-%m-%d")
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
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            today = datetime.now().strftime("%Y-%m-%d")
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Get all trades opened today
            c.execute(
                """
                SELECT chat_id, symbol, direction, entry_price, size, 
                       pnl, status, opened_at, closed_at, actual_pnl
                FROM trades 
                WHERE (opened_at >= ? AND opened_at < ?)
                   OR (closed_at >= ? AND closed_at < ?)
                """,
                (today, tomorrow, today, tomorrow)
            )
            
            trades = c.fetchall()
            conn.close()
            
            return trades
        except Exception as e:
            log.error(f"[StrategyReporter] Failed to fetch trades: {e}")
            return []
    
    def _calculate_market_trend(self, trades: list) -> str:
        """Analyze today's trades to determine market trend."""
        if not trades:
            return "No market activity today"
        
        # Count bullish vs bearish signals
        bullish = sum(1 for t in trades if t[2] in ["BUY", "LONG"])
        bearish = sum(1 for t in trades if t[2] in ["SELL", "SHORT"])
        total = len(trades)
        
        # Calculate win rate
        closed_trades = [t for t in trades if t[5] is not None or t[9] is not None]
        wins = sum(1 for t in closed_trades if (t[9] or t[5] or 0) > 0)
        
        if not closed_trades:
            win_rate = 0
        else:
            win_rate = (wins / len(closed_trades)) * 100
        
        # Build trend description
        if bullish > bearish * 1.5:
            trend = "Strong bullish momentum"
        elif bullish > bearish:
            trend = "Moderate bullish bias"
        elif bearish > bullish * 1.5:
            trend = "Strong bearish pressure"
        elif bearish > bullish:
            trend = "Moderate bearish sentiment"
        else:
            trend = "Mixed signals, sideways movement"
        
        return f"{trend} ({bullish} buy / {bearish} sell signals, {win_rate:.0f}% win rate)"
    
    # ═════════════════════════════════════════════════════════════════════════════
    # ADMIN-ONLY INSIGHTS (The Secret Sauce)
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _generate_admin_report(self, experiences: list, trades: list) -> str:
        """
        Generate Admin-only strategic report.
        
        Contains sensitive internal metrics:
        - Committee blocking performance
        - Losses prevented estimation
        - Consensus health metrics
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        # 1. Total Technical Signals
        total_signals = len(experiences)
        
        # 2. Committee Blocking Analysis
        approved = sum(1 for e in experiences if e.get("verdict") == "APPROVE")
        rejected = sum(1 for e in experiences if e.get("verdict") == "REJECT")
        uncertain = sum(1 for e in experiences if e.get("verdict") == "UNCERTAIN")
        blocked = rejected + uncertain  # Both prevent execution
        
        # Blocking rate
        block_rate = (blocked / total_signals * 100) if total_signals > 0 else 0
        
        # 3. Losses Prevented Estimate
        # Assume blocked signals would have had average negative outcome
        losses_prevented = blocked * ESTIMATED_TRADE_SIZE * ESTIMATED_LOSS_PER_BLOCKED
        
        # 4. Committee Consensus Health
        if experiences:
            avg_confidence = sum(
                e.get("ai_confidence", 0) for e in experiences
            ) / len(experiences)
            avg_technical = sum(
                e.get("technical_confidence", 0) for e in experiences
            ) / len(experiences)
        else:
            avg_confidence = 0
            avg_technical = 0
        
        # Health indicator
        if avg_confidence >= 70:
            health_emoji = "🟢"
            health_status = "Excellent"
        elif avg_confidence >= 55:
            health_emoji = "🟡"
            health_status = "Good"
        else:
            health_emoji = "🔴"
            health_status = "Needs Attention"
        
        # Blocking reasons breakdown
        reject_reasons = []
        if rejected > 0:
            reject_reasons.append(f"{rejected} explicit REJECT")
        if uncertain > 0:
            reject_reasons.append(f"{uncertain} insufficient consensus")
        
        block_reasons = " + ".join(reject_reasons) if reject_reasons else "N/A"
        
        # Build Admin Report
        report = f"""🧠🛡️ STRATEGIC INTELLIGENCE REPORT — ADMIN ONLY 🛡️🧠

📅 Date: {today}

═══════════════════════════════════════════════════════════
🎯 COMMITTEE PERFORMANCE (Internal)
═══════════════════════════════════════════════════════════

📊 Signal Generation:
   ├─ Total Technical Signals: {total_signals}
   ├─ ✅ Committee Approved: {approved}
   ├─ 🛡️ Committee Blocked: {blocked} ({block_rate:.1f}%)
   │   └─ Reasons: {block_reasons}
   └─ ⚖️ Execution Rate: {(approved/total_signals*100) if total_signals > 0 else 0:.1f}%

💰 Capital Protection:
   ├─ Losses Prevented (est.): ${losses_prevented:,.2f}
   ├─ Blocked Trades Value: ${blocked * ESTIMATED_TRADE_SIZE:,.2f}
   └─ Protection Efficiency: {health_status}

🧠 Consensus Health:
   ├─ {health_emoji} Status: {health_status}
   ├─ Avg AI Confidence: {avg_confidence:.1f}%
   ├─ Avg Technical Conf: {avg_technical:.1f}%
   └─ Committee Engagement: {total_signals} decisions today

═══════════════════════════════════════════════════════════
⚠️ This report contains internal strategy metrics.
   DO NOT share with clients.
═══════════════════════════════════════════════════════════

_NATB v2.0 — Phase 6-A Strategic Reporter_"""
        
        return report
    
    # ═════════════════════════════════════════════════════════════════════════════
    # CLIENT-FRIENDLY REPORT
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _generate_client_report(self, trades: list, market_trend: str) -> str:
        """
        Generate Client-friendly daily report.
        
        Shows only trading results, no internal committee logic.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Calculate P&L
        closed_trades = [t for t in trades if t[5] is not None or t[9] is not None]
        total_pnl = sum((t[9] or t[5] or 0) for t in closed_trades)
        
        wins = sum(1 for t in closed_trades if (t[9] or t[5] or 0) > 0)
        losses = sum(1 for t in closed_trades if (t[9] or t[5] or 0) <= 0)
        
        # Open positions
        open_trades = [t for t in trades if t[6] == "OPEN"]
        open_count = len(open_trades)
        
        # Determine emoji based on P&L
        if total_pnl > 0:
            pnl_emoji = "📈✅"
            sign = "+"
        elif total_pnl < 0:
            pnl_emoji = "📉❌"
            sign = ""
        else:
            pnl_emoji = "📊➖"
            sign = ""
        
        # Build Client Report
        report = f"""{pnl_emoji} DAILY TRADING REPORT — {today}

═══════════════════════════════════════════════════════════
📈 TRADING PERFORMANCE
═══════════════════════════════════════════════════════════

✅ Trades Executed Today: {len(closed_trades)}
   ├─ 🟢 Winning Trades: {wins}
   ├─ 🔴 Losing Trades: {losses}
   └─ 📊 Win Rate: {(wins/len(closed_trades)*100) if closed_trades else 0:.0f}%

💰 Daily P&L: {sign}${total_pnl:,.2f}
   └─ Session Return: {sign}{((total_pnl/10000)*100) if closed_trades else 0:.2f}% (est.)

📋 Active Positions: {open_count} open

═══════════════════════════════════════════════════════════
🌍 MARKET OVERVIEW
═══════════════════════════════════════════════════════════

{market_trend}

Key Highlights:
• Our strategies filtered through {len(trades)} market opportunities
• Risk management protocols active throughout session
• All positions monitored with automated protection

═══════════════════════════════════════════════════════════

_NATB v2.0 — Your Automated Trading Partner_
📊 Smart. Secure. Systematic."""
        
        return report
    
    # ═════════════════════════════════════════════════════════════════════════════
    # DELIVERY METHODS
    # ═════════════════════════════════════════════════════════════════════════════
    
    def send_admin_report(self) -> bool:
        """Send strategic report to Admin only."""
        try:
            # Collect data
            memory_data = self._load_agent_memory()
            experiences = self._get_today_experiences(memory_data)
            trades = self._get_today_trades()
            
            # Generate report
            report = self._generate_admin_report(experiences, trades)
            
            # Security check: Only send to ADMIN_CHAT_ID
            if not self.admin_chat_id:
                log.error("[StrategyReporter] ADMIN_CHAT_ID not configured — cannot send admin report")
                return False
            
            # Send to Admin
            send_telegram_message(self.admin_chat_id, report)
            log.info(f"[StrategyReporter] Admin strategic report sent to {self.admin_chat_id}")
            return True
            
        except Exception as e:
            log.error(f"[StrategyReporter] Failed to send admin report: {e}")
            return False
    
    def send_client_reports(self) -> int:
        """Send client reports to all active subscribers."""
        try:
            # Collect data
            trades = self._get_today_trades()
            market_trend = self._calculate_market_trend(trades)
            
            # Generate report
            report = self._generate_client_report(trades, market_trend)
            
            # Get active subscribers
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute(
                "SELECT chat_id, lang FROM subscribers WHERE is_active=1 AND email IS NOT NULL"
            )
            subscribers = c.fetchall()
            conn.close()
            
            # Send to all clients
            sent_count = 0
            for chat_id, lang in subscribers:
                try:
                    send_telegram_message(chat_id, report)
                    sent_count += 1
                except Exception as e:
                    log.error(f"[StrategyReporter] Failed to send to {chat_id}: {e}")
            
            log.info(f"[StrategyReporter] Client reports sent to {sent_count}/{len(subscribers)} subscribers")
            return sent_count
            
        except Exception as e:
            log.error(f"[StrategyReporter] Failed to send client reports: {e}")
            return 0
    
    def send_all_reports(self) -> dict:
        """Send both admin and client reports."""
        return {
            "admin_sent": self.send_admin_report(),
            "clients_sent": self.send_client_reports(),
            "timestamp": datetime.now().isoformat(),
        }


# ═════════════════════════════════════════════════════════════════════════════════
# SCHEDULING & COMMANDS
# ═════════════════════════════════════════════════════════════════════════════════

def should_run_scheduled_report() -> bool:
    """
    Check if it's time to run the scheduled report (23:30 UAE time).
    
    UAE is UTC+4, so 23:30 UAE = 19:30 UTC.
    """
    now = datetime.now(timezone.utc)
    target_hour = 19  # 19:30 UTC = 23:30 UAE
    target_minute = 30
    
    # Check if it's within the target minute
    if now.hour == target_hour and now.minute == target_minute:
        return True
    
    return False


def run_nightly_reports() -> dict:
    """Execute the nightly dual-channel report."""
    reporter = StrategyReporter()
    return reporter.send_all_reports()


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
    
    # Security check: Admin only
    if chat_id != ADMIN_CHAT_ID:
        await update.message.reply_text(
            "⛔ This command is restricted to admin users only."
        )
        log.warning(f"[StrategyReporter] Unauthorized /daily_report attempt from {chat_id}")
        return
    
    # Acknowledge
    await update.message.reply_text("📊 Generating strategic reports...")
    
    # Generate and send reports
    reporter = StrategyReporter()
    
    admin_sent = reporter.send_admin_report()
    clients_sent = reporter.send_client_reports()
    
    # Confirm to admin
    status_msg = f"""✅ Reports Generated

🧠 Admin Report: {'Sent' if admin_sent else 'Failed'}
📈 Client Reports: {clients_sent} subscribers notified"""
    
    await update.message.reply_text(status_msg)


# ═════════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY
# ═════════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Test the reporter
    print("=" * 60)
    print("[TEST] Strategy Reporter — Phase 6-A")
    print("=" * 60)
    
    reporter = StrategyReporter()
    
    # Test data collection
    memory_data = reporter._load_agent_memory()
    experiences = reporter._get_today_experiences(memory_data)
    trades = reporter._get_today_trades()
    
    print(f"✓ Loaded {len(experiences)} today's experiences from agent_memory")
    print(f"✓ Loaded {len(trades)} today's trades from database")
    print(f"✓ Admin Chat ID configured: {bool(ADMIN_CHAT_ID)}")
    
    # Generate sample reports (don't send in test)
    print("\n--- ADMIN REPORT PREVIEW ---")
    admin_report = reporter._generate_admin_report(experiences, trades)
    print(admin_report[:500] + "...")
    
    print("\n--- CLIENT REPORT PREVIEW ---")
    market_trend = reporter._calculate_market_trend(trades)
    client_report = reporter._generate_client_report(trades, market_trend)
    print(client_report[:500] + "...")
    
    print("\n[TEST] Ready for integration")
    print("-" * 60)
