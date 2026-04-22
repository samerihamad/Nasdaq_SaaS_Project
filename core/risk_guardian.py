"""
Risk Guardian Agent — NATB v3.1
Unified risk management architecture consolidating logic from risk_manager.py and executor.py.

Provides centralized:
- Position sizing with volatility adjustments
- ATR-based Take Profit calculation
- Pre-trade validation gates
- Circuit breaker state management
- Risk state transitions with notifications
"""

import os
import sqlite3
import math
import logging
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any

from config import (
    MAX_DAILY_TRADES,
    GLOBAL_MAX_OPEN_TRADES,
    MAX_DAILY_LOSS_PCT,
    TP1_ATR_MULT,
    TP2_ATR_MULT,
    MAX_RISK_PCT_VOLATILE,
    VOLATILITY_SIZE_REDUCTION_PCT,
)
from database.db_manager import (
    DB_PATH,
    is_master_kill_switch,
    get_user_kill_switch,
    get_user_risk_params,
    get_preferred_leverage,
    set_trading_enabled,
)
from bot.notifier import send_telegram_message

logger = logging.getLogger(__name__)

# Risk States
STATE_NORMAL = 'NORMAL'
STATE_CIRCUIT_BREAKER = 'CIRCUIT_BREAKER'
STATE_MANUAL_OVERRIDE = 'MANUAL_OVERRIDE'
STATE_HARD_BLOCK = 'HARD_BLOCK'
STATE_USER_DAY_HALT = 'USER_DAY_HALT'


class RiskGuardianAgent:
    """
    Centralized Risk Management Agent.
    
    Consolidates position sizing, stop-loss/take-profit logic,
    portfolio protections, and circuit breaker management.
    """
    
    # Confidence scaling constants
    MIN_CONF = 70.0
    MAX_CONF = 100.0
    MIN_RISK = 1.0
    MAX_RISK = 2.0
    
    # Consecutive loss limit for circuit breaker
    CONSECUTIVE_LOSS_LIMIT = int(os.getenv("CB_LOSS_LIMIT", "2"))
    
    # Pending orders shadow cap (moved from hardcoded to env-driven default)
    MAX_PENDING_SHADOW_CAP = int(os.getenv("MAX_PENDING_ORDERS", "12"))
    
    def __init__(self):
        self._state_cache: Dict[str, str] = {}
        self._last_refresh: Optional[datetime] = None
    
    def _conn(self):
        return sqlite3.connect(DB_PATH)
    
    def _utc_today(self) -> str:
        """Return today's date in UTC as string."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # ═════════════════════════════════════════════════════════════════════════════
    # POSITION SIZING
    # ═════════════════════════════════════════════════════════════════════════════
    
    def calculate_position_size(
        self,
        balance: float,
        confidence: float,
        entry_price: float,
        stop_loss_pct: float = 0.01,
        chat_id: str = None,
        regime_type: str = "UNKNOWN",
        atr: Optional[float] = None,
    ) -> float:
        """
        Calculate position size with confidence scaling and volatility adjustment.
        
        Args:
            balance: Account balance
            confidence: Signal confidence (70-100)
            entry_price: Entry price level
            stop_loss_pct: Stop loss as percentage of entry
            chat_id: User identifier for personalized risk params
            regime_type: Market regime (NORMAL, VOLATILE, etc.)
            atr: Optional ATR for additional volatility sizing
            
        Returns:
            Position size in units (minimum 1.0)
        """
        # Get user-specific risk parameters
        if chat_id:
            user_min, user_max = get_user_risk_params(chat_id)
        else:
            user_min, user_max = self.MIN_RISK, self.MAX_RISK
        
        # Volatility regime cap
        if regime_type.upper() == "VOLATILE":
            user_max = min(user_max, MAX_RISK_PCT_VOLATILE)
        
        # Confidence-weighted risk percentage (convex curve)
        clamped = max(self.MIN_CONF, min(self.MAX_CONF, confidence))
        conf_norm = (clamped - self.MIN_CONF) / (self.MAX_CONF - self.MIN_CONF)
        conf_weight = conf_norm ** 1.8  # Convex emphasis on high confidence
        risk_pct = user_min + (user_max - user_min) * conf_weight
        
        # Apply volatility cap again post-calculation
        if regime_type.upper() == "VOLATILE":
            risk_pct = min(risk_pct, MAX_RISK_PCT_VOLATILE)
            # Additional 50% size reduction in volatile regimes
            risk_pct *= (1.0 - VOLATILITY_SIZE_REDUCTION_PCT)
            logger.info(f"[RISK GUARDIAN] Volatile regime: size reduced by {VOLATILITY_SIZE_REDUCTION_PCT*100:.0f}%")
        
        # Calculate risk budget
        risk_budget = balance * (risk_pct / 100)
        
        # Apply leverage scaling (risk budget scales with effective/cap leverage)
        if chat_id:
            cap = self._get_user_max_leverage(chat_id)
            lev = self._get_effective_leverage(chat_id)
            if cap > 0:
                risk_budget *= lev / cap
        
        # Validate inputs
        if entry_price <= 0 or stop_loss_pct <= 0:
            logger.warning(f"[RISK GUARDIAN] Invalid inputs: entry_price={entry_price}, stop_loss_pct={stop_loss_pct}")
            return 1.0
        
        # Final size calculation
        size = risk_budget / (entry_price * stop_loss_pct)
        final_size = max(1.0, round(size, 2))
        
        logger.info(
            f"[RISK GUARDIAN] Size calc: balance={balance:.2f}, risk_pct={risk_pct:.2f}%, "
            f"regime={regime_type}, size={final_size:.2f}"
        )
        
        return final_size
    
    # ═════════════════════════════════════════════════════════════════════════════
    # ATR-BASED TAKE PROFIT CALCULATION
    # ═════════════════════════════════════════════════════════════════════════════
    
    def calculate_atr_targets(
        self,
        entry_price: float,
        atr: float,
        action: str,
    ) -> Tuple[float, float]:
        """
        Calculate ATR-based take profit targets.
        
        Args:
            entry_price: Entry price level
            atr: ATR value
            action: "BUY" or "SELL"
            
        Returns:
            Tuple of (target1, target2) price levels
        """
        if not atr or atr <= 0 or not entry_price or entry_price <= 0:
            logger.warning(f"[RISK GUARDIAN] Invalid ATR/entry for target calc: atr={atr}, entry={entry_price}")
            # Fallback to percentage-based (should not happen with valid data)
            from config import TP1_PCT, TP2_PCT
            if action == "BUY":
                return entry_price * (1 + TP1_PCT), entry_price * (1 + TP2_PCT)
            else:
                return entry_price * (1 - TP1_PCT), entry_price * (1 - TP2_PCT)
        
        # ATR-based calculation: TP1 = 1.5x ATR, TP2 = 3.0x ATR
        tp1_distance = atr * TP1_ATR_MULT
        tp2_distance = atr * TP2_ATR_MULT
        
        if action == "BUY":
            target1 = entry_price + tp1_distance
            target2 = entry_price + tp2_distance
        else:
            target1 = entry_price - tp1_distance
            target2 = entry_price - tp2_distance
        
        logger.info(
            f"[RISK GUARDIAN] ATR targets: entry={entry_price:.4f}, atr={atr:.4f}, "
            f"tp1={target1:.4f} (×{TP1_ATR_MULT}), tp2={target2:.4f} (×{TP2_ATR_MULT})"
        )
        
        return target1, target2
    
    def validate_rr_ratio(
        self,
        entry_price: float,
        stop_price: float,
        action: str,
        atr: float,
    ) -> Tuple[bool, float, str]:
        """
        Validate Risk:Reward ratio meets minimum requirement.
        
        Args:
            entry_price: Entry price
            stop_price: Stop loss price
            action: "BUY" or "SELL"
            atr: ATR for target achievability check
            
        Returns:
            Tuple of (is_valid, rr_ratio, reason)
        """
        from config import MIN_RR_RATIO
        
        if not all([entry_price, stop_price, atr]) or atr <= 0:
            return False, 0.0, "invalid_inputs"
        
        stop_dist = abs(entry_price - stop_price)
        if stop_dist <= 0:
            return False, 0.0, "zero_stop_distance"
        
        # Calculate potential targets
        target1, target2 = self.calculate_atr_targets(entry_price, atr, action)
        profit_dist = abs(target2 - entry_price)  # Use TP2 for R:R calculation
        
        rr_ratio = profit_dist / stop_dist if stop_dist > 0 else 0.0
        
        # Check minimum R:R
        if rr_ratio < MIN_RR_RATIO:
            return False, rr_ratio, f"rr_below_minimum_{MIN_RR_RATIO}"
        
        # Check target achievability (target beyond 4x ATR is unrealistic)
        max_achievable = entry_price + (4 * atr) if action == "BUY" else entry_price - (4 * atr)
        if (action == "BUY" and target2 > max_achievable) or (action == "SELL" and target2 < max_achievable):
            return False, rr_ratio, "target_beyond_atr_limit"
        
        return True, rr_ratio, "ok"
 
    # ═════════════════════════════════════════════════════════════════════════════
    # RISK STATE MANAGEMENT
    # ═════════════════════════════════════════════════════════════════════════════
    
    def get_risk_state(self, chat_id: str) -> str:
        """Get current risk state for user."""
        today = self._utc_today()
        db = self._conn()
        c = db.cursor()
        c.execute(
            "SELECT state FROM daily_risk_state WHERE chat_id=? AND day=?",
            (str(chat_id), today)
        )
        row = c.fetchone()
        db.close()
        return (row[0] if row else STATE_NORMAL)
    
    def can_open_trade(self, chat_id: str, is_pending_trigger: bool = False) -> Tuple[bool, str]:
        """
        Comprehensive pre-trade risk gate.
        
        Returns:
            Tuple of (allowed, reason_or_state)
        """
        # 1. Master kill switch
        if is_master_kill_switch():
            return False, "Master kill switch active — all trading halted by admin"
        
        # 2. User kill switch
        if get_user_kill_switch(chat_id):
            return False, "Your trading session has been halted by the admin"
        
        # 3. Risk state checks
        state = self.get_risk_state(chat_id)
        if state == STATE_USER_DAY_HALT:
            return False, "Full-day halt active — resume from dashboard to trade again"
        if state == STATE_CIRCUIT_BREAKER:
            return False, "Circuit Breaker active — awaiting your Telegram approval"
        if state == STATE_HARD_BLOCK:
            return False, "Hard Block active — lifts automatically at next trading day open"
        if state not in (STATE_NORMAL, STATE_MANUAL_OVERRIDE):
            return False, f"Unknown risk state: {state}"
        
        # 4. Daily trade count
        trades_today = self._get_daily_trade_count(chat_id)
        if trades_today >= MAX_DAILY_TRADES:
            return False, f"Max daily trades reached ({trades_today}/{MAX_DAILY_TRADES})"
        
        # 5. Global open positions
        open_positions = self._get_global_open_trades_count()
        if open_positions >= GLOBAL_MAX_OPEN_TRADES:
            return False, f"Global max open trades reached ({open_positions}/{GLOBAL_MAX_OPEN_TRADES})"
        
        # 6. Pending orders shadow cap
        if not is_pending_trigger:
            pending_orders = self._get_global_pending_orders_count()
            if pending_orders >= self.MAX_PENDING_SHADOW_CAP:
                return False, f"Max pending orders reached ({pending_orders}/{self.MAX_PENDING_SHADOW_CAP})"
        
        # 7. Daily loss limit
        live_balance = self._get_live_balance(chat_id)
        if live_balance and live_balance > 0:
            daily_pnl = self._get_daily_pnl(chat_id)
            if daily_pnl < 0:
                daily_loss_pct = abs(daily_pnl) / live_balance * 100
                if daily_loss_pct >= MAX_DAILY_LOSS_PCT:
                    # Trigger circuit breaker notification
                    self._send_circuit_breaker_alert(chat_id, daily_loss_pct)
                    return False, f"Max daily loss reached ({daily_loss_pct:.2f}% / {MAX_DAILY_LOSS_PCT:.2f}%)"
        
        return True, state
    
    def record_trade_result(self, chat_id: str, pnl: float) -> None:
        """
        Record trade outcome and manage circuit breaker state transitions.
        """
        today = self._utc_today()
        db = self._conn()
        c = db.cursor()
        
        # Get current state
        c.execute(
            "SELECT state, consecutive_losses FROM daily_risk_state WHERE chat_id=? AND day=?",
            (str(chat_id), today)
        )
        row = c.fetchone()
        
        if row:
            state, consecutive_losses = row
            consecutive_losses = int(consecutive_losses or 0)
        else:
            state = STATE_NORMAL
            consecutive_losses = 0
        
        # State machine transitions
        new_state = state
        new_consecutive = consecutive_losses
        
        if pnl < 0:
            # Loss recorded
            new_consecutive = consecutive_losses + 1
            
            if state == STATE_MANUAL_OVERRIDE:
                # Manual override + loss = HARD_BLOCK
                new_state = STATE_HARD_BLOCK
                self._send_hard_block_alert(chat_id, pnl)
            elif new_consecutive >= self.CONSECUTIVE_LOSS_LIMIT:
                # 2 consecutive losses = CIRCUIT_BREAKER
                new_state = STATE_CIRCUIT_BREAKER
                self._send_circuit_breaker_alert(chat_id, consecutive_losses=new_consecutive)
        else:
            # Win recorded - reset consecutive losses
            new_consecutive = 0
            if state == STATE_MANUAL_OVERRIDE:
                new_state = STATE_NORMAL
        
        # Update database
        c.execute(
            """INSERT INTO daily_risk_state (chat_id, day, state, consecutive_losses)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(chat_id, day) DO UPDATE SET
               state=excluded.state, consecutive_losses=excluded.consecutive_losses""",
            (str(chat_id), today, new_state, new_consecutive)
        )
        db.commit()
        db.close()
        
        logger.info(
            f"[RISK GUARDIAN] Trade result for {chat_id}: pnl={pnl:.2f}, "
            f"state={state}→{new_state}, consecutive={new_consecutive}"
        )
    
    # ═════════════════════════════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═════════════════════════════════════════════════════════════════════════════
    
    def _get_user_max_leverage(self, chat_id: str) -> int:
        """Return max leverage for user."""
        return 10
    
    def _get_effective_leverage(self, chat_id: str) -> int:
        """Get effective leverage (user preference capped at max)."""
        cap = self._get_user_max_leverage(chat_id)
        pref = get_preferred_leverage(chat_id)
        if pref is None:
            return cap
        return max(1, min(int(pref), cap))
    
    def _get_daily_trade_count(self, chat_id: str) -> int:
        """Count trades executed today."""
        today = self._utc_today()
        db = self._conn()
        c = db.cursor()
        c.execute(
            "SELECT COUNT(*) FROM trades WHERE chat_id=? AND DATE(created_at)=?",
            (str(chat_id), today)
        )
        count = c.fetchone()[0]
        db.close()
        return count
    
    def _get_global_open_trades_count(self) -> int:
        """Count all open trades across users."""
        db = self._conn()
        c = db.cursor()
        c.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'")
        count = c.fetchone()[0]
        db.close()
        return count
    
    def _get_global_pending_orders_count(self) -> int:
        """Count pending limit orders."""
        db = self._conn()
        c = db.cursor()
        c.execute("SELECT COUNT(*) FROM pending_limit_orders WHERE status='PENDING'")
        count = c.fetchone()[0]
        db.close()
        return count
    
    def _get_live_balance(self, chat_id: str) -> Optional[float]:
        """Get live account balance."""
        try:
            db = self._conn()
            c = db.cursor()
            c.execute(
                "SELECT live_balance FROM accounts WHERE chat_id=? ORDER BY updated_at DESC LIMIT 1",
                (str(chat_id),)
            )
            row = c.fetchone()
            db.close()
            return float(row[0]) if row and row[0] else None
        except Exception:
            return None
    
    def _get_daily_pnl(self, chat_id: str) -> float:
        """Get today's realized P&L."""
        today = self._utc_today()
        db = self._conn()
        c = db.cursor()
        c.execute(
            "SELECT SUM(pnl) FROM trades WHERE chat_id=? AND DATE(closed_at)=? AND status='CLOSED'",
            (str(chat_id), today)
        )
        row = c.fetchone()
        db.close()
        return float(row[0] or 0.0)
    
    def _send_circuit_breaker_alert(self, chat_id: str, loss_pct: Optional[float] = None, consecutive_losses: Optional[int] = None):
        """Send circuit breaker alert to Telegram."""
        try:
            if loss_pct:
                message = (
                    f"🚨 **[CIRCUIT BREAKER TRIGGERED]**\n"
                    f"Daily loss limit reached: {loss_pct:.2f}%\n"
                    f"Trading halted until manual override.\n"
                    f"Reply /override to resume with caution."
                )
            else:
                message = (
                    f"🚨 **[CIRCUIT BREAKER TRIGGERED]**\n"
                    f"Consecutive losses: {consecutive_losses}\n"
                    f"Trading halted until manual override.\n"
                    f"Reply /override to resume with caution."
                )
            send_telegram_message(chat_id, message)
            logger.warning(f"[RISK GUARDIAN] Circuit breaker alert sent to {chat_id}")
        except Exception as e:
            logger.error(f"[RISK GUARDIAN] Failed to send circuit breaker alert: {e}")
    
    def _send_hard_block_alert(self, chat_id: str, pnl: float):
        """Send hard block alert to Telegram."""
        try:
            message = (
                f"⛔ **[HARD BLOCK ACTIVATED]**\n"
                f"Manual override trade resulted in loss: {pnl:.2f}\n"
                f"Trading locked until next trading day.\n"
                f"Risk protection enforced."
            )
            send_telegram_message(chat_id, message)
            logger.warning(f"[RISK GUARDIAN] Hard block alert sent to {chat_id}")
        except Exception as e:
            logger.error(f"[RISK GUARDIAN] Failed to send hard block alert: {e}")


# ═════════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL INSTANCE (Singleton Pattern)
# ═════════════════════════════════════════════════════════════════════════════════

# Global Risk Guardian instance
_risk_guardian: Optional[RiskGuardianAgent] = None


def get_risk_guardian() -> RiskGuardianAgent:
    """Get or create the global RiskGuardianAgent instance."""
    global _risk_guardian
    if _risk_guardian is None:
        _risk_guardian = RiskGuardianAgent()
    return _risk_guardian


# ═════════════════════════════════════════════════════════════════════════════════
# BACKWARD COMPATIBILITY EXPORTS
# ═════════════════════════════════════════════════════════════════════════════════

def calculate_position_size(
    balance: float,
    confidence: float,
    entry_price: float,
    stop_loss_pct: float = 0.01,
    chat_id: str = None,
    regime_type: str = "UNKNOWN",
    atr: Optional[float] = None,
) -> float:
    """Backward-compatible wrapper for RiskGuardianAgent.calculate_position_size()"""
    return get_risk_guardian().calculate_position_size(
        balance, confidence, entry_price, stop_loss_pct, chat_id, regime_type, atr
    )


def can_open_trade(chat_id: str, is_pending_trigger: bool = False) -> Tuple[bool, str]:
    """Backward-compatible wrapper for RiskGuardianAgent.can_open_trade()"""
    return get_risk_guardian().can_open_trade(chat_id, is_pending_trigger)


def get_risk_state(chat_id: str) -> str:
    """Backward-compatible wrapper for RiskGuardianAgent.get_risk_state()"""
    return get_risk_guardian().get_risk_state(chat_id)


def record_trade_result(chat_id: str, pnl: float) -> None:
    """Backward-compatible wrapper for RiskGuardianAgent.record_trade_result()"""
    return get_risk_guardian().record_trade_result(chat_id, pnl)


def validate_rr_ratio(
    entry_price: float,
    stop_price: float,
    action: str,
    atr: float,
) -> Tuple[bool, float, str]:
    """Backward-compatible wrapper for RiskGuardianAgent.validate_rr_ratio()"""
    return get_risk_guardian().validate_rr_ratio(entry_price, stop_price, action, atr)


def calculate_atr_targets(
    entry_price: float,
    atr: float,
    action: str,
) -> Tuple[float, float]:
    """Backward-compatible wrapper for RiskGuardianAgent.calculate_atr_targets()"""
    return get_risk_guardian().calculate_atr_targets(entry_price, atr, action)


# Export risk states for backward compatibility
__all__ = [
    'RiskGuardianAgent',
    'get_risk_guardian',
    'calculate_position_size',
    'can_open_trade',
    'get_risk_state',
    'record_trade_result',
    'validate_rr_ratio',
    'calculate_atr_targets',
    'STATE_NORMAL',
    'STATE_CIRCUIT_BREAKER',
    'STATE_MANUAL_OVERRIDE',
    'STATE_HARD_BLOCK',
    'STATE_USER_DAY_HALT',
]
