"""
Decision Agent — NATB v2.0 AI Agent Layer

Phase 1-A: Shadow Mode Implementation

This module provides the DecisionAgent class that bridges the gap between
the trained AI models and live trading. It operates in SHADOW MODE,
meaning it observes signals, provides opinions, but does NOT block trades.

Architecture:
  - Loads existing Random Forest models from /root/Nasdaq_SaaS_Project/models/
  - Performs inference on 1d/4h/15m timeframes
  - Combines AI prediction with Technical Strategy result
  - Logs opinions and sends Telegram notifications
  - Never blocks execution (shadow mode safety)
"""

import os
import sys
import json
import logging
import pickle
from typing import Any
from dataclasses import dataclass

import numpy as np
import pandas as pd

# Ensure project root is in path for absolute imports
PROJECT_ROOT = "/root/Nasdaq_SaaS_Project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.ai_model import (
    load_or_train_model,
    evaluate_symbol,
    detect_regime,
    _direction_probability,
    _rule_based_probability,
    _flatten,
    MODEL_VERSION,
)
from bot.notifier import send_telegram_message
from config import ADMIN_CHAT_ID

log = logging.getLogger(__name__)

# Shadow mode flag - ensures agent never blocks trades
SHADOW_MODE = True

# AI Gate thresholds for agent's own decision logic
AI_APPROVE_THRESHOLD = 55.0  # Minimum AI confidence to consider "Approve"
AI_REJECT_THRESHOLD = 45.0   # Below this is a clear "Reject"


@dataclass
class AgentOpinion:
    """Structured opinion from the Decision Agent."""
    symbol: str
    direction: str
    technical_confidence: float
    technical_strategy: str
    ai_confidence: float
    ai_regime: str
    verdict: str  # "APPROVE", "REJECT", or "UNCERTAIN"
    reasoning: str
    shadow_mode: bool = True
    ai_score: float = 0.0
    model_version: int = MODEL_VERSION
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "technical_confidence": self.technical_confidence,
            "technical_strategy": self.technical_strategy,
            "ai_confidence": self.ai_confidence,
            "ai_regime": self.ai_regime,
            "verdict": self.verdict,
            "reasoning": self.reasoning,
            "shadow_mode": self.shadow_mode,
            "ai_score": self.ai_score,
            "model_version": self.model_version,
        }
    
    def to_telegram_format(self) -> str:
        """Format the opinion for Telegram notification."""
        emoji_verdict = "✅" if self.verdict == "APPROVE" else ("❌" if self.verdict == "REJECT" else "⚠️")
        
        return (
            f"🤖 AI Agent Opinion [SHADOW MODE]:\n"
            f"   ├─ Verdict: {emoji_verdict} {self.verdict}\n"
            f"   ├─ AI Confidence: {self.ai_confidence:.1f}%\n"
            f"   ├─ Technical Confidence: {self.technical_confidence:.1f}%\n"
            f"   ├─ Regime: {self.ai_regime}\n"
            f"   └─ Reasoning: {self.reasoning}"
        )


class DecisionAgent:
    """
    Decision Agent for trading signal validation.
    
    CURRENTLY IN SHADOW MODE - Provides opinions without blocking trades.
    
    Usage:
        agent = DecisionAgent()
        opinion = agent.analyze_signal(signal_data, market_data)
        # opinion is logged and sent to Telegram
        # Trade proceeds regardless of verdict
    """
    
    def __init__(self, shadow_mode: bool = True):
        self.shadow_mode = shadow_mode
        self.model_cache = {}
        self.opinion_history = []
        log.info(f"[DecisionAgent] Initialized (shadow_mode={shadow_mode}, model_version={MODEL_VERSION})")
    
    def analyze_signal(
        self,
        signal_data: dict,
        market_data: dict,
    ) -> AgentOpinion:
        """
        Analyze a trading signal using both Technical Strategy and AI Model.
        
        Args:
            signal_data: Dict with keys like 'symbol', 'action', 'confidence', 'strategy_label'
            market_data: Dict with timeframe DataFrames ('1d', '4h', '15m')
            
        Returns:
            AgentOpinion with verdict, confidence, and reasoning
        """
        try:
            return self._perform_analysis(signal_data, market_data)
        except Exception as exc:
            log.error(f"[DecisionAgent] Analysis failed for {signal_data.get('symbol', 'unknown')}: {exc}")
            # Return a fallback opinion that doesn't block trading
            return AgentOpinion(
                symbol=signal_data.get("symbol", "unknown"),
                direction=signal_data.get("action", "unknown"),
                technical_confidence=signal_data.get("confidence", 0.0),
                technical_strategy=signal_data.get("strategy_label", "unknown"),
                ai_confidence=0.0,
                ai_regime="UNKNOWN",
                verdict="UNCERTAIN",
                reasoning=f"Agent analysis failed: {str(exc)[:100]}. Trade proceeds on technical signal only.",
                shadow_mode=self.shadow_mode,
                ai_score=0.0,
            )
    
    def _perform_analysis(
        self,
        signal_data: dict,
        market_data: dict,
    ) -> AgentOpinion:
        """Internal analysis - raises exceptions on failure."""
        
        symbol = str(signal_data.get("symbol", "")).upper()
        direction = str(signal_data.get("action", "")).upper()
        technical_conf = float(signal_data.get("confidence", 0.0))
        technical_strategy = str(signal_data.get("strategy_label", "unknown"))
        
        # Prepare timeframe data for AI model
        timeframe_data = {
            "direction": direction,
            "1d": market_data.get("1d"),
            "4h": market_data.get("4h"),
            "15m": market_data.get("15m"),
        }
        
        # Get AI evaluation
        ai_result = evaluate_symbol(
            symbol=symbol,
            timeframe_data=timeframe_data,
        )
        
        ai_confidence = ai_result.get("confidence", 0.0)
        ai_regime = ai_result.get("regime", "UNKNOWN")
        ai_score = ai_result.get("ai_score", 0.0)
        is_approved_by_ai = ai_result.get("is_approved", False)
        
        # Generate reasoning based on AI vs Technical alignment
        reasoning = self._generate_reasoning(
            symbol=symbol,
            direction=direction,
            technical_conf=technical_conf,
            ai_confidence=ai_confidence,
            ai_regime=ai_regime,
            is_approved_by_ai=is_approved_by_ai,
            technical_strategy=technical_strategy,
        )
        
        # Determine verdict (shadow mode - informational only)
        if is_approved_by_ai and ai_confidence >= AI_APPROVE_THRESHOLD:
            verdict = "APPROVE"
        elif ai_confidence < AI_REJECT_THRESHOLD:
            verdict = "REJECT"
        else:
            verdict = "UNCERTAIN"
        
        opinion = AgentOpinion(
            symbol=symbol,
            direction=direction,
            technical_confidence=technical_conf,
            technical_strategy=technical_strategy,
            ai_confidence=ai_confidence,
            ai_regime=ai_regime,
            verdict=verdict,
            reasoning=reasoning,
            shadow_mode=self.shadow_mode,
            ai_score=ai_score,
        )
        
        # Store in history
        self.opinion_history.append(opinion.to_dict())
        
        # Log the opinion
        log.info(f"[DecisionAgent] {symbol} {direction} | AI: {ai_confidence:.1f}% | Tech: {technical_conf:.1f}% | Verdict: {verdict}")
        
        return opinion
    
    def _generate_reasoning(
        self,
        symbol: str,
        direction: str,
        technical_conf: float,
        ai_confidence: float,
        ai_regime: str,
        is_approved_by_ai: bool,
        technical_strategy: str,
    ) -> str:
        """Generate human-readable reasoning for the opinion."""
        
        parts = []
        
        # AI vs Technical alignment
        conf_diff = abs(ai_confidence - technical_conf)
        
        if ai_confidence > technical_conf + 10:
            parts.append(f"AI model shows higher confidence ({ai_confidence:.0f}%) than technical strategy ({technical_conf:.0f}%)")
        elif technical_conf > ai_confidence + 10:
            parts.append(f"Technical strategy ({technical_conf:.0f}%) outperforms AI model ({ai_confidence:.0f}%)")
        else:
            parts.append(f"AI and technical strategy are aligned (AI: {ai_confidence:.0f}%, Tech: {technical_conf:.0f}%)")
        
        # Regime context
        if ai_regime == "VOLATILE":
            parts.append("Market is in volatile regime — wider stops recommended")
        elif ai_regime == "TRENDING":
            parts.append("Clear trend detected — momentum strategy favorable")
        elif ai_regime == "RANGING":
            parts.append("Ranging market — mean reversion may be effective")
        
        # Strategy context
        parts.append(f"Technical approach: {technical_strategy}")
        
        # Shadow mode disclaimer
        if self.shadow_mode:
            parts.append("[SHADOW MODE] Opinion is advisory only — trade will proceed")
        
        return "; ".join(parts)
    
    def notify_opinion(self, opinion: AgentOpinion, chat_id: str | None = None) -> None:
        """
        Send the agent's opinion to Telegram.
        
        Args:
            opinion: The AgentOpinion to send
            chat_id: Optional specific chat ID, otherwise sends to ADMIN_CHAT_ID
        """
        try:
            message = opinion.to_telegram_format()
            target_chat = chat_id or ADMIN_CHAT_ID
            
            if target_chat:
                send_telegram_message(target_chat, message)
                log.info(f"[DecisionAgent] Opinion sent to Telegram for {opinion.symbol}")
            else:
                log.warning("[DecisionAgent] No chat_id available for Telegram notification")
        except Exception as exc:
            log.error(f"[DecisionAgent] Failed to send Telegram notification: {exc}")
    
    def get_statistics(self) -> dict:
        """Return statistics about the agent's opinions."""
        if not self.opinion_history:
            return {"total_opinions": 0}
        
        total = len(self.opinion_history)
        approvals = sum(1 for o in self.opinion_history if o["verdict"] == "APPROVE")
        rejections = sum(1 for o in self.opinion_history if o["verdict"] == "REJECT")
        uncertain = total - approvals - rejections
        
        avg_ai_conf = sum(o["ai_confidence"] for o in self.opinion_history) / total
        avg_tech_conf = sum(o["technical_confidence"] for o in self.opinion_history) / total
        
        return {
            "total_opinions": total,
            "approvals": approvals,
            "rejections": rejections,
            "uncertain": uncertain,
            "avg_ai_confidence": round(avg_ai_conf, 2),
            "avg_technical_confidence": round(avg_tech_conf, 2),
            "shadow_mode": self.shadow_mode,
        }
    
    def clear_history(self) -> None:
        """Clear the opinion history."""
        self.opinion_history.clear()
        log.info("[DecisionAgent] Opinion history cleared")


# Global singleton instance for convenience
_agent_instance: DecisionAgent | None = None


def get_decision_agent(shadow_mode: bool = True) -> DecisionAgent:
    """Get or create the global DecisionAgent singleton."""
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = DecisionAgent(shadow_mode=shadow_mode)
    return _agent_instance


def analyze_signal_shadow(
    signal_data: dict,
    market_data: dict,
    chat_id: str | None = None,
) -> AgentOpinion:
    """
    Convenience function for shadow mode analysis.
    
    This function is safe to call from anywhere - it never raises exceptions
    and never blocks trading decisions.
    
    Args:
        signal_data: Technical strategy signal dict
        market_data: Market timeframe data
        chat_id: Optional Telegram chat ID for notification
        
    Returns:
        AgentOpinion with the agent's assessment
    """
    agent = get_decision_agent(shadow_mode=True)
    opinion = agent.analyze_signal(signal_data, market_data)
    agent.notify_opinion(opinion, chat_id=chat_id)
    return opinion


if __name__ == "__main__":
    # Test the DecisionAgent
    print("[TEST] DecisionAgent initialized in Shadow Mode")
    agent = DecisionAgent(shadow_mode=True)
    print(f"Shadow Mode: {agent.shadow_mode}")
    print(f"Model Version: {MODEL_VERSION}")
    print("[TEST] Ready for integration")
