"""
Decision Agent — NATB v2.0 AI Agent Layer

Phase 3-A: Active Gating — Multi-Agent Committee with Trade Blocking

This module implements a Multi-Agent "Committee" structure with 3 Expert Personas:
  1. Technical Analyst: RSI, ADX, Bollinger Bands alignment
  2. Trend Strategist: EMA 20/50/200 and price action (Higher Highs/Lower Lows)
  3. Memory Historian: Symbol-specific success rates from agent_memory

The Lead Coordinator synthesizes expert reports into a final Committee Consensus.

Architecture:
  - Committee of specialized agents debate the signal
  - Each expert generates a brief "Agent Report"
  - Lead Coordinator synthesizes reports into final verdict
  - ACTIVE GATING: Committee can block trades (verdict = REJECT)
  - All paths use absolute: /root/Nasdaq_SaaS_Project/

Safety:
  - SHADOW_MODE = False (ACTIVE GATING — committee can block trades)
  - Emergency Bypass: On agent error, defaults to APPROVE (don't freeze)
  - All agent operations wrapped in try-except
  - Trade execution blocked ONLY on explicit REJECT verdict
"""

import os
import sys
import json
import logging
import pickle
from typing import Any
from dataclasses import dataclass

import requests
from datetime import datetime, timedelta, timezone

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
from config import ADMIN_CHAT_ID, NEWS_API_KEY, FMP_API_KEY

# Phase 1-B: Import Agent Memory for contextual reasoning
try:
    from core.agent_memory import get_symbol_insights, record_opinion, AgentMemory, SymbolInsights
    _AGENT_MEMORY_AVAILABLE = True
except Exception as _mem_err:
    _AGENT_MEMORY_AVAILABLE = False
    log.warning(f"[DecisionAgent] AgentMemory not available: {_mem_err}")

log = logging.getLogger(__name__)

# =============================================================================
# PHASE 3-A: ACTIVE GATING CONFIGURATION
# =============================================================================

# SHADOW_MODE = False enables active gating (committee can block trades)
# Set to True to return to shadow mode (opinions only, never blocks)
SHADOW_MODE = False

# Emergency Bypass: If True, agent errors default to APPROVE (safety valve)
EMERGENCY_BYPASS_ON_ERROR = True

# AI Gate thresholds for the committee's decision logic
COMMITTEE_APPROVE_THRESHOLD = 2  # At least 2 experts must approve
CONFIDENCE_THRESHOLD = 55.0  # Minimum confidence for an expert to approve


def is_gating_active() -> bool:
    """Check if the committee is in active gating mode (can block trades)."""
    return not SHADOW_MODE

# =============================================================================
# PHASE 2-A: Multi-Agent Committee Dataclasses
# =============================================================================

@dataclass
class ExpertReport:
    """Report from a single expert agent."""
    expert_name: str
    stance: str  # "BULLISH", "BEARISH", "NEUTRAL", "POSITIVE", "NEGATIVE", "NO_DATA"
    confidence: float  # 0-100
    key_points: list[str]
    
    def to_dict(self) -> dict:
        return {
            "expert_name": self.expert_name,
            "stance": self.stance,
            "confidence": self.confidence,
            "key_points": self.key_points,
        }


@dataclass
class CommitteeConsensus:
    """
    Final synthesized decision from the Multi-Agent Committee.
    
    Phase 4-A: Added Sentiment Analyst to the committee.
    """
    symbol: str
    direction: str
    technical_analyst_report: ExpertReport
    trend_strategist_report: ExpertReport
    memory_historian_report: ExpertReport
    sentiment_analyst_report: ExpertReport  # Phase 4-A: News sentiment
    final_verdict: str  # "APPROVE", "REJECT", "UNCERTAIN"
    consensus_confidence: float
    debate_summary: str
    shadow_mode: bool = True
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "technical_analyst": self.technical_analyst_report.to_dict(),
            "trend_strategist": self.trend_strategist_report.to_dict(),
            "memory_historian": self.memory_historian_report.to_dict(),
            "sentiment_analyst": self.sentiment_analyst_report.to_dict(),  # Phase 4-A
            "final_verdict": self.final_verdict,
            "consensus_confidence": self.consensus_confidence,
            "debate_summary": self.debate_summary,
            "shadow_mode": self.shadow_mode,
        }
    
    def to_telegram_format(self) -> str:
        """Format the committee consensus for Telegram notification."""
        emoji_verdict = "✅" if self.final_verdict == "APPROVE" else ("❌" if self.final_verdict == "REJECT" else "⚠️")
        
        ta = self.technical_analyst_report
        ts = self.trend_strategist_report
        mh = self.memory_historian_report
        sa = self.sentiment_analyst_report  # Phase 4-A
        
        # Emoji mapping for stances
        stance_emoji = {
            "BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪",
            "ALIGNED": "✅", "COUNTER_TREND": "⚠️", "POSITIVE": "📈",
            "NEGATIVE": "📉", "NO_DATA": "❓",
            "HIGH_POSITIVE": "🚀", "HIGH_NEGATIVE": "🔻",
        }
        
        # Build sentiment line with headline preview if available
        sentiment_line = f"📰 Sentiment Analyst: {stance_emoji.get(sa.stance, '⚪')} {sa.stance}"
        if sa.key_points and sa.key_points[0].startswith("Headline:"):
            headline_preview = sa.key_points[0][9:][:40]  # Remove "Headline: " prefix, limit to 40 chars
            sentiment_line += f"\n   │   └─ 💬 {headline_preview}..."
        
        mode_str = "SHADOW MODE" if self.shadow_mode else "ACTIVE GATING"
        
        return (
            f"🤖 COMMITTEE DECISION [{mode_str}]:\n"
            f"   ├─ 📊 Technical Analyst: {stance_emoji.get(ta.stance, '⚪')} {ta.stance} ({ta.confidence:.0f}%)\n"
            f"   ├─ 📈 Trend Strategist: {stance_emoji.get(ts.stance, '⚪')} {ts.stance} ({ts.confidence:.0f}%)\n"
            f"   ├─ 📚 Memory Historian: {stance_emoji.get(mh.stance, '⚪')} {mh.stance}\n"
            f"   ├─ {sentiment_line}\n"  # Phase 4-A: Sentiment line with headline
            f"   ├─ ⚖️ Final Verdict: {emoji_verdict} {self.final_verdict}\n"
            f"   └─ 💡 Summary: {self.debate_summary}"
        )


# =============================================================================
# EXPERT AGENT CLASSES
# =============================================================================

class TechnicalAnalyst:
    """
    Expert Persona 1: Technical Analyst
    
    Focuses on:
      - RSI (overbought/oversold conditions)
      - ADX (trend strength)
      - Bollinger Bands (volatility and mean reversion)
      - AI model technical confidence as primary input
    """
    
    def __init__(self):
        self.name = "Technical Analyst"
    
    def analyze(
        self,
        symbol: str,
        direction: str,
        market_data: dict,
        ai_confidence: float,
        ai_regime: str,
    ) -> ExpertReport:
        """
        Generate technical analysis report.
        
        Uses RSI, ADX, Bollinger Bands alignment to determine stance.
        """
        key_points = []
        
        # Get 15m data for technical indicators
        df_15m = market_data.get("15m")
        
        if df_15m is not None and len(df_15m) >= 20:
            try:
                # Calculate RSI
                rsi = self._calculate_rsi(df_15m)
                
                # Calculate ADX
                adx = self._calculate_adx(df_15m)
                
                # Calculate Bollinger Bands position
                bb_position = self._calculate_bb_position(df_15m)
                
                # Analyze based on direction
                if direction == "BUY":
                    stance, confidence = self._analyze_buy_signal(
                        rsi, adx, bb_position, ai_confidence, ai_regime
                    )
                else:  # SELL
                    stance, confidence = self._analyze_sell_signal(
                        rsi, adx, bb_position, ai_confidence, ai_regime
                    )
                
                # Build key points
                if rsi is not None:
                    key_points.append(f"RSI: {rsi:.1f}")
                if adx is not None:
                    key_points.append(f"ADX: {adx:.1f} ({'strong trend' if adx > 25 else 'weak trend'})")
                if bb_position is not None:
                    bb_desc = "lower band" if bb_position < 0.2 else ("upper band" if bb_position > 0.8 else "middle")
                    key_points.append(f"BB position: {bb_desc}")
                
            except Exception as exc:
                log.warning(f"[{self.name}] Analysis failed for {symbol}: {exc}")
                stance = "NEUTRAL"
                confidence = 50.0
                key_points.append("Indicator analysis failed")
        else:
            # Fallback to AI model confidence
            stance = "BULLISH" if ai_confidence > 55 else ("BEARISH" if ai_confidence < 45 else "NEUTRAL")
            confidence = ai_confidence
            key_points.append("Insufficient price data, using AI model confidence")
        
        return ExpertReport(
            expert_name=self.name,
            stance=stance,
            confidence=confidence,
            key_points=key_points,
        )
    
    def _calculate_rsi(self, df: pd.DataFrame, period: int = 14) -> float | None:
        """Calculate RSI from price data."""
        try:
            df = df.copy()
            df['close'] = pd.to_numeric(df['Close'], errors='coerce')
            delta = df['close'].diff()
            gain = delta.where(delta > 0, 0).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            return float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else None
        except Exception:
            return None
    
    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> float | None:
        """Calculate ADX from price data."""
        try:
            df = df.copy()
            high = pd.to_numeric(df['High'], errors='coerce')
            low = pd.to_numeric(df['Low'], errors='coerce')
            close = pd.to_numeric(df['Close'], errors='coerce')
            
            tr1 = high - low
            tr2 = abs(high - close.shift())
            tr3 = abs(low - close.shift())
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            atr = tr.rolling(window=period).mean()
            
            plus_dm = (high - high.shift()).where((high - high.shift()) > (low.shift() - low), 0)
            minus_dm = (low.shift() - low).where((low.shift() - low) > (high - high.shift()), 0)
            
            plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
            minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)
            dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
            adx = dx.rolling(window=period).mean()
            
            return float(adx.iloc[-1]) if not pd.isna(adx.iloc[-1]) else None
        except Exception:
            return None
    
    def _calculate_bb_position(self, df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> float | None:
        """Calculate position within Bollinger Bands (0-1 scale)."""
        try:
            df = df.copy()
            close = pd.to_numeric(df['Close'], errors='coerce')
            sma = close.rolling(window=period).mean()
            std = close.rolling(window=period).std()
            upper = sma + (std * std_dev)
            lower = sma - (std * std_dev)
            
            position = (close.iloc[-1] - lower.iloc[-1]) / (upper.iloc[-1] - lower.iloc[-1])
            return float(position) if not pd.isna(position) else None
        except Exception:
            return None
    
    def _analyze_buy_signal(
        self, rsi: float | None, adx: float | None, bb_position: float | None,
        ai_confidence: float, ai_regime: str
    ) -> tuple[str, float]:
        """Analyze BUY signal technical conditions."""
        score = 0
        max_score = 4
        
        # RSI: oversold is good for buy (RSI < 40)
        if rsi is not None:
            if rsi < 35:
                score += 2  # Strong oversold
            elif rsi < 45:
                score += 1  # Mild oversold
            elif rsi > 70:
                score -= 1  # Overbought warning
        
        # ADX: trend strength
        if adx is not None:
            if adx > 25:
                score += 1  # Good trend strength
        
        # Bollinger Bands: near lower band is good for buy
        if bb_position is not None:
            if bb_position < 0.2:
                score += 1  # Near lower band
            elif bb_position > 0.9:
                score -= 1  # Near upper band (warning)
        
        # AI confidence contribution
        if ai_confidence > 60:
            score += 1
        
        # Determine stance
        if score >= 3:
            return "BULLISH", min(85 + ai_confidence * 0.15, 95)
        elif score >= 1:
            return "BULLISH", min(60 + ai_confidence * 0.2, 75)
        elif score <= -1:
            return "BEARISH", max(40, ai_confidence * 0.5)
        else:
            return "NEUTRAL", ai_confidence
    
    def _analyze_sell_signal(
        self, rsi: float | None, adx: float | None, bb_position: float | None,
        ai_confidence: float, ai_regime: str
    ) -> tuple[str, float]:
        """Analyze SELL signal technical conditions."""
        score = 0
        
        # RSI: overbought is good for sell (RSI > 60)
        if rsi is not None:
            if rsi > 65:
                score += 2  # Strong overbought
            elif rsi > 55:
                score += 1  # Mild overbought
            elif rsi < 30:
                score -= 1  # Oversold warning
        
        # ADX: trend strength
        if adx is not None:
            if adx > 25:
                score += 1
        
        # Bollinger Bands: near upper band is good for sell
        if bb_position is not None:
            if bb_position > 0.8:
                score += 1
            elif bb_position < 0.1:
                score -= 1
        
        # AI confidence
        if ai_confidence > 60:
            score += 1
        
        # Determine stance
        if score >= 3:
            return "BEARISH", min(85 + ai_confidence * 0.15, 95)
        elif score >= 1:
            return "BEARISH", min(60 + ai_confidence * 0.2, 75)
        elif score <= -1:
            return "BULLISH", max(40, ai_confidence * 0.5)
        else:
            return "NEUTRAL", ai_confidence


class TrendStrategist:
    """
    Expert Persona 2: Trend Strategist
    
    Focuses on:
      - EMA 20/50/200 alignment (golden cross / death cross)
      - Price action: Higher Highs / Lower Lows
      - Trend direction vs signal direction alignment
    """
    
    def __init__(self):
        self.name = "Trend Strategist"
    
    def analyze(
        self,
        symbol: str,
        direction: str,
        market_data: dict,
        ai_confidence: float,
    ) -> ExpertReport:
        """Generate trend analysis report."""
        key_points = []
        
        # Analyze multiple timeframes
        df_1d = market_data.get("1d")
        df_4h = market_data.get("4h")
        df_15m = market_data.get("15m")
        
        trend_alignment = 0  # Positive = aligned with signal, Negative = counter-trend
        
        try:
            # 1D Trend (major trend)
            trend_1d, ema_aligned_1d = self._analyze_timeframe_trend(df_1d, direction)
            
            # 4H Trend (intermediate)
            trend_4h, ema_aligned_4h = self._analyze_timeframe_trend(df_4h, direction)
            
            # 15M Trend (immediate)
            trend_15m, ema_aligned_15m = self._analyze_timeframe_trend(df_15m, direction)
            
            # Price action analysis
            price_action = self._analyze_price_action(df_15m)
            
            # Calculate trend alignment score
            for trend, aligned in [(trend_1d, ema_aligned_1d), (trend_4h, ema_aligned_4h), (trend_15m, ema_aligned_15m)]:
                if trend != "UNKNOWN":
                    if aligned:
                        trend_alignment += 1
                    else:
                        trend_alignment -= 1
            
            # Build key points
            if trend_1d != "UNKNOWN":
                key_points.append(f"1D Trend: {trend_1d} {'✓' if ema_aligned_1d else '✗'}")
            if trend_4h != "UNKNOWN":
                key_points.append(f"4H Trend: {trend_4h} {'✓' if ema_aligned_4h else '✗'}")
            if trend_15m != "UNKNOWN":
                key_points.append(f"15M Trend: {trend_15m} {'✓' if ema_aligned_15m else '✗'}")
            if price_action != "UNKNOWN":
                key_points.append(f"Price action: {price_action}")
            
            # Determine stance
            if trend_alignment >= 2:
                stance = "ALIGNED"
                confidence = min(70 + trend_alignment * 5, 90)
            elif trend_alignment >= 0:
                stance = "ALIGNED"
                confidence = min(55 + trend_alignment * 5, 65)
            elif trend_alignment >= -1:
                stance = "NEUTRAL"
                confidence = 50
            else:
                stance = "COUNTER_TREND"
                confidence = max(30, 50 + trend_alignment * 5)
            
        except Exception as exc:
            log.warning(f"[{self.name}] Analysis failed for {symbol}: {exc}")
            stance = "NEUTRAL"
            confidence = 50.0
            key_points.append("Trend analysis failed")
        
        return ExpertReport(
            expert_name=self.name,
            stance=stance,
            confidence=confidence,
            key_points=key_points,
        )
    
    def _analyze_timeframe_trend(
        self, df: pd.DataFrame | None, signal_direction: str, period_fast: int = 20, period_slow: int = 50
    ) -> tuple[str, bool]:
        """
        Analyze trend for a single timeframe.
        
        Returns:
            (trend_description, is_aligned_with_signal)
        """
        if df is None or len(df) < period_slow + 5:
            return "UNKNOWN", False
        
        try:
            df = df.copy()
            close = pd.to_numeric(df['Close'], errors='coerce')
            
            # Calculate EMAs
            ema_fast = close.ewm(span=period_fast, adjust=False).mean()
            ema_slow = close.ewm(span=period_slow, adjust=False).mean()
            
            current_price = close.iloc[-1]
            fast_val = ema_fast.iloc[-1]
            slow_val = ema_slow.iloc[-1]
            
            # Determine trend
            if fast_val > slow_val and current_price > fast_val:
                trend = "UPTREND"
            elif fast_val < slow_val and current_price < fast_val:
                trend = "DOWNTREND"
            elif fast_val > slow_val:
                trend = "BULLISH_BIAS"
            elif fast_val < slow_val:
                trend = "BEARISH_BIAS"
            else:
                trend = "SIDEWAYS"
            
            # Check alignment
            is_aligned = False
            if signal_direction == "BUY":
                is_aligned = trend in ["UPTREND", "BULLISH_BIAS"]
            else:  # SELL
                is_aligned = trend in ["DOWNTREND", "BEARISH_BIAS"]
            
            return trend, is_aligned
            
        except Exception:
            return "UNKNOWN", False
    
    def _analyze_price_action(self, df: pd.DataFrame | None) -> str:
        """Analyze Higher Highs / Lower Lows pattern."""
        if df is None or len(df) < 10:
            return "UNKNOWN"
        
        try:
            df = df.copy()
            high = pd.to_numeric(df['High'], errors='coerce')
            low = pd.to_numeric(df['Low'], errors='coerce')
            
            # Check last 5 candles
            recent_highs = high.iloc[-5:]
            recent_lows = low.iloc[-5:]
            
            # Higher Highs detection
            hh_count = 0
            for i in range(1, len(recent_highs)):
                if recent_highs.iloc[i] > recent_highs.iloc[i-1]:
                    hh_count += 1
            
            # Higher Lows detection
            hl_count = 0
            for i in range(1, len(recent_lows)):
                if recent_lows.iloc[i] > recent_lows.iloc[i-1]:
                    hl_count += 1
            
            # Lower Highs / Lower Lows
            lh_count = 0
            ll_count = 0
            for i in range(1, len(recent_highs)):
                if recent_highs.iloc[i] < recent_highs.iloc[i-1]:
                    lh_count += 1
            for i in range(1, len(recent_lows)):
                if recent_lows.iloc[i] < recent_lows.iloc[i-1]:
                    ll_count += 1
            
            if hh_count >= 3 and hl_count >= 3:
                return "Higher Highs & Lows"
            elif lh_count >= 3 and ll_count >= 3:
                return "Lower Highs & Lows"
            elif hh_count >= 3:
                return "Higher Highs"
            elif ll_count >= 3:
                return "Lower Lows"
            else:
                return "Mixed/Consolidating"
                
        except Exception:
            return "UNKNOWN"


class MemoryHistorian:
    """
    Expert Persona 3: Memory Historian
    
    Focuses on:
      - Symbol-specific success rates from agent_memory
      - Historical performance when Agent approved/rejected
      - Recent trend in performance
    """
    
    def __init__(self):
        self.name = "Memory Historian"
    
    def analyze(
        self,
        symbol: str,
        direction: str,
    ) -> ExpertReport:
        """Generate historical analysis report."""
        key_points = []
        
        if not _AGENT_MEMORY_AVAILABLE:
            return ExpertReport(
                expert_name=self.name,
                stance="NO_DATA",
                confidence=50.0,
                key_points=["Memory system not available"],
            )
        
        try:
            insights = get_symbol_insights(symbol, direction=direction, lookback_days=30)
            
            if insights is None:
                return ExpertReport(
                    expert_name=self.name,
                    stance="NO_DATA",
                    confidence=50.0,
                    key_points=["No historical data for this symbol"],
                )
            
            # Build key points
            key_points.append(f"Total experiences: {insights.total_opinions}")
            
            if insights.success_rate_when_approved is not None:
                key_points.append(f"Success rate when approved: {insights.success_rate_when_approved:.0f}%")
            
            key_points.append(f"Recent trend: {insights.recent_trend}")
            
            # Determine stance based on success rate
            if insights.total_opinions < 3:
                stance = "NO_DATA"
                confidence = 50.0
            elif insights.success_rate_when_approved is None:
                stance = "NO_DATA"
                confidence = 50.0
            elif insights.success_rate_when_approved >= 75:
                stance = "POSITIVE"
                confidence = min(70 + (insights.success_rate_when_approved - 75) * 0.5, 90)
            elif insights.success_rate_when_approved >= 60:
                stance = "POSITIVE"
                confidence = 60 + (insights.success_rate_when_approved - 60) * 0.3
            elif insights.success_rate_when_approved >= 40:
                stance = "NEUTRAL"
                confidence = 50.0
            else:
                stance = "NEGATIVE"
                confidence = max(30, 50 - (40 - insights.success_rate_when_approved) * 0.5)
            
            # Adjust for trend
            if insights.recent_trend == "IMPROVING":
                confidence = min(confidence + 5, 95)
            elif insights.recent_trend == "DECLINING":
                confidence = max(confidence - 5, 20)
            
        except Exception as exc:
            log.warning(f"[{self.name}] Analysis failed for {symbol}: {exc}")
            stance = "NO_DATA"
            confidence = 50.0
            key_points.append(f"Memory query failed: {str(exc)[:50]}")
        
        return ExpertReport(
            expert_name=self.name,
            stance=stance,
            confidence=confidence,
            key_points=key_points,
        )


class SentimentAnalyst:
    """
    Expert Persona 4: Sentiment Analyst (Phase 4-A)
    
    Analyzes real-time news sentiment using:
      - NewsAPI (general financial news)
      - FMP (Financial Modeling Prep) News & Press Releases
    
    Stance Logic:
      - HIGH_POSITIVE: Strong bullish sentiment -> Boosts committee approval
      - POSITIVE: Moderate bullish sentiment
      - NEUTRAL: No significant news or mixed sentiment
      - NEGATIVE: Bearish sentiment detected -> May force REJECT
      - HIGH_NEGATIVE: Strong bearish news -> Forces REJECT
      - NO_DATA: API unavailable or timeout
    
    Safety:
      - 3-second timeout on all API calls
      - Defaults to NEUTRAL on any failure
      - Respects rate limits with minimal calls
    """
    
    # Timeout for API calls (seconds)
    API_TIMEOUT = 3
    
    # Lookback window for news (hours)
    NEWS_LOOKBACK_HOURS = 24
    
    # Positive/Bullish keywords (case-insensitive)
    BULLISH_KEYWORDS = [
        "upgrade", "beat", "outperform", "strong buy", "buy", "bullish",
        "growth", "surge", "rally", "breakthrough", "partnership", "deal",
        "expansion", "record revenue", "profit", "exceeds expectations",
        "positive", "optimistic", "momentum", "uptrend", "support"
    ]
    
    # Negative/Bearish keywords (case-insensitive)
    BEARISH_KEYWORDS = [
        "downgrade", "miss", "underperform", "sell", "bearish", "bear",
        "decline", "drop", "plunge", "crash", "layoff", "layoffs",
        "investigation", "lawsuit", "fine", "penalty", "recall",
        "bankruptcy", "debt", "loss", "losses", "negative", "pessimistic",
        "resistance", "downtrend", "warning", "alert", "concern"
    ]
    
    def __init__(self):
        self.name = "Sentiment Analyst"
        self._cache = {}  # Simple cache: {symbol: (timestamp, report)}
        self._cache_ttl_seconds = 300  # 5-minute cache
    
    def analyze(self, symbol: str, direction: str) -> ExpertReport:
        """
        Analyze news sentiment for the given symbol.
        
        Args:
            symbol: Stock symbol
            direction: "BUY" or "SELL" (signal direction)
        
        Returns:
            ExpertReport with sentiment stance and confidence
        """
        key_points = []
        
        # Check cache first
        cached = self._get_cached(symbol)
        if cached:
            return cached
        
        # Check if APIs are available
        has_news_api = bool(NEWS_API_KEY)
        has_fmp_api = bool(FMP_API_KEY)
        
        if not has_news_api and not has_fmp_api:
            return ExpertReport(
                expert_name=self.name,
                stance="NO_DATA",
                confidence=50.0,
                key_points=["No API keys configured for sentiment analysis"],
            )
        
        try:
            # Aggregate sentiment from available sources
            all_headlines = []
            sentiment_scores = []
            sources_used = []
            
            # Try FMP News API (more reliable for financial data)
            if has_fmp_api:
                fmp_headlines, fmp_score = self._fetch_fmp_news(symbol)
                if fmp_headlines:
                    all_headlines.extend(fmp_headlines)
                    sentiment_scores.append(fmp_score)
                    sources_used.append("FMP")
                    key_points.append(f"FMP: {len(fmp_headlines)} articles")
            
            # Try NewsAPI as supplement
            if has_news_api:
                newsapi_headlines, newsapi_score = self._fetch_newsapi(symbol)
                if newsapi_headlines:
                    all_headlines.extend(newsapi_headlines)
                    sentiment_scores.append(newsapi_score)
                    sources_used.append("NewsAPI")
                    key_points.append(f"NewsAPI: {len(newsapi_headlines)} articles")
            
            # Calculate aggregate sentiment
            if sentiment_scores:
                avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
                stance, confidence = self._sentiment_to_stance(avg_sentiment, direction)
                
                # Add representative headline to key points
                if all_headlines:
                    most_relevant = self._pick_most_relevant_headline(all_headlines, direction)
                    if most_relevant:
                        key_points.insert(0, f"Headline: {most_relevant[:60]}...")
                
                # Create report
                report = ExpertReport(
                    expert_name=self.name,
                    stance=stance,
                    confidence=confidence,
                    key_points=key_points,
                )
                
                # Cache the result
                self._cache_report(symbol, report)
                
                return report
            else:
                # No news found
                return ExpertReport(
                    expert_name=self.name,
                    stance="NEUTRAL",
                    confidence=50.0,
                    key_points=["No recent news found"] + key_points,
                )
                
        except Exception as exc:
            log.warning(f"[{self.name}] Analysis failed for {symbol}: {exc}")
            return ExpertReport(
                expert_name=self.name,
                stance="NEUTRAL",  # Default to neutral on error
                confidence=50.0,
                key_points=[f"Sentiment analysis failed: {str(exc)[:50]}"],
            )
    
    def _fetch_fmp_news(self, symbol: str) -> tuple[list[str], float]:
        """
        Fetch news from Financial Modeling Prep API.
        
        Returns: (headlines_list, sentiment_score)
        """
        headlines = []
        total_score = 0
        count = 0
        
        try:
            # FMP Stock News endpoint
            url = f"https://financialmodelingprep.com/api/v3/stock_news"
            params = {
                "tickers": symbol,
                "limit": 10,
                "apikey": FMP_API_KEY,
            }
            
            response = requests.get(url, params=params, timeout=self.API_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json()
                
                # Filter to recent news only
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=self.NEWS_LOOKBACK_HOURS)
                
                for article in data:
                    if isinstance(article, dict):
                        title = article.get("title", "")
                        text = article.get("text", "")
                        published = article.get("publishedDate", "")
                        
                        # Check if recent
                        try:
                            if published:
                                pub_time = datetime.fromisoformat(published.replace("Z", "+00:00"))
                                if pub_time < cutoff_time:
                                    continue
                        except:
                            pass  # Include if date parsing fails
                        
                        combined_text = f"{title} {text}".lower()
                        headlines.append(title)
                        
                        # Score sentiment
                        score = self._score_text_sentiment(combined_text)
                        total_score += score
                        count += 1
            
            avg_score = total_score / count if count > 0 else 0
            return headlines, avg_score
            
        except requests.Timeout:
            log.warning(f"[{self.name}] FMP API timeout for {symbol}")
            return [], 0
        except Exception as exc:
            log.warning(f"[{self.name}] FMP API error for {symbol}: {exc}")
            return [], 0
    
    def _fetch_newsapi(self, symbol: str) -> tuple[list[str], float]:
        """
        Fetch news from NewsAPI.
        
        Returns: (headlines_list, sentiment_score)
        """
        headlines = []
        total_score = 0
        count = 0
        
        try:
            # NewsAPI everything endpoint
            url = "https://newsapi.org/v2/everything"
            
            # Calculate date range (last 24 hours)
            to_date = datetime.now(timezone.utc)
            from_date = to_date - timedelta(hours=self.NEWS_LOOKBACK_HOURS)
            
            params = {
                "q": f"{symbol} stock OR earnings OR revenue OR financial",
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 10,
                "from": from_date.strftime("%Y-%m-%d"),
                "to": to_date.strftime("%Y-%m-%d"),
                "apiKey": NEWS_API_KEY,
            }
            
            response = requests.get(url, params=params, timeout=self.API_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])
                
                for article in articles:
                    title = article.get("title", "")
                    description = article.get("description", "")
                    
                    combined_text = f"{title} {description}".lower()
                    headlines.append(title)
                    
                    # Score sentiment
                    score = self._score_text_sentiment(combined_text)
                    total_score += score
                    count += 1
            
            avg_score = total_score / count if count > 0 else 0
            return headlines, avg_score
            
        except requests.Timeout:
            log.warning(f"[{self.name}] NewsAPI timeout for {symbol}")
            return [], 0
        except Exception as exc:
            log.warning(f"[{self.name}] NewsAPI error for {symbol}: {exc}")
            return [], 0
    
    def _score_text_sentiment(self, text: str) -> float:
        """
        Score text sentiment from -1.0 (very bearish) to +1.0 (very bullish).
        
        Returns 0 for neutral sentiment.
        """
        text_lower = text.lower()
        
        bullish_count = sum(1 for kw in self.BULLISH_KEYWORDS if kw in text_lower)
        bearish_count = sum(1 for kw in self.BEARISH_KEYWORDS if kw in text_lower)
        
        # Simple scoring: +0.2 per bullish keyword, -0.2 per bearish
        # Cap at ±1.0
        score = (bullish_count * 0.2) - (bearish_count * 0.2)
        return max(-1.0, min(1.0, score))
    
    def _sentiment_to_stance(self, sentiment_score: float, direction: str) -> tuple[str, float]:
        """
        Convert sentiment score to committee stance and confidence.
        
        Args:
            sentiment_score: -1.0 to +1.0
            direction: "BUY" or "SELL" (the signal direction)
        
        Returns:
            (stance, confidence)
        """
        # Sentiment alignment with signal direction
        if direction == "BUY":
            # For BUY signals, positive sentiment is supportive
            if sentiment_score >= 0.6:
                return "HIGH_POSITIVE", min(85 + sentiment_score * 10, 95)
            elif sentiment_score >= 0.2:
                return "POSITIVE", min(65 + sentiment_score * 20, 80)
            elif sentiment_score >= -0.2:
                return "NEUTRAL", 50.0
            elif sentiment_score >= -0.6:
                return "NEGATIVE", max(40 - abs(sentiment_score) * 10, 30)
            else:
                return "HIGH_NEGATIVE", max(25 - abs(sentiment_score) * 5, 15)
        else:  # SELL
            # For SELL signals, negative sentiment is supportive
            if sentiment_score <= -0.6:
                return "HIGH_POSITIVE", min(85 + abs(sentiment_score) * 10, 95)
            elif sentiment_score <= -0.2:
                return "POSITIVE", min(65 + abs(sentiment_score) * 20, 80)
            elif sentiment_score <= 0.2:
                return "NEUTRAL", 50.0
            elif sentiment_score <= 0.6:
                return "NEGATIVE", max(40 - sentiment_score * 10, 30)
            else:
                return "HIGH_NEGATIVE", max(25 - sentiment_score * 5, 15)
    
    def _pick_most_relevant_headline(self, headlines: list[str], direction: str) -> str:
        """Pick the most sentiment-relevant headline for display."""
        if not headlines:
            return ""
        
        # Score each headline and pick the most extreme (most informative)
        best_headline = headlines[0]
        best_score = abs(self._score_text_sentiment(best_headline.lower()))
        
        for headline in headlines[1:]:
            score = abs(self._score_text_sentiment(headline.lower()))
            if score > best_score:
                best_score = score
                best_headline = headline
        
        return best_headline
    
    def _get_cached(self, symbol: str) -> ExpertReport | None:
        """Check if we have a recent cached report for this symbol."""
        if symbol in self._cache:
            timestamp, report = self._cache[symbol]
            if (datetime.now(timezone.utc) - timestamp).seconds < self._cache_ttl_seconds:
                return report
            else:
                del self._cache[symbol]
        return None
    
    def _cache_report(self, symbol: str, report: ExpertReport) -> None:
        """Cache the report with current timestamp."""
        self._cache[symbol] = (datetime.now(timezone.utc), report)


@dataclass
class AgentOpinion:
    """
    Structured opinion from the Decision Agent.
    
    Phase 3-A: Active Gating — includes is_approved for trade execution control.
    """
    symbol: str
    direction: str
    technical_confidence: float
    technical_strategy: str
    ai_confidence: float
    ai_regime: str
    verdict: str  # "APPROVE", "REJECT", or "UNCERTAIN"
    reasoning: str
    shadow_mode: bool = False  # Phase 3-A: Default to active gating
    ai_score: float = 0.0
    model_version: int = MODEL_VERSION
    
    # Phase 2-A: Committee data
    committee_consensus: CommitteeConsensus | None = None
    
    @property
    def is_approved(self) -> bool:
        """
        Phase 3-A: Active Gating property.
        
        Returns True if trade should proceed, False if blocked.
        
        Logic:
          - APPROVE: True (trade proceeds)
          - UNCERTAIN: True (trade proceeds with caution)
          - REJECT: False (trade blocked)
          - ERROR: Depends on EMERGENCY_BYPASS_ON_ERROR (default True = approve)
        """
        if self.verdict == "REJECT":
            return False
        if self.verdict == "ERROR":
            # Emergency bypass: on error, default to approve to avoid freezing
            return EMERGENCY_BYPASS_ON_ERROR
        # APPROVE, UNCERTAIN, or any other state: allow trade
        return True
    
    @property
    def is_blocked(self) -> bool:
        """Convenience property — True if trade was blocked by committee."""
        return not self.is_approved
    
    def to_dict(self) -> dict:
        result = {
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
            "is_approved": self.is_approved,
            "is_blocked": self.is_blocked,
        }
        if self.committee_consensus:
            result["committee"] = self.committee_consensus.to_dict()
        return result
    
    def to_telegram_format(self, blocked_notification: bool = False) -> str:
        """
        Format for Telegram.
        
        Args:
            blocked_notification: If True, format as a trade block alert
        """
        if blocked_notification:
            # Phase 3-A: Blocked trade notification format
            return self._to_blocked_telegram_format()
        
        # Standard committee format for approved/uncertain trades
        if self.committee_consensus:
            return self.committee_consensus.to_telegram_format()
        
        # Fallback to legacy format
        emoji_verdict = "✅" if self.verdict == "APPROVE" else ("❌" if self.verdict == "REJECT" else "⚠️")
        mode_tag = "[SHADOW MODE]" if self.shadow_mode else "[ACTIVE GATING]"
        return (
            f"🤖 AI Agent Opinion {mode_tag}:\n"
            f"   ├─ Verdict: {emoji_verdict} {self.verdict}\n"
            f"   ├─ AI Confidence: {self.ai_confidence:.1f}%\n"
            f"   ├─ Technical Confidence: {self.technical_confidence:.1f}%\n"
            f"   ├─ Regime: {self.ai_regime}\n"
            f"   └─ Reasoning: {self.reasoning}"
        )
    
    def _to_blocked_telegram_format(self) -> str:
        """Phase 3-A: Format for blocked trade notification."""
        cc = self.committee_consensus
        
        if cc:
            # Detailed committee breakdown for blocked trades
            ta = cc.technical_analyst_report
            ts = cc.trend_strategist_report
            mh = cc.memory_historian_report
            
            # Build expert breakdown
            expert_lines = []
            if ta.stance in ["BULLISH", "BEARISH"]:
                expert_lines.append(f"📊 Technical: {ta.stance} ({ta.confidence:.0f}%)")
            if ts.stance in ["ALIGNED", "COUNTER_TREND"]:
                expert_lines.append(f"📈 Trend: {ts.stance} ({ts.confidence:.0f}%)")
            if mh.stance in ["POSITIVE", "NEGATIVE"]:
                expert_lines.append(f"📚 Memory: {mh.stance}")
            
            expert_block = "\n   ├─ " + "\n   ├─ ".join(expert_lines) if expert_lines else ""
            
            return (
                f"🛡️🛑 TRADE BLOCKED BY COMMITTEE 🛑🛡️\n"
                f"\n"
                f"🎯 Signal: {self.symbol} {self.direction}\n"
                f"❌ Verdict: {self.verdict}"
                f"{expert_block}\n"
                f"\n"
                f"📝 Reason: {self.reasoning[:150]}...\n"
                f"\n"
                f"⚠️ Technical strategy signal was NOT executed.\n"
                f"🤖 Committee protection activated."
            )
        else:
            # Simplified format without committee data
            return (
                f"🛡️🛑 TRADE BLOCKED BY COMMITTEE 🛑🛡️\n"
                f"\n"
                f"🎯 Signal: {self.symbol} {self.direction}\n"
                f"❌ Verdict: {self.verdict}\n"
                f"\n"
                f"📝 Reason: {self.reasoning[:150]}...\n"
                f"\n"
                f"⚠️ Technical strategy signal was NOT executed.\n"
                f"🤖 Committee protection activated."
            )


class DecisionAgent:
    """
    Decision Agent — Multi-Agent Committee Lead Coordinator.
    
    PHASE 3-A: ACTIVE GATING — Committee can block trades.
    
    The Lead Coordinator orchestrates 3 Expert Agents:
      1. TechnicalAnalyst: RSI, ADX, Bollinger Bands
      2. TrendStrategist: EMA alignment, price action
      3. MemoryHistorian: Symbol-specific historical success
    
    Each expert generates a report, and the coordinator synthesizes
    them into a Committee Consensus with final verdict.
    
    Usage:
        agent = DecisionAgent()
        opinion = agent.analyze_signal(signal_data, market_data)
        
        # Phase 3-A: Check if trade should proceed
        if opinion.is_approved:
            execute_trade()  # Trade proceeds
        else:
            log_block_event()  # Trade blocked by committee
            send_blocked_notification()
    
    Safety:
        - Emergency bypass: Agent errors default to APPROVE
        - SHADOW_MODE configurable: Set to True for advisory-only mode
    """
    
    def __init__(self, shadow_mode: bool | None = None):
        # Use module-level SHADOW_MODE if not specified
        self.shadow_mode = shadow_mode if shadow_mode is not None else SHADOW_MODE
        self.model_cache = {}
        self.opinion_history = []
        
        # Phase 4-A: Initialize Expert Agents (now 4 experts)
        self.technical_analyst = TechnicalAnalyst()
        self.trend_strategist = TrendStrategist()
        self.memory_historian = MemoryHistorian()
        self.sentiment_analyst = SentimentAnalyst()  # Phase 4-A: News sentiment
        
        mode_str = "SHADOW MODE (advisory only)" if self.shadow_mode else "ACTIVE GATING (can block trades)"
        log.info(
            f"[DecisionAgent] Multi-Agent Committee initialized — {mode_str} "
            f"(model_version={MODEL_VERSION}, "
            f"experts=[TechnicalAnalyst, TrendStrategist, MemoryHistorian, SentimentAnalyst], "
            f"emergency_bypass={EMERGENCY_BYPASS_ON_ERROR})"
        )
    
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
            # PHASE 3-A: Emergency Bypass — On error, default to APPROVE
            # This ensures the bot never freezes due to agent failures
            log.error(f"[DecisionAgent] Analysis failed for {signal_data.get('symbol', 'unknown')}: {exc}")
            log.warning(f"[DecisionAgent] EMERGENCY BYPASS activated — defaulting to APPROVE for {signal_data.get('symbol', 'unknown')}")
            
            symbol = str(signal_data.get("symbol", "")).upper()
            direction = str(signal_data.get("action", "")).upper()
            technical_conf = float(signal_data.get("confidence", 0.0))
            technical_strategy = str(signal_data.get("strategy_label", "unknown"))
            
            return AgentOpinion(
                symbol=symbol,
                direction=direction,
                technical_confidence=technical_conf,
                technical_strategy=technical_strategy,
                ai_confidence=0.0,
                ai_regime="ERROR",
                verdict="ERROR",  # is_approved will return True due to EMERGENCY_BYPASS_ON_ERROR
                reasoning=f"Agent analysis failed: {str(exc)[:100]}. Emergency bypass activated — trade approved.",
                shadow_mode=self.shadow_mode,
                ai_score=0.0,
            )
    
    def _perform_analysis(
        self,
        signal_data: dict,
        market_data: dict,
    ) -> AgentOpinion:
        """
        Multi-Agent Committee Analysis.
        
        Orchestrates 3 expert agents to debate the signal,
        then synthesizes their reports into a Committee Consensus.
        """
        
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
        
        # Get AI evaluation (used by Technical Analyst)
        ai_result = evaluate_symbol(
            symbol=symbol,
            timeframe_data=timeframe_data,
        )
        
        ai_confidence = ai_result.get("confidence", 0.0)
        ai_regime = ai_result.get("regime", "UNKNOWN")
        ai_score = ai_result.get("ai_score", 0.0)
        
        # ====================================================================
        # PHASE 2-A: Multi-Agent Committee Debate
        # ====================================================================
        
        log.info(f"[DecisionAgent] Starting Committee Debate for {symbol} {direction}")
        
        # 1. Technical Analyst Report
        ta_report = self.technical_analyst.analyze(
            symbol=symbol,
            direction=direction,
            market_data=market_data,
            ai_confidence=ai_confidence,
            ai_regime=ai_regime,
        )
        log.debug(f"[TechnicalAnalyst] {symbol}: {ta_report.stance} ({ta_report.confidence:.0f}%)")
        
        # 2. Trend Strategist Report
        ts_report = self.trend_strategist.analyze(
            symbol=symbol,
            direction=direction,
            market_data=market_data,
            ai_confidence=ai_confidence,
        )
        log.debug(f"[TrendStrategist] {symbol}: {ts_report.stance} ({ts_report.confidence:.0f}%)")
        
        # 3. Memory Historian Report
        mh_report = self.memory_historian.analyze(
            symbol=symbol,
            direction=direction,
        )
        log.debug(f"[MemoryHistorian] {symbol}: {mh_report.stance}")
        
        # 4. Sentiment Analyst Report (Phase 4-A)
        sa_report = self.sentiment_analyst.analyze(
            symbol=symbol,
            direction=direction,
        )
        log.debug(f"[SentimentAnalyst] {symbol}: {sa_report.stance} ({sa_report.confidence:.0f}%)")
        
        # ====================================================================
        # Synthesize Committee Consensus
        # ====================================================================
        
        consensus = self._synthesize_committee_consensus(
            symbol=symbol,
            direction=direction,
            technical_conf=technical_conf,
            ai_confidence=ai_confidence,
            ai_regime=ai_regime,
            ta_report=ta_report,
            ts_report=ts_report,
            mh_report=mh_report,
            sa_report=sa_report,  # Phase 4-A
        )
        
        # Create AgentOpinion with committee data
        opinion = AgentOpinion(
            symbol=symbol,
            direction=direction,
            technical_confidence=technical_conf,
            technical_strategy=technical_strategy,
            ai_confidence=ai_confidence,
            ai_regime=ai_regime,
            verdict=consensus.final_verdict,
            reasoning=consensus.debate_summary,
            shadow_mode=self.shadow_mode,
            ai_score=ai_score,
            committee_consensus=consensus,
        )
        
        # Store in history
        self.opinion_history.append(opinion.to_dict())
        
        # Record opinion in Agent Memory
        if _AGENT_MEMORY_AVAILABLE:
            try:
                record_opinion(
                    symbol=symbol,
                    verdict=consensus.final_verdict,
                    ai_confidence=ai_confidence,
                    technical_confidence=technical_conf,
                    technical_strategy=technical_strategy,
                    direction=direction,
                )
            except Exception as mem_exc:
                log.warning(f"[DecisionAgent] Failed to record opinion: {mem_exc}")
        
        # Log committee result
        log.info(
            f"[DecisionAgent] Committee Result for {symbol} {direction}: "
            f"{consensus.final_verdict} (Technical: {ta_report.stance}, "
            f"Trend: {ts_report.stance}, Memory: {mh_report.stance})"
        )
        
        return opinion
    
    def _synthesize_committee_consensus(
        self,
        symbol: str,
        direction: str,
        technical_conf: float,
        ai_confidence: float,
        ai_regime: str,
        ta_report: ExpertReport,
        ts_report: ExpertReport,
        mh_report: ExpertReport,
        sa_report: ExpertReport,  # Phase 4-A: Sentiment Analyst
    ) -> CommitteeConsensus:
        """
        Synthesize expert reports into a final Committee Consensus.
        
        Phase 4-A: Now includes 4 experts (added Sentiment Analyst).
        
        Voting Logic:
        - APPROVE: At least 2 experts approve with confidence >= threshold
        - REJECT: At least 2 experts reject or counter-trend (or HIGH_NEGATIVE sentiment)
        - UNCERTAIN: Mixed signals or insufficient confidence
        """
        
        # Count approvals
        approvals = 0
        rejections = 0
        
        # Technical Analyst vote
        if ta_report.stance in ["BULLISH", "BEARISH"] and ta_report.confidence >= CONFIDENCE_THRESHOLD:
            if (direction == "BUY" and ta_report.stance == "BULLISH") or \
               (direction == "SELL" and ta_report.stance == "BEARISH"):
                approvals += 1
            else:
                rejections += 1
        
        # Trend Strategist vote
        if ts_report.stance == "ALIGNED" and ts_report.confidence >= CONFIDENCE_THRESHOLD:
            approvals += 1
        elif ts_report.stance == "COUNTER_TREND":
            rejections += 1
        
        # Memory Historian vote
        if mh_report.stance == "POSITIVE" and mh_report.confidence >= CONFIDENCE_THRESHOLD:
            approvals += 1
        elif mh_report.stance == "NEGATIVE":
            rejections += 1
        
        # Phase 4-A: Sentiment Analyst vote
        # HIGH_NEGATIVE sentiment can single-handedly force REJECT
        if sa_report.stance in ["HIGH_POSITIVE", "POSITIVE"] and sa_report.confidence >= CONFIDENCE_THRESHOLD:
            approvals += 1
        elif sa_report.stance in ["NEGATIVE", "HIGH_NEGATIVE"]:
            rejections += 1
            # HIGH_NEGATIVE is a strong signal - count it extra
            if sa_report.stance == "HIGH_NEGATIVE":
                rejections += 1  # Double weight for very bad news
        
        # Determine final verdict
        if approvals >= COMMITTEE_APPROVE_THRESHOLD:
            verdict = "APPROVE"
            consensus_conf = (ta_report.confidence + ts_report.confidence + mh_report.confidence + sa_report.confidence) / 4
        elif rejections >= COMMITTEE_APPROVE_THRESHOLD:
            verdict = "REJECT"
            avg_conf = (ta_report.confidence + ts_report.confidence + mh_report.confidence + sa_report.confidence) / 4
            consensus_conf = max(30, 100 - avg_conf)
        else:
            verdict = "UNCERTAIN"
            consensus_conf = 50.0
        
        # Generate debate summary
        summary_parts = []
        
        # Technical summary
        if ta_report.stance in ["BULLISH", "BEARISH"]:
            summary_parts.append(
                f"Technical indicators show {ta_report.stance.lower()} bias ({ta_report.confidence:.0f}% confidence)"
            )
        else:
            summary_parts.append("Technical indicators are neutral")
        
        # Trend summary
        if ts_report.stance == "ALIGNED":
            summary_parts.append(f"Trend is aligned with signal ({ts_report.confidence:.0f}% confidence)")
        elif ts_report.stance == "COUNTER_TREND":
            summary_parts.append("Trend contradicts signal direction — caution advised")
        else:
            summary_parts.append("Trend direction is unclear")
        
        # Memory summary
        if mh_report.stance == "POSITIVE":
            summary_parts.append("Historical memory supports this setup")
        elif mh_report.stance == "NEGATIVE":
            summary_parts.append("Historical memory warns against this setup")
        elif mh_report.stance == "NO_DATA":
            summary_parts.append("Insufficient historical data for this symbol")
        
        # Phase 4-A: Sentiment summary
        if sa_report.stance == "HIGH_POSITIVE":
            summary_parts.append("Very positive news sentiment supports trade")
        elif sa_report.stance == "POSITIVE":
            summary_parts.append("News sentiment is favorable")
        elif sa_report.stance == "NEGATIVE":
            summary_parts.append("Negative news sentiment detected")
        elif sa_report.stance == "HIGH_NEGATIVE":
            summary_parts.append("⚠️ Very negative news - high caution advised")
        elif sa_report.stance == "NO_DATA":
            summary_parts.append("No recent news data available")
        
        # Regime context
        if ai_regime == "VOLATILE":
            summary_parts.append("High volatility regime — wider stops recommended")
        elif ai_regime == "TRENDING":
            summary_parts.append("Trending market — momentum approach favorable")
        
        # Shadow mode disclaimer
        mode_tag = "SHADOW MODE" if self.shadow_mode else "ACTIVE GATING"
        summary_parts.append(f"[{mode_tag}] Committee opinion is advisory only")
        
        debate_summary = "; ".join(summary_parts)
        
        return CommitteeConsensus(
            symbol=symbol,
            direction=direction,
            technical_analyst_report=ta_report,
            trend_strategist_report=ts_report,
            memory_historian_report=mh_report,
            sentiment_analyst_report=sa_report,  # Phase 4-A
            final_verdict=verdict,
            consensus_confidence=consensus_conf,
            debate_summary=debate_summary,
            shadow_mode=self.shadow_mode,
        )
    
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
        
        # Calculate average consensus confidence from committee data
        avg_consensus_conf = sum(
            o.get("committee", {}).get("consensus_confidence", 50) 
            for o in self.opinion_history
        ) / total if total > 0 else 0
        
        return {
            "total_opinions": total,
            "approvals": approvals,
            "rejections": rejections,
            "uncertain": uncertain,
            "avg_consensus_confidence": round(avg_consensus_conf, 1),
            "shadow_mode": self.shadow_mode,
            "committee_size": 4,  # Phase 4-A: Now 4 experts
            "experts": ["TechnicalAnalyst", "TrendStrategist", "MemoryHistorian", "SentimentAnalyst"],
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
    # Test the Multi-Agent Committee DecisionAgent
    print("=" * 60)
    print("[TEST] Multi-Agent Committee DecisionAgent (Phase 4-A)")
    print("=" * 60)
    
    agent = DecisionAgent(shadow_mode=True)
    print(f"✓ Shadow Mode: {agent.shadow_mode}")
    print(f"✓ Model Version: {MODEL_VERSION}")
    print(f"✓ Expert Agents: TechnicalAnalyst, TrendStrategist, MemoryHistorian, SentimentAnalyst")
    print(f"✓ Committee Threshold: {COMMITTEE_APPROVE_THRESHOLD} votes required")
    print(f"✓ API Integration: NEWS_API_KEY={bool(NEWS_API_KEY)}, FMP_API_KEY={bool(FMP_API_KEY)}")
    
    print("\n[TEST] Ready for integration")
    print("-" * 60)
