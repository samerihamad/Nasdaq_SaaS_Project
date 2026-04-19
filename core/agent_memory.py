"""
Agent Memory — NATB v2.0 Contextual Memory (MCP-inspired)

Phase 1-B: Experience Ledger and Symbol Insights

This module provides persistent memory for the Decision Agent:
  1. Experience Ledger: Stores Agent opinions vs actual trade outcomes
  2. Symbol Insights: Aggregated statistics per symbol
  3. Learning Loop: Reinforcement/penalty based on trade results

Architecture:
  - JSON-based storage at /root/Nasdaq_SaaS_Project/data/agent_memory.json
  - Atomic file operations (write to temp, then rename)
  - Thread-safe with file locking
  - Shadow mode compatible (does not affect live trading)

Usage:
  from core.agent_memory import AgentMemory, record_trade_outcome
  
  memory = AgentMemory()
  
  # Record agent opinion when signal is generated
  memory.record_opinion(symbol, verdict, confidence, direction)
  
  # Update with outcome when trade closes
  memory.record_outcome(symbol, verdict, pnl, outcome="WIN")
  
  # Query insights for a symbol
  insights = memory.get_symbol_insights(symbol, direction="BUY")
"""

import os
import sys
import json
import time
import fcntl
import logging
from typing import Any
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root is in path
PROJECT_ROOT = "/root/Nasdaq_SaaS_Project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

log = logging.getLogger(__name__)

# Constants
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
MEMORY_FILE = os.path.join(DATA_DIR, "agent_memory.json")
LOCK_FILE = os.path.join(DATA_DIR, ".agent_memory.lock")
MAX_EXPERIENCES_PER_SYMBOL = 100  # Rotate old entries
INSIGHT_WINDOW_DAYS = 30  # Only consider experiences from last 30 days

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)


@dataclass
class Experience:
    """Single experience record (opinion + outcome)."""
    symbol: str
    direction: str
    verdict: str  # APPROVE, REJECT, UNCERTAIN
    ai_confidence: float
    technical_confidence: float
    technical_strategy: str
    timestamp: str
    outcome: str | None = None  # WIN, LOSS, BREAKEVEN, UNKNOWN
    pnl: float | None = None
    outcome_timestamp: str | None = None
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "Experience":
        return cls(**data)


@dataclass
class SymbolInsights:
    """Aggregated insights for a symbol + direction combination."""
    symbol: str
    direction: str
    total_opinions: int
    approval_rate: float  # % of APPROVE verdicts
    success_rate_when_approved: float | None  # % of APPROVE that resulted in WIN
    rejection_rate: float  # % of REJECT verdicts
    avg_ai_confidence: float
    avg_technical_confidence: float
    recent_trend: str  # "IMPROVING", "DECLINING", "STABLE"
    insight_message: str  # Human-readable summary
    
    def to_dict(self) -> dict:
        return asdict(self)


class AgentMemory:
    """
    Contextual Memory for the Decision Agent.
    
    Manages the Experience Ledger and provides Symbol Insights.
    All operations are atomic and thread-safe.
    """
    
    def __init__(self, memory_file: str = MEMORY_FILE):
        self.memory_file = memory_file
        self.lock_file = LOCK_FILE
        self._ensure_file_exists()
        log.info(f"[AgentMemory] Initialized with file: {memory_file}")
    
    def _ensure_file_exists(self) -> None:
        """Create memory file if it doesn't exist."""
        if not os.path.exists(self.memory_file):
            self._write_data({
                "version": "1.0",
                "created_at": datetime.now().isoformat(),
                "experiences": {},  # symbol -> list of experiences
                "symbol_stats": {},  # symbol -> aggregated stats
                "metadata": {
                    "total_outcomes": 0,
                    "total_wins": 0,
                    "total_losses": 0,
                }
            })
            log.info("[AgentMemory] Created new memory file")
    
    def _acquire_lock(self) -> int:
        """Acquire file lock for thread safety. Returns fd."""
        fd = os.open(self.lock_file, os.O_CREAT | os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            # Lock is held by another process, wait
            fcntl.flock(fd, fcntl.LOCK_EX)
        return fd
    
    def _release_lock(self, fd: int) -> None:
        """Release file lock."""
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        except Exception:
            pass
    
    def _read_data(self) -> dict:
        """Read memory data from file."""
        try:
            with open(self.memory_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            log.warning("[AgentMemory] Corrupt or missing file, returning empty structure")
            return {"experiences": {}, "symbol_stats": {}, "metadata": {}}
    
    def _write_data(self, data: dict) -> None:
        """Write data atomically using temp file + rename."""
        temp_file = f"{self.memory_file}.tmp"
        try:
            # Write to temp file first
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            os.rename(temp_file, self.memory_file)
            
        except Exception as exc:
            log.error(f"[AgentMemory] Failed to write data: {exc}")
            # Clean up temp file
            try:
                os.remove(temp_file)
            except:
                pass
            raise
    
    def record_opinion(
        self,
        symbol: str,
        verdict: str,
        ai_confidence: float,
        technical_confidence: float,
        technical_strategy: str,
        direction: str,
    ) -> str:
        """
        Record a new Agent opinion (when signal is generated).
        
        Returns:
            experience_id: Unique ID for this experience (use to update outcome later)
        """
        symbol = symbol.upper()
        direction = direction.upper()
        
        experience = Experience(
            symbol=symbol,
            direction=direction,
            verdict=verdict,
            ai_confidence=ai_confidence,
            technical_confidence=technical_confidence,
            technical_strategy=technical_strategy,
            timestamp=datetime.now().isoformat(),
            outcome=None,
            pnl=None,
            outcome_timestamp=None,
        )
        
        fd = self._acquire_lock()
        try:
            data = self._read_data()
            
            # Initialize symbol bucket if needed
            if symbol not in data["experiences"]:
                data["experiences"][symbol] = []
            
            # Add experience
            data["experiences"][symbol].append(experience.to_dict())
            
            # Rotate old entries if needed
            if len(data["experiences"][symbol]) > MAX_EXPERIENCES_PER_SYMBOL:
                data["experiences"][symbol] = data["experiences"][symbol][-MAX_EXPERIENCES_PER_SYMBOL:]
            
            # Update symbol stats
            self._update_symbol_stats(data, symbol)
            
            self._write_data(data)
            
            log.info(
                f"[AgentMemory] Recorded opinion for {symbol} {direction}: "
                f"{verdict} (AI: {ai_confidence:.1f}%)"
            )
            
            # Return timestamp as ID (unique per symbol + time)
            return experience.timestamp
            
        finally:
            self._release_lock(fd)
    
    def record_outcome(
        self,
        symbol: str,
        direction: str,
        verdict: str,
        pnl: float,
        outcome: str | None = None,
    ) -> bool:
        """
        Update the most recent experience with trade outcome.
        
        Called when trade closes (sync.py or trade_session_finalize.py).
        
        Args:
            symbol: Trade symbol
            direction: BUY or SELL
            verdict: The agent's verdict (APPROVE, REJECT, UNCERTAIN)
            pnl: Realized P&L
            outcome: WIN, LOSS, BREAKEVEN (auto-detected if None)
        
        Returns:
            True if outcome was recorded, False if matching experience not found
        """
        symbol = symbol.upper()
        direction = direction.upper()
        
        # Auto-detect outcome if not provided
        if outcome is None:
            if pnl > 0:
                outcome = "WIN"
            elif pnl < 0:
                outcome = "LOSS"
            else:
                outcome = "BREAKEVEN"
        
        fd = self._acquire_lock()
        try:
            data = self._read_data()
            
            if symbol not in data["experiences"]:
                log.warning(f"[AgentMemory] No experiences found for {symbol}")
                return False
            
            # Find most recent matching experience (same verdict/direction, no outcome yet)
            experiences = data["experiences"][symbol]
            found = False
            
            for exp in reversed(experiences):
                if (exp["direction"] == direction and 
                    exp["verdict"] == verdict and 
                    exp["outcome"] is None):
                    
                    exp["outcome"] = outcome
                    exp["pnl"] = pnl
                    exp["outcome_timestamp"] = datetime.now().isoformat()
                    found = True
                    break
            
            if not found:
                # Create a new experience with just the outcome (orphaned trade)
                log.warning(
                    f"[AgentMemory] No matching open experience for {symbol} {direction} {verdict}. "
                    f"Creating orphaned record."
                )
                experiences.append({
                    "symbol": symbol,
                    "direction": direction,
                    "verdict": verdict,
                    "ai_confidence": 0.0,
                    "technical_confidence": 0.0,
                    "technical_strategy": "UNKNOWN",
                    "timestamp": datetime.now().isoformat(),
                    "outcome": outcome,
                    "pnl": pnl,
                    "outcome_timestamp": datetime.now().isoformat(),
                    "orphaned": True,
                })
            
            # Update stats and metadata
            self._update_symbol_stats(data, symbol)
            self._update_metadata(data, outcome)
            
            self._write_data(data)
            
            log.info(
                f"[AgentMemory] Recorded outcome for {symbol}: {outcome} (P&L: {pnl:.2f})"
            )
            return True
            
        finally:
            self._release_lock(fd)
    
    def get_symbol_insights(
        self,
        symbol: str,
        direction: str | None = None,
        lookback_days: int = INSIGHT_WINDOW_DAYS,
    ) -> SymbolInsights | None:
        """
        Get aggregated insights for a symbol.
        
        Args:
            symbol: Stock symbol
            direction: Optional filter for BUY/SELL
            lookback_days: Only consider experiences from last N days
        
        Returns:
            SymbolInsights dataclass or None if no data
        """
        symbol = symbol.upper()
        if direction:
            direction = direction.upper()
        
        data = self._read_data()
        
        if symbol not in data["experiences"]:
            return None
        
        # Filter experiences by date and direction
        cutoff = datetime.now() - timedelta(days=lookback_days)
        cutoff_str = cutoff.isoformat()
        
        experiences = [
            exp for exp in data["experiences"][symbol]
            if exp["timestamp"] > cutoff_str
            and (direction is None or exp["direction"] == direction)
        ]
        
        if not experiences:
            return None
        
        # Calculate stats
        total = len(experiences)
        approved = [e for e in experiences if e["verdict"] == "APPROVE"]
        rejected = [e for e in experiences if e["verdict"] == "REJECT"]
        
        approval_rate = len(approved) / total * 100 if total > 0 else 0
        rejection_rate = len(rejected) / total * 100 if total > 0 else 0
        
        # Success rate for APPROVE verdicts
        approved_with_outcome = [e for e in approved if e["outcome"] is not None]
        wins = [e for e in approved_with_outcome if e["outcome"] == "WIN"]
        success_rate = len(wins) / len(approved_with_outcome) * 100 if approved_with_outcome else None
        
        # Average confidences
        avg_ai_conf = sum(e["ai_confidence"] for e in experiences) / total if total > 0 else 0
        avg_tech_conf = sum(e["technical_confidence"] for e in experiences) / total if total > 0 else 0
        
        # Trend detection (compare first half vs second half)
        mid = total // 2
        first_half_wins = sum(1 for e in experiences[:mid] if e["outcome"] == "WIN")
        second_half_wins = sum(1 for e in experiences[mid:] if e["outcome"] == "WIN")
        
        if second_half_wins > first_half_wins:
            trend = "IMPROVING"
        elif second_half_wins < first_half_wins:
            trend = "DECLINING"
        else:
            trend = "STABLE"
        
        # Generate insight message
        msg_parts = []
        
        if success_rate is not None:
            if success_rate >= 70:
                msg_parts.append(f"Strong track record: {success_rate:.0f}% wins when I approve")
            elif success_rate >= 50:
                msg_parts.append(f"Moderate success: {success_rate:.0f}% wins when I approve")
            else:
                msg_parts.append(f"Caution: only {success_rate:.0f}% wins when I approve")
        
        if trend == "IMPROVING":
            msg_parts.append("Performance is improving lately")
        elif trend == "DECLINING":
            msg_parts.append("Performance has declined recently")
        
        if direction:
            msg_parts.append(f"for {direction} signals")
        
        insight_message = "; ".join(msg_parts) if msg_parts else "No significant pattern detected"
        
        return SymbolInsights(
            symbol=symbol,
            direction=direction or "ALL",
            total_opinions=total,
            approval_rate=round(approval_rate, 1),
            success_rate_when_approved=round(success_rate, 1) if success_rate is not None else None,
            rejection_rate=round(rejection_rate, 1),
            avg_ai_confidence=round(avg_ai_conf, 1),
            avg_technical_confidence=round(avg_tech_conf, 1),
            recent_trend=trend,
            insight_message=insight_message,
        )
    
    def _update_symbol_stats(self, data: dict, symbol: str) -> None:
        """Recalculate aggregated stats for a symbol."""
        if symbol not in data["experiences"]:
            return
        
        experiences = data["experiences"][symbol]
        
        # Count outcomes by verdict
        approved_wins = sum(1 for e in experiences if e["verdict"] == "APPROVE" and e["outcome"] == "WIN")
        approved_losses = sum(1 for e in experiences if e["verdict"] == "APPROVE" and e["outcome"] == "LOSS")
        
        data["symbol_stats"][symbol] = {
            "total_experiences": len(experiences),
            "approved_wins": approved_wins,
            "approved_losses": approved_losses,
            "net_reinforcement": approved_wins - approved_losses,
            "last_updated": datetime.now().isoformat(),
        }
    
    def _update_metadata(self, data: dict, outcome: str) -> None:
        """Update global metadata."""
        if "metadata" not in data:
            data["metadata"] = {}
        
        meta = data["metadata"]
        meta["total_outcomes"] = meta.get("total_outcomes", 0) + 1
        
        if outcome == "WIN":
            meta["total_wins"] = meta.get("total_wins", 0) + 1
        elif outcome == "LOSS":
            meta["total_losses"] = meta.get("total_losses", 0) + 1
        
        meta["last_updated"] = datetime.now().isoformat()
    
    def get_memory_summary(self) -> dict:
        """Get summary of entire memory system."""
        data = self._read_data()
        meta = data.get("metadata", {})
        
        total_symbols = len(data.get("experiences", {}))
        total_experiences = sum(
            len(exps) for exps in data.get("experiences", {}).values()
        )
        
        return {
            "total_symbols": total_symbols,
            "total_experiences": total_experiences,
            "total_outcomes": meta.get("total_outcomes", 0),
            "total_wins": meta.get("total_wins", 0),
            "total_losses": meta.get("total_losses", 0),
            "win_rate": (
                meta.get("total_wins", 0) / meta.get("total_outcomes", 1) * 100
                if meta.get("total_outcomes", 0) > 0 else 0
            ),
        }


# Convenience functions for external integration

_memory_instance: AgentMemory | None = None


def get_memory() -> AgentMemory:
    """Get or create global AgentMemory singleton."""
    global _memory_instance
    if _memory_instance is None:
        _memory_instance = AgentMemory()
    return _memory_instance


def record_opinion(
    symbol: str,
    verdict: str,
    ai_confidence: float,
    technical_confidence: float,
    technical_strategy: str,
    direction: str,
) -> str:
    """Convenience function to record an opinion."""
    return get_memory().record_opinion(
        symbol=symbol,
        verdict=verdict,
        ai_confidence=ai_confidence,
        technical_confidence=technical_confidence,
        technical_strategy=technical_strategy,
        direction=direction,
    )


def record_trade_outcome(
    symbol: str,
    direction: str,
    verdict: str,
    pnl: float,
    outcome: str | None = None,
) -> bool:
    """
    Convenience function to record a trade outcome.
    
    This should be called from sync.py or trade_session_finalize.py
    when a trade closes.
    """
    return get_memory().record_outcome(
        symbol=symbol,
        direction=direction,
        verdict=verdict,
        pnl=pnl,
        outcome=outcome,
    )


def get_symbol_insights(
    symbol: str,
    direction: str | None = None,
    lookback_days: int = INSIGHT_WINDOW_DAYS,
) -> SymbolInsights | None:
    """Convenience function to get insights."""
    return get_memory().get_symbol_insights(symbol, direction, lookback_days)


if __name__ == "__main__":
    # Test the AgentMemory
    print("[TEST] AgentMemory System")
    memory = AgentMemory()
    
    # Record some test opinions
    print("\n1. Recording opinions...")
    memory.record_opinion("TSLA", "APPROVE", 65.0, 70.0, "Momentum", "BUY")
    memory.record_opinion("AAPL", "REJECT", 40.0, 60.0, "MeanRev", "SELL")
    
    # Record outcomes
    print("\n2. Recording outcomes...")
    memory.record_outcome("TSLA", "BUY", "APPROVE", 150.50, "WIN")
    memory.record_outcome("AAPL", "SELL", "REJECT", 0.0, "BREAKEVEN")
    
    # Get insights
    print("\n3. Symbol Insights for TSLA:")
    insights = memory.get_symbol_insights("TSLA", "BUY")
    if insights:
        print(f"   {insights.insight_message}")
        print(f"   Success Rate: {insights.success_rate_when_approved}%")
    
    # Summary
    print("\n4. Memory Summary:")
    summary = memory.get_memory_summary()
    print(f"   Total Symbols: {summary['total_symbols']}")
    print(f"   Total Experiences: {summary['total_experiences']}")
    print(f"   Win Rate: {summary['win_rate']:.1f}%")
    
    print("\n[TEST] Complete!")
