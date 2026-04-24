"""
Session Management Agent — NATB v3.2

Autonomous middleware for high-scale Capital.com authentication.
Prevents 429 errors through centralized rate limiting and queue management.

Features:
- Token Bucket: 1 auth per 10 seconds globally
- Asyncio Queue: FIFO processing for 50+ concurrent users
- Smart Cache: Instant returns for valid existing sessions
- 429 Circuit Breaker: Auto-pause queue for 60 seconds on rate limit
"""

import os
import sys
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple, Any
from pathlib import Path
from dataclasses import dataclass, field

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)


@dataclass
class SessionRequest:
    """Represents a user's session request in the queue."""
    user_id: str
    creds: Tuple[str, str, bool, str]  # api_key, password, is_demo, email
    force_refresh: bool = False
    future: asyncio.Future = field(default_factory=lambda: asyncio.get_event_loop().create_future())
    enqueue_time: float = field(default_factory=lambda: time.time())


@dataclass
class TokenBucket:
    """Token bucket for rate limiting authentication requests."""
    rate: float = 0.1  # 1 token per 10 seconds (0.1 tps)
    capacity: int = 1
    tokens: float = field(default=1.0)
    last_update: float = field(default_factory=lambda: time.time())
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    async def acquire(self) -> bool:
        """Try to acquire a token. Returns True if successful."""
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False
    
    async def wait_time(self) -> float:
        """Calculate wait time until next token available."""
        async with self._lock:
            if self.tokens >= 1.0:
                return 0.0
            return (1.0 - self.tokens) / self.rate


class SessionManager:
    """
    Centralized session management agent for Capital.com API.
    
    Prevents 429 errors by:
    1. Queuing all auth requests
    2. Rate limiting to 1 auth per 10 seconds
    3. Caching valid sessions
    4. Auto-pausing on 429 for 60 seconds
    """
    
    _instance: Optional['SessionManager'] = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self.queue: asyncio.Queue[SessionRequest] = asyncio.Queue()
        self.token_bucket = TokenBucket(rate=0.1, capacity=1)  # 1 per 10 seconds
        self.session_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_file = os.path.join(PROJECT_ROOT, "data", "session_store.json")
        self._queue_paused = False
        self._pause_until = 0.0
        self._pause_reason = ""
        self._processed_count = 0
        self._worker_task: Optional[asyncio.Task] = None
        self._shutdown = False
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        
        # Load existing sessions
        self._load_cache()
    
    def _get_cache_key(self, creds: Tuple) -> str:
        """Generate cache key from credentials."""
        api_key, _, is_demo, user_email = creds
        return f"{api_key[:8]}|{user_email}|{int(bool(is_demo))}"
    
    def _load_cache(self):
        """Load session cache from disk."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Filter expired sessions
                    now = datetime.now(timezone.utc).timestamp()
                    self.session_cache = {
                        k: v for k, v in data.items()
                        if v.get("expires_ts", 0) > now
                    }
                    logger.info(f"[SessionAgent] Loaded {len(self.session_cache)} valid sessions from cache")
        except Exception as e:
            logger.error(f"[SessionAgent] Failed to load cache: {e}")
            self.session_cache = {}
    
    def _save_cache(self):
        """Save session cache to disk."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.session_cache, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"[SessionAgent] Failed to save cache: {e}")
    
    def _is_session_valid(self, cache_key: str) -> bool:
        """Check if cached session is still valid."""
        if cache_key not in self.session_cache:
            return False
        
        session = self.session_cache[cache_key]
        expires_ts = float(session.get("expires_ts", 0))
        now = datetime.now(timezone.utc).timestamp()
        
        # Buffer: consider expired 60 seconds early to avoid edge cases
        return (expires_ts - 60) > now
    
    async def start(self):
        """Start the queue processor worker in the current event loop."""
        # Ensure we're attached to the current running loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._queue_processor())
            logger.info("[SessionAgent] Queue processor started")
    
    async def stop(self):
        """Gracefully shutdown the agent."""
        self._shutdown = True
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info("[SessionAgent] Shutdown complete")
    
    async def _queue_processor(self):
        """Main worker: processes queue with rate limiting."""
        while not self._shutdown:
            try:
                # Check if paused due to 429
                if self._queue_paused:
                    wait_seconds = self._pause_until - time.time()
                    if wait_seconds > 0:
                        logger.warning(f"[SessionAgent] Queue paused ({self._pause_reason}). Resuming in {wait_seconds:.0f}s...")
                        await asyncio.sleep(min(wait_seconds, 5))  # Check every 5 seconds
                        continue
                    else:
                        logger.info("[SessionAgent] Queue resumed from pause")
                        self._queue_paused = False
                        self._pause_reason = ""
                
                # Wait for token bucket
                token_acquired = await self.token_bucket.acquire()
                if not token_acquired:
                    wait_time = await self.token_bucket.wait_time()
                    logger.debug(f"[SessionAgent] Rate limit: waiting {wait_time:.1f}s for token")
                    await asyncio.sleep(wait_time)
                    continue
                
                # Get next request from queue
                request = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                
                # Process the request
                await self._process_request(request)
                
                self._processed_count += 1
                self.queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[SessionAgent] Queue processor error: {e}")
                await asyncio.sleep(1)
    
    async def _process_request(self, request: SessionRequest):
        """Process a single session request."""
        try:
            # Import here to avoid circular dependency
            from core.executor import get_session as _do_auth
            
            cache_key = self._get_cache_key(request.creds)
            
            # Try to get fresh session
            result = _do_auth(request.creds, chat_id=request.user_id, force_refresh=request.force_refresh)
            
            if result and result[0] and result[1]:  # (base_url, headers)
                base_url, headers = result
                
                # Cache the session
                self.session_cache[cache_key] = {
                    "base_url": base_url,
                    "headers": dict(headers),
                    "expires_ts": datetime.now(timezone.utc).timestamp() + 1800,  # 30 min TTL
                }
                self._save_cache()
                
                # Resolve the future with success
                request.future.set_result((base_url, headers))
                logger.info(f"[SessionAgent] Session acquired for User {request.user_id[:8]}...")
                
            else:
                request.future.set_exception(Exception("Authentication failed"))
                
        except Exception as e:
            error_str = str(e)
            
            # Check for 429 rate limit
            if "429" in error_str or "rate limit" in error_str.lower():
                logger.error(f"[SessionAgent] 429 detected! Pausing queue for 60 seconds.")
                self._queue_paused = True
                self._pause_until = time.time() + 60
                self._pause_reason = "HTTP 429 Rate Limit"
            
            request.future.set_exception(e)
    
    async def get_valid_session(
        self,
        user_id: str,
        creds: Tuple[str, str, bool, str],
        force_refresh: bool = False
    ) -> Tuple[str, Dict[str, str]]:
        """
        Get a valid session for a user.
        
        Flow:
        1. Check cache first (instant return if valid)
        2. If not cached or expired, join authentication queue
        3. Wait for turn with position updates
        4. Return session when ready
        """
        cache_key = self._get_cache_key(creds)
        
        # FAST PATH: Check if we have a valid cached session
        if not force_refresh and self._is_session_valid(cache_key):
            session = self.session_cache[cache_key]
            logger.debug(f"[SessionAgent] Cache hit for User {user_id[:8]}...")
            return session["base_url"], session["headers"]
        
        # SLOW PATH: Queue for authentication
        # Ensure worker is running
        await self.start()
        
        # Create request and add to queue
        request = SessionRequest(
            user_id=user_id,
            creds=creds,
            force_refresh=force_refresh
        )
        
        await self.queue.put(request)
        
        # Calculate queue position
        queue_size = self.queue.qsize()
        position = queue_size  # Approximate position
        
        # Calculate estimated wait (10 seconds per auth ahead in queue)
        estimated_wait = position * 10
        
        if position > 1:
            print(
                f"[SessionAgent] Queue Position for User {user_id[:8]}...: {position}/{queue_size}. "
                f"Estimated wait: {estimated_wait}s",
                flush=True
            )
        
        # Wait for the request to be processed
        try:
            base_url, headers = await request.future
            return base_url, headers
        except Exception as e:
            logger.error(f"[SessionAgent] Session acquisition failed for User {user_id[:8]}...: {e}")
            raise
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status for monitoring."""
        return {
            "queue_size": self.queue.qsize(),
            "paused": self._queue_paused,
            "pause_reason": self._pause_reason,
            "pause_seconds_remaining": max(0, self._pause_until - time.time()) if self._queue_paused else 0,
            "processed_count": self._processed_count,
            "cached_sessions": len(self.session_cache),
        }


# Global singleton instance
_session_manager: Optional[SessionManager] = None
_manager_lock = asyncio.Lock()


async def get_session_manager() -> SessionManager:
    """Get or create the global SessionManager instance."""
    global _session_manager
    if _session_manager is None:
        async with _manager_lock:
            if _session_manager is None:
                _session_manager = SessionManager()
                await _session_manager.start()
    return _session_manager


async def get_valid_session(
    user_id: str,
    creds: Tuple[str, str, bool, str],
    force_refresh: bool = False
) -> Tuple[str, Dict[str, str]]:
    """
    Public API: Get a valid session through the SessionAgent.
    
    Use this instead of direct get_session() calls to prevent 429 errors.
    """
    manager = await get_session_manager()
    return await manager.get_valid_session(user_id, creds, force_refresh)


def get_queue_status() -> Dict[str, Any]:
    """Get current queue status (synchronous wrapper)."""
    if _session_manager is None:
        return {"error": "SessionManager not initialized"}
    return _session_manager.get_queue_status()


# Backward compatibility for direct imports
__all__ = [
    "SessionManager",
    "get_session_manager",
    "get_valid_session",
    "get_queue_status",
]
