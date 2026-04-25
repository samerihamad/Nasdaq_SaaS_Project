"""
Session Management Agent — NATB v3.2 (NON-BLOCKING)

Autonomous middleware for high-scale Capital.com authentication.
Prevents 429 errors through centralized rate limiting and queue management.

NON-BLOCKING DESIGN:
- Cache checks are synchronous and instant (NO async/NO blocking)
- Background thread handles async queue processing
- Main thread never blocked during startup

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
import threading
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple, Any
from pathlib import Path
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

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
    result_event: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: Optional[Exception] = None
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
    
    NON-BLOCKING DESIGN:
    - Uses threading.Lock (not asyncio.Lock) for singleton - no event loop needed
    - Background thread runs the async event loop - main thread never blocks
    - Cache checks are synchronous and instant
    - Only cache misses wait (with timeout) for background processing
    
    Prevents 429 errors by:
    1. Queuing all auth requests
    2. Rate limiting to 1 auth per 10 seconds
    3. Caching valid sessions
    4. Auto-pausing on 429 for 60 seconds
    """
    
    _instance: Optional['SessionManager'] = None
    _lock = threading.Lock()  # Use threading lock, not asyncio
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        
        # Thread-safe queue for requests
        self._requests: list[SessionRequest] = []
        self._request_lock = threading.Lock()
        self._request_event = threading.Event()  # Signals new requests
        
        self.session_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = threading.RLock()  # Reentrant for nested access
        self.cache_file = os.path.join(PROJECT_ROOT, "data", "session_store.json")
        
        # 429 handling
        self._queue_paused = False
        self._pause_until = 0.0
        self._pause_reason = ""
        self._pause_lock = threading.Lock()
        
        self._processed_count = 0
        self._shutdown = False
        self._worker_thread: Optional[threading.Thread] = None
        self._worker_started = threading.Event()
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        
        # Load existing sessions (synchronous)
        self._load_cache()
        
        # Start background worker (non-blocking)
        self._start_background_worker()
    
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
        """
        Check if cached session is still valid via LIVE BROKER CHECK.
        SessionAgent 4.0: Validates tokens by making lightweight API call to Capital.com.
        """
        with self._cache_lock:
            if cache_key not in self.session_cache:
                return False
            
            session = self.session_cache[cache_key]
            expires_ts = float(session.get("expires_ts", 0))
            now = datetime.now(timezone.utc).timestamp()
            
            # Check local TTL first
            if (expires_ts - 60) <= now:
                logger.debug(f"[SessionAgent] Cache expired for {cache_key[:16]}...")
                return False
            
            # SessionAgent 4.0: LIVE BROKER CHECK
            # Make lightweight request to validate token is still accepted by Capital.com
            try:
                base_url = session.get("base_url")
                headers = session.get("headers", {})
                
                if not base_url or not headers.get("CST"):
                    return False
                
                import requests
                # Lightweight check: /accounts endpoint (fast, read-only)
                check_url = f"{base_url}/accounts"
                check_headers = {
                    "X-CAP-API-KEY": headers.get("X-CAP-API-KEY", ""),
                    "CST": headers.get("CST", ""),
                    "X-SECURITY-TOKEN": headers.get("X-SECURITY-TOKEN", ""),
                }
                
                resp = requests.get(check_url, headers=check_headers, timeout=5)
                
                if resp.status_code == 401:
                    logger.warning(f"[SessionAgent] Token INVALID (401) for {cache_key[:16]}... Force re-login.")
                    return False
                elif resp.status_code == 403:
                    logger.warning(f"[SessionAgent] Token EXPIRED (403) for {cache_key[:16]}... Force re-login.")
                    return False
                elif resp.status_code == 200:
                    logger.debug(f"[SessionAgent] Token VALID (200) for {cache_key[:16]}...")
                    return True
                else:
                    # Unknown response - assume invalid to be safe
                    logger.warning(f"[SessionAgent] Token check ambiguous ({resp.status_code}) for {cache_key[:16]}... Force re-login.")
                    return False
                    
            except Exception as e:
                logger.warning(f"[SessionAgent] Live broker check failed for {cache_key[:16]}...: {e}")
                # On check failure, fall back to TTL validation (conservative)
                return (expires_ts - 60) > now
    
    def _start_background_worker(self):
        """Start the background worker thread (non-blocking)."""
        if self._worker_thread is None or not self._worker_thread.is_alive():
            self._worker_thread = threading.Thread(
                target=self._background_worker,
                name="SessionAgent-Worker",
                daemon=True
            )
            self._worker_thread.start()
            logger.info("[SessionAgent] Background worker thread started")
    
    def _background_worker(self):
        """
        Background thread that runs the async event loop.
        This runs in a separate thread so main thread never blocks.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Signal that worker is ready
        self._worker_started.set()
        
        try:
            loop.run_until_complete(self._async_worker_loop())
        except Exception as e:
            logger.error(f"[SessionAgent] Background worker error: {e}")
        finally:
            loop.close()
    
    async def _async_worker_loop(self):
        """Async loop running inside background thread."""
        token_bucket = TokenBucket(rate=0.1, capacity=1)  # 1 per 10 seconds
        
        while not self._shutdown:
            try:
                # Check if paused due to 429
                with self._pause_lock:
                    is_paused = self._queue_paused
                    pause_until = self._pause_until
                    pause_reason = self._pause_reason
                
                if is_paused:
                    wait_seconds = pause_until - time.time()
                    if wait_seconds > 0:
                        logger.warning(f"[SessionAgent] Queue paused ({pause_reason}). Resuming in {wait_seconds:.0f}s...")
                        await asyncio.sleep(min(wait_seconds, 5))
                        continue
                    else:
                        logger.info("[SessionAgent] Queue resumed from pause")
                        with self._pause_lock:
                            self._queue_paused = False
                            self._pause_reason = ""
                
                # Get next request (non-blocking check)
                request = None
                with self._request_lock:
                    if self._requests:
                        request = self._requests.pop(0)
                
                if request is None:
                    # No requests - wait a bit
                    await asyncio.sleep(0.1)
                    continue
                
                # Wait for token (rate limiting)
                token_acquired = await token_bucket.acquire()
                if not token_acquired:
                    wait_time = await token_bucket.wait_time()
                    logger.debug(f"[SessionAgent] Rate limit: waiting {wait_time:.1f}s for token")
                    await asyncio.sleep(wait_time)
                    # Put request back at front of queue
                    with self._request_lock:
                        self._requests.insert(0, request)
                    continue
                
                # Process the request
                await self._process_request_async(request)
                self._processed_count += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[SessionAgent] Worker loop error: {e}")
                await asyncio.sleep(1)
    
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
    
    async def _process_request_async(self, request: SessionRequest):
        """Process a single session request (called from background thread)."""
        try:
            # Import here to avoid circular dependency
            from core.executor import get_session as _do_auth
            
            cache_key = self._get_cache_key(request.creds)
            
            # Try to get fresh session (this may be a fallback, but we need it here)
            result = _do_auth(request.creds, chat_id=request.user_id, force_refresh=request.force_refresh)
            
            if result and result[0] and result[1]:  # (base_url, headers)
                base_url, headers = result
                
                # Cache the session
                with self._cache_lock:
                    self.session_cache[cache_key] = {
                        "base_url": base_url,
                        "headers": dict(headers),
                        "expires_ts": datetime.now(timezone.utc).timestamp() + 1800,  # 30 min TTL
                    }
                self._save_cache()
                
                # Set result and signal completion
                request.result = (base_url, headers)
                logger.info(f"[SessionAgent] Session acquired for User {request.user_id[:8]}...")
                
            else:
                request.error = Exception("Authentication failed")
                
        except Exception as e:
            error_str = str(e)
            
            # Check for 429 rate limit
            if "429" in error_str or "rate limit" in error_str.lower():
                logger.error(f"[SessionAgent] 429 detected! Pausing queue for 60 seconds.")
                with self._pause_lock:
                    self._queue_paused = True
                    self._pause_until = time.time() + 60
                    self._pause_reason = "HTTP 429 Rate Limit"
            
            request.error = e
        finally:
            # Signal completion regardless of success/failure
            request.result_event.set()
    
    def get_valid_session_sync(
        self,
        user_id: str,
        creds: Tuple[str, str, bool, str],
        force_refresh: bool = False,
        timeout: float = 60.0
    ) -> Tuple[str, Dict[str, str]]:
        """
        Get a valid session for a user (SYNCHRONOUS - NON-BLOCKING for cache hits).
        
        Flow:
        1. Check cache first (INSTANT return if valid - NO BLOCKING)
        2. If not cached or expired, join authentication queue and wait
        3. Wait for turn with position updates
        4. Return session when ready
        
        Args:
            timeout: Maximum seconds to wait for auth (default 60)
        """
        cache_key = self._get_cache_key(creds)
        
        # FAST PATH: Check if we have a valid cached session (NO BLOCKING)
        if not force_refresh and self._is_session_valid(cache_key):
            with self._cache_lock:
                session = self.session_cache[cache_key]
            logger.debug(f"[SessionAgent] Cache hit for User {user_id[:8]}...")
            return session["base_url"], session["headers"]
        
        # SLOW PATH: Queue for authentication
        # Ensure worker is running (non-blocking check)
        if not self._worker_started.is_set():
            # Worker hasn't started yet - this shouldn't happen normally
            # but handle it gracefully by falling back to direct auth
            logger.warning(f"[SessionAgent] Worker not ready for User {user_id[:8]}... Falling back to direct auth")
            from core.executor import get_session as _do_auth
            return _do_auth(creds, chat_id=user_id, force_refresh=force_refresh)
        
        # Create request and add to queue
        request = SessionRequest(
            user_id=user_id,
            creds=creds,
            force_refresh=force_refresh
        )
        
        with self._request_lock:
            self._requests.append(request)
            queue_size = len(self._requests)
        
        # Signal that there's work available
        self._request_event.set()
        
        # Calculate queue position and estimated wait
        position = queue_size
        estimated_wait = position * 10
        
        if position > 1:
            print(
                f"[SessionAgent] Queue Position for User {user_id[:8]}...: {position}/{queue_size}. "
                f"Estimated wait: {estimated_wait}s",
                flush=True
            )
        
        # Wait for the request to be processed (BLOCKING with timeout)
        # This is the ONLY blocking point, and only for cache misses
        if not request.result_event.wait(timeout=timeout):
            raise TimeoutError(f"Session acquisition timed out after {timeout}s for User {user_id[:8]}...")
        
        if request.error:
            raise request.error
        
        return request.result
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status for monitoring."""
        with self._request_lock:
            queue_size = len(self._requests)
        with self._pause_lock:
            is_paused = self._queue_paused
            pause_until = self._pause_until
        return {
            "queue_size": queue_size,
            "paused": is_paused,
            "pause_reason": self._pause_reason,
            "pause_seconds_remaining": max(0, pause_until - time.time()) if is_paused else 0,
            "processed_count": self._processed_count,
            "cached_sessions": len(self.session_cache),
        }


# Global singleton instance (SYNCHRONOUS ACCESS)
_session_manager: Optional[SessionManager] = None
_manager_lock = threading.Lock()


def get_session_manager() -> SessionManager:
    """Get or create the global SessionManager instance (SYNCHRONOUS)."""
    global _session_manager
    if _session_manager is None:
        with _manager_lock:
            if _session_manager is None:
                _session_manager = SessionManager()
                # Worker thread starts automatically in __init__
    return _session_manager


def get_valid_session(
    user_id: str,
    creds: Tuple[str, str, bool, str],
    force_refresh: bool = False,
    timeout: float = 60.0
) -> Tuple[str, Dict[str, str]]:
    """
    Public API: Get a valid session through the SessionAgent (SYNCHRONOUS).
    
    Use this instead of direct get_session() calls to prevent 429 errors.
    
    NON-BLOCKING for cached sessions.
    Only blocks for cache misses (with 60s timeout).
    """
    manager = get_session_manager()
    return manager.get_valid_session_sync(user_id, creds, force_refresh, timeout)


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
