"""Queue-based rate limiting helpers.

These utilities provide a simple, shared backoff gate that can be used by
multiple workers (threads or asyncio tasks) to coordinate pauses when a
rate-limit response is encountered.

The goal is to avoid a stampede where many workers repeatedly hit the API
at the same time.

The ``DistributedRateLimitGate`` extends this to cross-process coordination
via Redis, falling back to the process-local ``RateLimitGate`` when Redis is
unavailable.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 300.0
    backoff_factor: float = 2.0


class RateLimitGate:
    """Thread-safe, event-loop-friendly shared backoff gate."""

    def __init__(self, config: Optional[RateLimitConfig] = None) -> None:
        self._config = config or RateLimitConfig()
        self._lock = threading.Lock()
        self._next_allowed_at = 0.0
        self._current_backoff = self._config.initial_backoff_seconds

    def reset(self) -> None:
        with self._lock:
            self._current_backoff = self._config.initial_backoff_seconds

    def penalize(self, delay_seconds: Optional[float] = None) -> float:
        """Push the next allowed time into the future.

        If delay_seconds is not provided, uses exponential backoff.
        Returns the applied delay.
        """
        with self._lock:
            if delay_seconds is None:
                delay_seconds = min(
                    self._current_backoff,
                    self._config.max_backoff_seconds,
                )
                self._current_backoff = min(
                    self._current_backoff * self._config.backoff_factor,
                    self._config.max_backoff_seconds,
                )
            else:
                # If we get an explicit server reset delay, keep exponential
                # backoff state but still honor the explicit delay.
                delay_seconds = max(0.0, float(delay_seconds))

            now = time.time()
            self._next_allowed_at = max(
                self._next_allowed_at,
                now + delay_seconds,
            )
            return delay_seconds

    def _sleep_seconds(self) -> float:
        with self._lock:
            return max(0.0, self._next_allowed_at - time.time())

    def wait_sync(self) -> None:
        seconds = self._sleep_seconds()
        if seconds > 0:
            time.sleep(seconds)

    async def wait_async(self) -> None:
        seconds = self._sleep_seconds()
        if seconds > 0:
            await asyncio.sleep(seconds)


# ---------------------------------------------------------------------------
# Distributed (Redis-backed) rate limit gate
# ---------------------------------------------------------------------------

# Lua script for atomic penalize: reads current next_available_at, compares
# with proposed value, and SETs to the max.  Returns the delay that was
# actually applied.
_PENALIZE_LUA = """\
local key = KEYS[1]
local proposed = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local current = tonumber(redis.call('GET', key) or '0') or 0
if proposed > current then
    redis.call('SET', key, tostring(proposed))
    redis.call('EXPIRE', key, ttl)
    return tostring(proposed)
else
    -- Refresh TTL even when we don't update the value so the key
    -- doesn't expire while workers are still waiting.
    redis.call('EXPIRE', key, ttl)
    return tostring(current)
end
"""


def _token_hash(token: str) -> str:
    """Return first 8 hex chars of SHA-256 of *token*."""
    return hashlib.sha256(token.encode()).hexdigest()[:8]


class DistributedRateLimitGate(RateLimitGate):
    """Redis-backed rate limit gate with local fallback.

    Shares the ``next_available_at`` timestamp across processes via a Redis
    key.  If Redis becomes unavailable at any point the gate silently falls
    back to the inherited process-local behaviour and logs a single warning.
    """

    def __init__(
        self,
        provider: str,
        token_hint: str = "",
        config: Optional[RateLimitConfig] = None,
        *,
        redis_client: Any = None,
    ) -> None:
        super().__init__(config=config)
        self._provider = provider
        self._token_hint = token_hint

        token_part = f":{_token_hash(token_hint)}" if token_hint else ""
        self._redis_key = f"rate_limit:{provider}{token_part}"
        self._ttl = int((self._config.max_backoff_seconds or 300) * 2)

        self._redis: Any = None
        self._redis_available = True
        self._warned = False
        self._lua_sha: Any = None

        if redis_client is not None:
            self._redis = redis_client
            try:
                self._lua_sha = redis_client.script_load(_PENALIZE_LUA)
            except Exception:
                self._redis_available = False
                self._warn_once("Failed to load Lua script into Redis")
        else:
            self._connect()

    # -- connection helpers --------------------------------------------------

    def _connect(self) -> None:
        """Attempt to connect to Redis using environment variables."""
        redis_url = os.getenv("REDIS_URL") or os.getenv("CELERY_BROKER_URL") or ""
        if not redis_url:
            self._redis_available = False
            self._warn_once("No REDIS_URL or CELERY_BROKER_URL configured")
            return
        try:
            import redis as _redis_mod

            client = _redis_mod.from_url(redis_url, decode_responses=True)
            client.ping()
            self._redis = client
            self._lua_sha = client.script_load(_PENALIZE_LUA)
            logger.info(
                "Distributed rate-limit gate connected to Redis for %s",
                self._redis_key,
            )
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis unavailable, using local fallback: {exc}")

    def _warn_once(self, msg: str) -> None:
        if not self._warned:
            logger.warning(msg)
            self._warned = True

    # -- public interface (same as RateLimitGate) ----------------------------

    def penalize(self, delay_seconds: Optional[float] = None) -> float:
        """Push the shared next-allowed time into the future via Redis.

        Falls back to local penalize if Redis is unavailable.
        """
        with self._lock:
            if delay_seconds is None:
                delay_seconds = min(
                    self._current_backoff,
                    self._config.max_backoff_seconds,
                )
                self._current_backoff = min(
                    self._current_backoff * self._config.backoff_factor,
                    self._config.max_backoff_seconds,
                )
            else:
                delay_seconds = max(0.0, float(delay_seconds))

            now = time.time()
            proposed = now + delay_seconds

        if self._redis_available and self._redis is not None:
            try:
                result = self._redis.evalsha(
                    self._lua_sha,
                    1,
                    self._redis_key,
                    str(proposed),
                    str(self._ttl),
                )
                applied = float(result)
                with self._lock:
                    self._next_allowed_at = applied
                return delay_seconds
            except Exception:
                try:
                    result = self._redis.eval(
                        _PENALIZE_LUA,
                        1,
                        self._redis_key,
                        str(proposed),
                        str(self._ttl),
                    )
                    applied = float(result)
                    with self._lock:
                        self._next_allowed_at = applied
                    return delay_seconds
                except Exception as exc:
                    self._redis_available = False
                    self._warn_once(
                        f"Redis penalize failed, falling back to local: {exc}"
                    )

        with self._lock:
            self._next_allowed_at = max(self._next_allowed_at, proposed)
        return delay_seconds

    def _sleep_seconds(self) -> float:
        """Read shared next_available_at from Redis, fall back to local."""
        if self._redis_available and self._redis is not None:
            try:
                raw = self._redis.get(self._redis_key)
                if raw is not None:
                    return max(0.0, float(raw) - time.time())
                return 0.0
            except Exception as exc:
                self._redis_available = False
                self._warn_once(f"Redis read failed, falling back to local: {exc}")

        with self._lock:
            return max(0.0, self._next_allowed_at - time.time())

    def reset(self) -> None:
        """Reset backoff state locally and delete the Redis key."""
        super().reset()
        if self._redis_available and self._redis is not None:
            try:
                self._redis.delete(self._redis_key)
            except Exception as exc:
                self._redis_available = False
                self._warn_once(f"Redis reset failed, falling back to local: {exc}")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

# Module-level cache so we don't retry Redis connections on every call when
# Redis is known to be down.
_redis_unavailable_until: float = 0.0
_factory_lock = threading.Lock()


def create_rate_limit_gate(
    provider: str,
    token_hint: str = "",
    config: Optional[RateLimitConfig] = None,
) -> RateLimitGate:
    """Create a rate-limit gate, preferring Redis-backed when available.

    If Redis is unreachable the factory returns a plain ``RateLimitGate`` and
    caches the failure for 60 seconds so subsequent calls don't retry the
    connection immediately.
    """
    global _redis_unavailable_until  # noqa: PLW0603

    with _factory_lock:
        now = time.time()
        if now < _redis_unavailable_until:
            return RateLimitGate(config=config)

    try:
        gate = DistributedRateLimitGate(
            provider=provider,
            token_hint=token_hint,
            config=config,
        )
        if gate._redis_available:
            return gate
        with _factory_lock:
            _redis_unavailable_until = time.time() + 60.0
        return RateLimitGate(config=config)
    except Exception:
        with _factory_lock:
            _redis_unavailable_until = time.time() + 60.0
        return RateLimitGate(config=config)
