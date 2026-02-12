"""Redis-backed token pool with lease/return/penalize semantics.

Degrades gracefully when Redis is unavailable: ``lease_token`` returns
``None`` and mutating methods become silent no-ops.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua script — atomic lease
# ---------------------------------------------------------------------------
# KEYS[1] = availability sorted set
# KEYS[2] = tokens hash
# ARGV[1] = now (epoch float)
# ARGV[2] = lease_until (epoch float, now + lease_duration)
#
# Finds the member with the lowest score that is <= now (i.e. available),
# bumps its score to lease_until so no other worker can grab it, and returns
# the token hash + plaintext value.

_LEASE_LUA = """\
local avail_key = KEYS[1]
local tokens_key = KEYS[2]
local now = tonumber(ARGV[1])
local lease_until = tonumber(ARGV[2])

local candidates = redis.call('ZRANGEBYSCORE', avail_key, '-inf', tostring(now), 'LIMIT', 0, 1)
if #candidates == 0 then
    return nil
end

local token_hash = candidates[1]
redis.call('ZADD', avail_key, tostring(lease_until), token_hash)

local token_value = redis.call('HGET', tokens_key, token_hash)
return {token_hash, token_value or ''}
"""


def _hash_token(token: str) -> str:
    """Return first 16 hex chars of SHA-256 of *token*."""
    return hashlib.sha256(token.encode()).hexdigest()[:16]


class TokenPool:
    """Redis-backed multi-token manager with lease/return/penalize."""

    def __init__(
        self,
        provider: str,
        org_id: str = "default",
        redis_url: str | None = None,
        key_prefix: str = "token_pool",
        *,
        lease_duration: float = 300.0,
        redis_client: Any = None,
    ) -> None:
        self._provider = provider
        self._org_id = org_id
        self._lease_duration = lease_duration

        base = f"{key_prefix}:{provider}:{org_id}"
        self._avail_key = f"{base}:availability"
        self._tokens_key = f"{base}:tokens"

        self._redis: Any = None
        self._redis_available = True
        self._warned = False
        self._lua_sha: str | None = None

        if redis_client is not None:
            self._redis = redis_client
            try:
                self._lua_sha = redis_client.script_load(_LEASE_LUA)
            except Exception:
                self._redis_available = False
                self._warn_once("Failed to load Lua script into Redis")
        else:
            self._connect(redis_url)

    # -- connection helpers --------------------------------------------------

    def _connect(self, redis_url: str | None = None) -> None:
        """Attempt to connect to Redis."""
        url = redis_url or os.getenv("REDIS_URL") or ""
        if not url:
            self._redis_available = False
            self._warn_once("No REDIS_URL configured — token pool disabled")
            return
        try:
            import redis as _redis_mod

            client = _redis_mod.from_url(url, decode_responses=True)
            client.ping()
            self._redis = client
            self._lua_sha = client.script_load(_LEASE_LUA)
            logger.info(
                "Token pool connected to Redis for %s/%s",
                self._provider,
                self._org_id,
            )
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis unavailable, token pool disabled: {exc}")

    def _warn_once(self, msg: str) -> None:
        if not self._warned:
            logger.warning(msg)
            self._warned = True

    # -- public interface ----------------------------------------------------

    def register_token(self, token: str) -> str:
        """Add *token* to the pool.  Returns the token hash (16-char hex)."""
        token_hash = _hash_token(token)
        if not self._redis_available or self._redis is None:
            self._warn_once("Redis unavailable — register_token is a no-op")
            return token_hash
        try:
            self._redis.zadd(self._avail_key, {token_hash: 0})
            self._redis.hset(self._tokens_key, token_hash, token)
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis register_token failed: {exc}")
        return token_hash

    def lease_token(self) -> tuple[str, str] | None:
        """Atomically lease the most-available token, or ``None`` if all are cooling down."""
        if not self._redis_available or self._redis is None:
            return None
        now = time.time()
        lease_until = now + self._lease_duration
        try:
            result = self._redis.evalsha(
                self._lua_sha,
                2,
                self._avail_key,
                self._tokens_key,
                str(now),
                str(lease_until),
            )
        except Exception:
            # Retry with raw EVAL in case EVALSHA fails (NOSCRIPT).
            try:
                result = self._redis.eval(
                    _LEASE_LUA,
                    2,
                    self._avail_key,
                    self._tokens_key,
                    str(now),
                    str(lease_until),
                )
            except Exception as exc:
                self._redis_available = False
                self._warn_once(f"Redis lease_token failed: {exc}")
                return None

        if result is None:
            return None
        token_hash: str = result[0]
        token_value: str = result[1]
        return (token_hash, token_value)

    def return_token(self, token_hash: str) -> None:
        """Mark a leased token as immediately available again."""
        if not self._redis_available or self._redis is None:
            return
        try:
            self._redis.zadd(self._avail_key, {token_hash: 0})
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis return_token failed: {exc}")

    def penalize_token(self, token_hash: str, cooldown_until: float) -> None:
        """Set a token's cooldown so it won't be leased until *cooldown_until*."""
        if not self._redis_available or self._redis is None:
            return
        try:
            self._redis.zadd(self._avail_key, {token_hash: cooldown_until})
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis penalize_token failed: {exc}")

    def remove_token(self, token_hash: str) -> None:
        """Remove a token from the pool entirely."""
        if not self._redis_available or self._redis is None:
            return
        try:
            self._redis.zrem(self._avail_key, token_hash)
            self._redis.hdel(self._tokens_key, token_hash)
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis remove_token failed: {exc}")

    def pool_size(self) -> int:
        """Return the total number of tokens in the pool."""
        if not self._redis_available or self._redis is None:
            return 0
        try:
            return int(self._redis.zcard(self._avail_key))
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis pool_size failed: {exc}")
            return 0

    def available_count(self) -> int:
        """Return the number of tokens currently available (not cooling down)."""
        if not self._redis_available or self._redis is None:
            return 0
        try:
            now = time.time()
            return int(self._redis.zcount(self._avail_key, "-inf", str(now)))
        except Exception as exc:
            self._redis_available = False
            self._warn_once(f"Redis available_count failed: {exc}")
            return 0


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_token_pool(
    provider: str,
    org_id: str = "default",
) -> TokenPool:
    """Create a :class:`TokenPool` using ``REDIS_URL`` from the environment."""
    redis_url = os.getenv("REDIS_URL") or None
    return TokenPool(provider=provider, org_id=org_id, redis_url=redis_url)
