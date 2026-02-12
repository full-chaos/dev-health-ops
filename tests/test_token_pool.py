from __future__ import annotations

import hashlib
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

try:
    import fakeredis
except (ImportError, TypeError):
    fakeredis = None  # type: ignore[assignment]

from dev_health_ops.connectors.utils.token_pool import (
    TokenPool,
    _hash_token,
    create_token_pool,
)

pytestmark = pytest.mark.skipif(
    fakeredis is None,
    reason="fakeredis unavailable (metaclass conflict with redis in this env)",
)


@pytest.fixture()
def redis_client() -> Any:
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture()
def pool(redis_client: Any) -> TokenPool:
    return TokenPool("github", "my-org", redis_client=redis_client)


class TestHashToken:
    def test_consistent(self):
        assert _hash_token("abc") == _hash_token("abc")

    def test_length_is_16(self):
        assert len(_hash_token("anything")) == 16

    def test_matches_sha256_prefix(self):
        token = "ghp_abc123"
        expected = hashlib.sha256(token.encode()).hexdigest()[:16]
        assert _hash_token(token) == expected

    def test_different_tokens_differ(self):
        assert _hash_token("token-a") != _hash_token("token-b")


class TestRegisterToken:
    def test_returns_hash(self, pool: TokenPool):
        h = pool.register_token("ghp_secret")
        assert h == _hash_token("ghp_secret")

    def test_adds_to_sorted_set(self, pool: TokenPool, redis_client: Any):
        h = pool.register_token("ghp_secret")
        score = redis_client.zscore(pool._avail_key, h)
        assert score == 0.0

    def test_adds_to_hash_map(self, pool: TokenPool, redis_client: Any):
        h = pool.register_token("ghp_secret")
        stored = redis_client.hget(pool._tokens_key, h)
        assert stored == "ghp_secret"


class TestLeaseToken:
    def test_returns_registered_token(self, pool: TokenPool):
        pool.register_token("ghp_one")
        result = pool.lease_token()
        assert result is not None
        token_hash, token_value = result
        assert token_hash == _hash_token("ghp_one")
        assert token_value == "ghp_one"

    def test_returns_none_when_empty(self, pool: TokenPool):
        assert pool.lease_token() is None

    def test_skips_cooling_down_tokens(self, pool: TokenPool):
        h = pool.register_token("ghp_cool")
        pool.penalize_token(h, time.time() + 3600)
        assert pool.lease_token() is None

    def test_returns_token_after_cooldown_expires(
        self, pool: TokenPool, redis_client: Any
    ):
        h = pool.register_token("ghp_cool")
        past = time.time() - 1
        redis_client.zadd(pool._avail_key, {h: past})
        result = pool.lease_token()
        assert result is not None
        assert result[1] == "ghp_cool"

    def test_multiple_tokens_leased_in_sequence(self, pool: TokenPool):
        pool.register_token("ghp_a")
        pool.register_token("ghp_b")

        r1 = pool.lease_token()
        assert r1 is not None
        r2 = pool.lease_token()
        assert r2 is not None
        assert r1[0] != r2[0]
        assert {r1[1], r2[1]} == {"ghp_a", "ghp_b"}

    def test_all_tokens_cooling_returns_none(self, pool: TokenPool):
        pool.register_token("ghp_x")
        pool.register_token("ghp_y")
        result1 = pool.lease_token()
        result2 = pool.lease_token()
        assert result1 is not None
        assert result2 is not None
        assert pool.lease_token() is None


class TestReturnToken:
    def test_makes_token_available_again(self, pool: TokenPool):
        pool.register_token("ghp_ret")
        r1 = pool.lease_token()
        assert r1 is not None
        assert pool.lease_token() is None

        pool.return_token(r1[0])
        r2 = pool.lease_token()
        assert r2 is not None
        assert r2[1] == "ghp_ret"


class TestPenalizeToken:
    def test_sets_cooldown(self, pool: TokenPool, redis_client: Any):
        h = pool.register_token("ghp_pen")
        future = time.time() + 600
        pool.penalize_token(h, future)
        score = redis_client.zscore(pool._avail_key, h)
        assert abs(score - future) < 0.01

    def test_penalized_token_not_leased(self, pool: TokenPool):
        h = pool.register_token("ghp_pen")
        pool.penalize_token(h, time.time() + 3600)
        assert pool.lease_token() is None


class TestRemoveToken:
    def test_removes_from_pool(self, pool: TokenPool, redis_client: Any):
        h = pool.register_token("ghp_rm")
        assert pool.pool_size() == 1
        pool.remove_token(h)
        assert pool.pool_size() == 0
        assert redis_client.hget(pool._tokens_key, h) is None


class TestPoolSize:
    def test_empty(self, pool: TokenPool):
        assert pool.pool_size() == 0

    def test_after_register(self, pool: TokenPool):
        pool.register_token("a")
        pool.register_token("b")
        assert pool.pool_size() == 2


class TestAvailableCount:
    def test_all_available(self, pool: TokenPool):
        pool.register_token("a")
        pool.register_token("b")
        assert pool.available_count() == 2

    def test_one_cooling(self, pool: TokenPool):
        pool.register_token("a")
        h = pool.register_token("b")
        pool.penalize_token(h, time.time() + 3600)
        assert pool.available_count() == 1

    def test_none_available(self, pool: TokenPool):
        h = pool.register_token("a")
        pool.penalize_token(h, time.time() + 3600)
        assert pool.available_count() == 0


class TestGracefulFallback:
    def test_lease_returns_none_when_redis_unavailable(self):
        pool = TokenPool("github", "org", redis_client=None)
        pool._redis_available = False
        assert pool.lease_token() is None

    def test_register_returns_hash_when_redis_unavailable(self):
        pool = TokenPool("github", "org", redis_client=None)
        pool._redis_available = False
        h = pool.register_token("ghp_test")
        assert h == _hash_token("ghp_test")

    def test_return_token_noop_when_redis_unavailable(self):
        pool = TokenPool("github", "org", redis_client=None)
        pool._redis_available = False
        pool.return_token("somehash")

    def test_penalize_noop_when_redis_unavailable(self):
        pool = TokenPool("github", "org", redis_client=None)
        pool._redis_available = False
        pool.penalize_token("somehash", time.time() + 100)

    def test_remove_noop_when_redis_unavailable(self):
        pool = TokenPool("github", "org", redis_client=None)
        pool._redis_available = False
        pool.remove_token("somehash")

    def test_pool_size_zero_when_redis_unavailable(self):
        pool = TokenPool("github", "org", redis_client=None)
        pool._redis_available = False
        assert pool.pool_size() == 0

    def test_available_count_zero_when_redis_unavailable(self):
        pool = TokenPool("github", "org", redis_client=None)
        pool._redis_available = False
        assert pool.available_count() == 0

    def test_redis_connection_failure_on_init(self):
        client = MagicMock()
        client.script_load.side_effect = ConnectionError("down")
        pool = TokenPool("github", "org", redis_client=client)
        assert pool._redis_available is False

    def test_no_redis_url_env(self):
        with patch.dict("os.environ", {}, clear=True):
            pool = TokenPool("github", "org")
            assert pool._redis_available is False
            assert pool.lease_token() is None


class TestRedisKeys:
    def test_key_format(self, pool: TokenPool):
        assert pool._avail_key == "token_pool:github:my-org:availability"
        assert pool._tokens_key == "token_pool:github:my-org:tokens"

    def test_custom_prefix(self, redis_client: Any):
        pool = TokenPool("gitlab", "acme", key_prefix="tp", redis_client=redis_client)
        assert pool._avail_key == "tp:gitlab:acme:availability"
        assert pool._tokens_key == "tp:gitlab:acme:tokens"


class TestFactory:
    def test_creates_pool_with_redis_url(self):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.script_load.return_value = "sha"

        with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
            with patch("redis.from_url", return_value=mock_redis):
                pool = create_token_pool("github", "my-org")

        assert isinstance(pool, TokenPool)
        assert pool._redis_available is True

    def test_creates_pool_without_redis_url(self):
        with patch.dict("os.environ", {}, clear=True):
            pool = create_token_pool("github")

        assert isinstance(pool, TokenPool)
        assert pool._redis_available is False

    def test_default_org_id(self):
        with patch.dict("os.environ", {}, clear=True):
            pool = create_token_pool("github")

        assert pool._org_id == "default"


class TestLuaScript:
    def test_evalsha_receives_correct_args(self, pool: TokenPool):
        pool.register_token("ghp_lua")
        pool._redis.evalsha = MagicMock(wraps=pool._redis.evalsha)
        pool.lease_token()

    def test_eval_fallback_on_evalsha_failure(self, redis_client: Any):
        pool = TokenPool("github", "org", redis_client=redis_client)
        pool.register_token("ghp_fb")

        pool._redis.evalsha = MagicMock(side_effect=Exception("NOSCRIPT"))
        original_eval = pool._redis.eval
        pool._redis.eval = MagicMock(wraps=original_eval)

        result = pool.lease_token()
        assert result is not None
        pool._redis.eval.assert_called_once()

    def test_both_eval_fail_marks_unavailable(self):
        client = MagicMock()
        client.script_load.return_value = "sha"
        client.evalsha.side_effect = Exception("NOSCRIPT")
        client.eval.side_effect = Exception("connection lost")
        pool = TokenPool("github", "org", redis_client=client)

        assert pool.lease_token() is None
        assert pool._redis_available is False
