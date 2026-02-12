from __future__ import annotations

import hashlib
import time
from unittest.mock import MagicMock, patch

import pytest

import dev_health_ops.connectors.utils.rate_limit_queue as rate_limit_queue_mod
from dev_health_ops.connectors.utils.rate_limit_queue import (
    DistributedRateLimitGate,
    RateLimitConfig,
    RateLimitGate,
    _PENALIZE_LUA,
    _token_hash,
    create_rate_limit_gate,
)


def _make_redis_mock(*, script_load_ok: bool = True) -> MagicMock:
    client = MagicMock()
    if script_load_ok:
        client.script_load.return_value = "fake_sha"
    else:
        client.script_load.side_effect = ConnectionError("down")
    return client


class TestTokenHash:
    def test_consistent_output(self):
        assert _token_hash("my-secret") == _token_hash("my-secret")

    def test_length_is_8(self):
        assert len(_token_hash("anything")) == 8

    def test_matches_sha256_prefix(self):
        token = "ghp_abc123"
        expected = hashlib.sha256(token.encode()).hexdigest()[:8]
        assert _token_hash(token) == expected

    def test_different_tokens_differ(self):
        assert _token_hash("token-a") != _token_hash("token-b")


class TestDistributedGateInit:
    def test_redis_key_without_token(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("github", redis_client=client)
        assert gate._redis_key == "rate_limit:github"

    def test_redis_key_with_token(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate(
            "github", token_hint="ghp_abc", redis_client=client
        )
        expected_hash = _token_hash("ghp_abc")
        assert gate._redis_key == f"rate_limit:github:{expected_hash}"

    def test_ttl_is_2x_max_backoff(self):
        cfg = RateLimitConfig(max_backoff_seconds=600.0)
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("gh", config=cfg, redis_client=client)
        assert gate._ttl == 1200

    def test_script_load_called(self):
        client = _make_redis_mock()
        DistributedRateLimitGate("gh", redis_client=client)
        client.script_load.assert_called_once_with(_PENALIZE_LUA)

    def test_fallback_when_script_load_fails(self):
        client = _make_redis_mock(script_load_ok=False)
        gate = DistributedRateLimitGate("gh", redis_client=client)
        assert gate._redis_available is False

    def test_env_connect_no_url(self):
        with patch.dict("os.environ", {}, clear=True):
            gate = DistributedRateLimitGate("gh")
            assert gate._redis_available is False


class TestDistributedPenalize:
    def test_penalize_explicit_delay_calls_evalsha(self):
        client = _make_redis_mock()
        client.evalsha.return_value = str(time.time() + 5.0)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        result = gate.penalize(5.0)

        assert result == 5.0
        client.evalsha.assert_called_once()
        args = client.evalsha.call_args
        assert args[0][0] == "fake_sha"
        assert args[0][1] == 1
        assert args[0][2] == "rate_limit:gh"

    def test_penalize_exponential_backoff(self):
        cfg = RateLimitConfig(initial_backoff_seconds=2.0, backoff_factor=3.0)
        client = _make_redis_mock()
        client.evalsha.return_value = str(time.time() + 10)
        gate = DistributedRateLimitGate("gh", config=cfg, redis_client=client)

        d1 = gate.penalize()
        assert d1 == 2.0

        client.evalsha.return_value = str(time.time() + 20)
        d2 = gate.penalize()
        assert d2 == 6.0

    def test_penalize_falls_back_on_evalsha_and_eval_failure(self):
        client = _make_redis_mock()
        client.evalsha.side_effect = Exception("NOSCRIPT")
        client.eval.side_effect = Exception("connection lost")
        gate = DistributedRateLimitGate("gh", redis_client=client)

        result = gate.penalize(3.0)
        assert result == 3.0
        assert gate._redis_available is False

    def test_penalize_retries_with_eval_on_evalsha_failure(self):
        client = _make_redis_mock()
        client.evalsha.side_effect = Exception("NOSCRIPT")
        client.eval.return_value = str(time.time() + 5.0)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        result = gate.penalize(5.0)
        assert result == 5.0
        client.eval.assert_called_once()
        assert gate._redis_available is True

    def test_penalize_syncs_local_state(self):
        future = time.time() + 10.0
        client = _make_redis_mock()
        client.evalsha.return_value = str(future)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        gate.penalize(10.0)
        assert abs(gate._next_allowed_at - future) < 0.1

    def test_penalize_local_fallback_uses_max(self):
        client = _make_redis_mock()
        client.evalsha.side_effect = Exception("down")
        client.eval.side_effect = Exception("down")
        gate = DistributedRateLimitGate("gh", redis_client=client)

        gate.penalize(5.0)
        first = gate._next_allowed_at

        gate._redis_available = False
        gate.penalize(2.0)
        assert gate._next_allowed_at >= first


class TestDistributedSleepSeconds:
    def test_reads_from_redis(self):
        client = _make_redis_mock()
        future = time.time() + 5.0
        client.get.return_value = str(future)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        seconds = gate._sleep_seconds()
        assert 4.0 < seconds <= 5.0
        client.get.assert_called_once_with("rate_limit:gh")

    def test_returns_zero_when_key_missing(self):
        client = _make_redis_mock()
        client.get.return_value = None
        gate = DistributedRateLimitGate("gh", redis_client=client)

        assert gate._sleep_seconds() == 0.0

    def test_falls_back_to_local_on_redis_error(self):
        client = _make_redis_mock()
        client.get.side_effect = ConnectionError("gone")
        gate = DistributedRateLimitGate("gh", redis_client=client)
        gate._next_allowed_at = time.time() + 3.0

        seconds = gate._sleep_seconds()
        assert 2.0 < seconds <= 3.0
        assert gate._redis_available is False


class TestDistributedWait:
    def test_wait_sync_sleeps(self):
        client = _make_redis_mock()
        client.get.return_value = str(time.time() + 0.05)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        with patch(
            "dev_health_ops.connectors.utils.rate_limit_queue.time.sleep"
        ) as mock_sleep:
            gate.wait_sync()
            mock_sleep.assert_called_once()
            assert mock_sleep.call_args[0][0] > 0

    @pytest.mark.asyncio
    async def test_wait_async_sleeps(self):
        client = _make_redis_mock()
        client.get.return_value = str(time.time() + 0.05)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        async def _noop(*_args, **_kwargs):
            pass

        with patch(
            "dev_health_ops.connectors.utils.rate_limit_queue.asyncio.sleep",
            side_effect=_noop,
        ) as mock_sleep:
            await gate.wait_async()
            mock_sleep.assert_called_once()


class TestDistributedReset:
    def test_reset_deletes_redis_key(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("gh", redis_client=client)
        gate.reset()
        client.delete.assert_called_once_with("rate_limit:gh")

    def test_reset_falls_back_on_redis_error(self):
        client = _make_redis_mock()
        client.delete.side_effect = ConnectionError("gone")
        gate = DistributedRateLimitGate("gh", redis_client=client)
        gate.reset()
        assert gate._redis_available is False
        assert gate._current_backoff == gate._config.initial_backoff_seconds


class TestWarnOnce:
    def test_logs_only_once(self):
        client = _make_redis_mock()
        client.get.side_effect = ConnectionError("gone")
        gate = DistributedRateLimitGate("gh", redis_client=client)

        gate._sleep_seconds()
        gate._redis_available = True
        client.get.side_effect = ConnectionError("gone again")
        gate._sleep_seconds()

        assert gate._warned is True


class TestLuaScript:
    def test_evalsha_receives_correct_args(self):
        client = _make_redis_mock()
        client.evalsha.return_value = str(time.time() + 10)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        gate.penalize(10.0)

        args = client.evalsha.call_args[0]
        assert args[0] == "fake_sha"
        assert args[1] == 1
        assert args[2] == "rate_limit:gh"
        proposed = float(args[3])
        assert proposed > time.time()
        ttl = int(args[4])
        assert ttl == gate._ttl

    def test_eval_fallback_sends_lua_source(self):
        client = _make_redis_mock()
        client.evalsha.side_effect = Exception("NOSCRIPT")
        client.eval.return_value = str(time.time() + 5)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        gate.penalize(5.0)

        args = client.eval.call_args[0]
        assert args[0] == _PENALIZE_LUA


class TestTTL:
    def test_default_ttl(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("gh", redis_client=client)
        assert gate._ttl == 600

    def test_custom_ttl(self):
        cfg = RateLimitConfig(max_backoff_seconds=120.0)
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("gh", config=cfg, redis_client=client)
        assert gate._ttl == 240

    def test_ttl_passed_to_evalsha(self):
        client = _make_redis_mock()
        client.evalsha.return_value = str(time.time() + 5)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        gate.penalize(5.0)

        ttl_arg = client.evalsha.call_args[0][4]
        assert int(ttl_arg) == gate._ttl


class TestFactory:
    def setup_method(self):
        rate_limit_queue_mod._redis_unavailable_until = 0.0

    def test_returns_distributed_when_redis_available(self):
        mock_redis = _make_redis_mock()
        mock_redis.ping.return_value = True

        with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
            with patch("redis.from_url", return_value=mock_redis):
                gate = create_rate_limit_gate("github", "token123")

        assert isinstance(gate, DistributedRateLimitGate)

    def test_returns_local_when_no_redis_url(self):
        with patch.dict("os.environ", {}, clear=True):
            gate = create_rate_limit_gate("github")

        assert type(gate) is RateLimitGate

    def test_returns_local_when_redis_connection_fails(self):
        with patch.dict("os.environ", {"REDIS_URL": "redis://bad:6379"}):
            with patch("redis.from_url", side_effect=ConnectionError("refused")):
                gate = create_rate_limit_gate("github")

        assert type(gate) is RateLimitGate

    def test_caches_redis_unavailable_for_60s(self):
        with patch.dict("os.environ", {"REDIS_URL": "redis://bad:6379"}):
            with patch("redis.from_url", side_effect=ConnectionError("refused")):
                create_rate_limit_gate("github")

        assert rate_limit_queue_mod._redis_unavailable_until > time.time()
        assert rate_limit_queue_mod._redis_unavailable_until <= time.time() + 61.0

    def test_skips_redis_during_cooldown(self):
        rate_limit_queue_mod._redis_unavailable_until = time.time() + 30.0

        with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
            with patch("redis.from_url") as mock_from_url:
                gate = create_rate_limit_gate("github")

        mock_from_url.assert_not_called()
        assert type(gate) is RateLimitGate

    def test_accepts_config(self):
        cfg = RateLimitConfig(initial_backoff_seconds=5.0)
        with patch.dict("os.environ", {}, clear=True):
            gate = create_rate_limit_gate("github", config=cfg)

        assert gate._config.initial_backoff_seconds == 5.0

    def test_uses_celery_broker_url(self):
        mock_redis = _make_redis_mock()
        mock_redis.ping.return_value = True

        with patch.dict(
            "os.environ",
            {"CELERY_BROKER_URL": "redis://localhost:6379"},
            clear=True,
        ):
            with patch("redis.from_url", return_value=mock_redis):
                gate = create_rate_limit_gate("github")

        assert isinstance(gate, DistributedRateLimitGate)


class TestGracefulFallback:
    def test_penalize_then_wait_after_redis_dies(self):
        client = _make_redis_mock()
        client.evalsha.return_value = str(time.time() + 0.01)
        client.get.return_value = str(time.time() + 0.01)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        gate.penalize(0.01)

        client.get.side_effect = ConnectionError("gone")
        seconds = gate._sleep_seconds()
        assert seconds >= 0.0
        assert gate._redis_available is False

        gate.penalize(0.01)
        seconds = gate._sleep_seconds()
        assert seconds >= 0.0

    def test_is_drop_in_replacement(self):
        client = _make_redis_mock()
        client.evalsha.return_value = str(time.time() + 1)
        client.get.return_value = str(time.time() + 1)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        assert hasattr(gate, "penalize")
        assert hasattr(gate, "wait_sync")
        assert hasattr(gate, "wait_async")
        assert hasattr(gate, "reset")
        assert isinstance(gate, RateLimitGate)
