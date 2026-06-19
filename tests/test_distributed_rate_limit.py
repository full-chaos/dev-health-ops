from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

import dev_health_ops.connectors.utils.rate_limit_queue as rate_limit_queue_mod
from dev_health_ops.connectors.utils.rate_limit_queue import (
    _PENALIZE_LUA,
    DistributedRateLimitGate,
    RateLimitConfig,
    RateLimitGate,
    create_rate_limit_gate,
)


def _make_redis_mock(*, script_load_ok: bool = True) -> MagicMock:
    client = MagicMock()
    if script_load_ok:
        client.script_load.return_value = "fake_sha"
    else:
        client.script_load.side_effect = ConnectionError("down")
    return client


class _SharedRedisStub:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def script_load(self, _script: str) -> str:
        return "fake_sha"

    def evalsha(
        self, _sha: str, _num_keys: int, key: str, proposed: str, _ttl: str
    ) -> str:
        current = float(self.store.get(key, "0"))
        value = max(current, float(proposed))
        self.store[key] = str(value)
        return str(value)

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def delete(self, key: str) -> None:
        self.store.pop(key, None)


class TestTokenHash:
    def test_key_uses_provider_org_and_host(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate(
            "github", org_id="org-1", host="api.github.com", redis_client=client
        )
        assert gate._redis_key == "rate_limit:github:org-1:api.github.com"

    def test_key_uses_stable_sentinel_for_missing_parts(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("github", redis_client=client)
        assert gate._redis_key == "rate_limit:github:_:_"


class TestDistributedGateInit:
    def test_redis_key_without_token(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("github", redis_client=client)
        assert gate._redis_key == "rate_limit:github:_:_"

    def test_redis_key_with_token(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate(
            "github", token_hint="ghp_abc", redis_client=client
        )
        assert gate._redis_key == "rate_limit:github:_:ghp_abc"

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
        assert args[0][2] == "rate_limit:gh:_:_"

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


class TestLocalGatePenalizeClamping:
    """Explicit delay (e.g. server Retry-After) must be clamped to max_backoff."""

    def test_explicit_delay_above_max_is_clamped(self):
        cfg = RateLimitConfig(max_backoff_seconds=60.0)
        gate = RateLimitGate(cfg)
        applied = gate.penalize(99_999_999.0)
        assert applied == 60.0

    def test_explicit_delay_below_max_is_preserved(self):
        cfg = RateLimitConfig(max_backoff_seconds=60.0)
        gate = RateLimitGate(cfg)
        assert gate.penalize(10.0) == 10.0

    def test_negative_delay_is_zero(self):
        cfg = RateLimitConfig(max_backoff_seconds=60.0)
        gate = RateLimitGate(cfg)
        assert gate.penalize(-5.0) == 0.0


class TestDistributedGatePenalizeClamping:
    def test_explicit_delay_above_max_is_clamped(self):
        cfg = RateLimitConfig(max_backoff_seconds=30.0)
        client = _make_redis_mock()
        # force local-fallback path so we can inspect applied delay deterministically
        gate = DistributedRateLimitGate("gh", config=cfg, redis_client=client)
        gate._redis_available = False
        before = time.time()
        gate.penalize(10_000.0)
        assert gate._next_allowed_at - before <= 30.0 + 0.5


class TestDistributedSleepSeconds:
    def test_same_provider_org_host_share_backoff_window(self):
        redis = _SharedRedisStub()
        first = DistributedRateLimitGate(
            "github", org_id="org-1", host="api.github.com", redis_client=redis
        )
        second = DistributedRateLimitGate(
            "github", org_id="org-1", host="api.github.com", redis_client=redis
        )

        first.penalize(10.0)

        assert second._sleep_seconds() == pytest.approx(10.0, abs=0.5)

    def test_different_org_has_independent_backoff_window(self):
        redis = _SharedRedisStub()
        first = DistributedRateLimitGate(
            "github", org_id="org-1", host="api.github.com", redis_client=redis
        )
        second = DistributedRateLimitGate(
            "github", org_id="org-2", host="api.github.com", redis_client=redis
        )

        first.penalize(10.0)

        assert second._sleep_seconds() == 0.0

    def test_reads_from_redis(self):
        client = _make_redis_mock()
        future = time.time() + 5.0
        client.get.return_value = str(future)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        seconds = gate._sleep_seconds()
        assert 4.0 < seconds <= 5.0
        client.get.assert_called_once_with("rate_limit:gh:_:_")

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
        # Freeze the module clock so the future-reset margin cannot be eroded by
        # scheduler latency on a loaded runner. Real wall-clock here is a race:
        # if >margin elapses before _sleep_seconds() reads the clock, the
        # computed sleep is <=0 and sleep() is never called.
        fixed_now = 1_000_000.0
        client.get.return_value = str(fixed_now + 5.0)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        with (
            patch(
                "dev_health_ops.connectors.utils.rate_limit_queue.time.time",
                return_value=fixed_now,
            ),
            patch(
                "dev_health_ops.connectors.utils.rate_limit_queue.time.sleep"
            ) as mock_sleep,
        ):
            gate.wait_sync()
            mock_sleep.assert_called_once()
            assert mock_sleep.call_args[0][0] == pytest.approx(5.0)

    def test_wait_async_sleeps(self):
        client = _make_redis_mock()
        fixed_now = 1_000_000.0
        client.get.return_value = str(fixed_now + 5.0)
        gate = DistributedRateLimitGate("gh", redis_client=client)

        async def _noop(*_args, **_kwargs):
            pass

        with (
            patch(
                "dev_health_ops.connectors.utils.rate_limit_queue.time.time",
                return_value=fixed_now,
            ),
            patch(
                "dev_health_ops.connectors.utils.rate_limit_queue.asyncio.sleep",
                side_effect=_noop,
            ) as mock_sleep,
        ):
            asyncio.run(gate.wait_async())
            mock_sleep.assert_called_once()
            assert mock_sleep.call_args[0][0] == pytest.approx(5.0)


class TestDistributedReset:
    def test_reset_deletes_redis_key(self):
        client = _make_redis_mock()
        gate = DistributedRateLimitGate("gh", redis_client=client)
        gate.reset()
        client.delete.assert_called_once_with("rate_limit:gh:_:_")

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
        assert args[2] == "rate_limit:gh:_:_"
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
            with patch("valkey.from_url", return_value=mock_redis):
                gate = create_rate_limit_gate("github", token_hint="token123")

        assert isinstance(gate, DistributedRateLimitGate)

    def test_factory_uses_provider_org_host_key(self):
        mock_redis = _make_redis_mock()
        mock_redis.ping.return_value = True

        with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
            with patch("valkey.from_url", return_value=mock_redis):
                gate = create_rate_limit_gate(
                    "gitlab", org_id="org-a", host="gitlab.example.com"
                )

        assert isinstance(gate, DistributedRateLimitGate)
        assert gate._redis_key == "rate_limit:gitlab:org-a:gitlab.example.com"

    def test_returns_local_when_no_redis_url(self):
        with patch.dict("os.environ", {}, clear=True):
            gate = create_rate_limit_gate("github")

        assert type(gate) is RateLimitGate

    def test_returns_local_when_redis_connection_fails(self):
        with patch.dict("os.environ", {"REDIS_URL": "redis://bad:6379"}):
            with patch("valkey.from_url", side_effect=ConnectionError("refused")):
                gate = create_rate_limit_gate("github")

        assert type(gate) is RateLimitGate

    def test_caches_redis_unavailable_for_60s(self):
        with patch.dict("os.environ", {"REDIS_URL": "redis://bad:6379"}):
            with patch("valkey.from_url", side_effect=ConnectionError("refused")):
                create_rate_limit_gate("github")

        assert rate_limit_queue_mod._redis_unavailable_until > time.time()
        assert rate_limit_queue_mod._redis_unavailable_until <= time.time() + 61.0

    def test_skips_redis_during_cooldown(self):
        rate_limit_queue_mod._redis_unavailable_until = time.time() + 30.0

        with patch.dict("os.environ", {"REDIS_URL": "redis://localhost:6379"}):
            with patch("valkey.from_url") as mock_from_url:
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

        with (
            patch.dict(
                "os.environ",
                {"CELERY_BROKER_URL": "redis://localhost:6379"},
                clear=True,
            ),
            patch("valkey.from_url", return_value=mock_redis),
        ):
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
