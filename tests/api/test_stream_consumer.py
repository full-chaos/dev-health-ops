"""Tests for the shared StreamConsumer base and blocking-safe client factory.

Covers the two defects this module was created to fix:

1. Blocking-read clients must use ``socket_timeout=None`` so a
   ``XREADGROUP(block=BLOCK_MS)`` is bounded by the server-side BLOCK, not the
   socket read timeout (valkey-py defaults ``socket_timeout`` to 5s via
   ``from_url``, which equalled our 5s block and raised
   "Timeout reading from socket").
2. A transient read error (e.g. that very TimeoutError) must be caught and
   retried with bounded backoff, never escalated to a task failure.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from dev_health_ops.api import _stream_consumer as base
from dev_health_ops.api._stream_consumer import (
    StreamConsumer,
    get_consumer_redis_client,
)


class _RejectError(Exception):
    pass


class _Consumer(StreamConsumer):
    consumer_group = "test-consumers"
    dlq_stream = "test:dlq"
    reject_exceptions = (_RejectError,)

    def stream_patterns(self) -> list[str]:
        return ["test:*:events"]

    def process_entry(self, stream_key, entry_id, data) -> int:
        if data.get("reject"):
            raise _RejectError("poison")
        if data.get("boom"):
            raise RuntimeError("unexpected")
        return 1


class FakeRedis:
    def __init__(
        self,
        entries: list[tuple[str, dict[str, str]]] | None = None,
        *,
        xreadgroup_error: Exception | None = None,
    ) -> None:
        self._entries = entries or []
        self._xreadgroup_error = xreadgroup_error
        self.xreadgroup_calls = 0
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []
        self.dlq: list[tuple[str, dict[str, str]]] = []

    def scan_iter(self, match: str, _type: str | None = None):
        return ["test:org:events"]

    def xgroup_create(self, stream_key, group, id="0", mkstream=True) -> None:
        return None

    def xreadgroup(self, group, consumer, streams, count, block):
        self.xreadgroup_calls += 1
        if self._xreadgroup_error is not None:
            raise self._xreadgroup_error
        entries = self._entries
        self._entries = []
        if not entries:
            return []
        return [("test:org:events", entries)]

    def xack(self, stream_key, group, *entry_ids) -> None:
        self.acked.append((stream_key, group, entry_ids))

    def xadd(self, stream_key, data) -> None:
        self.dlq.append((stream_key, data))


# ---------------------------------------------------------------------------
# Blocking-safe client factory (the root-cause fix)
# ---------------------------------------------------------------------------


def test_consumer_client_uses_unbounded_socket_timeout(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://valkey:6379/1")
    with patch("valkey.from_url", return_value=MagicMock()) as from_url:
        get_consumer_redis_client()

    assert from_url.called
    _args, kwargs = from_url.call_args
    # The whole point: a blocking XREADGROUP must not be killed by the socket
    # read timeout. socket_timeout MUST be None for blocking reads.
    assert kwargs["socket_timeout"] is None
    assert kwargs["socket_connect_timeout"] == base.DEFAULT_CONNECT_TIMEOUT_S
    assert kwargs["decode_responses"] is True


def test_consumer_client_none_when_redis_url_unset(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    assert get_consumer_redis_client() is None


# ---------------------------------------------------------------------------
# Resilient loop
# ---------------------------------------------------------------------------


def test_socket_timeout_does_not_crash_loop(monkeypatch):
    """The original bug: a blocking-read TimeoutError crashed the task.

    Now it must be caught and retried with bounded backoff.
    """
    import valkey

    broken = FakeRedis(
        xreadgroup_error=valkey.exceptions.TimeoutError("Timeout reading from socket")
    )
    sleeps: list[float] = []
    monkeypatch.setattr(base.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: broken)

    # Must NOT raise — returns cleanly after the bounded iterations.
    processed = _Consumer().consume(max_iterations=4)

    assert processed == 0
    assert broken.xreadgroup_calls == 4
    assert len(sleeps) == 4
    assert sleeps[0] == 1
    assert sleeps[-1] <= base.DEFAULT_BACKOFF_MAX_S
    for a, b in zip(sleeps, sleeps[1:]):
        assert b >= a
    assert any(b > a for a, b in zip(sleeps, sleeps[1:]))


def test_backoff_resets_after_success(monkeypatch):
    class FlakyRedis(FakeRedis):
        def xreadgroup(self, group, consumer, streams, count, block):
            self.xreadgroup_calls += 1
            if self.xreadgroup_calls in (1, 3):
                raise RuntimeError("boom")
            return []

    flaky = FlakyRedis()
    sleeps: list[float] = []
    monkeypatch.setattr(base.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: flaky)

    _Consumer().consume(max_iterations=4)

    # Failures at iter 1 and 3; backoff resets to 1 after the success at iter 2.
    assert sleeps == [1.0, 1.0]


def test_returns_zero_when_redis_unavailable(monkeypatch):
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: None)
    assert _Consumer().consume(max_iterations=1) == 0


# ---------------------------------------------------------------------------
# Default per-entry handler: process / DLQ / ack
# ---------------------------------------------------------------------------


def test_happy_path_processes_and_acks(monkeypatch):
    redis = FakeRedis([("1-0", {"ok": "1"})])
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: redis)

    processed = _Consumer().consume(max_iterations=1)

    assert processed == 1
    assert redis.acked == [("test:org:events", "test-consumers", ("1-0",))]
    assert redis.dlq == []


def test_reject_exception_routes_to_dlq_and_acks(monkeypatch):
    redis = FakeRedis([("1-0", {"reject": "1"})])
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: redis)

    processed = _Consumer().consume(max_iterations=1)

    assert processed == 0
    assert redis.acked == [("test:org:events", "test-consumers", ("1-0",))]
    assert redis.dlq
    dlq_stream, dlq_data = redis.dlq[0]
    assert dlq_stream == "test:dlq"
    assert dlq_data["original_stream"] == "test:org:events"
    assert dlq_data["entry_id"] == "1-0"


def test_unexpected_exception_routes_to_dlq_and_acks(monkeypatch):
    redis = FakeRedis([("1-0", {"boom": "1"})])
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: redis)

    processed = _Consumer().consume(max_iterations=1)

    assert processed == 0
    assert redis.acked == [("test:org:events", "test-consumers", ("1-0",))]
    assert redis.dlq and redis.dlq[0][0] == "test:dlq"


def test_no_dlq_stream_skips_dlq(monkeypatch):
    class NoDlqConsumer(_Consumer):
        dlq_stream = ""

    redis = FakeRedis([("1-0", {"boom": "1"})])
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: redis)

    processed = NoDlqConsumer().consume(max_iterations=1)

    assert processed == 0
    assert redis.dlq == []
    assert redis.acked == [("test:org:events", "test-consumers", ("1-0",))]


def test_mixed_batch_counts_only_successes(monkeypatch):
    redis = FakeRedis(
        [("1-0", {"ok": "1"}), ("2-0", {"reject": "1"}), ("3-0", {"ok": "1"})]
    )
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: redis)

    processed = _Consumer().consume(max_iterations=1)

    assert processed == 2
    # All three entries ACKed in one batch call.
    assert redis.acked == [("test:org:events", "test-consumers", ("1-0", "2-0", "3-0"))]
    assert len(redis.dlq) == 1


def test_no_streams_returns_zero(monkeypatch):
    class WildcardEmpty(_Consumer):
        def discover_streams(self, rc: Any) -> dict[str, str]:
            return {}

    redis = FakeRedis()
    monkeypatch.setattr(base, "get_consumer_redis_client", lambda: redis)

    assert WildcardEmpty().consume(max_iterations=1) == 0
    assert redis.xreadgroup_calls == 0
