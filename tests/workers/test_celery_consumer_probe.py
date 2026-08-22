"""The Celery consumer detector itself (CHAOS-3941).

``dispatch_sync_run`` refuses to publish only when this probe reports ABSENT,
so the three states have to mean exactly what the dispatcher assumes they mean.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from dev_health_ops.workers import celery_consumers
from dev_health_ops.workers.celery_consumers import (
    CeleryConsumerPresence,
    probe_celery_consumers,
    reset_celery_consumer_probe_cache,
)


class _FakeConnection:
    def __init__(self, *, connect_error: Exception | None = None) -> None:
        self._connect_error = connect_error
        self.ensure_calls: list[dict] = []

    def ensure_connection(self, **kwargs):
        self.ensure_calls.append(kwargs)
        if self._connect_error is not None:
            raise self._connect_error


class _FakeInspect:
    def __init__(self, replies) -> None:
        self._replies = replies

    def active_queues(self):
        return self._replies

    def ping(self):  # pragma: no cover - present only to catch a regression
        raise AssertionError(
            "the probe must ask active_queues, not ping: a ping is answered by "
            "every worker that is up, including workers on unrelated queues"
        )


def _fake_celery_app(monkeypatch, *, replies, connect_error=None):
    """Replace the real broker round-trip; count the probes that happened."""

    from dev_health_ops.workers import celery_app as celery_app_module

    connection = _FakeConnection(connect_error=connect_error)
    probes: list[str] = []

    @contextmanager
    def _connection_for_read():
        yield connection

    class _Control:
        @staticmethod
        def inspect(**_kwargs):
            probes.append("inspect")
            return _FakeInspect(replies)

    class _App:
        control = _Control()

        def connection_for_read(self):
            return _connection_for_read()

    monkeypatch.setattr(celery_app_module, "celery_app", _App())
    return probes


def _worker_on(*queues: str) -> dict:
    return {f"celery@{q}": [{"name": q}] for q in queues}


@pytest.fixture(autouse=True)
def _live_probe(monkeypatch):
    """Undo the suite-wide probe stub; these tests exercise the real function."""

    monkeypatch.setattr(
        celery_consumers,
        "probe_celery_consumers",
        celery_consumers.probe_celery_consumers,
    )
    reset_celery_consumer_probe_cache()
    yield
    reset_celery_consumer_probe_cache()


def test_a_worker_on_the_queue_reports_present(monkeypatch) -> None:
    _fake_celery_app(monkeypatch, replies=_worker_on("sync"))

    assert probe_celery_consumers(["sync"]) is CeleryConsumerPresence.PRESENT


def test_a_worker_on_an_unrelated_queue_is_not_a_consumer(monkeypatch) -> None:
    """Review finding: "is any worker up" is not "is my queue consumed".

    The cutover leaves Celery workers running for reports/post-sync long after
    the provider-unit queue has no consumer. A worker-level check answers
    PRESENT there and puts the units straight back into the void this change
    exists to close.
    """

    _fake_celery_app(monkeypatch, replies=_worker_on("reports", "post_sync"))

    assert probe_celery_consumers(["sync"]) is CeleryConsumerPresence.ABSENT


def test_any_one_of_several_queues_is_enough(monkeypatch) -> None:
    _fake_celery_app(monkeypatch, replies=_worker_on("sync.github.heavy"))

    presence = probe_celery_consumers(["sync", "sync.github.heavy"])

    assert presence is CeleryConsumerPresence.PRESENT


def test_no_workers_at_all_reports_absent(monkeypatch) -> None:
    """The production failure: broker up, replicas at zero."""

    _fake_celery_app(monkeypatch, replies=None)

    assert probe_celery_consumers(["sync"]) is CeleryConsumerPresence.ABSENT


def test_unreachable_broker_reports_unknown_not_absent(monkeypatch) -> None:
    """A broker outage must not be mistaken for "consumers are gone".

    ABSENT terminalizes units. If a transient connect failure produced ABSENT,
    one broker blip would fail every planned unit of every run.
    """

    _fake_celery_app(
        monkeypatch, replies=None, connect_error=OSError("connection refused")
    )

    assert probe_celery_consumers(["sync"]) is CeleryConsumerPresence.UNKNOWN


def test_a_present_snapshot_is_cached_within_the_ttl(monkeypatch) -> None:
    """A per-minute dispatch cadence must not pay a broadcast per unit."""

    monkeypatch.setenv("WORKER_CELERY_CONSUMER_PROBE_TTL_SECONDS", "30")
    probes = _fake_celery_app(monkeypatch, replies=_worker_on("sync"))

    assert probe_celery_consumers(["sync"], now=100.0) is CeleryConsumerPresence.PRESENT
    assert probe_celery_consumers(["sync"], now=120.0) is CeleryConsumerPresence.PRESENT
    assert len(probes) == 1, "a cached PRESENT must not re-broadcast"

    assert probe_celery_consumers(["sync"], now=131.0) is CeleryConsumerPresence.PRESENT
    assert len(probes) == 2, "the snapshot must expire at the TTL"


def test_absent_is_confirmed_by_a_second_uncached_probe(monkeypatch) -> None:
    """ABSENT terminalizes units, so one ambiguous round-trip must not decide it.

    A pidbox broadcast under load can time out and return nothing while workers
    are consuming normally -- indistinguishable from scaled-to-zero. Requiring
    two independent agreeing round-trips makes that coincidence far weaker, and
    the cost lands only on the path that is about to destroy something.
    """

    probes = _fake_celery_app(monkeypatch, replies=_worker_on("reports"))

    assert probe_celery_consumers(["sync"], now=100.0) is CeleryConsumerPresence.ABSENT
    assert len(probes) == 2, "ABSENT must be confirmed, not taken on one reply"


def test_a_confirming_probe_that_disagrees_withdraws_the_absent_verdict(
    monkeypatch,
) -> None:
    """The transient-timeout case: first round-trip empty, second one healthy."""

    from dev_health_ops.workers import celery_app as celery_app_module

    rounds = [None, _worker_on("sync")]

    class _Inspect:
        def active_queues(self):
            return rounds.pop(0) if rounds else _worker_on("sync")

    class _Conn:
        def ensure_connection(self, **_kwargs):
            return None

    @contextmanager
    def _conn():
        yield _Conn()

    class _App:
        class control:  # noqa: N801 - mirrors celery's attribute shape
            @staticmethod
            def inspect(**_kwargs):
                return _Inspect()

        def connection_for_read(self):
            return _conn()

    monkeypatch.setattr(celery_app_module, "celery_app", _App())

    assert probe_celery_consumers(["sync"], now=100.0) is CeleryConsumerPresence.PRESENT


def test_connect_attempt_is_bounded(monkeypatch) -> None:
    """Celery's default retry policy would stall a dispatch pass for minutes."""

    from dev_health_ops.workers import celery_app as celery_app_module

    _fake_celery_app(monkeypatch, replies=_worker_on("sync"))
    monkeypatch.setenv("WORKER_CELERY_CONSUMER_PROBE_TIMEOUT_SECONDS", "0.25")

    probe_celery_consumers(["sync"])

    with celery_app_module.celery_app.connection_for_read() as connection:
        assert connection.ensure_calls == [{"max_retries": 0, "timeout": 0.25}]


def test_dispatcher_asks_about_the_queue_the_unit_would_land_in(monkeypatch) -> None:
    """The queue set is derived from dispatch_policy, not guessed.

    ``dispatch_policy.route``'s own contract says "no route targets an
    unconsumed queue"; this is the check that can enforce it.
    """

    from dev_health_ops.workers.provider_unit_transport import celery_fallback_queues

    class _Unit:
        org_id = "org"
        provider = "github"
        dataset_key = "commits"
        cost_class = "medium"

    assert celery_fallback_queues([_Unit()], river_owns_units=False) == frozenset(
        {"sync"}
    )
