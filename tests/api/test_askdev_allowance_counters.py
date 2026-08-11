"""Tests for the Valkey-backed Ask Dev platform allowance counters (CHAOS-3522).

Uses fakeredis (FakeAsyncValkey), same pattern as test_impersonation_cache.py:
monkeypatch the module's ``_client`` directly rather than going through
REDIS_URL, both because that is how the shared-store semantics get exercised
deterministically and because tests/_env_isolation.py scrubs REDIS_URL for the
whole unit tier (verified directly: a REDIS_URL set on the invoking shell does
not reach this module inside a pytest run at all).

NO LUA: fakeredis in this environment supports no EVAL (verified directly,
same gap as tests/test_token_pool.py's own ``requires_lua`` marker) -- every
test here exercises the real HINCRBY/HSETNX-based admit logic, not a
Lua-gated path that would silently skip.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

fakeredis = pytest.importorskip("fakeredis")

from dev_health_ops.api.services import (  # noqa: E402
    askdev_allowance_counters as counters,
)
from dev_health_ops.api.services.askdev_allowance_counters import (  # noqa: E402
    AdmitOutcome,
    AllowanceCounts,
    admit,
    force_reconcile,
    read_counts,
    reconcile_terminal_cost,
)

_ORG = "11111111-1111-1111-1111-111111111111"
_NOW = datetime(2026, 8, 15, 12, 0, 0, tzinfo=UTC)
_RESERVATION = 1_000_000


@pytest.fixture
def shared_server():
    return fakeredis.FakeServer()


@pytest.fixture
def valkey_client(shared_server, monkeypatch):
    """Wire the module's client to a fresh fakeredis store and reset breaker state."""
    client = fakeredis.FakeAsyncValkey(server=shared_server)
    monkeypatch.setattr(counters, "_client", client)
    monkeypatch.setattr(counters, "_circuit_open_until", 0.0)
    monkeypatch.setattr(counters, "_needs_recovery_recompute", False)
    return client


@pytest.fixture
def zero_baseline(monkeypatch):
    """No prior dev_runs history -- the common cold-key case."""
    mock = AsyncMock(return_value=AllowanceCounts(requests=0, cost_microusd=0))
    monkeypatch.setattr(counters, "_recompute_from_dev_runs", mock)
    return mock


class _FakeSession:
    """Placeholder session -- never actually touched once SQL is mocked out."""


def _fake_session() -> AsyncSession:
    return cast(AsyncSession, _FakeSession())


# -- admit(): boundary parity with the old SQL query -------------------------


@pytest.mark.asyncio
async def test_admit_grants_when_under_both_limits(valkey_client, zero_baseline):
    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.ADMITTED
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    assert counts == [b"1", str(_RESERVATION).encode()]


@pytest.mark.asyncio
async def test_admit_rejects_request_limit_and_compensates(valkey_client, monkeypatch):
    """At the request cap: rejected, and the attempted increment is reversed."""
    monkeypatch.setattr(
        counters,
        "_recompute_from_dev_runs",
        AsyncMock(return_value=AllowanceCounts(requests=5, cost_microusd=0)),
    )
    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=5,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.REQUEST_LIMIT_EXCEEDED
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    # Compensated back to exactly the seeded baseline -- the rejected
    # attempt left no trace.
    assert counts == [b"5", b"0"]


@pytest.mark.asyncio
async def test_admit_rejects_cost_limit_and_compensates_both_fields(
    valkey_client, monkeypatch
):
    """Cost cap exceeded: BOTH the cost and the request increment are reversed.

    Unlike the SQL version (a pure read), the counter version had already
    speculatively reserved the request slot before checking cost -- an
    admit that is fully rejected must leave zero trace on either counter.
    """
    monkeypatch.setattr(
        counters,
        "_recompute_from_dev_runs",
        AsyncMock(return_value=AllowanceCounts(requests=0, cost_microusd=4_500_000)),
    )
    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.COST_LIMIT_EXCEEDED
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    assert counts == [b"0", b"4500000"]


@pytest.mark.asyncio
async def test_admit_request_limit_checked_before_cost_limit(
    valkey_client, monkeypatch
):
    """When BOTH limits would be exceeded, the request rejection wins.

    Matches the original SQL's check order exactly (request check raises
    before the cost check is even evaluated).
    """
    monkeypatch.setattr(
        counters,
        "_recompute_from_dev_runs",
        AsyncMock(return_value=AllowanceCounts(requests=10, cost_microusd=9_999_999)),
    )
    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.REQUEST_LIMIT_EXCEEDED


@pytest.mark.asyncio
async def test_admit_boundary_exactly_at_request_limit_is_the_last_admitted(
    valkey_client, monkeypatch
):
    """existing == limit - 1 admits (becomes the limit-th); existing == limit rejects."""
    monkeypatch.setattr(
        counters,
        "_recompute_from_dev_runs",
        AsyncMock(return_value=AllowanceCounts(requests=4, cost_microusd=0)),
    )
    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=5,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.ADMITTED


# -- Amendment 1: init-if-absent is atomic, never a blind overwrite ----------


@pytest.mark.asyncio
async def test_concurrent_init_race_second_racer_baseline_discarded(
    valkey_client, monkeypatch
):
    """Plants the exact race amendment 1 names.

    Racer A misses the key, recomputes baseline X, and (in between its own
    recompute and its HSETNX) a real admission increments the counter.
    Racer B then ALSO misses (same cold key, sees requests=0 in its own
    stale recompute), and attempts to initialize with baseline Y. B's
    HSETNX must lose: the key already exists (A won), so B's write must be
    silently discarded, not overwrite A's already-incremented state.
    """
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))

    baseline_a = AllowanceCounts(requests=2, cost_microusd=1_000_000)
    await counters._ensure_initialized(
        valkey_client, key=key, ttl_seconds=1000, baseline=baseline_a
    )
    # A real admission lands in between the two racers.
    await valkey_client.hincrby(key, "requests", 1)
    await valkey_client.hincrby(key, "cost_microusd", _RESERVATION)

    after_admission = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    assert after_admission == [b"3", b"2000000"]

    # Racer B's belated, stale recompute (it never saw the admission above).
    baseline_b_stale = AllowanceCounts(requests=0, cost_microusd=0)
    await counters._ensure_initialized(
        valkey_client, key=key, ttl_seconds=1000, baseline=baseline_b_stale
    )

    final = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    # B's stale baseline must NOT have clobbered A's state.
    assert final == [b"3", b"2000000"]


@pytest.mark.asyncio
async def test_per_field_hsetnx_survives_admission_interleaved_between_the_two_fields(
    valkey_client,
):
    """CHAOS-3522 review round 2: the finer-grained race a single sentinel
    field cannot close.

    A prior version of ``_ensure_initialized`` gated one later ``HSET`` of
    BOTH fields behind a single ``HSETNX(key, "initialized", "1")``
    sentinel. That sentinel write and the baseline ``HSET`` are two separate
    round trips -- a real admission's ``HINCRBY`` landing on ONE field in
    between them would be silently wiped out when the baseline ``HSET``
    (writing both fields unconditionally) finally landed. This test plants
    exactly that interleaving -- an admission's increment lands on
    ``cost_microusd`` in between the initializer's two per-field writes --
    and proves the CURRENT per-field-``HSETNX`` implementation survives it:
    each field is independently atomic, so there is no window where the key
    "exists" without both fields already being live.
    """

    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    baseline = AllowanceCounts(requests=5, cost_microusd=999_999_999)

    class _InterleavingClient:
        """Proxies to the real fakeredis client, but injects a concurrent
        admission's HINCRBY right after the FIRST field's HSETNX lands --
        exercising _ensure_initialized's REAL two-call sequence, not a
        hand-simulated one."""

        def __init__(self, inner: Any) -> None:
            self._inner = inner
            self._hsetnx_calls = 0

        async def hsetnx(self, *args: Any, **kwargs: Any) -> Any:
            result = await self._inner.hsetnx(*args, **kwargs)
            self._hsetnx_calls += 1
            if self._hsetnx_calls == 1:
                await self._inner.hincrby(key, "cost_microusd", _RESERVATION)
            return result

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    await counters._ensure_initialized(
        _InterleavingClient(valkey_client),
        key=key,
        ttl_seconds=1000,
        baseline=baseline,
    )

    final = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    # requests: racer A's baseline (nothing else touched it).
    # cost_microusd: the admission's real increment, NOT racer A's stale
    # baseline -- HSETNX on an already-existing field is a no-op.
    assert final == [b"5", str(_RESERVATION).encode()]


@pytest.mark.asyncio
async def test_ensure_initialized_sets_ttl_only_on_the_winning_write(valkey_client):
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    baseline = AllowanceCounts(requests=0, cost_microusd=0)
    await counters._ensure_initialized(
        valkey_client, key=key, ttl_seconds=500, baseline=baseline
    )
    ttl = await valkey_client.ttl(key)
    assert 0 < ttl <= 500


# -- Amendment 2 belongs to persistence/service.py's own test suite (the
# exactly-once contract is a property of update_run's control flow, not of
# this module) -- see test_persistence_v2.py.


# -- Amendment 3: breaker recovery forces exactly one recompute --------------


@pytest.mark.asyncio
async def test_breaker_recovery_forces_one_recompute_then_resumes_counter_mode(
    valkey_client, monkeypatch
):
    recompute = AsyncMock(
        return_value=AllowanceCounts(requests=7, cost_microusd=3_000_000)
    )
    monkeypatch.setattr(counters, "_recompute_from_dev_runs", recompute)
    monkeypatch.setattr(counters, "_needs_recovery_recompute", True)

    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.ADMITTED
    recompute.assert_awaited_once()
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    # Baseline (7, 3_000_000) plus this admission's own increment.
    assert counts == [b"8", str(3_000_000 + _RESERVATION).encode()]
    assert counters._needs_recovery_recompute is False

    # A SECOND admission for a DIFFERENT org must NOT force another
    # recompute -- the bit is consumed exactly once per trip/recover cycle
    # (the stated cross-org limitation the module docstring names).
    recompute.reset_mock()
    other_org = "22222222-2222-2222-2222-222222222222"
    outcome_2 = await admit(
        _fake_session(),
        org_id=other_org,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome_2 is AdmitOutcome.ADMITTED
    # Cold key for other_org still triggers ITS OWN lazy-heal recompute
    # (unrelated to the recovery bit), so recompute IS called once here --
    # the assertion is that it is not called for the recovery reason twice.
    recompute.assert_awaited_once()


@pytest.mark.asyncio
async def test_trip_circuit_sets_recovery_flag(monkeypatch):
    monkeypatch.setattr(counters, "_needs_recovery_recompute", False)
    counters._trip_circuit(RuntimeError("boom"))
    assert counters._needs_recovery_recompute is True
    assert counters._circuit_is_open()


# -- Amendment 4: key/TTL derive from platform_month_window, not a literal --


def test_key_derives_from_platform_month_window():
    from dev_health_ops.api.dev.org_policy import platform_month_window

    now = datetime(2026, 12, 31, 23, 59, tzinfo=UTC)
    window_start, reset_at = platform_month_window(now)
    key = counters._key(_ORG, window_start)
    assert key == f"askdev:allowance:{_ORG}:2026-12"
    assert reset_at == datetime(2027, 1, 1, tzinfo=UTC)


def test_ttl_extends_past_reset_at_by_the_grace_window():
    now = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
    reset_at = datetime(2026, 9, 1, tzinfo=UTC)
    ttl = counters._ttl_seconds(reset_at, now)
    expected = int((reset_at - now).total_seconds()) + counters._TTL_GRACE_SECONDS
    assert ttl == expected
    assert ttl > int((reset_at - now).total_seconds())


# -- Valkey-down fallback -----------------------------------------------------


@pytest.mark.asyncio
async def test_circuit_open_falls_back_to_sql_only_no_writes(
    valkey_client, monkeypatch
):
    monkeypatch.setattr(
        counters, "_circuit_open_until", __import__("time").monotonic() + 60
    )
    recompute = AsyncMock(return_value=AllowanceCounts(requests=0, cost_microusd=0))
    monkeypatch.setattr(counters, "_recompute_from_dev_runs", recompute)

    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.ADMITTED
    recompute.assert_awaited_once()
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    assert await valkey_client.exists(key) == 0  # no Valkey write attempted


@pytest.mark.asyncio
async def test_unconfigured_valkey_goes_straight_to_sql(monkeypatch, zero_baseline):
    monkeypatch.setattr(counters, "_client", None)
    monkeypatch.delenv("REDIS_URL", raising=False)

    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.ADMITTED
    zero_baseline.assert_awaited_once()


@pytest.mark.asyncio
async def test_valkey_error_trips_circuit_and_falls_back(monkeypatch, zero_baseline):
    class _BrokenClient:
        async def exists(self, *_a):
            raise ConnectionError("valkey down")

    monkeypatch.setattr(counters, "_client", _BrokenClient())
    monkeypatch.setattr(counters, "_circuit_open_until", 0.0)

    outcome = await admit(
        _fake_session(),
        org_id=_ORG,
        request_limit=10,
        cost_limit_microusd=5_000_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert outcome is AdmitOutcome.ADMITTED
    zero_baseline.assert_awaited_once()
    assert counters._circuit_is_open()


# -- reconcile_terminal_cost() ------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_terminal_cost_adjusts_reservation_to_actual(valkey_client):
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=1, cost_microusd=_RESERVATION),
    )

    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source="platform",
        state="completed",
        estimated_cost_microusd=250_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )

    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    # Reservation (1_000_000) replaced by the real cost (250_000): net delta
    # -750_000 applied via HINCRBY, requests untouched.
    assert counts == [b"1", b"250000"]


@pytest.mark.asyncio
async def test_reconcile_terminal_cost_can_adjust_upward(valkey_client):
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=1, cost_microusd=_RESERVATION),
    )
    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source="platform",
        state="completed",
        estimated_cost_microusd=_RESERVATION + 500_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    assert counts == [b"1", str(_RESERVATION + 500_000).encode()]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_source", "state", "cost"),
    [
        ("byo", "completed", 250_000),  # not the platform provider
        ("platform", "running", 250_000),  # not terminal
    ],
)
async def test_reconcile_terminal_cost_noop_cases(
    valkey_client, provider_source, state, cost
):
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=1, cost_microusd=_RESERVATION),
    )
    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source=provider_source,
        state=state,
        estimated_cost_microusd=cost,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    # Unchanged -- the reservation is left exactly as it was.
    assert counts == [b"1", str(_RESERVATION).encode()]


# -- reconcile_terminal_cost(): CHAOS-3573 NULL-cost release ------------------
#
# RED-first pin: a terminal run whose real cost was never recorded (NULL)
# must have its worst-case admission-time reservation released down to 0,
# not held forever. Before the CHAOS-3573 fix, reconcile_terminal_cost
# returned early on `estimated_cost_microusd is None` and these three
# assertions failed (counts stayed at the full `_RESERVATION` instead of
# dropping to 0) -- pinning exactly the retention CHAOS-3573's evidence
# measured in production (6 of 29 runs, 2 failed + 4 insufficient_evidence,
# each permanently holding the $5 worst-case reservation).


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["failed", "insufficient_evidence"])
async def test_reconcile_terminal_cost_releases_reservation_on_null_cost(
    valkey_client, state
):
    """A terminal run with NULL cost never dispatched a provider call

    (ProviderBudget.require() in orchestrator.py is the only writer of a
    non-None estimated_cost_microusd, and it never resets back to None) --
    so its real cost is 0, and the worst-case reservation held since
    admission must be released in full.
    """
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=1, cost_microusd=_RESERVATION),
    )
    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source="platform",
        state=state,
        estimated_cost_microusd=None,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    assert counts == [b"1", b"0"], (
        "a NULL-cost terminal run must reconcile the reservation down to "
        "its real cost (0), not keep the worst-case reservation forever"
    )


@pytest.mark.asyncio
async def test_reconcile_terminal_cost_null_cost_only_releases_that_runs_share(
    valkey_client,
):
    """Two runs share the org/month key; only the NULL-cost run's own
    reservation portion is released -- the other run's real cost stands."""
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=2, cost_microusd=_RESERVATION * 2),
    )
    # First run already reconciled up/down to a real, non-null cost.
    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source="platform",
        state="completed",
        estimated_cost_microusd=300_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    # Second run terminates with no recorded cost at all.
    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source="platform",
        state="failed",
        estimated_cost_microusd=None,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    # Total charged: run 1's real 300_000 + run 2's real 0.
    assert counts == [b"2", b"300000"]


@pytest.mark.asyncio
async def test_reconcile_terminal_cost_regression_recorded_cost_still_reconciles(
    valkey_client,
):
    """Regression guard: a terminal run WITH a recorded cost must keep
    reconciling to that recorded value -- the NULL-cost fix must not touch
    this path at all."""
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=1, cost_microusd=_RESERVATION),
    )
    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source="platform",
        state="insufficient_evidence",
        estimated_cost_microusd=42_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    assert counts == [b"1", b"42000"]


@pytest.mark.asyncio
async def test_reconcile_terminal_cost_skips_silently_when_key_missing(valkey_client):
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    assert await valkey_client.exists(key) == 0

    # Must not raise, and must not fabricate a key.
    await reconcile_terminal_cost(
        _fake_session(),
        org_id=_ORG,
        provider_source="platform",
        state="completed",
        estimated_cost_microusd=250_000,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert await valkey_client.exists(key) == 0


# -- force_reconcile(): the on-demand operator reset -------------------------


@pytest.mark.asyncio
async def test_force_reconcile_overwrites_unconditionally(valkey_client, monkeypatch):
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=99, cost_microusd=99_000_000),
    )
    monkeypatch.setattr(
        counters,
        "_recompute_from_dev_runs",
        AsyncMock(return_value=AllowanceCounts(requests=3, cost_microusd=750_000)),
    )

    result, window_start, reset_at = await force_reconcile(
        _fake_session(),
        org_id=_ORG,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )

    assert result == AllowanceCounts(requests=3, cost_microusd=750_000)
    counts = await valkey_client.hmget(key, ["requests", "cost_microusd"])
    assert counts == [b"3", b"750000"]


@pytest.mark.asyncio
async def test_force_reconcile_returns_accurate_value_even_if_valkey_write_fails(
    monkeypatch, caplog
):
    class _BrokenClient:
        async def hset(self, *_a, **_k):
            raise ConnectionError("valkey down\r\nforged log entry")

    monkeypatch.setattr(counters, "_client", _BrokenClient())
    monkeypatch.setattr(counters, "_circuit_open_until", 0.0)
    monkeypatch.setattr(
        counters,
        "_recompute_from_dev_runs",
        AsyncMock(return_value=AllowanceCounts(requests=3, cost_microusd=750_000)),
    )

    result, _window_start, _reset_at = await force_reconcile(
        _fake_session(),
        org_id=_ORG,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert result == AllowanceCounts(requests=3, cost_microusd=750_000)
    assert caplog.records
    assert all("\r" not in record.getMessage() for record in caplog.records)
    assert all("\n" not in record.getMessage() for record in caplog.records)
    assert "valkey down forged log entry" in caplog.records[-1].getMessage()


# -- read_counts(): the admin usage surface -----------------------------------


@pytest.mark.asyncio
async def test_read_counts_lazy_heals_cold_key(valkey_client, monkeypatch):
    monkeypatch.setattr(
        counters,
        "_recompute_from_dev_runs",
        AsyncMock(return_value=AllowanceCounts(requests=4, cost_microusd=2_000_000)),
    )
    counts, window_start, reset_at = await read_counts(
        _fake_session(),
        org_id=_ORG,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert counts == AllowanceCounts(requests=4, cost_microusd=2_000_000)
    assert window_start == datetime(2026, 8, 1, tzinfo=UTC)
    assert reset_at == datetime(2026, 9, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_read_counts_serves_warm_key_without_recompute(
    valkey_client, monkeypatch
):
    key = counters._key(_ORG, datetime(2026, 8, 1, tzinfo=UTC))
    await counters._ensure_initialized(
        valkey_client,
        key=key,
        ttl_seconds=1000,
        baseline=AllowanceCounts(requests=9, cost_microusd=1_500_000),
    )
    recompute = AsyncMock(side_effect=AssertionError("must not recompute a warm key"))
    monkeypatch.setattr(counters, "_recompute_from_dev_runs", recompute)

    counts, _window_start, _reset_at = await read_counts(
        _fake_session(),
        org_id=_ORG,
        per_run_reservation_microusd=_RESERVATION,
        now=_NOW,
    )
    assert counts == AllowanceCounts(requests=9, cost_microusd=1_500_000)
    recompute.assert_not_awaited()
