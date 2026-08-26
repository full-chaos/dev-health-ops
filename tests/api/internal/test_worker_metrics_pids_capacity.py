"""CHAOS-4317: pids/thread capacity bound on the metrics compatibility bridge
runner subprocess.

Root cause the fix closes: on 2026-08-26, `pthread_create failed: Resource
temporarily unavailable` fired inside the api container at the top-of-hour
burst -- with `_RUNNER_CONCURRENCY_SEMAPHORE` already at its default
concurrency of 1. A count-only bound cannot protect a pids/thread BUDGET it
never reads. These tests pin the capacity-derived reader, the per-child cost
measurement, and the durable (queue, never drop) wait gate that replaces it.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

import pytest

from dev_health_ops.api.internal import worker_metrics


def _daily_execution() -> worker_metrics._Execution:
    scope = {"target_day": "2026-08-26", "repo_ids": []}
    digest = worker_metrics._scope_digest(scope)
    run_id = uuid.UUID("33333333-3333-4333-8333-333333333333")
    partition_id = uuid.UUID("44444444-4444-4444-8444-444444444444")
    return worker_metrics._Execution(
        id=worker_metrics._execution_id(
            worker_kind="daily",
            operation="partition",
            run_id=run_id,
            partition_id=partition_id,
            family="daily",
            generation="daily-v1",
            scope_digest=digest,
        ),
        worker_kind="daily",
        operation="partition",
        run_id=run_id,
        partition_id=partition_id,
        organization_id="66666666-6666-4666-8666-666666666666",
        family="daily",
        generation="daily-v1",
        claim_token=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        scope=scope,
        scope_digest=digest,
    )


def _runner_command(source: str, *arguments: str) -> tuple[str, ...]:
    return (sys.executable, "-c", source, *arguments)


_FAST_SUCCESS_RUNNER_SOURCE = (
    "import json, sys; json.load(sys.stdin); "
    "print(json.dumps({'outcome': {'family': 'daily', 'rows': 0}}))"
)


def _isolate_pids_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    pids_max: int,
    pids_current: int,
) -> Path:
    """Point every pids/thread ceiling source at a fake, writable cgroup
    tree except pids_current, which the caller mutates to simulate live
    consumption. Neutralizes RLIMIT_NPROC and host threads-max so the test's
    ceiling is deterministic (the real test process's rlimits/host state
    would otherwise leak in and make the ceiling non-reproducible), and
    resets the module-level reservation state so no earlier test in this
    process's shared module globals leaks a stale reservation in."""
    pids_max_file = tmp_path / "pids.max"
    pids_current_file = tmp_path / "pids.current"
    pids_max_file.write_text(str(pids_max))
    pids_current_file.write_text(str(pids_current))
    monkeypatch.setattr(worker_metrics, "_PIDS_MAX_PATHS", (str(pids_max_file),))
    monkeypatch.setattr(
        worker_metrics, "_PIDS_CURRENT_PATHS", (str(pids_current_file),)
    )
    monkeypatch.setattr(worker_metrics, "_read_rlimit_nproc", lambda: None)
    monkeypatch.setattr(
        worker_metrics, "_HOST_THREADS_MAX_PATH", str(tmp_path / "nope")
    )
    monkeypatch.setattr(worker_metrics, "_PIDS_RESERVED_HOLDER", [0])
    # Default the safety multiplier to 1.0 (no hedge) so tests can reason
    # about clean arithmetic; the dedicated multiplier test below overrides
    # this explicitly to exercise the real default.
    monkeypatch.setenv(worker_metrics._PIDS_PER_CHILD_SAFETY_MULTIPLIER_ENV, "1.0")
    return pids_current_file


# ---------------------------------------------------------------------------
# Ceiling readers
# ---------------------------------------------------------------------------


def test_read_int_cgroup_file_treats_literal_max_as_unbounded(tmp_path: Path) -> None:
    path = tmp_path / "pids.max"
    path.write_text("max\n")
    assert worker_metrics._read_int_cgroup_file(str(path)) is None


def test_read_int_cgroup_file_reads_a_finite_value(tmp_path: Path) -> None:
    path = tmp_path / "pids.max"
    path.write_text("2048\n")
    assert worker_metrics._read_int_cgroup_file(str(path)) == 2048


def test_read_int_cgroup_file_missing_file_is_none(tmp_path: Path) -> None:
    assert worker_metrics._read_int_cgroup_file(str(tmp_path / "absent")) is None


def test_read_int_cgroup_file_garbage_is_none(tmp_path: Path) -> None:
    path = tmp_path / "pids.max"
    path.write_text("not-a-number\n")
    assert worker_metrics._read_int_cgroup_file(str(path)) is None


def test_effective_pids_ceiling_takes_the_minimum_finite_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_pids_sources(monkeypatch, tmp_path, pids_max=500, pids_current=0)
    assert worker_metrics._effective_pids_ceiling() == 500


def test_effective_pids_ceiling_falls_back_when_nothing_is_readable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # No compose file anywhere sets pids_limit today (CHAOS-4317 design doc)
    # -- this is the realistic "unbounded everywhere" shape: cgroup pids.max
    # reads "max" (never present here), RLIMIT_NPROC is infinite, and the
    # host threads-max path does not exist either.
    monkeypatch.setattr(worker_metrics, "_PIDS_MAX_PATHS", (str(tmp_path / "absent1"),))
    monkeypatch.setattr(
        worker_metrics, "_PIDS_CURRENT_PATHS", (str(tmp_path / "absent2"),)
    )
    monkeypatch.setattr(worker_metrics, "_read_rlimit_nproc", lambda: None)
    monkeypatch.setattr(
        worker_metrics, "_HOST_THREADS_MAX_PATH", str(tmp_path / "absent3")
    )
    monkeypatch.delenv(worker_metrics._PIDS_FALLBACK_CEILING_ENV, raising=False)

    assert (
        worker_metrics._effective_pids_ceiling()
        == worker_metrics._DEFAULT_PIDS_FALLBACK_CEILING
    )


# ---------------------------------------------------------------------------
# Per-child cost: seeded, then converges to the observed peak (never guessed)
# ---------------------------------------------------------------------------


def test_per_child_pids_cost_seeds_conservative_then_converges_upward(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker_metrics,
        "_PIDS_PER_CHILD_COST_HOLDER",
        [worker_metrics._DEFAULT_PIDS_PER_CHILD_COST],
    )
    assert (
        worker_metrics._observed_per_child_pids_cost()
        == worker_metrics._DEFAULT_PIDS_PER_CHILD_COST
    )
    worker_metrics._record_per_child_pids_cost(64)
    assert worker_metrics._observed_per_child_pids_cost() == 64
    # A smaller sample than the current watermark never regresses it -- one
    # unusually cheap child must not make the gate under-reserve for the
    # next, typically-sized one.
    worker_metrics._record_per_child_pids_cost(10)
    assert worker_metrics._observed_per_child_pids_cost() == 64


# ---------------------------------------------------------------------------
# The capacity gate itself
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_pids_capacity_proceeds_immediately_when_headroom_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_pids_sources(monkeypatch, tmp_path, pids_max=1000, pids_current=10)
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, "0.0")

    waited, reserved = await worker_metrics._reserve_pids_capacity()
    assert waited == 0.0
    assert reserved == 10
    await worker_metrics._release_pids_capacity_reservation(reserved)


@pytest.mark.asyncio
async def test_reserve_pids_capacity_queues_then_proceeds_once_headroom_frees(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """CHAOS-4317 falsifier: before this ticket, nothing gated spawn on
    pids headroom at all -- a version that does not actually wait would
    return 0.0 here even though the fake container starts fully out of
    budget. Bounded polling, not a single fixed sleep: pids.current is
    mutated by a background task partway through, and the gate must notice
    on its next poll (poll interval is tiny so the test stays fast)."""
    pids_current_file = _isolate_pids_sources(
        monkeypatch, tmp_path, pids_max=100, pids_current=95
    )
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, "0.0")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_POLL_SECONDS_ENV, "0.02")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_UNIT_SECONDS_ENV, "5.0")

    async def free_up_headroom_shortly() -> None:
        await asyncio.sleep(0.05)
        pids_current_file.write_text("50")

    freer = asyncio.create_task(free_up_headroom_shortly())
    waited, reserved = await worker_metrics._reserve_pids_capacity()
    await freer

    assert waited > 0.0  # it actually queued, not a pass-through
    await worker_metrics._release_pids_capacity_reservation(reserved)


@pytest.mark.asyncio
async def test_reserve_pids_capacity_exhausted_wait_raises_retryable_capacity_exhausted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _isolate_pids_sources(monkeypatch, tmp_path, pids_max=100, pids_current=95)
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, "0.0")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_POLL_SECONDS_ENV, "0.01")
    # Deficit never resolves (pids_current never changes) -- a tiny wait
    # unit keeps this test fast while still exercising the real bounded
    # loop (several real polls, not a mocked single check).
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_UNIT_SECONDS_ENV, "0.03")

    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._reserve_pids_capacity()
    assert excinfo.value.reason == "capacity_exhausted"
    # Always retryable -- capacity pressure is transient container state,
    # never a reason to park a partition for human review.
    assert excinfo.value.safe_to_retry is True


@pytest.mark.asyncio
async def test_reserve_pids_capacity_is_atomic_across_concurrent_callers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """CHAOS-4317 codex review (PR #1931 round 1, P1): a plain check-then-
    spawn is not atomic -- two callers racing the same pids.current snapshot
    could both observe headroom and both reserve/spawn, together exceeding
    the ceiling. Falsifier: with the reservation lock removed (or the
    reservation not actually accounted for in the second caller's `needed`
    calculation), both calls below would return waited=0.0 immediately,
    since neither would see the other's claim on the only 15 units of
    headroom available for one 10-unit child.
    """
    _isolate_pids_sources(monkeypatch, tmp_path, pids_max=100, pids_current=75)
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, "0.0")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_POLL_SECONDS_ENV, "0.01")
    # ceiling=100, current=75 -> 25 units of headroom, enough for exactly
    # ONE 10-unit child (a second would need 20, pushing to 95... actually
    # fits) -- tighten to exactly one-child headroom: current=85 leaves 15,
    # enough for one child (10) but not two (20).
    (tmp_path / "pids.current").write_text("85")

    first_reserved: list[int] = []
    second_call_saw_first_reservation = []

    async def first_call() -> None:
        waited, reserved = await worker_metrics._reserve_pids_capacity()
        assert waited == 0.0
        first_reserved.append(reserved)

    async def second_call() -> None:
        # Give the first call's lock-held critical section a chance to run
        # and commit its reservation before this one starts checking.
        await asyncio.sleep(0.005)
        # A short wait unit + tiny wait ceiling: this call must EITHER wait
        # (because it correctly sees the first reservation and there isn't
        # room for a second 10-unit child in 15 units of headroom) or raise
        # capacity_exhausted -- either proves it did NOT admit past budget.
        monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_UNIT_SECONDS_ENV, "0.02")
        try:
            waited, _reserved = await worker_metrics._reserve_pids_capacity()
            second_call_saw_first_reservation.append(waited > 0.0)
        except worker_metrics._CompatibilityProcessFailure as exc:
            assert exc.reason == "capacity_exhausted"
            second_call_saw_first_reservation.append(True)

    await asyncio.gather(first_call(), second_call())

    assert first_reserved == [10]
    assert second_call_saw_first_reservation == [True], (
        "the second concurrent caller admitted immediately despite the "
        "first caller's reservation leaving no headroom -- the "
        "check-then-reserve step is not atomic"
    )
    await worker_metrics._release_pids_capacity_reservation(first_reserved[0])


def test_reserved_per_child_pids_cost_applies_the_default_safety_multiplier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4317 codex review (PR #1931 round 1, P1): the background
    sampler can miss a short-lived child's true peak entirely (a spike that
    both starts and recedes faster than one poll interval reports a 0
    delta). The admission math must not trust the raw observed watermark
    1:1 -- it applies a safety multiplier on top. The recorded watermark
    itself (_observed_per_child_pids_cost) stays the true measured value,
    unmultiplied, for telemetry accuracy; only the reservation math hedges.
    """
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.delenv(
        worker_metrics._PIDS_PER_CHILD_SAFETY_MULTIPLIER_ENV, raising=False
    )
    assert worker_metrics._observed_per_child_pids_cost() == 10
    assert worker_metrics._reserved_per_child_pids_cost() == 20  # default 2.0x
    assert (
        worker_metrics._observed_per_child_pids_cost() == 10
    )  # watermark itself unaffected


# ---------------------------------------------------------------------------
# Wired into the real spawn path (not just the isolated function)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_compatibility_process_propagates_capacity_exhausted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Proves the gate is actually called from _run_compatibility_process's
    real spawn path, not just reachable in isolation. Falsifier: removing
    the `await _reserve_pids_capacity()` call before delegating to
    _run_compatibility_process_locked makes this test hang until the runner
    subprocess exits normally instead of raising -- caught by asserting the
    exception, not merely "no exception"."""
    _isolate_pids_sources(monkeypatch, tmp_path, pids_max=100, pids_current=95)
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, "0.0")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_POLL_SECONDS_ENV, "0.01")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_UNIT_SECONDS_ENV, "0.03")
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(_FAST_SUCCESS_RUNNER_SOURCE),
    )

    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "capacity_exhausted"
    assert excinfo.value.safe_to_retry is True


@pytest.mark.asyncio
async def test_run_compatibility_process_succeeds_once_headroom_exists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Control for the test above: with real headroom, the same wiring
    lets the partition through and it completes normally -- the gate adds
    latency under pressure, never a permanent block."""
    _isolate_pids_sources(monkeypatch, tmp_path, pids_max=1000, pids_current=10)
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, "0.0")
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(_FAST_SUCCESS_RUNNER_SOURCE),
    )

    result = await worker_metrics._run_compatibility_process(_daily_execution())
    assert result == {"family": "daily", "rows": 0}


# ---------------------------------------------------------------------------
# Red-first: N partitions arriving over a fixed pids budget must all
# eventually proceed (queue), and the fake ceiling must never be exceeded.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partitions_over_budget_queue_durably_without_ever_exceeding_the_ceiling(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """CHAOS-4317 red-first proof (team-lead's exact ask): N concurrent
    partition arrivals whose combined pids cost exceeds the container's
    pids ceiling must queue rather than spawn over budget or be dropped --
    every one eventually proceeds, and the ceiling is never exceeded at any
    point.

    Falsifier: before this ticket there was no pids-aware gate at all, so
    every one of these 9 "partitions" would have proceeded immediately
    regardless of the fake ceiling -- peak_seen would exceed pids_max, which
    is exactly what this test's core assertion catches. With the fix,
    ceiling=100, ambient baseline=40, per_child=10, margin=0 leaves
    headroom for at most 6 concurrent "children" (40 + 6*10 = 100) before a
    7th arrival must wait its turn.
    """
    pids_current_file = _isolate_pids_sources(
        monkeypatch, tmp_path, pids_max=100, pids_current=40
    )
    monkeypatch.setattr(worker_metrics, "_PIDS_PER_CHILD_COST_HOLDER", [10])
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, "0.0")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_POLL_SECONDS_ENV, "0.01")
    monkeypatch.setenv(worker_metrics._PIDS_CAPACITY_WAIT_UNIT_SECONDS_ENV, "2.0")

    file_lock = asyncio.Lock()
    peak_seen = [0]
    completed: list[tuple[int, float]] = []

    async def simulate_partition(index: int) -> None:
        waited, reserved = await worker_metrics._reserve_pids_capacity()
        async with file_lock:
            current = int(pids_current_file.read_text()) + 10
            pids_current_file.write_text(str(current))
            peak_seen[0] = max(peak_seen[0], current)
        # The fake pids.current file now reflects this "child" directly, so
        # the transient reservation (which only needs to cover the gap
        # between "decided" and "the live signal reflects it") is released
        # immediately -- holding both would double-count the same child.
        await worker_metrics._release_pids_capacity_reservation(reserved)
        await asyncio.sleep(0.03)  # simulated child lifetime
        async with file_lock:
            current = int(pids_current_file.read_text()) - 10
            pids_current_file.write_text(str(current))
        completed.append((index, waited))

    await asyncio.gather(*(simulate_partition(index) for index in range(9)))

    assert len(completed) == 9, "every partition must eventually run -- none dropped"
    assert peak_seen[0] <= 100, "the fake pids ceiling was exceeded"
    assert any(waited > 0.0 for _, waited in completed), (
        "at least one of the 9 partitions must have queued -- otherwise "
        "this test never actually exercised the over-budget path"
    )
