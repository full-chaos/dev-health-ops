"""Dormant, authenticated compatibility bridge for Go metric workers.

The wire contract intentionally carries only durable identifiers and a fixed
operation. PostgreSQL supplies every compute argument, and a durable execution
ledger fences retries that arrive after an effect may already have happened.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import json
import logging
import math
import os
import signal
import sys
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from time import monotonic as _monotonic
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.internal.worker_auth import (
    authorize_metric_repair,
    authorize_worker_bridge,
)
from dev_health_ops.db import require_clickhouse_uri
from dev_health_ops.metrics.prometheus import (
    DEV_HEALTH_METRIC_COMPAT_CAPACITY_WAIT_EXHAUSTED_TOTAL,
    DEV_HEALTH_METRIC_COMPAT_EXECUTION_DURATION_SECONDS,
    DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL,
    DEV_HEALTH_METRIC_COMPAT_PIDS_CEILING,
    DEV_HEALTH_METRIC_COMPAT_PIDS_CURRENT,
    DEV_HEALTH_METRIC_COMPAT_PIDS_WAIT_SECONDS,
    DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL,
    DEV_HEALTH_METRIC_COMPAT_RETRY_TOTAL,
    DEV_HEALTH_METRIC_COMPAT_RUNNER_RSS_BYTES,
    DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE,
)
from dev_health_ops.metrics.remaining_scope_contract import (
    MembershipBackfillScope,
    RecommendationsScope,
    parse_scope,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/internal/worker", include_in_schema=False)

_EXECUTION_NAMESPACE = uuid.UUID("e6678cc4-a4e9-55c5-9354-9c6202a1834e")
_MAX_EVIDENCE_BYTES = 4096
_MAX_COMPATIBILITY_PROCESS_BYTES = 1024 * 1024
# CHAOS-4543: bounds how much of the runner subprocess's stderr this process
# captures at all -- generous, since the FULL capture is what `logger.error`
# below logs (container-log/SigNoz visibility, no downstream size
# constraint). Well under _MAX_COMPATIBILITY_PROCESS_BYTES: stderr is
# diagnostic text, not the bounded stdout JSON protocol.
_MAX_COMPATIBILITY_STDERR_BYTES = 8 * 1024
# CHAOS-4543: bounds how much of the CAPTURED stderr gets threaded into
# _CompatibilityProcessFailure's message, which _mark_ambiguous/
# _mark_retry_authorized persist as f"{reason}: {message}"[:1024] into
# metric_compatibility_executions.failure_detail -- a HEAD truncation. Since
# the informative content sits at the END of a tail-captured stderr (see
# _read_bounded_stderr's own docstring), embedding the full
# _MAX_COMPATIBILITY_STDERR_BYTES capture would itself get head-truncated
# away by that 1024-byte ledger cap, silently discarding exactly what this
# ticket exists to preserve (verified against a real repro: an 8 KiB
# embedded message left the useful tail past byte 1024, invisible in the
# ledger). This is deliberately smaller than the ledger's own 1024 bytes to
# leave room for the fixed "{reason}: {message} -- " prefix ahead of it.
_MAX_COMPATIBILITY_STDERR_MESSAGE_BYTES = 700
_PROCESS_TERMINATION_TIMEOUT_SECONDS = 1.0
_DISCONNECT_POLL_SECONDS = 0.1
_COMPATIBILITY_RUNNER_COMMAND = (
    sys.executable,
    "-m",
    "dev_health_ops.api.internal.worker_metrics_runner",
)


def _runner_max_concurrency() -> int:
    raw = os.environ.get("DEV_HEALTH_METRICS_RUNNER_MAX_CONCURRENCY", "").strip()
    if not raw:
        return 1
    try:
        value = int(raw)
    except ValueError:
        return 1
    return value if value > 0 else 1


# CHAOS-4264 (codex R1): a per-runner RLIMIT_AS is not an aggregate memory
# bound -- the api container's cgroup enforcement is on the WHOLE container,
# so N concurrent runner subprocesses can still exhaust it (or starve the API
# process) even when each individually stays under its own rlimit. This
# semaphore is the aggregate control: with the default of 1, at most one
# runner subprocess exists at a time, so "container limit minus API headroom"
# IS the per-runner budget with no multiplication -- exactly the calculation
# codex's review asked for. Raise DEV_HEALTH_METRICS_RUNNER_MAX_CONCURRENCY
# only alongside a correspondingly smaller
# DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES (limit * concurrency must stay
# under container_limit - API_headroom).
_RUNNER_CONCURRENCY_SEMAPHORE = asyncio.Semaphore(_runner_max_concurrency())


# CHAOS-3092 (2026-09-07): the progress-based liveness bound on
# ComputePartition (CHAOS-4316) is deleted from here. It existed only to
# bound a hung daily "partition" bridge subprocess -- runWithLeaseRenewal
# (Go, internal/jobs/metrics/daily/daily.go) renewed the partition's lease
# on a fixed ticker independent of whether this bridge was making real
# progress, and the Go HTTP client deliberately set no Client.Timeout, so
# this process was the only place such a hang could ever be observed and
# bounded. The Go daily worker's Python compatibility bridge (the
# execute_daily_metrics route) is gone outright now -- every daily family is
# native Go -- so there is no daily "partition" subprocess left for this
# watchdog to watch. Deleted along with it: _watch_progress_stall,
# _progress_stall_watchdog_enabled, _progress_stall_window_seconds,
# _progress_hard_ceiling_seconds, the _PROGRESS_STALL_*_ENV names and their
# defaults, _PROGRESS_STALL_WATCHDOG_POLL_SECONDS, _configured_positive_
# float_env (its only callers), _read_cgroup_oom_kill_count and
# _OOM_RSS_FALLBACK_FRACTION (used only by this watchdog's own OOM
# disambiguation), and the liveness_watched/stall_reason_holder/
# last_progress_holder wiring inside _run_compatibility_process_locked.

# CHAOS-4264's own memory-limit env key/default, duplicated (not imported)
# rather than shared with worker_metrics_runner.py: that module already
# imports from this one (_canonical_json, _execution_from_process_payload,
# _run_execution_direct), so importing back would be circular. Kept in sync
# by convention -- both are small, stable, and reviewed together.
_RUNNER_MEMORY_LIMIT_ENV_KEY = "DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES"
_RUNNER_DEFAULT_MEMORY_LIMIT_BYTES = 640 * 1024 * 1024


def _configured_runner_memory_limit_bytes() -> int:
    raw = os.environ.get(_RUNNER_MEMORY_LIMIT_ENV_KEY, "").strip()
    if not raw:
        return _RUNNER_DEFAULT_MEMORY_LIMIT_BYTES
    try:
        value = int(raw)
    except ValueError:
        return _RUNNER_DEFAULT_MEMORY_LIMIT_BYTES
    return value if value > 0 else _RUNNER_DEFAULT_MEMORY_LIMIT_BYTES


# ---------------------------------------------------------------------------
# CHAOS-4317: pids/thread capacity bound on runner subprocess spawn.
#
# _RUNNER_CONCURRENCY_SEMAPHORE (above) bounds only this feature's own
# subprocess COUNT -- it has no idea how many OS threads/pids the container's
# cgroup budget has left, and cannot see other consumers sharing the same
# budget (OTel span-exporter init, sync_run threads, the runner child's own
# internal native-library threads). The 2026-08-26 incident hit
# "pthread_create failed: Resource temporarily unavailable" with the
# semaphore's default concurrency already at 1 -- proof a count-only bound
# cannot protect a budget it never reads.
#
# The ceiling is read fresh at each decision point (cheap file/syscall
# reads, not cached across the process lifetime) from cgroup pids.max ONLY
# -- the one source guaranteed to share pids.current's container/cgroup
# scope (codex review, PR #1931 round 2: an earlier version also mixed in
# RLIMIT_NPROC and host-wide /proc/sys/kernel/threads-max, which are NOT
# container-scoped and could under-report a real host-wide exhaustion).
# `effective_pids_ceiling` falls back to a documented conservative constant
# when pids.max is absent/unbounded ("max") -- this ticket's compose changes
# give every environment a real, finite, container-scoped `pids_limit` so
# this fallback should be the exception, not the rule.
# ---------------------------------------------------------------------------

_PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV = (
    "DEV_HEALTH_METRICS_RUNNER_PIDS_SAFETY_MARGIN_FRACTION"
)
_DEFAULT_PIDS_SAFETY_MARGIN_FRACTION = 0.2  # reserve 20% of the ceiling for
# ambient consumers this feature does not control (OTel, sync_run threads,
# uvicorn/gunicorn worker threads) -- the exact gap the 2026-08-26 incident
# fell into: this feature's own concurrency was already at its floor (1).

_PIDS_CAPACITY_WAIT_POLL_SECONDS_ENV = (
    "DEV_HEALTH_METRICS_RUNNER_PIDS_WAIT_POLL_SECONDS"
)
_DEFAULT_PIDS_CAPACITY_WAIT_POLL_SECONDS = 2.0

_PIDS_CAPACITY_WAIT_UNIT_SECONDS_ENV = (
    "DEV_HEALTH_METRICS_RUNNER_PIDS_WAIT_UNIT_SECONDS"
)
_DEFAULT_PIDS_CAPACITY_WAIT_UNIT_SECONDS = 30.0  # seconds of allowed wait per
# "deficit unit" (one more per-child pids cost's worth of headroom missing)
# -- the wait ceiling scales with how far over budget the container
# currently is, never a flat wall-clock number (standing rule: timeouts
# never fix capacity races). A partition that is barely over budget waits a
# short, bounded time; one arriving into a badly starved container is given
# proportionally longer before it gives up and reports capacity_exhausted.

_PIDS_FALLBACK_CEILING_ENV = "DEV_HEALTH_METRICS_RUNNER_PIDS_FALLBACK_CEILING"
_DEFAULT_PIDS_FALLBACK_CEILING = 4096  # used ONLY when no cgroup/rlimit/proc
# source is readable at all (non-Linux dev/CI) -- never silently "no bound".

_DEFAULT_PIDS_PER_CHILD_COST = 32  # conservative seed for the per-runner-child
# pids/thread cost, used until the first real measurement lands (see
# _record_per_child_pids_cost); converges upward to the observed peak, same
# seed-then-converge shape _RUNNER_DEFAULT_MEMORY_LIMIT_BYTES already uses.

_PIDS_PER_CHILD_COST_HOLDER = [_DEFAULT_PIDS_PER_CHILD_COST]
_PIDS_POLL_SECONDS = 0.25


def _read_int_cgroup_file(path: str) -> int | None:
    """Read one cgroup accounting file, returning None if absent/unbounded.

    A literal "max" (cgroup v2's spelling for "no limit on this axis") reads
    as None, exactly like a missing file -- both mean "this source has
    nothing to contribute to the ceiling", not "the ceiling is zero".
    """
    try:
        with open(path, encoding="ascii") as handle:
            raw = handle.read().strip()
    except (FileNotFoundError, PermissionError, OSError):
        return None
    if raw == "max" or not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


# Module-level, individually monkeypatch-able so tests can point these at a
# fake cgroup file tree (a tmp_path fixture) instead of the real filesystem --
# cgroup v2 path first, v1 fallback path second, in the order each reader
# tries them.
_PIDS_MAX_PATHS = ("/sys/fs/cgroup/pids.max", "/sys/fs/cgroup/pids/pids.max")
_PIDS_CURRENT_PATHS = (
    "/sys/fs/cgroup/pids.current",
    "/sys/fs/cgroup/pids/pids.current",
)


def _read_pids_max() -> int | None:
    for path in _PIDS_MAX_PATHS:
        value = _read_int_cgroup_file(path)
        if value is not None:
            return value
    return None


def _read_pids_current() -> int | None:
    for path in _PIDS_CURRENT_PATHS:
        value = _read_int_cgroup_file(path)
        if value is not None:
            return value
    return None


def _configured_int_env(key: str, default: int) -> int:
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _configured_float_env(key: str, default: float) -> float:
    """Parse a non-negative float env override, falling back to `default`.

    Zero is a valid, meaningful configuration for several callers of this
    helper (e.g. DEV_HEALTH_METRICS_RUNNER_PIDS_SAFETY_MARGIN_FRACTION="0"
    deliberately disables the margin) -- only a negative or unparseable
    value falls back to the default. An earlier version rejected 0 the same
    way as a negative/garbage value, silently overriding an operator's
    explicit "0" back to this function's default (caught by this module's
    own test suite: setting the margin fraction to "0.0" for a test had no
    effect until this was fixed).
    """
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def _effective_pids_ceiling() -> int:
    """The container-scoped pids/thread ceiling, or a documented fallback.

    CHAOS-4317 codex review (PR #1931 round 2, P1): an earlier version also
    considered RLIMIT_NPROC (system-wide per-UID, not container-scoped) and
    /proc/sys/kernel/threads-max (host-wide, not namespaced per-container)
    as candidates, then compared the result against _read_pids_current()
    (container-cgroup-scoped). Mixing scopes like that can under-report:
    the container's own pids.current can sit far below a host-wide ceiling
    while the HOST is actually exhausted, so the gate would keep admitting
    spawns during the exact failure this ticket exists to prevent -- a
    correctness gap, not a conservative-by-accident one. cgroup pids.max is
    the only source guaranteed to share pids.current's scope, so it is the
    only one used here now; everything else falls back to the documented,
    conservative _DEFAULT_PIDS_FALLBACK_CEILING constant (env-tunable via
    _PIDS_FALLBACK_CEILING_ENV) -- the honest signal for "no reliable
    same-scope ceiling is visible here, an operator must set an explicit
    pids_limit for a real one" (which this ticket's compose changes do).
    """
    pids_max = _read_pids_max()
    if pids_max is not None:
        return pids_max
    return _configured_int_env(
        _PIDS_FALLBACK_CEILING_ENV, _DEFAULT_PIDS_FALLBACK_CEILING
    )


def _observed_per_child_pids_cost() -> int:
    return _PIDS_PER_CHILD_COST_HOLDER[0]


def _record_per_child_pids_cost(sample: int) -> None:
    if sample > _PIDS_PER_CHILD_COST_HOLDER[0]:
        _PIDS_PER_CHILD_COST_HOLDER[0] = sample


_PIDS_PER_CHILD_SAFETY_MULTIPLIER_ENV = (
    "DEV_HEALTH_METRICS_RUNNER_PIDS_PER_CHILD_SAFETY_MULTIPLIER"
)
_DEFAULT_PIDS_PER_CHILD_SAFETY_MULTIPLIER = 2.0  # codex review (2026-08-26,
# PR #1931 round 1): the background poller samples every _PIDS_POLL_SECONDS
# and is only reliably able to observe a child whose thread burst outlasts
# its own scheduling latency -- a short-lived runner, or a startup thread
# burst that spikes and recedes faster than one poll interval, can leave
# peak_thread_count_holder at 0 even though the child briefly cost more than
# the seeded/converged watermark. Admission math must not trust the raw
# sampler 1:1; this multiplier is applied ONLY to the reservation/admission
# calculation (_reserved_per_child_pids_cost), never to the recorded
# watermark itself (_observed_per_child_pids_cost stays the true measured
# value, for telemetry/debugging accuracy).


def _configured_per_child_safety_multiplier() -> float:
    """Parse the safety multiplier, rejecting values that would defeat it.

    CHAOS-4317 codex review (PR #1931 round 2, P2): the generic
    _configured_float_env accepts any non-negative finite float, including
    "0" (which would zero out _reserved_per_child_pids_cost entirely,
    silently disabling the hedge this multiplier exists to provide) and
    "inf" (which reaches math.ceil below and raises an unclassified
    OverflowError instead of a controlled fallback). This validator is
    stricter than the generic one: only a finite value strictly greater
    than zero is accepted; anything else -- unset, unparseable, zero,
    negative, or non-finite (inf/nan) -- falls back to the documented
    default.
    """
    raw = os.environ.get(_PIDS_PER_CHILD_SAFETY_MULTIPLIER_ENV, "").strip()
    if not raw:
        return _DEFAULT_PIDS_PER_CHILD_SAFETY_MULTIPLIER
    try:
        value = float(raw)
    except ValueError:
        return _DEFAULT_PIDS_PER_CHILD_SAFETY_MULTIPLIER
    if not math.isfinite(value) or value <= 0:
        return _DEFAULT_PIDS_PER_CHILD_SAFETY_MULTIPLIER
    return value


def _reserved_per_child_pids_cost() -> int:
    multiplier = _configured_per_child_safety_multiplier()
    return math.ceil(_observed_per_child_pids_cost() * multiplier)


# CHAOS-4317 codex review (2026-08-26, PR #1931 round 1): a plain
# check-then-spawn is not atomic across concurrent callers -- with
# DEV_HEALTH_METRICS_RUNNER_MAX_CONCURRENCY above 1 (or simply two callers
# racing the same read), both could observe the same pre-spawn pids.current
# snapshot, both conclude there is headroom, and both spawn, reproducing the
# exact over-budget condition this gate exists to prevent. _PIDS_RESERVATION_
# LOCK makes "read current, decide, and reserve" one atomic step; the
# reservation is held for the reserving child's full lifetime (released by
# the caller once its pids/rss pollers are torn down, mirroring their
# existing cancel-then-await pattern) so a second concurrent caller sees the
# first's reservation even before pids.current itself reflects the new
# child's real thread creation.
_PIDS_RESERVATION_LOCK = asyncio.Lock()
_PIDS_RESERVED_HOLDER = [0]


async def _poll_peak_child_thread_count(
    pid: int,
    process: asyncio.subprocess.Process,
    peak_holder: list[int],
    *,
    interval_seconds: float = _PIDS_POLL_SECONDS,
) -> None:
    """Sample this ONE child's own thread count from /proc/<pid>/status.

    CHAOS-4317 codex review (PR #1931 round 2, P1): an earlier version
    measured the delta in the container-wide cgroup pids.current instead --
    if unrelated activity (OTel, sync_run threads, another concurrent
    runner) grew the container's pids.current while THIS child was alive,
    that growth was misattributed to this child's cost, and because
    _record_per_child_pids_cost only ever ratchets the watermark UP, one
    transient ambient burst could permanently inflate every future
    admission decision long after the burst ended. worker_metrics_runner
    never forks its own subprocesses (grep-confirmed) -- the pthread_create
    failures this ticket exists to prevent are native-library threads
    inside this ONE process -- so /proc/<pid>/status's own "Threads:" field
    is a precise, ambient-immune measurement of exactly this child's cost,
    the same file/pattern _poll_peak_rss_bytes already uses for VmRSS.
    """
    status_path = f"/proc/{pid}/status"
    while True:
        try:
            with open(status_path, encoding="ascii") as handle:
                for line in handle:
                    if line.startswith("Threads:"):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            peak_holder[0] = max(peak_holder[0], int(parts[1]))
                        break
        except (FileNotFoundError, ProcessLookupError, OSError):
            return
        if process.returncode is not None:
            return
        await asyncio.sleep(interval_seconds)


async def _reserve_pids_capacity() -> tuple[float, int]:
    """Block until the container has pids headroom, then atomically claim it.

    Returns (seconds_waited, reserved_amount). seconds_waited is 0.0 if
    capacity was already available (observed into DEV_HEALTH_METRIC_COMPAT_
    PIDS_WAIT_SECONDS either way). reserved_amount is the pids/thread budget
    claimed against _PIDS_RESERVED_HOLDER -- 0 when no live cgroup signal
    was available to gate on at all (nothing was reserved because nothing
    could be checked). The caller MUST release exactly this amount via
    _release_pids_capacity_reservation once the reserving child's lifetime
    has ended (same teardown point as its pids/rss pollers), or the
    reservation leaks and the gate becomes permanently over-conservative.

    Never spawns anything itself, purely a gate checked before asyncio.
    create_subprocess_exec. This IS the durable, no-drop queue CHAOS-4317
    asks for: the Go caller's HTTP request is already kept alive
    independently by River's lease-renewal loop (runWithLeaseRenewal keeps
    workCtx alive as long as the cheap RenewPartition UPDATE keeps
    succeeding, regardless of whether this call is progressing) -- so
    waiting here inside the still-open request drops nothing and needs no
    new disk-backed queue.

    Bounded, not infinite: the wait ceiling scales with how far over budget
    the container currently is (deficit_units * a per-unit second budget),
    not a flat wall-clock number -- re-derived on every poll, so it shrinks
    as headroom frees up and grows if the deficit worsens. Raises
    _CompatibilityProcessFailure(reason="capacity_exhausted",
    safe_to_retry=True) if the bound is exceeded -- always retryable, never
    a silent drop.

    The read-decide-reserve step happens under _PIDS_RESERVATION_LOCK so two
    concurrent callers (DEV_HEALTH_METRICS_RUNNER_MAX_CONCURRENCY above 1)
    cannot both observe the same pre-spawn snapshot and both admit past the
    ceiling (codex review, PR #1931 round 1) -- the lock is released before
    sleeping, so waiters don't serialize on the poll delay itself.
    """
    poll_interval = _configured_float_env(
        _PIDS_CAPACITY_WAIT_POLL_SECONDS_ENV, _DEFAULT_PIDS_CAPACITY_WAIT_POLL_SECONDS
    )
    wait_unit_seconds = _configured_float_env(
        _PIDS_CAPACITY_WAIT_UNIT_SECONDS_ENV, _DEFAULT_PIDS_CAPACITY_WAIT_UNIT_SECONDS
    )
    margin_fraction = _configured_float_env(
        _PIDS_CAPACITY_SAFETY_MARGIN_FRACTION_ENV, _DEFAULT_PIDS_SAFETY_MARGIN_FRACTION
    )
    waited = 0.0
    while True:
        async with _PIDS_RESERVATION_LOCK:
            ceiling = _effective_pids_ceiling()
            current = _read_pids_current()
            if current is None:
                # No live cgroup signal at all (non-Linux/dev) -- nothing to
                # gate on, proceed exactly as before this feature existed.
                return waited, 0
            DEV_HEALTH_METRIC_COMPAT_PIDS_CURRENT.set(current)
            DEV_HEALTH_METRIC_COMPAT_PIDS_CEILING.set(ceiling)
            per_child = _reserved_per_child_pids_cost()
            margin = int(ceiling * margin_fraction)
            reserved_by_others = _PIDS_RESERVED_HOLDER[0]
            needed = current + reserved_by_others + margin + per_child
            if needed <= ceiling:
                _PIDS_RESERVED_HOLDER[0] += per_child
                if waited:
                    DEV_HEALTH_METRIC_COMPAT_PIDS_WAIT_SECONDS.observe(waited)
                return waited, per_child
            deficit_units = max(1, math.ceil((needed - ceiling) / max(per_child, 1)))
            wait_ceiling = deficit_units * wait_unit_seconds
            if waited >= wait_ceiling:
                DEV_HEALTH_METRIC_COMPAT_PIDS_WAIT_SECONDS.observe(waited)
                DEV_HEALTH_METRIC_COMPAT_CAPACITY_WAIT_EXHAUSTED_TOTAL.inc()
                raise _CompatibilityProcessFailure(
                    f"metric compatibility process capacity_exhausted -- "
                    f"waited {waited:.1f}s for pids headroom (ceiling="
                    f"{ceiling}, current={current}, reserved_by_others="
                    f"{reserved_by_others}, margin={margin}, "
                    f"per_child={per_child})",
                    reason="capacity_exhausted",
                    safe_to_retry=True,
                )
        await asyncio.sleep(poll_interval)
        waited += poll_interval


async def _release_pids_capacity_reservation(amount: int) -> None:
    if amount <= 0:
        return
    async with _PIDS_RESERVATION_LOCK:
        _PIDS_RESERVED_HOLDER[0] = max(0, _PIDS_RESERVED_HOLDER[0] - amount)


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RemainingMetricsExecutionRequest(_StrictRequest):
    operation: Literal["partition"]
    run_id: uuid.UUID
    partition_id: uuid.UUID


class MetricExecutionRepairRequest(_StrictRequest):
    expected_state: Literal["executing", "ambiguous"]
    expected_attempt_count: int = Field(ge=1)
    resolution: Literal["retry_safe", "confirm_succeeded"]
    review_evidence: str = Field(min_length=1, max_length=2048)
    output_evidence: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_resolution_evidence(self) -> MetricExecutionRepairRequest:
        if len(self.review_evidence.encode()) > 2048:
            raise ValueError("review_evidence must not exceed 2048 UTF-8 bytes")
        if (self.resolution == "confirm_succeeded") != (
            self.output_evidence is not None
        ):
            raise ValueError("output_evidence is required only when confirming success")
        if self.output_evidence is not None:
            encoded = _canonical_json(self.output_evidence)
            if len(encoded.encode()) > _MAX_EVIDENCE_BYTES:
                raise ValueError("output_evidence exceeds the durable bound")
        return self


_DAILY_REDRIVE_DEFAULT_OPERATIONS: tuple[Literal["partition", "finalize"], ...] = (
    "partition",
)


class DailyMetricsRedriveRequest(_StrictRequest):
    """CHAOS-4304: bulk-unblock ledger rows for a set of runs an operator has
    already scoped for redrive (typically via the Go-side
    daily.RedriveStrandedPartitions, CHAOS-4358). Distinct from
    MetricExecutionRepairRequest: that endpoint repairs ONE execution id an
    operator must already know; this one takes the run ids the operator
    actually has (from the stranding evidence) and finds every ambiguous
    daily/partition row underneath them itself.
    """

    run_ids: list[uuid.UUID] = Field(min_length=1, max_length=200)
    review_evidence: str = Field(min_length=1, max_length=2048)
    # CHAOS-4409 (codex review, round 1, P1): this endpoint is shared by two
    # callers whose review_evidence means DIFFERENT things -- daily-redrive's
    # evidence is about partition output, daily-finalize's is about finalize
    # output. Before this field existed, both callers implicitly repaired
    # BOTH operations for their run_ids: a daily-redrive call (which only
    # ever republishes partition jobs) could silently move an UNRELATED
    # finalize ledger row to retry_authorized without anyone reviewing
    # finalize output specifically, letting some later, unrelated finalize
    # attempt redrive it without the ledger's protection and duplicate
    # already-written finalization output. Defaults to `["partition"]` --
    # daily-redrive's pre-CHAOS-4409 behavior, byte-for-byte -- so every
    # caller must opt in explicitly to repairing finalize rows.
    operations: list[Literal["partition", "finalize"]] = Field(
        default_factory=lambda: list(_DAILY_REDRIVE_DEFAULT_OPERATIONS),
        min_length=1,
        max_length=2,
    )

    @model_validator(mode="after")
    def validate_review_evidence(self) -> DailyMetricsRedriveRequest:
        if len(self.review_evidence.encode()) > 2048:
            raise ValueError("review_evidence must not exceed 2048 UTF-8 bytes")
        if len(set(self.operations)) != len(self.operations):
            raise ValueError("operations must not contain duplicates")
        return self


@dataclass(frozen=True)
class _Execution:
    id: uuid.UUID
    worker_kind: Literal["daily", "remaining"]
    operation: Literal["partition", "finalize"]
    run_id: uuid.UUID
    partition_id: uuid.UUID | None
    organization_id: str
    family: str
    generation: str
    claim_token: uuid.UUID
    scope: dict[str, Any]
    scope_digest: str
    generation_seed: int | None = None


def _execution_process_payload(execution: _Execution) -> dict[str, Any]:
    return {
        "worker_kind": execution.worker_kind,
        "operation": execution.operation,
        "run_id": str(execution.run_id),
        "partition_id": (
            str(execution.partition_id) if execution.partition_id is not None else None
        ),
        "organization_id": execution.organization_id,
        "family": execution.family,
        "generation": execution.generation,
        "claim_token": str(execution.claim_token),
        "scope": execution.scope,
        "generation_seed": execution.generation_seed,
    }


def _execution_from_process_payload(payload: object) -> _Execution:
    expected_fields = {
        "worker_kind",
        "operation",
        "run_id",
        "partition_id",
        "organization_id",
        "family",
        "generation",
        "claim_token",
        "scope",
        "generation_seed",
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise ValueError("metric compatibility process input is invalid")
    worker_kind = payload["worker_kind"]
    operation = payload["operation"]
    if worker_kind not in {"daily", "remaining"} or operation not in {
        "partition",
        "finalize",
    }:
        raise ValueError("metric compatibility process operation is invalid")
    if worker_kind == "remaining" and operation != "partition":
        raise ValueError("remaining metrics only support partition execution")
    for field in ("run_id", "organization_id", "family", "generation", "claim_token"):
        if not isinstance(payload[field], str) or not payload[field]:
            raise ValueError("metric compatibility process identity is invalid")
    if not isinstance(payload["scope"], dict):
        raise ValueError("metric compatibility process scope is invalid")
    seed = payload["generation_seed"]
    if seed is not None and (not isinstance(seed, int) or isinstance(seed, bool)):
        raise ValueError("metric compatibility process seed is invalid")

    run_id = uuid.UUID(payload["run_id"])
    claim_token = uuid.UUID(payload["claim_token"])
    if str(run_id) != payload["run_id"] or str(claim_token) != payload["claim_token"]:
        raise ValueError("metric compatibility process identity is not canonical")
    raw_partition_id = payload["partition_id"]
    partition_id: uuid.UUID | None = None
    if raw_partition_id is not None:
        if not isinstance(raw_partition_id, str):
            raise ValueError("metric compatibility process partition is invalid")
        partition_id = uuid.UUID(raw_partition_id)
        if str(partition_id) != raw_partition_id:
            raise ValueError("metric compatibility process partition is not canonical")
    if (operation == "partition") != (partition_id is not None):
        raise ValueError("metric compatibility process partition identity is invalid")

    scope = payload["scope"]
    if worker_kind == "daily":
        if (
            payload["family"] != "daily"
            or seed is not None
            or set(scope) != {"target_day", "repo_ids"}
            or not isinstance(scope["target_day"], str)
            or not isinstance(scope["repo_ids"], list)
        ):
            raise ValueError("daily metric compatibility scope is invalid")
        row: dict[str, Any] = {
            "run_id": run_id,
            "org_id": payload["organization_id"],
            "target_day": date.fromisoformat(scope["target_day"]),
            "generation": payload["generation"],
            "repo_ids": scope["repo_ids"],
            "claim_token": claim_token,
        }
    else:
        row = {
            "run_id": run_id,
            "org_id": payload["organization_id"],
            "family": payload["family"],
            "generation": payload["generation"],
            "generation_seed": seed,
            "scope": scope,
            "claim_token": claim_token,
        }
    return _execution_from_row(
        worker_kind=worker_kind,
        operation=operation,
        row=row,
        partition_id=partition_id,
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _scope_digest(scope: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(scope).encode()).hexdigest()


def _execution_id(
    *,
    worker_kind: str,
    operation: str,
    run_id: uuid.UUID,
    partition_id: uuid.UUID | None,
    family: str,
    generation: str,
    scope_digest: str,
) -> uuid.UUID:
    identity = _canonical_json(
        [
            "metric-compatibility-execution",
            worker_kind,
            operation,
            str(run_id),
            str(partition_id) if partition_id else "",
            family,
            generation,
            scope_digest,
        ]
    )
    return uuid.uuid5(_EXECUTION_NAMESPACE, identity)


def _execution_from_row(
    *,
    worker_kind: Literal["daily", "remaining"],
    operation: Literal["partition", "finalize"],
    row: Any,
    partition_id: uuid.UUID | None,
) -> _Execution:
    if worker_kind == "daily":
        repo_ids: list[str] = []
        for value in row.get("repo_ids") or []:
            parsed = uuid.UUID(str(value))
            if str(parsed) != str(value):
                raise ValueError("daily scope contains a non-canonical repository ID")
            repo_ids.append(str(parsed))
        scope = {
            "target_day": row["target_day"].isoformat(),
            "repo_ids": repo_ids,
        }
        family = "daily"
        seed = None
    else:
        raw_scope = dict(row["scope"])
        validated = parse_scope(str(row["family"]), raw_scope)
        scope = validated.model_dump(
            mode="json", exclude_none=True, exclude_defaults=True
        )
        family = str(row["family"])
        seed = row["generation_seed"]

    digest = _scope_digest(scope)
    run_id = uuid.UUID(str(row["run_id"]))
    return _Execution(
        id=_execution_id(
            worker_kind=worker_kind,
            operation=operation,
            run_id=run_id,
            partition_id=partition_id,
            family=family,
            generation=str(row["generation"]),
            scope_digest=digest,
        ),
        worker_kind=worker_kind,
        operation=operation,
        run_id=run_id,
        partition_id=partition_id,
        organization_id=str(row["org_id"]),
        family=family,
        generation=str(row["generation"]),
        claim_token=uuid.UUID(str(row["claim_token"])),
        scope=scope,
        scope_digest=digest,
        generation_seed=seed,
    )


async def _load_remaining_execution(
    session: AsyncSession, request: RemainingMetricsExecutionRequest
) -> _Execution:
    result = await session.execute(
        text(
            """
            SELECT r.id AS run_id, r.org_id, r.family, r.generation,
                   r.generation_seed, p.scope, p.claim_token
            FROM remaining_metric_runs AS r
            JOIN remaining_metric_partitions AS p ON p.run_id = r.id
            WHERE r.id = CAST(:run_id AS uuid)
              AND p.id = CAST(:partition_id AS uuid)
              AND r.status = 'running'
              AND r.canceled_at IS NULL
              AND p.status = 'running'
              AND p.lease_expires_at > statement_timestamp()
            FOR UPDATE OF r, p
            """
        ),
        {
            "run_id": str(request.run_id),
            "partition_id": str(request.partition_id),
        },
    )
    row = result.mappings().first()
    if row is None:
        raise HTTPException(
            status_code=409, detail="Remaining metrics lease is absent or expired"
        )
    try:
        return _execution_from_row(
            worker_kind="remaining",
            operation="partition",
            row=row,
            partition_id=request.partition_id,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=409, detail="Remaining metrics durable scope is invalid"
        ) from exc


async def _reserve_execution(
    session: AsyncSession, execution: _Execution
) -> Literal["execute", "skipped"]:
    result = await session.execute(
        text(
            """
            INSERT INTO metric_compatibility_executions (
                id, worker_kind, operation, run_id, partition_id, family,
                generation, scope_digest, claim_token, state
            )
            VALUES (
                CAST(:id AS uuid), :worker_kind, :operation,
                CAST(:run_id AS uuid), CAST(:partition_id AS uuid), :family,
                :generation, :scope_digest, CAST(:claim_token AS uuid), 'executing'
            )
            ON CONFLICT (id) DO NOTHING
            RETURNING id
            """
        ),
        {
            "id": str(execution.id),
            "worker_kind": execution.worker_kind,
            "operation": execution.operation,
            "run_id": str(execution.run_id),
            "partition_id": (
                str(execution.partition_id) if execution.partition_id else None
            ),
            "family": execution.family,
            "generation": execution.generation,
            "scope_digest": execution.scope_digest,
            "claim_token": str(execution.claim_token),
        },
    )
    if result.scalar_one_or_none() is not None:
        await session.commit()
        return "execute"

    existing_result = await session.execute(
        text(
            """
            SELECT worker_kind, operation, run_id, partition_id, family,
                   generation, scope_digest, state, attempt_count, claim_token
            FROM metric_compatibility_executions
            WHERE id = CAST(:id AS uuid)
            FOR UPDATE
            """
        ),
        {"id": str(execution.id)},
    )
    existing = existing_result.mappings().first()
    if existing is None:
        raise HTTPException(status_code=503, detail="Execution ledger unavailable")
    expected = (
        execution.worker_kind,
        execution.operation,
        execution.run_id,
        execution.partition_id,
        execution.family,
        execution.generation,
        execution.scope_digest,
    )
    actual = (
        existing["worker_kind"],
        existing["operation"],
        existing["run_id"],
        existing["partition_id"],
        existing["family"],
        existing["generation"],
        existing["scope_digest"],
    )
    if actual != expected:
        raise HTTPException(status_code=409, detail="Execution identity collision")
    if existing["state"] == "succeeded":
        await session.commit()
        return "skipped"
    if existing["state"] == "retry_authorized":
        retried = await session.execute(
            text(
                """
                UPDATE metric_compatibility_executions
                SET state = 'executing',
                    claim_token = CAST(:claim_token AS uuid),
                    attempt_count = attempt_count + 1,
                    last_attempt_at = statement_timestamp()
                WHERE id = CAST(:id AS uuid)
                  AND state = 'retry_authorized'
                  AND attempt_count = :attempt_count
                RETURNING id
                """
            ),
            {
                "id": str(execution.id),
                "claim_token": str(execution.claim_token),
                "attempt_count": existing["attempt_count"],
            },
        )
        if retried.scalar_one_or_none() is None:
            raise HTTPException(
                status_code=409, detail="Execution repair state changed"
            )
        await session.commit()
        return "execute"
    # CHAOS-4264 (codex R2): an earlier version of this function auto-reaped
    # any ambiguous/executing row back to executing once
    # _original_claim_is_active went false -- but that check only proves no
    # one else currently holds the lease, which is ALWAYS eventually true
    # (every River retry renews the claim_token before calling this endpoint
    # again). It is not evidence that no partial write happened, so it
    # defeated the ambiguous state's entire purpose for exactly the
    # progress-having failures that state exists to protect. Removed: a
    # stuck ambiguous/executing row falls through to the same 409 below as
    # it always did before this ticket, requiring the manual
    # /metric-executions/v1/{id}/repair readback. The only automatic
    # resolution this ticket adds is _mark_retry_authorized in _execute,
    # which has real same-execution evidence (see safe_to_retry above) --
    # not a claim-staleness proxy for it.
    reported_state = str(existing["state"])
    # CHAOS-4361: a row stuck at "executing" -- not "ambiguous" -- means the
    # process that owned it (worker_metrics._execute) died before any of its
    # exception handlers could run: a kernel OOM kill of the whole api
    # process, a container restart, a hard crash. Nothing ever marks that
    # row ambiguous/retry_authorized/succeeded, so it sits at "executing"
    # forever. Go's classifyCompatibilityError treats "executing" as
    # transient (ErrCompatibilityAmbiguousRefused -- "resolves itself once
    # that claim finishes or its lease expires"), which is correct ONLY
    # while the original Go-side claim could still be alive. Once
    # _original_claim_is_active is false, the original claim is provably
    # dead (a fresh claim_token/lease already exists, or the lease itself
    # expired) and this row can never resolve on its own -- every future
    # retry will hit this identical 409 until River exhausts its attempt
    # budget and silently discards the job, leaving the partition 'failed'
    # with no failure_reason (the exact 2026-08-27 incident). Reporting
    # "ambiguous" here does NOT resume or re-execute anything -- it only
    # feeds Go's EXISTING state=="ambiguous" classification
    # (ErrCompatibilityAmbiguousStuck), which durably fails the partition
    # permanently and still requires a human /repair call before this
    # ledger row can move again. That is strictly safer than today's
    # infinite-retry loop, and no less conservative than a genuine
    # 'ambiguous' row about whether partial output exists.
    if reported_state == "executing" and not await _original_claim_is_active(
        session, existing
    ):
        reported_state = "ambiguous"
    await session.commit()
    raise HTTPException(
        status_code=409,
        detail={
            "message": "Execution outcome requires readback",
            "execution_id": str(execution.id),
            "state": reported_state,
            "reason": "ambiguous_refused",
        },
    )


def _repair_id(
    execution_id: uuid.UUID, request: MetricExecutionRepairRequest
) -> uuid.UUID:
    identity = _canonical_json(
        [
            "metric-compatibility-execution-repair",
            str(execution_id),
            request.expected_state,
            request.expected_attempt_count,
            request.resolution,
        ]
    )
    return uuid.uuid5(_EXECUTION_NAMESPACE, identity)


async def _original_claim_is_active(session: AsyncSession, row: Any) -> bool:
    parameters = {
        "run_id": str(row["run_id"]),
        "partition_id": (
            str(row["partition_id"]) if row["partition_id"] is not None else None
        ),
        "claim_token": str(row["claim_token"]),
    }
    if row["worker_kind"] == "remaining":
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM remaining_metric_runs AS r
                JOIN remaining_metric_partitions AS p ON p.run_id = r.id
                WHERE r.id = CAST(:run_id AS uuid)
                  AND p.id = CAST(:partition_id AS uuid)
                  AND r.status = 'running'
                  AND r.canceled_at IS NULL
                  AND p.status = 'running'
                  AND p.claim_token = CAST(:claim_token AS uuid)
                  AND p.lease_expires_at > statement_timestamp()
            )
        """
    elif row["operation"] == "partition":
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM daily_metrics_runs AS r
                JOIN daily_metrics_partitions AS p ON p.run_id = r.id
                WHERE r.id = CAST(:run_id AS uuid)
                  AND p.id = CAST(:partition_id AS uuid)
                  AND r.status = 'running'
                  AND p.status = 'running'
                  AND p.claim_token = CAST(:claim_token AS uuid)
                  AND p.lease_expires_at > statement_timestamp()
            )
        """
    else:
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM daily_metrics_runs AS r
                WHERE r.id = CAST(:run_id AS uuid)
                  AND r.status = 'running'
                  AND r.finalization_status = 'running'
                  AND r.finalization_claim_token = CAST(:claim_token AS uuid)
                  AND r.finalization_lease_expires_at > statement_timestamp()
            )
        """
    result = await session.execute(text(query), parameters)
    return bool(result.scalar_one())


async def _repair_execution(
    session: AsyncSession,
    execution_id: uuid.UUID,
    request: MetricExecutionRepairRequest,
) -> dict[str, str]:
    result = await session.execute(
        text(
            """
            SELECT id, worker_kind, operation, run_id, partition_id, claim_token,
                   state, attempt_count
            FROM metric_compatibility_executions
            WHERE id = CAST(:id AS uuid)
            FOR UPDATE
            """
        ),
        {"id": str(execution_id)},
    )
    row = result.mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Execution not found")

    repair_id = _repair_id(execution_id, request)
    prior_result = await session.execute(
        text(
            """
            SELECT resolution, review_evidence, output_evidence
            FROM metric_compatibility_execution_repairs
            WHERE id = CAST(:id AS uuid)
            """
        ),
        {"id": str(repair_id)},
    )
    prior = prior_result.mappings().first()
    encoded_output = (
        _canonical_json(request.output_evidence)
        if request.output_evidence is not None
        else None
    )
    if prior is not None:
        if (
            prior["resolution"] != request.resolution
            or prior["review_evidence"] != request.review_evidence
            or (
                prior["output_evidence"] is not None
                and _canonical_json(prior["output_evidence"]) != encoded_output
            )
        ):
            raise HTTPException(status_code=409, detail="Repair identity conflict")
        await session.commit()
        return {
            "status": "already_applied",
            "execution_id": str(execution_id),
            "state": str(row["state"]),
        }

    if (
        row["state"] != request.expected_state
        or row["attempt_count"] != request.expected_attempt_count
    ):
        raise HTTPException(
            status_code=409, detail="Execution state or attempt changed"
        )
    if await _original_claim_is_active(session, row):
        raise HTTPException(
            status_code=409, detail="Original execution claim is still active"
        )

    if request.resolution == "retry_safe":
        update = """
            UPDATE metric_compatibility_executions
            SET state = 'retry_authorized',
                last_attempt_at = statement_timestamp()
            WHERE id = CAST(:id AS uuid)
              AND state = :expected_state
              AND attempt_count = :expected_attempt_count
            RETURNING id
        """
        target_state = "retry_authorized"
    else:
        update = """
            UPDATE metric_compatibility_executions
            SET state = 'succeeded',
                output_evidence = CAST(:output_evidence AS jsonb),
                completed_at = statement_timestamp(),
                last_attempt_at = statement_timestamp()
            WHERE id = CAST(:id AS uuid)
              AND state = :expected_state
              AND attempt_count = :expected_attempt_count
            RETURNING id
        """
        target_state = "succeeded"
    updated = await session.execute(
        text(update),
        {
            "id": str(execution_id),
            "expected_state": request.expected_state,
            "expected_attempt_count": request.expected_attempt_count,
            "output_evidence": encoded_output,
        },
    )
    if updated.scalar_one_or_none() is None:
        raise HTTPException(status_code=409, detail="Execution repair CAS failed")
    await session.execute(
        text(
            """
            INSERT INTO metric_compatibility_execution_repairs (
                id, execution_id, expected_state, expected_attempt_count,
                resolution, review_evidence, output_evidence
            )
            VALUES (
                CAST(:id AS uuid), CAST(:execution_id AS uuid), :expected_state,
                :expected_attempt_count, :resolution, :review_evidence,
                CAST(:output_evidence AS jsonb)
            )
            """
        ),
        {
            "id": str(repair_id),
            "execution_id": str(execution_id),
            "expected_state": request.expected_state,
            "expected_attempt_count": request.expected_attempt_count,
            "resolution": request.resolution,
            "review_evidence": request.review_evidence,
            "output_evidence": encoded_output,
        },
    )
    await session.commit()
    return {
        "status": "repaired",
        "execution_id": str(execution_id),
        "state": target_state,
    }


async def _bulk_redrive_ambiguous_executions(
    session: AsyncSession,
    run_ids: list[uuid.UUID],
    review_evidence: str,
    operations: Sequence[Literal["partition", "finalize"]] = ("partition",),
) -> dict[str, int]:
    """CHAOS-4304: the ledger-side half of a stranded daily-metrics redrive.

    A run's daily/partition ledger row can be stuck at "ambiguous" (a
    progress-having failure -- see _mark_ambiguous) long after the run itself
    has been stranded by CHAOS-4358 (River discarded every daily_partition
    job for it). _reserve_execution refuses that row 409 ambiguous_refused
    forever, identically on every future attempt at the SAME (run,
    partition, family, generation, scope_digest) identity -- it is never
    "skipped" (that only happens for a genuine 'succeeded' row), but it is
    just as permanently unable to recompute without this repair, exactly the
    CHAOS-4304 gap: "a failed partition can never be recomputed" without an
    operator-authorized transition out of ambiguous first.

    This does not invent a new ledger rule: it applies _repair_execution's
    existing "retry_safe" resolution (gated on the original claim being
    provably dead) to every daily/partition OR daily/finalize row under the
    named runs whose state is either 'ambiguous' (a progress-having failure)
    or stuck 'executing' (CHAOS-4361: the owning api process died before any
    exception handler ran), in one pass, so an operator who has already
    identified stranded RUNS (from daily_metrics_partitions/
    daily_metrics_runs evidence, or the Go-side redrive's
    RedispatchedRunIDs) does not also have to enumerate and repair each
    execution id by hand. A row whose original claim is still active is left
    untouched and counted "skipped_claim_active", exactly as a single
    /repair call against it would refuse with 409 today -- this bulk path
    changes nothing about that safety rule (a live 'executing' claim is
    real, still-in-flight work, never something to repair out from under),
    only how many rows one operator action can advance.

    CHAOS-4409: originally this only ever selected operation='partition'
    rows -- a run's *finalize* execution row (worker_kind='daily',
    operation='finalize', partition_id NULL) can get stuck ambiguous/
    executing exactly the same way (the api process died mid-Finalize, or a
    progress-having finalize failure), and was invisible to this function
    even though _repair_execution/_original_claim_is_active ALREADY handle
    operation='finalize' generically (see _original_claim_is_active's own
    branch reading daily_metrics_runs.finalization_status/
    finalization_claim_token/finalization_lease_expires_at). Prod evidence:
    13 daily_metrics_runs stuck 'running' with 100% partitions succeeded
    (CHAOS-4389's stranded-finalize shape) whose finalize ledger row was
    stuck ambiguous/executing from the ORIGINAL stranding -- daily-finalize
    --run answered JobCancelError ambiguous_refused on every one of them,
    forever, because nothing ever repaired that row.

    `operations` (codex review, round 1, P1) scopes which operation this
    call is authorized to touch -- defaults to `("partition",)`,
    daily-redrive's original behavior byte-for-byte. A caller's
    review_evidence means something DIFFERENT for each operation
    (daily-redrive's is about partition output; daily-finalize's is about
    finalize output), so a caller must opt in to `"finalize"` explicitly:
    without this, daily-redrive's own call (which only ever republishes
    partition jobs) could silently move an UNRELATED finalize ledger row to
    retry_authorized without anyone having reviewed finalize output
    specifically, and some later, unrelated finalize attempt could then
    redrive it without the ledger's protection and duplicate already-
    written finalization output. No new resolution, no new safety rule --
    just an explicit scope on which of the two operations this ledger
    tracks for a daily run a given call may advance.

    codex review (round 3): "ambiguous" means a progress-having failure MAY
    have already written real output -- claim expiration alone is not
    evidence retry is safe (see _repair_execution's own docstring history).
    This function therefore NEVER auto-selects "confirm_succeeded" (which
    would need per-row output_evidence a bulk call cannot supply); it only
    ever authorizes "retry_safe", and review_evidence is a caller-supplied
    string, never a hardcoded default -- callers (the internal HTTP
    endpoint below, and the Go CLI's --review-evidence flag) MUST require
    an operator to state what they actually verified before invoking this
    at scale. A needless retry is not free: families whose readers do not
    argMax/dedup by computed_at (file_hotspots/file_metrics_daily, which
    SUMs raw rows) silently inflate their scores on a duplicate write.
    """
    if not run_ids:
        return {"repaired": 0, "skipped_claim_active": 0}
    candidates = await session.execute(
        text(
            """
            SELECT id, state, attempt_count
            FROM metric_compatibility_executions
            WHERE run_id = ANY(CAST(:run_ids AS uuid[]))
              AND worker_kind = 'daily' AND operation = ANY(CAST(:operations AS text[]))
              AND state IN ('ambiguous', 'executing')
            """
        ),
        {
            "run_ids": [str(run_id) for run_id in run_ids],
            "operations": list(operations),
        },
    )
    rows = candidates.mappings().all()
    repaired = 0
    skipped = 0
    for row in rows:
        try:
            await _repair_execution(
                session,
                row["id"],
                MetricExecutionRepairRequest(
                    expected_state=row["state"],
                    expected_attempt_count=row["attempt_count"],
                    resolution="retry_safe",
                    review_evidence=review_evidence,
                ),
            )
            repaired += 1
        except HTTPException as exc:
            # A CAS/claim-active refusal on ONE row (409) must not abort the
            # rest of the batch -- the whole point of a bulk redrive is to
            # make progress on every row that is actually safe, not to be as
            # fragile as calling /repair once per execution id by hand.
            if exc.status_code != 409:
                raise
            skipped += 1
    return {"repaired": repaired, "skipped_claim_active": skipped}


async def _mark_ambiguous(
    session: AsyncSession, execution: _Execution, detail: str
) -> None:
    await session.execute(
        text(
            """
            UPDATE metric_compatibility_executions
            SET state = 'ambiguous', failure_detail = :detail,
                last_attempt_at = statement_timestamp()
            WHERE id = CAST(:id AS uuid) AND state = 'executing'
            """
        ),
        {"id": str(execution.id), "detail": detail[:1024]},
    )
    await session.commit()


async def _mark_retry_authorized(
    session: AsyncSession, execution: _Execution, detail: str
) -> None:
    """Move a fresh failure straight to retry_authorized, skipping ambiguous.

    CHAOS-4264: only reached when the runner subprocess emitted zero
    progress lines before failing (signaled, resource-exhausted, or a plain
    exception) -- i.e. no repository's families were written for this
    execution, so there is nothing an ambiguous-state human review could
    confirm or refute that a retry doesn't already handle safely. This is
    the same terminal value _repair_execution's "retry_safe" resolution
    writes; the only difference is that it fires automatically instead of
    waiting on a human, and only under that stronger safety condition.
    """
    await session.execute(
        text(
            """
            UPDATE metric_compatibility_executions
            SET state = 'retry_authorized', failure_detail = :detail,
                last_attempt_at = statement_timestamp()
            WHERE id = CAST(:id AS uuid) AND state = 'executing'
            """
        ),
        {"id": str(execution.id), "detail": detail[:1024]},
    )
    await session.commit()
    # CHAOS-4319: mirrors the Go-side dev_health_metric_compat_retry_total
    # (internal/jobruntime) "persisted_failed" label -- this is the
    # "retry_authorized" half of the same bounded decision axis, emitted
    # from whichever side actually made the call.
    DEV_HEALTH_METRIC_COMPAT_RETRY_TOTAL.labels(
        worker_kind=execution.worker_kind, decision="retry_authorized"
    ).inc()


_MARK_SUCCEEDED_REMAINING_PARTITION = """
    UPDATE metric_compatibility_executions
    SET state = 'succeeded',
        output_evidence = CAST(:evidence AS jsonb),
        completed_at = statement_timestamp(),
        last_attempt_at = statement_timestamp()
    WHERE id = CAST(:id AS uuid)
      AND state = 'executing'
      AND EXISTS (
          SELECT 1
          FROM remaining_metric_runs AS r
          JOIN remaining_metric_partitions AS p ON p.run_id = r.id
          WHERE r.id = CAST(:run_id AS uuid)
            AND p.id = CAST(:partition_id AS uuid)
            AND r.status = 'running'
            AND r.canceled_at IS NULL
            AND p.status = 'running'
            AND p.claim_token = CAST(:claim_token AS uuid)
            AND p.lease_expires_at > statement_timestamp()
      )
    RETURNING id
"""

_MARK_SUCCEEDED_DAILY_PARTITION = """
    UPDATE metric_compatibility_executions
    SET state = 'succeeded',
        output_evidence = CAST(:evidence AS jsonb),
        completed_at = statement_timestamp(),
        last_attempt_at = statement_timestamp()
    WHERE id = CAST(:id AS uuid)
      AND state = 'executing'
      AND EXISTS (
          SELECT 1
          FROM daily_metrics_runs AS r
          JOIN daily_metrics_partitions AS p ON p.run_id = r.id
          WHERE r.id = CAST(:run_id AS uuid)
            AND p.id = CAST(:partition_id AS uuid)
            AND r.status = 'running'
            AND p.status = 'running'
            AND p.claim_token = CAST(:claim_token AS uuid)
            AND p.lease_expires_at > statement_timestamp()
      )
    RETURNING id
"""

_MARK_SUCCEEDED_DAILY_FINALIZE = """
    UPDATE metric_compatibility_executions
    SET state = 'succeeded',
        output_evidence = CAST(:evidence AS jsonb),
        completed_at = statement_timestamp(),
        last_attempt_at = statement_timestamp()
    WHERE id = CAST(:id AS uuid)
      AND state = 'executing'
      AND EXISTS (
          SELECT 1
          FROM daily_metrics_runs AS r
          WHERE r.id = CAST(:run_id AS uuid)
            AND r.status = 'running'
            AND r.finalization_status = 'running'
            AND r.finalization_claim_token = CAST(:claim_token AS uuid)
            AND r.finalization_lease_expires_at > statement_timestamp()
      )
    RETURNING id
"""


def _mark_succeeded_statement(execution: _Execution) -> str:
    if execution.worker_kind == "remaining":
        return _MARK_SUCCEEDED_REMAINING_PARTITION
    if execution.operation == "partition":
        return _MARK_SUCCEEDED_DAILY_PARTITION
    return _MARK_SUCCEEDED_DAILY_FINALIZE


async def _mark_succeeded(
    session: AsyncSession, execution: _Execution, evidence: dict[str, Any]
) -> None:
    encoded = _canonical_json(evidence)
    if len(encoded.encode()) > _MAX_EVIDENCE_BYTES:
        raise RuntimeError("metric execution evidence exceeds durable bound")
    result = await session.execute(
        text(_mark_succeeded_statement(execution)),
        {
            "id": str(execution.id),
            "evidence": encoded,
            "run_id": str(execution.run_id),
            "partition_id": (
                str(execution.partition_id) if execution.partition_id else None
            ),
            "claim_token": str(execution.claim_token),
        },
    )
    if result.scalar_one_or_none() is not None:
        await session.commit()
        return
    await _mark_ambiguous(
        session, execution, "lease changed before output acknowledgement"
    )
    raise HTTPException(
        status_code=409,
        detail={
            "message": "Execution completed after its durable lease changed",
            "execution_id": str(execution.id),
            "state": "ambiguous",
        },
    )


async def _run_recommendations(
    execution: _Execution, scope: RecommendationsScope
) -> dict[str, Any]:
    from dev_health_ops.workers.recommendations_tasks import (
        _compute_recommendations_for_org,
    )

    if scope.as_of:
        as_of_day = date.fromisoformat(scope.as_of)
        now = datetime.combine(
            as_of_day + timedelta(days=1), time.min, tzinfo=timezone.utc
        )
    else:
        now = datetime.now(timezone.utc)
        as_of_day = now.date()
    fired = await run_in_threadpool(
        _compute_recommendations_for_org,
        org_id=execution.organization_id,
        db_url=require_clickhouse_uri(),
        window=scope.window,
        now=now,
        as_of_day=as_of_day,
        team_id=scope.team_id,
    )
    return {"family": execution.family, "fired": fired}


async def _run_membership(
    execution: _Execution, scope: MembershipBackfillScope
) -> dict[str, Any]:
    from dev_health_ops.work_graph.investment.backfill import (
        MembershipBackfillConfig,
        backfill_memberships,
    )

    stats = await run_in_threadpool(
        backfill_memberships,
        MembershipBackfillConfig(
            dsn=require_clickhouse_uri(),
            org_id=execution.organization_id,
            repo_ids=scope.repo_ids or None,
        ),
    )
    # CHAOS-4243: stats["memberships"] is backfill_memberships's own total
    # membership-row count; surfaced as a flat top-level int (rather than
    # only nested in `stats`) so _evidence_row_count can report it.
    return {
        "family": execution.family,
        "stats": stats,
        "memberships_written": stats.get("memberships", 0),
    }


_RemainingRunner = Callable[[_Execution, Any], Awaitable[dict[str, Any]]]
_REMAINING_RUNNERS: dict[str, _RemainingRunner] = {
    "recommendations": _run_recommendations,
    "membership_backfill": _run_membership,
}


async def _run_remaining_direct(execution: _Execution) -> dict[str, Any]:
    try:
        runner = _REMAINING_RUNNERS[execution.family]
    except KeyError as exc:
        raise RuntimeError("remaining metrics family is not allowlisted") from exc
    scope = parse_scope(execution.family, execution.scope)
    return await runner(execution, scope)


async def _run_execution_direct(
    execution: _Execution,
    *,
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    # CHAOS-3092: this used to dispatch worker_kind == "daily" to
    # _run_daily_direct too -- deleted outright, the Go daily worker no
    # longer has a Python compatibility fallback to call. `on_progress` is
    # kept on the signature (accepted, currently unused) rather than
    # threaded further: it was _run_daily_direct's own per-repo progress
    # hook, and no surviving family under _run_remaining_direct reports
    # progress at all.
    if execution.worker_kind == "remaining":
        return await _run_remaining_direct(execution)
    raise RuntimeError("metric compatibility worker kind is not allowlisted")


async def _read_bounded_process_stream(
    stream: asyncio.StreamReader,
    maximum_bytes: int,
    *,
    on_progress: Callable[[], None] | None = None,
) -> bytes:
    """Accumulate the runner's stdout, optionally reacting to progress lines.

    ``on_progress`` (CHAOS-4316) is called once per ``{"progress": ...}``
    NDJSON line, AS IT ARRIVES -- not after the process exits. This is the
    only place that can see the subprocess still working in real time:
    ``stream.read()`` returns as soon as data is available, so a chunk
    containing a progress line is visible here well before ``process.wait()``
    or the outer ``stdout_task`` completes. The full byte accumulation this
    function already did is unchanged; only line-splitting a rolling buffer
    is new.
    """
    chunks: list[bytes] = []
    total = 0
    buffer = b""
    while chunk := await stream.read(64 * 1024):
        total += len(chunk)
        if total > maximum_bytes:
            raise ValueError("metric compatibility process output exceeds the bound")
        chunks.append(chunk)
        if on_progress is None:
            continue
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(parsed, dict) and "progress" in parsed:
                on_progress()
    return b"".join(chunks)


async def _read_bounded_stderr(
    stream: asyncio.StreamReader,
    maximum_bytes: int,
    *,
    live_log_context: str | None = None,
) -> tuple[bytes, bool]:
    """Drain the runner subprocess's stderr, keeping only the last
    ``maximum_bytes`` (truncating from the FRONT, not the back) rather than
    raising.

    CHAOS-4543: stderr is best-effort diagnostic text threaded into
    _CompatibilityProcessFailure's message, never something whose own size
    can turn into a DIFFERENT failure than the one actually classified (that
    is why this is not just _read_bounded_process_stream at a smaller bound
    -- that function's ValueError-on-overflow is correct for the bounded
    JSON protocol on stdout, wrong for advisory stderr). Continues draining
    past the bound (discarding the OLDEST bytes, not the newest) so a chatty
    child's write() never blocks on a full pipe once this task decides it
    has read enough to log -- but more importantly, keeps a TAIL window:
    the run_daily_metrics_job compute path logs one INFO line per ClickHouse
    query it issues (verbose, front-loaded) BEFORE anything fails, so a
    head-truncated bound reliably captures only that startup chatter and
    discards the one line that actually explains the failure (e.g.
    TestopsRowCapExceeded's table/org_id/max_rows/fetched detail, logged
    immediately before the process dies) -- observed directly against a
    real high-volume day during this ticket's own local repro.

    ``live_log_context`` (codex review): before CHAOS-4543, stderr was
    inherited (stderr=None), so an operator watching this container's own
    log stream in real time (e.g. `docker logs -f`) could see a child's
    diagnostics AS THEY HAPPENED -- including while it was still running or
    hanging. Piping it instead means this function is now the ONLY reader,
    so without this, nothing appears in the log until the whole run
    finishes, and a genuinely hung child (the exact case an operator most
    needs live output for) produces total silence until it is eventually
    killed. When set, each chunk is logged via `logger.info` as it arrives
    -- independent of, and in addition to, the bounded tail this function
    still returns for _CompatibilityProcessFailure's message and the
    post-exit summary log.

    Returns ``(tail, live_logged)``. ``live_logged`` is True when at least
    one chunk was emitted live via `logger.info` (codex round-3 P3): the
    caller's post-exit summary log must not re-embed the SAME full text a
    second time at WARNING/ERROR once it has already been streamed live --
    that would double-count the one diagnostic as two log records, which
    can read as two separate failures to a text-matching alert. The caller
    still logs a severity-correct, correlation-id-bearing line either way;
    only the redundant full-text repeat is skipped.
    """
    tail = b""
    truncated = False
    live_logged = False
    while chunk := await stream.read(64 * 1024):
        if live_log_context is not None:
            live_logged = True
            logger.info(
                "metric compatibility process stderr chunk (%s): %s",
                live_log_context,
                chunk.decode("utf-8", errors="replace").rstrip(),
            )
        tail += chunk
        if len(tail) > maximum_bytes:
            truncated = True
            tail = tail[-maximum_bytes:]
    if truncated:
        tail = b"...(truncated)\n" + tail
    return tail, live_logged


async def _terminate_compatibility_process(
    process: asyncio.subprocess.Process,
) -> None:
    if process.returncode is not None:
        return
    if os.name == "posix":
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
    else:
        process.terminate()
    try:
        await asyncio.wait_for(
            process.wait(), timeout=_PROCESS_TERMINATION_TIMEOUT_SECONDS
        )
    except TimeoutError:
        if os.name == "posix":
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        await process.wait()


class _CompatibilityProcessFailure(RuntimeError):
    """A classified runner subprocess failure (CHAOS-4264).

    ``reason`` is drawn from a fixed, bounded vocabulary
    ({"process_signaled", "resource_exhausted", "process_failed"}) safe to
    cross the HTTP boundary to the Go caller. ``safe_to_retry`` is true only
    when the runner emitted zero progress lines before failing -- meaning no
    repository's families were written for this execution, so a retry cannot
    create partial/duplicate state and does not need a human to confirm it.

    ``deterministic`` (CHAOS-4543) is true only when ``reason ==
    "resource_exhausted"`` AND the runner classified the specific failure as
    a KNOWN deterministic guard (today, only the testops loader's row-cap --
    EXIT_RESOURCE_EXHAUSTED_DETERMINISTIC), never a true memory kill that can
    vary attempt to attempt. A bounded bool, not raw text: crosses the HTTP
    boundary the same way ``reason``/``safe_to_retry`` already do, read by
    ops/internal/jobs/metrics/daily/compatibility_http.go to classify the
    Go-side job Permanent instead of Retryable.
    """

    def __init__(
        self,
        message: str,
        *,
        reason: str,
        safe_to_retry: bool,
        deterministic: bool = False,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.safe_to_retry = safe_to_retry
        self.deterministic = deterministic


_RUNNER_RESOURCE_EXHAUSTED_EXIT_CODE = 2
# CHAOS-4543: mirrors worker_metrics_runner.EXIT_RESOURCE_EXHAUSTED_DETERMINISTIC.
_RUNNER_RESOURCE_EXHAUSTED_DETERMINISTIC_EXIT_CODE = 3

# CHAOS-4317 (codex round 3, P1): the errno values a spawn-time OSError must
# carry to be reclassified as capacity_exhausted/retryable. EAGAIN is the
# literal 2026-08-26 incident ("pthread_create failed: Resource temporarily
# unavailable"); ENOMEM/EMFILE/ENFILE are the same class of transient,
# container-capacity-shaped failure. Anything else -- FileNotFoundError
# (bad command path), PermissionError (bad file mode) -- is a permanent
# deployment defect that must NOT be silently retried forever; it falls
# through to _execute's generic handler instead, same as before this ticket.
_CAPACITY_EXHAUSTED_SPAWN_ERRNOS = frozenset(
    {errno.EAGAIN, errno.ENOMEM, errno.EMFILE, errno.ENFILE}
)


async def _poll_peak_rss_bytes(
    process: asyncio.subprocess.Process,
    peak_holder: list[int],
    rss_kill_holder: list[bool],
    *,
    interval_seconds: float = 0.25,
) -> None:
    """Sample /proc/<pid>/status while the runner subprocess is alive, and
    kill it the moment real RSS crosses the configured memory bound.

    A watermark read after the fact (e.g. resource.getrusage(RUSAGE_CHILDREN))
    is unusable here: ru_maxrss is a lifetime max across every child the api
    process has ever reaped, not this one call, so it under-reports once a
    single earlier child has set a higher watermark. Polling VmRSS directly
    survives a SIGKILL too -- the last sample taken before the kill is still
    a real reading, unlike anything the child would have to report about
    itself on a graceful exit path it never reaches.

    codex R2: the peak is written into ``peak_holder`` (a mutable one-element
    list) on every iteration rather than returned at the end. The caller
    cancels this task as soon as it observes the subprocess has exited --
    almost always while this coroutine is inside ``asyncio.sleep``, which
    raises CancelledError there and never reaches a ``return`` statement. A
    return-value-only design silently reported 0 for every execution in
    practice, discarding the one signal this metric exists to expose.

    CHAOS-4361: this is now the PRIMARY memory enforcement, not just
    telemetry. worker_metrics_runner.py's child-side RLIMIT_AS self-bound
    measures virtual address space (thread stacks, malloc arenas,
    interpreter mappings), not resident memory, so it fires a classified
    MemoryError far below the real ceiling -- prod's 640 MiB RLIMIT_AS
    killed a child that peaked at 465 MB RSS with no rlimit applied at all.
    RSS is what the container's memcg actually accounts, so bounding on it
    here (the same real /proc/<pid>/status VmRSS this function already
    reads for telemetry) tracks the resource that actually matters and
    cannot false-fire on interpreter/driver overhead the way RLIMIT_AS does.
    ``rss_kill_holder`` (the same mutable-one-element-list pattern as
    ``peak_holder``) records that THIS watcher initiated the kill, so the
    caller's post-mortem classification can label it "resource_exhausted"
    regardless of the exact signal/exit code a SIGKILL produces -- the same
    reason a child's own RLIMIT_AS MemoryError has always been classified
    as, just enforced one layer up where the real number lives.
    """
    status_path = f"/proc/{process.pid}/status"
    ceiling = _configured_runner_memory_limit_bytes()
    while True:
        try:
            with open(status_path, encoding="ascii") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            peak_holder[0] = max(peak_holder[0], int(parts[1]) * 1024)
                        break
        except (FileNotFoundError, ProcessLookupError, OSError):
            return
        if peak_holder[0] >= ceiling:
            rss_kill_holder[0] = True
            await _terminate_compatibility_process(process)
            return
        await asyncio.sleep(interval_seconds)


async def _run_compatibility_process(execution: _Execution) -> dict[str, Any]:
    # CHAOS-4264: bound aggregate concurrency BEFORE spawning -- see
    # _RUNNER_CONCURRENCY_SEMAPHORE for why a per-process RLIMIT_AS alone
    # cannot protect the api container's shared cgroup.
    async with _RUNNER_CONCURRENCY_SEMAPHORE:
        # CHAOS-4316: makes "every slot occupied" directly observable instead
        # of only inferable after the fact from queued-partition latency, as
        # it was during the 2026-08-26 incident.
        # CHAOS-4317 (codex round 3, P2): the gauge now goes up in the SAME
        # try that guarantees its dec() -- previously inc() ran before
        # _reserve_pids_capacity(), so a timeout/cancellation raised while
        # still waiting for capacity (i.e. before the try/finally below even
        # started) left the gauge permanently pinned with no matching dec(),
        # reading as saturation forever after just one capacity refusal.
        DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE.inc()
        try:
            # CHAOS-4317: reserve pids/thread capacity BEFORE spawning
            # (waits, bounded and capacity-derived, if none is available --
            # see _reserve_pids_capacity's docstring). The reservation is
            # released below, in a finally wrapping the ENTIRE locked call,
            # so every exit path -- success, a classified
            # _CompatibilityProcessFailure, or any other exception --
            # releases exactly once. Held at this outer layer (not inside
            # _run_compatibility_process_locked) so the reservation covers
            # the child's full lifetime with a single, unmissable release
            # point, rather than needing every return/raise inside the
            # locked function to remember to release it.
            _waited, reserved = await _reserve_pids_capacity()
            try:
                return await _run_compatibility_process_locked(execution)
            finally:
                await _release_pids_capacity_reservation(reserved)
        finally:
            DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE.dec()


async def _run_compatibility_process_locked(execution: _Execution) -> dict[str, Any]:
    payload = _canonical_json(_execution_process_payload(execution)).encode()
    if len(payload) > _MAX_COMPATIBILITY_PROCESS_BYTES:
        raise ValueError("metric compatibility process input exceeds the bound")
    # CHAOS-4317: capacity is already reserved by the caller
    # (_run_compatibility_process) before this function is even invoked.
    # Even so, the reservation's estimate can be wrong or another consumer
    # can fill the budget between admission and this exact syscall -- codex
    # review (PR #1931 round 2, P1): a raw OSError/BlockingIOError here
    # (EAGAIN from pthread_create -- exactly the 2026-08-26 incident's
    # failure) would otherwise propagate uncaught through _execute's
    # generic `except Exception` handler, which marks the execution
    # ambiguous (needs a human /repair call) even though no computation
    # ever started. Reclassify it the same way as every other
    # capacity_exhausted case: always retryable, never a silent drop or an
    # unnecessary human-review parking.
    try:
        process = await asyncio.create_subprocess_exec(
            *_COMPATIBILITY_RUNNER_COMMAND,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            # The child reserves stdout for the bounded JSON protocol.
            # CHAOS-4543: stderr used to be inherited (stderr=None) so
            # compatibility diagnostics stayed visible in this container's
            # own log stream -- but that meant this process could never READ
            # what the child printed, so a failure's message here was always
            # one of a handful of hardcoded static strings regardless of how
            # specific the child's own stderr was (e.g. TestopsRowCapExceeded's
            # table/org_id/max_rows/fetched detail never survived past the
            # child's own log line). Piped instead, bounded-read below, and
            # re-logged via `logger` so container-log visibility is
            # unchanged -- but now also threaded into the
            # _CompatibilityProcessFailure message these branches raise,
            # which _mark_ambiguous/_mark_retry_authorized persist into
            # metric_compatibility_executions.failure_detail.
            stderr=asyncio.subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
    except OSError as exc:
        # CHAOS-4317 (codex round 3, P1): only reclassify spawn failures
        # whose errno is actually resource-shaped -- see
        # _CAPACITY_EXHAUSTED_SPAWN_ERRNOS. A FileNotFoundError/
        # PermissionError here means the runner command itself is broken
        # (bad path, bad mode), not a transient capacity shortage; retrying
        # that forever would mask a deployment defect as an operational
        # hiccup, so it propagates unchanged to _execute's generic handler.
        if exc.errno not in _CAPACITY_EXHAUSTED_SPAWN_ERRNOS:
            raise
        DEV_HEALTH_METRIC_COMPAT_CAPACITY_WAIT_EXHAUSTED_TOTAL.inc()
        raise _CompatibilityProcessFailure(
            f"metric compatibility process capacity_exhausted -- spawn "
            f"itself failed: {exc}",
            reason="capacity_exhausted",
            safe_to_retry=True,
        ) from exc
    if process.stdin is None or process.stdout is None or process.stderr is None:
        await _terminate_compatibility_process(process)
        raise RuntimeError("metric compatibility process pipes are unavailable")
    started_at = _monotonic()
    # CHAOS-3092: this used to gate a `liveness_watched`/progress-stall
    # watchdog on worker_kind == "daily" and operation == "partition" --
    # deleted outright along with the watchdog itself (see the module-level
    # comment near the top of this file). `on_progress` stays `None`
    # unconditionally: _read_bounded_process_stream still detects a
    # "progress" line for `progress_seen` below (read from the stdout bytes
    # after the process exits), it just no longer reacts to one in real
    # time.
    stdout_task = asyncio.create_task(
        _read_bounded_process_stream(
            process.stdout,
            _MAX_COMPATIBILITY_PROCESS_BYTES,
        )
    )
    # CHAOS-4543: draining stderr (not just reading it after exit) avoids the
    # same full-pipe-buffer hang stdout's own concurrent read already avoids
    # -- a chatty child (e.g. a full traceback on the generic except-Exception
    # path) could otherwise block on write() forever with nobody reading.
    # Uses its own truncate-not-raise helper (_read_bounded_stderr), not
    # _read_bounded_process_stream: stderr is best-effort diagnostic text --
    # exceeding its bound must never itself become the failure (raising
    # ValueError here would replace the real classified failure below with
    # an unrelated one), unlike stdout's bounded JSON protocol, where
    # exceeding the bound IS a real failure.
    stderr_task = asyncio.create_task(
        _read_bounded_stderr(
            process.stderr,
            _MAX_COMPATIBILITY_STDERR_BYTES,
            live_log_context=(
                f"run_id={execution.run_id} partition_id={execution.partition_id} "
                f"operation={execution.operation}"
            ),
        )
    )
    peak_rss_holder = [0]
    rss_kill_holder = [False]
    rss_task = asyncio.create_task(
        _poll_peak_rss_bytes(process, peak_rss_holder, rss_kill_holder)
    )
    peak_thread_count_holder = [0]
    pids_task = asyncio.create_task(
        _poll_peak_child_thread_count(process.pid, process, peak_thread_count_holder)
    )
    input_error: BrokenPipeError | ConnectionResetError | None = None
    try:
        try:
            process.stdin.write(payload)
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            input_error = exc
        finally:
            process.stdin.close()
        stdout, (stderr, stderr_live_logged), return_code = await asyncio.gather(
            stdout_task, stderr_task, process.wait()
        )
    except BaseException:
        await _terminate_compatibility_process(process)
        raise
    finally:
        if not stdout_task.done():
            stdout_task.cancel()
        if not stderr_task.done():
            stderr_task.cancel()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        if not rss_task.done():
            rss_task.cancel()
        if not pids_task.done():
            pids_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            # CodeQL py/ineffectual-statement flags this as a no-op because
            # the coroutine's return value (always None) is discarded, but
            # the await itself is the point: it blocks until rss_task
            # actually finishes reacting to the cancel() above (or raises
            # CancelledError, suppressed here), which is what guarantees
            # peak_rss_holder's last write below has already happened.
            # Dismissed at the GitHub code-scanning API level with this
            # justification -- inline `# lgtm[...]` comments do not
            # suppress GitHub CodeQL (that syntax is a legacy LGTM.com-only
            # mechanism; see tests/api/dev/test_terminal_frames.py for the
            # same established pattern in this codebase).
            await rss_task
        with contextlib.suppress(asyncio.CancelledError):
            # Same cancel-then-await pattern as rss_task immediately above,
            # for the same reason: guarantees peak_thread_count_holder's
            # last write below has already happened.
            await pids_task
        # codex R2: read the shared holder, NOT the task's return value --
        # cancelling rss_task almost always interrupts it inside
        # asyncio.sleep, which raises CancelledError before any `return`
        # statement runs. The holder already has every sample taken up to
        # one polling interval before this point, which is what actually
        # survives the cancellation.
        DEV_HEALTH_METRIC_COMPAT_RUNNER_RSS_BYTES.labels(
            worker_kind=execution.worker_kind
        ).set(peak_rss_holder[0])
        DEV_HEALTH_METRIC_COMPAT_EXECUTION_DURATION_SECONDS.labels(
            worker_kind=execution.worker_kind, operation=execution.operation
        ).observe(_monotonic() - started_at)
        # CHAOS-4317: converge the per-child thread-count estimate toward
        # the real observed peak of THIS child specifically (not the
        # container-wide count -- see _poll_peak_child_thread_count's
        # docstring). Only a positive, plausible sample counts, so a /proc
        # read that never succeeded (peak_thread_count_holder stays 0)
        # never regresses the seeded/converged estimate.
        if peak_thread_count_holder[0] > 0:
            _record_per_child_pids_cost(peak_thread_count_holder[0])

    # CHAOS-3092: this used to also track `progress_seen` (any "progress"
    # NDJSON line seen in stdout) to feed the now-deleted worker_kind ==
    # "daily" branches of `safe_to_retry` below -- removed along with them,
    # since nothing reads it any more. Only the outcome line still matters.
    lines = [line for line in stdout.split(b"\n") if line.strip()]
    outcome_line: bytes | None = None
    for line in lines:
        try:
            parsed = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(parsed, dict) and "outcome" in parsed:
            outcome_line = line

    # CHAOS-4543: decode+log the child's captured stderr once, here, for
    # EVERY exit including success -- codex review: stderr used to be
    # inherited (stderr=None), so a successful run's own diagnostics (e.g.
    # the cgroup/RLIMIT_AS backstop WARN in worker_metrics_runner.py) still
    # streamed straight to this container's log in real time. Piping it
    # instead (so a failure's message can embed it -- see below) means this
    # process is now the ONLY thing that ever sees it; logging it
    # unconditionally here, before the return_code branch, is what keeps
    # that visibility rather than silently dropping it on the success path.
    # `errors="replace"` because a signal-killed/OOM-adjacent child can
    # truncate mid-multibyte-character; this is diagnostic text, never
    # something a decode error should hide entirely.
    #
    # codex round-3 P3: when the same content was already streamed live
    # (see _read_bounded_stderr's live_logged), re-embedding the full text
    # here too means a text-matching alert sees the ONE diagnostic as TWO
    # log records (an INFO chunk, then this WARNING/ERROR). This process is
    # the only call site and always passes live_log_context, so
    # stderr_live_logged is True whenever there was any stderr at all --
    # log a severity-correct, correlation-id-bearing summary line either
    # way, but only repeat the full text when it was NOT already live.
    stderr_text = stderr.decode("utf-8", errors="replace").strip()
    if stderr_text:
        stderr_log = logger.error if return_code != 0 else logger.warning
        if stderr_live_logged:
            stderr_log(
                "metric compatibility process stderr already streamed live "
                "(run_id=%s partition_id=%s operation=%s return_code=%s, "
                "%d bytes)",
                execution.run_id,
                execution.partition_id,
                execution.operation,
                return_code,
                len(stderr_text),
            )
        else:
            stderr_log(
                "metric compatibility process stderr (run_id=%s partition_id=%s "
                "operation=%s return_code=%s): %s",
                execution.run_id,
                execution.partition_id,
                execution.operation,
                return_code,
                stderr_text,
            )

    if return_code == 0:
        DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL.labels(reason="success").inc()
        if input_error is not None:
            raise RuntimeError(
                "metric compatibility process rejected its input"
            ) from input_error
        if outcome_line is None:
            raise ValueError("metric compatibility process returned invalid JSON")
        try:
            decoded = json.loads(outcome_line)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "metric compatibility process returned invalid JSON"
            ) from exc
        if (
            not isinstance(decoded, dict)
            or set(decoded) != {"outcome"}
            or not isinstance(decoded["outcome"], dict)
        ):
            raise ValueError(
                "metric compatibility process returned an invalid response"
            )
        return decoded["outcome"]

    if rss_kill_holder[0]:
        # CHAOS-4361: this exit is OURS -- _poll_peak_rss_bytes observed real
        # RSS cross the configured DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES
        # bound and killed the child via the same _terminate_compatibility_
        # process every other kill path uses. Classified identically to the
        # child's own (now-secondary) RLIMIT_AS backstop MemoryError exit
        # -- "resource_exhausted" -- since both mean the same thing: this
        # execution needed more memory than its configured budget. Checked
        # BEFORE stall_reason_holder: an RSS breach can itself stop progress
        # lines from arriving (the child stalls while thrashing/allocating
        # right before it dies), and the memory diagnosis is the more
        # specific, more actionable one when both would otherwise apply.
        #
        # CHAOS-3092: safe_to_retry is unconditionally False now -- it used
        # to require worker_kind == "daily" and operation == "partition"
        # (only _run_daily_direct's "partition" branch ever wired real
        # per-scope write evidence through on_write_starting), but that
        # bridge path is deleted outright and this function is reached only
        # by the remaining-metrics dispatch, which never reports progress at
        # all (treating silence as "definitely wrote nothing" there would be
        # a fabricated safety claim, not an observed one -- codex R2 on
        # CHAOS-4264).
        safe_to_retry = False
        DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL.labels(
            reason="resource_exhausted"
        ).inc()
        DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL.labels(
            reason="resource_exhausted"
        ).inc()
        raise _CompatibilityProcessFailure(
            f"metric compatibility process exceeded its memory bound "
            f"({peak_rss_holder[0]} bytes RSS >= "
            f"{_configured_runner_memory_limit_bytes()} bytes configured)",
            reason="resource_exhausted",
            safe_to_retry=safe_to_retry,
        )

    # CHAOS-3092: the CHAOS-4316 progress-stall watchdog's own classification
    # branch (`if stall_reason_holder[0] is not None: ...`, reason=
    # "progress_stalled") is deleted from here -- stall_reason_holder no
    # longer exists (see the module-level comment near the top of this
    # file), so this exit path can no longer occur.

    # CHAOS-4264: a non-zero exit is classified instead of collapsed into one
    # generic RuntimeError.
    #
    # CHAOS-3092: safe_to_retry is unconditionally False now -- see the RSS
    # branch above for why. This was already true independent of that
    # deletion for every path this function still reaches (the
    # remaining-metrics dispatch never reports progress), so behavior here
    # is unchanged; only the now-impossible worker_kind == "daily" condition
    # is gone.
    safe_to_retry = False
    deterministic = False
    if return_code < 0:
        reason = "process_signaled"
        message = (
            f"metric compatibility process was terminated by signal {-return_code}"
        )
    elif return_code == _RUNNER_RESOURCE_EXHAUSTED_DETERMINISTIC_EXIT_CODE:
        # CHAOS-4543: a KNOWN deterministic guard (e.g. the testops loader's
        # row cap) -- see _CompatibilityProcessFailure's docstring. Reason
        # stays "resource_exhausted" (same River-visible classification as
        # the non-deterministic case); only `deterministic` differs.
        reason = "resource_exhausted"
        deterministic = True
        message = "metric compatibility process exceeded its memory bound"
    elif return_code == _RUNNER_RESOURCE_EXHAUSTED_EXIT_CODE:
        reason = "resource_exhausted"
        message = "metric compatibility process exceeded its memory bound"
    else:
        reason = "process_failed"
        message = "metric compatibility process failed"
    # CHAOS-4543: append the child's own captured stderr (already logged in
    # full above) to the classified message when there is any -- a signaled
    # process (kernel OOM, SIGTERM) or a true last-gasp interpreter
    # MemoryError may legitimately have written nothing before dying, in
    # which case the bare classified message above is exactly as informative
    # as before this ticket. But a deliberate guard raised with plenty of
    # headroom (e.g. TestopsRowCapExceeded's table/org_id/max_rows/fetched
    # detail) always DOES print something, and that detail previously never
    # survived past the runner's own generic `except MemoryError:` handler --
    # this is what closes that gap end to end into
    # metric_compatibility_executions.failure_detail (via
    # _mark_ambiguous/_mark_retry_authorized, which persist
    # f"{reason}: {message}"[:1024]).
    #
    # Sliced to _MAX_COMPATIBILITY_STDERR_MESSAGE_BYTES (smaller than the
    # full logged capture) and to its OWN tail again here -- the informative
    # content is at the end of an already-tail-captured stderr, and the
    # ledger's[:1024] truncation is head-only, so embedding the full 8 KiB
    # capture would let that same 1024-byte cap discard exactly the part
    # this fix exists to preserve (verified against a real repro).
    if stderr_text:
        embedded = stderr_text[-_MAX_COMPATIBILITY_STDERR_MESSAGE_BYTES:]
        if len(embedded) < len(stderr_text):
            embedded = "...(truncated)\n" + embedded
        message = f"{message} -- {embedded}"
    DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL.labels(reason=reason).inc()
    raise _CompatibilityProcessFailure(
        message, reason=reason, safe_to_retry=safe_to_retry, deterministic=deterministic
    )


async def _wait_for_client_disconnect(connection: Request) -> None:
    while not await connection.is_disconnected():
        await asyncio.sleep(_DISCONNECT_POLL_SECONDS)


async def _run_until_client_disconnect(
    connection: Request, execution: _Execution
) -> dict[str, Any]:
    process_task = asyncio.create_task(_run_compatibility_process(execution))
    disconnect_task = asyncio.create_task(_wait_for_client_disconnect(connection))
    try:
        done, _pending = await asyncio.wait(
            {process_task, disconnect_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if process_task in done:
            return process_task.result()
        process_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await process_task
        raise ConnectionError("metric compatibility request client disconnected")
    finally:
        for task in (process_task, disconnect_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(process_task, disconnect_task, return_exceptions=True)


# CHAOS-4243: the (now-deleted, CHAOS-4291) Go compatibility bridge parsed
# an optional rows_written field so a zero-row completion was never stored
# identically to a real write. This maps each remaining-metrics family still
# reachable through _REMAINING_RUNNERS to the evidence key that carries a
# genuine row count. A family absent here, or whose evidence value isn't a
# plain int, gets no rows_written key at all -- "not applicable", never
# coerced to a false 0.
#
# dora and capacity are gone from _REMAINING_RUNNERS entirely (CHAOS-5336:
# both native Go executors -- dora_native.go/capacity_native.go -- have no
# Python fallback, and job_dora.py/job_capacity.py are deleted outright), so
# their old evidence-key entries here are moot rather than fixed. complexity
# is gone from _REMAINING_RUNNERS entirely too (CHAOS-4291: the native
# ComplexityExecutor has no Python fallback), so its old "deliberate gap"
# entry here is moot rather than fixed.
#
# extra_metrics and team_metrics no longer exist: both were registered
# handlers with zero producer anywhere (CHAOS-4243), retired (removed, not
# left dormant) rather than fixed. See
# docs/contribute/architecture/go-worker-runtime.md for the decision note
# naming the inline compute sites that already cover every table they would
# have written.
#
# recommendations is ALSO a deliberate omission (CHAOS-4243 codex round 3):
# _compute_recommendations_for_org's docstring is explicit that its int
# return is "the number of *fired* recommendations written (tombstones
# excluded)" -- the function persists the FULL rule state per team, fired
# rows AND explicit fired=False tombstones, so a run can write many rows
# while `fired` reads 0. Mapping "fired" here would report a misleading
# rows_written (a wrong non-zero-looking-like-zero case), which is worse
# than reporting none at all. Fixing this properly needs
# _compute_recommendations_for_org to return the true persisted count
# (len(records)) alongside fired_count -- a signature change with several
# existing test call sites (tests/test_recommendations_task.py), deferred
# as a separate, larger change.
_EVIDENCE_ROW_COUNT_KEYS: dict[str, str] = {
    "membership_backfill": "memberships_written",
}


def _evidence_row_count(family: str, evidence: dict[str, Any]) -> int | None:
    key = _EVIDENCE_ROW_COUNT_KEYS.get(family)
    if key is None:
        return None
    value = evidence.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


async def _execute(
    session: AsyncSession,
    execution: _Execution,
    connection: Request,
) -> dict[str, Any]:
    reservation = await _reserve_execution(session, execution)
    if reservation == "skipped":
        return {"status": "skipped", "execution_id": str(execution.id)}
    try:
        evidence = await _run_until_client_disconnect(connection, execution)
    except asyncio.CancelledError:
        await _mark_ambiguous(session, execution, "request canceled during execution")
        raise
    except _CompatibilityProcessFailure as exc:
        # CHAOS-4264: a signaled/resource-exhausted/failed runner subprocess
        # that produced zero progress lines is safe to hand straight back to
        # River as retryable -- skip the human-review-only ambiguous state
        # entirely, since there is nothing to review (nothing was written).
        # Anything with at least one progress line stays ambiguous, exactly
        # as any other failure always has.
        if exc.safe_to_retry:
            await _mark_retry_authorized(session, execution, f"{exc.reason}: {exc}")
            raise HTTPException(
                status_code=503,
                detail={
                    "message": "Metric execution failed before any output was produced",
                    "execution_id": str(execution.id),
                    "state": "failed",
                    "reason": exc.reason,
                    "deterministic": exc.deterministic,
                },
            ) from exc
        await _mark_ambiguous(session, execution, f"{exc.reason}: {exc}")
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Metric execution outcome is ambiguous",
                "execution_id": str(execution.id),
                "state": "ambiguous",
                "reason": exc.reason,
                "deterministic": exc.deterministic,
            },
        ) from exc
    except Exception as exc:
        await _mark_ambiguous(
            session, execution, f"executor raised {type(exc).__name__}"
        )
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Metric execution outcome is ambiguous",
                "execution_id": str(execution.id),
                "state": "ambiguous",
            },
        ) from exc
    await _mark_succeeded(session, execution, evidence)
    response: dict[str, Any] = {"status": "success", "execution_id": str(execution.id)}
    if execution.worker_kind == "remaining":
        rows_written = _evidence_row_count(execution.family, evidence)
        if rows_written is not None:
            response["rows_written"] = rows_written
    return response


@router.post("/remaining-metrics/v1/execute")
async def execute_remaining_metrics(
    request: RemainingMetricsExecutionRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    connection: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    authorize_worker_bridge(authorization)
    execution = await _load_remaining_execution(session, request)
    return await _execute(session, execution, connection)


@router.get("/metric-executions/v1/{execution_id}")
async def read_metric_execution(
    execution_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    authorize_worker_bridge(authorization)
    result = await session.execute(
        text(
            """
            SELECT id, worker_kind, operation, run_id, partition_id, family,
                   generation, state, attempt_count, output_evidence
            FROM metric_compatibility_executions
            WHERE id = CAST(:id AS uuid)
            """
        ),
        {"id": str(execution_id)},
    )
    row = result.mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {
        "execution_id": str(row["id"]),
        "worker_kind": row["worker_kind"],
        "operation": row["operation"],
        "run_id": str(row["run_id"]),
        "partition_id": (
            str(row["partition_id"]) if row["partition_id"] is not None else None
        ),
        "family": row["family"],
        "generation": row["generation"],
        "state": row["state"],
        "attempt_count": row["attempt_count"],
        "output_evidence": row["output_evidence"],
    }


@router.post("/metric-executions/v1/{execution_id}/repair")
async def repair_metric_execution(
    execution_id: uuid.UUID,
    request: MetricExecutionRepairRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    authorize_metric_repair(authorization)
    return await _repair_execution(session, execution_id, request)


@router.post("/daily-metrics/v1/redrive")
async def redrive_daily_metrics(
    request: DailyMetricsRedriveRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, int]:
    authorize_metric_repair(authorization)
    result = await _bulk_redrive_ambiguous_executions(
        session, request.run_ids, request.review_evidence, request.operations
    )
    await session.commit()
    return result
