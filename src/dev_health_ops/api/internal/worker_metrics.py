"""Dormant, authenticated compatibility bridge for Go metric workers.

The wire contract intentionally carries only durable identifiers and a fixed
operation. PostgreSQL supplies every compute argument, and a durable execution
ledger fences retries that arrive after an effect may already have happened.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import signal
import sys
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from dataclasses import replace as dataclass_replace
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
    DEV_HEALTH_METRIC_COMPAT_CHILD_SILENCE_SECONDS,
    DEV_HEALTH_METRIC_COMPAT_EXECUTION_DURATION_SECONDS,
    DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL,
    DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL,
    DEV_HEALTH_METRIC_COMPAT_RETRY_TOTAL,
    DEV_HEALTH_METRIC_COMPAT_RUNNER_RSS_BYTES,
    DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE,
)
from dev_health_ops.metrics.remaining_scope_contract import (
    CapacityScope,
    ComplexityScope,
    DoraScope,
    MembershipBackfillScope,
    RecommendationsScope,
    ReleaseImpactScope,
    parse_scope,
)

router = APIRouter(prefix="/internal/worker", include_in_schema=False)

_EXECUTION_NAMESPACE = uuid.UUID("e6678cc4-a4e9-55c5-9354-9c6202a1834e")
_MAX_EVIDENCE_BYTES = 4096
_MAX_COMPATIBILITY_PROCESS_BYTES = 1024 * 1024
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


# ---------------------------------------------------------------------------
# CHAOS-4316: progress-based liveness bound on ComputePartition.
#
# runWithLeaseRenewal (Go, internal/jobs/metrics/daily/daily.go) renews the
# partition's lease on a fixed ticker purely because RenewPartition (a cheap
# PG UPDATE) succeeds -- independent of whether this bridge is making any
# real progress. The Go HTTP client deliberately sets no Client.Timeout
# either. So this process is the only place a hang can ever be observed and
# bounded: it is the "fixed, killable process boundary" worker_metrics_runner
# already exists to be (see that module's docstring), and it already emits a
# real per-repo progress signal (_emit_progress) that today is only read
# AFTER the subprocess exits, to decide safe_to_retry. The watchdog below is
# the first thing that reads it WHILE the subprocess is still alive.
#
# Both bounds are derived from the partition's own repo_count, never a flat
# wall-clock number (standing rule: timeouts never fix capacity races) -- a
# 1-repo partition is reclaimed quickly, a 40-repo partition gets
# proportionally longer. Scoped to worker_kind == "daily" and
# operation == "partition" only: that is the sole compatibility path with a
# real progress signal today (same scope _CompatibilityProcessFailure's
# existing safe_to_retry rule already uses). Finalize and every
# remaining-metrics family have no progress instrumentation -- a bound there
# would be a guess, not a derived value; tracked as CHAOS-4331.
_PROGRESS_STALL_BASE_SECONDS_ENV = (
    "DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_BASE_SECONDS"
)
_PROGRESS_STALL_PER_REPO_SECONDS_ENV = (
    "DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_PER_REPO_SECONDS"
)
_PROGRESS_STALL_HARD_CEILING_MULTIPLIER_ENV = (
    "DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_HARD_CEILING_MULTIPLIER"
)

# Defaults derived from the 2026-08-26 incident's own timeline (a single
# partition sat silent for 74 minutes before a human killed it by hand): a
# 120s grace period covers ClickHouse connect + the first repo's setup before
# any progress line is expected, 90s per additional repo covers a
# pagination-heavy repo's worst observed per-repo compute time with headroom,
# and a 3x multiplier on the resulting window is the hard ceiling backstop
# for a partition that trickles just enough progress to dodge the interval
# check. All three are env-tunable, not blessed as universal constants.
_DEFAULT_PROGRESS_STALL_BASE_SECONDS = 120.0
_DEFAULT_PROGRESS_STALL_PER_REPO_SECONDS = 90.0
_DEFAULT_PROGRESS_STALL_HARD_CEILING_MULTIPLIER = 3.0

_PROGRESS_STALL_WATCHDOG_POLL_SECONDS = 1.0

# CHAOS-4264's own memory-limit env key/default, duplicated (not imported)
# rather than shared with worker_metrics_runner.py: that module already
# imports from this one (_canonical_json, _execution_from_process_payload,
# _run_execution_direct), so importing back would be circular. Kept in sync
# by convention -- both are small, stable, and reviewed together.
_RUNNER_MEMORY_LIMIT_ENV_KEY = "DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES"
_RUNNER_DEFAULT_MEMORY_LIMIT_BYTES = 640 * 1024 * 1024
# Fraction of the configured memory limit that counts as "near enough to the
# ceiling to call it OOM" when the authoritative memcg signal
# (/sys/fs/cgroup/memory.events) is unavailable (non-Linux, cgroup v1, no
# permission) -- a fallback, not the primary signal (team-lead direction,
# CHAOS-4316: the OOM label must come from the memcg signal where available).
_OOM_RSS_FALLBACK_FRACTION = 0.9


def _configured_positive_float_env(key: str, default: float) -> float:
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _configured_runner_memory_limit_bytes() -> int:
    raw = os.environ.get(_RUNNER_MEMORY_LIMIT_ENV_KEY, "").strip()
    if not raw:
        return _RUNNER_DEFAULT_MEMORY_LIMIT_BYTES
    try:
        value = int(raw)
    except ValueError:
        return _RUNNER_DEFAULT_MEMORY_LIMIT_BYTES
    return value if value > 0 else _RUNNER_DEFAULT_MEMORY_LIMIT_BYTES


def _progress_stall_watchdog_enabled() -> bool:
    """True unless the base stall window was explicitly set to "0".

    On by default for the same reason the Go backstop is (team-lead ruling
    2026-08-26): deployed configuration never sets these env vars, so an
    opt-in design would silently never activate. A window of exactly 0 is
    not a usable value on its own (it would kill a child before it could
    ever emit a first progress line), so it is reserved as the explicit,
    deliberate opt-out signal instead -- any other value, including one
    that fails to parse, leaves the watchdog enabled with its normal
    (default or configured) window.
    """
    raw = os.environ.get(_PROGRESS_STALL_BASE_SECONDS_ENV, "").strip()
    if not raw:
        return True
    try:
        return float(raw) != 0
    except ValueError:
        return True


def _progress_stall_window_seconds(repo_count: int) -> float:
    base = _configured_positive_float_env(
        _PROGRESS_STALL_BASE_SECONDS_ENV, _DEFAULT_PROGRESS_STALL_BASE_SECONDS
    )
    per_repo = _configured_positive_float_env(
        _PROGRESS_STALL_PER_REPO_SECONDS_ENV, _DEFAULT_PROGRESS_STALL_PER_REPO_SECONDS
    )
    return base + per_repo * max(repo_count, 0)


def _progress_hard_ceiling_seconds(repo_count: int) -> float:
    multiplier = _configured_positive_float_env(
        _PROGRESS_STALL_HARD_CEILING_MULTIPLIER_ENV,
        _DEFAULT_PROGRESS_STALL_HARD_CEILING_MULTIPLIER,
    )
    return _progress_stall_window_seconds(repo_count) * multiplier


def _read_cgroup_oom_kill_count() -> int | None:
    """Read this container's own cgroup v2 ``memory.events`` oom_kill count.

    CHAOS-4264: Docker's OOMKilled flag only tracks PID 1, so a memcg kill of
    a child this process spawned is invisible to ``docker inspect``.
    ``memory.events`` IS visible -- the api process and every runner
    subprocess it spawns share one cgroup, so a real memcg OOM anywhere in
    this container increments the same counter this reads. Returns None (not
    0) when the file is unavailable (cgroup v1, non-Linux, no permission) so
    callers can tell "definitely zero kills" apart from "signal not
    observable here" and fall back to the RSS heuristic only in the latter
    case.
    """
    try:
        with open("/sys/fs/cgroup/memory.events", encoding="ascii") as handle:
            for line in handle:
                parts = line.split()
                if len(parts) == 2 and parts[0] == "oom_kill":
                    return int(parts[1])
    except (FileNotFoundError, PermissionError, OSError, ValueError):
        return None
    return None


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DailyMetricsExecutionRequest(_StrictRequest):
    operation: Literal["partition", "finalize"]
    run_id: uuid.UUID
    partition_id: uuid.UUID | None = None
    # CHAOS-4276: families a NativeFamilyExecutor already computed and wrote
    # for this partition on the Go side -- run_daily_metrics_job must not
    # recompute or rewrite them. Empty/omitted is a no-op, byte-identical to
    # every request before this field existed.
    skip_families: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_operation_identity(self) -> DailyMetricsExecutionRequest:
        if (self.operation == "partition") != (self.partition_id is not None):
            raise ValueError("partition_id must be supplied only for partition")
        if self.operation != "partition" and self.skip_families:
            raise ValueError("skip_families must be supplied only for partition")
        return self


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
    # CHAOS-4276: families a NativeFamilyExecutor already computed on the Go
    # side, from DailyMetricsExecutionRequest.skip_families. Deliberately NOT
    # part of _execution_id's identity digest (an orchestration hint about
    # WHO computes a family, not part of the durable scope being computed) --
    # attached via dataclasses.replace after _load_daily_execution builds the
    # rest of _Execution from durable Postgres state.
    skip_families: tuple[str, ...] = ()


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
        "skip_families": list(execution.skip_families),
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
        "skip_families",
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise ValueError("metric compatibility process input is invalid")
    raw_skip_families = payload["skip_families"]
    if not isinstance(raw_skip_families, list) or not all(
        isinstance(value, str) for value in raw_skip_families
    ):
        raise ValueError("metric compatibility process skip_families is invalid")
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
    execution = _execution_from_row(
        worker_kind=worker_kind,
        operation=operation,
        row=row,
        partition_id=partition_id,
    )
    return dataclass_replace(execution, skip_families=tuple(raw_skip_families))


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


async def _load_daily_execution(
    session: AsyncSession, request: DailyMetricsExecutionRequest
) -> _Execution:
    if request.operation == "partition":
        result = await session.execute(
            text(
                """
                SELECT r.id AS run_id, r.org_id, r.target_day, r.generation,
                       p.repo_ids, p.claim_token
                FROM daily_metrics_runs AS r
                JOIN daily_metrics_partitions AS p ON p.run_id = r.id
                WHERE r.id = CAST(:run_id AS uuid)
                  AND p.id = CAST(:partition_id AS uuid)
                  AND r.status = 'running'
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
        partition_id = request.partition_id
    else:
        result = await session.execute(
            text(
                """
                SELECT r.id AS run_id, r.org_id, r.target_day, r.generation,
                       r.finalization_claim_token AS claim_token
                FROM daily_metrics_runs AS r
                WHERE r.id = CAST(:run_id AS uuid)
                  AND r.status = 'running'
                  AND r.finalization_status = 'running'
                  AND r.finalization_lease_expires_at > statement_timestamp()
                FOR UPDATE OF r
                """
            ),
            {"run_id": str(request.run_id)},
        )
        row = result.mappings().first()
        partition_id = None
    if row is None:
        raise HTTPException(
            status_code=409, detail="Daily metrics lease is absent or expired"
        )
    try:
        return _execution_from_row(
            worker_kind="daily",
            operation=request.operation,
            row=row,
            partition_id=partition_id,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=409, detail="Daily metrics durable scope is invalid"
        ) from exc


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
    await session.commit()
    raise HTTPException(
        status_code=409,
        detail={
            "message": "Execution outcome requires readback",
            "execution_id": str(execution.id),
            "state": str(existing["state"]),
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


async def _run_daily_direct(
    execution: _Execution,
    *,
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    from dev_health_ops.metrics.job_daily import (
        run_daily_metrics_finalize,
        run_daily_metrics_job,
    )

    db_url = require_clickhouse_uri()
    target_day = date.fromisoformat(str(execution.scope["target_day"]))
    if execution.operation == "finalize":
        await run_daily_metrics_finalize(
            db_url=db_url,
            day=target_day,
            org_id=execution.organization_id,
            sink="clickhouse",
        )
        return {"operation": "finalize", "target_day": target_day.isoformat()}

    repo_ids = [uuid.UUID(value) for value in execution.scope["repo_ids"]]
    # CHAOS-4246: run_daily_metrics_job degrades (never raises) on a family
    # that computed zero rows for the day -- collect that per repo so a
    # partition that reports "succeeded" still carries which families, if
    # any, produced nothing. families_zero_rows is deliberately NOT used to
    # fail the partition (zero rows is often a legitimate quiet day); it is
    # surfaced so staleness is visible in the execution result instead of
    # silently indistinguishable from a fully-populated run.
    families_zero_rows: dict[str, list[str]] = {}
    repo_count = len(repo_ids)
    for index, repo_id in enumerate(repo_ids):
        # CHAOS-4264: one repo_id at a time -- each run_daily_metrics_job call
        # loads and releases only that repo's source rows, so a partition's
        # peak working set does not scale with repo_count.
        #
        # on_write_starting fires INSIDE run_daily_metrics_job, immediately
        # before its first ClickHouse write for this repo -- not after the
        # call returns. A repo-level "finished" signal is too coarse: codex
        # review (CHAOS-4264 R1) correctly flagged that a kill between the
        # first write block and the function's return would still land rows
        # while reporting zero progress, so a killed-mid-write execution
        # could be misclassified safe_to_retry. Firing at the write boundary
        # instead means "no progress at all" only ever means "definitely
        # wrote nothing" -- any write attempt, even a single one, is treated
        # as unsafe-to-blindly-retry (ambiguous), exactly as before this
        # ticket.
        def _mark_progress() -> None:
            if on_progress is not None:
                on_progress(index + 1, repo_count)

        zero_rows_by_day = await run_daily_metrics_job(
            db_url=db_url,
            day=target_day,
            backfill_days=1,
            repo_id=repo_id,
            skip_finalize=True,
            sink="clickhouse",
            provider="auto",
            org_id=execution.organization_id,
            on_write_starting=_mark_progress,
            skip_families=set(execution.skip_families) or None,
        )
        for day, families in zero_rows_by_day.items():
            if families:
                families_zero_rows[f"{repo_id}:{day.isoformat()}"] = families
    result: dict[str, Any] = {
        "operation": "partition",
        "target_day": target_day.isoformat(),
        "repo_count": repo_count,
    }
    if families_zero_rows:
        result["families_zero_rows"] = families_zero_rows
    return result


async def _run_capacity(execution: _Execution, scope: CapacityScope) -> dict[str, Any]:
    from dev_health_ops.metrics.job_capacity import run_capacity_forecast

    if execution.generation_seed is None:
        raise RuntimeError("capacity execution is missing its generation seed")
    results = await run_capacity_forecast(
        db_url=require_clickhouse_uri(),
        org_id=execution.organization_id,
        team_id=scope.team_id,
        work_scope_id=scope.work_scope_id,
        target_items=scope.target_items,
        target_date=date.fromisoformat(scope.target_date)
        if scope.target_date
        else None,
        history_days=scope.history_days,
        simulations=scope.simulations,
        all_teams=scope.all_teams,
        persist=True,
        seed=execution.generation_seed,
    )
    return {"family": execution.family, "forecast_count": len(results)}


async def _run_complexity(
    execution: _Execution, scope: ComplexityScope
) -> dict[str, Any]:
    from dev_health_ops.metrics.job_complexity_db import run_complexity_db_job

    result = await run_in_threadpool(
        run_complexity_db_job,
        repo_id=uuid.UUID(scope.repo_id) if scope.repo_id else None,
        db_url=require_clickhouse_uri(),
        date=date.fromisoformat(scope.day),
        backfill_days=scope.backfill_days,
        language_globs=scope.language_globs or None,
        max_files=scope.max_files,
        search_pattern=scope.search_pattern,
        exclude_globs=scope.exclude_globs or None,
        org_id=execution.organization_id,
    )
    if result not in {0}:
        raise RuntimeError("complexity executor returned a non-success status")
    return {"family": execution.family, "exit_code": result}


async def _run_dora(execution: _Execution, scope: DoraScope) -> dict[str, Any]:
    from dev_health_ops.metrics.job_dora import run_dora_metrics_job

    await run_in_threadpool(
        run_dora_metrics_job,
        db_url=require_clickhouse_uri(),
        day=date.fromisoformat(scope.day),
        backfill_days=scope.backfill_days,
        repo_id=uuid.UUID(scope.repo_id) if scope.repo_id else None,
        repo_name=scope.repo_name,
        sink=scope.sink,
        metrics=scope.metrics,
        interval=scope.interval,
        org_id=execution.organization_id,
    )
    return {"family": execution.family, "day": scope.day}


async def _run_release_impact(
    execution: _Execution, scope: ReleaseImpactScope
) -> dict[str, Any]:
    from dev_health_ops.metrics.job_release_impact import run_release_impact_job

    written = await run_release_impact_job(
        db_url=require_clickhouse_uri(),
        day=date.fromisoformat(scope.day),
        backfill_days=scope.backfill_days,
        recomputation_window_days=scope.recomputation_window_days,
        org_id=execution.organization_id,
    )
    return {"family": execution.family, "records_written": written}


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
    "capacity": _run_capacity,
    "complexity": _run_complexity,
    "dora": _run_dora,
    "release_impact": _run_release_impact,
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
    if execution.worker_kind == "daily":
        return await _run_daily_direct(execution, on_progress=on_progress)
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
    """

    def __init__(self, message: str, *, reason: str, safe_to_retry: bool) -> None:
        super().__init__(message)
        self.reason = reason
        self.safe_to_retry = safe_to_retry


_RUNNER_RESOURCE_EXHAUSTED_EXIT_CODE = 2


async def _poll_peak_rss_bytes(
    pid: int, peak_holder: list[int], *, interval_seconds: float = 0.25
) -> None:
    """Sample /proc/<pid>/status while the runner subprocess is alive.

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
    """
    status_path = f"/proc/{pid}/status"
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
        await asyncio.sleep(interval_seconds)


async def _watch_progress_stall(
    process: asyncio.subprocess.Process,
    repo_count: int,
    last_progress_holder: list[float],
    started_at: float,
    stall_reason_holder: list[str | None],
    *,
    interval_seconds: float = _PROGRESS_STALL_WATCHDOG_POLL_SECONDS,
) -> None:
    """Kill the runner subprocess if it stops reporting progress (CHAOS-4316).

    Runs alongside ``_poll_peak_rss_bytes`` on the same event loop, so a hang
    inside the subprocess (the thing being watched) cannot block this task --
    both read completely independent state (``/proc``, and the timestamp
    ``on_progress`` below writes). Two independent bounds, both derived from
    ``repo_count``, never a flat wall-clock number:

    - ``stall``: no progress line since ``last_progress_holder[0]`` (updated
      by the caller's ``on_progress`` callback) for
      ``_progress_stall_window_seconds(repo_count)``. Covers the observed
      incident exactly: zero progress ever, silent for the whole window.
    - ``timeout``: total elapsed time since ``started_at`` exceeds
      ``_progress_hard_ceiling_seconds(repo_count)`` regardless of whether
      progress keeps trickling in -- the backstop for a partition that
      resets the stall clock just often enough to dodge it forever.

    Writes its verdict into ``stall_reason_holder`` (the same
    mutable-one-element-list pattern ``_poll_peak_rss_bytes`` already uses,
    for the same reason: the caller cancels this task, and a cancelled
    coroutine's return value is never seen) and terminates the process via
    the existing ``_terminate_compatibility_process`` -- no new kill
    mechanism, this only decides WHEN to call the one that already exists.
    """
    stall_window = _progress_stall_window_seconds(repo_count)
    hard_ceiling = _progress_hard_ceiling_seconds(repo_count)
    while True:
        await asyncio.sleep(interval_seconds)
        if process.returncode is not None:
            return
        now = _monotonic()
        if now - started_at >= hard_ceiling:
            stall_reason_holder[0] = "timeout"
        elif now - last_progress_holder[0] >= stall_window:
            stall_reason_holder[0] = "stalled"
        else:
            continue
        await _terminate_compatibility_process(process)
        return


async def _run_compatibility_process(execution: _Execution) -> dict[str, Any]:
    # CHAOS-4264: bound aggregate concurrency BEFORE spawning -- see
    # _RUNNER_CONCURRENCY_SEMAPHORE for why a per-process RLIMIT_AS alone
    # cannot protect the api container's shared cgroup.
    async with _RUNNER_CONCURRENCY_SEMAPHORE:
        # CHAOS-4316: makes "every slot occupied" directly observable instead
        # of only inferable after the fact from queued-partition latency, as
        # it was during the 2026-08-26 incident.
        DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE.inc()
        try:
            return await _run_compatibility_process_locked(execution)
        finally:
            DEV_HEALTH_METRIC_COMPAT_RUNNER_SLOTS_IN_USE.dec()


async def _run_compatibility_process_locked(execution: _Execution) -> dict[str, Any]:
    payload = _canonical_json(_execution_process_payload(execution)).encode()
    if len(payload) > _MAX_COMPATIBILITY_PROCESS_BYTES:
        raise ValueError("metric compatibility process input exceeds the bound")
    process = await asyncio.create_subprocess_exec(
        *_COMPATIBILITY_RUNNER_COMMAND,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        # The child reserves stdout for the bounded JSON protocol and inherits
        # stderr so compatibility diagnostics remain visible to operators.
        stderr=None,
        start_new_session=os.name == "posix",
    )
    if process.stdin is None or process.stdout is None:
        await _terminate_compatibility_process(process)
        raise RuntimeError("metric compatibility process pipes are unavailable")
    started_at = _monotonic()
    # CHAOS-4316: only the daily "partition" operation carries a real
    # per-repo progress signal (see the module-level comment above
    # _PROGRESS_STALL_BASE_SECONDS_ENV) -- repo_count is 0 for every other
    # operation, which _watch_progress_stall below is only started for when
    # this is nonzero-eligible (guarded by the same worker_kind/operation
    # check as safe_to_retry further down).
    liveness_watched = (
        execution.worker_kind == "daily"
        and execution.operation == "partition"
        and _progress_stall_watchdog_enabled()
    )
    repo_count = len(execution.scope.get("repo_ids") or []) if liveness_watched else 0
    last_progress_holder = [started_at]
    stall_reason_holder: list[str | None] = [None]
    stdout_task = asyncio.create_task(
        _read_bounded_process_stream(
            process.stdout,
            _MAX_COMPATIBILITY_PROCESS_BYTES,
            on_progress=(
                (lambda: last_progress_holder.__setitem__(0, _monotonic()))
                if liveness_watched
                else None
            ),
        )
    )
    peak_rss_holder = [0]
    rss_task = asyncio.create_task(_poll_peak_rss_bytes(process.pid, peak_rss_holder))
    stall_task: asyncio.Task[None] | None = None
    oom_kill_before = _read_cgroup_oom_kill_count()
    if liveness_watched:
        stall_task = asyncio.create_task(
            _watch_progress_stall(
                process,
                repo_count,
                last_progress_holder,
                started_at,
                stall_reason_holder,
                # Read the module global at call time, not bound as the
                # function's default at definition time, so tests can
                # monkeypatch a fast poll interval and have it actually take
                # effect (a bound default would ignore the patch).
                interval_seconds=_PROGRESS_STALL_WATCHDOG_POLL_SECONDS,
            )
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
        stdout, return_code = await asyncio.gather(stdout_task, process.wait())
    except BaseException:
        await _terminate_compatibility_process(process)
        raise
    finally:
        if not stdout_task.done():
            stdout_task.cancel()
        await asyncio.gather(stdout_task, return_exceptions=True)
        if not rss_task.done():
            rss_task.cancel()
        if stall_task is not None and not stall_task.done():
            stall_task.cancel()
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
        if stall_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await stall_task
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

    lines = [line for line in stdout.split(b"\n") if line.strip()]
    progress_seen = False
    outcome_line: bytes | None = None
    for line in lines:
        try:
            parsed = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(parsed, dict) and "progress" in parsed:
            progress_seen = True
        elif isinstance(parsed, dict) and "outcome" in parsed:
            outcome_line = line

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

    if stall_reason_holder[0] is not None:
        # CHAOS-4316: this exit is OURS -- the liveness watchdog decided the
        # child was silent too long and killed it via the same
        # _terminate_compatibility_process every other kill path uses.
        # safe_to_retry reuses the EXACT existing rule below (progress_seen
        # computed from the same lines, scoped the same way) rather than a
        # new one: zero progress ever observed is safe to hand straight back
        # to River, any partial progress stays ambiguous, unchanged from
        # CHAOS-4264's contract.
        silence_seconds = _monotonic() - last_progress_holder[0]
        oom_kill_after = _read_cgroup_oom_kill_count()
        memcg_oom = (
            oom_kill_before is not None
            and oom_kill_after is not None
            and oom_kill_after > oom_kill_before
        )
        rss_near_limit = peak_rss_holder[0] >= (
            _configured_runner_memory_limit_bytes() * _OOM_RSS_FALLBACK_FRACTION
        )
        # Team-lead direction (CHAOS-4316): the memcg signal is authoritative
        # when observable; RSS-vs-limit is a fallback only for when it is
        # not (non-Linux, cgroup v1, no permission) -- never the other way
        # around, since a busy-but-not-yet-OOM process can sit near the
        # limit without ever being killed for memory.
        if memcg_oom:
            local_reason = "oom"
        elif oom_kill_before is None or oom_kill_after is None:
            local_reason = "oom" if rss_near_limit else stall_reason_holder[0]
        else:
            local_reason = stall_reason_holder[0]
        DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL.labels(reason=local_reason).inc()
        DEV_HEALTH_METRIC_COMPAT_CHILD_SILENCE_SECONDS.observe(silence_seconds)
        DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL.labels(
            reason="progress_stalled"
        ).inc()
        safe_to_retry = (
            execution.worker_kind == "daily"
            and execution.operation == "partition"
            and not progress_seen
        )
        raise _CompatibilityProcessFailure(
            f"metric compatibility process {stall_reason_holder[0]} -- no "
            f"progress for {silence_seconds:.1f}s",
            reason="progress_stalled",
            safe_to_retry=safe_to_retry,
        )

    # CHAOS-4264: a non-zero exit is classified instead of collapsed into one
    # generic RuntimeError. safe_to_retry additionally requires
    # worker_kind == "daily" AND operation == "partition" (codex R2 + R3):
    # only _run_daily_direct's "partition" branch wires on_write_starting
    # through job_daily.py, so it is the only path with real per-scope
    # write evidence. Every remaining-metrics family (capacity/complexity/
    # dora/release_impact/recommendations/membership_backfill) never
    # reports progress at all -- treating that silence as "definitely wrote
    # nothing" would be a fabricated safety claim, not an observed one
    # (codex R2). The daily "finalize" branch is the same trap (codex R3):
    # run_daily_metrics_finalize writes user_metrics_daily and
    # ic_landscape_rolling_30d directly with no progress callback of its
    # own, so a kill after its first write would ALSO report zero progress
    # despite having written rows, if worker_kind alone gated this. Both
    # non-partition paths stay ambiguous unconditionally, exactly as before
    # this ticket.
    #
    # CHAOS-4319 (considered, NOT applied): an earlier version of this
    # change also dropped `not progress_seen` for daily/partition, reasoning
    # that every table run_daily_metrics_job writes is an append-only
    # MergeTree table readers dedup by design (platform contract), so a
    # repeated write should be a harmless duplicate. Codex round-1 review
    # falsified that premise: file_metrics_daily (the file_hotspots family)
    # IS append-only, but its readers (api/queries/heatmap.py's
    # fetch_hotspot_risk, and its sibling code-hotspots query) `SUM(...)`
    # over the raw rows with no `argMax`/`computed_at` dedup at all -- a
    # retry-caused duplicate row silently inflates hotspot/churn scores, not
    # a "harmless duplicate a reader already collapses." Proving the
    # append-only+reader-dedup property per table (team-lead's CHAOS-4319 GO
    # condition 1) is real work spanning every family in families.json and
    # every reader of every table it writes -- out of this ticket's scope.
    # `not progress_seen` stays required here; CHAOS-4319's durable-truth
    # fix (Go-side FailPartitionPermanently + the state-aware classification
    # above) still fully closes the "silently discarded" bug on its own --
    # a progress-having failure now durably persists failed_permanent
    # (visible, with a reason and telemetry) the FIRST time it lands
    # ambiguous, rather than looping through 5 guaranteed 409s first.
    safe_to_retry = (
        execution.worker_kind == "daily"
        and execution.operation == "partition"
        and not progress_seen
    )
    if return_code < 0:
        reason = "process_signaled"
        message = (
            f"metric compatibility process was terminated by signal {-return_code}"
        )
    elif return_code == _RUNNER_RESOURCE_EXHAUSTED_EXIT_CODE:
        reason = "resource_exhausted"
        message = "metric compatibility process exceeded its memory bound"
    else:
        reason = "process_failed"
        message = "metric compatibility process failed"
    DEV_HEALTH_METRIC_COMPAT_PROCESS_EXITS_TOTAL.labels(reason=reason).inc()
    raise _CompatibilityProcessFailure(
        message, reason=reason, safe_to_retry=safe_to_retry
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


# CHAOS-4243: the Go compatibility bridge (internal/jobs/metrics/remaining/
# compatibility_http.go) parses an optional rows_written field so a
# zero-row completion is never stored identically to a real write. This maps
# each remaining-metrics family to the evidence key (see the runners in
# _REMAINING_RUNNERS below) that carries a genuine row count. A family absent
# here, or whose evidence value isn't a plain int, gets no rows_written key
# at all -- "not applicable", never coerced to a false 0.
#
# dora and capacity's native Go executors report their own rows_written
# through CompatibilityOutcome directly (dora_native.go/capacity_native.go)
# and never call this HTTP bridge, so they are not listed here.
#
# complexity is a DELIBERATE GAP, not an oversight: run_complexity_db_job
# (job_complexity_db.py) returns a process exit code, not a row count, which
# would need a return-contract change to the underlying compute function
# itself, a larger and riskier change than this ticket's wire-contract fix.
# It stays silently "success" with no rows_written signal until that
# follow-up lands.
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
    "capacity": "forecast_count",
    "release_impact": "records_written",
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


@router.post("/daily-metrics/v1/execute")
async def execute_daily_metrics(
    request: DailyMetricsExecutionRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    connection: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    authorize_worker_bridge(authorization)
    execution = await _load_daily_execution(session, request)
    if request.skip_families:
        # CHAOS-4276: an orchestration hint from the Go dispatcher, not part
        # of the durable scope _load_daily_execution reads from Postgres --
        # attached here, after the execution identity is already fixed.
        execution = dataclass_replace(
            execution, skip_families=tuple(request.skip_families)
        )
    return await _execute(session, execution, connection)


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
