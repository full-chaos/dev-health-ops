"""Dormant, authenticated compatibility bridge for Go metric workers.

The wire contract intentionally carries only durable identifiers and a fixed
operation. PostgreSQL supplies every compute argument, and a durable execution
ledger fences retries that arrive after an effect may already have happened.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.internal.worker_auth import authorize_worker_bridge
from dev_health_ops.db import require_clickhouse_uri
from dev_health_ops.metrics.remaining_scope_contract import (
    CapacityScope,
    ComplexityScope,
    DoraScope,
    ExtraMetricsScope,
    MembershipBackfillScope,
    RecommendationsScope,
    ReleaseImpactScope,
    TeamMetricsScope,
    parse_scope,
)

router = APIRouter(prefix="/internal/worker", include_in_schema=False)

_EXECUTION_NAMESPACE = uuid.UUID("e6678cc4-a4e9-55c5-9354-9c6202a1834e")
_MAX_EVIDENCE_BYTES = 4096


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DailyMetricsExecutionRequest(_StrictRequest):
    operation: Literal["partition", "finalize"]
    run_id: uuid.UUID
    partition_id: uuid.UUID | None = None

    @model_validator(mode="after")
    def validate_operation_identity(self) -> DailyMetricsExecutionRequest:
        if (self.operation == "partition") != (self.partition_id is not None):
            raise ValueError("partition_id must be supplied only for partition")
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
                   generation, scope_digest, state, attempt_count
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
    await session.commit()
    raise HTTPException(
        status_code=409,
        detail={
            "message": "Execution outcome requires readback",
            "execution_id": str(execution.id),
            "state": str(existing["state"]),
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
    if request.expected_state == "executing" and await _original_claim_is_active(
        session, row
    ):
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


def _completion_fence(execution: _Execution) -> str:
    if execution.worker_kind == "remaining":
        return """
            EXISTS (
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
    if execution.operation == "partition":
        return """
            EXISTS (
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
    return """
        EXISTS (
            SELECT 1
            FROM daily_metrics_runs AS r
            WHERE r.id = CAST(:run_id AS uuid)
              AND r.status = 'running'
              AND r.finalization_status = 'running'
              AND r.finalization_claim_token = CAST(:claim_token AS uuid)
              AND r.finalization_lease_expires_at > statement_timestamp()
        )
    """


async def _mark_succeeded(
    session: AsyncSession, execution: _Execution, evidence: dict[str, Any]
) -> None:
    encoded = _canonical_json(evidence)
    if len(encoded.encode()) > _MAX_EVIDENCE_BYTES:
        raise RuntimeError("metric execution evidence exceeds durable bound")
    result = await session.execute(
        text(
            f"""
            UPDATE metric_compatibility_executions
            SET state = 'succeeded',
                output_evidence = CAST(:evidence AS jsonb),
                completed_at = statement_timestamp(),
                last_attempt_at = statement_timestamp()
            WHERE id = CAST(:id AS uuid)
              AND state = 'executing'
              AND {_completion_fence(execution)}
            RETURNING id
            """
        ),
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


async def _run_daily_direct(execution: _Execution) -> dict[str, Any]:
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
    for repo_id in repo_ids:
        await run_daily_metrics_job(
            db_url=db_url,
            day=target_day,
            backfill_days=1,
            repo_id=repo_id,
            skip_finalize=True,
            sink="clickhouse",
            provider="auto",
            org_id=execution.organization_id,
        )
    return {
        "operation": "partition",
        "target_day": target_day.isoformat(),
        "repo_count": len(repo_ids),
    }


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
    return {"family": execution.family, "stats": stats}


async def _run_extra_metrics(
    execution: _Execution, scope: ExtraMetricsScope
) -> dict[str, Any]:
    from dev_health_ops.metrics.benchmarking.runner import run_benchmarking_for_day
    from dev_health_ops.metrics.job_compounding_risk import run_compounding_risk_job
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    db_url = require_clickhouse_uri()
    target_day = date.fromisoformat(scope.day)
    await run_compounding_risk_job(
        db_url=db_url,
        day=target_day,
        backfill_days=scope.backfill_days,
        org_id=execution.organization_id,
    )
    sink = ClickHouseMetricsSink(db_url)
    try:
        setattr(sink, "org_id", execution.organization_id)
        total = 0
        for offset in range(scope.backfill_days):
            current = target_day - timedelta(days=offset)
            outputs = await run_in_threadpool(
                run_benchmarking_for_day,
                sink,
                as_of_day=current,
                computed_at=datetime.now(timezone.utc),
                org_id=execution.organization_id,
            )
            total += sum(len(rows) for rows in outputs.values())
    finally:
        sink.close()
    return {"family": execution.family, "benchmark_records": total}


async def _run_team_metrics(
    execution: _Execution, scope: TeamMetricsScope
) -> dict[str, Any]:
    from dev_health_ops.metrics.job_daily import run_daily_metrics_job

    await run_daily_metrics_job(
        db_url=require_clickhouse_uri(),
        day=date.fromisoformat(scope.day),
        backfill_days=scope.backfill_days,
        repo_id=None,
        repo_name=None,
        sink="clickhouse",
        provider="auto",
        org_id=execution.organization_id,
    )
    return {"family": execution.family, "day": scope.day}


_RemainingRunner = Callable[[_Execution, Any], Awaitable[dict[str, Any]]]
_REMAINING_RUNNERS: dict[str, _RemainingRunner] = {
    "capacity": _run_capacity,
    "complexity": _run_complexity,
    "dora": _run_dora,
    "release_impact": _run_release_impact,
    "recommendations": _run_recommendations,
    "membership_backfill": _run_membership,
    "extra_metrics": _run_extra_metrics,
    "team_metrics": _run_team_metrics,
}


async def _run_remaining_direct(execution: _Execution) -> dict[str, Any]:
    try:
        runner = _REMAINING_RUNNERS[execution.family]
    except KeyError as exc:
        raise RuntimeError("remaining metrics family is not allowlisted") from exc
    scope = parse_scope(execution.family, execution.scope)
    return await runner(execution, scope)


async def _execute(
    session: AsyncSession,
    execution: _Execution,
    runner: Callable[[_Execution], Awaitable[dict[str, Any]]],
) -> dict[str, str]:
    reservation = await _reserve_execution(session, execution)
    if reservation == "skipped":
        return {"status": "skipped", "execution_id": str(execution.id)}
    try:
        evidence = await runner(execution)
    except asyncio.CancelledError:
        await _mark_ambiguous(session, execution, "request canceled during execution")
        raise
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
    return {"status": "success", "execution_id": str(execution.id)}


@router.post("/daily-metrics/v1/execute")
async def execute_daily_metrics(
    request: DailyMetricsExecutionRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    authorize_worker_bridge(authorization)
    execution = await _load_daily_execution(session, request)
    return await _execute(session, execution, _run_daily_direct)


@router.post("/remaining-metrics/v1/execute")
async def execute_remaining_metrics(
    request: RemainingMetricsExecutionRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    authorize_worker_bridge(authorization)
    execution = await _load_remaining_execution(session, request)
    return await _execute(session, execution, _run_remaining_direct)


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
    authorize_worker_bridge(authorization)
    return await _repair_execution(session, execution_id, request)
