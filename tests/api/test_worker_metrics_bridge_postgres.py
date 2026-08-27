"""Live PostgreSQL checks for the metric compatibility execution fence."""

from __future__ import annotations

import os
import uuid

import pytest
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.api.internal import worker_metrics

_TEST_URI = os.getenv("METRIC_BRIDGE_POSTGRES_TEST_URI")
pytestmark = pytest.mark.skipif(
    not _TEST_URI, reason="METRIC_BRIDGE_POSTGRES_TEST_URI is not configured"
)


@pytest.mark.asyncio
async def test_effect_before_ack_is_never_reexecuted_after_lease_reclaim() -> None:
    assert _TEST_URI is not None
    engine = create_async_engine(_TEST_URI)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    run_id = uuid.uuid4()
    partition_id = uuid.uuid4()
    org_id = uuid.uuid4()
    first_claim = uuid.uuid4()
    second_claim = uuid.uuid4()
    scope = {
        "version": 1,
        "all_teams": True,
        "history_days": 90,
        "simulations": 1000,
    }
    effects: list[str] = []
    try:
        async with session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO remaining_metric_runs (
                        id, org_id, family, generation, scope_key,
                        generation_seed, status
                    )
                    VALUES (
                        CAST(:run_id AS uuid), CAST(:org_id AS uuid), 'capacity',
                        'generation-v1', 'all-teams', 1234, 'running'
                    )
                    """
                ),
                {"run_id": str(run_id), "org_id": str(org_id)},
            )
            await session.execute(
                text(
                    """
                    INSERT INTO remaining_metric_partitions (
                        id, run_id, ordinal, scope, status, claim_token,
                        lease_expires_at
                    )
                    VALUES (
                        CAST(:partition_id AS uuid), CAST(:run_id AS uuid), 1,
                        CAST(:scope AS jsonb), 'running',
                        CAST(:claim_token AS uuid),
                        statement_timestamp() + interval '10 minutes'
                    )
                    """
                ),
                {
                    "partition_id": str(partition_id),
                    "run_id": str(run_id),
                    "scope": worker_metrics._canonical_json(scope),
                    "claim_token": str(first_claim),
                },
            )
            await session.commit()

            request = worker_metrics.RemainingMetricsExecutionRequest(
                operation="partition",
                run_id=run_id,
                partition_id=partition_id,
            )
            execution = await worker_metrics._load_remaining_execution(session, request)
            assert await worker_metrics._reserve_execution(session, execution) == (
                "execute"
            )

            # The append happened, but the process died before it could
            # acknowledge the Go claim. A recovery path marks the persisted
            # execution ambiguous.
            effects.append("append-output")
            await worker_metrics._mark_ambiguous(
                session, execution, "simulated kill after append"
            )

            await session.execute(
                text(
                    """
                    UPDATE remaining_metric_partitions
                    SET claim_token = CAST(:claim_token AS uuid),
                        lease_expires_at = statement_timestamp() + interval '10 minutes'
                    WHERE id = CAST(:partition_id AS uuid)
                    """
                ),
                {
                    "claim_token": str(second_claim),
                    "partition_id": str(partition_id),
                },
            )
            await session.commit()
            reclaimed = await worker_metrics._load_remaining_execution(session, request)
            assert reclaimed.id == execution.id
            with pytest.raises(HTTPException) as retry:
                await worker_metrics._reserve_execution(session, reclaimed)
            assert retry.value.status_code == 409
            assert effects == ["append-output"]

            repaired = await worker_metrics._repair_execution(
                session,
                execution.id,
                worker_metrics.MetricExecutionRepairRequest(
                    expected_state="ambiguous",
                    expected_attempt_count=1,
                    resolution="confirm_succeeded",
                    review_evidence="verified one capacity output for this generation",
                    output_evidence={"forecast_count": 1, "reviewed": True},
                ),
            )
            assert repaired["state"] == "succeeded"
            assert await worker_metrics._reserve_execution(session, reclaimed) == (
                "skipped"
            )

            ledger = (
                (
                    await session.execute(
                        text(
                            """
                        SELECT state, attempt_count
                        FROM metric_compatibility_executions
                        WHERE id = CAST(:id AS uuid)
                        """
                        ),
                        {"id": str(execution.id)},
                    )
                )
                .mappings()
                .one()
            )
            assert ledger == {"state": "succeeded", "attempt_count": 1}
            assert effects == ["append-output"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_state", ["executing", "ambiguous"])
async def test_repair_waits_for_claim_loss_and_retries_exact_attempt(
    expected_state: str,
) -> None:
    assert _TEST_URI is not None
    engine = create_async_engine(_TEST_URI)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    run_id = uuid.uuid4()
    partition_id = uuid.uuid4()
    org_id = uuid.uuid4()
    first_claim = uuid.uuid4()
    second_claim = uuid.uuid4()
    scope = {
        "version": 1,
        "all_teams": True,
        "history_days": 90,
        "simulations": 1000,
    }
    try:
        async with session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO remaining_metric_runs (
                        id, org_id, family, generation, scope_key,
                        generation_seed, status
                    )
                    VALUES (
                        CAST(:run_id AS uuid), CAST(:org_id AS uuid), 'capacity',
                        'generation-v2', 'all-teams', 4321, 'running'
                    )
                    """
                ),
                {"run_id": str(run_id), "org_id": str(org_id)},
            )
            await session.execute(
                text(
                    """
                    INSERT INTO remaining_metric_partitions (
                        id, run_id, ordinal, scope, status, claim_token,
                        lease_expires_at
                    )
                    VALUES (
                        CAST(:partition_id AS uuid), CAST(:run_id AS uuid), 1,
                        CAST(:scope AS jsonb), 'running',
                        CAST(:claim_token AS uuid),
                        statement_timestamp() + interval '10 minutes'
                    )
                    """
                ),
                {
                    "partition_id": str(partition_id),
                    "run_id": str(run_id),
                    "scope": worker_metrics._canonical_json(scope),
                    "claim_token": str(first_claim),
                },
            )
            await session.commit()
            request = worker_metrics.RemainingMetricsExecutionRequest(
                operation="partition",
                run_id=run_id,
                partition_id=partition_id,
            )
            execution = await worker_metrics._load_remaining_execution(session, request)
            assert await worker_metrics._reserve_execution(session, execution) == (
                "execute"
            )
            if expected_state == "ambiguous":
                await session.execute(
                    text(
                        """
                        UPDATE metric_compatibility_executions
                        SET state = 'ambiguous'
                        WHERE id = CAST(:id AS uuid)
                        """
                    ),
                    {"id": str(execution.id)},
                )
                await session.commit()
            repair = worker_metrics.MetricExecutionRepairRequest(
                expected_state=expected_state,
                expected_attempt_count=1,
                resolution="retry_safe",
                review_evidence="verified no output exists for attempt one",
            )
            with pytest.raises(HTTPException) as active:
                await worker_metrics._repair_execution(session, execution.id, repair)
            assert active.value.status_code == 409
            await session.rollback()

            await session.execute(
                text(
                    """
                    UPDATE remaining_metric_partitions
                    SET claim_token = CAST(:claim_token AS uuid),
                        lease_expires_at = statement_timestamp() + interval '10 minutes'
                    WHERE id = CAST(:partition_id AS uuid)
                    """
                ),
                {
                    "claim_token": str(second_claim),
                    "partition_id": str(partition_id),
                },
            )
            await session.commit()
            repaired = await worker_metrics._repair_execution(
                session, execution.id, repair
            )
            assert repaired["state"] == "retry_authorized"

            reclaimed = await worker_metrics._load_remaining_execution(session, request)
            assert await worker_metrics._reserve_execution(session, reclaimed) == (
                "execute"
            )
            replay = await worker_metrics._repair_execution(
                session, execution.id, repair
            )
            assert replay == {
                "status": "already_applied",
                "execution_id": str(execution.id),
                "state": "executing",
            }
            attempt_count = (
                await session.execute(
                    text(
                        """
                        SELECT attempt_count
                        FROM metric_compatibility_executions
                        WHERE id = CAST(:id AS uuid)
                        """
                    ),
                    {"id": str(execution.id)},
                )
            ).scalar_one()
            assert attempt_count == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_orphaned_executing_row_reports_ambiguous_once_original_claim_is_dead() -> (
    None
):
    """CHAOS-4361: a ledger row stuck at "executing" (the owning api process
    died -- kernel OOM, container restart -- before any of
    worker_metrics._execute's exception handlers could run) must NOT be
    reported as transient "executing" once the Go-side claim that started it
    has provably moved on (a fresh claim_token/lease already exists). Go's
    classifyCompatibilityError treats "executing" as
    ErrCompatibilityAmbiguousRefused (retryable forever) and "ambiguous" as
    ErrCompatibilityAmbiguousStuck (durably fails the partition permanently).
    Before this ticket's fix, every retry against a dead-claim "executing"
    row loops until River discards the job after 5 attempts, leaving the
    partition 'failed' with NO failure_reason -- the exact 2026-08-27
    incident (ambiguous_refused x5, no durable trace)."""
    assert _TEST_URI is not None
    engine = create_async_engine(_TEST_URI)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    run_id = uuid.uuid4()
    partition_id = uuid.uuid4()
    org_id = uuid.uuid4()
    first_claim = uuid.uuid4()
    second_claim = uuid.uuid4()
    try:
        async with session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO daily_metrics_runs (
                        id, org_id, target_day, generation, status,
                        finalization_status, created_at, updated_at
                    ) VALUES (
                        CAST(:run_id AS uuid), CAST(:org_id AS uuid),
                        '2026-08-27', 'daily-v1', 'running', 'pending',
                        now(), now()
                    )
                    """
                ),
                {"run_id": str(run_id), "org_id": str(org_id)},
            )
            await session.execute(
                text(
                    """
                    INSERT INTO daily_metrics_partitions (
                        id, run_id, ordinal, repo_ids, status, claim_token,
                        lease_expires_at, attempt_count, created_at, updated_at
                    ) VALUES (
                        CAST(:partition_id AS uuid), CAST(:run_id AS uuid), 0,
                        '[]'::jsonb, 'running', CAST(:claim_token AS uuid),
                        statement_timestamp() + interval '10 minutes',
                        1, now(), now()
                    )
                    """
                ),
                {
                    "partition_id": str(partition_id),
                    "run_id": str(run_id),
                    "claim_token": str(first_claim),
                },
            )
            await session.commit()

            request = worker_metrics.DailyMetricsExecutionRequest(
                operation="partition",
                run_id=run_id,
                partition_id=partition_id,
            )
            execution = await worker_metrics._load_daily_execution(session, request)
            assert await worker_metrics._reserve_execution(session, execution) == (
                "execute"
            )
            await session.commit()

            # Simulate the api process dying mid-execution: nothing ever
            # calls _mark_ambiguous/_mark_retry_authorized, so the row stays
            # "executing" forever. Go's PartitionHandler.Work releases the
            # dead claim and reclaims the partition with a FRESH claim_token
            # -- the original claim (first_claim) is now provably dead.
            await session.execute(
                text(
                    """
                    UPDATE daily_metrics_partitions
                    SET claim_token = CAST(:claim_token AS uuid),
                        lease_expires_at = statement_timestamp() + interval '10 minutes',
                        attempt_count = attempt_count + 1
                    WHERE id = CAST(:partition_id AS uuid)
                    """
                ),
                {
                    "claim_token": str(second_claim),
                    "partition_id": str(partition_id),
                },
            )
            await session.commit()

            reclaimed = await worker_metrics._load_daily_execution(session, request)
            assert reclaimed.id == execution.id
            with pytest.raises(HTTPException) as retry:
                await worker_metrics._reserve_execution(session, reclaimed)
            assert retry.value.status_code == 409
            detail = retry.value.detail
            assert isinstance(detail, dict)
            assert detail["reason"] == "ambiguous_refused"
            # The fix: a dead-claim "executing" row must be reported as
            # "ambiguous" (Go's ErrCompatibilityAmbiguousStuck -- durably
            # fails the partition, requires a human /repair call) rather
            # than "executing" (Go's ErrCompatibilityAmbiguousRefused --
            # retried forever, since Go believes the original claim might
            # still finish on its own).
            assert detail["state"] == "ambiguous"

            ledger_state = (
                await session.execute(
                    text(
                        """
                        SELECT state FROM metric_compatibility_executions
                        WHERE id = CAST(:id AS uuid)
                        """
                    ),
                    {"id": str(execution.id)},
                )
            ).scalar_one()
            # The ledger row's OWN state column is untouched -- this is a
            # reporting-only fix, not a state mutation. A human /repair call
            # still resolves the ledger row itself.
            assert ledger_state == "executing"
    finally:
        await engine.dispose()
