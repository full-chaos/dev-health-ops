"""Live PostgreSQL checks for the metric compatibility execution fence."""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops.api.internal import worker_metrics

_TEST_URI = os.getenv("METRIC_BRIDGE_POSTGRES_TEST_URI")
pytestmark = pytest.mark.skipif(
    not _TEST_URI, reason="METRIC_BRIDGE_POSTGRES_TEST_URI is not configured"
)


# CHAOS-3092 (2026-09-07): these two helpers reproduce, for THIS TEST FILE
# only, the exact SELECT ... FOR UPDATE + _execution_from_row shape that
# worker_metrics._load_daily_execution (and the DailyMetricsExecutionRequest
# it consumed) used to provide before both were deleted outright -- the Go
# daily worker's Python compatibility bridge (execute_daily_metrics) has no
# callers left, every daily family being native Go now. The redrive/repair
# ledger surface these tests actually exercise
# (_reserve_execution/_mark_ambiguous/_original_claim_is_active) all stay
# live -- they still operate on historical daily ledger rows -- so these
# tests are adjusted to seed and load those rows directly rather than
# through the deleted route. The operator repair and redrive verbs are
# Go-native (internal/jobs/repair); their tests live there.
async def _daily_partition_execution(
    session: Any, run_id: uuid.UUID, partition_id: uuid.UUID
) -> worker_metrics._Execution:
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
        {"run_id": str(run_id), "partition_id": str(partition_id)},
    )
    row = result.mappings().first()
    assert row is not None, "daily metrics lease is absent or expired"
    return worker_metrics._execution_from_row(
        worker_kind="daily",
        operation="partition",
        row=row,
        partition_id=partition_id,
    )


async def _daily_finalize_execution(
    session: Any, run_id: uuid.UUID
) -> worker_metrics._Execution:
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
        {"run_id": str(run_id)},
    )
    row = result.mappings().first()
    assert row is not None, "daily metrics finalize lease is absent or expired"
    return worker_metrics._execution_from_row(
        worker_kind="daily",
        operation="finalize",
        row=row,
        partition_id=None,
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
            # The reclaimed claim is refused and nothing re-executes: the row
            # stays ambiguous at the exact attempt until an operator repairs it.
            assert ledger == {"state": "ambiguous", "attempt_count": 1}
            assert effects == ["append-output"]
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

            execution = await _daily_partition_execution(session, run_id, partition_id)
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

            reclaimed = await _daily_partition_execution(session, run_id, partition_id)
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


@pytest.mark.asyncio
async def test_finalize_execution_is_skipped_not_reexecuted_for_the_same_identity_after_reclaim() -> (
    None
):
    """Go<->bridge ledger-identity contract (pins the exact assumption
    CHAOS-4405's finalize-redrive design got wrong, per the finding posted
    on CHAOS-4405/#1971): a daily/finalize execution's identity is
    uuid5(run_id, family, generation, scope_digest) -- NOT anything derived
    from daily_metrics_runs.status/finalization_status. Once that identity
    has reached 'succeeded', reclaiming the run's finalization lease (a
    fresh finalization_claim_token/finalization_lease_expires_at, exactly
    what ClaimFinalize does on every redrive, including a hypothetical
    status='succeeded'->'running' reset that leaves generation unchanged)
    does NOT create a new execution to run: _reserve_execution finds the
    SAME row already 'succeeded' and returns "skipped" -- the real work
    (run_daily_metrics_finalize) is never invoked again, and attempt_count
    never advances. Any caller that resets a run's Go-side state expecting
    a "succeeded" identity to redo real work MUST first change generation
    (or otherwise repair the ledger row itself, CHAOS-4409's own pattern) --
    a bare Go-side status reset is a guaranteed silent no-op through this
    endpoint, not a retry."""
    assert _TEST_URI is not None
    engine = create_async_engine(_TEST_URI)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    org_id = uuid.uuid4()
    run_id = uuid.uuid4()
    first_claim = uuid.uuid4()
    try:
        async with session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO daily_metrics_runs (
                        id, org_id, target_day, generation, status,
                        finalization_status, finalization_claim_token,
                        finalization_lease_expires_at, created_at, updated_at
                    ) VALUES (
                        CAST(:run_id AS uuid), CAST(:org_id AS uuid),
                        '2026-08-20', :generation, 'running', 'running',
                        CAST(:claim_token AS uuid),
                        statement_timestamp() + interval '10 minutes',
                        now(), now()
                    )
                    """
                ),
                {
                    "run_id": str(run_id),
                    "org_id": str(org_id),
                    "generation": f"daily-v1:{run_id}",
                    "claim_token": str(first_claim),
                },
            )
            await session.commit()

            first_execution = await _daily_finalize_execution(session, run_id)
            assert await worker_metrics._reserve_execution(
                session, first_execution
            ) == ("execute")
            await worker_metrics._mark_succeeded(session, first_execution, {})

            succeeded_state = (
                await session.execute(
                    text(
                        "SELECT state, attempt_count FROM metric_compatibility_executions WHERE id = CAST(:id AS uuid)"
                    ),
                    {"id": str(first_execution.id)},
                )
            ).one()
            assert succeeded_state.state == "succeeded"
            attempt_count_after_success = succeeded_state.attempt_count

            # Reclaim the run's finalization lease with a FRESH claim token --
            # exactly what ClaimFinalize does on every redrive -- WITHOUT
            # bumping generation. This is the precise shape a Go-side
            # status='succeeded'->'running' reset (CHAOS-4405) produces.
            await session.execute(
                text(
                    """
                    UPDATE daily_metrics_runs
                    SET finalization_claim_token = CAST(:claim_token AS uuid),
                        finalization_lease_expires_at = statement_timestamp() + interval '10 minutes'
                    WHERE id = CAST(:run_id AS uuid)
                    """
                ),
                {"claim_token": str(uuid.uuid4()), "run_id": str(run_id)},
            )
            await session.commit()

            reclaimed_execution = await _daily_finalize_execution(session, run_id)
            # Same identity: the reclaim did not change run_id, family,
            # generation, or scope_digest.
            assert reclaimed_execution.id == first_execution.id

            outcome = await worker_metrics._reserve_execution(
                session, reclaimed_execution
            )
            assert outcome == "skipped"

            unchanged_state = (
                await session.execute(
                    text(
                        "SELECT state, attempt_count FROM metric_compatibility_executions WHERE id = CAST(:id AS uuid)"
                    ),
                    {"id": str(first_execution.id)},
                )
            ).one()
            assert unchanged_state.state == "succeeded"
            assert unchanged_state.attempt_count == attempt_count_after_success
    finally:
        await engine.dispose()
