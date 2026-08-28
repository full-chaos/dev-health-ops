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


@pytest.mark.asyncio
async def test_bulk_redrive_authorizes_retry_for_ambiguous_daily_partitions() -> None:
    """CHAOS-4304: a daily/partition ledger row stuck 'ambiguous' (a
    progress-having failure) must not be permanently unrecomputable once an
    operator has scoped its run for redrive. Before
    _bulk_redrive_ambiguous_executions existed, the ONLY way to move this row
    was a per-execution-id /repair call an operator would have to discover
    the execution id for by hand; every _reserve_execution against the
    identical (run, partition, family, generation, scope_digest) identity
    hit the same 409 ambiguous_refused forever -- "a failed partition can
    never be recomputed" for that generation.

    Two ledger rows are seeded: one whose original claim is provably dead
    (the redrive must repair it), one whose original claim is STILL live
    (the redrive must refuse it exactly as a single /repair call would,
    leaving it ambiguous)."""
    assert _TEST_URI is not None
    engine = create_async_engine(_TEST_URI)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    org_id = uuid.uuid4()
    stale_run_id, live_run_id = uuid.uuid4(), uuid.uuid4()
    stale_partition_id, live_partition_id = uuid.uuid4(), uuid.uuid4()
    try:
        async with session_factory() as session:
            for run_id, partition_id in (
                (stale_run_id, stale_partition_id),
                (live_run_id, live_partition_id),
            ):
                claim = uuid.uuid4()
                await session.execute(
                    text(
                        """
                        INSERT INTO daily_metrics_runs (
                            id, org_id, target_day, generation, status,
                            finalization_status, created_at, updated_at
                        ) VALUES (
                            CAST(:run_id AS uuid), CAST(:org_id AS uuid),
                            '2026-08-20', :generation, 'running', 'pending',
                            now(), now()
                        )
                        """
                    ),
                    {
                        "run_id": str(run_id),
                        "org_id": str(org_id),
                        "generation": f"daily-v1:{run_id}",
                    },
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
                        "claim_token": str(claim),
                    },
                )
            await session.commit()

            stale_request = worker_metrics.DailyMetricsExecutionRequest(
                operation="partition",
                run_id=stale_run_id,
                partition_id=stale_partition_id,
            )
            live_request = worker_metrics.DailyMetricsExecutionRequest(
                operation="partition",
                run_id=live_run_id,
                partition_id=live_partition_id,
            )
            stale_execution = await worker_metrics._load_daily_execution(
                session, stale_request
            )
            live_execution = await worker_metrics._load_daily_execution(
                session, live_request
            )
            assert await worker_metrics._reserve_execution(
                session, stale_execution
            ) == ("execute")
            assert await worker_metrics._reserve_execution(session, live_execution) == (
                "execute"
            )
            # Both fail with real progress written -- a genuine ambiguous
            # outcome, not the zero-progress retry_authorized fast path.
            await worker_metrics._mark_ambiguous(
                session,
                stale_execution,
                "resource_exhausted: simulated OOM after partial write",
            )
            await worker_metrics._mark_ambiguous(
                session,
                live_execution,
                "resource_exhausted: simulated OOM after partial write",
            )

            # Only the stale run's claim moves on (Go reclaims with a fresh
            # token, proving the original claim is dead). The live run's
            # claim is left exactly as _reserve_execution set it --
            # indistinguishable from a claim still legitimately in flight.
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
                    "claim_token": str(uuid.uuid4()),
                    "partition_id": str(stale_partition_id),
                },
            )
            await session.commit()

            # RED baseline (true today, unchanged by this fix): the identical
            # identity is refused forever, never "skipped".
            reclaimed_stale = await worker_metrics._load_daily_execution(
                session, stale_request
            )
            with pytest.raises(HTTPException) as before:
                await worker_metrics._reserve_execution(session, reclaimed_stale)
            assert before.value.status_code == 409
            assert before.value.detail["reason"] == "ambiguous_refused"  # type: ignore[index]

            outcome = await worker_metrics._bulk_redrive_ambiguous_executions(
                session, [stale_run_id, live_run_id], "chaos-4358 operator redrive test"
            )
            await session.commit()
            assert outcome == {"repaired": 1, "skipped_claim_active": 1}

            # GREEN: the stale row is now retry_authorized, so the identical
            # identity actually executes instead of hitting the wall again.
            reclaimed_stale_again = await worker_metrics._load_daily_execution(
                session, stale_request
            )
            assert await worker_metrics._reserve_execution(
                session, reclaimed_stale_again
            ) == ("execute")

            # The live-claim row is untouched: still ambiguous, still refused.
            reclaimed_live = await worker_metrics._load_daily_execution(
                session, live_request
            )
            with pytest.raises(HTTPException) as still_refused:
                await worker_metrics._reserve_execution(session, reclaimed_live)
            assert still_refused.value.status_code == 409
            live_state = (
                await session.execute(
                    text(
                        "SELECT state FROM metric_compatibility_executions WHERE id = CAST(:id AS uuid)"
                    ),
                    {"id": str(live_execution.id)},
                )
            ).scalar_one()
            assert live_state == "ambiguous"
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

            finalize_request = worker_metrics.DailyMetricsExecutionRequest(
                operation="finalize", run_id=run_id
            )
            first_execution = await worker_metrics._load_daily_execution(
                session, finalize_request
            )
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

            reclaimed_execution = await worker_metrics._load_daily_execution(
                session, finalize_request
            )
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


@pytest.mark.asyncio
async def test_bulk_redrive_authorizes_retry_for_ambiguous_daily_finalize() -> None:
    """CHAOS-4409: a daily/finalize ledger row can get stuck 'ambiguous' or
    stuck-'executing' the exact same way a partition row can (the owning api
    process died before any exception handler ran, or a progress-having
    finalize failure) -- and _bulk_redrive_ambiguous_executions used to be
    blind to it entirely (hardcoded `operation = 'partition'`). Prod hit
    this for real: 13 daily_metrics_runs stuck 'running' with 100%
    partitions succeeded (the CHAOS-4389 stranded-finalize shape) whose
    finalize ledger row was stuck ambiguous/executing from the original
    stranding -- `daily-finalize --run` answered JobCancelError
    ambiguous_refused on every one of them, forever, because nothing ever
    repaired the finalize row specifically.

    RED baseline (true before this fix, reproduced here first): the bulk
    redrive scoped to this run reports NOTHING repaired for a stuck
    finalize row, and a reclaimed execution against the identical identity
    still hits the same 409 ambiguous_refused wall. GREEN (after the fix):
    the finalize row is repaired to retry_authorized exactly like a
    partition row would be, under the identical claim-liveness gate
    (_original_claim_is_active's operation != 'partition' branch, which
    already reads daily_metrics_runs.finalization_status/
    finalization_claim_token/finalization_lease_expires_at -- this test is
    the first to actually exercise it via the bulk path)."""
    assert _TEST_URI is not None
    engine = create_async_engine(_TEST_URI)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    org_id = uuid.uuid4()
    run_id = uuid.uuid4()
    original_claim = uuid.uuid4()
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
                    "claim_token": str(original_claim),
                },
            )
            # A run needs at least one succeeded partition for this shape to
            # be reachable in practice (100% partitions succeeded is what
            # made Go dispatch metrics.daily_finalize in the first place) --
            # not load-bearing for this test itself, included for realism.
            await session.execute(
                text(
                    """
                    INSERT INTO daily_metrics_partitions (
                        id, run_id, ordinal, repo_ids, status, claim_token,
                        lease_expires_at, attempt_count, created_at, updated_at
                    ) VALUES (
                        gen_random_uuid(), CAST(:run_id AS uuid), 0,
                        '[]'::jsonb, 'succeeded', NULL,
                        NULL, 1, now(), now()
                    )
                    """
                ),
                {"run_id": str(run_id)},
            )
            await session.commit()

            finalize_request = worker_metrics.DailyMetricsExecutionRequest(
                operation="finalize", run_id=run_id
            )
            execution = await worker_metrics._load_daily_execution(
                session, finalize_request
            )
            assert await worker_metrics._reserve_execution(session, execution) == (
                "execute"
            )
            # A progress-having failure -- the api process died mid-write, or
            # Finalize itself raised after real output landed.
            await worker_metrics._mark_ambiguous(
                session, execution, "executor raised RuntimeError"
            )

            # The original claim is now provably dead: Go reclaims with a
            # fresh finalization_claim_token, exactly as ClaimFinalize does
            # on a retry.
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

            # RED baseline (true before CHAOS-4409, reproduced here first):
            # the identical identity is refused forever, never "skipped".
            reclaimed = await worker_metrics._load_daily_execution(
                session, finalize_request
            )
            with pytest.raises(HTTPException) as before:
                await worker_metrics._reserve_execution(session, reclaimed)
            assert before.value.status_code == 409
            assert before.value.detail["reason"] == "ambiguous_refused"  # type: ignore[index]

            outcome = await worker_metrics._bulk_redrive_ambiguous_executions(
                session, [run_id], "chaos-4409 operator redrive test", ["finalize"]
            )
            await session.commit()
            assert outcome == {"repaired": 1, "skipped_claim_active": 0}

            # GREEN: the finalize row is now retry_authorized, so the
            # identical identity actually executes instead of hitting the
            # wall again -- unblocking daily-finalize --run/--all-complete
            # for a run whose finalize ledger row was the thing stranding
            # it, not (only) the partition table.
            reclaimed_again = await worker_metrics._load_daily_execution(
                session, finalize_request
            )
            assert await worker_metrics._reserve_execution(
                session, reclaimed_again
            ) == ("execute")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_bulk_redrive_operation_scope_never_touches_the_other_operations_row() -> (
    None
):
    """CHAOS-4409 (codex review, round 1, P1): daily-redrive and
    daily-finalize share this ONE bulk-repair endpoint, but their
    review_evidence means different things (partition output vs. finalize
    output). A daily-redrive call (operations=["partition"], its default)
    must NEVER move a run's finalize ledger row to retry_authorized under
    partition-scoped evidence -- that would authorize a later, unrelated
    finalize attempt to redrive it without anyone having reviewed finalize
    output specifically. Seeds a run with a stuck (dead-claim) 'ambiguous'
    FINALIZE row only, calls the bulk repair scoped to operations=
    ["partition"] (daily-redrive's exact shape), and asserts nothing moves:
    zero repaired, and the finalize row's own state is untouched."""
    assert _TEST_URI is not None
    engine = create_async_engine(_TEST_URI)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    org_id = uuid.uuid4()
    run_id = uuid.uuid4()
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
                    "claim_token": str(uuid.uuid4()),
                },
            )
            await session.commit()

            finalize_request = worker_metrics.DailyMetricsExecutionRequest(
                operation="finalize", run_id=run_id
            )
            execution = await worker_metrics._load_daily_execution(
                session, finalize_request
            )
            assert await worker_metrics._reserve_execution(session, execution) == (
                "execute"
            )
            await worker_metrics._mark_ambiguous(
                session, execution, "simulated dead claim"
            )

            # Dead claim: a fresh finalization_claim_token, exactly as
            # ClaimFinalize does on a retry.
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

            # daily-redrive's exact call shape: operations defaults to
            # ["partition"] when omitted -- pass it explicitly here to pin
            # the behavior even if that default ever changes.
            outcome = await worker_metrics._bulk_redrive_ambiguous_executions(
                session, [run_id], "chaos-4358 partition-only evidence", ["partition"]
            )
            await session.commit()
            assert outcome == {"repaired": 0, "skipped_claim_active": 0}

            untouched_state = (
                await session.execute(
                    text(
                        "SELECT state FROM metric_compatibility_executions WHERE id = CAST(:id AS uuid)"
                    ),
                    {"id": str(execution.id)},
                )
            ).scalar_one()
            assert untouched_state == "ambiguous"
    finally:
        await engine.dispose()
