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
            assert ledger == {"state": "ambiguous", "attempt_count": 2}
    finally:
        await engine.dispose()
