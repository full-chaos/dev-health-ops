"""Operation rollout registry access (CHAOS-4366 Wave 0).

This is the Python-edge-side read/write layer over the three tables added
by alembic 0114 (:mod:`dev_health_ops.models.go_api_registry`). It does NOT
yet route any request -- Wave 0 builds proof infrastructure only; no
resolver is ported to Go, so nothing calls :func:`lookup_routing_state` from
a live request path yet. This module exists so:

1. The comparator (CHAOS-4366 deliverable 5) has a real place to record
   :class:`~dev_health_ops.models.go_api_registry.ProofRun` rows instead of
   writing ad hoc SQL.
2. The eventual operation router (a later wave, once resolvers start
   porting) has a tested lookup function to call rather than building one
   under deadline pressure alongside the first live cutover.

Every function is intentionally narrow and explicit about failure: a lookup
that cannot reach the database raises, it does not silently return "no
routing state" (which reads identically to "not registered yet" -- see the
``result`` label distinction in ``go_api_registry_telemetry``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.graphql.go_api_registry_telemetry import (
    GO_API_CANDIDATE_BUILD_REGISTERED_TOTAL,
    GO_API_PROOF_RUN_RECORDED_TOTAL,
    GO_API_REGISTRY_LOOKUP_TOTAL,
)
from dev_health_ops.models.go_api_registry import (
    MODES,
    OWNERS,
    STAGES,
    TERMINAL_STATES,
    CandidateBuild,
    ProofRun,
    RoutingState,
)

logger = logging.getLogger(__name__)

__all__ = [
    "lookup_routing_state",
    "register_candidate_build",
    "record_proof_run",
]


async def lookup_routing_state(
    session: AsyncSession,
    *,
    schema_digest: str,
    document_digest: str,
    selected_operation: str,
) -> RoutingState | None:
    """Read the current routing decision for one operation triple.

    Returns ``None`` when no :class:`RoutingState` row exists -- an
    unregistered operation, which the caller must treat as ``mode=python``
    (the safe default; nothing routes to Go until a row explicitly says so).
    Raises on any database failure rather than treating it as a miss: an
    unreachable registry is an incident, not "no rollout yet" (see
    ``go_api_registry_telemetry``'s ``result`` label doc).
    """
    try:
        result = await session.execute(
            select(RoutingState).where(
                RoutingState.schema_digest == schema_digest,
                RoutingState.document_digest == document_digest,
                RoutingState.selected_operation == selected_operation,
            )
        )
        row = result.scalar_one_or_none()
    except Exception:
        GO_API_REGISTRY_LOOKUP_TOTAL.labels(result="error", mode="unknown").inc()
        logger.exception(
            "go_api_registry.lookup_failed",
            extra={
                "schema_digest": schema_digest,
                "document_digest": document_digest,
                "selected_operation": selected_operation,
            },
        )
        raise

    if row is None:
        GO_API_REGISTRY_LOOKUP_TOTAL.labels(result="miss", mode="python").inc()
        return None

    GO_API_REGISTRY_LOOKUP_TOTAL.labels(result="hit", mode=row.mode).inc()
    return row


async def register_candidate_build(
    session: AsyncSession,
    *,
    schema_digest: str,
    document_digest: str,
    selected_operation: str,
    candidate_build: str,
) -> None:
    """Register a candidate build for an operation triple, idempotently.

    :class:`CandidateBuild` is immutable and append-only (see its docstring):
    registering the same build twice is a no-op, never an error, and this
    function never issues an UPDATE against the table.
    """
    stmt = (
        pg_insert(CandidateBuild)
        .values(
            schema_digest=schema_digest,
            document_digest=document_digest,
            selected_operation=selected_operation,
            candidate_build=candidate_build,
            registered_at=datetime.now(timezone.utc),
        )
        .on_conflict_do_nothing(
            index_elements=[
                "schema_digest",
                "document_digest",
                "selected_operation",
                "candidate_build",
            ]
        )
        .returning(CandidateBuild.candidate_build)
    )
    result = await session.execute(stmt)
    inserted = result.first() is not None
    GO_API_CANDIDATE_BUILD_REGISTERED_TOTAL.labels(
        outcome="inserted" if inserted else "already_registered"
    ).inc()


async def record_proof_run(
    session: AsyncSession,
    *,
    schema_digest: str,
    document_digest: str,
    selected_operation: str,
    candidate_build: str,
    request_identity: str,
    stage: str,
    terminal_state: str,
    baseline_response_ref: str | None = None,
    candidate_response_ref: str | None = None,
    side_effect_digest: str | None = None,
    data_watermark: str | None = None,
    org_id: str | None = None,
    proof_run_id: UUID | None = None,
) -> ProofRun:
    """Record one proof-gate outcome. Raises ``ValueError`` before touching
    the database on an out-of-vocabulary ``stage``/``terminal_state`` --
    the DB CHECK constraints are the backstop, this is the fast, specific
    failure for a programming error at the call site.
    """
    if stage not in STAGES:
        raise ValueError(
            f"invalid proof-run stage: {stage!r}, expected one of {STAGES}"
        )
    if terminal_state not in TERMINAL_STATES:
        raise ValueError(
            f"invalid proof-run terminal_state: {terminal_state!r}, "
            f"expected one of {TERMINAL_STATES}"
        )
    if stage == "shadow" and data_watermark is None:
        raise ValueError(
            "a stage='shadow' proof run requires data_watermark "
            "(CHAOS-4381 parity rule 4)"
        )

    proof_run = ProofRun(
        id=proof_run_id or uuid4(),
        schema_digest=schema_digest,
        document_digest=document_digest,
        selected_operation=selected_operation,
        candidate_build=candidate_build,
        request_identity=request_identity,
        stage=stage,
        terminal_state=terminal_state,
        baseline_response_ref=baseline_response_ref,
        candidate_response_ref=candidate_response_ref,
        side_effect_digest=side_effect_digest,
        data_watermark=data_watermark,
        org_id=org_id,
        observed_at=datetime.now(timezone.utc),
    )
    session.add(proof_run)
    await session.flush()

    GO_API_PROOF_RUN_RECORDED_TOTAL.labels(
        stage=stage, terminal_state=terminal_state
    ).inc()
    return proof_run


# OWNERS/MODES re-exported for callers that need to validate a routing-state
# write without importing the models module directly.
__all__ += ["OWNERS", "MODES"]
