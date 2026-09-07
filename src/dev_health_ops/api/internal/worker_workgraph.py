"""Operator repair for the work-graph execution ledger.

CHAOS-3092 (leftovers): this router used to also serve POST /execute, the
fenced HTTP compatibility endpoint the Go worker called to run
investment.materialize's compute in-process. That caller no longer exists --
cmd/dev-health-worker/workgraph.go wires investment.materialize entirely
natively (buildNativeInvestmentExecutor -> investment.NewNativeExecutor,
internal/jobs/investment/nativeexecutor.go), calling
workgraph.CompatibilityExecutor.Execute directly with no HTTP round trip --
so /execute, its request/scope model, and every helper that only existed to
run a compatibility subprocess for it were deleted along with the Celery task
they invoked (work_graph_tasks.py's run_investment_materialize).

What remains, and stays live: an ambiguous work_graph_execution_requests /
work_graph_execution_ledger row can still occur when the NATIVE executor's
outcome can't be positively classified as sent/not-sent (see
internal/jobs/workgraph/handler.go's materializeHandler.work), and this
repair endpoint is the only way an operator clears one --
`dev-health-workerctl workgraph repair` (cmd/dev-health-workerctl/
repair_workgraph.go) POSTs here.
"""

from __future__ import annotations

import json
import uuid
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.internal.worker_auth import authorize_workgraph_repair

router = APIRouter(prefix="/internal/worker/workgraph/v1", include_in_schema=False)

_MAX_EVIDENCE_BYTES = 4096


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RepairRequest(_Strict):
    expected_attempt_count: int = Field(ge=1)
    resolution: Literal["retry_safe", "confirm_succeeded"]
    review_evidence: str = Field(min_length=1, max_length=2048)
    output_evidence: dict[str, Any] | None = None


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _evidence(value: object) -> dict[str, Any]:
    encoded = _canonical(value)
    if len(encoded.encode()) > _MAX_EVIDENCE_BYTES:
        raise ValueError("execution evidence exceeds durable bound")
    return json.loads(encoded)


@router.post("/executions/{request_id}/repair")
async def repair(
    request_id: uuid.UUID,
    request: RepairRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    authorize_workgraph_repair(authorization)
    if len(request.review_evidence.encode()) > 2048:
        raise HTTPException(status_code=422, detail="Repair evidence is too large")
    if (request.resolution == "confirm_succeeded") != (
        request.output_evidence is not None
    ):
        raise HTTPException(status_code=422, detail="Resolution evidence is invalid")
    if request.output_evidence is not None:
        try:
            _evidence(request.output_evidence)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    row = (
        (
            await session.execute(
                text(
                    """
                SELECT request.id, request.state, request.claim_token,
                       request.lease_expires_at, ledger.state AS ledger_state,
                       ledger.attempt_count
                FROM work_graph_execution_requests AS request
                JOIN work_graph_execution_ledger AS ledger ON ledger.request_id = request.id
                WHERE request.id = CAST(:id AS uuid)
                FOR UPDATE OF request, ledger
                """
                ),
                {"id": str(request_id)},
            )
        )
        .mappings()
        .first()
    )
    if (
        row is None
        or row["state"] != "ambiguous"
        or row["ledger_state"] != "ambiguous"
        or int(row["attempt_count"]) != request.expected_attempt_count
        or row["claim_token"] is not None
        or row["lease_expires_at"] is not None
    ):
        raise HTTPException(
            status_code=409, detail="Only unleased ambiguous executions can be repaired"
        )
    evidence = (
        _canonical(request.output_evidence)
        if request.output_evidence is not None
        else None
    )
    resolution_state = (
        "succeeded" if request.resolution == "confirm_succeeded" else "pending"
    )
    ledger_state = (
        "succeeded" if request.resolution == "confirm_succeeded" else "repaired"
    )
    await session.execute(
        text(
            """
            INSERT INTO work_graph_execution_repairs (
                id, request_id, expected_attempt_count, resolution, review_evidence, output_evidence
            ) VALUES (
                CAST(:repair_id AS uuid), CAST(:request_id AS uuid), :attempt_count,
                :resolution, :review_evidence, CAST(:output_evidence AS jsonb)
            )
            """
        ),
        {
            "repair_id": str(uuid.uuid4()),
            "request_id": str(request_id),
            "attempt_count": request.expected_attempt_count,
            "resolution": request.resolution,
            "review_evidence": request.review_evidence,
            "output_evidence": evidence,
        },
    )
    await session.execute(
        text(
            "UPDATE work_graph_execution_requests SET state = :state, updated_at = statement_timestamp() WHERE id = CAST(:id AS uuid) AND state = 'ambiguous'"
        ),
        {"state": resolution_state, "id": str(request_id)},
    )
    await session.execute(
        text(
            "UPDATE work_graph_execution_ledger SET state = :state, output_evidence = CAST(:evidence AS jsonb), failure_detail = NULL, completed_at = CASE WHEN :state = 'succeeded' THEN statement_timestamp() ELSE NULL END WHERE request_id = CAST(:id AS uuid) AND state = 'ambiguous'"
        ),
        {"state": ledger_state, "evidence": evidence, "id": str(request_id)},
    )
    await session.commit()
    return {"status": "repaired", "request_id": str(request_id)}
