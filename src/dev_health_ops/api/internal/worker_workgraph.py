"""Fenced compatibility executor for dormant work-graph River handlers.

Only the Go worker may call this endpoint and it supplies only the durable
request id plus its current lease token. Scope, LLM controls, and the reviewed
operation are reloaded from PostgreSQL; no request can select a module, prompt,
model, credential, or database URL.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import uuid
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.dependencies import get_postgres_session_dep
from dev_health_ops.api.internal.worker_auth import (
    authorize_worker_bridge,
    authorize_workgraph_repair,
)

router = APIRouter(prefix="/internal/worker/workgraph/v1", include_in_schema=False)
logger = logging.getLogger(__name__)

# r2 finding F1 (P2, codex, CHAOS-4438): these 3 Go kinds were deleted
# outright -- registerKind's switch (internal/jobrescue/registry.go) and
# addWorkgraphWorker's dispatch (cmd/dev-health-worker/workgraph.go) have no
# case for them any more. cmd/dev-health-worker/workgraph.go's own comment
# states "zero producers ever created a request row for them ...  confirmed
# exhaustively" -- meaning no live path can create a work_graph_execution_
# requests row of these kinds today. This bridge's mapping/execution code for
# them was nonetheless still live and reachable if one somehow existed
# (a manual DB write, a bug elsewhere, a future regression), which does not
# fail closed -- the exact same "unreachable today, but a code path that
# would still run it exists" gap the Go-side strand-repair fix (F1 in that
# round) was given the same defense-in-depth treatment for. Rejected
# explicitly and logged, rather than left to fall through as a merely
# "unsupported kind" -- an operator seeing this specific rejection knows
# immediately that the kind is deliberately retired, not malformed.
_RETIRED_KINDS = frozenset(
    {"investment.dispatch", "investment.chunk", "investment.finalize"}
)
_MAX_EVIDENCE_BYTES = 4096
_MAX_COMPATIBILITY_PROCESS_BYTES = 1024 * 1024
_PROCESS_TERMINATION_TIMEOUT_SECONDS = 1.0
_DISCONNECT_POLL_SECONDS = 0.1
_COMPATIBILITY_RUNNER_COMMAND = (
    sys.executable,
    "-m",
    "dev_health_ops.api.internal.worker_workgraph_runner",
)


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExecuteRequest(_Strict):
    request_id: uuid.UUID
    claim_token: uuid.UUID


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


def _scope_arguments(kind: str, scope: object, row: Any) -> dict[str, Any]:
    if not isinstance(scope, dict):
        raise ValueError("request scope must be an object")
    allowed: dict[str, set[str]] = {
        "workgraph.build": {
            "from_date",
            "to_date",
            "repo_id",
            "heuristic_window",
            "heuristic_confidence",
        },
        "investment.materialize": {
            "from_date",
            "to_date",
            "window_days",
            "repo_ids",
            "team_ids",
            "llm_provider",
            "force",
            "allow_unscoped",
            "llm_batch_mode",
            "llm_batch_min_items",
            "llm_batch_poll_interval_seconds",
            "llm_batch_timeout_seconds",
        },
        # investment.dispatch/chunk/finalize retired outright under
        # CHAOS-4438 (r2 finding F1) -- removed from this table so a call
        # that somehow bypasses execute()'s _RETIRED_KINDS guard still
        # fails closed here with "unsupported fields" rather than being
        # scoped and dispatched.
    }
    if kind not in allowed or set(scope) - allowed[kind]:
        raise ValueError("request scope contains unsupported fields")
    arguments = dict(scope)
    arguments["org_id"] = str(row["org_id"])
    if kind == "investment.materialize":
        arguments["llm_model"] = row["model_ref"] or None
        arguments["llm_concurrency"] = int(row["llm_concurrency"])
    return arguments


def _run_sync(kind: str, arguments: dict[str, Any]) -> dict[str, Any]:
    from dev_health_ops.workers import work_graph_tasks

    operations = {
        "workgraph.build": work_graph_tasks.run_work_graph_build.run,
        "investment.materialize": work_graph_tasks.run_investment_materialize.run,
        # investment.dispatch/chunk/finalize retired outright under
        # CHAOS-4438 (r2 finding F1) -- removed from this table, same
        # reasoning as _scope_arguments' allowed table above.
    }
    try:
        operation = operations[kind]
    except KeyError as exc:
        raise ValueError("unsupported work graph request kind") from exc
    result = operation(**arguments)
    if not isinstance(result, dict):
        raise ValueError("compatibility operation returned an invalid result")
    return result


async def _read_bounded_stream(
    stream: asyncio.StreamReader, maximum_bytes: int
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while chunk := await stream.read(64 * 1024):
        total += len(chunk)
        if total > maximum_bytes:
            raise ValueError("compatibility process output exceeds the durable bound")
        chunks.append(chunk)
    return b"".join(chunks)


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
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


async def _run_compatibility_process(
    kind: str, arguments: dict[str, Any]
) -> dict[str, Any]:
    payload = _canonical({"kind": kind, "arguments": arguments}).encode()
    if len(payload) > _MAX_COMPATIBILITY_PROCESS_BYTES:
        raise ValueError("compatibility process input exceeds the durable bound")
    process = await asyncio.create_subprocess_exec(
        *_COMPATIBILITY_RUNNER_COMMAND,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        # The child reserves stdout for the bounded JSON protocol and inherits
        # stderr so compatibility-task diagnostics remain visible to operators.
        stderr=None,
        start_new_session=os.name == "posix",
    )
    if process.stdin is None or process.stdout is None:
        await _terminate_process(process)
        raise RuntimeError("compatibility process pipes are unavailable")
    stdout_task = asyncio.create_task(
        _read_bounded_stream(process.stdout, _MAX_COMPATIBILITY_PROCESS_BYTES)
    )
    input_error: BrokenPipeError | ConnectionResetError | None = None
    try:
        try:
            process.stdin.write(payload)
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            # A fast non-zero child exit may close stdin before drain finishes.
            # Reap it and report the stable process-result contract below
            # instead of leaking a platform-dependent pipe exception.
            input_error = exc
        finally:
            process.stdin.close()
        stdout, return_code = await asyncio.gather(stdout_task, process.wait())
    except BaseException:
        await _terminate_process(process)
        raise
    finally:
        if not stdout_task.done():
            stdout_task.cancel()
        await asyncio.gather(stdout_task, return_exceptions=True)
    if return_code != 0:
        raise RuntimeError("compatibility process failed")
    if input_error is not None:
        raise RuntimeError("compatibility process rejected its input") from input_error
    try:
        decoded = json.loads(stdout)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("compatibility process returned invalid JSON") from exc
    if (
        not isinstance(decoded, dict)
        or set(decoded) != {"outcome"}
        or not isinstance(decoded["outcome"], dict)
    ):
        raise ValueError("compatibility process returned an invalid response")
    return decoded["outcome"]


async def _wait_for_client_disconnect(connection: Request) -> None:
    while not await connection.is_disconnected():
        await asyncio.sleep(_DISCONNECT_POLL_SECONDS)


async def _run_until_client_disconnect(
    connection: Request, kind: str, arguments: dict[str, Any]
) -> dict[str, Any]:
    execution = asyncio.create_task(_run_compatibility_process(kind, arguments))
    disconnect = asyncio.create_task(_wait_for_client_disconnect(connection))
    try:
        done, _pending = await asyncio.wait(
            {execution, disconnect}, return_when=asyncio.FIRST_COMPLETED
        )
        if execution in done:
            return execution.result()
        execution.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await execution
        raise ConnectionError("compatibility request client disconnected")
    finally:
        for task in (execution, disconnect):
            if not task.done():
                task.cancel()
        await asyncio.gather(execution, disconnect, return_exceptions=True)


async def _mark_ambiguous(
    session: AsyncSession, request: ExecuteRequest, detail: str
) -> None:
    request_result = await session.execute(
        text(
            """
            UPDATE work_graph_execution_requests
            SET state = 'ambiguous', claim_token = NULL, lease_expires_at = NULL,
                updated_at = statement_timestamp()
            WHERE id = CAST(:id AS uuid) AND state = 'running'
              AND claim_token = CAST(:token AS uuid)
              AND lease_expires_at > statement_timestamp()
            """
        ),
        {"id": str(request.request_id), "token": str(request.claim_token)},
    )
    # r3 finding F1 (P2, codex, CHAOS-4438): the request update above is
    # guarded by lease validity; the ledger update below was NOT -- if the
    # lease expired between execute() reading the row and this function
    # running (a caller can take arbitrarily long, e.g. a retired-kind
    # rejection racing a concurrent lease expiry, or a long-running
    # compatibility process finishing just as its lease lapses), the
    # request update above would affect zero rows while the ledger still
    # flipped to 'ambiguous' unconditionally -- leaving the request stuck
    # 'running' with an expired claim while the repair endpoint (which
    # requires BOTH records ambiguous) refuses to repair it. Skipping the
    # ledger update whenever the request update did not actually apply
    # keeps the two mutations atomic in effect: either both flip to
    # ambiguous, or neither does.
    if int(getattr(request_result, "rowcount", 0) or 0) == 0:
        # Nothing to commit -- the UPDATE above matched no row -- but still
        # close the transaction cleanly rather than leaving it open for
        # whatever the caller does next with this session.
        await session.commit()
        return
    await session.execute(
        text(
            """
            UPDATE work_graph_execution_ledger
            SET state = 'ambiguous', failure_detail = :detail, output_evidence = NULL,
                completed_at = NULL
            WHERE request_id = CAST(:id AS uuid) AND state = 'executing'
              AND claim_token = CAST(:token AS uuid)
            """
        ),
        {
            "id": str(request.request_id),
            "token": str(request.claim_token),
            "detail": detail[:1024],
        },
    )
    await session.commit()


@router.post("/execute")
async def execute(
    request: ExecuteRequest,
    session: Annotated[AsyncSession, Depends(get_postgres_session_dep)],
    connection: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, object]:
    authorize_worker_bridge(authorization)
    result = await session.execute(
        text(
            """
            SELECT id, org_id, kind, scope, model_ref, prompt_ref, llm_concurrency,
                   spend_limit_microunits, claim_token
            FROM work_graph_execution_requests
            WHERE id = CAST(:id AS uuid) AND state = 'running'
              AND claim_token = CAST(:token AS uuid)
              AND lease_expires_at > statement_timestamp()
            """
        ),
        {"id": str(request.request_id), "token": str(request.claim_token)},
    )
    row = result.mappings().first()
    if row is None:
        await session.rollback()
        raise HTTPException(status_code=409, detail="Execution lease is unavailable")
    # End the implicit read transaction before starting long-running Python
    # work. Holding a row lock here blocks the Go worker's lease renewals and
    # turns a healthy materialization into an expired execution.
    execution_row = dict(row)
    await session.rollback()
    kind = str(execution_row["kind"])
    if kind in _RETIRED_KINDS:
        # r2 finding F1 (P2, codex, CHAOS-4438): fail closed rather than
        # execute a retired kind's Python compatibility path -- see
        # _RETIRED_KINDS' module-level comment for why this is reachable in
        # principle even though nothing produces such a row today.
        logger.error(
            "rejecting execute request for retired job kind %s (request_id=%s org_id=%s)",
            kind,
            execution_row["id"],
            execution_row["org_id"],
        )
        await _mark_ambiguous(
            session, request, f"job kind {kind} was retired under CHAOS-4438"
        )
        raise HTTPException(
            status_code=410,
            detail=f"Job kind {kind} is retired and no longer executable",
        )
    try:
        arguments = _scope_arguments(
            str(execution_row["kind"]), execution_row["scope"], execution_row
        )
        outcome = await _run_until_client_disconnect(
            connection, str(execution_row["kind"]), arguments
        )
        evidence = _evidence(
            {
                "kind": execution_row["kind"],
                "model_ref": execution_row["model_ref"],
                "prompt_ref": execution_row["prompt_ref"],
                "llm_concurrency": execution_row["llm_concurrency"],
                "spend_limit_microunits": execution_row["spend_limit_microunits"],
                "outcome": outcome,
            }
        )
    except asyncio.CancelledError as exc:
        await _mark_ambiguous(
            session, request, f"compatibility executor raised {type(exc).__name__}"
        )
        raise
    except Exception as exc:
        await _mark_ambiguous(
            session, request, f"compatibility executor raised {type(exc).__name__}"
        )
        raise HTTPException(
            status_code=503, detail="Execution outcome is ambiguous"
        ) from exc
    return {"status": "success", "output_evidence": evidence}


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
