from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException, Request

from dev_health_ops.api.internal import worker_workgraph, worker_workgraph_runner
from dev_health_ops.api.internal.worker_workgraph import (
    ExecuteRequest,
    _evidence,
    _mark_ambiguous,
    _run_compatibility_process,
    _run_sync,
    _run_until_client_disconnect,
    _scope_arguments,
)


async def _wait_for_pid_pair(path: Path) -> tuple[int, int]:
    for _ in range(100):
        try:
            values = path.read_text(encoding="utf-8").split(":")
        except FileNotFoundError:
            values = []
        if len(values) == 2 and all(value.isdecimal() for value in values):
            return int(values[0]), int(values[1])
        await asyncio.sleep(0.01)
    pytest.fail("compatibility child did not publish a complete PID marker")


async def _assert_process_reaped(pid: int) -> None:
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"compatibility process {pid} was not reaped")


@pytest.mark.asyncio
async def test_wait_for_pid_pair_ignores_a_partially_written_marker(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "partial.pid"
    marker.write_text("123:", encoding="utf-8")

    waiter = asyncio.create_task(_wait_for_pid_pair(marker))
    await asyncio.sleep(0)
    assert not waiter.done()

    marker.write_text("123:456", encoding="utf-8")
    assert await waiter == (123, 456)


def _runner_command(source: str, *arguments: str) -> tuple[str, ...]:
    return (sys.executable, "-c", source, *arguments)


def test_scope_arguments_reloads_only_allowlisted_workgraph_fields() -> None:
    row = {
        "org_id": "00000000-0000-4000-8000-000000000009",
        "model_ref": "gpt-test",
        "llm_concurrency": 2,
    }
    assert _scope_arguments(
        "workgraph.build",
        {"from_date": "2026-07-01", "heuristic_window": 7},
        row,
    ) == {
        "from_date": "2026-07-01",
        "heuristic_window": 7,
        "org_id": "00000000-0000-4000-8000-000000000009",
    }


@pytest.mark.parametrize(
    "kind", ["investment.dispatch", "investment.chunk", "investment.finalize"]
)
def test_scope_arguments_rejects_retired_kinds(kind: str) -> None:
    # r2 finding F1 (P2, codex, CHAOS-4438): these 3 kinds were removed from
    # the allowed-fields table outright, so a call that somehow bypasses
    # execute()'s explicit _RETIRED_KINDS guard still fails closed here
    # rather than being scoped and dispatched.
    row = {
        "org_id": "00000000-0000-4000-8000-000000000009",
        "model_ref": "gpt-test",
        "llm_concurrency": 2,
    }
    with pytest.raises(ValueError, match="unsupported"):
        _scope_arguments(kind, {}, row)


def test_scope_arguments_rejects_callable_or_credential_injection() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        _scope_arguments(
            "investment.materialize",
            {"from_date": "2026-07-01", "callable": "os.system"},
            {
                "org_id": "00000000-0000-4000-8000-000000000009",
                "model_ref": "gpt-test",
                "llm_concurrency": 1,
            },
        )


def test_evidence_is_canonical_and_bounded() -> None:
    assert _evidence({"z": 1, "a": ["evidence"]}) == {
        "a": ["evidence"],
        "z": 1,
    }
    with pytest.raises(ValueError, match="durable bound"):
        _evidence({"output": "x" * 5000})


def test_compatibility_runner_preserves_canonical_datetime_and_uuid_output() -> None:
    request_id = uuid.UUID("00000000-0000-4000-8000-000000000110")
    completed_at = datetime(2026, 7, 23, 12, 34, 56, tzinfo=timezone.utc)

    assert json.loads(
        worker_workgraph_runner._encode_outcome(
            {"request_id": request_id, "completed_at": completed_at}
        )
    ) == {
        "outcome": {
            "completed_at": "2026-07-23 12:34:56+00:00",
            "request_id": str(request_id),
        }
    }


@pytest.mark.parametrize(
    "kind", ["investment.dispatch", "investment.chunk", "investment.finalize"]
)
def test_run_sync_rejects_retired_kinds(kind: str) -> None:
    # r2 finding F1 (P2, codex, CHAOS-4438): these 3 kinds were removed from
    # the operations dispatch table outright -- the multi-step build +
    # materialize + membership orchestration investment.dispatch used to
    # perform (and investment.chunk/finalize's own partitioned-materialize
    # operations) no longer exists at all. A call that somehow bypasses
    # execute()'s explicit _RETIRED_KINDS guard still fails closed here.
    with pytest.raises(ValueError, match="unsupported"):
        _run_sync(kind, {"org_id": "00000000-0000-4000-8000-000000000009"})


@pytest.mark.asyncio
async def test_execute_releases_read_transaction_before_long_running_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = {
        "id": uuid.UUID("00000000-0000-4000-8000-000000000101"),
        "org_id": uuid.UUID("00000000-0000-4000-8000-000000000009"),
        # investment.dispatch is retired (r2 finding F1, CHAOS-4438) and
        # would now be rejected before this test's actual target (the
        # read-transaction release timing) is ever reached -- any live
        # kind exercises the same timing, so investment.materialize
        # stands in for it here.
        "kind": "investment.materialize",
        "scope": {},
        "model_ref": None,
        "prompt_ref": None,
        "llm_concurrency": 1,
        "spend_limit_microunits": 0,
        "claim_token": uuid.UUID("00000000-0000-4000-8000-000000000102"),
    }
    session.execute.return_value = result
    monkeypatch.setattr(worker_workgraph, "authorize_worker_bridge", lambda _auth: None)

    async def run_after_transaction_release(
        _connection: object, _kind: str, _arguments: dict[str, object]
    ) -> dict[str, object]:
        session.rollback.assert_awaited_once()
        return {"status": "success"}

    monkeypatch.setattr(
        worker_workgraph,
        "_run_until_client_disconnect",
        run_after_transaction_release,
    )

    response = await worker_workgraph.execute(
        ExecuteRequest(
            request_id=uuid.UUID("00000000-0000-4000-8000-000000000101"),
            claim_token=uuid.UUID("00000000-0000-4000-8000-000000000102"),
        ),
        session,
        MagicMock(),
        "Bearer test",
    )

    assert response["status"] == "success"
    statement = str(session.execute.await_args.args[0])
    assert "FOR UPDATE" not in statement


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["investment.dispatch", "investment.chunk", "investment.finalize"]
)
async def test_execute_rejects_a_retired_kind_request_row(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    kind: str,
) -> None:
    # r2 finding F1 (P2, codex, CHAOS-4438) red/green: a request row naming
    # a retired kind must be REJECTED -- logged with kind + request id,
    # marked ambiguous so it does not sit claimed forever, and refused with
    # a distinct status code -- rather than reaching _scope_arguments/
    # _run_sync at all. GREEN is this test; RED would be the pre-fix
    # behavior where such a row ran the retired kind's Python operation
    # successfully (see the now-deleted test_river_investment_dispatch_
    # runs_sequentially_without_celery for what that used to look like).
    request_id = uuid.UUID("00000000-0000-4000-8000-000000000141")
    org_id = uuid.UUID("00000000-0000-4000-8000-000000000009")
    claim_token = uuid.UUID("00000000-0000-4000-8000-000000000142")
    session = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = {
        "id": request_id,
        "org_id": org_id,
        "kind": kind,
        "scope": {},
        "model_ref": None,
        "prompt_ref": None,
        "llm_concurrency": 1,
        "spend_limit_microunits": 0,
        "claim_token": claim_token,
    }
    session.execute.return_value = result
    monkeypatch.setattr(worker_workgraph, "authorize_worker_bridge", lambda _auth: None)
    mark_ambiguous = AsyncMock()
    monkeypatch.setattr(worker_workgraph, "_mark_ambiguous", mark_ambiguous)
    run_until_disconnect = AsyncMock()
    monkeypatch.setattr(
        worker_workgraph, "_run_until_client_disconnect", run_until_disconnect
    )
    request = ExecuteRequest(request_id=request_id, claim_token=claim_token)

    with caplog.at_level("ERROR", logger=worker_workgraph.__name__):
        with pytest.raises(HTTPException) as exc_info:
            await worker_workgraph.execute(request, session, MagicMock(), "Bearer test")

    assert exc_info.value.status_code == 410
    assert kind in exc_info.value.detail
    mark_ambiguous.assert_awaited_once()
    await_args = mark_ambiguous.await_args
    assert await_args is not None
    assert await_args.args[0] is session
    assert await_args.args[1] is request
    assert kind in await_args.args[2]
    # The retired kind must never reach the actual dispatch path.
    run_until_disconnect.assert_not_awaited()
    assert any(
        kind in record.message and str(request_id) in record.message
        for record in caplog.records
    ), f"no log line named both {kind!r} and the request id:\n{caplog.text}"


@pytest.mark.asyncio
async def test_mark_ambiguous_flips_both_records_when_the_request_update_applies() -> (
    None
):
    # r3 finding F1 GREEN half: when the request update actually matches a
    # row (lease still valid), the ledger update runs too and the
    # transaction commits.
    session = AsyncMock()
    request_update_result = MagicMock(rowcount=1)
    session.execute.side_effect = [request_update_result, MagicMock()]
    request = ExecuteRequest(
        request_id=uuid.UUID("00000000-0000-4000-8000-000000000151"),
        claim_token=uuid.UUID("00000000-0000-4000-8000-000000000152"),
    )

    await _mark_ambiguous(session, request, "some failure detail")

    assert session.execute.await_count == 2
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_mark_ambiguous_skips_the_ledger_update_when_the_lease_already_expired(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # r3 finding F1 (P2, codex, CHAOS-4438) RED half: the request update is
    # guarded by lease validity (WHERE ... lease_expires_at >
    # statement_timestamp()); if the lease already expired, that UPDATE
    # matches zero rows. Before the fix, the ledger update ran anyway --
    # flipping to 'ambiguous' unconditionally -- leaving the request stuck
    # 'running' with an expired claim while the ledger says ambiguous, a
    # state the repair endpoint (requires BOTH ambiguous) can never
    # resolve. The fix must skip the second update, still commit, and log
    # the skip by request_id (team-lead's discard-on-error sweep) so an
    # operator has a lead on why the ledger stayed 'executing'.
    session = AsyncMock()
    request_update_result = MagicMock(rowcount=0)
    session.execute.side_effect = [request_update_result]
    request = ExecuteRequest(
        request_id=uuid.UUID("00000000-0000-4000-8000-000000000161"),
        claim_token=uuid.UUID("00000000-0000-4000-8000-000000000162"),
    )

    with caplog.at_level("WARNING", logger=worker_workgraph.__name__):
        await _mark_ambiguous(session, request, "some failure detail")

    assert session.execute.await_count == 1, (
        "the ledger update ran even though the request update matched zero "
        "rows -- the two records can now disagree about ambiguous state"
    )
    session.commit.assert_awaited_once()
    assert any(
        str(request.request_id) in record.message for record in caplog.records
    ), f"no log line named the request id when the lease had expired:\n{caplog.text}"


@pytest.mark.asyncio
async def test_execute_marks_ambiguous_when_compatibility_process_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = {
        "id": uuid.UUID("00000000-0000-4000-8000-000000000121"),
        "org_id": uuid.UUID("00000000-0000-4000-8000-000000000009"),
        "kind": "workgraph.build",
        "scope": {},
        "model_ref": None,
        "prompt_ref": None,
        "llm_concurrency": 1,
        "spend_limit_microunits": 0,
        "claim_token": uuid.UUID("00000000-0000-4000-8000-000000000122"),
    }
    session.execute.return_value = result
    monkeypatch.setattr(worker_workgraph, "authorize_worker_bridge", lambda _auth: None)
    mark_ambiguous = AsyncMock()
    monkeypatch.setattr(worker_workgraph, "_mark_ambiguous", mark_ambiguous)

    async def fail_after_transaction_release(
        _connection: object, _kind: str, _arguments: dict[str, object]
    ) -> dict[str, object]:
        session.rollback.assert_awaited_once()
        raise RuntimeError("runner failed")

    monkeypatch.setattr(
        worker_workgraph,
        "_run_until_client_disconnect",
        fail_after_transaction_release,
    )
    request = ExecuteRequest(
        request_id=uuid.UUID("00000000-0000-4000-8000-000000000121"),
        claim_token=uuid.UUID("00000000-0000-4000-8000-000000000122"),
    )

    with pytest.raises(HTTPException, match="Execution outcome is ambiguous"):
        await worker_workgraph.execute(request, session, MagicMock(), "Bearer test")

    mark_ambiguous.assert_awaited_once_with(
        session, request, "compatibility executor raised RuntimeError"
    )


@pytest.mark.asyncio
async def test_execute_marks_ambiguous_and_preserves_task_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = {
        "id": uuid.UUID("00000000-0000-4000-8000-000000000131"),
        "org_id": uuid.UUID("00000000-0000-4000-8000-000000000009"),
        "kind": "workgraph.build",
        "scope": {},
        "model_ref": None,
        "prompt_ref": None,
        "llm_concurrency": 1,
        "spend_limit_microunits": 0,
        "claim_token": uuid.UUID("00000000-0000-4000-8000-000000000132"),
    }
    session.execute.return_value = result
    monkeypatch.setattr(worker_workgraph, "authorize_worker_bridge", lambda _auth: None)
    mark_ambiguous = AsyncMock()
    monkeypatch.setattr(worker_workgraph, "_mark_ambiguous", mark_ambiguous)

    async def cancel_after_transaction_release(
        _connection: object, _kind: str, _arguments: dict[str, object]
    ) -> dict[str, object]:
        raise asyncio.CancelledError

    monkeypatch.setattr(
        worker_workgraph,
        "_run_until_client_disconnect",
        cancel_after_transaction_release,
    )
    request = ExecuteRequest(
        request_id=uuid.UUID("00000000-0000-4000-8000-000000000131"),
        claim_token=uuid.UUID("00000000-0000-4000-8000-000000000132"),
    )

    with pytest.raises(asyncio.CancelledError):
        await worker_workgraph.execute(request, session, MagicMock(), "Bearer test")

    mark_ambiguous.assert_awaited_once_with(
        session, request, "compatibility executor raised CancelledError"
    )


@pytest.mark.asyncio
async def test_compatibility_process_returns_fixed_json_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker_workgraph,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, sys; json.load(sys.stdin); "
            "print(json.dumps({'outcome': {'status': 'success'}}))"
        ),
    )

    assert await _run_compatibility_process("workgraph.build", {"org_id": "org"}) == {
        "status": "success"
    }


@pytest.mark.asyncio
async def test_compatibility_process_rejects_nonzero_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker_workgraph,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command("raise SystemExit(7)"),
    )

    with pytest.raises(RuntimeError, match="compatibility process failed"):
        await _run_compatibility_process("workgraph.build", {"org_id": "org"})


@pytest.mark.asyncio
async def test_compatibility_process_cancellation_terminates_and_reaps_child(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    marker = tmp_path / "compatibility-child.pid"
    monkeypatch.setattr(
        worker_workgraph,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, pathlib, sys, time\n"
            "import subprocess\n"
            "json.load(sys.stdin)\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
            "pathlib.Path(sys.argv[1]).write_text(f'{os.getpid()}:{child.pid}')\n"
            "time.sleep(60)",
            str(marker),
        ),
    )

    execution = asyncio.create_task(
        _run_compatibility_process("workgraph.build", {"org_id": "org"})
    )
    pid, child_pid = await _wait_for_pid_pair(marker)
    execution.cancel()

    with pytest.raises(asyncio.CancelledError):
        await execution
    await _assert_process_reaped(pid)
    await _assert_process_reaped(child_pid)


@pytest.mark.asyncio
async def test_client_disconnect_terminates_compatibility_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    marker = tmp_path / "compatibility-disconnect-child.pid"
    monkeypatch.setattr(
        worker_workgraph,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, pathlib, sys, time\n"
            "json.load(sys.stdin)\n"
            "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))\n"
            "time.sleep(60)",
            str(marker),
        ),
    )

    class _DisconnectAfterStart:
        async def is_disconnected(self) -> bool:
            return marker.exists()

    with pytest.raises(ConnectionError, match="client disconnected"):
        await _run_until_client_disconnect(
            cast(Request, _DisconnectAfterStart()),
            "workgraph.build",
            {"org_id": "org"},
        )

    pid = int(marker.read_text())
    await _assert_process_reaped(pid)
