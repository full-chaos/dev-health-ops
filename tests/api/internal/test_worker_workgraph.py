from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import uuid
from collections.abc import Callable
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
    _run_compatibility_process,
    _run_sync,
    _run_until_client_disconnect,
    _scope_arguments,
)


async def _wait_for_marker(path: Path) -> None:
    for _ in range(100):
        if path.exists():
            return
        await asyncio.sleep(0.01)
    pytest.fail("compatibility child did not start")


async def _assert_process_reaped(pid: int) -> None:
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"compatibility process {pid} was not reaped")


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


def test_scope_arguments_allows_separate_dispatch_build_window() -> None:
    row = {
        "org_id": "00000000-0000-4000-8000-000000000009",
        "model_ref": "gpt-test",
        "llm_concurrency": 2,
    }
    assert _scope_arguments(
        "investment.dispatch",
        {
            "build_from_date": "2026-07-01T03:04:05Z",
            "build_to_date": "2026-07-14T23:59:58Z",
            "from_date": "2026-07-01",
            "to_date": "2026-07-14",
        },
        row,
    ) == {
        "build_from_date": "2026-07-01T03:04:05Z",
        "build_to_date": "2026-07-14T23:59:58Z",
        "from_date": "2026-07-01",
        "to_date": "2026-07-14",
        "org_id": "00000000-0000-4000-8000-000000000009",
        "llm_model": "gpt-test",
        "llm_concurrency": 2,
    }


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


def test_river_investment_dispatch_runs_sequentially_without_celery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import work_graph_tasks

    calls: list[tuple[str, dict[str, object]]] = []

    def record(name: str) -> Callable[..., dict[str, object]]:
        def run(**kwargs: object) -> dict[str, object]:
            calls.append((name, kwargs))
            return {"status": "success", "operation": name}

        return run

    monkeypatch.setattr(work_graph_tasks.run_work_graph_build, "run", record("build"))
    monkeypatch.setattr(
        work_graph_tasks.run_investment_materialize, "run", record("materialize")
    )
    monkeypatch.setattr(
        work_graph_tasks.run_membership_backfill, "run", record("membership")
    )
    monkeypatch.setattr(
        work_graph_tasks.dispatch_investment_materialize_partitioned,
        "run",
        lambda **_kwargs: pytest.fail("River dispatch called the Celery chord task"),
    )

    outcome = _run_sync(
        "investment.dispatch",
        {
            "org_id": "00000000-0000-4000-8000-000000000009",
            "build_from_date": "2026-07-01T03:04:05Z",
            "build_to_date": "2026-07-14T23:59:58Z",
            "from_date": "2026-07-01",
            "to_date": "2026-07-14",
            "llm_model": "gpt-test",
            "llm_concurrency": 2,
            "run_membership_backfill_after": True,
        },
    )

    assert [name for name, _kwargs in calls] == [
        "build",
        "materialize",
        "membership",
    ]
    assert calls[0][1] == {
        "org_id": "00000000-0000-4000-8000-000000000009",
        "from_date": "2026-07-01T03:04:05Z",
        "to_date": "2026-07-14T23:59:58Z",
    }
    assert "llm_model" in calls[1][1]
    assert calls[1][1]["from_date"] == "2026-07-01"
    assert calls[1][1]["to_date"] == "2026-07-14"
    assert "build_from_date" not in calls[1][1]
    assert "build_to_date" not in calls[1][1]
    assert calls[2][1] == {"org_id": "00000000-0000-4000-8000-000000000009"}
    assert outcome["status"] == "success"


def test_large_dispatch_results_produce_bounded_durable_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.workers import work_graph_tasks

    results: dict[str, dict[str, object]] = {}

    def large_result(name: str) -> Callable[..., dict[str, object]]:
        def run(**_kwargs: object) -> dict[str, object]:
            result: dict[str, object] = {
                "status": "success",
                "operation": name,
                "payload": "x" * 5000,
            }
            results[name] = result
            return result

        return run

    monkeypatch.setattr(
        work_graph_tasks.run_work_graph_build, "run", large_result("build")
    )
    monkeypatch.setattr(
        work_graph_tasks.run_investment_materialize,
        "run",
        large_result("materialize"),
    )
    monkeypatch.setattr(
        work_graph_tasks.run_membership_backfill,
        "run",
        large_result("membership"),
    )

    outcome = _run_sync(
        "investment.dispatch",
        {
            "org_id": "00000000-0000-4000-8000-000000000009",
            "run_membership_backfill_after": True,
        },
    )
    evidence = _evidence({"kind": "investment.dispatch", "outcome": outcome})

    for name, key in (
        ("build", "build"),
        ("materialize", "materialize"),
        ("membership", "membership"),
    ):
        result = results[name]
        encoded = worker_workgraph._canonical(result).encode()
        assert len(encoded) > 4096
        assert evidence["outcome"][key] == {
            "status": "success",
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "encoded_bytes": len(encoded),
        }
    assert len(worker_workgraph._canonical(evidence).encode()) <= 4096


@pytest.mark.asyncio
async def test_execute_releases_read_transaction_before_long_running_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.first.return_value = {
        "id": uuid.UUID("00000000-0000-4000-8000-000000000101"),
        "org_id": uuid.UUID("00000000-0000-4000-8000-000000000009"),
        "kind": "investment.dispatch",
        "scope": {"run_membership_backfill_after": True},
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
    await _wait_for_marker(marker)
    pid, child_pid = (int(value) for value in marker.read_text().split(":"))
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
