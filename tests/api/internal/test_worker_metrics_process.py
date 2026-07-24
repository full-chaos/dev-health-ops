from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from dev_health_ops.api.internal import worker_metrics, worker_metrics_runner


def _execution() -> worker_metrics._Execution:
    scope = {
        "version": 1,
        "day": "2026-07-23",
        "backfill_days": 1,
    }
    digest = worker_metrics._scope_digest(scope)
    run_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    partition_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    return worker_metrics._Execution(
        id=worker_metrics._execution_id(
            worker_kind="remaining",
            operation="partition",
            run_id=run_id,
            partition_id=partition_id,
            family="complexity",
            generation="post-sync:generation",
            scope_digest=digest,
        ),
        worker_kind="remaining",
        operation="partition",
        run_id=run_id,
        partition_id=partition_id,
        organization_id="55555555-5555-4555-8555-555555555555",
        family="complexity",
        generation="post-sync:generation",
        claim_token=uuid.UUID("33333333-3333-4333-8333-333333333333"),
        scope=scope,
        scope_digest=digest,
    )


def _runner_command(source: str, *arguments: str) -> tuple[str, ...]:
    return (sys.executable, "-c", source, *arguments)


async def _wait_for_marker(path: Path) -> None:
    for _ in range(100):
        if path.exists():
            return
        await asyncio.sleep(0.01)
    pytest.fail("metric compatibility child did not start")


async def _assert_process_reaped(pid: int) -> None:
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"metric compatibility process {pid} was not reaped")


def test_metric_process_payload_round_trips_only_durable_execution_fields() -> None:
    execution = _execution()
    payload = worker_metrics._execution_process_payload(execution)

    assert set(payload) == {
        "worker_kind",
        "operation",
        "run_id",
        "partition_id",
        "organization_id",
        "family",
        "generation",
        "claim_token",
        "scope",
        "generation_seed",
    }
    assert worker_metrics._execution_from_process_payload(payload) == execution
    with pytest.raises(ValueError, match="input is invalid"):
        worker_metrics._execution_from_process_payload(
            {**payload, "callable": "os.system"}
        )


def test_metric_runner_encodes_a_fixed_bounded_outcome() -> None:
    assert json.loads(
        worker_metrics_runner._encode_outcome({"family": "complexity", "rows": 1})
    ) == {"outcome": {"family": "complexity", "rows": 1}}


@pytest.mark.asyncio
async def test_metric_compatibility_process_returns_fixed_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, sys; json.load(sys.stdin); "
            "print(json.dumps({'outcome': {'family': 'complexity', 'rows': 1}}))"
        ),
    )

    assert await worker_metrics._run_compatibility_process(_execution()) == {
        "family": "complexity",
        "rows": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("raise SystemExit(7)", "process failed"),
        (
            "import json, sys; json.load(sys.stdin); raise SystemExit(0)",
            "invalid JSON",
        ),
    ],
)
async def test_metric_compatibility_process_rejects_nonzero_and_early_exit(
    monkeypatch: pytest.MonkeyPatch, source: str, message: str
) -> None:
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(source),
    )

    with pytest.raises((RuntimeError, ValueError), match=message):
        await worker_metrics._run_compatibility_process(_execution())


@pytest.mark.asyncio
async def test_metric_process_cancellation_terminates_and_reaps_descendants(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    marker = tmp_path / "metric-child.pid"
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, pathlib, subprocess, sys, time\n"
            "json.load(sys.stdin)\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
            "pathlib.Path(sys.argv[1]).write_text(f'{os.getpid()}:{child.pid}')\n"
            "time.sleep(60)",
            str(marker),
        ),
    )
    task = asyncio.create_task(worker_metrics._run_compatibility_process(_execution()))
    await _wait_for_marker(marker)
    pid, child_pid = (int(value) for value in marker.read_text().split(":"))

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await _assert_process_reaped(pid)
    await _assert_process_reaped(child_pid)


@pytest.mark.asyncio
async def test_metric_client_disconnect_terminates_the_process_group(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    marker = tmp_path / "metric-disconnect-child.pid"
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, pathlib, subprocess, sys, time\n"
            "json.load(sys.stdin)\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
            "pathlib.Path(sys.argv[1]).write_text(f'{os.getpid()}:{child.pid}')\n"
            "time.sleep(60)",
            str(marker),
        ),
    )
    connection = AsyncMock()

    async def disconnected() -> bool:
        return marker.exists()

    connection.is_disconnected.side_effect = disconnected

    with pytest.raises(ConnectionError, match="client disconnected"):
        await worker_metrics._run_until_client_disconnect(connection, _execution())

    pid, child_pid = (int(value) for value in marker.read_text().split(":"))
    await _assert_process_reaped(pid)
    await _assert_process_reaped(child_pid)
