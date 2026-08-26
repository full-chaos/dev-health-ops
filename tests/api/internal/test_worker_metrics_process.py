from __future__ import annotations

import asyncio
import dataclasses
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


def _daily_execution() -> worker_metrics._Execution:
    """A daily/partition execution -- the only worker_kind whose progress
    signal is real (CHAOS-4264 codex R2: safe_to_retry requires
    worker_kind == "daily" because only _run_daily_direct wires
    on_write_starting through job_daily.py)."""
    scope = {"target_day": "2026-08-24", "repo_ids": []}
    digest = worker_metrics._scope_digest(scope)
    run_id = uuid.UUID("44444444-4444-4444-8444-444444444444")
    partition_id = uuid.UUID("55555555-5555-4555-8555-555555555555")
    return worker_metrics._Execution(
        id=worker_metrics._execution_id(
            worker_kind="daily",
            operation="partition",
            run_id=run_id,
            partition_id=partition_id,
            family="daily",
            generation="daily-v1",
            scope_digest=digest,
        ),
        worker_kind="daily",
        operation="partition",
        run_id=run_id,
        partition_id=partition_id,
        organization_id="66666666-6666-4666-8666-666666666666",
        family="daily",
        generation="daily-v1",
        claim_token=uuid.UUID("77777777-7777-4777-8777-777777777777"),
        scope=scope,
        scope_digest=digest,
    )


def _daily_finalize_execution() -> worker_metrics._Execution:
    """A daily/finalize execution. CHAOS-4264 codex R3: run_daily_metrics_
    finalize writes user_metrics_daily/ic_landscape_rolling_30d directly
    with no progress callback of its own -- worker_kind == "daily" alone is
    not enough; safe_to_retry must also require operation == "partition"."""
    scope = {"target_day": "2026-08-24", "repo_ids": []}
    digest = worker_metrics._scope_digest(scope)
    run_id = uuid.UUID("88888888-8888-4888-8888-888888888888")
    return worker_metrics._Execution(
        id=worker_metrics._execution_id(
            worker_kind="daily",
            operation="finalize",
            run_id=run_id,
            partition_id=None,
            family="daily",
            generation="daily-v1",
            scope_digest=digest,
        ),
        worker_kind="daily",
        operation="finalize",
        run_id=run_id,
        partition_id=None,
        organization_id="66666666-6666-4666-8666-666666666666",
        family="daily",
        generation="daily-v1",
        claim_token=uuid.UUID("99999999-9999-4999-8999-999999999999"),
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
        "skip_families",
    }
    assert worker_metrics._execution_from_process_payload(payload) == execution
    with pytest.raises(ValueError, match="input is invalid"):
        worker_metrics._execution_from_process_payload(
            {**payload, "callable": "os.system"}
        )


def test_metric_process_payload_round_trips_a_non_empty_skip_families() -> None:
    """CHAOS-4276: skip_families must survive the subprocess boundary
    round-trip, not just default to empty -- the field this ticket added is
    exactly the one the earlier round-trip test does not exercise with a
    non-default value."""
    execution = dataclasses.replace(
        _daily_execution(), skip_families=("team_wellbeing",)
    )
    payload = worker_metrics._execution_process_payload(execution)
    assert payload["skip_families"] == ["team_wellbeing"]
    round_tripped = worker_metrics._execution_from_process_payload(payload)
    assert round_tripped == execution
    assert round_tripped.skip_families == ("team_wellbeing",)


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


# --------------------------------------------------------------------------
# CHAOS-4264: memory bounding + classified (not -9) subprocess failures.
# --------------------------------------------------------------------------


@pytest.mark.skipif(
    sys.platform != "linux",
    reason=(
        "RLIMIT_AS enforcement is deterministic on Linux (what every "
        "deployment target runs); macOS's virtual memory allocator does not "
        "reliably fail an over-limit allocation the same way, so this would "
        "be a flaky assertion about the host, not about the code."
    ),
)
@pytest.mark.asyncio
async def test_runner_memory_limit_converts_oversized_allocation_to_memory_error() -> (
    None
):
    """The falsifier for CHAOS-4264 item 1(a): a synthetic oversized compute
    under a small configured limit must fail with a classified MemoryError,
    never reach kernel OOM-kill territory in the first place."""
    source = (
        "import os, sys\n"
        "os.environ['DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES'] = str(64 * 1024 * 1024)\n"
        "from dev_health_ops.api.internal import worker_metrics_runner as runner\n"
        "runner._apply_memory_limit()\n"
        "try:\n"
        "    buf = bytearray(1024 * 1024 * 1024)\n"
        "    buf[0] = 1\n"
        "except MemoryError:\n"
        "    raise SystemExit(runner.EXIT_RESOURCE_EXHAUSTED)\n"
        "raise SystemExit(97)\n"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        source,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    assert process.returncode == worker_metrics_runner.EXIT_RESOURCE_EXHAUSTED, (
        f"returncode={process.returncode} stderr={stderr!r}"
    )


def _synthetic_runner_source(*, allocate_bytes: int, limit_bytes: int) -> str:
    """A stand-in for the real runner that exercises the SAME memory-bound
    mechanism (worker_metrics_runner._apply_memory_limit) and the SAME
    progress-then-outcome NDJSON wire protocol
    (worker_metrics_runner._emit_progress / _encode_outcome), without
    needing a live ClickHouse-backed compute to produce an oversized working
    set. This is what test_metric_compatibility_process_* below drive
    through _run_compatibility_process to prove the PARENT's classification
    (not just the child's own exit path).
    """
    return (
        "import contextlib, json, os, sys\n"
        f"os.environ['DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES'] = str({limit_bytes})\n"
        "from dev_health_ops.api.internal import worker_metrics_runner as runner\n"
        "runner._apply_memory_limit()\n"
        "json.load(sys.stdin)\n"
        "runner._emit_progress(1, 3)\n"
        "runner._emit_progress(2, 3)\n"
        "with contextlib.suppress(Exception):\n"
        f"    buf = bytearray({allocate_bytes})\n"
        "    buf[0] = 1\n"
        "    sys.stdout.write(runner._encode_outcome({'repo_count': 3}) + chr(10))\n"
        "    raise SystemExit(0)\n"
        "raise SystemExit(runner.EXIT_RESOURCE_EXHAUSTED)\n"
    )


@pytest.mark.skipif(sys.platform != "linux", reason="see memory-limit test above")
@pytest.mark.asyncio
async def test_metric_compatibility_process_classifies_resource_exhausted_with_progress_as_ambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Progress WAS emitted (2 of 3 repos already written) before the
    resource bound was hit -- CHAOS-4264 says this must stay conservative
    (ambiguous-eligible), not be waved through as safe_to_retry."""
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            _synthetic_runner_source(
                allocate_bytes=1024**3, limit_bytes=64 * 1024 * 1024
            )
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "resource_exhausted"
    assert excinfo.value.safe_to_retry is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_classifies_signal_kill_with_no_progress_as_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No progress line was ever emitted, and the process was killed by a
    signal (the actual CHAOS-4264 production shape: SIGKILL, return_code<0)
    -- this must be safe to hand straight back to River, not parked in the
    ambiguous state a human has to repair. Uses a daily execution: only the
    daily path has real per-scope write evidence (codex R2), so it is the
    only worker_kind that can ever be safe_to_retry."""
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, signal, sys\n"
            "json.load(sys.stdin)\n"
            "os.kill(os.getpid(), signal.SIGKILL)\n"
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "process_signaled"
    assert excinfo.value.safe_to_retry is True


@pytest.mark.asyncio
async def test_metric_compatibility_process_remaining_metrics_never_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex R2 finding (CHAOS-4264): remaining-metrics families
    (capacity/complexity/dora/release_impact/recommendations/
    membership_backfill) never emit progress at all, so "zero progress"
    would be indistinguishable from "wrote one repo's rows then crashed" --
    a fabricated safety claim, not an observed one. Even a signal-killed
    remaining execution with zero progress lines must stay unsafe to
    auto-retry (safe_to_retry requires worker_kind == "daily")."""
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, signal, sys\n"
            "json.load(sys.stdin)\n"
            "os.kill(os.getpid(), signal.SIGKILL)\n"
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_execution())
    assert excinfo.value.reason == "process_signaled"
    assert excinfo.value.safe_to_retry is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_daily_finalize_never_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex R3 finding (CHAOS-4264): run_daily_metrics_finalize writes
    user_metrics_daily and ic_landscape_rolling_30d directly with no
    progress callback -- worker_kind == "daily" alone is not a safe gate.
    A signal-killed finalize with zero progress lines must stay unsafe to
    auto-retry (safe_to_retry additionally requires operation ==
    "partition")."""
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, signal, sys\n"
            "json.load(sys.stdin)\n"
            "os.kill(os.getpid(), signal.SIGKILL)\n"
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_finalize_execution())
    assert excinfo.value.reason == "process_signaled"
    assert excinfo.value.safe_to_retry is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_progress_then_signal_kill_is_not_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same signal kill, but at least one repo's families were already
    written first -- must NOT be classified safe_to_retry, matching the
    conservative default that predates this ticket."""
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, os, signal, sys\n"
            "from dev_health_ops.api.internal import worker_metrics_runner as runner\n"
            "json.load(sys.stdin)\n"
            "runner._emit_progress(1, 3)\n"
            "os.kill(os.getpid(), signal.SIGKILL)\n"
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "process_signaled"
    assert excinfo.value.safe_to_retry is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_generic_failure_with_no_progress_is_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command("raise SystemExit(1)"),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "process_failed"
    assert excinfo.value.safe_to_retry is True


# --------------------------------------------------------------------------
# CHAOS-4264: per-repo streaming for the daily partition path.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_daily_direct_computes_one_repo_at_a_time_and_reports_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The falsifier for CHAOS-4264 item 1(b): a partition's repo_ids are
    computed one at a time (each run_daily_metrics_job call scoped to a
    single repo_id, its source rows released before the next call starts),
    not loaded all at once -- and on_progress fires after each one, in
    order, so the parent can tell how far a killed execution got."""
    repo_ids = [uuid.uuid4() for _ in range(3)]
    calls: list[uuid.UUID | None] = []
    progress: list[tuple[int, int]] = []

    async def fake_run_daily_metrics_job(*, repo_id, on_write_starting=None, **_kwargs):
        calls.append(repo_id)
        if on_write_starting is not None:
            on_write_starting()
        return {}

    import dev_health_ops.metrics.job_daily as job_daily_module

    monkeypatch.setattr(
        job_daily_module, "run_daily_metrics_job", fake_run_daily_metrics_job
    )
    scope = {
        "target_day": "2026-08-25",
        "repo_ids": [str(value) for value in repo_ids],
    }
    digest = worker_metrics._scope_digest(scope)
    run_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
    partition_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    execution = worker_metrics._Execution(
        id=worker_metrics._execution_id(
            worker_kind="daily",
            operation="partition",
            run_id=run_id,
            partition_id=partition_id,
            family="daily",
            generation="fixed-schedule:daily_metrics_fanout:2026-08-25T01:00:00Z",
            scope_digest=digest,
        ),
        worker_kind="daily",
        operation="partition",
        run_id=run_id,
        partition_id=partition_id,
        organization_id="55555555-5555-4555-8555-555555555555",
        family="daily",
        generation="fixed-schedule:daily_metrics_fanout:2026-08-25T01:00:00Z",
        claim_token=uuid.UUID("33333333-3333-4333-8333-333333333333"),
        scope=scope,
        scope_digest=digest,
    )
    monkeypatch.setattr(
        worker_metrics, "require_clickhouse_uri", lambda: "clickhouse://test"
    )
    result = await worker_metrics._run_daily_direct(
        execution, on_progress=lambda index, total: progress.append((index, total))
    )
    assert calls == repo_ids, (
        "expected one run_daily_metrics_job call per repo_id, in order"
    )
    assert progress == [(1, 3), (2, 3), (3, 3)]
    assert result["repo_count"] == 3


@pytest.mark.asyncio
async def test_run_daily_direct_reports_progress_before_a_repo_crashes_mid_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex R1 finding (CHAOS-4264): progress must fire at the write
    BOUNDARY, not only on a repo's successful completion -- otherwise a
    kill/crash after writes started but before run_daily_metrics_job returns
    would report zero progress despite having written rows. This proves
    on_progress observes the crashed repo's write-starting signal before the
    exception propagates out of _run_daily_direct."""
    repo_ids = [uuid.uuid4(), uuid.uuid4()]
    progress: list[tuple[int, int]] = []

    async def fake_run_daily_metrics_job(*, repo_id, on_write_starting=None, **_kwargs):
        if on_write_starting is not None:
            on_write_starting()
        if repo_id == repo_ids[0]:
            raise RuntimeError("simulated crash after the first write began")
        return {}

    import dev_health_ops.metrics.job_daily as job_daily_module

    monkeypatch.setattr(
        job_daily_module, "run_daily_metrics_job", fake_run_daily_metrics_job
    )
    scope = {"target_day": "2026-08-25", "repo_ids": [str(value) for value in repo_ids]}
    digest = worker_metrics._scope_digest(scope)
    run_id = uuid.UUID("11111111-1111-4111-8111-111111111112")
    partition_id = uuid.UUID("22222222-2222-4222-8222-222222222223")
    execution = worker_metrics._Execution(
        id=worker_metrics._execution_id(
            worker_kind="daily",
            operation="partition",
            run_id=run_id,
            partition_id=partition_id,
            family="daily",
            generation="chaos-4264-progress-boundary-test",
            scope_digest=digest,
        ),
        worker_kind="daily",
        operation="partition",
        run_id=run_id,
        partition_id=partition_id,
        organization_id="55555555-5555-4555-8555-555555555555",
        family="daily",
        generation="chaos-4264-progress-boundary-test",
        claim_token=uuid.UUID("33333333-3333-4333-8333-333333333334"),
        scope=scope,
        scope_digest=digest,
    )
    monkeypatch.setattr(
        worker_metrics, "require_clickhouse_uri", lambda: "clickhouse://test"
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        await worker_metrics._run_daily_direct(
            execution, on_progress=lambda index, total: progress.append((index, total))
        )
    assert progress == [(1, 2)], (
        "the crashed repo's write-starting signal must still have fired"
    )
