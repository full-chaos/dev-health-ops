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


@pytest.mark.asyncio
async def test_read_bounded_stderr_keeps_the_tail_not_the_head() -> None:
    """CHAOS-4543: _read_bounded_stderr must truncate from the FRONT,
    keeping the LAST maximum_bytes -- the opposite of
    _read_bounded_process_stream's ValueError-on-overflow, and the opposite
    of a naive head-keeping bound, which this ticket's own local repro
    proved silently discards the one line that explains a failure (the
    verbose per-query INFO logging run_daily_metrics_job emits comes BEFORE
    a guard like TestopsRowCapExceeded raises, so the useful content is
    always at the tail)."""
    # Deliberately non-periodic (a head-truncation and a tail-truncation of a
    # periodic string can coincide byte-for-byte when the window aligns with
    # the period -- caught in this ticket's own first draft of this test,
    # which passed unchanged under a head-keeping mutation).
    stream = asyncio.StreamReader()
    stream.feed_data(b"HEAD_MARKER_" + b"noise" * 20 + b"_TAIL_MARKER")
    stream.feed_eof()
    result = await worker_metrics._read_bounded_stderr(stream, maximum_bytes=20)
    assert result.endswith(b"_TAIL_MARKER")
    assert b"HEAD_MARKER_" not in result
    assert result.startswith(b"...(truncated)\n")


@pytest.mark.asyncio
async def test_read_bounded_stderr_returns_everything_under_the_bound_untouched() -> (
    None
):
    stream = asyncio.StreamReader()
    stream.feed_data(b"short diagnostic line")
    stream.feed_eof()
    result = await worker_metrics._read_bounded_stderr(stream, maximum_bytes=4096)
    assert result == b"short diagnostic line"
    assert b"truncated" not in result


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


def test_rlimit_as_backstop_clamped_below_a_container_ceiling_with_room_to_spare(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """codex R1 (PR #1940): the raw 4x multiplier on the 640 MiB default is
    2.5 GiB, which can exceed a container's own ceiling -- a backstop
    bigger than its own container can never fire before the kernel's
    memcg OOM killer does, silently reintroducing an un-classified kill.
    When the real cgroup ceiling is observable, leaves room for the api
    headroom, AND still leaves room for a safe address-space margin above
    the RSS limit, the backstop is clamped to that ceiling."""
    monkeypatch.setenv(
        worker_metrics_runner._MEMORY_LIMIT_ENV_KEY, str(640 * 1024 * 1024)
    )
    # A 1.5G container: ceiling = 1536 - 384 = 1152 MiB, which sits between
    # the 960 MiB safety floor (640 * 1.5) and the 2560 MiB preferred value
    # (640 * 4) -- this is the one container size that exercises REAL
    # clamping distinct from both the "plenty of room" and "no room at
    # all" fallback paths below.
    monkeypatch.setattr(
        worker_metrics_runner,
        "_cgroup_memory_max_bytes",
        lambda: int(1.5 * 1024 * 1024 * 1024),
    )
    backstop = worker_metrics_runner._rlimit_as_backstop_bytes()
    assert backstop == int(1.5 * 1024 * 1024 * 1024) - (384 * 1024 * 1024)
    assert backstop < 640 * 1024 * 1024 * 4
    assert backstop >= 640 * 1024 * 1024 * 1.5


def test_rlimit_as_backstop_uses_the_raw_multiplier_when_cgroup_is_unobservable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-Linux dev, cgroup v1, or no permission -- must not raise or
    silently zero out the backstop; falls back to the plain multiplier."""
    monkeypatch.setenv(
        worker_metrics_runner._MEMORY_LIMIT_ENV_KEY, str(64 * 1024 * 1024)
    )
    monkeypatch.setattr(worker_metrics_runner, "_cgroup_memory_max_bytes", lambda: None)
    assert worker_metrics_runner._rlimit_as_backstop_bytes() == 64 * 1024 * 1024 * 4


def test_rlimit_as_backstop_never_drops_to_the_rss_limit_under_the_documented_1g_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """codex R2 (PR #1940): the R1 clamp over-corrected. Under the exact
    documented 1G shared `api` container with the 640 MiB RSS-limit
    default, `ceiling = 1024 MiB - 384 MiB = 640 MiB` -- clamping to that
    ceiling would set RLIMIT_AS to EXACTLY the RSS limit, reintroducing
    the precise false positive this ticket exists to close (prod fired at
    640 MiB RLIMIT_AS while real RSS was only 465 MiB). There is not
    enough slack in this container to fit both the api headroom AND a
    safe address-space margin above the RSS limit, so the backstop must
    fall back to the plain (unclamped) multiplier instead of shrinking
    into a value at or near the RSS limit itself."""
    monkeypatch.setenv(
        worker_metrics_runner._MEMORY_LIMIT_ENV_KEY, str(640 * 1024 * 1024)
    )
    monkeypatch.setattr(
        worker_metrics_runner, "_cgroup_memory_max_bytes", lambda: 1024 * 1024 * 1024
    )
    backstop = worker_metrics_runner._rlimit_as_backstop_bytes()
    assert backstop == 640 * 1024 * 1024 * 4
    assert backstop > 640 * 1024 * 1024 * 1.5


def test_rlimit_as_backstop_falls_back_when_cgroup_smaller_than_headroom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cgroup smaller than the reserved headroom (even negative after
    subtracting it) is nowhere near enough room for a safe backstop --
    falls back to the plain multiplier, same as the unobservable-cgroup
    case, rather than a value at or below the RSS limit itself."""
    monkeypatch.setenv(
        worker_metrics_runner._MEMORY_LIMIT_ENV_KEY, str(64 * 1024 * 1024)
    )
    monkeypatch.setattr(
        worker_metrics_runner, "_cgroup_memory_max_bytes", lambda: 100 * 1024 * 1024
    )
    assert worker_metrics_runner._rlimit_as_backstop_bytes() == 64 * 1024 * 1024 * 4


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


@pytest.mark.skipif(sys.platform != "linux", reason="see memory-limit test above")
@pytest.mark.asyncio
async def test_metric_compatibility_process_kills_on_rss_breach_below_the_rlimit_backstop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4361: the falsifier for the actual bug -- a child whose real RSS
    crosses the configured DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES bound
    but stays well UNDER the child's own RLIMIT_AS backstop (4x the same
    bound; see worker_metrics_runner._RLIMIT_AS_BACKSTOP_MULTIPLIER) must
    still be killed and classified resource_exhausted. This is exactly the
    prod shape: RLIMIT_AS at 640 MiB fired while real RSS was only 465 MB --
    i.e. a process whose RESIDENT memory alone should be enough to trigger
    the bound, without ever touching virtual-address-space territory. If
    this test passes only because the child's own rlimit fired, the parent's
    RSS watchdog (_poll_peak_rss_bytes) would be untested and this exact
    regression could reappear silently."""
    # 160 MiB: comfortably above this process's own import-time RSS baseline
    # (importing worker_metrics_runner pulls in the whole worker_metrics
    # module -- FastAPI, SQLAlchemy, pydantic, prometheus_client,
    # OpenTelemetry -- which alone measures ~103-110 MB RSS before any test
    # code runs, empirically) so the kill genuinely waits for
    # _emit_progress and the deliberate allocation below, not for import
    # overhead alone.
    monkeypatch.setenv(
        "DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES", str(160 * 1024 * 1024)
    )
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, sys\n"
            "from dev_health_ops.api.internal import worker_metrics_runner as runner\n"
            "runner._apply_memory_limit()\n"
            "json.load(sys.stdin)\n"
            "runner._emit_progress(1, 3)\n"
            # 150 MiB on top of the ~110 MB import baseline clears the 160
            # MiB configured bound with a wide margin, but stays far below
            # the 640 MiB RLIMIT_AS backstop (4x) the child sets on itself
            # -- only the parent's RSS poll can catch this. CPython's
            # bytearray() zero-fills (and empirically commits) the whole
            # buffer at allocation time in this build, so RSS jumps by the
            # full amount immediately -- no separate page-touching needed.
            "buf = bytearray(150 * 1024 * 1024)\n"
            "buf[0] = 1\n"
            "import time; time.sleep(10)\n"
            "raise SystemExit(97)\n"
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "resource_exhausted"
    # One progress line was emitted before the breach -- conservative,
    # same as the child's own MemoryError path with progress already seen.
    assert excinfo.value.safe_to_retry is False


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
    (ambiguous-eligible), not be waved through as safe_to_retry.

    CHAOS-4319 considered and REJECTED broadening this: codex round-1 review
    found file_metrics_daily's readers (api/queries/heatmap.py's
    fetch_hotspot_risk and its sibling) `SUM(...)` with no `computed_at`
    dedup, so a retry-caused duplicate would silently inflate hotspot
    scores. The append-only+reader-dedup property is not uniformly true
    across every table this path writes, so this stays conservative; see
    the CHAOS-4319 comment on `safe_to_retry`'s definition below for the
    durable-truth fix that closes the ticket's actual bug without relying
    on that premise."""
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
    conservative default that predates this ticket. CHAOS-4319 considered
    and rejected broadening this generally (see the resource_exhausted test
    above); the actual fix is durable persistence on the Go side once this
    lands ambiguous, not authorizing a retry that risks a reader-side
    double-count."""
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


@pytest.mark.asyncio
async def test_metric_compatibility_process_resource_exhausted_embeds_captured_stderr_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4543 red-first proof (verified red against the pre-fix code via
    a real local repro against org 70d529e0's dev-health-ops repo on a
    high-volume day before this fix landed): the exit-code==2
    (EXIT_RESOURCE_EXHAUSTED) branch used to build its
    _CompatibilityProcessFailure message from a hardcoded static string
    ("...exceeded its memory bound") regardless of what the runner
    subprocess actually printed to stderr -- so a deliberate,
    already-diagnostic guard failure (e.g. TestopsRowCapExceeded's
    table/org_id/max_rows/fetched detail, logged via `logger.error` right
    before the runner's own generic `except MemoryError:` print) never
    survived into the message this process raises, and therefore never into
    metric_compatibility_executions.failure_detail
    (_mark_ambiguous/_mark_retry_authorized persist f"{reason}: {message}").

    Drives a real subprocess that writes a large, distinct HEAD marker, then
    >_MAX_COMPATIBILITY_STDERR_MESSAGE_BYTES of filler (simulating the
    verbose per-query INFO logging run_daily_metrics_job emits before a
    failure), then one short TAIL marker, then exits 2 -- proving three
    things at once: (1) the tail marker (the actually-useful diagnostic)
    survives into the raised message, (2) the head marker (unhelpful noise
    from long before the failure) does not, and (3) the whole message stays
    small enough to survive metric_compatibility_executions.failure_detail's
    own 1024-byte (head) truncation -- embedding the FULL captured stderr
    instead of a second, smaller tail slice would have let that same
    1024-byte cap discard the tail marker all over again (caught by this
    ticket's own local repro before the second slice was added).
    """
    head_marker = "HEAD_ONLY_NOISE_MARKER_NEVER_SHOULD_SURVIVE"
    tail_marker = "testops_row_cap_exceeded: table=test_case_results fetched=200001"
    source = (
        "import sys\n"
        f"sys.stderr.write({head_marker!r} + chr(10))\n"
        "sys.stderr.write('noise ' * 1000)\n"  # >> _MAX_COMPATIBILITY_STDERR_MESSAGE_BYTES
        f"sys.stderr.write({tail_marker!r} + chr(10))\n"
        "sys.stderr.flush()\n"
        "raise SystemExit(2)\n"
    )
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(source),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "resource_exhausted"
    message = str(excinfo.value)
    assert tail_marker in message, message
    assert head_marker not in message, message
    assert len(message) < 1024, len(message)


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


# --------------------------------------------------------------------------
# CHAOS-4316: progress-based liveness bound on ComputePartition. Prod
# incident (deploy-4 readback, 2026-08-26): a hung worker_metrics_runner
# child on api-2 held a partition `executing` for 74 minutes with no
# progress and no wall-clock bound, pinning that replica's only slot. These
# tests use tiny, monkeypatched stall windows/poll interval so the whole
# suite runs in well under a second while proving the SAME mechanism that
# would fire on a real multi-minute hang.
# --------------------------------------------------------------------------


def _set_tiny_stall_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    base_seconds: float = 0.3,
    per_repo_seconds: float = 0.0,
    hard_ceiling_multiplier: float = 3.0,
) -> None:
    monkeypatch.setenv(
        "DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_BASE_SECONDS", str(base_seconds)
    )
    monkeypatch.setenv(
        "DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_PER_REPO_SECONDS",
        str(per_repo_seconds),
    )
    monkeypatch.setenv(
        "DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_HARD_CEILING_MULTIPLIER",
        str(hard_ceiling_multiplier),
    )
    # The watchdog's own poll interval is not env-configurable (it is a
    # constant sized for production, not a knob operators should tune) --
    # monkeypatch the module attribute directly so these tests still run
    # fast. worker_metrics._run_compatibility_process_locked reads this at
    # call time (not as a bound default), so the patch takes effect.
    monkeypatch.setattr(worker_metrics, "_PROGRESS_STALL_WATCHDOG_POLL_SECONDS", 0.01)


@pytest.mark.asyncio
async def test_metric_compatibility_process_stalled_child_is_killed_and_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The falsifier for CHAOS-4316: a child that reads its input and then
    reports ZERO progress, forever, must be killed within the derived stall
    window rather than holding the slot indefinitely (today's bug on
    origin/main -- runWithLeaseRenewal has nothing that would ever return
    here). Zero progress ever observed -> safe_to_retry."""
    _set_tiny_stall_env(monkeypatch)
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command("import json, sys, time; json.load(sys.stdin); time.sleep(60)"),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "progress_stalled"
    assert excinfo.value.safe_to_retry is True


@pytest.mark.asyncio
async def test_metric_compatibility_process_stalled_after_progress_is_not_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same stall, but one repo's progress line already arrived -- must stay
    conservative (ambiguous-eligible), matching CHAOS-4264's existing
    safe_to_retry contract exactly (this ticket reuses that rule verbatim,
    it does not introduce a new one). Writes the NDJSON progress line
    directly (the exact wire shape worker_metrics_runner._emit_progress
    produces) rather than importing that module, so this test's timing
    depends only on bare subprocess startup, not dev_health_ops's own
    (heavy, ~0.5-1s) import chain."""
    _set_tiny_stall_env(monkeypatch)
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, sys, time\n"
            "json.load(sys.stdin)\n"
            "sys.__stdout__.write(json.dumps({'progress': {'repo_index': 1, 'repo_count': 2}}) + chr(10))\n"
            "sys.__stdout__.flush()\n"
            "time.sleep(60)\n"
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "progress_stalled"
    assert excinfo.value.safe_to_retry is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_liveness_kill_only_scoped_to_daily_partition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A remaining-metrics family (no progress instrumentation, CHAOS-4331 is
    the follow-up to add one) must NOT be killed by this watchdog -- there is
    no signal to derive a bound from, and a guessed one would be exactly the
    anti-pattern this ticket avoids. Uses a bounded sleep (not 60s) so the
    test still finishes quickly while proving no liveness kill occurred."""
    _set_tiny_stall_env(monkeypatch, base_seconds=0.02)
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, sys, time; json.load(sys.stdin); time.sleep(0.2); "
            "raise SystemExit(1)"
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_execution())
    assert excinfo.value.reason == "process_failed", (
        "a remaining-metrics execution must fall through to the ordinary "
        "CHAOS-4264 exit-code classification, never progress_stalled"
    )


@pytest.mark.asyncio
async def test_metric_compatibility_process_hard_ceiling_fires_despite_trickling_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backstop half of CHAOS-4316: a child that emits progress often
    enough to dodge the per-interval stall check must still be reclaimed by
    the hard ceiling (base * multiplier) rather than running unbounded
    forever. Distinguishable from a real stall in the local Prometheus
    counter's reason label (wire-level reason to Go is progress_stalled
    either way -- Go's retry decision does not need the finer cut). Writes
    the NDJSON progress line directly (see the sibling test above) so the
    0.03s emit interval is not swamped by dev_health_ops's own import time."""
    _set_tiny_stall_env(
        monkeypatch, base_seconds=0.3, hard_ceiling_multiplier=2.0
    )  # hard ceiling = 0.6s
    before = worker_metrics.DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL.labels(
        reason="timeout"
    )._value.get()
    silence_before = (
        worker_metrics.DEV_HEALTH_METRIC_COMPAT_CHILD_SILENCE_SECONDS.labels(
            reason="timeout"
        )._sum.get()
    )
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, sys, time\n"
            "json.load(sys.stdin)\n"
            "while True:\n"
            "    sys.__stdout__.write(json.dumps({'progress': {'repo_index': 1, 'repo_count': 1}}) + chr(10))\n"
            "    sys.__stdout__.flush()\n"
            "    time.sleep(0.03)\n"  # well under the 0.3s stall window
        ),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "progress_stalled"
    assert excinfo.value.safe_to_retry is False  # progress was seen
    after = worker_metrics.DEV_HEALTH_METRIC_COMPAT_LIVENESS_KILL_TOTAL.labels(
        reason="timeout"
    )._value.get()
    assert after == before + 1, (
        "the hard-ceiling kill must be counted with reason='timeout', "
        "distinct from an ordinary interval stall"
    )
    silence_after = (
        worker_metrics.DEV_HEALTH_METRIC_COMPAT_CHILD_SILENCE_SECONDS.labels(
            reason="timeout"
        )._sum.get()
    )
    assert silence_after > silence_before, (
        "child-silence-seconds must be labeled by reason (codex review P2) "
        "so 'stalled' and 'timeout' samples are queryable separately, "
        "matching the metric's own help text"
    )


def test_liveness_ceiling_derived_from_repo_count_not_a_flat_number() -> None:
    """Standing rule: timeouts never fix capacity races. Both bounds must
    scale with the partition's own repo_count, never be a fixed constant."""
    small = worker_metrics._progress_stall_window_seconds(1)
    large = worker_metrics._progress_stall_window_seconds(20)
    assert large > small
    assert worker_metrics._progress_hard_ceiling_seconds(
        1
    ) > worker_metrics._progress_stall_window_seconds(1)


def test_progress_stall_watchdog_enabled_by_default() -> None:
    """Team-lead ruling 2026-08-26: on by default, since deployed
    configuration never sets DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_BASE_
    SECONDS -- an opt-in design would silently never activate in prod."""
    assert worker_metrics._progress_stall_watchdog_enabled() is True


def test_progress_stall_watchdog_explicit_zero_opts_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEV_HEALTH_METRICS_RUNNER_PROGRESS_STALL_BASE_SECONDS", "0")
    assert worker_metrics._progress_stall_watchdog_enabled() is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_explicit_opt_out_never_kills_a_stalled_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even with a stall window tiny enough to kill within milliseconds,
    forcing _progress_stall_watchdog_enabled() False must let a
    zero-progress child run to its own completion -- proves the opt-out
    actually disables the watchdog's wiring (liveness_watched), not just
    the env-lookup helper tested in isolation above. The tiny window is
    the point: if the opt-out wiring were broken, the child would be
    killed well before its own (deliberately longer) sleep completes."""
    _set_tiny_stall_env(monkeypatch, base_seconds=0.02)
    monkeypatch.setattr(
        worker_metrics, "_progress_stall_watchdog_enabled", lambda: False
    )
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(
            "import json, sys, time\n"
            "json.load(sys.stdin)\n"
            "time.sleep(0.15)\n"
            "print(json.dumps({'outcome': {'family': 'daily', 'rows': 0}}))\n"
        ),
    )
    result = await worker_metrics._run_compatibility_process(_daily_execution())
    assert result == {"family": "daily", "rows": 0}
