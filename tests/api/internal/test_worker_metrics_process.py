from __future__ import annotations

import asyncio
import json
import logging
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
    """A daily/partition execution.

    CHAOS-3092 (2026-09-07): worker_kind == "daily" no longer flows through
    _run_compatibility_process at all in production -- the Go daily
    worker's Python compatibility bridge (execute_daily_metrics,
    _load_daily_execution, _run_daily_direct) is deleted outright, every
    daily family being native Go now. Kept as a test fixture: it still
    exercises the shared subprocess-classification machinery
    (_run_compatibility_process_locked, _CompatibilityProcessFailure) with a
    worker_kind value distinct from "remaining", which several tests below
    still use to prove that machinery is worker_kind-agnostic. Historically
    (CHAOS-4264 codex R2) this was "the only worker_kind whose progress
    signal is real"; safe_to_retry is unconditionally False now regardless
    of worker_kind (see _run_compatibility_process_locked)."""
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
    """A daily/finalize execution.

    CHAOS-3092 (2026-09-07): see _daily_execution's docstring -- this
    worker_kind/operation combination no longer flows through
    _run_compatibility_process in production either; kept as a fixture for
    the same reason. safe_to_retry is unconditionally False now."""
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
    result, live_logged = await worker_metrics._read_bounded_stderr(
        stream, maximum_bytes=20
    )
    assert result.endswith(b"_TAIL_MARKER")
    assert b"HEAD_MARKER_" not in result
    assert result.startswith(b"...(truncated)\n")
    assert live_logged is False


@pytest.mark.asyncio
async def test_read_bounded_stderr_returns_everything_under_the_bound_untouched() -> (
    None
):
    stream = asyncio.StreamReader()
    stream.feed_data(b"short diagnostic line")
    stream.feed_eof()
    result, live_logged = await worker_metrics._read_bounded_stderr(
        stream, maximum_bytes=4096
    )
    assert result == b"short diagnostic line"
    assert b"truncated" not in result
    assert live_logged is False


@pytest.mark.asyncio
async def test_read_bounded_stderr_logs_each_chunk_live_when_context_given(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Codex review (CHAOS-4543 round 3): before this ticket, stderr was
    inherited (stderr=None), so an operator watching this container's own
    log stream in real time could see a child's diagnostics AS THEY
    HAPPENED, including while it was still running or hanging. Piping it
    instead (so a failure's message can embed a bounded tail) made this
    function the ONLY reader -- without live logging, nothing would appear
    until the whole run finished, and a genuinely hung child (the case an
    operator most needs live output for) would produce total silence until
    it was eventually killed. `live_log_context`, when given, must log each
    chunk via `logger.info` AS IT ARRIVES, independent of the final bounded
    tail this function still returns."""
    stream = asyncio.StreamReader()
    stream.feed_data(b"first chunk of diagnostic output\n")
    stream.feed_eof()
    with caplog.at_level(
        logging.INFO, logger="dev_health_ops.api.internal.worker_metrics"
    ):
        result, live_logged = await worker_metrics._read_bounded_stderr(
            stream, maximum_bytes=4096, live_log_context="run_id=test partition_id=test"
        )
    assert result == b"first chunk of diagnostic output\n"
    assert live_logged is True
    live_records = [r for r in caplog.records if "stderr chunk" in r.message]
    assert len(live_records) == 1, caplog.records
    assert "run_id=test partition_id=test" in live_records[0].message
    assert "first chunk of diagnostic output" in live_records[0].message


@pytest.mark.asyncio
async def test_read_bounded_stderr_logs_nothing_live_without_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Sibling of the test above: live_log_context=None (the default) must
    not log anything -- callers that only want the bounded tail (e.g. the
    focused unit tests above) must not pay for or produce log noise."""
    stream = asyncio.StreamReader()
    stream.feed_data(b"some output\n")
    stream.feed_eof()
    with caplog.at_level(
        logging.INFO, logger="dev_health_ops.api.internal.worker_metrics"
    ):
        _, live_logged = await worker_metrics._read_bounded_stderr(
            stream, maximum_bytes=4096
        )
    assert live_logged is False
    assert not any("stderr chunk" in r.message for r in caplog.records)


def test_metric_process_payload_round_trips_only_durable_execution_fields() -> None:
    execution = _execution()
    payload = worker_metrics._execution_process_payload(execution)

    # CHAOS-3092: "skip_families" is deleted from this set -- the field
    # existed only to serve the Go daily worker's now-fully-deleted Python
    # compatibility bridge (execute_daily_metrics). The sibling test that
    # used to round-trip a non-empty skip_families value
    # (test_metric_process_payload_round_trips_a_non_empty_skip_families)
    # is deleted with it.
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
async def test_metric_compatibility_process_signal_kill_with_no_progress_is_not_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No progress line was ever emitted, and the process was killed by a
    signal (the actual CHAOS-4264 production shape: SIGKILL, return_code<0).

    CHAOS-4264 originally made this safe to hand straight back to River
    (skipping the ambiguous state) ONLY for a daily/partition execution --
    the sole worker_kind with real per-scope write evidence (codex R2).
    CHAOS-3092 (2026-09-07): that daily bridge path is deleted outright
    (every daily family is native Go now), so safe_to_retry is
    unconditionally False for every execution this function can still
    reach -- including this once-True daily case, kept as a fixture to pin
    the new behavior explicitly rather than relying only on
    test_metric_compatibility_process_remaining_metrics_never_safe_to_retry."""
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
    assert excinfo.value.safe_to_retry is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_remaining_metrics_never_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex R2 finding (CHAOS-4264): remaining-metrics families
    (capacity/complexity/dora/recommendations/membership_backfill --
    release_impact deleted, CHAOS-5234) never emit progress at all, so
    "zero progress"
    would be indistinguishable from "wrote one repo's rows then crashed" --
    a fabricated safety claim, not an observed one. Even a signal-killed
    remaining execution with zero progress lines must stay unsafe to
    auto-retry. CHAOS-3092: safe_to_retry is unconditionally False now (the
    daily bridge path this used to carve an exception out for is deleted
    outright), so this is no longer a worker_kind-specific rule -- kept as
    its own test since it exercises the family this function actually
    dispatches to in production today."""
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
    progress callback -- worker_kind == "daily" alone was never a safe gate,
    finalize also needed operation == "partition". CHAOS-3092: the daily
    bridge path is deleted outright now, so safe_to_retry is unconditionally
    False regardless of worker_kind/operation -- a signal-killed finalize
    with zero progress lines still must stay unsafe to auto-retry, now for
    the simpler reason that nothing is ever safe_to_retry any more."""
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
async def test_metric_compatibility_process_generic_failure_with_no_progress_is_not_safe_to_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4264 originally authorized safe_to_retry here for a
    daily/partition execution with zero progress lines. CHAOS-3092
    (2026-09-07): the daily bridge path is deleted outright, so
    safe_to_retry is unconditionally False now -- kept as a fixture pinning
    the new behavior on this once-True case."""
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command("raise SystemExit(1)"),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "process_failed"
    assert excinfo.value.safe_to_retry is False


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


@pytest.mark.asyncio
async def test_metric_compatibility_process_stderr_not_logged_twice_when_live_streamed(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Codex review (CHAOS-4543 round 3, P3): _run_compatibility_process_locked
    always passes live_log_context to _read_bounded_stderr, so every child's
    stderr was logged once live (INFO, per chunk) AND once more in full in
    the post-exit summary (WARNING/ERROR) -- the same diagnostic text
    appearing as two separate log records, which a text-matching alert can
    read as two distinct failures. The post-exit summary must still carry
    severity + correlation ids, but must not repeat text already streamed
    live.
    """
    marker = "distinct_diagnostic_marker_should_appear_exactly_once"
    source = (
        "import sys\n"
        f"sys.stderr.write({marker!r} + chr(10))\n"
        "sys.stderr.flush()\n"
        "raise SystemExit(2)\n"
    )
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(source),
    )
    with caplog.at_level(
        logging.INFO, logger="dev_health_ops.api.internal.worker_metrics"
    ):
        with pytest.raises(worker_metrics._CompatibilityProcessFailure):
            await worker_metrics._run_compatibility_process(_daily_execution())

    marker_records = [r for r in caplog.records if marker in r.message]
    assert len(marker_records) == 1, (
        "expected the diagnostic text to appear in exactly one log record "
        f"(live-streamed only), got {len(marker_records)}: {caplog.records}"
    )
    assert marker_records[0].levelno == logging.INFO

    summary_records = [
        r for r in caplog.records if "already streamed live" in r.message
    ]
    assert len(summary_records) == 1, caplog.records
    assert summary_records[0].levelno == logging.ERROR
    assert marker not in summary_records[0].message


@pytest.mark.asyncio
async def test_metric_compatibility_process_classifies_testops_row_cap_as_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-4543 red-first proof (team-lead/codex direction): the runner
    must classify a TestopsRowCapExceeded failure with the NEW
    EXIT_RESOURCE_EXHAUSTED_DETERMINISTIC exit code (3), not the generic
    EXIT_RESOURCE_EXHAUSTED (2) every other MemoryError -- including the RSS
    watchdog's own kill and a true last-gasp interpreter MemoryError, both of
    which CAN legitimately vary attempt to attempt -- uses. The parent must
    then surface `deterministic=True` on the raised
    _CompatibilityProcessFailure, which crosses the HTTP boundary as a
    bounded bool for ops/internal/jobs/metrics/daily/compatibility_http.go to
    read (never raw text) and classify the Go-side job Permanent instead of
    Retryable (5 retries on a deterministic guard only reproduce the
    identical refusal).

    Drives the REAL worker_metrics_runner module (not a synthetic stand-in)
    with `_run_execution_direct` monkeypatched to raise TestopsRowCapExceeded
    synchronously -- exercises main()'s actual except-clause ORDERING (the
    deterministic branch must be checked before the generic
    `except MemoryError:`, since TestopsRowCapExceeded IS a MemoryError
    subclass and would otherwise be caught by the wrong branch first).
    """
    source = (
        "import sys\n"
        "from dev_health_ops.api.internal import worker_metrics_runner as runner\n"
        "from dev_health_ops.metrics.loaders.clickhouse import TestopsRowCapExceeded\n"
        "\n"
        "def _boom(execution, on_progress=None):\n"
        "    raise TestopsRowCapExceeded(\n"
        "        table='test_case_results', org_id='x', max_rows=200000, fetched=200001\n"
        "    )\n"
        "\n"
        "runner._run_execution_direct = _boom\n"
        "sys.exit(runner.main())\n"
    )
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(source),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "resource_exhausted"
    assert excinfo.value.deterministic is True
    # CHAOS-3092: unconditionally False now (was True here pre-CHAOS-3092,
    # since zero progress was emitted before the raise on a daily/partition
    # execution) -- the daily bridge path is deleted outright.
    assert excinfo.value.safe_to_retry is False


@pytest.mark.asyncio
async def test_metric_compatibility_process_generic_memory_error_is_not_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sibling of the test above: a plain MemoryError (NOT
    TestopsRowCapExceeded, or any other class CHAOS-4543 knows is a
    deterministic guard) must keep the ORIGINAL exit code and
    `deterministic=False` -- this is the conservative fallback the ticket's
    own scope commits to (team-lead: 'if you cannot distinguish it from a
    real RSS kill at the Go boundary, say so and keep retryable')."""
    source = (
        "import sys\n"
        "from dev_health_ops.api.internal import worker_metrics_runner as runner\n"
        "\n"
        "def _boom(execution, on_progress=None):\n"
        "    raise MemoryError('a generic, unclassified memory failure')\n"
        "\n"
        "runner._run_execution_direct = _boom\n"
        "sys.exit(runner.main())\n"
    )
    monkeypatch.setattr(
        worker_metrics,
        "_COMPATIBILITY_RUNNER_COMMAND",
        _runner_command(source),
    )
    with pytest.raises(worker_metrics._CompatibilityProcessFailure) as excinfo:
        await worker_metrics._run_compatibility_process(_daily_execution())
    assert excinfo.value.reason == "resource_exhausted"
    assert excinfo.value.deterministic is False
