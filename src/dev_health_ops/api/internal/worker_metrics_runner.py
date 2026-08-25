"""Fixed, killable process boundary for metric compatibility execution.

CHAOS-4264: this process is memory-bounded on its own (independent of the
parent api container's cgroup) so an oversized compute fails with a
classified MemoryError instead of relying solely on the kernel OOM killer,
which the parent cannot distinguish from any other SIGKILL.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import traceback
from typing import Any

from dev_health_ops.api.internal.worker_metrics import (
    _canonical_json,
    _execution_from_process_payload,
    _run_execution_direct,
)

_MAX_INPUT_BYTES = 1024 * 1024

# Exit codes read by the parent (worker_metrics._run_compatibility_process) to
# classify a failure without any stdout payload -- a signaled process (kernel
# OOM kill, SIGTERM) never gets to write anything, so the exit code is the
# only channel guaranteed to survive.
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_RESOURCE_EXHAUSTED = 2

_MEMORY_LIMIT_ENV_KEY = "DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES"
# CHAOS-4264 (codex R1): this must leave real headroom under the SMALLEST
# container memory limit this repo configures the api service with (1G in
# deploy/docker-compose/compose.production.yml) for the API process itself
# plus overhead -- a runner default equal to the container's own limit
# leaves zero room for its parent and does not reliably convert the
# incident class into a classified failure, it just moves the same OOM race
# one layer down. 640 MiB reserves ~384 MiB for the API process under a 1G
# container. Paired with worker_metrics._RUNNER_CONCURRENCY_SEMAPHORE
# (default max_concurrency=1), this is the WHOLE per-runner budget, not a
# per-runner slice of a larger aggregate. An org whose legitimate compute
# needs more than this will get a classified resource_exhausted instead of
# succeeding -- the correct operator response is to raise this env var
# explicitly for that deployment's actual container limit (prod's real
# limit is reportedly 2G on a host-only compose file this repo does not
# control -- see deploy/go-workers/README.md), not to bump this default.
_DEFAULT_MEMORY_LIMIT_BYTES = 640 * 1024 * 1024  # 640 MiB


def _configured_memory_limit_bytes() -> int:
    raw = os.environ.get(_MEMORY_LIMIT_ENV_KEY, "").strip()
    if not raw:
        return _DEFAULT_MEMORY_LIMIT_BYTES
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_MEMORY_LIMIT_BYTES
    return value if value > 0 else _DEFAULT_MEMORY_LIMIT_BYTES


def _apply_memory_limit() -> None:
    """Bound this process's address space so it fails loud, not silently.

    Enforced via RLIMIT_AS (and RLIMIT_DATA where distinct) rather than left
    to the container cgroup: the cgroup limit is shared with the parent api
    process (CHAOS-4264 -- the runner alone reached 1.7 GB inside a 2 GiB
    container), so a runaway compute here could still starve or kill the API.
    A self-imposed rlimit turns that into a MemoryError this process can
    catch and report, well before the container-wide ceiling is reached.
    POSIX-only: the ``resource`` module does not exist on Windows, and this
    process only ever runs inside the Linux container image.
    """
    try:
        import resource
    except ImportError:
        return
    limit = _configured_memory_limit_bytes()
    for kind in ("RLIMIT_AS", "RLIMIT_DATA"):
        rlimit = getattr(resource, kind, None)
        if rlimit is None:
            continue
        with contextlib.suppress(ValueError, OSError):
            current_soft, current_hard = resource.getrlimit(rlimit)
            hard = current_hard if current_hard != resource.RLIM_INFINITY else limit
            resource.setrlimit(rlimit, (min(limit, hard), hard))


def _payload() -> object:
    encoded = sys.stdin.buffer.read(_MAX_INPUT_BYTES + 1)
    if len(encoded) > _MAX_INPUT_BYTES:
        raise ValueError("metric compatibility process input exceeds the durable bound")
    return json.loads(encoded)


def _encode_outcome(outcome: dict[str, Any]) -> str:
    return _canonical_json({"outcome": outcome})


def _emit_progress(repo_index: int, repo_count: int) -> None:
    """Write one NDJSON progress line to the REAL stdout.

    Compute code below redirects ``sys.stdout`` to stderr for the duration of
    the run (see main()), so this writes through ``sys.__stdout__`` directly.
    The parent (worker_metrics._run_compatibility_process) reads the pipe
    incrementally: even if this process is SIGKILLed moments later, whatever
    progress lines were already flushed remain in the pipe buffer and are
    still visible to the parent after the kill. That is what lets the parent
    tell "nothing was computed yet" (safe to retry) apart from "at least one
    repository's families were already written" (conservative: stays
    ambiguous, same as before this ticket).
    """
    real_stdout = sys.__stdout__
    if real_stdout is None:
        return
    line = _canonical_json(
        {"progress": {"repo_index": repo_index, "repo_count": repo_count}}
    )
    with contextlib.suppress(BrokenPipeError, ValueError, OSError):
        real_stdout.write(line + "\n")
        real_stdout.flush()


def main() -> int:
    _apply_memory_limit()
    try:
        execution = _execution_from_process_payload(_payload())
        # Compatibility computations may write progress to stdout. Reserve it
        # for the fixed JSON protocol inherited by the parent bridge.
        with contextlib.redirect_stdout(sys.stderr):
            outcome = asyncio.run(
                _run_execution_direct(execution, on_progress=_emit_progress)
            )
        sys.stdout.write(_encode_outcome(outcome) + "\n")
        return EXIT_SUCCESS
    except MemoryError:
        # Do not build/print a traceback here: the process is already at its
        # memory ceiling, and traceback formatting itself allocates. A short
        # static message is enough -- the classified exit code is what the
        # parent actually keys its retry decision on.
        with contextlib.suppress(Exception):
            print(
                "metric compatibility process exceeded its memory bound",
                file=sys.stderr,
            )
        return EXIT_RESOURCE_EXHAUSTED
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return EXIT_FAILURE


if __name__ == "__main__":
    raise SystemExit(main())
