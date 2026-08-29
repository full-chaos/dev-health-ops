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
# CHAOS-4543: a KNOWN deterministic resource guard (today, only the testops
# loader's row-cap -- TestopsRowCapExceeded, a fixed per-repo/day threshold
# against data that does not change within a day once synced), distinct from
# EXIT_RESOURCE_EXHAUSTED's generic bucket (a true interpreter-level
# MemoryError, or the parent's own RSS-watchdog kill, both of which CAN
# legitimately vary attempt to attempt under different concurrent load). The
# parent maps this to a bounded "deterministic": true field across the HTTP
# boundary (never raw text) so the Go side can stop retrying a refusal that
# will reproduce identically 5 times before River discards the job anyway --
# see ops/internal/jobs/metrics/daily/compatibility_http.go's
# ErrCompatibilityResourceExhaustedDeterministic.
EXIT_RESOURCE_EXHAUSTED_DETERMINISTIC = 3

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

# CHAOS-4361: RLIMIT_AS is no longer the primary enforcement -- it counts
# virtual ADDRESS SPACE (thread stacks, malloc arenas, every mmap mapping
# clickhouse_connect's C driver makes), not resident memory. The 2026-08-27
# prod incident hit MemoryError inside
# clickhouse_connect/driverc/buffer.pyx's ResponseBuffer.read_str_col with
# RLIMIT_AS==640 MiB while a local direct run of the SAME day (no rlimit)
# measured only 465 MiB RSS -- the rlimit was firing on address-space
# bookkeeping, not on real memory pressure. Real enforcement now happens in
# the PARENT (worker_metrics._run_compatibility_process_locked's RSS
# watchdog), which polls this process's actual VmRSS via /proc and kills it
# on a real breach of DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES -- see
# that module's docstring on _poll_peak_rss_bytes.
#
# This self-imposed RLIMIT_AS stays only as a generous backstop: the
# parent's poll interval (0.25s) means a sufficiently explosive allocation
# spike could exceed the real budget before the parent observes and kills
# it. A multiplier well above the configured RSS budget still converts that
# pathological case into a MemoryError THIS process can catch and report
# (EXIT_RESOURCE_EXHAUSTED) rather than an un-classified kernel OOM kill of
# the whole api container -- without re-introducing the false-positive rate
# that motivated this ticket. RLIMIT_DATA is dropped entirely: it bounds
# only the brk/sbrk heap, and glibc's malloc routes any allocation at or
# above its mmap threshold (128 KiB by default) through mmap instead --
# exactly the class of large String/bytes buffer clickhouse_connect
# materializes per column, so RLIMIT_DATA was never actually a backstop for
# this failure mode, only a false sense of one.
#
# codex R1 (PR #1940): the raw multiplier alone is not enough -- at the
# 640 MiB default this is 2.5 GiB, which EXCEEDS the smallest documented
# container limit (1G, shared `api` service in
# deploy/docker-compose/compose.production.yml). A backstop bigger than the
# container it runs in can never fire before the kernel's own memcg OOM
# killer does, silently reintroducing the un-classified-kill problem
# CHAOS-4264 exists to close. _cgroup_memory_max_bytes reads this
# container's REAL cgroup v2 ceiling (when available -- cgroup v1, no
# permission, or a non-container dev run all return None) and the backstop
# is clamped to leave headroom for the parent api process.
#
# codex R2 (PR #1940): clamping alone over-corrected -- for the SAME 1G
# container this arithmetic clamps the backstop down to exactly 640 MiB,
# equal to the RSS limit itself, reintroducing the EXACT false positive
# this whole ticket exists to close (RLIMIT_AS firing on address-space
# overhead for a compute that never approached the real RSS budget; prod
# fired at 640 MiB RLIMIT_AS while real RSS was only 465 MiB, a ~1.38x
# overhead ratio for that one incident alone -- other workloads with more
# thread/arena overhead could need more). So the backstop must never drop
# below `_RLIMIT_AS_BACKSTOP_MIN_MULTIPLIER` x the RSS limit, REGARDLESS of
# the container-derived ceiling: under the smallest documented containers
# (1G shared api, 2G dedicated metrics-api with a 1.25G RSS limit) there is
# simply not enough total slack to satisfy both the API's own headroom
# reservation and a safe address-space margin at the same time. When that
# happens, this prioritizes NOT reintroducing the false-positive bug (a
# certain, everyday failure) over guaranteeing the backstop fires before
# an unclassified kernel OOM for a rare spike-between-polls case (not a
# new regression for that container size -- the same residual risk that
# existed before CHAOS-4264 introduced any classified bound at all) --
# falling back to the plain multiplier and logging so it's operator
# visible, rather than silently shrinking into unsafe territory.
_RLIMIT_AS_BACKSTOP_MULTIPLIER = 4
_RLIMIT_AS_BACKSTOP_MIN_MULTIPLIER = 1.5
_RLIMIT_AS_BACKSTOP_CONTAINER_HEADROOM_BYTES = 384 * 1024 * 1024  # 384 MiB


def _configured_memory_limit_bytes() -> int:
    raw = os.environ.get(_MEMORY_LIMIT_ENV_KEY, "").strip()
    if not raw:
        return _DEFAULT_MEMORY_LIMIT_BYTES
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_MEMORY_LIMIT_BYTES
    return value if value > 0 else _DEFAULT_MEMORY_LIMIT_BYTES


def _cgroup_memory_max_bytes() -> int | None:
    """Read this container's real cgroup v2 memory ceiling, if observable.

    Returns None (not a sentinel int) for every case where the value cannot
    be trusted: cgroup v1, no permission, the file absent (non-container
    dev run), or an unbounded "max" ceiling -- callers must fall back to
    the plain multiplier in all of these, not treat None as "no limit
    applies."
    """
    try:
        with open("/sys/fs/cgroup/memory.max", encoding="ascii") as handle:
            raw = handle.read().strip()
    except (FileNotFoundError, PermissionError, OSError):
        return None
    if raw == "max":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _rlimit_as_backstop_bytes() -> int:
    """The RLIMIT_AS backstop: the configured multiplier, clamped to leave
    headroom under this container's real cgroup ceiling when one is
    observable -- but NEVER below ``_RLIMIT_AS_BACKSTOP_MIN_MULTIPLIER`` x
    the RSS limit, or the clamp itself reintroduces the false-positive bug
    this ticket exists to close. See the module-level comment above
    ``_RLIMIT_AS_BACKSTOP_MULTIPLIER`` for the full reasoning and the
    codex R1/R2 history behind both constraints."""
    configured = _configured_memory_limit_bytes()
    preferred = configured * _RLIMIT_AS_BACKSTOP_MULTIPLIER
    minimum_safe = int(configured * _RLIMIT_AS_BACKSTOP_MIN_MULTIPLIER)
    cgroup_max = _cgroup_memory_max_bytes()
    if cgroup_max is None:
        return preferred
    ceiling = cgroup_max - _RLIMIT_AS_BACKSTOP_CONTAINER_HEADROOM_BYTES
    if ceiling < minimum_safe:
        # This container cannot fit both the api headroom reservation and a
        # safe address-space margin above the RSS limit -- see the R2
        # comment above. Falling back to the plain multiplier (uncapped by
        # this container's ceiling) is the safer of two imperfect options.
        print(
            "metric compatibility runner: cgroup ceiling "
            f"({cgroup_max} bytes) leaves no room for a safe RLIMIT_AS "
            f"backstop above the configured RSS limit ({configured} "
            f"bytes) after reserving "
            f"{_RLIMIT_AS_BACKSTOP_CONTAINER_HEADROOM_BYTES} bytes for the "
            "api process -- falling back to the unclamped multiplier; "
            "consider raising the container's memory limit or lowering "
            "DEV_HEALTH_METRICS_RUNNER_MEMORY_LIMIT_BYTES",
            file=sys.stderr,
        )
        return preferred
    return min(preferred, ceiling)


def _apply_memory_limit() -> None:
    """Set a generous RLIMIT_AS backstop; real enforcement is the parent's.

    See ``_rlimit_as_backstop_bytes`` for how the backstop is derived.
    POSIX-only: the ``resource`` module does not exist on Windows, and this
    process only ever runs inside the Linux container image.
    """
    try:
        import resource
    except ImportError:
        return
    limit = _rlimit_as_backstop_bytes()
    rlimit = getattr(resource, "RLIMIT_AS", None)
    if rlimit is None:
        return
    with contextlib.suppress(ValueError, OSError):
        current_soft, current_hard = resource.getrlimit(rlimit)
        hard = current_hard if current_hard != resource.RLIM_INFINITY else limit
        resource.setrlimit(rlimit, (min(limit, hard), hard))


def _deterministic_resource_exhaustion_classes() -> tuple[type[BaseException], ...]:
    """The closed set of exception classes CHAOS-4543 knows are a
    deterministic resource guard, never a real attempt-to-attempt-variable
    memory kill -- see EXIT_RESOURCE_EXHAUSTED_DETERMINISTIC's module-level
    comment. Lazily imported (matching this module's existing lazy-import
    style for compute-path dependencies -- see main()'s own
    _run_execution_direct call) so a success/generic-failure run never pays
    for importing the loader module at all.
    """
    from dev_health_ops.metrics.loaders.clickhouse import TestopsRowCapExceeded

    return (TestopsRowCapExceeded,)


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
    except _deterministic_resource_exhaustion_classes():
        # CHAOS-4543: caught BEFORE the generic `except MemoryError:` below
        # (Python matches except clauses in source order; every class this
        # returns is itself a MemoryError subclass, so this branch would
        # never be reached if it came second). The specific diagnostic text
        # (table/org_id/max_rows/fetched) was already logged by the raising
        # module via `logger.error` before this exception ever reached here
        # -- redirected to this process's real stderr the same way this
        # print is, and captured/embedded by the parent
        # (worker_metrics._run_compatibility_process_locked). Still no
        # str(exc) here: consistent with the MemoryError branch below, this
        # avoids allocating right at (or near) the process's own memory
        # ceiling; the parent's stderr capture already has the real detail.
        with contextlib.suppress(Exception):
            print(
                "metric compatibility process exceeded its memory bound "
                "(deterministic guard)",
                file=sys.stderr,
            )
        return EXIT_RESOURCE_EXHAUSTED_DETERMINISTIC
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
