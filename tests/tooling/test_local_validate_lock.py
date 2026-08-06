"""Regression coverage for ci/local_validate.sh's single-flight lock (CHAOS-3403).

Before this lock, ``ci/local_validate.sh`` was single-flight by convention only:
every operator/agent had to ``ps aux | grep local_validate`` before launching --
a time-of-check-to-time-of-use race. One collision had to be killed by hand and
two near-misses were caught only by a human watching ``ps`` in real time.

These tests race the REAL ``acquire_lock``/``release_lock``/``reclaim_stale_lock``
functions in local_validate.sh via its ``--lock-probe`` test-only harness hook --
not a reimplementation and not a mock. ``--lock-probe`` skips preflight, lint,
mypy, and every ClickHouse-touching stage, so the race is cheap; what remains IS
the production lock-acquisition code path (see the hook's comment in
local_validate.sh, and ci/check_go.sh's "integration-coverage" verb for the same
precedent of a narrow harness-only verb backing a pytest regression test).

Every test below points LOCK_DIR at a pytest ``tmp_path`` so it can never collide
with a real gate's lock (which defaults to a path keyed on CH_CONTAINER under
/tmp) -- these tests are safe to run even while a real gate is in flight
elsewhere on the host.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "local_validate.sh"


def _spawn_probe(lock_dir: Path, hold_secs: float, wait_secs: int = 20):
    return subprocess.Popen(
        ["bash", str(SCRIPT), "--lock-probe", str(hold_secs)],
        cwd=ROOT,
        env={
            "LOCK_DIR": str(lock_dir),
            "LOCK_WAIT_SECS": str(wait_secs),
            "LOCK_POLL_SECS": "1",
            "PATH": "/usr/bin:/bin:/usr/local/bin",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _spawn_probe_with_env(hold_secs: float, wait_secs: int, extra_env: dict):
    """Like _spawn_probe, but does NOT set LOCK_DIR -- lets the script compute
    its own default, with whatever extra env (e.g. a distinct TMPDIR) the
    caller supplies layered on top of a minimal base."""
    env = {
        "LOCK_WAIT_SECS": str(wait_secs),
        "LOCK_POLL_SECS": "1",
        "PATH": "/usr/bin:/bin:/usr/local/bin",
    }
    env.update(extra_env)
    return subprocess.Popen(
        ["bash", str(SCRIPT), "--lock-probe", str(hold_secs)],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def test_two_concurrent_invocations_serialize_not_both_run(tmp_path):
    """The actual race: launch two invocations near-simultaneously.

    A sequential-invocation test would prove nothing -- sequential already works
    today without any lock. This launches both processes with no ordering
    guarantee and asserts exactly one acquired the lock immediately while the
    other observed "already running" and blocked until the first released --
    i.e. real mutual exclusion, not two independent runs that happened not to
    collide.
    """
    lock_dir = tmp_path / "race.lock"
    hold_secs = 3

    proc_a = _spawn_probe(lock_dir, hold_secs)
    proc_b = _spawn_probe(lock_dir, hold_secs)

    started_at = time.monotonic()
    out_a = proc_a.communicate(timeout=30)[0]
    out_b = proc_b.communicate(timeout=30)[0]
    elapsed = time.monotonic() - started_at

    assert proc_a.returncode == 0, out_a
    assert proc_b.returncode == 0, out_b

    # If the lock did nothing, both would finish in ~hold_secs (they'd run
    # concurrently). Real serialization means the second-to-acquire waits out
    # the first's full hold before it even starts its own -- total wall time is
    # bounded below by 2x hold_secs, not ~1x.
    assert elapsed >= 2 * hold_secs - 0.5, (
        f"expected serialized execution (>= {2 * hold_secs}s), got {elapsed:.1f}s "
        f"-- both invocations may have run concurrently instead of being mutually "
        f"exclusive.\nA:\n{out_a}\nB:\n{out_b}"
    )

    # Exactly one of the two logs must show it waited on the other.
    waited_count = sum(1 for out in (out_a, out_b) if "already running" in out)
    acquired_count = sum(out.count("lock-probe: acquired") for out in (out_a, out_b))
    assert waited_count == 1, f"expected exactly one waiter.\nA:\n{out_a}\nB:\n{out_b}"
    assert acquired_count == 2, (
        f"expected both to eventually acquire.\nA:\n{out_a}\nB:\n{out_b}"
    )

    # The lock must be fully released after both complete -- no leaked directory.
    assert not lock_dir.exists(), "lock directory leaked after both probes exited"


def test_stale_lock_from_kill_9_is_reclaimed_not_wedged(tmp_path):
    """A run killed with kill -9 must not wedge every future run.

    mkdir-based locks (unlike flock) have no release-on-process-death, so this
    is the property that actually needs proving: after a SIGKILL leaves the
    lock directory behind with a now-dead PID recorded inside it, the next
    invocation must reclaim it promptly (via PID liveness) rather than waiting
    out the full LOCK_WAIT_SECS timeout or hanging forever.
    """
    lock_dir = tmp_path / "stale.lock"

    holder = _spawn_probe(lock_dir, hold_secs=60, wait_secs=5)
    # Give it time to actually acquire before we kill it.
    deadline = time.monotonic() + 10
    while not lock_dir.exists() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert lock_dir.exists(), "holder never acquired the lock to begin with"

    holder.kill()  # SIGKILL -- no EXIT trap runs; lock_dir is left behind.
    holder.wait(timeout=10)
    assert lock_dir.exists(), (
        "lock directory should still exist post-kill-9 (no auto-release)"
    )

    # A fresh probe, with a short LOCK_WAIT_SECS, must reclaim near-instantly --
    # NOT time out. If reclamation were broken this would hang for wait_secs.
    started_at = time.monotonic()
    fresh = _spawn_probe(lock_dir, hold_secs=0.1, wait_secs=5)
    out = fresh.communicate(timeout=15)[0]
    elapsed = time.monotonic() - started_at

    assert fresh.returncode == 0, out
    assert "reclaiming" in out, f"expected stale-lock reclamation message.\n{out}"
    assert elapsed < 5, (
        f"reclamation took {elapsed:.1f}s -- looks like it waited out LOCK_WAIT_SECS instead of reclaiming"
    )
    assert not lock_dir.exists(), (
        "lock directory leaked after the reclaiming probe exited"
    )


def test_default_lock_dir_ignores_tmpdir_and_still_serializes(tmp_path):
    """The default LOCK_DIR must NOT be derived from $TMPDIR.

    This whole epic is "the gate inherits the shell environment and that
    produces wrong results" -- keying host-wide mutual exclusion on an
    inherited env var would repeat exactly that class of bug: two agents with
    different ambient TMPDIR (a sandbox, a session-scoped scratch dir) would
    silently acquire two DIFFERENT lock directories and both proceed, with no
    error and no diagnostic.

    This does NOT override LOCK_DIR -- that would only prove the override
    works, not that the default is TMPDIR-independent. It exercises the
    script's real default-resolution logic with two DELIBERATELY DIFFERENT
    TMPDIR values and asserts they still serialize (i.e. resolve to the same
    path). CH_CONTAINER is pinned to a unique per-test value (not the real
    'dev-health-clickhouse-1') so this can never collide with a real gate
    running elsewhere on the host, even though it uses the real default path
    template under /tmp.
    """
    container = f"test-lockdir-tmpdir-{tmp_path.name}"
    hold_secs = 3
    tmpdir_a = tmp_path / "tmpdir_a"
    tmpdir_b = tmp_path / "tmpdir_b"
    tmpdir_a.mkdir()
    tmpdir_b.mkdir()
    expected_lock_dir = Path(f"/tmp/dev-health-ops-local-validate.{container}.lock")

    try:
        proc_a = _spawn_probe_with_env(
            hold_secs,
            wait_secs=20,
            extra_env={"TMPDIR": str(tmpdir_a), "CH_CONTAINER": container},
        )
        proc_b = _spawn_probe_with_env(
            hold_secs,
            wait_secs=20,
            extra_env={"TMPDIR": str(tmpdir_b), "CH_CONTAINER": container},
        )

        started_at = time.monotonic()
        out_a = proc_a.communicate(timeout=30)[0]
        out_b = proc_b.communicate(timeout=30)[0]
        elapsed = time.monotonic() - started_at

        assert proc_a.returncode == 0, out_a
        assert proc_b.returncode == 0, out_b

        # Same proof as the main race test: if the two different TMPDIRs had
        # produced two different lock paths, both would run concurrently and
        # finish in ~hold_secs instead of serializing to ~2x hold_secs.
        assert elapsed >= 2 * hold_secs - 0.5, (
            f"expected serialized execution (>= {2 * hold_secs}s) despite "
            f"different TMPDIR values, got {elapsed:.1f}s -- the default "
            f"LOCK_DIR may have picked up $TMPDIR after all.\nA:\n{out_a}\nB:\n{out_b}"
        )
        assert str(expected_lock_dir) in out_a + out_b, (
            f"expected the fixed /tmp default path in the logs.\nA:\n{out_a}\nB:\n{out_b}"
        )
    finally:
        subprocess.run(["rm", "-rf", str(expected_lock_dir)], check=False)
