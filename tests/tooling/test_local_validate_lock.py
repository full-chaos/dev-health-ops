"""Regression coverage for ci/local_validate.sh's single-flight lock (CHAOS-3403).

Before this lock, ``ci/local_validate.sh`` was single-flight by convention only:
every operator/agent had to ``ps aux | grep local_validate`` before launching --
a time-of-check-to-time-of-use race. One collision had to be killed by hand and
two near-misses were caught only by a human watching ``ps`` in real time.

An adversarial review of the first version of this lock (mkdir + separate
pid/cwd files) found it was not actually atomic: a contender could see the
just-``mkdir``'d directory before its metadata files were written, conclude
"stale", and delete a lock its rightful owner had only just acquired -- the
exact failure this lock exists to prevent. The lock is now a single ``ln -s``
whose target string IS the fully-formed owner metadata (see the header
comment in local_validate.sh); there is no window where it exists but is
unpopulated. The tests below exercise that property directly, along with the
other findings from that review: PID-reuse resistance, refusal to touch an
unsafe ``LOCK_DIR``, and release-before-cleanup ordering in the exit trap.

These tests race the REAL ``acquire_lock``/``release_lock``/``reclaim_stale_lock``
functions in local_validate.sh via its ``--lock-probe``/``--lock-probe-exit-order``
test-only harness hooks -- not a reimplementation and not a mock. The hooks skip
preflight, lint, mypy, and every ClickHouse-touching stage, so the race is
cheap; what remains IS the production lock-acquisition code path (see the
hooks' comments in local_validate.sh, and ci/check_go.sh's
"integration-coverage" verb for the same precedent of a narrow harness-only
verb backing a pytest regression test).

The lock is a SYMLINK, not a directory -- ``lock_dir.exists()`` would silently
be wrong here (it follows the link and checks whether the target STRING,
which is metadata, not a path, exists on disk -- always False). Presence is
checked with ``is_symlink()`` throughout.

Every test below points LOCK_DIR at a pytest ``tmp_path`` (or a uniquely-named
default under /tmp, never the real ``dev-health-clickhouse-1``-keyed path) so
none of this can ever collide with a real gate's lock elsewhere on the host.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "local_validate.sh"
_BASE_PATH = "/usr/bin:/bin:/usr/local/bin"


# wait_secs default is large (900), not a tight budget: this test file runs
# as part of the FULL gate's own unit suite (~12000 tests under pytest-xdist
# -n4), and a run through the real gate hit a genuine TimeoutExpired at
# wait_secs=60 that never reproduced standalone or under deliberately
# reconstructed load (repeated direct measurements, including racing under
# two concurrent -n4 full-suite runs, all completed in ~6s). Investigated,
# not assumed, per this repo's own rule against just raising a timeout: in
# that failure, BOTH subprocess.communicate() calls returned without a
# Python-level TimeoutExpired, meaning the "loser" process was not stuck --
# it completed and released on its own, just far slower than its expected
# ~3s hold, consistent with a transient scheduling stall (that gate run's
# total suite time was 214s vs. 172s for a clean rerun -- a real, if
# unreproduced-on-demand, load spike) rather than a lost wakeup or hung
# lock. wait_secs=900 gives real scheduling delays room to resolve without
# the test's own pass/fail depending on exactly how fast the host is right
# now -- see the test below, which asserts the actual invariant (no
# overlapping ownership) rather than a wall-clock budget for how fast
# acquisition "should" happen.
def _spawn_probe(lock_dir: Path, hold_secs: float, wait_secs: int = 900):
    return subprocess.Popen(
        ["bash", str(SCRIPT), "--lock-probe", str(hold_secs)],
        cwd=ROOT,
        env={
            "LOCK_DIR": str(lock_dir),
            "LOCK_WAIT_SECS": str(wait_secs),
            "LOCK_POLL_SECS": "1",
            "PATH": _BASE_PATH,
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
        "PATH": _BASE_PATH,
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


def _parse_intervals(outputs: list[str]) -> list[tuple[float, float]]:
    """Extract (acquired_at, released_at) pairs from lock-probe stdout."""
    intervals = []
    for out in outputs:
        acquired = released = None
        for line in out.splitlines():
            if "lock-probe: acquired" in line:
                acquired = float(line.rsplit(" at ", 1)[1])
            elif "lock-probe: releasing" in line:
                released = float(line.rsplit(" at ", 1)[1])
        assert acquired is not None and released is not None, (
            f"could not parse acquire/release timestamps from:\n{out}"
        )
        intervals.append((acquired, released))
    return intervals


def test_two_concurrent_invocations_serialize_not_both_run(tmp_path):
    """The actual race: launch two invocations near-simultaneously.

    A sequential-invocation test would prove nothing -- sequential already works
    today without any lock. This launches both processes with no ordering
    guarantee and asserts exactly one acquired the lock immediately while the
    other observed "already running" and blocked until the first released --
    i.e. real mutual exclusion, not two independent runs that happened not to
    collide.

    Deliberately does NOT assert a wall-clock lower bound ("took at least
    2x hold_secs") as the correctness signal. That coupled the test's
    pass/fail to how fast THIS host happens to be right now, which is
    self-referential in exactly the way that produces "just raise the
    timeout" fixes: this test runs inside the same gate/host it is
    protecting, so a busier host makes both the real acquisition AND this
    test's own budget slower together. The actual invariant -- mutual
    exclusion -- is checked directly from the two probes' own
    high-resolution acquire/release timestamps: their [acquired, released)
    intervals must not overlap, regardless of how long either one took to
    get there. A slow-but-correct serialization still produces
    non-overlapping intervals; only an actual double-acquisition bug
    violates this, and it does so independent of host speed.
    """
    lock_dir = tmp_path / "race.lock"
    hold_secs = 3

    proc_a = _spawn_probe(lock_dir, hold_secs)
    proc_b = _spawn_probe(lock_dir, hold_secs)

    out_a = proc_a.communicate(timeout=960)[0]
    out_b = proc_b.communicate(timeout=960)[0]

    assert proc_a.returncode == 0, out_a
    assert proc_b.returncode == 0, out_b

    # Exactly one of the two logs must show it waited on the other.
    waited_count = sum(1 for out in (out_a, out_b) if "already running" in out)
    acquired_count = sum(out.count("lock-probe: acquired") for out in (out_a, out_b))
    assert waited_count == 1, f"expected exactly one waiter.\nA:\n{out_a}\nB:\n{out_b}"
    assert acquired_count == 2, (
        f"expected both to eventually acquire.\nA:\n{out_a}\nB:\n{out_b}"
    )

    # The invariant itself: no overlapping [acquired, released) ownership.
    (a_start, a_end), (b_start, b_end) = sorted(_parse_intervals([out_a, out_b]))
    assert a_end <= b_start, (
        f"overlapping lock ownership detected: [{a_start}, {a_end}) vs "
        f"[{b_start}, {b_end}) -- both probes held the lock at the same "
        f"time.\nA:\n{out_a}\nB:\n{out_b}"
    )

    # The lock must be fully released after both complete -- no leaked symlink.
    assert not lock_dir.is_symlink(), "lock leaked after both probes exited"


def test_high_concurrency_stress_no_double_acquire(tmp_path):
    """The atomicity property itself, under real contention, not just N=2.

    Launches several probes at once with high-resolution (EPOCHREALTIME)
    acquire/release timestamps and asserts NO two [acquired, released)
    intervals overlap, across the whole run.

    N is deliberately modest (4, not a larger number). This repo's dev host
    runs many concurrent, genuinely independent users and agent sessions at
    once (observed directly during this fix: 16-18 concurrent logged-in users
    via `who`, load averages in the 10-24 range from processes with no
    relation to this test) -- an 8-plus-way concurrent subprocess fan-out
    reliably got starved for MINUTES on that host, not because of a lock
    defect (no overlapping interval was ever observed, only scheduling
    delay), but because launching that many bash+ps+ln subprocesses at once
    competes for a CPU this host does not have free to give. A smaller N
    still exercises real multi-way contention (more than the N=2 test) while
    staying schedulable. (A fractional LOCK_POLL_SECS was tried here to
    shrink the per-handoff polling floor and abandoned -- it crashes
    acquire_lock's retry loop outright: `waited=$((waited + LOCK_POLL_SECS))`
    is bash integer arithmetic, and "0.1" is a syntax error there, not a
    silently-truncated value. That is a real, separate, minor finding --
    LOCK_POLL_SECS must be a whole number of seconds, undocumented before
    this -- noted in local_validate.sh rather than fixed, since the default
    and every real caller only ever need whole seconds.)

    HONEST LIMITATION, stated rather than glossed over: reverting acquire_lock
    to the old mkdir-then-separate-file-write design does NOT reliably make
    this specific test fail on a fast, lightly-loaded machine -- the real
    window between `mkdir` succeeding and the pid file being written is a
    handful of microseconds. This test stays in as a real (if probabilistic)
    stress guard, not as the proof of the defect. The actual deterministic
    proof lives in the PR body: an isolated reproduction of the old design
    with an artificial delay injected between `mkdir` and the pid write
    (widening the real window to something a human can watch) shows a second
    contender reclaiming and re-acquiring while the first still believes it
    holds the lock. The current design closes this structurally, not just
    narrows it: `ln -s` is a single syscall that publishes the name and its
    fully-formed metadata together, so there is no code path left to inject
    an equivalent delay into -- existence and content are the same atomic
    step, not two.
    """
    lock_dir = tmp_path / "stress.lock"
    n = 4
    hold_secs = 0.15

    procs = [
        subprocess.Popen(
            ["bash", str(SCRIPT), "--lock-probe", str(hold_secs)],
            cwd=ROOT,
            env={
                "LOCK_DIR": str(lock_dir),
                "LOCK_WAIT_SECS": "900",
                "PATH": _BASE_PATH,
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for _ in range(n)
    ]
    outputs = [p.communicate(timeout=960)[0] for p in procs]
    for p, out in zip(procs, outputs):
        assert p.returncode == 0, out

    intervals = sorted(_parse_intervals(outputs))
    for (a_start, a_end), (b_start, b_end) in zip(intervals, intervals[1:]):
        assert a_end <= b_start, (
            f"overlapping lock ownership detected: [{a_start}, {a_end}) vs "
            f"[{b_start}, {b_end}) -- two probes held the lock at the same "
            f"time.\nAll outputs:\n" + "\n---\n".join(outputs)
        )

    assert not lock_dir.is_symlink(), "lock leaked after all probes exited"


def test_stale_lock_from_kill_9_is_reclaimed_not_wedged(tmp_path):
    """A run killed with kill -9 must not wedge every future run.

    The lock symlink (unlike flock) has no release-on-process-death, so this
    is the property that actually needs proving: after a SIGKILL leaves the
    symlink behind with a now-dead PID recorded in its target, the next
    invocation must reclaim it promptly (via PID liveness) rather than waiting
    out the full LOCK_WAIT_SECS timeout or hanging forever.
    """
    lock_dir = tmp_path / "stale.lock"

    holder = _spawn_probe(lock_dir, hold_secs=60, wait_secs=30)
    # Give it time to actually acquire before we kill it. Generous deadline:
    # see the host-load note on the exit-order test above.
    deadline = time.monotonic() + 30
    while not lock_dir.is_symlink() and time.monotonic() < deadline:
        time.sleep(0.1)
    assert lock_dir.is_symlink(), "holder never acquired the lock to begin with"

    holder.kill()  # SIGKILL -- no EXIT trap runs; the symlink is left behind.
    holder.wait(timeout=30)
    assert lock_dir.is_symlink(), (
        "lock should still exist post-kill-9 (no auto-release for a symlink)"
    )

    # A fresh probe, with a short-ish LOCK_WAIT_SECS, must reclaim well before
    # that timeout -- NOT time out. If reclamation were broken this would hang
    # for the full wait_secs.
    started_at = time.monotonic()
    fresh = _spawn_probe(lock_dir, hold_secs=0.1, wait_secs=30)
    out = fresh.communicate(timeout=40)[0]
    elapsed = time.monotonic() - started_at

    assert fresh.returncode == 0, out
    assert "reclaiming" in out, f"expected stale-lock reclamation message.\n{out}"
    assert elapsed < 20, (
        f"reclamation took {elapsed:.1f}s -- looks like it waited out LOCK_WAIT_SECS instead of reclaiming"
    )
    assert not lock_dir.is_symlink(), "lock leaked after the reclaiming probe exited"


def test_pid_reuse_is_not_trusted_via_start_time_mismatch(tmp_path):
    """kill -0 alone would be fooled by PID reuse; the lock must not be.

    Fabricates a lock symlink pointing at a genuinely LIVE process (so plain
    `kill -0` would say "alive") but with a start-time that does NOT match
    that process's real start time -- simulating the PID having been reused by
    an unrelated process after the true lock holder was kill -9'd. The lock
    must detect the mismatch and reclaim promptly, not trust the PID number
    alone and wait out the full timeout.
    """
    lock_dir = tmp_path / "reuse.lock"
    lock_dir.parent.mkdir(parents=True, exist_ok=True)

    # A real, currently-alive process to reference by PID.
    live = subprocess.Popen(["sleep", "30"])
    try:
        real_lstart = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(live.pid)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        assert real_lstart, "could not read the live process's start time"

        # Sanity check first: a CORRECT lstart must be respected (waited on,
        # not reclaimed) -- otherwise this test would trivially "pass" no
        # matter what the mismatch branch does, because reclaim already fires
        # unconditionally.
        correct = lock_dir.with_name("correct.lock")
        os.symlink(f"{live.pid}|{real_lstart}|/some/cwd", correct)
        try:
            probe = _spawn_probe(correct, hold_secs=0.1, wait_secs=5)
            out = probe.communicate(timeout=20)[0]
            assert probe.returncode != 0, (
                f"expected a timeout against a genuinely live, correctly-recorded "
                f"owner, got success instead.\n{out}"
            )
            assert "reclaiming" not in out, (
                f"a correctly-recorded live owner must not be reclaimed.\n{out}"
            )
        finally:
            correct.unlink(missing_ok=True)

        # Now the actual case under test: same live PID, WRONG lstart.
        os.symlink(f"{live.pid}|Mon Jan  1 00:00:00 1999|/some/other/cwd", lock_dir)
        started_at = time.monotonic()
        probe = _spawn_probe(lock_dir, hold_secs=0.1, wait_secs=30)
        out = probe.communicate(timeout=40)[0]
        elapsed = time.monotonic() - started_at

        assert probe.returncode == 0, out
        assert "reclaiming" in out, (
            f"expected the start-time mismatch to be treated as stale.\n{out}"
        )
        assert elapsed < 20, (
            f"took {elapsed:.1f}s -- looks like kill -0 alone was trusted "
            f"instead of cross-checking the start time"
        )
    finally:
        live.kill()
        live.wait()
        lock_dir.unlink(missing_ok=True)


def test_unsafe_lock_dir_values_are_refused(tmp_path):
    """LOCK_DIR pointed at something catastrophic must be refused outright.

    The reclaim path used to `rm -rf` LOCK_DIR unconditionally once it judged
    the (recursively-deleted) directory stale. Since LOCK_DIR is
    environment-overridable (by design -- these tests need it), a typo'd or
    mistaken override pointing at, say, a worktree root would have destroyed
    its entire contents. The fix is structural (the lock is a symlink now,
    nothing to recurse into) plus an explicit refusal for the most
    catastrophic values. This test covers both layers.
    """
    # Layer 1: the explicit guard for the worktree root itself.
    result = subprocess.run(
        ["bash", str(SCRIPT), "--lock-probe", "0.1"],
        cwd=ROOT,
        env={"LOCK_DIR": str(ROOT), "PATH": _BASE_PATH},
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "refusing to use LOCK_DIR" in (result.stdout + result.stderr)
    assert (ROOT / "ci" / "local_validate.sh").exists(), (
        "the worktree must be completely untouched"
    )

    # Layer 2: an existing real directory that is NOT a lock symlink. Even
    # values that pass layer 1's exact-match guard (this is neither "/", ROOT,
    # nor $HOME) must still be refused rather than silently destroyed, because
    # reclaim_stale_lock() only ever acts on something it can prove is its own
    # symlink format.
    precious = tmp_path / "precious_dir"
    precious.mkdir()
    (precious / "data.txt").write_text("important data\n")

    result = subprocess.run(
        ["bash", str(SCRIPT), "--lock-probe", "0.1"],
        cwd=ROOT,
        env={"LOCK_DIR": str(precious), "LOCK_WAIT_SECS": "2", "PATH": _BASE_PATH},
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "is not a lock symlink" in (result.stdout + result.stderr)
    assert (precious / "data.txt").read_text() == "important data\n", (
        "an unrelated directory must never be deleted just because LOCK_DIR pointed at it"
    )


def test_release_lock_runs_before_slow_cleanup_on_exit(tmp_path):
    """The consolidated on_exit() trap must release the lock BEFORE running
    cleanup_scratch(), not after.

    cleanup_scratch() shells out to an unbounded `docker exec`; if it hangs
    during SIGINT/SIGTERM and release_lock() ran second, the host mutex would
    wedge behind a stuck ClickHouse cleanup that has nothing to do with the
    lock itself. This uses the --lock-probe-exit-order test-only hook, which
    swaps cleanup_scratch() for a stand-in that sleeps and then writes a
    marker file, and proves the REAL release_lock() already removed the lock
    while that stand-in is still sleeping -- not after it finishes.

    hold_secs matters here: an earlier version of this hook acquired and
    immediately exited with no hold, which released the lock in a near-zero
    window (a handful of shell statements, no I/O wait) -- too short for an
    external poll loop to reliably observe "it existed" at all, which
    produced a real, reproducible false failure during this hook's own
    development (confirmed by process-tree inspection: by the time the poll
    loop's first check ran, the lock had already been created AND removed).
    The explicit hold below makes the existence window a real, controllable
    duration instead of an accident of scheduling.
    """
    lock_dir = tmp_path / "order.lock"
    marker = tmp_path / "cleanup-marker"
    hold_secs = 2
    cleanup_delay = 2

    proc = subprocess.Popen(
        [
            "bash",
            str(SCRIPT),
            "--lock-probe-exit-order",
            str(hold_secs),
            str(cleanup_delay),
            str(marker),
        ],
        cwd=ROOT,
        env={"LOCK_DIR": str(lock_dir), "LOCK_WAIT_SECS": "10", "PATH": _BASE_PATH},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Generous deadlines: this repo's dev host routinely runs many concurrent
    # agent-driven processes (observed directly during this fix: load average
    # 20+ from unrelated concurrent gate runs), so a tight deadline here would
    # produce environmental flakiness unrelated to the property under test.
    deadline = time.monotonic() + 30
    while not lock_dir.is_symlink() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert lock_dir.is_symlink(), "probe never acquired the lock"

    deadline = time.monotonic() + hold_secs + cleanup_delay + 20
    while lock_dir.is_symlink() and time.monotonic() < deadline:
        time.sleep(0.02)
    lock_released_at = time.monotonic()
    assert not lock_dir.is_symlink(), "lock was never released"

    # The critical assertion: the marker (proof the slow "cleanup" finished)
    # must NOT exist yet at the moment the lock disappeared -- release ran
    # strictly first. This is a logical-ordering check, not a wall-clock
    # bound, so it stays meaningful regardless of host load.
    assert not marker.exists(), (
        "the lock was released only after the slow cleanup step finished, "
        "not before it -- on_exit() is calling cleanup before release"
    )

    # And it does eventually run (this isn't testing that cleanup is skipped).
    remaining = max(0.0, (lock_released_at + cleanup_delay + 3) - time.monotonic())
    proc.communicate(timeout=remaining + 20)
    assert marker.exists(), "cleanup_scratch's stand-in never ran at all"


def test_default_lock_dir_ignores_tmpdir_and_still_serializes(tmp_path):
    """The default LOCK_DIR must NOT be derived from $TMPDIR.

    This whole epic is "the gate inherits the shell environment and that
    produces wrong results" -- keying host-wide mutual exclusion on an
    inherited env var would repeat exactly that class of bug: two agents with
    different ambient TMPDIR (a sandbox, a session-scoped scratch dir) would
    silently acquire two DIFFERENT lock names and both proceed, with no
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
            wait_secs=900,
            extra_env={"TMPDIR": str(tmpdir_a), "CH_CONTAINER": container},
        )
        proc_b = _spawn_probe_with_env(
            hold_secs,
            wait_secs=900,
            extra_env={"TMPDIR": str(tmpdir_b), "CH_CONTAINER": container},
        )

        out_a = proc_a.communicate(timeout=960)[0]
        out_b = proc_b.communicate(timeout=960)[0]

        assert proc_a.returncode == 0, out_a
        assert proc_b.returncode == 0, out_b

        # Same proof as the main race test, and for the same reason NOT a
        # wall-clock bound: if the two different TMPDIRs had produced two
        # different lock paths, the probes' [acquired, released) intervals
        # would overlap (both running concurrently against independent
        # locks). Checking the actual intervals is host-speed-independent;
        # checking "took long enough" is not.
        (a_start, a_end), (b_start, b_end) = sorted(_parse_intervals([out_a, out_b]))
        assert a_end <= b_start, (
            f"overlapping lock ownership detected: [{a_start}, {a_end}) vs "
            f"[{b_start}, {b_end}) -- the default LOCK_DIR may have picked up "
            f"$TMPDIR after all, giving each probe its own lock.\nA:\n{out_a}\nB:\n{out_b}"
        )
        assert str(expected_lock_dir) in out_a + out_b, (
            f"expected the fixed /tmp default path in the logs.\nA:\n{out_a}\nB:\n{out_b}"
        )
    finally:
        expected_lock_dir.unlink(missing_ok=True)
