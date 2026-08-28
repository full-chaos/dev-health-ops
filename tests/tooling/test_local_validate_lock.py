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

HARNESS RULE, learned the expensive way (CHAOS-3468): a probe that is running
CONCURRENTLY with the test body must NEVER have a pipe for stdout/stderr.
Every probe below writes to a plain FILE under the test's own tmp_path, read
back after the process exits. See ``_Probe`` for the measured evidence.
"""

from __future__ import annotations

import os
import signal
import stat
import subprocess
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "local_validate.sh"
_BASE_PATH = "/usr/bin:/bin:/usr/local/bin"

# Honest budgets, restored in CHAOS-3468.
#
# These were 900s (wait) / 960s (reap) until CHAOS-3468. That was a
# timeout-raise standing in for a diagnosis: this file's own comment used to
# attribute a gate-run TimeoutExpired at wait_secs=60 to "a transient
# scheduling stall" on a loaded host. That story is REFUTED -- the real cause
# was in this harness (see ``_Probe``), not in the host's scheduler and not in
# the lock. The 900s budget did not fix it; it converted a deadlock into
# ~16 minutes of every local gate run and hid the defect behind "the gate is
# mysteriously slow".
#
# Probes hold the lock for ~0.1-4s by design, so a waiter that is going to
# acquire at all acquires within a couple of seconds. 60s is ~15x the longest
# designed hold: enough slack for a genuinely loaded host, short enough that a
# real wedge fails the test in a minute instead of a quarter of an hour.
_WAIT_SECS = 60
# Ceiling for waiting on a probe process to EXIT (its own LOCK_WAIT_SECS
# budget, plus its hold, plus slack).
_REAP_SECS = 90

# Probes spawned by the test currently running, reaped by the autouse fixture
# below. Populated by _spawn_probe*/_spawn_exit_order_probe.
_SPAWNED: list[_Probe] = []


class _Probe:
    """A running ``--lock-probe`` child whose output goes to a FILE, not a pipe.

    Popen-compatible enough for this file (``communicate``, ``wait``, ``kill``,
    ``returncode``), so the tests read the same way they always did.

    WHY NO PIPE (CHAOS-3468). These tests deliberately run two or more probes
    at once, and the old harness gave every one of them ``stdout=PIPE`` while
    draining them SERIALLY (``a.communicate(); b.communicate()``). That
    deadlocks: the undrained probe blocks in ``write()`` on its own stdout
    pipe WHILE HOLDING THE LOCK, so the probe being drained can never acquire,
    and the drainer never gets to the blocked one. Directly observed on this
    host, not theorised: the lock symlink named the blocked PID as owner, and
    a stack sample of it was 1804/1808 samples deep in
    ``__swbuf -> __sflush -> _swrite -> __write_nocancel``, with fd 1 and 2
    both pointing at the pipe whose read end pytest was not reading. Both
    probes were still alive 5+ minutes into a 3-second hold; the waiter then
    burned its full LOCK_WAIT_SECS and failed.

    A probe emits ~370 bytes total, and ``lsof`` reports these pipes with the
    nominal 16K capacity -- yet they block anyway, because nominal is not
    actual. Measured on this host while the deadlock was live: a single
    ``write()`` of 512 bytes to an unread pipe succeeds and 1024 bytes BLOCKS,
    i.e. the effective buffer is 512 bytes, not 16K (macOS hands out the small
    buffer and defers expansion under kernel pipe-memory pressure, which a
    machine running many concurrent agent sessions sits in permanently).
    So "the output is small, the pipe is safe" is not a defense here at any
    size worth writing. A file has no writer-side blocking at all, which is why
    this is the fix rather than "drain concurrently with threads".

    Files also survive the process: a probe killed in teardown still leaves
    whatever it managed to print, which a pipe drained after the fact does not.
    """

    def __init__(self, proc: subprocess.Popen, out_path: Path) -> None:
        self._proc = proc
        self._out_path = out_path

    @property
    def pid(self) -> int:
        return self._proc.pid

    @property
    def returncode(self) -> int | None:
        return self._proc.returncode

    def output(self) -> str:
        """Everything the probe has written so far (stdout+stderr, merged)."""
        return self._out_path.read_text(errors="replace")

    def communicate(self, timeout: float | None = None) -> tuple[str, None]:
        """Wait for exit, then return ``(output, None)`` -- Popen-shaped."""
        self._proc.wait(timeout=timeout)
        return self.output(), None

    def wait(self, timeout: float | None = None) -> int:
        return self._proc.wait(timeout=timeout)

    def poll(self) -> int | None:
        return self._proc.poll()

    def kill(self) -> None:
        """SIGKILL the probe AND its ``sleep`` child (whole process group).

        Killing only the bash PID leaves its ``sleep`` behind as an orphan for
        the rest of that sleep's duration. Every probe is spawned into its own
        session (``start_new_session=True``), so its PID is its process-group
        id and one ``killpg`` takes the tree. No EXIT trap runs either way, so
        this is still the "run was SIGKILLed, the symlink is left behind"
        scenario the stale-lock test needs.
        """
        self._killpg()

    def _killpg(self) -> None:
        if self._proc.poll() is not None:
            # Already exited: the pid may have been recycled by now, so
            # signalling "its" group could hit an unrelated process.
            return
        try:
            os.killpg(self._proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    def reap(self) -> None:
        """Teardown: leave nothing running, whatever the test did or failed at.

        CHAOS-3468 also recorded probes from a FAILED gate run still alive 12+
        minutes later, holding tmp locks and CPU, because a test that raised
        never got back to its ``communicate()``. Reaping is therefore a
        fixture's job, not a test's.

        Stated tradeoff: with a pipe, a probe whose parent died got EPIPE on
        its next write and exited -- accidental reaping that only worked
        because the pipe was also the deadlock. A file-backed probe survives
        its parent, so if pytest itself is SIGKILLed (no finalizers run) a
        probe outlives it. It is bounded, though: LOCK_WAIT_SECS <= 60 plus
        its hold, so ~2 minutes worst case, against the 12+ minutes actually
        observed.
        """
        self._killpg()
        try:
            self._proc.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            pass


@pytest.fixture(autouse=True)
def _reap_probes():
    """Kill and reap every probe this module spawns, pass or fail."""
    _SPAWNED.clear()
    try:
        yield
    finally:
        for probe in _SPAWNED:
            probe.reap()
        _SPAWNED.clear()


def _spawn(argv: list[str], out_dir: Path, env: dict[str, str]) -> _Probe:
    """Start a probe with its output on a FILE and its own process group."""
    out_path = out_dir / f"probe-{len(_SPAWNED)}.out"
    # The parent's handle is closed immediately -- the child holds its own dup,
    # and the test reads the path back after exit.
    with out_path.open("wb") as out_fh:
        assert stat.S_ISREG(os.fstat(out_fh.fileno()).st_mode), (
            f"probe output must go to a regular file, got mode "
            f"{os.fstat(out_fh.fileno()).st_mode:#o}"
        )
        proc = subprocess.Popen(
            argv,
            cwd=ROOT,
            env=env,
            stdout=out_fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    # THE INVARIANT, checked on every spawn rather than assumed: this child has
    # NO pipe to block on. CHAOS-3468's defect is a probe blocked in write()
    # while holding the lock, and the only thing that can block a writer that
    # way here is a pipe nobody is reading. ``Popen.stdout``/``stderr`` are
    # non-None if and only if this call asked for a pipe, so a future edit that
    # reintroduces ``stdout=PIPE`` fails HERE, immediately and by name, instead
    # of deadlocking the file for a quarter of an hour.
    #
    # This -- not the file's wall-clock runtime -- is the proof the deadlock is
    # gone. Wall clock is a symptom, and it would also "improve" from the
    # LOCK_WAIT_SECS 900 -> 60 change on its own; treating a faster run as the
    # proof is the same self-confirming reasoning that produced the 900s budget
    # in the first place. (Verified falsifiable: putting ``stdout=PIPE`` back
    # makes this assert fire at spawn.)
    assert proc.stdout is None and proc.stderr is None, (
        "a probe must never be given a pipe: it can block in write() while "
        "holding the lock, which deadlocks every test in this file"
    )
    probe = _Probe(proc, out_path)
    _SPAWNED.append(probe)
    return probe


def _spawn_probe(
    lock_dir: Path,
    hold_secs: float,
    wait_secs: int = _WAIT_SECS,
    *,
    extra_env: dict[str, str] | None = None,
    path: str = _BASE_PATH,
) -> _Probe:
    env = {
        "LOCK_DIR": str(lock_dir),
        "LOCK_WAIT_SECS": str(wait_secs),
        "LOCK_POLL_SECS": "1",
        "PATH": path,
    }
    env.update(extra_env or {})
    return _spawn(
        ["bash", str(SCRIPT), "--lock-probe", str(hold_secs)],
        lock_dir.parent,
        env,
    )


def _spawn_probe_with_env(
    hold_secs: float, wait_secs: int, extra_env: dict[str, str], out_dir: Path
) -> _Probe:
    """Like _spawn_probe, but does NOT set LOCK_DIR -- lets the script compute
    its own default, with whatever extra env (e.g. a distinct TMPDIR) the
    caller supplies layered on top of a minimal base."""
    env = {
        "LOCK_WAIT_SECS": str(wait_secs),
        "LOCK_POLL_SECS": "1",
        "PATH": _BASE_PATH,
    }
    env.update(extra_env)
    return _spawn(["bash", str(SCRIPT), "--lock-probe", str(hold_secs)], out_dir, env)


def _spawn_exit_order_probe(
    lock_dir: Path, hold_secs: float, cleanup_delay: float, marker: Path
) -> _Probe:
    return _spawn(
        [
            "bash",
            str(SCRIPT),
            "--lock-probe-exit-order",
            str(hold_secs),
            str(cleanup_delay),
            str(marker),
        ],
        lock_dir.parent,
        {"LOCK_DIR": str(lock_dir), "LOCK_WAIT_SECS": "10", "PATH": _BASE_PATH},
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

    # Draining one after the other is safe ONLY because probe output goes to
    # files: with pipes, the undrained probe blocks in write() while holding
    # the lock and neither ever finishes (CHAOS-3468 -- see _Probe).
    out_a = proc_a.communicate(timeout=_REAP_SECS)[0]
    out_b = proc_b.communicate(timeout=_REAP_SECS)[0]

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

    procs = [_spawn_probe(lock_dir, hold_secs) for _ in range(n)]
    outputs = [p.communicate(timeout=_REAP_SECS)[0] for p in procs]
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


# CHAOS-4397: fabricates ps_lstart()'s ANSWER for one specific PID instead of
# relying on a real spawned reference process's real start time. Every other
# invocation (any other PID, any other flag set) falls through to the real ps
# binary unmodified -- same PATH-shadowing technique as _SLOW_PS_WRAPPER
# below, applied to lstart instead of an artificial delay.
_FAKE_LSTART_PS_WRAPPER = """#!/bin/bash
real_ps=/bin/ps
if [ -n "${FAKE_LSTART_TARGET_PID:-}" ] && [ -n "${FAKE_LSTART_VALUE:-}" ]; then
  for a in "$@"; do
    if [ "$a" = "$FAKE_LSTART_TARGET_PID" ]; then
      printf '%s\\n' "$FAKE_LSTART_VALUE"
      exit 0
    fi
  done
fi
exec "$real_ps" "$@"
"""


def test_pid_reuse_is_not_trusted_via_start_time_mismatch(tmp_path):
    """kill -0 alone would be fooled by PID reuse; the lock must not be.

    Fabricates a lock symlink pointing at a genuinely LIVE process (so plain
    `kill -0` would say "alive") but with a start-time that does NOT match
    that process's real start time -- simulating the PID having been reused by
    an unrelated process after the true lock holder was kill -9'd. The lock
    must detect the mismatch and reclaim promptly, not trust the PID number
    alone and wait out the full timeout.

    CHAOS-4397: this used to spawn a real ``sleep 30`` as the "genuinely
    alive" reference and prove promptness with a wall-clock ``elapsed < 20``
    assertion. Both were host-load dependent and both actually flaked in
    production (five lanes, 2026-08-28, load 11-14): a scheduling delay of
    30s+ between spawning the reference process and this test getting to
    check it lets `sleep 30` exit naturally first -- deterministic repro:
    injecting a 31s delay before the correctly-recorded-owner check below
    reliably reproduces the exact reported "PID ... not running" failure,
    because the reference process is simply gone by the time `ps` looks for
    it, nothing to do with the lock logic under test. A slow host can
    separately blow the old 20s budget on subprocess-spawn scheduling alone,
    independent of how fast the lock itself decided to reclaim.

    Fixed by injecting a fake process table instead: this pytest process's
    OWN pid (``os.getpid()``) is used as the "genuinely alive" reference --
    guaranteed alive for the whole test, no spawned helper to race against --
    and a PATH-shadowing `ps` stub (``_FAKE_LSTART_PS_WRAPPER``) hands back a
    fixed, canned ``lstart`` string for that one pid instead of a real,
    timing-derived value. Promptness is proved by a behavioral fact instead of
    a wall-clock threshold: ``reclaim_stale_lock()`` runs before
    ``try_acquire_once()`` inside ``acquire_lock()``, so a mismatch caught on
    that very first call never emits the "already running ... waiting"
    message the polling branch would print -- that message's absence proves
    the reclaim did not fall through to (and merely survive) the wait loop,
    regardless of how slow the host is at scheduling the probe's subprocesses.
    """
    lock_dir = tmp_path / "reuse.lock"
    lock_dir.parent.mkdir(parents=True, exist_ok=True)

    fake_ps_dir = tmp_path / "fake_lstart_ps_bin"
    fake_ps_dir.mkdir()
    fake_ps = fake_ps_dir / "ps"
    fake_ps.write_text(_FAKE_LSTART_PS_WRAPPER)
    fake_ps.chmod(0o755)
    path = f"{fake_ps_dir}:{_BASE_PATH}"

    # This pytest (worker) process itself: guaranteed alive for the entire
    # test, so `kill -0` against it is never a race against anything's real
    # lifetime. Its real lstart is irrelevant -- the fake ps wrapper above
    # substitutes a canned value for it regardless of when it actually started.
    live_pid = os.getpid()
    real_lstart = "Wed Aug  5 20:46:09 2026"
    extra_env = {
        "FAKE_LSTART_TARGET_PID": str(live_pid),
        "FAKE_LSTART_VALUE": real_lstart,
    }

    # Sanity check first: a CORRECT lstart must be respected (waited on,
    # not reclaimed) -- otherwise this test would trivially "pass" no
    # matter what the mismatch branch does, because reclaim already fires
    # unconditionally.
    correct = lock_dir.with_name("correct.lock")
    os.symlink(f"{live_pid}|{real_lstart}|/some/cwd", correct)
    try:
        probe = _spawn_probe(
            correct, hold_secs=0.1, wait_secs=5, extra_env=extra_env, path=path
        )
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
    os.symlink(f"{live_pid}|Mon Jan  1 00:00:00 1999|/some/other/cwd", lock_dir)
    try:
        probe = _spawn_probe(
            lock_dir, hold_secs=0.1, wait_secs=30, extra_env=extra_env, path=path
        )
        out = probe.communicate(timeout=40)[0]

        assert probe.returncode == 0, out
        assert "reclaiming" in out, (
            f"expected the start-time mismatch to be treated as stale.\n{out}"
        )
        # Host-speed-independent promptness proof (see docstring): a mismatch
        # caught on the first reclaim_stale_lock() call never falls into the
        # "already running ... waiting" polling branch at all.
        assert "already running" not in out, (
            f"expected the mismatch to be caught before the wait loop ever "
            f"started, not reclaimed only after polling toward the "
            f"timeout.\n{out}"
        )
    finally:
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

    proc = _spawn_exit_order_probe(lock_dir, hold_secs, cleanup_delay, marker)

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
            wait_secs=_WAIT_SECS,
            extra_env={"TMPDIR": str(tmpdir_a), "CH_CONTAINER": container},
            out_dir=tmp_path,
        )
        proc_b = _spawn_probe_with_env(
            hold_secs,
            wait_secs=_WAIT_SECS,
            extra_env={"TMPDIR": str(tmpdir_b), "CH_CONTAINER": container},
            out_dir=tmp_path,
        )

        out_a = proc_a.communicate(timeout=_REAP_SECS)[0]
        out_b = proc_b.communicate(timeout=_REAP_SECS)[0]

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


# --- CHAOS-3403 adversarial-review follow-up findings. -----------------------------
#
# The tests above cover the review that introduced the lock. The tests below cover
# five further defects found in a SECOND adversarial review of that same lock:
# ps -o lstart= rendering under the caller's locale (Finding 1), reclaim/release
# acting on "whatever is currently at the path" instead of what was actually read
# (Findings 1 and 2), every `ln -s` failure being treated as "already held"
# (Finding 3), and --lock-probe's own timestamps silently degrading to whole-second
# resolution on this repo's exact test PATH (Finding 5) -- which made the
# interval-overlap tests above unable to ever fail. Finding 4 (SIGINT delivered to
# only the tracked PID cannot interrupt a blocked foreground external command) has
# no deterministic regression test here -- see the comment above the INT trap in
# local_validate.sh for why, and the honest limitation is documented there rather
# than hidden behind a test that would always pass.


def _locale_available(loc: str) -> bool:
    try:
        out = subprocess.run(
            ["locale", "-a"], capture_output=True, text=True, timeout=5
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return loc in out.split()


@pytest.mark.skipif(
    not _locale_available("de_DE.UTF-8"),
    reason="de_DE.UTF-8 locale not installed on this host",
)
def test_reclaim_ignores_locale_when_checking_holder_liveness(tmp_path):
    """A live holder recorded under one locale must not be judged dead by a
    checker running under a different locale.

    ``ps -o lstart=`` renders under the caller's LC_TIME/LC_ALL ("Wed Aug 5
    20:46:09 2026" under C, "Mi.  5 Aug. 20:46:09 2026" under de_DE.UTF-8).
    Before every lock-metadata ``ps`` call pinned LC_ALL=C, a holder that
    acquired under one locale and a checker running under another would
    compare two different renderings of the exact same start time, see a
    byte-for-byte mismatch, and reclaim a perfectly live lock -- no timing
    race required, purely a function of the two processes' ambient locale.

    This launches a real, currently-held holder under LC_ALL=C and a checker
    under LC_ALL=de_DE.UTF-8, and asserts the checker waits (never reclaims)
    the live holder.
    """
    lock_dir = tmp_path / "locale.lock"
    holder = _spawn_probe(
        lock_dir, hold_secs=5, wait_secs=30, extra_env={"LC_ALL": "C"}
    )
    try:
        deadline = time.monotonic() + 15
        while not lock_dir.is_symlink() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert lock_dir.is_symlink(), "holder never acquired the lock to begin with"

        checker = subprocess.run(
            ["bash", str(SCRIPT), "--lock-probe", "0.1"],
            cwd=ROOT,
            env={
                "LOCK_DIR": str(lock_dir),
                "LOCK_WAIT_SECS": "3",
                "LOCK_POLL_SECS": "1",
                "PATH": _BASE_PATH,
                "LC_ALL": "de_DE.UTF-8",
            },
            capture_output=True,
            text=True,
            timeout=20,
        )
        combined = checker.stdout + checker.stderr
        assert checker.returncode != 0, (
            f"checker under a different locale must time out waiting on the "
            f"live holder, not reclaim it.\n{combined}"
        )
        assert "reclaiming" not in combined, (
            f"a live holder must never be reclaimed just because the checker "
            f"runs under a different locale.\n{combined}"
        )
        assert "already running" in combined, combined
    finally:
        out = holder.communicate(timeout=20)[0]
        assert holder.returncode == 0, out
        assert not lock_dir.is_symlink(), "lock leaked after the holder exited"


def test_release_lock_is_compare_and_delete_not_unconditional(tmp_path):
    """release_lock() must verify the lock still points at ITS OWN metadata
    before removing it -- not ``rm -f`` gated only on the local LOCK_HELD flag.

    Simulates the reviewer's cascade: holder H is (for whatever reason --
    the locale bug above is one way, a widened reclaim race is another)
    wrongly judged stale and reclaimed while still alive, a legitimate new
    owner T acquires the now-free lock, and H eventually exits normally. H's
    own release_lock() must NOT delete T's live lock just because H once
    held it -- LOCK_HELD only proves H once acquired, not that the symlink
    at LOCK_DIR right now is still the one H wrote.
    """
    lock_dir = tmp_path / "cascade.lock"
    holder = _spawn_probe(lock_dir, hold_secs=4, wait_secs=30)
    try:
        deadline = time.monotonic() + 15
        while not lock_dir.is_symlink() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert lock_dir.is_symlink(), "holder never acquired the lock to begin with"

        # Simulate T reclaiming + acquiring while H still believes it holds
        # the lock (H is mid-`sleep 4`, unaware anything happened).
        lock_dir.unlink()
        fake_t_target = "999999|Wed Jan  1 00:00:00 2020|/some/t/cwd"
        os.symlink(fake_t_target, lock_dir)

        out = holder.communicate(timeout=20)[0]
        assert holder.returncode == 0, out

        assert lock_dir.is_symlink(), (
            "T's live lock must survive H's exit -- H's release_lock deleted it "
            "outright instead of no-op'ing against content it no longer owns"
        )
        assert os.readlink(lock_dir) == fake_t_target, (
            f"H's release_lock altered T's live lock -- expected {fake_t_target!r}, "
            f"got {os.readlink(lock_dir)!r}"
        )
    finally:
        lock_dir.unlink(missing_ok=True)


_SLOW_PS_WRAPPER = """#!/bin/bash
# Test-only ps wrapper (Finding 2 repro): delays the specific "-p
# $SLOW_PS_TARGET_PID" lookup used by lock_holder_alive's re-check, widening
# the gap between reclaim_stale_lock's stale DECISION and its DELETE to
# something a test can reliably land a concurrent reclaimer inside of --
# the same technique this file's module docstring says was needed to prove
# the original mkdir-based bug.
real_ps=/bin/ps
if [ -n "${SLOW_PS_TARGET_PID:-}" ] && [ -n "${SLOW_PS_DELAY:-}" ]; then
  for a in "$@"; do
    if [ "$a" = "$SLOW_PS_TARGET_PID" ]; then
      sleep "${SLOW_PS_DELAY}"
      break
    fi
  done
fi
exec "$real_ps" "$@"
"""


def test_reclaim_is_compare_and_delete_under_widened_window(tmp_path):
    """Widen reclaim_stale_lock's stale-decision-to-delete gap and confirm a
    concurrent live reclaimer's fresh lock survives.

    Process X's ``ps -p <pid>`` lookup (inside lock_holder_alive) is delayed
    several seconds via a PATH-shadowing wrapper. In that gap, process Y
    independently reclaims the same lock (fast ps, no delay) and acquires it
    for real. When X's delayed decision finally resolves "stale", X's delete
    must be a no-op against Y's now-different lock content -- not an
    unconditional ``rm -f`` of whatever happens to be at the path when X
    finally gets there. Asserted the same way the concurrency tests above
    assert real mutual exclusion: via non-overlapping high-resolution
    [acquired, released) intervals, not a wall-clock budget.
    """
    slow_ps_dir = tmp_path / "slow_ps_bin"
    slow_ps_dir.mkdir()
    slow_ps = slow_ps_dir / "ps"
    slow_ps.write_text(_SLOW_PS_WRAPPER)
    slow_ps.chmod(0o755)

    lock_dir = tmp_path / "widen.lock"
    # A genuinely live process, referenced with a WRONG recorded lstart, so
    # lock_holder_alive's ps-based re-check (not a dead-PID kill -0
    # short-circuit) is what decides "stale".
    live = subprocess.Popen(["sleep", "60"])
    try:
        os.symlink(f"{live.pid}|Mon Jan  1 00:00:00 1999|/some/x/cwd", lock_dir)

        x = _spawn_probe(
            lock_dir,
            hold_secs=1,
            wait_secs=30,
            extra_env={
                "SLOW_PS_TARGET_PID": str(live.pid),
                "SLOW_PS_DELAY": "3",
            },
            path=f"{slow_ps_dir}:{_BASE_PATH}",
        )
        # Let X get into its delayed ps call before Y starts racing it.
        time.sleep(1)
        y = _spawn_probe(lock_dir, hold_secs=4, wait_secs=30)

        # Y is deliberately NOT drained first: it holds the lock while X is
        # still deciding, and with pipes that undrained hold deadlocked the
        # whole file (CHAOS-3468). Output is on files, so drain order is free.
        out_x = x.communicate(timeout=_REAP_SECS)[0]
        out_y = y.communicate(timeout=_REAP_SECS)[0]
        assert x.returncode == 0, out_x
        assert y.returncode == 0, out_y

        (a_start, a_end), (b_start, b_end) = sorted(_parse_intervals([out_x, out_y]))
        assert a_end <= b_start, (
            f"overlapping lock ownership detected: [{a_start}, {a_end}) vs "
            f"[{b_start}, {b_end}) -- X's widened stale-reclaim window let it "
            f"delete Y's live lock and re-acquire while Y still held it.\n"
            f"X:\n{out_x}\nY:\n{out_y}"
        )
        assert not lock_dir.is_symlink(), "lock leaked after both exited"
    finally:
        live.kill()
        live.wait()
        lock_dir.unlink(missing_ok=True)


def test_ln_s_failure_from_missing_parent_dir_dies_immediately(tmp_path):
    """A LOCK_DIR whose parent directory doesn't exist must fail FAST with
    the real reason -- not be treated as lock contention and polled out to
    the full LOCK_WAIT_SECS with a diagnostic naming a PID that doesn't exist.

    Every nonzero ``ln -s`` exit used to be treated as "someone else holds
    the lock": a missing parent directory, a read-only filesystem, and a
    permissions error all looked identical to real contention, and none of
    them will ever resolve by polling.
    """
    bad_lock_dir = tmp_path / "no" / "such" / "parent" / "lock.file"
    started_at = time.monotonic()
    result = subprocess.run(
        ["bash", str(SCRIPT), "--lock-probe", "0.1"],
        cwd=ROOT,
        env={"LOCK_DIR": str(bad_lock_dir), "LOCK_WAIT_SECS": "20", "PATH": _BASE_PATH},
        capture_output=True,
        text=True,
        timeout=30,
    )
    elapsed = time.monotonic() - started_at
    combined = result.stdout + result.stderr

    assert result.returncode != 0, combined
    assert "NOT lock contention" in combined, (
        f"expected the real-failure diagnosis, not a contention message.\n{combined}"
    )
    assert "PID ?" not in combined, (
        f"must not report a diagnosis naming a nonexistent PID.\n{combined}"
    )
    assert elapsed < 10, (
        f"took {elapsed:.1f}s -- looks like this was treated as contention and "
        f"polled toward LOCK_WAIT_SECS=20 instead of dying immediately.\n{combined}"
    )


def test_lock_probe_time_has_subsecond_resolution(tmp_path):
    """--lock-probe's own timestamps must carry real sub-second precision
    under this test file's exact PATH (_BASE_PATH) -- not silently degrade to
    whole seconds.

    Whole-second resolution doesn't just make the probe coarser: it makes
    the interval-overlap tests earlier in this file UNFALSIFIABLE. Two
    racing acquisitions rounding to the same integer second would satisfy
    ``a_end <= b_start`` and PASS even in the presence of a real
    microsecond-scale double-acquisition bug.
    """
    lock_dir = tmp_path / "resolution.lock"
    probe = _spawn_probe(lock_dir, hold_secs=0.2, wait_secs=10)
    out = probe.communicate(timeout=20)[0]
    assert probe.returncode == 0, out

    (acquired, released) = _parse_intervals([out])[0]
    assert released - acquired < 1.0, (
        f"hold was 0.2s but the measured interval is {released - acquired}s -- "
        f"resolution looks like it degraded to whole seconds.\n{out}"
    )
    # A whole-second fallback reports exact integers; a real sub-second
    # source practically never lands on one.
    assert acquired != int(acquired) or released != int(released), (
        f"acquired/released look like exact integers -- resolution may have "
        f"silently degraded to whole seconds.\n{out}"
    )


def test_lock_probe_fails_loudly_without_subsecond_time_source(tmp_path):
    """With NO sub-second time source available at all (an old bash lacking
    EPOCHREALTIME, a ``date`` that doesn't interpret %N, and no python3/perl
    on PATH), the probe must FAIL LOUDLY -- not silently hand back
    whole-second timestamps that make the interval-overlap tests unable to
    ever fail. A measurement that did not happen must FAIL, not pass quietly.

    Only meaningful on a host where /bin/bash itself lacks EPOCHREALTIME
    (true on macOS's stock bash 3.2; typically false on a modern Linux image
    where /bin/bash is bash 5+) -- skips rather than asserting anything on a
    host where this fallback path can't actually be forced this way.
    """
    system_bash = Path("/bin/bash")
    has_epochrealtime = (
        subprocess.run(
            [str(system_bash), "-c", "echo ${EPOCHREALTIME:-UNSET}"],
            capture_output=True,
            text=True,
        ).stdout.strip()
        != "UNSET"
    )
    if has_epochrealtime:
        pytest.skip(
            "/bin/bash on this host already has EPOCHREALTIME -- cannot force "
            "the no-resolution branch this way"
        )

    # fake_bin is put FIRST on PATH (ahead of _BASE_PATH) so it shadows just
    # four names: `bash` (the no-EPOCHREALTIME system one), `date` (%N not
    # interpreted, like old BSD date), and `python3`/`perl` (stubs that exit
    # nonzero with no output, so lock_probe_time's own output validation --
    # not just a bare `command -v` check -- is what makes it fall through).
    # Everything else the script needs (dirname, awk, sed, ln, ps, ...)
    # still resolves from _BASE_PATH behind it.
    fake_bin = tmp_path / "fakebin"
    fake_bin.mkdir()
    os.symlink(system_bash, fake_bin / "bash")

    fake_date = fake_bin / "date"
    fake_date.write_text(
        "#!/bin/bash\n"
        "# Mimics old BSD date: %N is not a real conversion, emitted literally.\n"
        'if [ "$1" = "+%s.%N" ]; then printf \'%s.N\\n\' "$(command date +%s)"; exit 0; fi\n'
        'exec /bin/date "$@"\n'
    )
    fake_date.chmod(0o755)

    for absent_tool in ("python3", "perl"):
        stub = fake_bin / absent_tool
        stub.write_text("#!/bin/bash\nexit 1\n")
        stub.chmod(0o755)

    lock_dir = tmp_path / "no-resolution.lock"
    result = subprocess.run(
        [str(fake_bin / "bash"), str(SCRIPT), "--lock-probe", "0.1"],
        cwd=ROOT,
        env={
            "LOCK_DIR": str(lock_dir),
            "LOCK_WAIT_SECS": "10",
            "PATH": f"{fake_bin}:{_BASE_PATH}",
        },
        capture_output=True,
        text=True,
        timeout=20,
    )
    combined = result.stdout + result.stderr
    assert result.returncode != 0, (
        f"expected the probe to fail loudly with no sub-second time source "
        f"available, got success.\n{combined}"
    )
    assert "FATAL" in combined and "sub-second" in combined, (
        f"expected an explicit fail-loud message, not a silent whole-second "
        f"fallback.\n{combined}"
    )
