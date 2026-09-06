"""CHAOS-5025: regression coverage for the shared Go worker teardown mechanism.

Why this file exists (codex round 2, P3): the other tests added for CHAOS-5025
cover the PYTHON half of the fix, but every defect actually found in this work
was in the SHELL half, which Python tests structurally cannot reach -- so the
shell defects passed the new coverage. Two of them were introduced by the fix
for the previous one:

  * ``service_pgid()`` ended on an ``&&`` chain, so the not-a-group-leader case
    returned 1; under ``set -e`` the caller's assignment then aborted
    ``stop_service()`` before the service was signalled AT ALL, making the
    fallback path worse than no fix at all.
  * the process-group id was originally resolved AFTER ``wait`` returned, when
    the leader is already reaped and ``ps`` reports nothing, so the group was
    never swept and the descendant it existed to kill survived.

These drive the SHIPPED functions, sliced out of the real sources by anchor, so
they cannot drift from them.

CHAOS-5362: ``service_pgid``/``stop_service`` moved out of
``ci/run_metrics_executed_proof.sh`` into the shared library
``ci/lib/go_worker_fixture.sh`` (same names, unprefixed -- a plain relocation,
not a rename), alongside a new ``stop_worker_stack`` that orders the
worker-then-reconciler ``stop_service`` calls (the api is stopped separately,
directly, by the calling script, to preserve the full worker -> reconciler ->
api kill order across the library boundary). ``cleanup()``/``on_signal()``/the
three ``trap`` lines stayed in the calling script -- ``cleanup()``'s body now
calls ``stop_worker_stack`` followed by a direct ``stop_service`` for the api,
instead of three inline ``stop_service`` calls. Tests that only exercise the
pgid/kill/watchdog mechanism now slice the LIBRARY; tests that exercise the
script's own re-entrancy-safe trap wiring interacting with that mechanism
source BOTH.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ci" / "run_metrics_executed_proof.sh"
LIB = REPO_ROOT / "ci" / "lib" / "go_worker_fixture.sh"

# A module-level `pytest.skip(...)` call rather than a `pytestmark` assignment:
# tests/tooling's orphan-definition guard parses the AST and cannot see pytest's
# implicit consumption of `pytestmark`, so the assignment reads as a definition
# that is never used. This is a call, so there is nothing to orphan.
if shutil.which("bash") is None or not SCRIPT.is_file() or not LIB.is_file():
    pytest.skip(
        "needs bash, the proof script, and the shared worker-fixture lib",
        allow_module_level=True,
    )

PRELUDE = "set -euo pipefail\nTEARDOWN_WAIT_SECS=1\n"


def _slice_text(text: str, start_pattern: str, end_pattern: str) -> str:
    """Slice ``text`` between two anchors, both required to be UNIQUE.

    Uniqueness is the whole guard (codex round 3, P2). With only "first match"
    semantics a script could carry a WORKING decoy definition before the anchor
    and a BROKEN real one after it: bash uses the later definition, while these
    tests would happily exercise the decoy and report green. That was
    demonstrated -- 13 tests passed against a `stop_service()` that returned
    without signalling any real service.
    """
    starts = re.findall(start_pattern, text, re.M)
    ends = re.findall(end_pattern, text, re.M)
    assert len(starts) == 1, (
        f"anchor {start_pattern!r} occurs {len(starts)}x, want exactly 1"
    )
    assert len(ends) == 1, f"anchor {end_pattern!r} occurs {len(ends)}x, want exactly 1"
    start = re.search(start_pattern, text, re.M)
    end = re.search(end_pattern, text, re.M)
    assert start is not None and end is not None
    assert start.start() < end.start()
    return text[start.start() : end.start()]


def _slice(path: Path, start_pattern: str, end_pattern: str) -> str:
    return _slice_text(path.read_text(), start_pattern, end_pattern)


#: Every spelling bash accepts for a function definition. The first version of
#: the uniqueness guard below matched only ``name() {`` with exactly one space,
#: so ``function stop_service { ... }`` -- the keyword form, equally valid and
#: equally shadowing -- slipped straight past it (found by the peer read). A
#: guard that enumerates syntax must enumerate ALL of it, or it narrows silently
#: to the one spelling its author happened to use.
def _definition_pattern(name: str) -> str:
    return rf"^(?:function\s+{name}\b|{name}\s*\(\)\s*\{{)"


def _assert_defined_exactly_once(text: str, name: str, where: str) -> None:
    found = re.findall(_definition_pattern(name), text, re.M)
    assert len(found) == 1, (
        f"{name} is defined {len(found)}x in {where} (counting both "
        "`name() {` and `function name {`); a second definition would "
        "shadow the one these tests slice out"
    )


def test_teardown_functions_are_defined_exactly_once():
    """codex round 3, P2: a duplicate definition makes the slice test a decoy.

    Covers both `name() {` (any spacing) and `function name {`.

    CHAOS-5362: service_pgid/stop_service/stop_worker_stack now live in the
    shared library; cleanup/on_signal stayed in the calling script.
    """
    lib_text = LIB.read_text()
    for name in ("service_pgid", "stop_service", "stop_worker_stack"):
        _assert_defined_exactly_once(lib_text, name, str(LIB))

    script_text = SCRIPT.read_text()
    for name in ("cleanup", "on_signal"):
        _assert_defined_exactly_once(script_text, name, str(SCRIPT))


@pytest.mark.parametrize(
    "decoy",
    [
        "stop_service() {\n  return 0\n}\n",
        "stop_service  ()  {\n  return 0\n}\n",
        "function stop_service {\n  return 0\n}\n",
        "function stop_service() {\n  return 0\n}\n",
    ],
    ids=["paren", "paren-spaced", "keyword", "keyword-paren"],
)
def test_a_shadowing_redefinition_is_rejected_in_every_bash_spelling(
    decoy, tmp_path, monkeypatch
):
    """Red -> green for the decoy attack, in each form bash accepts.

    Bash uses the LAST definition, so a second `stop_service` appended
    after the real one silently replaces it while an anchor-sliced test keeps
    exercising the first. The keyword form evaded the original guard entirely.
    """
    shadowed = tmp_path / "shadowed.sh"
    shadowed.write_text(LIB.read_text() + "\n" + decoy)
    monkeypatch.setattr("tests.tooling.test_metrics_proof_teardown.LIB", shadowed)
    with pytest.raises(AssertionError):
        test_teardown_functions_are_defined_exactly_once()


def _teardown_source() -> str:
    """service_pgid + stop_service + stop_worker_stack, sliced out of the
    shared library."""
    return _slice(
        LIB, r"^service_pgid\(\) \{", r"^seed_and_finalize_sync_targets\(\) \{"
    )


def _run(body: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-c", body],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ},
    )


def test_descendant_is_reaped_when_the_service_leads_a_group(tmp_path):
    marker = tmp_path / "child.pid"
    body = (
        PRELUDE
        + _teardown_source()
        + f"""
set -m
( bash -c 'sh -c "trap \\"\\" TERM; while :; do sleep 1; done" & echo $! > {marker}; trap "exit 0" TERM; while :; do sleep 1; done' ) &
LEADER=$!
set +m
sleep 1
stop_service worker "$LEADER"
echo REACHED_END
"""
    )
    result = _run(body)
    assert "REACHED_END" in result.stdout, result.stderr
    child = int(marker.read_text().strip())
    assert (
        subprocess.run(["kill", "-0", str(child)], capture_output=True).returncode != 0
    ), f"descendant {child} outlived stop_service()"


def test_fallback_still_signals_when_pid_is_not_a_group_leader():
    """codex r2 P2: errexit must not abort stop_service() before it signals."""
    body = (
        PRELUDE
        + _teardown_source()
        + """
set +m
bash -c 'trap : TERM; while :; do sleep 1; done' & SERVICE=$!
sleep 0.3
stop_service fallback "$SERVICE"
echo REACHED_END
if kill -0 "$SERVICE" 2>/dev/null; then echo SERVICE_ALIVE; kill -KILL "$SERVICE"; else echo SERVICE_DEAD; fi
"""
    )
    result = _run(body)
    assert result.returncode == 0, f"stop_service aborted: {result.stderr}"
    assert "REACHED_END" in result.stdout, result.stderr
    assert "SERVICE_DEAD" in result.stdout, result.stdout


def test_escalation_warning_is_emitted_for_a_sigterm_ignoring_service():
    body = (
        PRELUDE
        + _teardown_source()
        + """
set -m
( trap "" TERM; while :; do sleep 1; done ) & SERVICE=$!
set +m
sleep 0.3
stop_service stubborn "$SERVICE"
"""
    )
    result = _run(body)
    assert "ignored SIGTERM" in result.stderr, result.stderr
    assert "escalated to SIGKILL" in result.stderr, result.stderr


def test_stop_service_never_signals_init_or_a_bogus_pid(tmp_path):
    """codex round 3, P3: the previous version of this test could SIGTERM pid 1.

    `service_pgid()` declines to resolve a group for pid<=1, but that only
    suppressed the GROUP form -- the direct `kill -TERM "${pid}"` still fired, so
    as root in a container this would have signalled init. Asserting "it returned
    0" hid that, because signalling pid 1 also returns 0. Shadow `kill` and
    assert NOTHING was signalled at all, including `kill -0`.
    """
    log = tmp_path / "kill.log"
    body = (
        PRELUDE
        + _teardown_source()
        + f"""
kill() {{ echo "KILL $*" >> {log}; return 0; }}
stop_service none ""
stop_service bogus "not-a-pid"
stop_service negative "-1"
stop_service zero "0"
stop_service init "1"
echo REACHED_END
"""
    )
    result = _run(body)
    assert result.returncode == 0, result.stderr
    assert "REACHED_END" in result.stdout
    recorded = log.read_text() if log.exists() else ""
    assert recorded == "", (
        f"stop_service signalled something it must refuse: {recorded!r}"
    )


def _teardown_wait_secs_source() -> str:
    """validate_teardown_wait_secs's body, sliced out of the shared library.

    CHAOS-5362: the function only validates whatever TEARDOWN_WAIT_SECS already
    holds -- the METRICS_PROOF_TEARDOWN_WAIT_SECS-to-TEARDOWN_WAIT_SECS mapping
    is now the CALLER's job (ci/run_metrics_executed_proof.sh does it inline,
    immediately before calling this function), not the function's own. Tests
    below do that mapping themselves before sourcing/calling it.
    """
    return _slice(LIB, r"^validate_teardown_wait_secs\(\) \{", r"^service_pgid\(\) \{")


@pytest.mark.parametrize("bad", ["abc", "12.5", "0", "-5", "999999", "60s"])
def test_teardown_wait_secs_fails_closed_on_junk(bad):
    """codex r1 P2: an unvalidated value silently became an instant SIGKILL."""
    result = _run(
        f'set -euo pipefail\nMETRICS_PROOF_TEARDOWN_WAIT_SECS="{bad}"\n'
        f'TEARDOWN_WAIT_SECS="${{METRICS_PROOF_TEARDOWN_WAIT_SECS:-60}}"\n'
        + _teardown_wait_secs_source()
        + '\nvalidate_teardown_wait_secs\necho "resolved=${TEARDOWN_WAIT_SECS}"\n'
    )
    assert result.returncode == 0, result.stderr
    assert "resolved=60" in result.stdout, result.stdout
    assert "WARNING" in result.stderr, result.stderr


def test_empty_override_defaults_silently():
    """An EMPTY value is `unset`, not junk: `${VAR:-60}` already yields 60.

    Asserting a warning here was a wrong expectation on my part, not a defect --
    recorded so the distinction between "absent" and "invalid" stays explicit.
    """
    result = _run(
        'set -euo pipefail\nMETRICS_PROOF_TEARDOWN_WAIT_SECS=""\n'
        'TEARDOWN_WAIT_SECS="${METRICS_PROOF_TEARDOWN_WAIT_SECS:-60}"\n'
        + _teardown_wait_secs_source()
        + '\nvalidate_teardown_wait_secs\necho "resolved=${TEARDOWN_WAIT_SECS}"\n'
    )
    assert result.returncode == 0, result.stderr
    assert "resolved=60" in result.stdout, result.stdout
    assert "WARNING" not in result.stderr, result.stderr


def test_teardown_wait_secs_accepts_a_valid_override():
    result = _run(
        'set -euo pipefail\nMETRICS_PROOF_TEARDOWN_WAIT_SECS="45"\n'
        'TEARDOWN_WAIT_SECS="${METRICS_PROOF_TEARDOWN_WAIT_SECS:-60}"\n'
        + _teardown_wait_secs_source()
        + '\nvalidate_teardown_wait_secs\necho "resolved=${TEARDOWN_WAIT_SECS}"\n'
    )
    assert "resolved=45" in result.stdout, result.stdout


def test_worker_shutdown_timeout_stays_at_the_contract_minimum():
    """CHAOS-3873 contract: shutdownTimeout >= longestTimeout + 60s buffer.

    Lowering this to 30s made the worker refuse to start, reported by the caller
    as the unrelated `queue_coverage_validation_failed` -- which is what broke
    #2212's CI. The value is a computed minimum, not a round number.

    CHAOS-5362: the dev-health-worker invocation carrying this flag moved into
    start_worker_stack() in the shared library.
    """
    assert "--shutdown-timeout=7260s" in LIB.read_text(), (
        "the shared worker fixture's shutdown-timeout must stay at the contract "
        "minimum (7200s longest selected timeout + 60s workerFinalizationBuffer); "
        "see cmd/dev-health-worker/dependencies.go"
    )


def _composed_teardown_and_trap_source() -> str:
    """service_pgid/stop_service/stop_worker_stack (library) PLUS
    cleanup()/on_signal()/the trap wiring (script), concatenated in the same
    order the real scripts source/define them in.

    CHAOS-5362 split what used to be one contiguous slice across two files:
    the pgid/kill/watchdog mechanism moved to the shared library, while the
    re-entrancy-safe trap wiring that calls it stayed in the script. Composing
    both here keeps these two tests exercising the REAL interaction between
    them, not a decoy reconstruction.
    """
    lib_part = _slice(
        LIB, r"^service_pgid\(\) \{", r"^seed_and_finalize_sync_targets\(\) \{"
    )
    script_part = _slice(
        SCRIPT, r"^CLEANUP_DONE=\"\"$", r"^trap 'on_signal TERM' TERM$"
    )
    return lib_part + script_part + "trap 'on_signal TERM' TERM\n"


def test_a_cancelled_run_tears_down_and_exits_instead_of_resuming(tmp_path):
    """codex round 3, P1: a trapped signal runs the handler and then RESUMES.

    Bound directly to INT/TERM, `cleanup` tore down the services, deleted
    TMP_DIR, and the script carried on executing the proof against nothing. This
    exercises the REAL trap wiring -- `on_signal` and the `trap` lines, not just
    the helpers above -- which is the coverage gap round 3 named.
    """
    full = _composed_teardown_and_trap_source()
    marker = tmp_path / "resumed"
    body = f"""set -euo pipefail
TMP_DIR="$(mktemp -d)"
API_PID=""; WORKER_PID=""; RECONCILER_PID=""
TEARDOWN_WAIT_SECS=1
{full}
set -m
( trap "" TERM; while :; do sleep 1; done ) & WORKER_PID=$!
set +m
( sleep 0.3; kill -INT $$ ) &
sleep 5
echo RESUMED > {marker}
"""
    result = _run(body, timeout=45)
    assert not marker.exists(), "the script resumed after a cancellation signal"
    # subprocess reports death-by-signal as -signum; a shell in between would
    # report the same thing as 128+signum. Accept either spelling, reject 0.
    assert result.returncode in (-2, 130), (
        f"want death-by-SIGINT (-2, or 130 via a shell) so the run reports "
        f"cancellation rather than success, got {result.returncode}"
    )


def test_teardown_is_fast_with_the_cleanup_signal_disposition_in_effect():
    """The regression that a PASSING CI run hid (run 33869945044).

    `cleanup()` sets ``trap '' INT TERM`` on entry, and an IGNORED disposition is
    INHERITED across fork. The watchdog is forked after that line, so cancelling
    it with SIGTERM was a no-op and `wait` blocked for the FULL bound -- per
    service, every run: worker 11:57:38, reconciler 11:58:38, api 11:59:38, step
    end 12:00:38. Every service had exited instantly; the 3 minutes were pure
    watchdog wait, with no escalation warning because `elapsed` is measured
    around `wait "${pid}"`, which really was fast.

    Every earlier test missed it by calling `stop_service` in ISOLATION,
    where the disposition is never set -- the defect lives precisely in the
    interaction the isolation removed. So this one sets the disposition FIRST,
    exactly as cleanup() does, and asserts on elapsed WALL TIME.
    """
    body = (
        "set -euo pipefail\nTEARDOWN_WAIT_SECS=20\n"
        + _teardown_source()
        + """
# ORDER MIRRORS THE REAL SCRIPT and is load-bearing: the services are forked
# FIRST (they must NOT inherit the ignore -- in the real run they are launched
# long before cleanup() is ever entered), and only THEN is the disposition set,
# as cleanup() does on entry. Setting it first made the services themselves
# ignore SIGTERM, so even the prompt one took the full bound -- a test-setup
# artefact, not the defect under test.
set -m
( trap "" TERM; while :; do sleep 1; done ) & STUBBORN=$!
( while :; do sleep 1; done ) & PROMPT=$!
set +m
sleep 0.3
trap '' INT TERM          # what cleanup() does on entry, before forking the watchdog
start=$SECONDS
stop_service prompt "$PROMPT"
echo "PROMPT_ELAPSED=$((SECONDS - start))"
start=$SECONDS
stop_service stubborn "$STUBBORN"
echo "STUBBORN_ELAPSED=$((SECONDS - start))"
"""
    )
    result = _run(body, timeout=120)
    assert result.returncode == 0, result.stderr

    prompt_match = re.search(r"PROMPT_ELAPSED=(\d+)", result.stdout)
    stubborn_match = re.search(r"STUBBORN_ELAPSED=(\d+)", result.stdout)
    # Assert the markers exist rather than indexing a possibly-None match: a
    # missing marker means the script died before reaching the echo, which is a
    # different failure and deserves to say so instead of an AttributeError.
    assert prompt_match is not None, f"no PROMPT_ELAPSED in: {result.stdout!r}"
    assert stubborn_match is not None, f"no STUBBORN_ELAPSED in: {result.stdout!r}"
    prompt = int(prompt_match.group(1))
    stubborn = int(stubborn_match.group(1))

    # A service that exits on SIGTERM must cost ~0s, NOT the bound: the whole
    # defect was paying the full bound for a service that was already gone.
    assert prompt <= 5, (
        f"a service that exits promptly took {prompt}s against a 20s bound -- "
        "the watchdog was not cancelled (SIGTERM ignored via the inherited "
        "disposition?)"
    )
    # A service that IGNORES SIGTERM legitimately costs the bound, and must
    # still be reported. This is the control: without it the assertion above
    # could pass for a stop_service() that never waits for anything at all.
    assert stubborn >= 20, (
        f"a SIGTERM-ignoring service took only {stubborn}s against a 20s bound; "
        "the bound is not being honoured"
    )
    assert "ignored SIGTERM" in result.stderr, result.stderr
