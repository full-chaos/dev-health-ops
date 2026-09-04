"""CHAOS-5025: regression coverage for ci/run_metrics_executed_proof.sh teardown.

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

These drive the SHIPPED functions, sliced out of the real script by anchor, so
they cannot drift from it.
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

# A module-level `pytest.skip(...)` call rather than a `pytestmark` assignment:
# tests/tooling's orphan-definition guard parses the AST and cannot see pytest's
# implicit consumption of `pytestmark`, so the assignment reads as a definition
# that is never used. This is a call, so there is nothing to orphan.
if shutil.which("bash") is None or not SCRIPT.is_file():
    pytest.skip("needs bash and the proof script", allow_module_level=True)

PRELUDE = "set -euo pipefail\nTEARDOWN_WAIT_SECS=1\n"


def _slice(start_pattern: str, end_pattern: str) -> str:
    """Slice the shipped source between two anchors, both required to be UNIQUE.

    Uniqueness is the whole guard (codex round 3, P2). With only "first match"
    semantics a script could carry a WORKING decoy definition before the anchor
    and a BROKEN real one after it: bash uses the later definition, while these
    tests would happily exercise the decoy and report green. That was
    demonstrated -- 13 tests passed against a `stop_service()` that returned
    without signalling any real service.
    """
    text = SCRIPT.read_text()
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


def test_teardown_functions_are_defined_exactly_once():
    """codex round 3, P2: a duplicate definition makes the slice test a decoy."""
    text = SCRIPT.read_text()
    for name in ("service_pgid", "stop_service", "cleanup", "on_signal"):
        found = re.findall(rf"^{name}\(\) \{{", text, re.M)
        assert len(found) == 1, (
            f"{name}() is defined {len(found)}x in the script; a second "
            "definition would shadow the one these tests slice out"
        )


def _teardown_source() -> str:
    return _slice(r"^service_pgid\(\) \{", r"^cleanup\(\) \{")


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


@pytest.mark.parametrize("bad", ["abc", "12.5", "0", "-5", "999999", "60s"])
def test_teardown_wait_secs_fails_closed_on_junk(bad):
    """codex r1 P2: an unvalidated value silently became an instant SIGKILL."""
    block = _slice(r"^TEARDOWN_WAIT_SECS=", r"^service_pgid\(\) \{")
    result = _run(
        f'set -euo pipefail\nMETRICS_PROOF_TEARDOWN_WAIT_SECS="{bad}"\n'
        + block
        + '\necho "resolved=${TEARDOWN_WAIT_SECS}"\n'
    )
    assert result.returncode == 0, result.stderr
    assert "resolved=60" in result.stdout, result.stdout
    assert "WARNING" in result.stderr, result.stderr


def test_empty_override_defaults_silently():
    """An EMPTY value is `unset`, not junk: `${VAR:-60}` already yields 60.

    Asserting a warning here was a wrong expectation on my part, not a defect --
    recorded so the distinction between "absent" and "invalid" stays explicit.
    """
    block = _slice(r"^TEARDOWN_WAIT_SECS=", r"^service_pgid\(\) \{")
    result = _run(
        'set -euo pipefail\nMETRICS_PROOF_TEARDOWN_WAIT_SECS=""\n'
        + block
        + '\necho "resolved=${TEARDOWN_WAIT_SECS}"\n'
    )
    assert result.returncode == 0, result.stderr
    assert "resolved=60" in result.stdout, result.stdout
    assert "WARNING" not in result.stderr, result.stderr


def test_teardown_wait_secs_accepts_a_valid_override():
    block = _slice(r"^TEARDOWN_WAIT_SECS=", r"^service_pgid\(\) \{")
    result = _run(
        'set -euo pipefail\nMETRICS_PROOF_TEARDOWN_WAIT_SECS="45"\n'
        + block
        + '\necho "resolved=${TEARDOWN_WAIT_SECS}"\n'
    )
    assert "resolved=45" in result.stdout, result.stdout


def test_worker_shutdown_timeout_stays_at_the_contract_minimum():
    """CHAOS-3873 contract: shutdownTimeout >= longestTimeout + 60s buffer.

    Lowering this to 30s made the worker refuse to start, reported by the caller
    as the unrelated `queue_coverage_validation_failed` -- which is what broke
    #2212's CI. The value is a computed minimum, not a round number.
    """
    assert "--shutdown-timeout=7260s" in SCRIPT.read_text(), (
        "the proof's worker shutdown-timeout must stay at the contract minimum "
        "(7200s longest selected timeout + 60s workerFinalizationBuffer); see "
        "cmd/dev-health-worker/dependencies.go"
    )


def test_a_cancelled_run_tears_down_and_exits_instead_of_resuming(tmp_path):
    """codex round 3, P1: a trapped signal runs the handler and then RESUMES.

    Bound directly to INT/TERM, `cleanup` tore down the services, deleted
    TMP_DIR, and the script carried on executing the proof against nothing. This
    exercises the REAL trap wiring -- `on_signal` and the `trap` lines, not just
    the helpers above -- which is the coverage gap round 3 named.
    """
    full = _slice(r"^service_pgid\(\) \{", r"^trap 'on_signal TERM' TERM$")
    full += "trap 'on_signal TERM' TERM\n"
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
