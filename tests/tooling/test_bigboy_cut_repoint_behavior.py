"""bigboy-cut.sh: the post-cut routing repoint STEP actually fails the cut on failure.

D2823 (r1 P1 review of CHAOS-7022, #3362): test_bigboy_cut_routing_carry.py only reads the
script as TEXT (line order, substring presence) -- it never executes anything, so it cannot
tell a real guard from a guard whose `exit 1` is dead code. The reviewer proved this directly:
inserting an unconditional `exit 0` immediately before the post-cut repoint call left that
text-only suite's `7 passed` unchanged.

This module extracts the real STEP block VERBATIM (content-anchored, never a re-typed copy)
and actually RUNS it under bash with a stubbed `vt`/`st`, the same technique the r1 reviewer
used to reproduce both P1s. Two properties are proven behaviorally, not textually:

1. A failing post-cut repoint (stubbed `vt` rc=42) must abort the harness (nonzero exit,
   the sentinel line after the block never prints) -- not just log the rc and continue.
2. The repoint call is pinned with `-expect-build $NEW` -- captured from the REAL call the
   stubbed `vt` actually received, not grepped from elsewhere in the file.
"""

from __future__ import annotations

import stat
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CUT = ROOT / "ci" / "bigboy" / "bigboy-cut.sh"

# Start right after the PRECEDING step (go-api-ready), not the repoint call itself: a
# mutation that inserts an early `exit`/`return` ANYWHERE in the gap between the two steps
# -- not just on the exact line immediately above the repoint call -- must still land
# inside the executed block, or this test would miss it the same way the text-only suite
# did (D2823 finding: the reviewer's `exit 0` landed one line above the repoint call and
# that text-only suite never noticed).
_START_NEEDLE = "# CHAOS-7022 (D2811 addendum): repoint after EVERY cut"
_END_NEEDLE = "routing rows may be stale (CHAOS-7022)"

_HARNESS_SENTINEL = "HARNESS_REACHED_END_OF_BLOCK"


def _extract_block() -> str:
    lines = CUT.read_text().splitlines()
    start = next(i for i, ln in enumerate(lines) if _START_NEEDLE in ln)
    end = next(i for i in range(start, len(lines)) if _END_NEEDLE in lines[i])
    return "\n".join(lines[start : end + 1])


def _run_harness(*, vt_stub_rc: int) -> tuple[subprocess.CompletedProcess[str], str]:
    block = _extract_block()
    with tempfile.TemporaryDirectory(prefix="chaos7022-repoint-behavior-") as tmp:
        tmp_path = Path(tmp)
        vt_capture = tmp_path / "vt_capture.txt"
        rec_prefix = tmp_path / "rec"
        harness = tmp_path / "harness.sh"
        harness.write_text(
            "#!/usr/bin/env bash\n"
            "set -u\n"
            f"REC={rec_prefix}\n"
            "NEW=1111111111111111111111111111111111111111\n"
            "OLD8=aaaaaaaa\n"
            "N8=${NEW:0:8}\n"
            "ROUTING_ORG=test-org-fixture\n"
            f"vt() {{ printf '%s\\n' \"$1\" > {vt_capture}; return {vt_stub_rc}; }}\n"
            'st() { echo "STEP $1 rc=$2"; }\n'
            f"{block}\n"
            f"echo {_HARNESS_SENTINEL}\n"
        )
        harness.chmod(harness.stat().st_mode | stat.S_IEXEC)
        proc = subprocess.run(
            ["bash", str(harness)],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        captured_call = vt_capture.read_text() if vt_capture.exists() else ""
        return proc, captured_call


def test_a_failing_post_cut_repoint_actually_fails_the_harness() -> None:
    """D2823 P1 #1, behaviorally: rc=42 from the real repoint call must abort, not just log."""
    proc, _ = _run_harness(vt_stub_rc=42)
    assert proc.returncode != 0, (
        "a post-cut repoint that fails (rc=42) must make the harness exit nonzero -- "
        f"got rc={proc.returncode}, stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert _HARNESS_SENTINEL not in proc.stdout, (
        "the harness reached the line AFTER the repoint block despite the repoint call "
        "failing -- the abort guard did not actually stop execution"
    )
    assert "STEP routing-repoint rc=42" in proc.stdout


def test_a_succeeding_post_cut_repoint_lets_the_harness_continue() -> None:
    """Positive control: rc=0 must NOT abort -- proves the assertion above tests the real
    guard, not an unconditional failure."""
    proc, _ = _run_harness(vt_stub_rc=0)
    assert proc.returncode == 0, (
        f"a succeeding post-cut repoint (rc=0) must not abort the harness -- "
        f"got rc={proc.returncode}, stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    assert _HARNESS_SENTINEL in proc.stdout, (
        "the harness never reached the line after the repoint block even though the "
        "repoint call succeeded"
    )


def test_the_repoint_call_actually_received_is_pinned_with_expect_build() -> None:
    """D2823 P1 #2, behaviorally: the ACTUAL string the stubbed vt() received (not a grep of
    the source file) must carry -expect-build $NEW."""
    _, captured_call = _run_harness(vt_stub_rc=0)
    assert captured_call, (
        "the stub vt() was never invoked -- the block did not run at all"
    )
    assert "-expect-build 1111111111111111111111111111111111111111" in captured_call, (
        "the real post-cut repoint invocation the stub received does not carry "
        f"-expect-build $NEW: {captured_call!r}"
    )
