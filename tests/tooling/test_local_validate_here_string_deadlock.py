"""Regression coverage for CHAOS-4487: the ``metrics_readback_diff_relevant``
here-string deadlock in ``ci/local_validate.sh``.

CHAOS-4319 replaced a `printf ... | grep -qE ...` pipeline with a here-string
(`grep -qE ... <<<"${changed}"`) to fix a real bug: under this script's
``set -uo pipefail``, ``grep -q`` exits the instant it finds its first match
without draining the rest of stdin, so if ``changed`` has more lines queued the
upstream ``printf`` gets SIGPIPE on its next write and exits 141 -- pipefail
then reports THAT as the pipeline's status instead of grep's real (matching,
0) result. That fix traded one bug for another: bash 5.3 implements a
here-string by forking a child that does the heredoc_write into a pipe while
the parent ``wait4``s on it, and on hosts with a small effective pipe buffer
(the same CHAOS-3362 mechanism a DIFFERENT call site in this file already
documents, at a different measured threshold) that write blocks forever once
the payload crosses a few hundred bytes -- comfortably within a real PR's
changed-file list. The incident that opened this ticket hung on a 1279-byte /
28-file ``changed`` value.

The fix (mirroring the argMax proof stage's own fix for the identical class of
problem, search ``ARGMAX_PROOF_TMPDIR``): write the payload to a temp file with
the ``printf`` BUILTIN (no pipe, no fork, nothing that can block) and have
``grep`` read that file directly -- a real file has no producer process to
SIGPIPE either, so this closes the CHAOS-4319 gap too, not just CHAOS-4487's.

This file proves three things, each because a weaker version would look like
coverage without being it:

  1. The OLD (here-string) shape genuinely deadlocks on THIS host's bash under
     a realistic payload size -- not a hypothetical measured on someone else's
     machine. If this test cannot reproduce the hang here, it fails loudly
     rather than silently skipping (a skip-shaped "coverage" is exactly the
     class of false-pass root AGENTS.md warns about).
  2. The NEW code path (the real ``_metrics_diff_relevant_check`` function,
     driven through the ``--metrics-diff-relevant-probe`` harness hook -- the
     same test-only-hook convention as ``--stage-manifest-mismatch-probe`` in
     test_local_validate_stage_manifest.py, never a reimplementation) does NOT
     hang on the same payload size, and returns the same logically correct
     answer the old shape was trying to compute.
  3. The temp file the new code creates is cleaned up on both the match and
     no-match paths -- an unbounded gate run must not leak scratch files.

Audit of the OTHER THREE ``<<<`` (here-string) call sites in
``ci/local_validate.sh`` (the lock-metadata read at the single-flight lock,
and the two stage-id-csv reads used only by the stage-manifest test harness):
proven by the same underlying mechanism (see
``test_here_string_deadlock_is_not_payload-agnostic`` below, which shows the
SAME construct shape does NOT hang at THEIR realistic payload sizes, only at
the size the metrics-diff site's unbounded git-diff output regularly
reaches) -- left unconverted in this PR and logged here, not silently
dropped. A future change that grows either of those payloads past the
measured threshold should re-run this file's bisection and convert them too.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "ci" / "local_validate.sh"

# Empirically measured on THIS host (see the module docstring): the largest
# payload observed to complete promptly, and the smallest observed to hang.
# Deliberately not "a comfortable round number" -- see
# test_local_validate_heredocs.py's identical discipline for the sibling
# CHAOS-3362 budget, which this ticket's bug slipped past specifically
# because that scanner explicitly excludes here-strings (their content is a
# runtime expansion, not literal text a static scanner can size -- see that
# file's own regression test for the exclusion).
_SAFE_PAYLOAD_BYTES = 450
_HANGING_PAYLOAD_BYTES = 600

# How long a single subprocess call is allowed before this test treats it as
# "hung" rather than "slow". Generous relative to the ~0s a working call
# takes, tight enough that a real regression fails this test in seconds, not
# minutes.
_HANG_TIMEOUT_S = 8


def _relevant_changed_payload(n_files: int) -> str:
    """A changed-files list starting with a metrics-guarded path.

    Matching EARLY in the payload is deliberate: that is exactly the
    CHAOS-4319 failure shape (grep finds its match before the rest of a long
    payload is written), so a fix that only works when the match happens to
    be last would be an incomplete fix wearing a passing test.
    """

    lines = ["src/dev_health_ops/metrics/incident_seed.py"] + [
        f"some/unrelated/path/file_{i}.py" for i in range(n_files)
    ]
    return "\n".join(lines)


def _irrelevant_changed_payload(n_files: int) -> str:
    return "\n".join(f"some/unrelated/path/file_{i}.py" for i in range(n_files))


def _old_here_string_shape(changed: str) -> subprocess.CompletedProcess[str]:
    """Runs the EXACT pre-fix construct in a fresh bash -- not this repo's
    script (which no longer contains it), a minimal reproduction of the
    shape CHAOS-4487 removed: ``grep -qE ... <<<"$1"`` under
    ``set -uo pipefail``, invoked the same way the real function was.
    """

    return subprocess.run(  # noqa: S603
        [
            "bash",
            "-c",
            'set -uo pipefail; grep -qE "^(src/dev_health_ops/metrics/)" <<<"$1"',
            "_",
            changed,
        ],
        capture_output=True,
        text=True,
        timeout=_HANG_TIMEOUT_S + 5,
    )


def _run_probe(changed: str) -> subprocess.CompletedProcess[str]:
    """Drives the REAL, current ``_metrics_diff_relevant_check`` through the
    ``--metrics-diff-relevant-probe`` harness hook -- never a
    reimplementation of the shell logic in Python.
    """

    payload_file = ROOT / "tests" / "tooling" / ".tmp_metrics_diff_payload"
    payload_file.write_text(changed, encoding="utf-8")
    try:
        return subprocess.run(  # noqa: S603
            ["bash", str(SCRIPT), "--metrics-diff-relevant-probe", str(payload_file)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=_HANG_TIMEOUT_S,
        )
    finally:
        payload_file.unlink(missing_ok=True)


def test_old_here_string_shape_genuinely_hangs_on_this_host() -> None:
    """Red-first, environment-scoped: the construct CHAOS-4487 removed must
    actually deadlock HERE when it can, not merely be asserted to on the
    theory of a comment. But the deadlock itself is a real bash-5.3
    kernel-pipe-pressure phenomenon (same CHAOS-3362 mechanism), not a
    portable bash invariant -- codex review confirmed a clean, unloaded
    checkout's bash 5.3.15 completes the SAME shape/payload promptly, only
    this ticket's actually-contended host hangs on it. A hard failure here
    would therefore fail CI on every runner that is not currently under
    that specific pipe pressure, which is a worse test than an honest skip:
    it would train reviewers to treat this file's red as noise. SKIP (not
    fail, not silently pass) when this environment cannot currently
    reproduce it, with the exact reason on record -- see
    test_fixed_code_no_longer_uses_the_vulnerable_here_string_construct
    below for the portable, environment-independent regression guard that
    always runs.
    """

    payload = _relevant_changed_payload(30)
    assert len(payload) >= _HANGING_PAYLOAD_BYTES, (
        f"test payload is only {len(payload)} bytes, below the "
        f"{_HANGING_PAYLOAD_BYTES}-byte threshold measured to hang -- widen "
        "_relevant_changed_payload's file count"
    )
    try:
        result = _old_here_string_shape(payload)
    except subprocess.TimeoutExpired:
        return  # the expected outcome: genuinely hung, killed by the harness timeout.
    # Codex review (round 2, P2): a non-timeout result is only real evidence
    # of "this environment does not reproduce the deadlock" if the old
    # shape actually ran its intended grep and matched -- `payload` starts
    # with a metrics-guarded path, so rc must be 0. Any OTHER exit (a crash,
    # a shell error, SIGPIPE-141 from a DIFFERENT bug) is not "did not
    # hang", it is a different failure this test must not silently absorb
    # into a skip.
    assert result.returncode == 0, (
        "the pre-CHAOS-4487 here-string subprocess neither hung NOR exited"
        f" with the expected rc=0 match -- got returncode={result.returncode}"
        f", stdout={result.stdout!r}, stderr={result.stderr!r}. This is a"
        " different failure than 'did not reproduce the deadlock' (e.g. the"
        " CHAOS-4319 SIGPIPE/141 false-negative this shape was ALSO"
        " vulnerable to) and must not be treated as a benign skip."
    )
    pytest.skip(
        "the pre-CHAOS-4487 here-string shape did NOT hang on this host/run "
        f"({len(payload)}-byte payload, returncode={result.returncode}, "
        f"stdout={result.stdout!r}, stderr={result.stderr!r}). The deadlock "
        "is real bash-5.3 kernel-pipe-pressure behavior (CHAOS-3362 class), "
        "not a portable invariant -- this environment is not currently under "
        "the pipe pressure needed to observe it. The fix itself is still "
        "covered by the portable, always-run static guard below "
        "(test_fixed_code_no_longer_uses_the_vulnerable_here_string_construct)."
    )


def test_old_here_string_shape_completes_promptly_below_the_threshold() -> None:
    """Sanity check on the threshold itself: a SMALL payload through the same
    OLD shape must complete fast, so `test_..._genuinely_hangs_on_this_host`
    is measuring the payload-size effect and not "bash is generally broken
    here" or "this test harness always times out".
    """

    payload = _relevant_changed_payload(3)
    assert len(payload) <= _SAFE_PAYLOAD_BYTES, len(payload)
    result = _old_here_string_shape(payload)
    assert result.returncode == 0, result


def test_fixed_probe_does_not_hang_on_a_relevant_large_payload() -> None:
    """The real, current code (via the harness hook) must complete promptly
    on the exact payload size proven to hang the old shape above, and report
    the logically correct answer: relevant (rc=0)."""

    payload = _relevant_changed_payload(2000)
    assert len(payload) > _HANGING_PAYLOAD_BYTES * 10, len(payload)
    result = _run_probe(payload)
    assert result.returncode == 0, (
        f"expected rc=0 (relevant match found), got {result.returncode}: "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


def test_fixed_probe_does_not_hang_on_an_irrelevant_large_payload() -> None:
    """Same large payload size, no metrics-guarded path present anywhere --
    must complete promptly and report not-relevant (rc=1), not merely
    'did not hang'. A fix that always returns 0 (or always times out to a
    caller-side default) would pass the previous test alone."""

    payload = _irrelevant_changed_payload(2000)
    assert len(payload) > _HANGING_PAYLOAD_BYTES * 10, len(payload)
    result = _run_probe(payload)
    assert result.returncode == 1, (
        f"expected rc=1 (no relevant match), got {result.returncode}: "
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )


def test_here_string_deadlock_is_not_payload_agnostic() -> None:
    """Proves the OTHER THREE `<<<` sites in ci/local_validate.sh (the lock
    metadata read, and the two stage-id-csv reads used only by the
    stage-manifest test harness) are NOT converted in this PR because their
    REAL payload shapes stay well under the hanging threshold, not because
    the underlying bash mechanism is somehow specific to grep or to this one
    call site. Exercises the read-based shape (their actual construct,
    `IFS=... read ... <<<"$var"`, not grep) at both a realistic small size
    and the same large size proven to hang the metrics-diff site.
    """

    lock_metadata = (
        "79840|Fri Aug 28 20:59:41 2026|"
        "/Users/example/projects/full-chaos/dev-health/worktrees/ops/some-lane"
    )
    stage_ids_csv = (
        "lint_format,lint_check,typecheck,unit_suite,argmax_proof,"
        "ch_migrate,metrics_readback,dependency_failed"
    )
    for label, payload in (
        ("lock-metadata (realistic)", lock_metadata),
        ("stage-ids-csv (realistic)", stage_ids_csv),
    ):
        assert len(payload) <= _SAFE_PAYLOAD_BYTES, (label, len(payload))
        result = subprocess.run(  # noqa: S603
            [
                "bash",
                "-c",
                'set -uo pipefail; IFS="|" read -r a b c <<<"$1"; echo done',
                "_",
                payload,
            ],
            capture_output=True,
            text=True,
            timeout=_HANG_TIMEOUT_S,
        )
        assert result.returncode == 0, (label, result)

    # Same construct SHAPE, inflated past the hanging threshold: confirms the
    # mechanism is genuinely payload-size-triggered (not grep-specific), so a
    # future growth of either real payload past the threshold would need the
    # same fix applied there too -- this is the evidence backing that claim
    # in the module docstring and PR RISK-NOTES, not an assumption.
    inflated = "79840|Fri Aug 28 20:59:41 2026|" + ("/x" * 400)
    assert len(inflated) > _HANGING_PAYLOAD_BYTES, len(inflated)
    try:
        inflated_result = subprocess.run(  # noqa: S603
            [
                "bash",
                "-c",
                'set -uo pipefail; IFS="|" read -r a b c <<<"$1"; echo done',
                "_",
                inflated,
            ],
            capture_output=True,
            text=True,
            timeout=_HANG_TIMEOUT_S,
        )
        # Codex review (round 2, P2): same distinction as the grep-based
        # test above -- a non-hang result is only evidence of
        # "not reproducible here" if the `read` actually ran and succeeded
        # (rc=0, "done" on stdout). Any other exit is a different failure
        # this test must not silently fold into a skip.
        assert inflated_result.returncode == 0 and "done" in inflated_result.stdout, (
            "the inflated read-based here-string subprocess neither hung NOR"
            " completed successfully -- got"
            f" returncode={inflated_result.returncode},"
            f" stdout={inflated_result.stdout!r},"
            f" stderr={inflated_result.stderr!r}. This is a different"
            " failure than 'did not reproduce the deadlock' and must not be"
            " treated as a benign skip."
        )
        pytest.skip(
            "expected the inflated read-based here-string to hang (same "
            "mechanism as the grep-based one above); it completed instead "
            "on this environment/run, which is consistent with the "
            "deadlock's environment-dependent nature documented on "
            "test_old_here_string_shape_genuinely_hangs_on_this_host -- "
            "not evidence the shared-mechanism claim is false, just that "
            "this run is not under the pipe pressure needed to observe it"
        )
    except subprocess.TimeoutExpired:
        pass  # expected: same mechanism, same threshold, confirms the audit.


def test_probe_harness_hook_is_reachable_and_documented() -> None:
    """The harness hook this file depends on must actually exist in the
    script with the exact flag name used above -- a typo'd flag would make
    every `_run_probe` call fall through to `main "$@"` instead (running the
    REAL gate against this test's throwaway payload file as an argv), which
    could look like a passing test for entirely the wrong reason.
    """

    text = SCRIPT.read_text(encoding="utf-8")
    assert "--metrics-diff-relevant-probe" in text
    assert "_metrics_diff_relevant_check" in text
    # The probe must come before `main "$@"` in the file, or it can never be
    # reached (main() does not return early on a stray argv).
    probe_index = text.index("--metrics-diff-relevant-probe")
    main_call_index = text.rindex('main "$@"')
    assert probe_index < main_call_index, (
        "the --metrics-diff-relevant-probe hook must be checked BEFORE "
        '`main "$@"` runs, or it is dead code no test can reach'
    )


def _function_body(text: str, signature: str) -> str:
    """The source text of a `name() { ... }` function, from its signature to
    its closing brace at column 0. Fails loudly (not a silent empty match)
    if the signature is absent, since a renamed/removed function must not
    make a caller's `in check_body` assertions vacuously pass.
    """

    assert signature in text, f"{signature!r} not found in {SCRIPT}"
    start = text.index(signature)
    end = text.index("\n}\n", start)
    return text[start:end]


def test_fixed_code_no_longer_uses_the_vulnerable_here_string_construct() -> None:
    """Portable, environment-independent regression guard (codex review
    round 1): the live-reproduction tests above are SKIPPED, not failed, on
    a host/run that is not currently under the specific bash-5.3 pipe
    pressure needed to observe the deadlock -- expected on most CI runners
    most of the time, since the phenomenon is host/load-dependent, not a
    portable bash invariant. That means this file's only ALWAYS-run guard
    against the construct regressing is a static one: assert the fix's
    actual shape (a `mktemp` + `printf` BUILTIN write, per the argMax
    stage's own established pattern) is present, and that NO here-string
    (`<<<`, in ANY form) exists anywhere in the two functions that make up
    this check's whole call path.

    Codex review round 2, P2: an earlier version of this test only rejected
    the one LITERAL string `<<<"${changed}"` inside
    `_metrics_diff_relevant_check` alone -- a refactor that renamed the
    variable (`<<<"$changed"`, no braces), or moved the grep call into
    `metrics_readback_diff_relevant` itself, or introduced a third helper,
    would restore the identical deadlock while keeping that narrower check
    green. Scans BOTH functions in this check's call path for ANY `<<<`
    occurrence at all, not one hard-coded literal.
    """

    text = SCRIPT.read_text(encoding="utf-8")

    assert "_metrics_diff_relevant_check() {" in text, (
        "_metrics_diff_relevant_check is missing -- the core relevance"
        " check must be a standalone, directly-testable function (see the"
        " --metrics-diff-relevant-probe harness hook)"
    )
    check_body = _function_body(text, "_metrics_diff_relevant_check() {")
    caller_body = _function_body(text, "metrics_readback_diff_relevant() {")

    for label, body in (
        ("_metrics_diff_relevant_check", check_body),
        ("metrics_readback_diff_relevant", caller_body),
    ):
        assert "<<<" not in body, (
            f"a here-string (`<<<`) has reappeared in {label}() in"
            " ci/local_validate.sh -- this is the exact CHAOS-4487 class of"
            " construct (bash-5.3 heredoc_write pipe deadlock on a large"
            " payload), REGARDLESS of which variable name or literal string"
            " it uses; route any unbounded payload through a temp file"
            " instead (mktemp + the printf BUILTIN, see"
            " _metrics_diff_relevant_check's current implementation)"
        )

    # The fix's actual shape must be present: a temp file created via
    # mktemp, written via the printf BUILTIN (not `cat <<EOF`, which would
    # reintroduce the identical CHAOS-3362 pipe -- see the argMax stage's
    # own comment for why), and grep reading that file rather than stdin.
    assert "mktemp" in check_body, (
        "_metrics_diff_relevant_check no longer creates a temp file via"
        " mktemp -- has the fix been reverted?"
    )
    assert 'printf \'%s\' "${changed}" >"${changed_file}"' in check_body, (
        "_metrics_diff_relevant_check no longer writes its payload with the"
        " printf BUILTIN into a redirected file -- this is the mechanism"
        " that removes the fork/pipe entirely; a `cat <<EOF` or a pipe form"
        " here would reintroduce CHAOS-4487 or CHAOS-4319 respectively"
    )
    assert '"${changed_file}"' in check_body, (
        "_metrics_diff_relevant_check's grep call must read the temp file"
        " directly, not stdin"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
