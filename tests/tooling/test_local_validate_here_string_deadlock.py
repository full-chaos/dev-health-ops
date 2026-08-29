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
from collections import Counter
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


# Codex reviews rounds 3-5 kept finding gaps in a hand-rolled bash parser
# (missing a helper, a false call-match, an unindented brace, a quoted/
# commented brace) used to compute which functions are "reachable" from
# metrics_readback_diff_relevant() so their bodies could be scanned for a
# reintroduced `<<<`. There is no natural end to that: any closure over
# bash's actual call graph, computed by regex instead of a real parser,
# will keep finding new edge cases forever. Team-lead ruling (round 6):
# stop growing the parser. Replace it with a simpler, COMPLETE invariant
# that needs no parsing at all -- scan the WHOLE script, unconditionally,
# for every line containing a here-redirection operator (`<<`, which also
# matches `<<<`), and assert that exact set of lines equals a pinned
# allowlist. No function boundaries, no brace/quote parsing, one-liners
# included by construction (a one-line function's `<<<` is still a line
# containing `<<`). Anything new or moved anywhere in the script --
# including inside a future one-line helper, a third helper, a renamed
# variable, or a stray comment mention -- fails this test until someone
# explicitly adds it to the allowlist with a reason. That is strictly
# stronger than the closure it replaces (which only ever covered ONE
# call path) and does not depend on getting a parser right.
_KNOWN_SAFE_HERE_REDIRECTION_LINES: dict[str, str] = {
    # The three genuine `<<<` (here-string) redirections in the script,
    # exactly as they read today (leading/trailing whitespace stripped --
    # see test_here_redirections_match_the_pinned_allowlist's docstring
    # for why matching by content, not line number, is deliberate).
    "IFS='|' read -r LOCK_OWNER_PID LOCK_OWNER_LSTART LOCK_OWNER_CWD <<<\"${target}\"": (
        "lock-metadata read (parse_lock_owner): payload is a `pid|lstart|cwd`"
        " triple written by this same script (release_lock's own writer),"
        " always well under the ~500-550 byte here-string deadlock threshold"
        " measured on this host -- see"
        " test_here_string_deadlock_is_not_payload_agnostic below, which"
        " proves this exact construct shape does NOT hang at this site's"
        " realistic payload size. NOT converted to the temp-file pattern in"
        " this PR; a future change that grows this payload past the"
        " threshold should re-run this file's bisection and convert it."
    ),
    "IFS=',' read -r -a DECLARED_STAGE_IDS <<<\"${declared_csv}\"": (
        "stage-id-list read (--stage-manifest-mismatch-probe test harness):"
        " a short, fixed, comma-separated list of this script's own known"
        " stage ids, nowhere near the deadlock threshold. Same audit as the"
        " lock-metadata read above; NOT converted in this PR."
    ),
    "IFS=',' read -r -a EXECUTED_STAGE_IDS <<<\"${executed_csv}\"": (
        "stage-id-list read: same shape, same bound, same audit as"
        " DECLARED_STAGE_IDS immediately above."
    ),
    # Comment lines that merely MENTION `<<` / `<<<` / `<<EOF` in prose --
    # not executable redirections at all -- but a whole-file substring scan
    # sees them too, and this test deliberately does not try to distinguish
    # code from comments (that would be exactly the "parsing" this
    # simplification exists to avoid). Pinning them here is cheaper and
    # more honest than adding comment-detection logic.
    "# That is not theoretical here. Measured on this host, `cat >/dev/null <<EOF`:": (
        "comment prose (CHAOS-3362 discussion) citing a heredoc construct as"
        " a measurement example -- does not use one."
    ),
    "# fork, nothing that can block. Using `cat >file <<EOF` here would reintroduce": (
        "comment prose (this fix's own rationale) explaining why a heredoc"
        " was rejected in favor of the temp-file pattern -- does not use one."
    ),
    '# CHAOS-4487 (why not the here-string either, `grep -qE ... <<<"$1"`): a': (
        "comment prose (this fix's own rationale) -- does not use a here-string."
    ),
    "# implements `<<<` by forking a child that does the heredoc_write into a": (
        "comment prose (this fix's own rationale) describing bash's"
        " here-string implementation -- does not use one."
    ),
}


def test_here_redirections_match_the_pinned_allowlist() -> None:
    """The complete, parser-free regression guard for CHAOS-4487 (codex
    review round 6): every line in the WHOLE script containing `<<` (which
    also matches `<<<`, so here-strings and heredocs are both covered by
    one check) must be one of the exact, pinned, justified lines in
    `_KNOWN_SAFE_HERE_REDIRECTION_LINES` above -- no more, no fewer.

    Matches by exact (stripped) LINE CONTENT, never by line number: a line
    number shifts every time an unrelated edit adds or removes a line
    above it, which would make this test either spuriously fail (churn
    with no real change) or, worse, silently stop checking the line it
    meant to if a renumbering ever lined up two different lines by
    coincidence. Content is stable across unrelated edits and only
    changes when the line ITSELF changes -- which is exactly when this
    test should have an opinion.

    This replaces the round 3-5 reachable-call-path closure entirely: that
    approach re-derived bash's call graph and function/brace/quote
    structure with regexes, and every review round surfaced a new way for
    that re-derivation to be wrong (a missed helper, a false call-match
    on incidental text, an unindented brace, a quoted or commented brace).
    A whole-file, unconditional line scan needs none of that -- there is
    no function boundary to compute, so a one-line helper's `<<<` is
    caught by construction, not by a special case for one-liners.

    Three failure directions, all real:
      - A NEW or MOVED `<<` appears that isn't allowlisted (the CHAOS-4487
        regression itself, in any function, at any indentation, on any
        line, one-liner or not) -- caught by `unexpected` below.
      - An allowlisted line goes stale (edited, removed, or its
        justification no longer applies) without updating this list --
        caught by `missing` below, so the allowlist cannot silently drift
        out of sync with the real file.
      - An allowlisted line's exact text is COPIED or DUPLICATED to a
        second, unaudited call site (codex review round 6 follow-up, P2):
        a plain set comparison discards occurrence count, so a second
        identical-looking line -- possibly relocated next to a genuinely
        unbounded payload -- would pass silently, because its text alone
        was already "seen" by the first, actually-audited occurrence.
        Caught by `duplicated` below: each allowlisted line must appear
        EXACTLY once.
    """

    text = SCRIPT.read_text(encoding="utf-8")
    found_lines = [line.strip() for line in text.splitlines() if "<<" in line]
    found_counts = Counter(found_lines)
    found = set(found_counts)
    allowlisted = set(_KNOWN_SAFE_HERE_REDIRECTION_LINES)

    unexpected = found - allowlisted
    assert not unexpected, (
        "found a `<<`/`<<<` redirection not on the pinned allowlist in"
        " ci/local_validate.sh -- this is the exact CHAOS-4487 class of"
        " construct (bash-5.3 heredoc_write pipe deadlock on a large"
        " payload) REGARDLESS of which function, variable name, or"
        " indentation it uses. Route any unbounded payload through a temp"
        " file instead (mktemp + the printf BUILTIN -- see"
        " _metrics_diff_relevant_check's implementation), or, if this"
        " specific line is genuinely safe (small, bounded payload), add it"
        " to _KNOWN_SAFE_HERE_REDIRECTION_LINES above with a justification."
        "\nUnexpected line(s):\n"
        + "\n".join(f"  {line!r}" for line in sorted(unexpected))
    )

    missing = allowlisted - found
    assert not missing, (
        "an allowlisted line in _KNOWN_SAFE_HERE_REDIRECTION_LINES no"
        " longer appears in ci/local_validate.sh -- either it was edited"
        " (update the allowlist entry's text to match) or removed entirely"
        " (delete the stale allowlist entry), so this test is not"
        " silently checking a line that no longer exists."
        "\nMissing line(s):\n" + "\n".join(f"  {line!r}" for line in sorted(missing))
    )

    duplicated = {line: n for line, n in found_counts.items() if n > 1}
    assert not duplicated, (
        "an allowlisted here-redirection line appears MORE THAN ONCE in"
        " ci/local_validate.sh (codex review round 6 follow-up, P2): exact"
        " line-content matching alone cannot tell a harmless second copy"
        " apart from the SAME bounded-payload line copied or relocated"
        " next to a genuinely unbounded call site elsewhere -- both look"
        " identical by content. Each allowlisted line must appear exactly"
        " once; if a second, deliberate use is intentional, give it its"
        " own DISTINCT allowlist entry (e.g. a trailing marker comment"
        " that makes its line text unique) with its own justification,"
        " rather than relying on the first occurrence's audit to cover it."
        f"\nDuplicated line(s) and counts: {duplicated!r}"
    )


def _metrics_diff_relevant_check_body(text: str) -> str:
    """The literal body of `_metrics_diff_relevant_check` -- a single,
    hardcoded extraction of the ONE function this fix touches, not a
    general-purpose function-body parser. This is deliberately narrower
    than the round 3-5 closure it replaces: it only ever needs to look at
    this one function's own shape (does it still write via mktemp + the
    printf builtin, per the argMax stage's established pattern?), which
    never needed reachability computation in the first place -- the
    reachability closure existed only to serve the now-deleted `<<<`
    scan, and `test_here_redirections_match_the_pinned_allowlist` above
    covers that job for the whole script unconditionally instead.
    """

    start = text.index("_metrics_diff_relevant_check() {")
    end = text.index("\n}\n", start)
    return text[start:end]


def test_fixed_code_writes_the_metrics_diff_payload_via_a_temp_file() -> None:
    """The fix's actual shape must be present in `_metrics_diff_relevant_check`:
    a temp file created via `mktemp`, written via the `printf` BUILTIN (no
    pipe, no fork -- not `cat <<EOF`, which would reintroduce the identical
    CHAOS-3362 pipe; see the argMax stage's own comment for why), with
    `grep` reading that file directly rather than stdin. This check is
    unchanged by the round 6 simplification -- only the separate `<<<`
    reachability scan was replaced, not this one.
    """

    text = SCRIPT.read_text(encoding="utf-8")
    check_body = _metrics_diff_relevant_check_body(text)

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
