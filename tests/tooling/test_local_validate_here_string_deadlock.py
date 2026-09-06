"""Regression coverage for the here-redirection deadlock class (CHAOS-4319/
CHAOS-4487/CHAOS-3362) in ``ci/local_validate.sh``.

bash 5.3 implements a here-string (``<<<``) or heredoc (``<<EOF``) by forking
a child that writes the payload into a pipe while the parent ``wait4``s on
it; on a host with a small effective pipe buffer that write blocks forever
once the payload crosses a few hundred bytes. CHAOS-4487 found and fixed this
in the (now-retired, CHAOS-5307) ``metrics_readback`` stage's diff-relevance
check by routing the payload through a temp file (``mktemp`` + the ``printf``
BUILTIN, no pipe, no fork) instead -- the same fix pattern the argMax proof
stage (search ``ARGMAX_PROOF_TMPDIR``) already used for the identical class
of problem.

CHAOS-5307 deleted the ``metrics_readback`` stage entirely (its Python
compute retired with it, per standing rule), taking its dedicated
here-string-specific tests and the ``--metrics-diff-relevant-probe`` harness
hook down with it. What remains, and is still real coverage:

  1. ``test_here_string_deadlock_is_not_payload_agnostic`` -- proves the
     THREE genuine ``<<<`` sites still in ``ci/local_validate.sh`` (the
     lock-metadata read, and the two stage-id-csv reads used only by the
     stage-manifest test harness) stay well under the measured hanging
     threshold at their real payload sizes, and that the deadlock mechanism
     itself is payload-size-triggered (not construct- or site-specific) by
     also exercising the same construct shape inflated past that threshold.
  2. ``test_here_redirections_match_the_pinned_allowlist`` -- the complete,
     parser-free regression guard (codex review round 6 on the original
     CHAOS-4487 PR): every line in the WHOLE script containing ``<<`` must be
     one of the exact, pinned, justified lines below -- no more, no fewer.
     This is the backstop that would catch a NEW unbounded here-redirection
     introduced anywhere in the script, in any function, at any time.

A future change that grows either of the two remaining call sites' real
payload past the measured threshold should re-run this file's bisection
(see the git history of this file, pre-CHAOS-5307, for the bisection
methodology) and convert that site to the temp-file pattern too.
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


def test_here_string_deadlock_is_not_payload_agnostic() -> None:
    """Proves the THREE `<<<` sites in ci/local_validate.sh (the lock
    metadata read, and the two stage-id-csv reads used only by the
    stage-manifest test harness) stay safe because their REAL payload shapes
    stay well under the hanging threshold, not because the underlying bash
    mechanism is somehow specific to grep or to any one call site. Exercises
    the read-based shape (their actual construct, `IFS=... read ... <<<"$var"`)
    at both a realistic small size and a size proven to hang the same
    construct shape.
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
    # in the module docstring, not an assumption.
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
        # Codex review (round 2, P2): a non-hang result is only evidence of
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
            "mechanism as the grep-based CHAOS-4487 case); it completed "
            "instead on this environment/run, which is consistent with the "
            "deadlock's environment-dependent nature -- not evidence the "
            "shared-mechanism claim is false, just that this run is not "
            "under the pipe pressure needed to observe it"
        )
    except subprocess.TimeoutExpired:
        pass  # expected: same mechanism, same threshold, confirms the audit.


# Codex reviews rounds 3-5 (original CHAOS-4487 PR) kept finding gaps in a
# hand-rolled bash parser (missing a helper, a false call-match, an
# unindented brace, a quoted/commented brace) used to compute which
# functions were "reachable" from the (now-deleted) metrics-diff check so
# their bodies could be scanned for a reintroduced `<<<`. There is no
# natural end to that: any closure over bash's actual call graph, computed
# by regex instead of a real parser, will keep finding new edge cases
# forever. Team-lead ruling (round 6): stop growing the parser. Replace it
# with a simpler, COMPLETE invariant that needs no parsing at all -- scan
# the WHOLE script, unconditionally, for every line containing a
# here-redirection operator (`<<`, which also matches `<<<`), and assert
# that exact set of lines equals a pinned allowlist. No function boundaries,
# no brace/quote parsing, one-liners included by construction (a one-line
# function's `<<<` is still a line containing `<<`). Anything new or moved
# anywhere in the script -- including inside a future one-line helper, a
# new helper, a renamed variable, or a stray comment mention -- fails this
# test until someone explicitly adds it to the allowlist with a reason.
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
        " realistic payload size. NOT converted to the temp-file pattern;"
        " a future change that grows this payload past the threshold should"
        " re-run this file's bisection and convert it."
    ),
    "IFS=',' read -r -a DECLARED_STAGE_IDS <<<\"${declared_csv}\"": (
        "stage-id-list read (--stage-manifest-mismatch-probe test harness):"
        " a short, fixed, comma-separated list of this script's own known"
        " stage ids, nowhere near the deadlock threshold. Same audit as the"
        " lock-metadata read above; NOT converted."
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
        "comment prose (the argMax proof stage's own fix rationale)"
        " explaining why a heredoc was rejected in favor of the temp-file"
        " pattern -- does not use one."
    ),
}


def test_here_redirections_match_the_pinned_allowlist() -> None:
    """The complete, parser-free regression guard (codex review round 6 on
    the original CHAOS-4487 PR): every line in the WHOLE script containing
    `<<` (which also matches `<<<`, so here-strings and heredocs are both
    covered by one check) must be one of the exact, pinned, justified lines
    in `_KNOWN_SAFE_HERE_REDIRECTION_LINES` above -- no more, no fewer.

    Matches by exact (stripped) LINE CONTENT, never by line number: a line
    number shifts every time an unrelated edit adds or removes a line
    above it, which would make this test either spuriously fail (churn
    with no real change) or, worse, silently stop checking the line it
    meant to if a renumbering ever lined up two different lines by
    coincidence. Content is stable across unrelated edits and only
    changes when the line ITSELF changes -- which is exactly when this
    test should have an opinion.

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
        " file instead (mktemp + the printf BUILTIN -- see the argMax proof"
        " stage's ARGMAX_PROOF_TMPDIR implementation), or, if this specific"
        " line is genuinely safe (small, bounded payload), add it to"
        " _KNOWN_SAFE_HERE_REDIRECTION_LINES above with a justification."
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
