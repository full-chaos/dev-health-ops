"""The Go quality gate must not run the same test twice.

WHY THIS EXISTS
---------------
`ci/check_go.sh` ran `TestSumGoldenMatchesLivePython` **three times**: two
byte-identical solo blocks and once more inside the combined live-oracle block.
Each solo block re-ran the same test against the same fixture and re-checked the
same `python-sum-golden` marker, so the second and third runs could not fail
unless the first already had.

That is not merely wasted CI time, though it was that too. A duplicated block is
a maintenance hazard with a specific failure mode: the next person to change
one of them changes only one, and the gate then contains two versions of the
same check disagreeing about what it checks. Nothing reports the disagreement,
because both blocks pass.

The duplicate survived because nothing looked. Deleting it fixes the instance;
this fixes the class.

WHAT IS ASSERTED, AND WHY IT IS A PROPERTY RATHER THAN A LIST
------------------------------------------------------------
Every Go test named in a `-run` pattern must appear in exactly ONE such pattern.
There is no allow-list: at the time of writing all 42 distinct test names are
unique, so the property holds with zero exceptions, and an exception added later
has to argue for itself rather than being inherited.

A test that genuinely needs two invocations -- different environment, different
build tags -- is a real possibility, and the right response then is to say so in
this file with a reason, not to weaken the check into a count.
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECK_GO = REPO_ROOT / "ci/check_go.sh"

# `-run` patterns are multi-line in this script (a long alternation split across
# continuation lines), so the DOTALL flag is load-bearing rather than defensive.
RUN_PATTERN = re.compile(r"-run\s+'([^']*)'", re.S)
TEST_NAME = re.compile(r"Test[A-Za-z0-9_]+")


def _test_names_per_run_pattern() -> list[set[str]]:
    source = CHECK_GO.read_text(encoding="utf-8")
    return [set(TEST_NAME.findall(pattern)) for pattern in RUN_PATTERN.findall(source)]


def test_no_go_test_is_invoked_by_two_run_patterns() -> None:
    """A test named in two `-run` patterns is executed twice by one gate run."""
    counts: Counter[str] = Counter()
    for names in _test_names_per_run_pattern():
        counts.update(names)

    duplicated = {name: count for name, count in counts.items() if count > 1}

    assert not duplicated, (
        "these Go tests are invoked by more than one `-run` pattern in "
        "ci/check_go.sh, so the gate runs them repeatedly:\n  "
        + "\n  ".join(f"{count}x  {name}" for name, count in sorted(duplicated.items()))
        + "\n\nThe repeat runs cannot fail unless the first already did, and the "
        "real cost is that the next person to edit one block edits only one -- "
        "leaving two versions of the same check that disagree, with nothing to "
        "report the disagreement because both pass.\n"
        "If a test genuinely needs two invocations (different environment or "
        "build tags), say so with a reason in this file rather than deleting "
        "this assertion."
    )


def test_the_gate_still_names_a_plausible_number_of_tests() -> None:
    """Vacuity guard: an emptied or unparsed script must fail, not pass.

    Without this, a rename of the `-run` flag, a quoting change, or a truncated
    file would yield zero patterns, zero names, zero duplicates -- and the check
    above would go green while asserting nothing at all. That is the exact shape
    of failure this repository has hit repeatedly: a guard that passes by
    failing to look.

    The floor sits well below the current count so that ADDING gate coverage
    never fails the build, while losing it does.
    """
    patterns = _test_names_per_run_pattern()
    distinct = {name for names in patterns for name in names}

    assert len(patterns) >= 15, (
        f"only {len(patterns)} `-run` pattern(s) found in ci/check_go.sh; the "
        "gate invokes far more than that, so the parse has probably broken "
        "rather than the script having shrunk"
    )
    assert len(distinct) >= 30, (
        f"only {len(distinct)} distinct Go test name(s) found across those "
        "patterns; the uniqueness check above would pass vacuously on a set "
        "this small"
    )
