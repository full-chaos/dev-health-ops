"""The Go workflow's path filters must not let a guarded change go unchecked.

WHY THIS TEST EXISTS
--------------------
`go-quality` is a REQUIRED check on main. `.github/workflows/go.yml` only runs
when the change touches one of its `paths`; for everything else,
`.github/workflows/go-quality-noop.yml` supplies a passing context so the
required check is satisfied. Its step is named, accurately, "Vacuously satisfy
the required go-quality check".

Both workflows declare a job with the SAME id, `go-quality`. So branch
protection is satisfied by whichever one ran, and a path that is missing from
`go.yml`'s list does not produce a missing check -- it produces a GREEN one.

That bit the live-Python rot guards. Those guards exist to fire when a frozen
fixture stops matching the interpreter, and the change that trips them is,
characteristically, a change to a fixture and nothing else. The lists named
individual fixture FILES, so a fixture-only PR was classified non-Go, the no-op
ran, `go-quality` went green, and the guard never executed -- on exactly the
change it was built for. 23 fixtures were unlisted at the time this was found,
including several already merged.

Only `issue_pr_links_python_golden.*` had been listed, added when an earlier
lane hit the same hole and fixed it by adding two more entries. That is the tell
this test is aimed at: a filter maintained by enumeration fails silently, and
the previous fix being *more entries* is what guarantees the next recurrence.

WHAT THIS TEST ASSERTS
----------------------
1. The two files' lists are EXACT MIRRORS. They were not: `go.yml` had three
   entries (`Makefile`, `.dockerignore`, `.github/workflows/go.yml`) that the
   no-op's `paths-ignore` lacked, so both workflows ran for those changes and
   produced two `go-quality` contexts. Harmless in that direction; the reverse
   would not be, and nothing was checking either way.
2. Every fixture a Go test reads is matched by the filter. This is the one that
   would have caught the original defect, and it tests the WIRING rather than
   the presence of a particular string -- a future fixture is covered
   automatically, and a future NARROWING of the glob fails here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
GO_WORKFLOW = REPO_ROOT / ".github/workflows/go.yml"
NOOP_WORKFLOW = REPO_ROOT / ".github/workflows/go-quality-noop.yml"

# The job id both workflows declare. If these ever stop matching, the no-op no
# longer satisfies the required check and this whole class evaporates -- which
# would be a behaviour change worth noticing, hence the assertion below.
SHARED_JOB_ID = "go-quality"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _on_block(document: dict) -> dict:
    """Return the workflow's trigger block.

    PyYAML parses the bare key `on` as the BOOLEAN True (YAML 1.1's
    yes/no/on/off), so `document["on"]` is a KeyError on these files. Reading
    both spellings is not defensiveness, it is the actual behaviour.
    """
    return document.get(True) or document.get("on") or {}


def _github_glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a GitHub path filter to a regex.

    `**` spans separators; `*` does not; everything else is literal. This is a
    deliberately small translation covering the forms these two files actually
    use -- if a filter ever needs `?`, `[...]` or `!` negation, this must grow
    rather than silently mis-match.
    """
    out: list[str] = []
    index = 0
    while index < len(pattern):
        if pattern.startswith("**", index):
            out.append(".*")
            index += 2
        elif pattern[index] == "*":
            out.append("[^/]*")
            index += 1
        else:
            out.append(re.escape(pattern[index]))
            index += 1
    return re.compile("^" + "".join(out) + "$")


def _matches_any(path: str, patterns: list[str]) -> bool:
    return any(_github_glob_to_regex(p).match(path) for p in patterns)


def _fixtures_read_by_go_tests() -> set[str]:
    """Every `tests/fixtures/...` path referenced from a Go test file.

    Derived from the tree rather than listed here, so a new guard is covered the
    day it lands. A hand-kept list would reproduce the very failure this file
    exists to prevent.
    """
    reference = re.compile(r"tests/fixtures/[A-Za-z0-9_./-]+\.(?:json|py|tsv|txt)")
    found: set[str] = set()
    for source in REPO_ROOT.rglob("*_test.go"):
        if ".venv" in source.parts or "node_modules" in source.parts:
            continue
        found.update(reference.findall(source.read_text(errors="ignore")))
    return found


@pytest.mark.parametrize("event", ["push", "pull_request"])
def test_go_workflow_and_noop_path_lists_are_exact_mirrors(event: str) -> None:
    """The no-op must ignore exactly what the real workflow includes.

    A path in `go.yml` but not in the no-op's ignore list makes BOTH run: two
    `go-quality` contexts, wasteful but safe. A path in the ignore list but not
    in `go.yml` makes NEITHER run. Nothing was checking either direction, and
    the first had already drifted by three entries.
    """
    go_paths = (_on_block(_load(GO_WORKFLOW)).get(event) or {}).get("paths") or []
    noop_ignored = (_on_block(_load(NOOP_WORKFLOW)).get(event) or {}).get(
        "paths-ignore"
    ) or []

    assert go_paths, f"go.yml has no {event} paths filter"
    only_go = sorted(set(go_paths) - set(noop_ignored))
    only_noop = sorted(set(noop_ignored) - set(go_paths))

    assert not only_go and not only_noop, (
        f"the {event} path lists have diverged.\n"
        f"  in go.yml but not ignored by the no-op: {only_go}\n"
        f"    -> BOTH workflows run; two go-quality contexts for one change\n"
        f"  ignored by the no-op but not in go.yml: {only_noop}\n"
        f"    -> NEITHER runs; the required check has no producer\n"
        "Keep the two lists identical; they are mirrors by construction."
    )


def test_both_workflows_declare_the_shared_job_id() -> None:
    """The no-op only satisfies the required check while the ids match.

    If this fails, the vacuous-green mechanism is gone -- which may be an
    improvement, but it changes what every other assertion here is protecting
    against, so it should be noticed rather than silently inherited.
    """
    for path in (GO_WORKFLOW, NOOP_WORKFLOW):
        jobs = _load(path).get("jobs") or {}
        assert SHARED_JOB_ID in jobs, (
            f"{path.name} no longer declares a job called {SHARED_JOB_ID!r}; "
            "the no-op's ability to satisfy the required check depends on it"
        )


@pytest.mark.parametrize("event", ["push", "pull_request"])
def test_every_fixture_a_go_test_reads_triggers_the_go_workflow(event: str) -> None:
    """A change to a guarded fixture must run the guard.

    This is the assertion that would have caught the original defect. It tests
    the WIRING, not the presence of any particular entry: a fixture added
    tomorrow is covered by the glob automatically, and a future narrowing of the
    filter fails here instead of silently going green.
    """
    go_paths = (_on_block(_load(GO_WORKFLOW)).get(event) or {}).get("paths") or []
    fixtures = _fixtures_read_by_go_tests()

    assert fixtures, (
        "no tests/fixtures path was found in any *_test.go -- the discovery "
        "regex has stopped matching, so this test would pass vacuously"
    )

    unmatched = sorted(f for f in fixtures if not _matches_any(f, go_paths))
    assert not unmatched, (
        f"{len(unmatched)} fixture(s) read by a Go test do not match any {event} "
        f"path filter in go.yml, so a PR changing only one of them is classified "
        f"non-Go and go-quality is satisfied VACUOUSLY by the no-op workflow -- "
        f"the guard never runs:\n  " + "\n  ".join(unmatched)
    )


def test_the_fixture_filter_is_a_glob_not_an_enumeration() -> None:
    """Pin the SHAPE, because the shape is what failed.

    An earlier revision listed fixtures one file at a time. It went wrong twice:
    once for `issue_pr_links` (fixed by adding two entries) and again for 23
    more. Re-enumerating would pass the coverage test above on the day it was
    written and fail for the next fixture added, which is precisely the failure
    mode this file exists to end.
    """
    for path in (GO_WORKFLOW, NOOP_WORKFLOW):
        block = _on_block(_load(path))
        for event in ("push", "pull_request"):
            key = "paths" if path is GO_WORKFLOW else "paths-ignore"
            patterns = (block.get(event) or {}).get(key) or []
            fixture_patterns = [p for p in patterns if p.startswith("tests/fixtures/")]
            assert fixture_patterns == ["tests/fixtures/**"], (
                f"{path.name} {event}.{key} should cover fixtures with the single "
                f"glob 'tests/fixtures/**', not by enumeration; found "
                f"{fixture_patterns}"
            )
