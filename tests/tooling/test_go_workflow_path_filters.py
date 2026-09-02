"""The Go workflow's path filters must not let a guarded change go unchecked.

WHY THIS TEST EXISTS
--------------------
`go-quality` is a REQUIRED check on main. It USED to be produced by two
workflows: `.github/workflows/go.yml`, which ran only when the change touched
one of its `paths`, and `.github/workflows/go-quality-noop.yml`, which supplied
a passing context for everything else -- its step named, accurately, "Vacuously
satisfy the required go-quality check". Both declared a job with the SAME id, so
branch protection was satisfied by whichever ran, and a path missing from
`go.yml`'s list did not produce a missing check -- it produced a GREEN one.

CHAOS-4834 ended that: `go-quality.yml` is now the single producer, the no-op is
deleted, and relevance is decided in-job by `ci/go_relevance.py` reading
`go.yml`'s own list. The two assertions here that compared the two lists went
with the second list they compared.

**The fixture-coverage assertions below still apply, and are the reason this
file survives.** They test `go.yml`'s `paths` -- which is still the list
`go_relevance.py` reads, so a fixture missing from it still means a fixture-only
change is judged irrelevant and the gate still skips. The producer changed; the
list did not, and neither did the way it can fail.

That bit the live-Python rot guards. Those guards exist to fire when a frozen
fixture stops matching the interpreter, and the change that trips them is,
characteristically, a change to a fixture and nothing else. The lists named
individual fixture FILES, so a fixture-only PR was classified non-Go, the no-op
ran, `go-quality` went green, and the guard never executed -- on exactly the
change it was built for.

Measured on `main` at the time this was written: of the 48 files under
`tests/fixtures/`, **46 matched no path filter**. The 2 that did were
`issue_pr_links_python_golden.{json,py}` -- the pair an earlier lane added by
hand when it hit this same hole.

That is the tell this test is aimed at. A filter maintained by enumeration fails
silently, and the previous fix being *two more entries* is what guaranteed the
recurrence: it repaired the instance and left the mechanism, so coverage decayed
from 2/2 to 2/48 as fixtures accumulated.

The count moved during review, and how it moved is the point. The first version
of this test discovered fixtures by regex over `*_test.go` and reported 23 found
/ 21 unmatched. A codex round showed the regex missed every path built with
`filepath.Join` -- two real reads, and no way to know there were only two. The
oracle is now the directory listing, which cannot miss anything, and the honest
number is 46. A coverage test whose oracle is a pattern measures the pattern.

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


def _fixture_files_on_disk() -> set[str]:
    """Every file under `tests/fixtures/`, as repo-relative POSIX paths.

    THIS is the coverage oracle, and it is complete by construction: it asks the
    filesystem what exists rather than asking source code what it mentions. A
    fixture cannot hide from it.

    The first version of this file used a regex over `*_test.go` to find fixture
    references instead. That was wrong in the specific way this whole file is
    about. The regex only matches paths written as one contiguous literal, so it
    silently missed every read assembled with `filepath.Join`:

        filepath.Join(repoRoot(t), "tests", "fixtures", "cpython_random_golden.json")
        filepath.Join(filepath.Dir(filename), "..", "..", "tests", "fixtures", ...)

    It found 23 paths where a static inventory found 25. Both misses happened to
    sit inside the new glob, so nothing was actually uncovered -- but a coverage
    test whose oracle is a pattern only checks what the pattern's author thought
    to match. That is the enumeration failure this PR replaced in the workflow,
    reintroduced one layer up in the test meant to prevent it. Walking the
    directory removes the oracle entirely.
    """
    root = REPO_ROOT / "tests" / "fixtures"
    return {
        path.relative_to(REPO_ROOT).as_posix()
        for path in root.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }


def _fixture_paths_named_in_go_tests() -> set[str]:
    """Contiguous `tests/fixtures/...` literals appearing in Go test files.

    Deliberately INCOMPLETE, and used only to catch typos -- a literal naming a
    fixture that does not exist. It must never be used as a coverage oracle; see
    `_fixture_files_on_disk` for why.
    """
    reference = re.compile(r"tests/fixtures/[A-Za-z0-9_./-]+\.(?:json|py|tsv|txt)")
    found: set[str] = set()
    for source in REPO_ROOT.rglob("*_test.go"):
        if ".venv" in source.parts or "node_modules" in source.parts:
            continue
        found.update(reference.findall(source.read_text(errors="ignore")))
    return found


@pytest.mark.parametrize("event", ["push", "pull_request"])
def test_every_fixture_on_disk_triggers_the_go_workflow(event: str) -> None:
    """A change to any fixture must run the workflow that guards fixtures.

    This is the assertion that would have caught the original defect. It tests
    the WIRING, not the presence of any particular entry: a fixture added
    tomorrow is covered by the glob automatically, and a future narrowing of the
    filter fails here instead of silently going green.

    The oracle is the directory, not a scan of source. Anything under
    `tests/fixtures/` must trigger the workflow, whether a Go test reads it, a
    Python test reads it, or nothing reads it yet -- because deciding which is
    which requires the very source analysis that proved unreliable.
    """
    go_paths = (_on_block(_load(GO_WORKFLOW)).get(event) or {}).get("paths") or []
    fixtures = _fixture_files_on_disk()

    assert fixtures, (
        "tests/fixtures/ is empty or missing -- this test would pass vacuously, "
        "which is precisely the failure mode it exists to prevent"
    )

    unmatched = sorted(f for f in fixtures if not _matches_any(f, go_paths))
    assert not unmatched, (
        f"{len(unmatched)} of {len(fixtures)} fixture(s) do not match any "
        f"{event} path filter in go.yml, so a PR changing only one of them is "
        f"classified non-Go and go-quality is satisfied VACUOUSLY by the no-op "
        f"workflow -- the guard never runs:\n  " + "\n  ".join(unmatched)
    )


def test_every_fixture_named_in_a_go_test_exists() -> None:
    """A fixture literal in a Go test must name a file that is really there.

    Separate concern from coverage, and a separate oracle: this one catches a
    renamed or deleted fixture whose reference was left behind. It uses the
    deliberately-incomplete literal scan, which is fine here -- a typo it misses
    is a typo, not a silent gap in a guard.
    """
    on_disk = _fixture_files_on_disk()
    named = _fixture_paths_named_in_go_tests()

    missing = sorted(named - on_disk)
    assert not missing, (
        "Go test(s) reference fixture paths that do not exist on disk:\n  "
        + "\n  ".join(missing)
    )


def test_the_fixture_filter_is_a_glob_not_an_enumeration() -> None:
    """Pin the SHAPE, because the shape is what failed.

    An earlier revision listed fixtures one file at a time. It went wrong twice:
    once for `issue_pr_links` (fixed by adding two entries) and again for 23
    more. Re-enumerating would pass the coverage test above on the day it was
    written and fail for the next fixture added, which is precisely the failure
    mode this file exists to end.
    """
    # go.yml only. CHAOS-4834 deleted go-quality-noop.yml, so there is no second
    # list to keep in the same shape -- the drift this guarded against is retired
    # by construction rather than by assertion.
    for path in (GO_WORKFLOW,):
        block = _on_block(_load(path))
        for event in ("push", "pull_request"):
            key = "paths"
            patterns = (block.get(event) or {}).get(key) or []
            fixture_patterns = [p for p in patterns if p.startswith("tests/fixtures/")]
            assert fixture_patterns == ["tests/fixtures/**"], (
                f"{path.name} {event}.{key} should cover fixtures with the single "
                f"glob 'tests/fixtures/**', not by enumeration; found "
                f"{fixture_patterns}"
            )
