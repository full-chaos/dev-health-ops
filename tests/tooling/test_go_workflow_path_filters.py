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

    `**` spans separators; `*` does not; everything else is literal.

    UNSUPPORTED METACHARACTERS ARE A HARD ERROR, not an escape. An earlier
    revision escaped `?`, `[`, `]` and `!` into literal text, so a negation like
    `!tests/fixtures/**` would have been translated into a pattern matching a
    path that literally begins with "!" -- i.e. matching nothing, silently. This
    test would then report "0 unmatched fixtures" and pass, which is precisely
    the failure mode the whole file exists to prevent.

    Raised by lane-ci-flakes, who took the same change in `ci/go_relevance.py`
    where it is worse: there the translator DECIDES RELEVANCE on a live PR, so a
    silently-empty pattern marks real changes irrelevant and skips the gate.
    Same bug, one line of severity apart -- here it weakens an assertion, there
    it skips a required check.

    On `**` matching ZERO characters: the evidence is deliberately external to
    this function, because lane-ci-flakes' implementation descends from this one
    and two agreeing reimplementations are one implementation counted twice.
    GitHub documents `docs/**` matching `docs/README.md`; this repo's `docs/**`
    filter ran for PR #1472, which changed `docs/index.md` directly; and
    `docs-guards.yml` run 30947483495 did the same. Observed behaviour of the
    real matcher, not a second opinion from a sibling translator.
    """
    unsupported = sorted({character for character in "?[]!" if character in pattern})
    if unsupported:
        raise AssertionError(
            f"path filter {pattern!r} uses {unsupported}, which this translator "
            "does not implement. Extend _github_glob_to_regex rather than letting "
            "the pattern be escaped into literal text -- a filter that matches "
            "nothing silently reports full coverage."
        )

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


# Directories whose fixture-shaped files are deliberately NOT required to match
# the Go path filters, each with the reason and the condition that would remove
# the exclusion.
#
# This map is the successor to a worse design. The first version of this test
# walked `tests/fixtures/` and called itself "complete by construction" -- for
# that directory. The DIRECTORY was the enumeration, and the docstring above it
# said, in as many words, that a coverage oracle keyed on a pattern only checks
# what its author thought to match. lane-3092 found the gap: their corpus under
# `internal/pythonparity/testdata/` matched nothing, because `go.yml` has no
# `internal/**` pattern -- only `**/*.go` (which does not cover a .json) and two
# hand-listed testdata directories. Measured residue at that point: 63 unmatched
# files across a dozen directories, belonging to five lanes that did not know.
#
# So the oracle now walks the whole tree, and a directory leaves coverage only by
# being named here WITH a reason.
UNCOVERED_FIXTURE_DIRECTORIES: dict[str, str] = {
    "src/dev_health_ops/fixtures": (
        "the Python source tree, not Go testdata -- no Go test reads these, so "
        "requiring them to trigger the Go workflow would make every Python-only "
        "fixture change run the Go suite for nothing"
    ),
    "tests/testops/fixtures": (
        "read by the Python testops suite, not by Go tests; covered by the "
        "Python workflow's own filters"
    ),
    "tests/api/dev/fixtures": (
        "read by the Python API test suite (tests/api/), not by Go tests; "
        "covered by the Python workflow's own filters"
    ),
    "tests/docs/fixtures": (
        "documentation-lint fixtures consumed by the docs workflow; markdown "
        "and mkdocs config, with no Go reader"
    ),
    "tests/acceptance/corpus": (
        "acceptance-suite corpus consumed by the acceptance workflow, which has "
        "its own path filters and does not run under go-quality"
    ),
}


def _fixture_like_files_on_disk() -> set[str]:
    """Every non-`.go` file under any `testdata/` or `fixtures/` directory.

    THIS is the coverage oracle, and the scope is the whole tree rather than one
    chosen directory.

    The previous version walked `tests/fixtures/` only. It was complete for that
    directory and blind to everywhere else fixtures live, which is at least a
    dozen directories across five lanes. Replacing a regex with a directory walk
    removed the pattern and kept the assumption -- the same enumeration failure,
    one level up, inside the fix for that failure.

    `.go` files are excluded because `**/*.go` already covers them; what is at
    risk is data -- `.json`, `.py`, `.tsv`, `.txt` -- which no Go-source pattern
    matches.
    """
    root = REPO_ROOT
    skip = {
        ".git",
        ".venv",
        "node_modules",
        "__pycache__",
        ".mypy_cache",
        ".ruff_cache",
    }
    found: set[str] = set()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        parts = path.relative_to(root).parts
        if skip.intersection(parts):
            continue
        if path.suffix == ".go":
            continue
        if not any(segment in ("testdata", "fixtures") for segment in parts):
            continue
        found.add(path.relative_to(root).as_posix())
    return found


def _is_excused(path: str) -> str | None:
    """Return the reason this path is excused from coverage, or None."""
    for directory, reason in UNCOVERED_FIXTURE_DIRECTORIES.items():
        if path == directory or path.startswith(directory + "/"):
            return reason
    return None


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
def test_every_fixture_on_disk_triggers_the_go_workflow(event: str) -> None:
    """A change to any Go-relevant fixture must run the workflow that guards it.

    The oracle is the whole tree, not one directory. Anything under a `testdata/`
    or `fixtures/` directory must either match the filter or be named in
    UNCOVERED_FIXTURE_DIRECTORIES with a reason.
    """
    go_paths = (_on_block(_load(GO_WORKFLOW)).get(event) or {}).get("paths") or []
    fixtures = _fixture_like_files_on_disk()

    assert fixtures, (
        "no fixture-like files found anywhere in the tree -- the walk has broken, "
        "and this test would pass vacuously, which is the failure mode it exists "
        "to prevent"
    )

    unmatched = sorted(
        path
        for path in fixtures
        if not _matches_any(path, go_paths) and _is_excused(path) is None
    )
    assert not unmatched, (
        f"{len(unmatched)} of {len(fixtures)} fixture-like file(s) match no "
        f"{event} path filter in go.yml and are not excused, so a PR changing "
        f"only one of them is classified non-Go and go-quality is satisfied "
        f"VACUOUSLY by the no-op workflow -- the guard never runs:\n  "
        + "\n  ".join(unmatched)
        + "\n\nEither add a pattern covering them to ALL FOUR lists, or add the "
        "directory to UNCOVERED_FIXTURE_DIRECTORIES with a reason. Do not add "
        "individual files: that is the enumeration this test exists to end."
    )


@pytest.mark.parametrize("event", ["push", "pull_request"])
def test_no_excused_directory_is_actually_covered(event: str) -> None:
    """An exclusion that stops being necessary must fail, not linger.

    Same discipline as a documented-divergence entry: it asserts the exclusion
    rather than describing it. If a directory here becomes matched by the
    filters, the entry is stale and should be deleted -- otherwise the map grows
    into a list of things nobody rechecks, which is the enumeration failure
    wearing a different hat.
    """
    go_paths = (_on_block(_load(GO_WORKFLOW)).get(event) or {}).get("paths") or []
    fixtures = _fixture_like_files_on_disk()

    for directory, reason in UNCOVERED_FIXTURE_DIRECTORIES.items():
        covered = sorted(
            path
            for path in fixtures
            if (path == directory or path.startswith(directory + "/"))
            and _matches_any(path, go_paths)
        )
        assert not covered, (
            f"{directory!r} is excused because {reason!r}, but {len(covered)} of "
            f"its files now MATCH the {event} filters. The exclusion is stale: "
            f"delete the entry rather than leaving it to describe something that "
            f"is no longer true.\n  " + "\n  ".join(covered[:5])
        )


def test_every_excused_directory_actually_exists() -> None:
    """An exclusion naming a directory that is gone is dead weight.

    A stale path here silently excuses nothing while looking like it excuses
    something, so the map's length stops meaning what a reader assumes.
    """
    for directory in UNCOVERED_FIXTURE_DIRECTORIES:
        assert (REPO_ROOT / directory).is_dir(), (
            f"{directory!r} is excused from fixture coverage but does not exist. "
            "Delete the entry; an exclusion for a missing directory reads as "
            "coverage policy and is noise."
        )


def test_every_fixture_named_in_a_go_test_exists() -> None:
    """A fixture literal in a Go test must name a file that is really there.

    Separate concern from coverage, and a separate oracle: this one catches a
    renamed or deleted fixture whose reference was left behind. It uses the
    deliberately-incomplete literal scan, which is fine here -- a typo it misses
    is a typo, not a silent gap in a guard.
    """
    on_disk = _fixture_like_files_on_disk()
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
