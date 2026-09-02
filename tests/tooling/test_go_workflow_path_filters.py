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


# ONE implementation of `**/` semantics, not two kept in sync.
#
# This file used to carry its own translator. It diverged from `ci/go_relevance.py`'s
# within a day: mine special-cased only a LEADING `**/`, so `docs/**/x.md` did not
# match `docs/x.md`, while theirs treats `**/` as a token at any position. Two
# copies of a rule is two answers to the same question, and the guard and the
# relevance decider disagreeing about which paths are covered is exactly the drift
# both exist to prevent.
#
# IMPORT COUPLING, stated because it is a real cost: this makes a test depend on a
# CI script at import time. `tests/tooling/conftest.py` puts the repo root on
# sys.path and `ci/` resolves as an implicit namespace package. If go_relevance.py
# ever grows a heavy import or a module-scope side effect, this file inherits it.
# That is worth accepting -- a shared rule with one owner beats two that agree today.
from ci.go_relevance import github_glob_to_regex  # noqa: E402


def _matches_any(path: str, patterns: list[str]) -> bool:
    return any(github_glob_to_regex(p).match(path) for p in patterns)


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
    "internal/providersync/JIRA_PROVIDER_GAP_MATRIX.md": (
        'surfaced by the os.ReadDir(".") reader shape, which enumerates a whole '
        "package directory. The three tests doing that all filter to `jira_*.go` "
        "or `*.go`, which `**/*.go` already covers -- so a markdown file in that "
        "directory cannot change what any of them assert. Excused rather than "
        "given a path filter, because adding a pattern for it would claim a "
        "dependency that does not exist"
    ),
    "tests/acceptance/corpus": (
        "acceptance-suite corpus consumed by the acceptance workflow, which has "
        "its own path filters and does not run under go-quality"
    ),
}


# Directories no walk in this file descends: VCS internals, virtualenvs, caches.
# Shared so the two oracles below cannot drift apart on what they skip.
_SKIP_DIRECTORIES = frozenset(
    {".git", ".venv", "node_modules", "__pycache__", ".mypy_cache", ".ruff_cache"}
)


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
    found: set[str] = set()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        parts = path.relative_to(root).parts
        if _SKIP_DIRECTORIES.intersection(parts):
            continue
        if path.suffix == ".go":
            continue
        if not any(segment in ("testdata", "fixtures") for segment in parts):
            continue
        found.add(path.relative_to(root).as_posix())
    return found


_GO_DATA_SUFFIXES = (
    ".json",
    # `.py` is here because Go tests read PYTHON as an input: a parity test
    # scrapes the real sink module, a migration file, a model. Excluding it made
    # the scanner blind to exactly the files whose drift the Go test exists to
    # catch -- a PR touching only one of them was classified irrelevant, the Go
    # contract test never ran, and this oracle passed anyway.
    ".py",
    ".sql",
    ".csv",
    ".tsv",
    ".txt",
    ".yaml",
    ".yml",
    ".golden",
)


def _files_named_by_go_tests() -> set[str]:
    """Every repo file Go code NAMES, resolved from the naming file's directory.

    THIS is the coverage oracle for Go inputs, keyed on the property that decides
    the question: a file is a Go input because GO CODE NAMES IT, not because of
    what its parent directory is called.

    Three earlier versions keyed on directories -- a regex over one, a walk of
    `tests/fixtures/`, then a walk over the two names `testdata` and `fixtures`.
    Each replaced an enumeration with a slightly larger enumeration. Four
    contract files read by Go tests were invisible to all three:

        contracts/metrics/v1/remaining-scopes.json          scopes_test.go:56
        contracts/cache-invalidation/v1/org_cache_epoch_key.json
        contracts/provider-matrix/v1/matrix.json            capability_matrix_test.go
        internal/jobs/metrics/daily/families.json           families_test.go:11

    # THE READER SHAPES THIS COVERS

    All four are mechanically enumerable from source, and each has a fixture in
    `test_every_covered_reader_shape_is_found`:

      1. a data-suffixed string literal in any `.go` file -- test OR production,
         because a test's input is just as real when the production file it calls
         is what names the path (`families.go` embeds `families.json`);
      2. `//go:embed` directives, including multi-pattern lines and globs;
      3. `filepath.Join("a", "b.json")` -- covered by (1), since the segments are
         literals and the join is resolved against the file's directory;
      4. `os.ReadDir("literal")` -- the directory's own non-`.go` files.

    # WHAT IT CANNOT SEE, STATED BOTH WAYS

    It cannot see a path BUILT AT RUNTIME: assembled from variables, a function
    return, an env var, or a `fmt.Sprintf`. Those readers are THE RESIDUAL RISK
    of this guard -- a corpus reached only that way can still rot silently, and
    no amount of widening the literal scan finds it.

    That limit is red-tested rather than narrated:
    `test_the_runtime_built_path_limit_is_real` asserts a variable-built path is
    NOT found, so the day someone teaches this to resolve them, the test fails
    and the docstring gets corrected instead of quietly going stale.
    """
    found: set[str] = set()
    for source_file in REPO_ROOT.rglob("*.go"):
        parts = source_file.relative_to(REPO_ROOT).parts
        if _SKIP_DIRECTORIES.intersection(parts):
            continue
        try:
            source = source_file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        found |= _resolve_named_files(source, source_file.parent, REPO_ROOT)
    return found


def _resolve_named_files(source: str, base: Path, root: Path | None = None) -> set[str]:
    """Resolve every file the Go source names, relative to its own directory.

    `root` is injectable so the reader-shape fixtures can exercise this against a
    tmp_path tree. A resolver that only works against the real repository cannot
    be fixture-tested, and an untestable resolver is how the last three versions
    of this oracle each shipped a coverage claim nobody had checked.
    """
    anchor = root if root is not None else REPO_ROOT
    found: set[str] = set()

    def add(candidate: Path) -> None:
        try:
            relative = candidate.resolve().relative_to(anchor)
        except ValueError:
            return  # escapes the repo; not ours to cover
        if candidate.is_file() and candidate.suffix != ".go":
            found.add(relative.as_posix())

    # (1) and (3): data-suffixed string literals, including filepath.Join parts.
    #
    # Resolved against BOTH the naming file's directory and the repository root,
    # because Go names inputs in both spellings and only one of them was tried.
    # A sibling fixture is written `"testdata/x.json"`; a cross-tree input is
    # written whole -- `"src/dev_health_ops/.../work_graph.py"` -- and handed to
    # a helper that joins it onto the repo root itself. Resolving that second
    # spelling against `internal/providersync/` yields a path that does not
    # exist, so `add` dropped it and the file was invisible to the oracle while
    # LOOKING covered: the literal is right there in the test.
    #
    # `add` already requires the candidate to be a real file, so trying both
    # anchors cannot invent coverage -- at most one of them can exist.
    for literal in re.findall(r'"([^"\n]+)"', source):
        if literal.endswith(_GO_DATA_SUFFIXES):
            add(base / literal)
            add(anchor / literal)

    # (3) filepath.Join("a", "b.json") -- the segments are separate literals, so
    # the literal scan above sees "b.json" and resolves it against the wrong
    # directory. Joining them is what makes a multi-segment call resolvable; a
    # fixture caught this docstring claiming (1) covered it when it did not.
    for arguments in re.findall(r"filepath\.Join\(([^)]*)\)", source):
        segments = re.findall(r'"([^"\n]*)"', arguments)
        if segments and segments[-1].endswith(_GO_DATA_SUFFIXES):
            # BOTH anchors again, and for the same reason as the plain literal
            # above -- this is the shape that fix missed.
            #
            # The dominant spelling in this repo is
            # `filepath.Join(repoRoot, "src", ..., "x.json")`: the FIRST segment
            # is a VARIABLE, so the literal segments are repo-root-relative.
            # Joining them onto the naming file's directory produced
            # `cmd/query-api/src/dev_health_ops/...`, which does not exist, and
            # the file was dropped -- invisible, while the literals sit in plain
            # sight in the Go source.
            #
            # `len(segments) > 1` was also wrong: `filepath.Join(root, "x.json")`
            # yields ONE literal and is exactly the variable-rooted shape that
            # matters most. A single literal is now resolved against both
            # anchors, which is what the plain-literal branch already does.
            add(base.joinpath(*segments))
            add(anchor.joinpath(*segments))

        # (2) //go:embed — space-separated patterns, globs allowed.
    for directive in re.findall(r"//go:embed\s+(.+)", source):
        for pattern in directive.split():
            # Go's `all:` prefix changes DIRECTORY WALKING to include names
            # starting with `.` or `_` (go doc embed). It is not part of the
            # path, so it must be stripped before globbing -- otherwise the glob
            # looks for a directory literally named `all:uncovered` and silently
            # finds nothing. A file reachable only through an `all:` embed was
            # invisible to this oracle and to the workflow filters.
            include_dot_prefixed = pattern.startswith("all:")
            if include_dot_prefixed:
                pattern = pattern[len("all:") :]
            for match in base.glob(pattern):
                # A directory pattern embeds the whole TREE beneath it. `add`
                # requires a file, so globbing a directory added nothing at all
                # -- `//go:embed uncovered` covered none of its contents.
                if match.is_dir():
                    for entry in match.rglob("*"):
                        if not entry.is_file():
                            continue
                        # Without `all:`, Go excludes any path element starting
                        # with `.` or `_`, so this oracle must exclude them too:
                        # claiming coverage Go does not give is the same defect
                        # as missing coverage it does.
                        parts = entry.relative_to(match).parts
                        if not include_dot_prefixed and any(
                            part.startswith((".", "_")) for part in parts
                        ):
                            continue
                        add(entry)
                    continue
                add(match)

    # (4) os.ReadDir on a literal directory: its own non-.go files.
    for directory in re.findall(r'os\.ReadDir\("([^"\n]*)"\)', source):
        target = base / directory
        if target.is_dir():
            for entry in target.iterdir():
                add(entry)

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
def test_every_fixture_on_disk_triggers_the_go_workflow(event: str) -> None:
    """A change to any Go-relevant fixture must run the workflow that guards it.

    The oracle is the whole tree, not one directory. Anything under a `testdata/`
    or `fixtures/` directory must either match the filter or be named in
    UNCOVERED_FIXTURE_DIRECTORIES with a reason.
    """
    go_paths = (_on_block(_load(GO_WORKFLOW)).get(event) or {}).get("paths") or []
    fixtures = _fixture_like_files_on_disk() | _files_named_by_go_tests()

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
        + "\n\nEither add a pattern covering them to BOTH path lists in go.yml "
        "(push and pull_request), or add the "
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
        # A file is a legitimate exclusion key: the os.ReadDir reader shape
        # surfaces individual files a directory-wide entry would over-excuse.
        target = REPO_ROOT / directory
        assert target.is_dir() or target.is_file(), (
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


def test_every_covered_reader_shape_is_found(tmp_path: Path) -> None:
    """One fixture per reader shape the oracle claims to cover.

    A docstring listing four shapes is a claim. These are the claim's evidence,
    and each fails on its own if that shape regresses -- so the list cannot go
    stale while still reading as covered.
    """
    (tmp_path / "corpus.json").write_text("{}", encoding="utf-8")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "embedded.json").write_text("{}", encoding="utf-8")
    (tmp_path / "listed").mkdir()
    (tmp_path / "listed" / "scanned.sql").write_text("SELECT 1", encoding="utf-8")

    shapes = {
        "string literal": 'raw, _ := os.ReadFile("corpus.json")',
        "filepath.Join literal segments": 'os.ReadFile(filepath.Join("nested", "embedded.json"))',
        "go:embed directive": "//go:embed nested/embedded.json",
        "go:embed glob": "//go:embed nested/*.json",
        "os.ReadDir literal directory": 'entries, _ := os.ReadDir("listed")',
    }
    for name, source in shapes.items():
        found = _resolve_named_files(source, tmp_path, tmp_path)
        assert found, (
            f"reader shape {name!r} found nothing. The oracle's docstring claims "
            f"this shape is covered; either the shape regressed or the claim is "
            f"wrong -- and an overstated coverage claim is worse than an admitted "
            f"gap, because a reader stops checking."
        )


def test_a_repo_root_relative_literal_is_found(tmp_path: Path) -> None:
    """A whole-path literal resolves against the ROOT, not just its own directory.

    Go names inputs in two spellings. A sibling fixture is written relative to
    the test -- `"testdata/x.json"` -- but a cross-tree input is written whole,
    `"src/dev_health_ops/.../work_graph.py"`, and handed to a helper that joins
    it onto the repository root.

    Only the first spelling used to resolve. The second produced
    `internal/providersync/src/dev_health_ops/...`, which does not exist, so it
    was dropped -- and the file looked covered, because the literal is right
    there in the test. Nine real Go inputs were invisible this way, including
    two YAML configs and a SQL migration that no `.py` fix would have reached.
    """
    # Deliberately a `.json`, not a `.py`: this test isolates the ANCHOR fix.
    # A `.py` fixture here would also fail when the suffix-set fix is reverted,
    # so one test would report two different defects and neither would be named.
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "contract.json").write_text("{}", encoding="utf-8")
    package = tmp_path / "internal" / "providersync"
    package.mkdir(parents=True)

    found = _resolve_named_files(
        'readSource(t, "src/contract.json")', package, tmp_path
    )

    assert "src/contract.json" in found, (
        "a repo-root-relative literal was not resolved. It is named verbatim in "
        "Go source, so it reads as covered; if only the naming file's directory "
        "is tried, the path silently resolves to nothing and the input drops out "
        f"of the oracle entirely. found={sorted(found)}"
    )


def test_python_named_by_go_is_an_input(tmp_path: Path) -> None:
    """`.py` is a Go INPUT suffix, because Go tests read Python as data.

    A parity test scrapes the real sink module; another reads a migration. The
    suffix set excluded `.py`, so those reads were invisible: a PR touching only
    the Python file was classified non-Go, the contract test never ran, and the
    coverage oracle passed anyway -- the exact vacuous green it exists to stop.
    """
    (tmp_path / "sink.py").write_text("x = 1", encoding="utf-8")

    found = _resolve_named_files('readSource(t, "sink.py")', tmp_path, tmp_path)

    assert "sink.py" in found, (
        "a Python file named by Go source was not treated as an input; the Go "
        f"test that reads it would skip on a PR that changed it. found={sorted(found)}"
    )


def test_a_variable_rooted_filepath_join_is_found(tmp_path: Path) -> None:
    """`filepath.Join(repoRoot, "a", "b.json")` resolves against the ROOT.

    This is the DOMINANT spelling in this repo and it was the third instance of
    one root cause. The single-literal fix tried both anchors; the Join branch
    still joined only onto the naming file's directory, so a variable-rooted
    call produced `cmd/query-api/src/dev_health_ops/...` -- a path that does not
    exist -- and the file vanished from the oracle while its segments sat in
    plain sight in the Go source.

    `src/dev_health_ops/api/graphql/go_api_operations.json` was reached exactly
    this way from `cmd/query-api/query_route_mounted_log_test.go:188`, matched
    no path filter, and was invisible to two rounds of review.

    The single-segment case is covered deliberately: `filepath.Join(root,
    "x.json")` yields ONE literal, and the old `len(segments) > 1` guard
    skipped precisely the variable-rooted shape that matters most.
    """
    (tmp_path / "src" / "graphql").mkdir(parents=True)
    (tmp_path / "src" / "graphql" / "ops.json").write_text("{}", encoding="utf-8")
    (tmp_path / "one.json").write_text("{}", encoding="utf-8")
    package = tmp_path / "cmd" / "api"
    package.mkdir(parents=True)

    multi = _resolve_named_files(
        'p := filepath.Join(repoRoot, "src", "graphql", "ops.json")', package, tmp_path
    )
    assert "src/graphql/ops.json" in multi, (
        "a variable-rooted multi-segment filepath.Join was not resolved against "
        f"the repository root. found={sorted(multi)}"
    )

    single = _resolve_named_files(
        'p := filepath.Join(root, "one.json")', package, tmp_path
    )
    assert "one.json" in single, (
        "a variable-rooted SINGLE-segment filepath.Join was not resolved; the "
        f"old len(segments) > 1 guard skipped exactly this shape. found={sorted(single)}"
    )


def test_go_embed_prefix_and_directory_forms_are_found(tmp_path: Path) -> None:
    """`all:` is a walking MODE, not part of the path, and a directory embeds a tree.

    Two ways this silently found nothing. `//go:embed all:dir/x.json` was passed
    to `Path.glob` verbatim, so it looked for a directory literally named
    `all:dir` -- no match, no error. And a bare directory pattern globbed to the
    directory itself, which `add` rejects because it is not a file, so
    `//go:embed dir` covered none of its contents.

    The exclusion rule is asserted too, not just the inclusion. Without `all:`,
    Go skips any path element starting with `.` or `_` (go doc embed). An oracle
    that claimed those files were covered would be overstating coverage, which
    is the same defect as missing it -- it just fails in the other direction.
    """
    package = tmp_path / "cmd" / "pkg"
    (package / "data" / "sub").mkdir(parents=True)
    for name in (
        "data/.hidden.json",
        "data/_under.json",
        "data/plain.json",
        "data/sub/deep.json",
    ):
        (package / name).write_text("{}", encoding="utf-8")

    def resolve(pattern: str) -> set[str]:
        found = _resolve_named_files(f"//go:embed {pattern}", package, tmp_path)
        return {path.split("pkg/", 1)[1] for path in found}

    assert resolve("all:data/.hidden.json") == {"data/.hidden.json"}, (
        "an `all:`-prefixed file pattern found nothing; the prefix is a walking "
        "mode and must be stripped before globbing"
    )
    assert resolve("all:data") == {
        "data/.hidden.json",
        "data/_under.json",
        "data/plain.json",
        "data/sub/deep.json",
    }, "`all:` on a directory must embed the whole tree INCLUDING dot/underscore names"
    assert resolve("data") == {"data/plain.json", "data/sub/deep.json"}, (
        "a bare directory embeds its tree but EXCLUDES dot/underscore names; "
        "including them would claim coverage Go does not actually give"
    )


def test_the_runtime_built_path_limit_is_real(tmp_path: Path) -> None:
    """The CONTROL for the uncovered shape, so the stated limit is red-tested.

    The oracle cannot resolve a path assembled at runtime, and says so. If that
    ever becomes false -- someone teaches it to evaluate variables -- this test
    fails and the docstring gets corrected, instead of the limit quietly
    outliving its truth. A narrated limitation rots; a tested one cannot.
    """
    (tmp_path / "runtime.json").write_text("{}", encoding="utf-8")
    built_at_runtime = (
        'name := "runtime"\nraw, _ := os.ReadFile(fmt.Sprintf("%s.json", name))\n'
    )
    found = _resolve_named_files(built_at_runtime, tmp_path, tmp_path)
    assert "runtime.json" not in {Path(p).name for p in found}, (
        "the oracle resolved a runtime-built path. That is an improvement, but "
        "the docstring still calls those readers the residual risk -- correct "
        "the docstring in the same change that made this pass."
    )

    # CONTROL for the control: the same file IS found when named literally, so a
    # pass above means "runtime paths are unresolved", not "nothing resolves".
    literal = 'raw, _ := os.ReadFile("runtime.json")'
    assert {
        Path(p).name for p in _resolve_named_files(literal, tmp_path, tmp_path)
    } == {"runtime.json"}, (
        "the literal form is not found either, so the test above proves nothing"
    )


@pytest.mark.parametrize(
    "pattern,path,expected",
    [
        # The case this file's own translator got WRONG before consolidation: a
        # LEADING `**/` means zero or more directories, so a top-level file
        # matches. Kept from that translator's fixtures, per the consolidation.
        ("**/testdata/**", "testdata/a.json", True),
        ("**/testdata/**", "internal/x/testdata/a.json", True),
        ("**/testdata/**", "notestdata/a.json", False),
        # The case the SHARED translator handles and the deleted one did not:
        # `**/` is a token at any position, not only at the start.
        ("docs/**/x.md", "docs/x.md", True),
        ("docs/**/x.md", "docs/a/b/x.md", True),
        # Root-level under `**/`, the CHAOS-4834 false-green.
        ("**/*.go", "root.go", True),
        ("**/go.mod", "go.mod", True),
        ("**/go.mod", "mygo.mod", False),
        # `*` must not span a separator; `**` must.
        ("docs/*.md", "docs/a/b.md", False),
        ("docs/**", "docs/a/b.md", True),
    ],
)
def test_the_shared_translator_matches_github_semantics(
    pattern: str, path: str, expected: bool
) -> None:
    """Semantics of the ONE translator this file now shares with the relevance gate.

    These moved here when this file's own copy was deleted. They are not
    duplicated coverage: nothing else in `tests/` exercises
    `github_glob_to_regex` directly -- the sharding suite only invokes
    `go_relevance.py` as a subprocess, which cannot pin a per-pattern answer.

    Both directions matter. A false *match* over-triggers the Go workflow, which
    costs minutes. A false *non-match* marks a real change irrelevant and skips
    a required gate, which is the defect CHAOS-4834 exists to prevent.
    """
    assert bool(github_glob_to_regex(pattern).match(path)) is expected


def test_the_shared_translator_refuses_what_it_cannot_express() -> None:
    """The hard error, which was pinned NOWHERE before this.

    `?`, `[`, `]` and `!` are not implemented. Silently escaping them into
    literal text yields a filter matching nothing, which reports full coverage
    and skips the gate -- a false green in the direction that hides work.

    It raises SystemExit rather than AssertionError: `go_relevance.py` is a CLI
    first and a library second. Asserting the type here is deliberate, so a
    future refactor to a normal exception fails this test instead of silently
    changing how callers must guard.
    """
    for unsupported in ("a?b", "a[bc]d", "!negated"):
        with pytest.raises(SystemExit):
            github_glob_to_regex(unsupported)
