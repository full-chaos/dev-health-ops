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
import subprocess
from functools import lru_cache
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


# Every walk in this file enumerates TRACKED files rather than the filesystem.
#
# It used to walk `rglob("*")` and subtract a hardcoded set of six directory
# names (.git, .venv, node_modules, __pycache__, .mypy_cache, .ruff_cache). That
# list had to be extended by hand for every new ignored directory, and it had
# already fallen behind: `.uv-cache`, which ci/run_tests.sh creates at the repo
# root, was NOT in it. That cache holds third-party `testdata/` and `fixtures/`
# directories. On the machine where this was measured that was 11 non-Go files
# (sqlalchemy fixtures, protobuf testdata), and the coverage oracle collected
# every one as a REPO fixture and then failed for not finding it named in any
# workflow. The COUNT is machine state, not a property of this repository -- a
# fresh checkout has no cache and reproduces nothing -- so treat it as evidence
# the mechanism fires, not as a number to re-verify.
#
# Tracking is also the correct domain rather than merely a convenient filter: CI
# path filters match paths in a COMMIT, so an untracked file cannot be the reason
# a job should have run. Deriving the exclusion from git rather than from a list
# fixes the class -- a directory added to .gitignore tomorrow is excluded without
# anyone remembering to edit this file.
@lru_cache(maxsize=8)
def _tracked_paths(anchor: Path) -> tuple[Path, ...]:
    """Absolute paths of every file git tracks under *anchor*."""
    # The fallback is keyed on "is this anchor inside a git repository", NOT on
    # git having exited non-zero.
    #
    # lane-ci-flakes' caveat: if TMPDIR ever landed inside a git worktree -- some
    # CI setups put it in the workspace -- a synthetic tmp_path tree would be
    # INSIDE a repo, `ls-files` would succeed and return nothing for it, and the
    # non-empty assertion below would fire. That failure would surface in
    # whoever's PR happened to be running and look like a defect in their change.
    # A guard that misfires into a stranger's diff is worse than one that misses.
    #
    # Asking the real question -- is there a repository here -- separates "not a
    # repo, walk it" from "a repo that legitimately has no tracked files under
    # this anchor" from "git is broken", which an exit code alone conflates.
    inside_repo = subprocess.run(
        ["git", "-C", str(anchor), "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        check=False,
    )
    if inside_repo.returncode != 0 or inside_repo.stdout.strip() != b"true":
        # Not a git repository. This file's own tests build synthetic trees
        # under pytest's tmp_path to exercise the resolver, and those are not
        # repos. Walking them is correct there -- a temp tree has no ignored
        # directories to exclude -- but this branch must NEVER apply to the real
        # tree, where the whole point is that git decides what is excluded.
        # Resolved comparison, not a lexical one. codex round 1 showed the
        # lexical version was defeatable: a same-tree alias containing `..`
        # compares unequal to REPO_ROOT, and with a poisoned GIT_DIR the
        # fallback then walked the real tree and collected an ignored
        # .uv-cache fixture. Present callers all pass REPO_ROOT exactly, so
        # that was not a live omission -- but the guard claimed more than it
        # enforced, which is the failure this file exists to catch.
        assert anchor.resolve() != REPO_ROOT.resolve(), (
            f"no git work tree at the repo root ({anchor}): "
            f"{inside_repo.stderr.decode()[:200]}. Refusing to fall back to a "
            "filesystem walk for the real tree, because that reinstates the "
            "hardcoded-exclusion bug this helper exists to remove"
        )
        return tuple(path for path in anchor.rglob("*") if path.is_file())

    completed = subprocess.run(
        ["git", "-C", str(anchor), "ls-files", "-z"],
        capture_output=True,
        check=True,
    )
    names = [name for name in completed.stdout.decode().split("\0") if name]

    # EMPTY MEANS DIFFERENT THINGS DEPENDING ON THE ANCHOR, and conflating them
    # is what lane-ci-flakes' caveat was really about.
    #
    # For REPO_ROOT, empty is impossible in a working checkout: a walk that finds
    # nothing reports exactly what a clean tree reports, so every oracle below
    # would pass by examining nothing. That must fail loudly.
    #
    # For any OTHER anchor, empty is ordinary -- a directory inside the repo with
    # nothing tracked under it, which is exactly what this file's own synthetic
    # tmp_path trees look like if TMPDIR happens to sit inside a worktree. My
    # first attempt keyed the fallback on "is this inside a repo" and STILL
    # asserted here, so that case kept firing; I only found it by building the
    # scenario rather than reasoning about the fix. Walking is correct there.
    if not names:
        if anchor.resolve() == REPO_ROOT.resolve():
            raise AssertionError(
                f"git ls-files found no tracked files under the repo root "
                f"({anchor}); every check in this file would pass vacuously, so "
                "this fails loudly instead"
            )
        return tuple(path for path in anchor.rglob("*") if path.is_file())
    return tuple(anchor / name for name in names)


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
    for path in _tracked_paths(root):
        parts = path.relative_to(root).parts
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
    for source_file in _tracked_paths(REPO_ROOT):
        if source_file.suffix != ".go":
            continue
        try:
            source = source_file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        found |= _resolve_named_files(source, source_file.parent, REPO_ROOT)
    return found


@lru_cache(maxsize=8)
def _files_by_basename(anchor: Path) -> dict[str, tuple[str, ...]]:
    """Every real file under *anchor*, indexed by basename.

    Cached because it is consulted once per Go source file and rebuilding it
    each time is quadratic over the tree.
    """
    index: dict[str, list[str]] = {}
    for path in _tracked_paths(anchor):
        relative = path.relative_to(anchor)
        index.setdefault(path.name, []).append(relative.as_posix())
    return {name: tuple(paths) for name, paths in index.items()}


def _looks_like_a_path(literal: str) -> bool:
    """Reject prose that happens to end in a data suffix.

    Three Go error messages in this repo end in a real path -- "httpx2 is added
    to ci/requirements-live-python-oracles.txt" is a sentence, not a literal
    naming an input. Whitespace or a format verb is the tell.
    """
    return not (
        " " in literal
        or "\t" in literal
        or "%" in literal
        or literal.startswith(("http://", "https://"))
    )


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
            before = len(found)
            add(base / literal)
            add(anchor / literal)
            # Neither anchor worked, so the literal is relative to a directory
            # only known at runtime -- `filepath.Join(contractsDir, "x.json")`,
            # or a helper that joins the repo root itself. If exactly ONE file in
            # the tree carries that basename, the reference is unambiguous and
            # resolving it is strictly more coverage. Ambiguity is NOT resolved
            # here: it is reported by _unresolvable_named_paths instead, because
            # guessing between candidates is how an oracle starts asserting
            # coverage it does not have.
            if len(found) == before and _looks_like_a_path(literal):
                candidates = _files_by_basename(anchor).get(Path(literal).name, ())
                if len(candidates) == 1:
                    add(anchor / candidates[0])

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


# The one class that cannot be decided by property.
#
# A provider file-listing fixture fabricates a repository layout and names
# `__init__.py` in it. Over a hundred real `__init__.py` files exist, so the
# literal matches many candidates while referring to NONE of them -- it is not a
# reference to this repository at all.
#
# Every other case is decided by a property and needs no entry here: prose is
# filtered by _looks_like_a_path, absolute paths are deployment locations,
# a basename matching no file has nothing to cover, and an ambiguous literal
# passes when every candidate is already covered. This is the residue, and it is
# deliberately two lines rather than a growing list.
FABRICATED_PATH_LITERALS: dict[str, str] = {
    "__init__.py": "synthetic name in a fabricated provider file listing",
    "src/__init__.py": "same fixture, same fabricated layout",
}


def _unresolvable_named_paths() -> list[tuple[str, str, tuple[str, ...]]]:
    """Every data-suffixed literal naming a real file we could not resolve.

    THIS is the guard keyed on the PROPERTY rather than on a list of shapes.
    Four instances of one root cause reached review as separate findings --
    a missing suffix, a plain literal, a filepath.Join, a go:embed prefix --
    because each was a new WAY of naming a path, and the resolver's answer to
    every one of them was the same: resolve to nothing, and say nothing.

    A fifth way will exist. This does not try to enumerate it; it fails on the
    property all five share -- a literal that names a real file, which this
    resolver could not locate. The shape does not matter.

    Ambiguity is reported rather than guessed, and there is deliberately NO
    exception list: the caller decides whether an ambiguous literal is safe by
    the only question that matters -- whether EVERY candidate it could mean is
    already covered. If they all are, the ambiguity cannot hide a gap. If even
    one is not, it can.
    """
    root = REPO_ROOT
    index = _files_by_basename(root)
    unresolvable: list[tuple[str, str, tuple[str, ...]]] = []

    for source_file in _tracked_paths(root):
        if source_file.suffix != ".go":
            continue
        relative = source_file.relative_to(root)
        try:
            source = source_file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        unresolvable.extend(
            (f"{relative.as_posix()}:{number}", literal, candidates)
            for number, literal, candidates in _unresolvable_in_source(
                source, source_file.parent, root, index
            )
        )
    return unresolvable


def _unresolvable_in_source(
    source: str,
    base: Path,
    root: Path,
    index: dict[str, tuple[str, ...]],
) -> list[tuple[int, str, tuple[str, ...]]]:
    """The guard's core, split out so it can be exercised against a fixture tree.

    A guard that only runs against the real repository cannot be tested with a
    CONSTRUCTED input, and an untestable guard is how the last three versions of
    this oracle each shipped a coverage claim nobody had checked.
    """
    out: list[tuple[int, str, tuple[str, ...]]] = []
    for number, line in enumerate(source.splitlines(), 1):
        for literal in re.findall(r'"([^"\n]+)"', line):
            if not literal.endswith(_GO_DATA_SUFFIXES):
                continue
            if not _looks_like_a_path(literal):
                # Prose only. There is deliberately no absolute-path branch.
                #
                # There used to be, excusing `/app/config/x.yaml` as a
                # deployment location. It was the last SHAPE-based exception in
                # a guard whose whole principle is that there are none, and it
                # was inert: all ten absolute literals in this repo either name
                # no file at all (8) or have every candidate covered (2), so
                # deleting it changed no outcome -- a mutation that SURVIVED.
                #
                # Inert is not harmless. It was silent on exactly the risk this
                # guard exists to catch: `/app/config/secrets.yaml` and
                # `config/secrets.yaml` carry identical ambiguity and identical
                # uncovered candidates, and it reported only the second. A
                # leading slash decided it.
                #
                # The two cases it excused are already handled by properties
                # that were here anyway: a literal naming no file falls out at
                # `not candidates`, and one whose candidates are all covered
                # falls out at the caller's coverage check.
                continue
            if literal in FABRICATED_PATH_LITERALS:
                continue
            candidates = index.get(Path(literal).name, ())
            if not candidates:
                # Names no file in the tree, so there is nothing to cover.
                continue
            # Ask the same questions the resolver asks, rather than
            # string-matching its OUTPUT against the literal. The output is
            # normalised (`internal/x/../../tests/f.json` becomes
            # `tests/f.json`) while the literal is not, so a substring
            # comparison reported seven `../..` paths as unresolvable when
            # the resolver had resolved every one of them.
            if (base / literal).is_file() or (root / literal).is_file():
                continue
            if len(candidates) == 1:
                continue  # the unique-basename fallback resolves it
            out.append((number, literal, candidates))
    return out


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


def test_no_named_path_resolves_to_nothing() -> None:
    """A literal naming a real file must never be dropped in silence.

    THE POINT OF THIS TEST, which is different from every other test here.

    Four instances of one root cause reached review as four separate findings:
    a missing `.py` suffix, a plain repo-root-relative literal, a variable-rooted
    `filepath.Join`, and a `go:embed all:` prefix. Each was a new WAY of naming a
    path, each was found by a different round, and the resolver's response to all
    four was identical -- resolve to nothing, report nothing, stay green.

    Enumerating shapes cannot end that, because the next shape is by definition
    the one nobody listed. This keys on the PROPERTY the four share instead: a
    literal that names a file which really exists, that this resolver could not
    locate. When the fifth shape arrives it fails here, named, with its
    file:line -- rather than silently subtracting from coverage.

    Ambiguous literals are not guessed at. They pass only when EVERY candidate
    is already covered, which is the question that actually decides whether the
    ambiguity can hide a gap.
    """
    go_paths = (_on_block(_load(GO_WORKFLOW)).get("pull_request") or {}).get(
        "paths"
    ) or []

    dangerous = []
    for location, literal, candidates in _unresolvable_named_paths():
        uncovered = [c for c in candidates if not _matches_any(c, go_paths)]
        if uncovered:
            dangerous.append((location, literal, uncovered))

    assert not dangerous, (
        "these literals name a real file the resolver could not locate, and at "
        "least one candidate matches no path filter -- so a PR changing it is "
        "classified non-Go and the Go test that reads it never runs:\n  "
        + "\n  ".join(
            f"{location}: {literal!r} could mean {uncovered}"
            for location, literal, uncovered in dangerous
        )
        + "\n\nEither teach _resolve_named_files the shape that names it, or add "
        "a pattern covering every candidate. Do NOT add the literal to an "
        "exception list: the whole point of this check is that it keys on the "
        "property rather than on a list of known shapes."
    )


def test_a_sixth_unknown_shape_fails_loud_instead_of_vanishing(tmp_path: Path) -> None:
    """The guard must fire on a naming shape the resolver has never heard of.

    Four instances of one root cause arrived as four separate findings, each a
    new WAY of naming a path. A fifth and a sixth will exist. This constructs
    one the resolver genuinely does not implement -- `os.DirFS` plus a relative
    `Open` -- and asserts it is REPORTED rather than silently dropped.

    The file is deliberately ambiguous (two `data.json` in the tree) so the
    unique-basename fallback cannot rescue it. That is the honest test: the
    resolver still cannot locate it, and the requirement is that it says so.
    """
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "a" / "data.json").write_text("{}", encoding="utf-8")
    (tmp_path / "b" / "data.json").write_text("{}", encoding="utf-8")
    package = tmp_path / "cmd" / "svc"
    package.mkdir(parents=True)

    source = 'fsys := os.DirFS(repoRoot)\n f, _ := fsys.Open("data.json")'

    assert not _resolve_named_files(source, package, tmp_path), (
        "the resolver is not expected to understand os.DirFS; if it now does, "
        "this test's premise is stale and should be rewritten around a shape "
        "that is genuinely unknown"
    )

    index: dict[str, tuple[str, ...]] = {"data.json": ("a/data.json", "b/data.json")}
    reported = _unresolvable_in_source(source, package, tmp_path, index)

    assert reported, (
        "a literal naming a real file was neither resolved NOR reported -- it "
        "vanished. That is the exact failure mode four review rounds found in "
        "four different shapes, and the guard exists to end it."
    )
    assert reported[0][1] == "data.json"
    assert set(reported[0][2]) == {"a/data.json", "b/data.json"}, (
        "the report must name every candidate, so the reader can decide whether "
        "the ambiguity can hide a gap"
    )


def test_an_absolute_literal_is_judged_like_any_other(tmp_path: Path) -> None:
    """A leading slash must not buy an exemption.

    Constructed by lane-ci-flakes during its executed read: two literals with
    identical ambiguity and identical uncovered candidates, differing only in a
    leading `/`. The old absolute-path branch reported the relative one and
    stayed silent on the absolute one.

    That branch was inert -- deleting it changed no current outcome, a mutation
    that survived -- which is exactly why it was worth deleting. A dead branch
    that reads as a judgement is a judgement nobody has had to defend, and this
    one would have woken up the first time a Go test read a deployment path
    whose repo-side original was ambiguous and uncovered.
    """
    package = tmp_path / "cmd" / "svc"
    package.mkdir(parents=True)
    index: dict[str, tuple[str, ...]] = {
        "secrets.yaml": ("a/secrets.yaml", "b/secrets.yaml")
    }

    absolute = _unresolvable_in_source(
        'readConfig("/app/config/secrets.yaml")', package, tmp_path, index
    )
    relative = _unresolvable_in_source(
        'readConfig("config/secrets.yaml")', package, tmp_path, index
    )

    assert absolute, (
        "an absolute literal naming an ambiguous, real file was not reported. "
        "It carries the same risk as the relative spelling, and a leading slash "
        "is not a reason to treat it differently."
    )
    assert [literal for _, literal, _ in absolute] == ["/app/config/secrets.yaml"]
    assert absolute[0][2] == relative[0][2], (
        "both spellings name the same candidates, so both must report the same "
        "candidates"
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


def test_a_gitignored_directory_is_not_collected_as_a_repo_fixture() -> None:
    """The defect that motivated enumerating tracked files instead of walking.

    `ci/run_tests.sh` creates `.uv-cache/` at the repo root. It is gitignored,
    but the old walk subtracted a hardcoded list of six directory names and
    `.uv-cache` was not among them -- so its contents were walked. uv's cache
    contains third-party packages, and some ship directories literally named
    `fixtures/` and `testdata/`: sqlalchemy and protobuf between them put 11
    non-Go files there on the machine where this was measured. Every one would
    have been collected as a REPO fixture and then reported as uncovered by any
    workflow path filter -- a failure caused entirely by a dependency's internal
    layout, in a check about this repository's CI configuration.

    Asserting the property rather than adding `.uv-cache` to a list is the whole
    point: the next ignored directory is excluded without anyone editing this
    file.
    """
    probe_root = REPO_ROOT / ".uv-cache" / "synthetic-probe" / "fixtures"
    probe = probe_root / "probe.json"
    created_marker = REPO_ROOT / ".uv-cache"
    already_existed = created_marker.exists()

    probe_root.mkdir(parents=True, exist_ok=True)
    probe.write_text("{}\n", encoding="utf-8")
    try:
        # Control 1: git really does ignore it. If this ever stops being true the
        # test below would pass for the wrong reason -- the file being absent
        # because it is tracked-and-missing rather than ignored.
        ignored = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "check-ignore", str(probe)],
            capture_output=True,
            check=False,
        )
        assert ignored.returncode == 0, (
            f"{probe} is not gitignored, so this test no longer exercises the "
            "ignored-directory path at all"
        )

        # Control 2: the probe satisfies every criterion the collector uses, so
        # its absence below is due to tracking and nothing else.
        assert probe.suffix != ".go"
        assert "fixtures" in probe.relative_to(REPO_ROOT).parts

        _tracked_paths.cache_clear()
        collected = _fixture_like_files_on_disk()

        # Control 3: the collector is live. A collector returning nothing would
        # pass the real assertion below while checking nothing at all.
        assert collected, "the fixture collector returned nothing; it is not live"

        relative = probe.relative_to(REPO_ROOT).as_posix()
        assert relative not in collected, (
            f"{relative} was collected as a repo fixture. It lives in a "
            "gitignored dependency cache; treating it as this repository's "
            "fixture makes the coverage oracle fail on a third-party package's "
            "internal directory names"
        )
    finally:
        probe.unlink(missing_ok=True)
        probe_root.rmdir()
        probe_root.parent.rmdir()
        if not already_existed:
            created_marker.rmdir()
        _tracked_paths.cache_clear()


def test_an_empty_tracked_list_for_the_repo_root_fails_loudly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The vacuous-pass guard must still fire where it matters.

    `_tracked_paths` returns a filesystem walk when an anchor inside the repo has
    nothing tracked under it -- that is ordinary, and asserting there made this
    file's own synthetic trees fail whenever TMPDIR sat inside a worktree.

    But for REPO_ROOT an empty list is impossible in a working checkout, and
    silently walking instead would restore exactly the bug this helper removed:
    every oracle in this file would then examine an unfiltered tree, or nothing,
    and pass either way.

    Softening a guard is where guards die, so this asserts the surviving half
    still bites rather than trusting that it does.
    """

    def no_tracked_files(
        args: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        if "rev-parse" in args:
            return subprocess.CompletedProcess(args, 0, b"true\n", b"")
        return subprocess.CompletedProcess(args, 0, b"", b"")

    _tracked_paths.cache_clear()
    monkeypatch.setattr(subprocess, "run", no_tracked_files)
    try:
        with pytest.raises(
            AssertionError, match="no tracked files under the repo root"
        ):
            _tracked_paths(REPO_ROOT)
    finally:
        monkeypatch.undo()
        _tracked_paths.cache_clear()
