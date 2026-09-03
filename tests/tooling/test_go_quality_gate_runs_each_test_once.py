"""The live-Python oracle section must not run the same test twice.

WHY THIS EXISTS
---------------
`ci/check_go.sh` ran `TestSumGoldenMatchesLivePython` **three times**: two
byte-identical solo blocks and once more inside the combined live-oracle block.
Each solo block re-ran the same test against the same fixture and re-checked the
same `python-sum-golden` marker, so the second and third runs could not fail
unless the first already had.

That is not merely wasted CI time. A duplicated block is a maintenance hazard
with a specific failure mode: the next person to change one of them changes only
one, and the gate then contains two versions of the same check disagreeing about
what it checks. Nothing reports the disagreement, because both blocks pass.

WHY THE FIRST VERSION OF THIS FILE WAS UNSOUND
----------------------------------------------
It extracted literal `Test[A-Za-z0-9_]+` tokens from `-run` patterns and counted
them. A codex round broke it twice, by construction, and both breaks were the
same mistake: **it asked what an invocation NAMED, not what it RUNS.**

  1. A wildcard selector names nothing. `-run '^Test.*GoldenMatchesLivePython$'`
     runs the sum oracle again and yields zero literal tokens, so the counter,
     the floors and the package check all passed with the duplicate present.

  2. The unfiltered-package inventory was a HARD-CODED list of four packages,
     and the comment above it claimed it was "re-derived by the test below"
     when the test simply iterated the constant. Adding an unfiltered
     `go test ./internal/pythonparity` re-ran the sum oracle, and that package
     was not on the list, so nothing saw it.

The second one is the sharper lesson: I replaced an enumeration with another
enumeration, in a file whose entire argument is that enumerations do not work,
and wrote a comment asserting the opposite of what the code did.

WHAT IT DOES NOW
----------------
It DERIVES, for each `go test` invocation in the script, the set of tests that
invocation actually executes:

  * every invocation is parsed out of the script, with its package paths and its
    `-run` selector if it has one -- single OR double quoted;
  * the real test inventory comes from scanning `func Test...` declarations in
    the packages named;
  * a selector is applied as Go applies it, and an invocation with NO selector
    runs everything in its package.

Then it asserts no test is in two invocations' sets. Wildcards, unfiltered
packages, and packages nobody listed are all handled by the same mechanism,
because none of them is a special case of "what does this name".

GO'S `-run` IS UNANCHORED, WHICH MATTERS HERE
---------------------------------------------
`go test -run TestFoo` also runs `TestFooBar`: the pattern is matched with
`regexp.MatchString` semantics against each slash-separated part of the name,
not compared for equality. The simulation below uses `re.search` for that
reason. Treating it as an equality test would under-count what a loose selector
executes, which is the same error in a new place.
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CHECK_GO = REPO_ROOT / "ci/check_go.sh"

GO_TEST = re.compile(r"\bgo test\b")
# `-run` may be single- or double-quoted. The first version handled only single,
# so a double-quoted duplicate was invisible.
SELECTOR = re.compile(r"-run\s+(?:'([^']*)'|\"([^\"]*)\")")
PACKAGE = re.compile(r"(\./[A-Za-z0-9_./-]+)")
DECLARED_TEST = re.compile(r"^func (Test[A-Za-z0-9_]+)", re.M)

ORACLE_ENV = "DEV_HEALTH_LIVE_PYTHON_ORACLES=1"


def _invocations() -> list[tuple[str | None, list[str]]]:
    """Every LIVE-ORACLE `go test` command as (selector or None, packages).

    # PARSED BY WALKING THE CONTINUATION CHAIN, NOT BY ONE REGEX
    #
    # A shell command here spans many lines: the environment prefix sits ABOVE
    # `go test` and the package argument BELOW it, all joined by trailing
    # backslashes. A single regex anchored on `go test` found 13 of them and
    # silently dropped the rest, which would have made this check under-count
    # exactly the way its predecessor did. Walking backward and forward from the
    # `go test` line over the backslash chain finds 28, of which 21 are oracle
    # runs -- and the count is asserted below rather than trusted.
    #
    # WHY SCOPED TO THE LIVE-ORACLE RUNS
    #
    # Unscoped, this fires 8-9 times per test, and correctly: the gate really
    # does run `go test ./...` for the unit pass, again under -race, and again
    # elsewhere. Those are different MODES over the same code, not duplicates.
    # A live-Python oracle test SKIPS unless DEV_HEALTH_LIVE_PYTHON_ORACLES=1 is
    # set, so a sweep without it does not execute the oracle at all. The
    # duplicate this file exists to prevent lived entirely in the oracle
    # section, where the variable IS set and the test really ran three times.
    """
    lines = CHECK_GO.read_text(encoding="utf-8").splitlines()
    found: list[tuple[str | None, list[str]]] = []
    for index, line in enumerate(lines):
        if not GO_TEST.search(line):
            continue
        # Prose mentioning `go test` is not an invocation.
        if line.lstrip().startswith("#") or "printf" in line:
            continue
        start = index
        while start > 0 and lines[start - 1].rstrip().endswith("\\"):
            start -= 1
        end = index
        while lines[end].rstrip().endswith("\\") and end + 1 < len(lines):
            end += 1
        block = "\n".join(lines[start : end + 1])
        packages = PACKAGE.findall(block)
        if not packages or ORACLE_ENV not in block:
            continue
        match = SELECTOR.search(block)
        found.append(((match.group(1) or match.group(2)) if match else None, packages))
    return found


def _tests_declared_in(package_argument: str) -> set[str]:
    """Test functions declared under a `./path` or `./path/...` argument."""
    relative = package_argument.lstrip("./").removesuffix("/...")
    directory = REPO_ROOT / relative
    if not directory.is_dir():
        return set()
    names: set[str] = set()
    for source in directory.rglob("*_test.go"):
        names.update(
            DECLARED_TEST.findall(source.read_text(encoding="utf-8", errors="ignore"))
        )
    return names


def _tests_executed_by(selector: str | None, packages: list[str]) -> set[str]:
    """What this invocation actually runs.

    No selector means EVERY test in the package -- the case the previous version
    could not see at all, because it had no name to extract.
    """
    available: set[str] = set()
    for package in packages:
        available |= _tests_declared_in(package)
    if selector is None:
        return available
    try:
        pattern = re.compile(selector)
    except re.error:
        # An unparseable selector must not be silently treated as matching
        # nothing; that would hide every test it runs.
        raise AssertionError(
            f"`-run {selector!r}` in ci/check_go.sh is not a valid regex, so this "
            "check cannot determine what it executes and must not guess"
        ) from None
    # Go matches unanchored, per slash-separated part; re.search mirrors that.
    return {name for name in available if pattern.search(name)}


def test_no_go_test_is_executed_by_two_invocations() -> None:
    """A test reachable from two invocations is run twice by one gate pass."""
    by_test: dict[str, int] = defaultdict(int)
    for selector, packages in _invocations():
        for name in _tests_executed_by(selector, packages):
            by_test[name] += 1

    duplicated = {name: count for name, count in by_test.items() if count > 1}

    assert not duplicated, (
        "these Go tests are executed by more than one `go test` invocation in "
        "ci/check_go.sh:\n  "
        + "\n  ".join(f"{count}x  {name}" for name, count in sorted(duplicated.items()))
        + "\n\nThis is computed from what each invocation RUNS -- resolving "
        "wildcard selectors against the real test inventory, and treating an "
        "invocation with no `-run` as running its whole package -- not from the "
        "names a selector happens to spell out.\n"
        "If a test genuinely needs two invocations (different environment or "
        "build tags), say so with a reason in this file rather than deleting "
        "this assertion."
    )


def test_the_parser_finds_a_plausible_number_of_invocations() -> None:
    """Vacuity guard: an unparsed script must fail, not pass silently.

    A renamed flag, a quoting change or a truncated file would otherwise yield
    zero invocations, zero executed tests, zero duplicates -- and the check
    above would go green while looking at nothing.
    """
    invocations = _invocations()
    executed = set()
    for selector, packages in invocations:
        executed |= _tests_executed_by(selector, packages)

    assert len(invocations) >= 15, (
        f"only {len(invocations)} `go test` invocation(s) parsed from "
        "ci/check_go.sh; the gate runs far more, so the parse has broken rather "
        "than the script having shrunk"
    )
    assert len(executed) >= 30, (
        f"only {len(executed)} test(s) resolved across those invocations; the "
        "duplicate check above would pass vacuously on a set this small"
    )


def test_the_parser_reads_the_whole_continuation_chain() -> None:
    """Pin the parser property that a single regex got wrong.

    The environment prefix is ABOVE `go test` and the package argument BELOW it.
    A parser that reads only the `go test` line, or only forward from it, finds
    neither -- and an invocation it cannot see is an invocation it cannot report
    as a duplicate. That is how the previous version failed, so the replacement
    asserts its own reach rather than describing it.
    """
    invocations = _invocations()
    assert invocations, "no live-oracle invocations parsed at all"

    with_selector = [selector for selector, _ in invocations if selector is not None]
    without_selector = [selector for selector, _ in invocations if selector is None]

    assert with_selector, (
        "no invocation with a `-run` selector was found; the selector sits on a "
        "continuation line, so this means the parser is not reading forward"
    )
    assert without_selector, (
        "no unfiltered invocation was found; the gate has several, and missing "
        "them is the blind spot that let a duplicate through before"
    )


ORACLE_ENV_LITERAL = '"DEV_HEALTH_LIVE_PYTHON_ORACLES"'
DECLARATION_WINDOW = 1500


def _oracle_env_identifiers(package_dir: Path) -> set[str]:
    """Every identifier in this package whose value IS the oracle env var.

    DERIVED, not listed. Three different spellings exist in the tree --
    `livePythonOraclesEnv`, `fmaLivePythonOraclesEnv`, and the bare literal --
    and a fourth will appear the moment someone adds a package. Enumerating the
    names is the mistake this whole file is about: an earlier version of this
    very check listed two of them, reported five gated tests as ungated, and
    would have reported a sixth spelling as a real finding.

    So instead: find the constants in the package that are DEFINED as the
    literal, and accept a gate that reads any of them.
    """
    identifiers = {ORACLE_ENV_LITERAL.strip('"')}
    assignment = re.compile(
        r"(?:const\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*" + re.escape(ORACLE_ENV_LITERAL)
    )
    for source in package_dir.rglob("*.go"):
        identifiers.update(
            assignment.findall(source.read_text(encoding="utf-8", errors="ignore"))
        )
    return identifiers


def _gating_functions(package_dir: Path, gates: set[str]) -> set[str]:
    """Functions in this package that themselves perform the skip.

    A test may not read the variable at all: four in
    internal/jobs/metrics/testops call `runPythonOracle(t, ...)`, and the skip
    lives inside that helper. Gating one hop away is still gating.

    Derived the same way as the identifiers -- find the functions whose body
    reads a gate and calls t.Skip, rather than naming `runPythonOracle` here and
    waiting for the next helper to be written.
    """
    found: set[str] = set()
    declaration = re.compile(r"^func ([A-Za-z_][A-Za-z0-9_]*)\(", re.M)
    for source in package_dir.rglob("*.go"):
        text = source.read_text(encoding="utf-8", errors="ignore")
        matches = list(declaration.finditer(text))
        for index, match in enumerate(matches):
            stop = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            body = text[match.start() : stop]
            if "t.Skip(" not in body:
                continue
            if any(
                f"os.Getenv({gate})" in body or f'os.Getenv("{gate}")' in body
                for gate in gates
            ):
                found.add(match.group(1))
    return found


def test_every_oracle_named_test_skips_without_the_env_var() -> None:
    """The property that makes the scoping above SOUND rather than convenient.

    This file only counts invocations that set DEV_HEALTH_LIVE_PYTHON_ORACLES,
    on the reasoning that `check_test` and `check_race` run `go test ./...`
    across every package and therefore reach these tests too -- but do not
    EXECUTE them, because each one skips without the variable.

    That reasoning is load-bearing and was, until now, only stated. If a named
    oracle test were not gated, the `./...` sweeps would run it for real and
    this file's central claim -- that only oracle-scoped invocations can
    duplicate an oracle run -- would be false while every test here stayed
    green.

    The gate identifier is DERIVED from the package rather than listed: three
    spellings exist (`livePythonOraclesEnv`, `fmaLivePythonOraclesEnv`, the bare
    literal) and the first version of this check listed two, reporting five
    gated tests as ungated and one real spelling as a finding. Constants are
    resolved to their value instead.

    Note what is NOT asserted: that every `-run` name anywhere in the script is
    gated this way. `TestExplicitQueueMultiReplicaClaimDrainRestart` is named in
    `check_multi_replica_workers`, which sets no oracle variable and passes
    `-tags=integration`; a plain `./...` sweep does not compile it in at all.
    Gating by build tag is a different mechanism and this check has no opinion
    on it -- which is why the population here is the oracle-scoped invocations,
    not every selector in the file.
    """
    named: set[str] = set()
    for selector, _ in _invocations():
        if selector is not None:
            named.update(re.findall(r"Test[A-Za-z0-9_]+", selector))

    assert named, "no oracle-scoped test names resolved; the parse has broken"

    ungated: list[str] = []
    for name in sorted(named):
        declaration = re.compile(rf"^func {re.escape(name)}\(", re.M)
        located = False
        for source in REPO_ROOT.rglob("*_test.go"):
            text = source.read_text(encoding="utf-8", errors="ignore")
            match = declaration.search(text)
            if not match:
                continue
            located = True
            window = text[match.start() : match.start() + DECLARATION_WINDOW]
            gates = _oracle_env_identifiers(source.parent)
            reads_gate = (
                any(
                    f"os.Getenv({gate})" in window or f'os.Getenv("{gate}")' in window
                    for gate in gates
                )
                and "t.Skip(" in window
            )
            calls_gate = any(
                f"{helper}(" in window
                for helper in _gating_functions(source.parent, gates)
            )
            if not (reads_gate or calls_gate):
                ungated.append(f"{name}  ({source.relative_to(REPO_ROOT)})")
            break
        if not located:
            ungated.append(f"{name}  (DECLARATION NOT FOUND)")

    assert not ungated, (
        "these tests are named in a live-oracle `-run` but do not skip when "
        "DEV_HEALTH_LIVE_PYTHON_ORACLES is unset:\n  "
        + "\n  ".join(ungated)
        + "\n\nThat breaks this file's scoping argument. `check_test` and "
        "`check_race` run `go test ./...` across every package, so an ungated "
        "oracle test is executed there as well as by its named invocation -- a "
        "duplicate run that the scoped check above deliberately does not look "
        "for, because it assumes this property holds."
    )
