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

import os
import re
import subprocess
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


ASSIGNMENT = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)=(\((?P<arr>[^)]*)\)|'(?P<sq>[^']*)'|\"(?P<dq>[^\"]*)\"|(?P<bare>\S+))",
    re.M,
)
UNRESOLVED = re.compile(r"\$\{?[A-Za-z_]")


def _shell_values(source: str) -> dict[str, str]:
    """Scalar and array assignments, so the checks see what the SHELL sees.

    Round 2 broke the previous version three times and every break was the same
    thing: the script says `$ORACLE_SELECTOR` or `"${ORACLE_ENV[@]}"` and a
    literal-text scan reads the variable NAME. The value is right there in the
    file; not resolving it was the bug.

    Only the simple forms this script actually uses are resolved. Anything left
    unresolved is REPORTED, not skipped -- see
    test_no_go_test_command_is_unresolvable. A parser that cannot be complete
    must at least refuse to be silently incomplete.
    """
    values: dict[str, str] = {}
    for match in ASSIGNMENT.finditer(source):
        name = match.group(1)
        raw = (
            match.group("arr")
            or match.group("sq")
            or match.group("dq")
            or match.group("bare")
            or ""
        )
        values[name] = " ".join(raw.split())
    return values


def _expand(block: str, values: dict[str, str]) -> str:
    """Substitute known variables until it stops changing."""
    for _ in range(5):
        before = block
        for name, value in values.items():
            block = (
                block.replace(f'"${{{name}[@]}}"', value)
                .replace(f"${{{name}[@]}}", value)
                .replace(f'"${{{name}}}"', value)
                .replace(f"${{{name}}}", value)
                .replace(f'"${name}"', value)
                .replace(f"${name}", value)
            )
        if block == before:
            break
    return block


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
    raw_source = CHECK_GO.read_text(encoding="utf-8")
    lines = raw_source.splitlines()
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
        block = _expand("\n".join(lines[start : end + 1]), _shell_values(raw_source))
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


# Go prints a TOP-LEVEL result at column 0 and indents subtests beneath it:
#
#     --- PASS: TestOracle (0.00s)
#         --- SKIP: TestOracle/unrelated (0.00s)
#
# So `"--- SKIP: TestOracle" in output` is satisfied by a skipped SUBTEST while
# the PARENT passed -- that is, while the parent really executes under the
# `./...` sweeps, which is the exact thing this file exists to prevent. Round 3
# constructed it. Anchoring to column 0 and requiring a space after the name
# rejects the subtest while keeping the genuine top-level skip.
def _top_level_result(output: str, name: str, verdict: str) -> bool:
    """Did `name` ITSELF report `verdict`, ignoring any subtest of it?"""
    pattern = rf"^--- {verdict}: {re.escape(name)} "
    return re.search(pattern, output, re.M) is not None


def test_every_oracle_named_test_skips_without_the_env_var() -> None:
    """RUN them. Do not pattern-match the source for something skip-shaped.

    This assertion is what makes the oracle-scoping SOUND: `check_test` and
    `check_race` sweep every package with `go test ./...`, so these tests are
    REACHED there -- and only skip because the variable is unset. If one did not
    skip, the sweep would execute it for real and this file's central claim
    would be false while every check here stayed green.

    # WHY THIS EXECUTES INSTEAD OF READING THE SOURCE
    #
    # Two independent reviewers broke the source-reading version, and both
    # breaks were things no pattern can see:
    #
    #   lane-ci-flakes  inverting `!=` to `==` -- ONE CHARACTER -- leaves a gate
    #                   that reads the variable and calls t.Skip, so every
    #                   pattern still matches, while the test now runs under the
    #                   sweep and skips in the oracle block. Coverage moves to
    #                   the machine without Python configured.
    #
    #   codex round 2   `if false { if os.Getenv(env) == "" { t.Skip(...) } }`
    #                   satisfies a substring scan over the declaration and is
    #                   unreachable.
    #
    # Both are answered by the same move, and it is the move this whole file is
    # about: stop asking what the source LOOKS LIKE and ask what the code DOES.
    # A skip that does not fire is not a gate, however it is spelled.
    """
    by_package: dict[str, set[str]] = defaultdict(set)
    for selector, packages in _invocations():
        if selector is None:
            continue
        names = re.findall(r"Test[A-Za-z0-9_]+", selector)
        for package in packages:
            for name in names:
                by_package[package].add(name)

    assert by_package, "no oracle-scoped test names resolved; the parse has broken"

    environment = dict(os.environ)
    environment.pop("DEV_HEALTH_LIVE_PYTHON_ORACLES", None)
    environment["GOFLAGS"] = "-p=2"
    environment["GOMAXPROCS"] = "4"

    not_skipped: list[str] = []
    checked = 0
    for package, package_names in sorted(by_package.items()):
        pattern = "^(" + "|".join(sorted(package_names)) + ")$"
        completed = subprocess.run(
            ["go", "test", "-mod=readonly", "-count=1", "-run", pattern, "-v", package],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=600,
        )
        for name in sorted(package_names):
            checked += 1
            # `--- SKIP: Name` is the only proof the gate fired. A test that did
            # not run at all (build failure, wrong package) is reported too --
            # absence of a PASS is not evidence of a SKIP.
            if not _top_level_result(completed.stdout, name, "SKIP"):
                ran = _top_level_result(
                    completed.stdout, name, "PASS"
                ) or _top_level_result(completed.stdout, name, "FAIL")
                not_skipped.append(
                    f"{name} ({package}): {'EXECUTED' if ran else 'no SKIP and no result'}"
                )

    assert not not_skipped, (
        f"of {checked} oracle-named tests, these did not SKIP with "
        "DEV_HEALTH_LIVE_PYTHON_ORACLES unset:\n  "
        + "\n  ".join(not_skipped)
        + "\n\nThe `./...` sweeps in check_test and check_race reach these "
        "tests, so one that does not skip is executed there -- on a runner with "
        "no live interpreter configured -- as well as by its named invocation."
    )


def test_no_go_test_command_is_unresolvable() -> None:
    """Anything the checks cannot model must be REPORTED, never skipped.

    The scope filter and the selector resolver both work on expanded text. If a
    command still contains an unexpanded `$NAME` after substitution, then this
    file cannot tell whether it is an oracle run, nor which tests it executes --
    and the previous version handled exactly that case by silently ignoring the
    command, which is how round 2 slipped two duplicates past it.

    Failing loudly is the only honest option: the alternative is a checker that
    quietly narrows its own scope whenever the script uses a construct it does
    not parse.
    """
    raw_source = CHECK_GO.read_text(encoding="utf-8")
    values = _shell_values(raw_source)
    lines = raw_source.splitlines()

    unresolvable: list[str] = []
    for index, line in enumerate(lines):
        if (
            not GO_TEST.search(line)
            or line.lstrip().startswith("#")
            or "printf" in line
        ):
            continue
        start = index
        while start > 0 and lines[start - 1].rstrip().endswith("\\"):
            start -= 1
        end = index
        while lines[end].rstrip().endswith("\\") and end + 1 < len(lines):
            end += 1
        block = _expand("\n".join(lines[start : end + 1]), values)
        if not PACKAGE.findall(block):
            continue
        # Only the positions that decide WHAT RUNS matter. `${PYTHON:-python3}`
        # and `${PYTHONPATH:+...}` are parameter expansions with defaults: they
        # choose an interpreter and a path, not a test set, and flagging them
        # would make this check fire on legitimate structure -- which is how a
        # guard gets weakened by the next person who meets it.
        #
        # What DOES matter: an unresolved selector (we cannot tell which tests
        # run) and an unresolved array expansion in the environment prefix (we
        # cannot tell whether this is an oracle run at all). Those are exactly
        # the two constructions that slipped duplicates past round 2.
        # An unresolved selector only matters if this IS an oracle run. The
        # providersync integration shards build `-run "${test_regex}"` at
        # runtime from their shard assignment, and that is fine: no oracle
        # variable is set, so the command is classified as non-oracle without
        # needing to know which tests it names.
        selector = SELECTOR.search(block)
        if (
            ORACLE_ENV in block
            and selector
            and UNRESOLVED.search(selector.group(1) or selector.group(2) or "")
        ):
            unresolvable.append(
                f"line {index + 1}: unresolved `-run` selector in a live-oracle command"
            )
        for expansion in re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\[@\]\}", block):
            unresolvable.append(
                f"line {index + 1}: unresolved array ${{{expansion}[@]}} in the "
                "environment prefix"
            )

    assert not unresolvable, (
        "these `go test` commands in ci/check_go.sh contain a variable this "
        "checker could not resolve:\n  "
        + "\n  ".join(unresolvable)
        + "\n\nIt therefore cannot tell whether they are live-oracle runs, nor "
        "which tests they execute, so it cannot report them as duplicates. "
        "Either use a literal, or teach _shell_values the assignment form -- do "
        "not leave the checker silently ignoring a command it cannot read."
    )


def test_the_build_tag_boundary_is_asserted_not_described() -> None:
    """The one claim this file makes about a mechanism it does not model.

    `TestExplicitQueueMultiReplicaClaimDrainRestart` is named in a `-run` but is
    deliberately outside the oracle population: its invocation sets no oracle
    variable and passes `-tags=integration`, so a plain `./...` sweep does not
    compile it in and cannot double-run it.

    That reasoning was stated in a docstring and nothing enforced it. Drop the
    flag from the invocation, or the tag from the file, and the test runs in
    both places -- via a mechanism this file has explicitly declined to model,
    so nothing else here would notice. lane-ci-flakes pointed out that this is
    the same "stated, not asserted" defect I had just finished fixing one level
    down, and they were right.
    """
    source = CHECK_GO.read_text(encoding="utf-8")
    marker = "TestExplicitQueueMultiReplicaClaimDrainRestart"
    if marker not in source:
        return  # the invocation is gone; nothing to protect

    index = source.index(marker)
    block_start = source.rfind("go test", 0, index)
    assert block_start != -1, f"{marker} is named outside any `go test` command"
    block = source[block_start:index]
    assert "-tags=integration" in block, (
        f"the invocation naming {marker} no longer passes `-tags=integration`. "
        "That tag is the only reason a `./...` sweep does not also run it -- "
        "without it the test executes twice, through a mechanism the duplicate "
        "check above deliberately does not model."
    )

    declaring = [
        path
        for path in (REPO_ROOT / "cmd/dev-health-worker").rglob("*_test.go")
        if re.search(
            rf"^func {marker}\(",
            path.read_text(encoding="utf-8", errors="ignore"),
            re.M,
        )
    ]
    assert declaring, (
        f"{marker} is invoked but not declared under cmd/dev-health-worker"
    )
    for path in declaring:
        head = path.read_text(encoding="utf-8", errors="ignore")[:400]
        assert "//go:build integration" in head or "+build integration" in head, (
            f"{path.relative_to(REPO_ROOT)} declares {marker} but carries no "
            "integration build tag, so a plain `./...` sweep compiles and runs it"
        )


def test_a_skipped_subtest_does_not_prove_the_parent_is_gated() -> None:
    """Round 3's construction, pinned as a permanent test.

    `--- SKIP: TestOracle/unrelated` contains `--- SKIP: TestOracle`, so an
    unanchored search reported the PARENT as gated while it had actually
    PASSED -- meaning it executes for real under the `./...` sweeps.

    The output below is copied from a real `go test -v` run rather than written
    from memory: a hand-made approximation would test my belief about Go's
    format instead of Go's format, which is the failure one level up from the
    one being fixed.
    """
    output = (
        "=== RUN   TestOracle\n"
        "=== RUN   TestOracle/unrelated\n"
        "    oracle_test.go:7: fixture condition\n"
        "--- PASS: TestOracle (0.00s)\n"
        "    --- SKIP: TestOracle/unrelated (0.00s)\n"
        "--- SKIP: TestGated (0.00s)\n"
        "PASS\n"
    )

    assert not _top_level_result(output, "TestOracle", "SKIP"), (
        "a skipped SUBTEST must not prove the parent is gated; the parent PASSED, "
        "so it executes under the ./... sweeps"
    )
    assert _top_level_result(output, "TestOracle", "PASS"), (
        "the parent's own PASS must still be seen, or it would be misreported as "
        "'no result at all' rather than as executed"
    )
    assert _top_level_result(output, "TestGated", "SKIP"), (
        "a genuine top-level skip must still be recognised -- anchoring must not "
        "reject the case the check exists to accept"
    )
    assert "--- SKIP: TestOracle" in output, (
        "the naive predicate must still match here, or this test no longer "
        "demonstrates why the anchoring is needed and someone will remove it"
    )

    # The trailing space does a SECOND job the anchoring alone does not: it
    # blocks PREFIX matches. Without it, a probe for `TestGate` is satisfied by
    # `--- SKIP: TestGated`, so an ungated test would be reported as gated
    # whenever some OTHER test's name extends its own. That is the `26` vs `260`
    # trap in a different alphabet (lane-ci-flakes).
    #
    # Asserted rather than described. Every property this file lost today was
    # one that had been written down and not checked -- the build-tag boundary,
    # the `re.S` claim, the gate itself. A comment here would be removed by
    # whoever next tidies the regex, and nothing would fail.
    assert not _top_level_result(output, "TestGate", "SKIP"), (
        "a probe for `TestGate` must not be satisfied by `--- SKIP: TestGated`; "
        "the trailing space in the pattern is what separates them, and dropping "
        "it silently restores prefix matching"
    )
    assert not _top_level_result(output, "TestOrac", "PASS"), (
        "prefix matching must be blocked for PASS as well, or a parent's result "
        "could be attributed to a shorter-named test"
    )
