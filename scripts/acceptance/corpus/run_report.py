"""CHAOS-3462 B1: the positive "this armed run actually executed" assertion.

The arming guard (``arming.py``) closes one half of the false green: an
armed run can no longer be silently downgraded to a skip by the CHAOS-3402
env scrub. It cannot close the other half. It runs once, at session start,
and has no idea how many cases eventually ran -- a run can still report
``exit 0`` having executed nothing, because a ``-k`` expression matched no
ids, a collection filter emptied the corpus directory, or every case skipped
for some reason invented after this code was written.

So the launcher asserts it from OUTSIDE the session, off the run's own JUnit
XML. This is the "positively assert non-zero collected+executed tests for
this file specifically" that ``test_wave4_corpus_runner_live.py``'s module
docstring names as Phase 5c's wiring; CHAOS-3462 pulls it forward because
the Phase 2 exit run needs it now.

WHAT "EXECUTED" DOES AND DOES NOT MEAN, precisely: a case that ERRORS IN
SETUP counts as executed here, because it has no ``<skipped>`` element. That
is deliberate and pinned by a test against real pytest output, but it means
this check answers "was anything other than a silent skip reported for a
corpus case", NOT "did a case reach the product". The distinction cannot
produce a false green -- a setup-errored run is non-zero from pytest and the
launcher propagates that status -- but do not read a passing
``assert_armed_run_executed`` as evidence the stack was exercised. The
receipts are that evidence.

RULE 4 THROUGHOUT ("a measurement that did not happen must FAIL, loudly"):
every function here raises :class:`UnmeasurableRunError` rather than
returning a zeroed summary when the report is absent, empty, or
unparseable. A pytest that crashed before writing its XML produces exactly
that shape, and a parser that quietly reported "0 failures" for it would
turn a crash into a pass.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

__all__ = [
    "ArmedRunNotExecutedError",
    "RunSummary",
    "UnmeasurableRunError",
    "assert_armed_run_executed",
    "parse_junit_xml",
    "read_junit_report",
]

_COUNTER_ATTRS = ("tests", "failures", "errors", "skipped")

#: Exit code for "the run executed nothing". Distinct from pytest's own
#: 1..5 so a launcher's failure is never mistaken for a test failure.
EXIT_NOT_EXECUTED = 66
#: Exit code for "there was nothing to measure" -- see rule 4.
EXIT_UNMEASURABLE = 67


class UnmeasurableRunError(Exception):
    """The run produced no report this module can read.

    Never downgraded to a zeroed summary: "I could not measure" and "I
    measured zero problems" are different claims, and conflating them is
    how an absent measurement starts reading as coverage.
    """


class ArmedRunNotExecutedError(Exception):
    """An armed run finished without executing any case."""


#: The parametrized test that drives ONE real corpus case. Counting any
#: non-skipped testcase is not good enough (Codex adversarial round-1, HIGH,
#: reproduced): with ``-k test_at_least_one_corpus_case_is_collected`` the
#: run executes only the collection guard -- tests=1, skipped=0 -- and a
#: bare executed-count assertion passes while ZERO product cases ran.
#: ``test_declared_blocked_case`` has the same shape: it executes, touches
#: nothing, and would satisfy a naive count.
CORPUS_CASE_TEST_NAME = "test_corpus_case"


@dataclass(frozen=True, slots=True)
class RunSummary:
    tests: int
    failures: int
    errors: int
    skipped: int
    #: Names of the ``<testcase>`` elements that actually RAN (no
    #: ``<skipped>`` child), e.g. ``test_corpus_case[scope.ambiguous]``.
    executed_names: tuple[str, ...] = ()

    @property
    def executed(self) -> int:
        """Cases that actually ran (passed, failed, or errored).

        Skips are subtracted because a skip is precisely the outcome that
        proves nothing ran -- it is the shape the whole B1 false green took
        (``144 skipped``, exit 0).
        """

        return self.tests - self.skipped

    @property
    def executed_corpus_cases(self) -> tuple[str, ...]:
        """The executed testcases that are real corpus cases."""

        return tuple(
            name
            for name in self.executed_names
            if name.startswith(CORPUS_CASE_TEST_NAME)
        )


def _int_attr(element: ElementTree.Element, name: str) -> int:
    raw = element.get(name)
    if raw is None:
        raise UnmeasurableRunError(
            f"<{element.tag}> has no {name!r} attribute -- this is not a "
            "pytest JUnit report, or it was truncated mid-write; refusing "
            "to infer a count that was never reported"
        )
    try:
        return int(raw)
    except ValueError as exc:
        raise UnmeasurableRunError(
            f"<{element.tag}> {name}={raw!r} is not an integer"
        ) from exc


def parse_junit_xml(text: str) -> RunSummary:
    """Sum the counters across every ``<testsuite>`` in a JUnit document.

    Accepts either a ``<testsuites>`` wrapper (what pytest writes) or a
    bare ``<testsuite>`` root (what some older/other writers produce).
    """

    if not text.strip():
        raise UnmeasurableRunError(
            "the JUnit report is empty -- pytest produced no report at all, "
            "which almost always means it died before the session finished"
        )
    try:
        root = ElementTree.fromstring(text)  # noqa: S314 - our own pytest output
    except ElementTree.ParseError as exc:
        raise UnmeasurableRunError(f"the JUnit report is not valid XML: {exc}") from exc

    if root.tag == "testsuite":
        suites = [root]
    elif root.tag == "testsuites":
        suites = list(root.iter("testsuite"))
    else:
        raise UnmeasurableRunError(
            f"unexpected JUnit root element <{root.tag}> -- expected "
            "<testsuites> or <testsuite>"
        )
    if not suites:
        raise UnmeasurableRunError(
            "the JUnit report contains no <testsuite> element -- nothing to measure"
        )

    totals = {name: 0 for name in _COUNTER_ATTRS}
    executed_names: list[str] = []
    for suite in suites:
        for name in _COUNTER_ATTRS:
            totals[name] += _int_attr(suite, name)
        for case in suite.iter("testcase"):
            # A skipped testcase carries a <skipped> child; anything else
            # (pass, failure, error) actually ran.
            if case.find("skipped") is not None:
                continue
            case_name = case.get("name")
            if case_name:
                executed_names.append(case_name)
    return RunSummary(**totals, executed_names=tuple(executed_names))


def read_junit_report(path: Path) -> RunSummary:
    """Read and parse a JUnit report written to ``path``.

    An absent file is the single most likely real-world shape of "the
    measurement did not happen" -- pytest died, or was never invoked -- so
    it raises rather than returning anything.
    """

    if not path.exists():
        raise UnmeasurableRunError(
            f"the JUnit report {path} does not exist -- the corpus run never "
            "produced one, so there is no evidence it executed anything"
        )
    return parse_junit_xml(path.read_text(encoding="utf-8"))


def assert_armed_run_executed(summary: RunSummary, *, min_executed: int = 1) -> None:
    """Raise unless an ARMED run executed at least ``min_executed`` real
    corpus cases.

    Counts ``test_corpus_case[...]`` specifically, not "any non-skipped
    testcase" (Codex adversarial round-1, HIGH, reproduced): the module's
    other tests -- the collection guard and the declared-blocked receipts --
    execute without touching the stack, so a run filtered down to them alone
    would satisfy a naive count while proving nothing about the product.

    Deliberately says nothing about whether those cases PASSED. A red run is
    already loud on its own; re-flagging it here would blur two distinct
    signals ("nothing ran" vs "things ran and failed"), and the first is the
    one no other check in this stack can see.
    """

    executed_cases = summary.executed_corpus_cases
    if len(executed_cases) >= min_executed:
        return
    raise ArmedRunNotExecutedError(
        f"ARMED corpus run executed {len(executed_cases)} real corpus "
        f"case(s) -- at least {min_executed} must execute. The run reported "
        f"{summary.tests} collected / {summary.executed} executed / "
        f"{summary.skipped} skipped / {summary.failures} failed / "
        f"{summary.errors} errored, but only testcases named "
        f"{CORPUS_CASE_TEST_NAME}[...] count: the collection guard and the "
        "declared-blocked receipts execute without touching the stack. An "
        "armed run that executes no product case reports exit 0 and reads "
        "as a pass; that is the CHAOS-3462 B1 false green. Likely causes: "
        "the CHAOS-3402 env scrub removed the arming variables (check that "
        "DEV_HEALTH_TEST_ENV_ALLOW was exported -- "
        "scripts/acceptance/run_wave4_corpus.sh does this, and the pytest "
        "header echoes every scrubbed and exempted name), or a -k/--deselect "
        "filter narrowed the run away from the corpus."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Assert an armed Wave 4 corpus run executed a non-zero number of "
            "cases, reading the run's own JUnit XML."
        )
    )
    parser.add_argument("report", type=Path, help="path to the pytest JUnit XML")
    parser.add_argument(
        "--min-executed",
        type=int,
        default=1,
        help="minimum number of executed cases required (default: 1)",
    )
    args = parser.parse_args(argv)

    try:
        summary = read_junit_report(args.report)
    except UnmeasurableRunError as exc:
        print(f"UNMEASURABLE: {exc}", file=sys.stderr)
        return EXIT_UNMEASURABLE
    try:
        assert_armed_run_executed(summary, min_executed=args.min_executed)
    except ArmedRunNotExecutedError as exc:
        print(f"NOT EXECUTED: {exc}", file=sys.stderr)
        return EXIT_NOT_EXECUTED
    # Both counters, explicitly labelled and never both called "case(s)".
    # Found by the phase5-ci lane comparing two CI runs (2026-08-07): this
    # success line used to print `summary.executed`, which counts EVERY
    # non-skipped testcase (real cases + declared-blocked receipts + the
    # collection guard), while the failure path's message prints
    # `len(executed_corpus_cases)`, which counts real corpus cases only.
    # Same word, two denominators: comparing a failed run's line to a
    # successful one read as the corpus gaining 53 cases when nothing had
    # changed, and very nearly got a correct prediction filed as a wrong one.
    # A harness whose whole job is answering "did this run measure anything"
    # must not be ambiguous about what it counted.
    print(
        f"armed run executed {len(summary.executed_corpus_cases)} real corpus "
        f"case(s) of {summary.executed} executed testcase(s) "
        f"(collected {summary.tests}, skipped {summary.skipped}, "
        f"failed {summary.failures}, errored {summary.errors})"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
