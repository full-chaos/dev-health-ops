"""CHAOS-3575: assert every case that EXECUTED also RECORDED a result.

``run_report.assert_armed_run_executed`` closes "did anything run". It cannot
close "did what ran actually measure anything", and the difference is not
theoretical -- it produced a two-thirds-degraded run that read as complete.

THE RUN THIS EXISTS BECAUSE OF (2026-08-07, 10:03 PT, armed corpus):

    134 collected, 0 skipped, 90 real corpus cases executed, 67 failed
    receipts on disk: 74  =  31 active + 43 declared-blocked

Fifty-nine of the ninety active cases raised ``HTTP 429 cost_limit_reached``
*before* their recorder ran. They asserted nothing. Every count the harness
already checked was correct and clean: the collected total matched the corpus
exactly, nothing was skipped, and the executed-case floor passed comfortably.
Nothing in the launcher compared "cases that ran" against "cases that wrote a
receipt", so a run that measured a third of the corpus was indistinguishable
from one that measured all of it until the receipts were counted by hand.

Grading those 59 as reds would have invented 59 results the run never produced;
grading the survivors as a pass would have certified the corpus on a third of
its cases. Both readings are wrong in the same way, and this module makes the
run refuse to be read either way.

The rule it enforces: **a case that executed and recorded nothing measured
nothing, and a run containing one is UNMEASURED -- not a result.** This is
rule 4 ("a measurement that did not happen must FAIL, loudly") applied at the
receipt layer, and it is deliberately a HARD assertion rather than a warning,
because the failure it catches is invisible in every other signal.

Deliberately NOT symmetric: a receipt with no executed case is not an error
here. Declared-blocked cases legitimately write receipts from a different test,
and stale receipts from a previous run are a real hazard but a different one
(the launcher clears the report; the caller owns the receipts directory). This
module answers exactly one question, so a failure from it has exactly one
meaning.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from scripts.acceptance.corpus.run_report import (
    CORPUS_CASE_TEST_NAME,
    RunSummary,
    UnmeasurableRunError,
    read_junit_report,
)

__all__ = [
    "EXIT_INCOMPLETE_COVERAGE",
    "IncompleteReceiptCoverageError",
    "ReceiptCoverage",
    "assess_receipt_coverage",
    "executed_case_ids",
    "recorded_case_ids",
    "require_complete_receipt_coverage",
]

#: Exit code for "cases executed without recording a result". Distinct from
#: pytest's 1..5 AND from run_report's 66/67, so a launcher can tell "the
#: corpus went red" from "the corpus did not get measured" from "nothing ran".
EXIT_INCOMPLETE_COVERAGE = 68

#: The receipt statuses that constitute a RECORDED RESULT for an executed case.
#: ``declared-blocked`` is deliberately absent: those receipts come from
#: ``test_declared_blocked_case``, which never executes a case against the
#: stack, so they can never satisfy coverage for an executed one.
RECORDING_STATUSES = frozenset({"passed", "failed"})


class IncompleteReceiptCoverageError(Exception):
    """Raised when an executed case recorded no result."""


@dataclass(frozen=True)
class ReceiptCoverage:
    executed: tuple[str, ...]
    recorded: tuple[str, ...]
    missing: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.missing


def executed_case_ids(summary: RunSummary) -> tuple[str, ...]:
    """The bare case ids of executed corpus cases.

    ``executed_corpus_cases`` yields pytest node names shaped
    ``test_corpus_case[<id>]``. A name that does not carry a bracketed id is
    dropped rather than guessed at -- an id this module invented would be
    reported as missing forever, turning a naming change into a permanent
    false failure.
    """

    ids: list[str] = []
    for name in summary.executed_corpus_cases:
        if not name.startswith(CORPUS_CASE_TEST_NAME):
            continue
        start, end = name.find("["), name.rfind("]")
        if start == -1 or end == -1 or end < start:
            continue
        ids.append(name[start + 1 : end])
    return tuple(sorted(ids))


def recorded_case_ids(receipts_dir: Path) -> tuple[str, ...]:
    """Case ids that wrote a receipt carrying a real result.

    An unreadable or malformed receipt does NOT count as recorded. That is the
    conservative direction on purpose: a receipt whose JSON cannot be parsed is
    not evidence that a case measured anything, and counting it would let a
    truncated write masquerade as coverage.
    """

    recorded: set[str] = set()
    for path in sorted(receipts_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        case_id = payload.get("case_id")
        if payload.get("status") in RECORDING_STATUSES and isinstance(case_id, str):
            recorded.add(case_id)
    return tuple(sorted(recorded))


def assess_receipt_coverage(summary: RunSummary, receipts_dir: Path) -> ReceiptCoverage:
    executed = executed_case_ids(summary)
    recorded = recorded_case_ids(receipts_dir)
    missing = tuple(sorted(set(executed) - set(recorded)))
    return ReceiptCoverage(executed=executed, recorded=recorded, missing=missing)


def require_complete_receipt_coverage(coverage: ReceiptCoverage) -> None:
    if coverage.complete:
        return
    shown = ", ".join(coverage.missing[:20])
    if len(coverage.missing) > 20:
        shown += f", ... and {len(coverage.missing) - 20} more"
    raise IncompleteReceiptCoverageError(
        f"{len(coverage.missing)} of {len(coverage.executed)} executed corpus "
        f"case(s) recorded NO result: {shown}. Those cases asserted nothing, so "
        "this run measured less than it collected and cannot be graded -- "
        "neither as a pass (the corpus was not covered) nor as failures (the "
        "results do not exist). Treat the run as UNMEASURED, find why the "
        "recorder never ran -- an exhausted platform allowance returning HTTP "
        "429 is the known cause -- and re-run."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Assert every executed Wave 4 corpus case also wrote a receipt "
            "recording its result."
        )
    )
    parser.add_argument("report", type=Path, help="path to the pytest JUnit XML")
    parser.add_argument(
        "--receipts-dir",
        type=Path,
        required=True,
        help="directory the run's per-case receipts were written to",
    )
    args = parser.parse_args(argv)

    try:
        summary = read_junit_report(args.report)
    except UnmeasurableRunError as exc:
        print(f"UNMEASURABLE: {exc}", file=sys.stderr)
        return EXIT_INCOMPLETE_COVERAGE

    coverage = assess_receipt_coverage(summary, args.receipts_dir)
    try:
        require_complete_receipt_coverage(coverage)
    except IncompleteReceiptCoverageError as exc:
        print(f"INCOMPLETE RECEIPT COVERAGE: {exc}", file=sys.stderr)
        return EXIT_INCOMPLETE_COVERAGE

    print(
        f"receipt coverage complete: {len(coverage.executed)} executed corpus "
        f"case(s), {len(coverage.executed)} recorded a result"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
