"""CHAOS-3575: tests for the receipt-coverage assertion.

The central test here is ``test_reproduces_the_degraded_run_that_read_as_clean``
-- it rebuilds the exact shape of the 2026-08-07 10:03 run (every existing
count clean, two thirds of the cases having recorded nothing) and asserts the
new guard rejects it. Without that case the module is only asserted against
inputs invented to suit it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acceptance.corpus.receipt_coverage import (
    EXIT_INCOMPLETE_COVERAGE,
    IncompleteReceiptCoverageError,
    assess_receipt_coverage,
    executed_case_ids,
    main,
    recorded_case_ids,
    require_complete_receipt_coverage,
)
from scripts.acceptance.corpus.run_report import parse_junit_xml


def _junit(case_ids: list[str], *, skipped: list[str] | None = None) -> str:
    skipped = skipped or []
    body = "".join(
        f'<testcase name="test_corpus_case[{cid}]">'
        + ("<skipped/>" if cid in skipped else "")
        + "</testcase>"
        for cid in case_ids
    )
    return (
        '<?xml version="1.0"?><testsuites><testsuite name="pytest" '
        f'tests="{len(case_ids)}" failures="0" errors="0" '
        f'skipped="{len(skipped)}">{body}</testsuite></testsuites>'
    )


def _receipt(tmp: Path, case_id: str, status: str = "passed") -> None:
    (tmp / f"{case_id}.json").write_text(
        json.dumps({"case_id": case_id, "status": status}), encoding="utf-8"
    )


def test_complete_coverage_passes(tmp_path: Path) -> None:
    summary = parse_junit_xml(_junit(["a", "b"]))
    _receipt(tmp_path, "a")
    _receipt(tmp_path, "b", status="failed")

    coverage = assess_receipt_coverage(summary, tmp_path)

    assert coverage.complete
    assert coverage.missing == ()
    require_complete_receipt_coverage(coverage)  # must not raise


def test_reproduces_the_degraded_run_that_read_as_clean(tmp_path: Path) -> None:
    """The 2026-08-07 10:03 run: clean counts, two thirds unrecorded.

    Ninety executed cases, thirty-one of which recorded a result. Nothing is
    skipped and the collected total is correct, so every pre-existing check
    passes -- which is precisely why this one has to fail.
    """

    case_ids = [f"case.{i:03d}" for i in range(90)]
    summary = parse_junit_xml(_junit(case_ids))
    for cid in case_ids[:31]:
        _receipt(tmp_path, cid)

    coverage = assess_receipt_coverage(summary, tmp_path)

    assert summary.skipped == 0, "the degraded run skipped nothing -- that was the trap"
    assert len(summary.executed_corpus_cases) == 90
    assert len(coverage.missing) == 59
    with pytest.raises(IncompleteReceiptCoverageError) as excinfo:
        require_complete_receipt_coverage(coverage)
    assert "59 of 90" in str(excinfo.value)
    assert "UNMEASURED" in str(excinfo.value)


def test_declared_blocked_receipt_does_not_satisfy_an_executed_case(
    tmp_path: Path,
) -> None:
    """A declared-blocked receipt is not a measurement of an executed case."""

    summary = parse_junit_xml(_junit(["a"]))
    _receipt(tmp_path, "a", status="declared-blocked")

    coverage = assess_receipt_coverage(summary, tmp_path)

    assert coverage.missing == ("a",)


def test_malformed_receipt_does_not_count_as_recorded(tmp_path: Path) -> None:
    """A truncated write must not masquerade as coverage."""

    summary = parse_junit_xml(_junit(["a"]))
    (tmp_path / "a.json").write_text(
        '{"case_id": "a", "status": "pas', encoding="utf-8"
    )

    coverage = assess_receipt_coverage(summary, tmp_path)

    assert coverage.missing == ("a",)


def test_skipped_cases_are_not_required_to_record(tmp_path: Path) -> None:
    """A skipped case never executed, so it owes no receipt.

    Requiring one would make every ``-k`` run fail, and a guard that fires on
    correct usage gets disabled.
    """

    summary = parse_junit_xml(_junit(["a", "b"], skipped=["b"]))
    _receipt(tmp_path, "a")

    coverage = assess_receipt_coverage(summary, tmp_path)

    assert coverage.complete


def test_extra_receipts_are_not_an_error(tmp_path: Path) -> None:
    """Coverage is one-directional; a receipt with no executed case is a
    different question this module deliberately does not answer."""

    summary = parse_junit_xml(_junit(["a"]))
    _receipt(tmp_path, "a")
    _receipt(tmp_path, "leftover-from-a-previous-run")

    assert assess_receipt_coverage(summary, tmp_path).complete


def test_executed_case_ids_ignores_unbracketed_names() -> None:
    summary = parse_junit_xml(
        '<?xml version="1.0"?><testsuites><testsuite name="pytest" tests="2" '
        'failures="0" errors="0" skipped="0">'
        '<testcase name="test_corpus_case[real.case]"></testcase>'
        '<testcase name="test_corpus_case"></testcase>'
        "</testsuite></testsuites>"
    )

    assert executed_case_ids(summary) == ("real.case",)


def test_recorded_case_ids_ignores_non_object_payloads(tmp_path: Path) -> None:
    (tmp_path / "list.json").write_text("[1, 2, 3]", encoding="utf-8")
    _receipt(tmp_path, "a")

    assert recorded_case_ids(tmp_path) == ("a",)


def test_cli_exit_code_is_distinct_from_pytest_and_run_report(tmp_path: Path) -> None:
    """The launcher must be able to tell 'did not measure' from 'went red'."""

    report = tmp_path / "junit.xml"
    report.write_text(_junit(["a"]), encoding="utf-8")
    receipts = tmp_path / "receipts"
    receipts.mkdir()

    code = main([str(report), "--receipts-dir", str(receipts)])

    assert code == EXIT_INCOMPLETE_COVERAGE
    assert code not in {0, 1, 2, 3, 4, 5}, "must not collide with pytest's codes"
    assert code not in {66, 67}, "must not collide with run_report's codes"


def test_cli_passes_on_complete_coverage(tmp_path: Path) -> None:
    report = tmp_path / "junit.xml"
    report.write_text(_junit(["a"]), encoding="utf-8")
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    _receipt(receipts, "a")

    assert main([str(report), "--receipts-dir", str(receipts)]) == 0


def test_cli_treats_a_missing_report_as_a_failure(tmp_path: Path) -> None:
    """Rule 4: a measurement that did not happen must fail, loudly."""

    receipts = tmp_path / "receipts"
    receipts.mkdir()

    code = main([str(tmp_path / "absent.xml"), "--receipts-dir", str(receipts)])

    assert code == EXIT_INCOMPLETE_COVERAGE
