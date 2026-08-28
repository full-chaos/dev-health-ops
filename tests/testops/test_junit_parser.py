from __future__ import annotations

# pyright: reportMissingImports=false
from pathlib import Path
from uuid import uuid4

import pytest

from dev_health_ops.parsers.junit import (
    parse_junit_xml,
)
from dev_health_ops.processors.testops_tests import (
    build_case_id,
    process_test_report,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_parse_junit_xml_extracts_suites_and_cases() -> None:
    suites = parse_junit_xml(FIXTURES_DIR / "sample_junit.xml")

    assert len(suites) == 2
    assert suites[0].suite_name == "services.api.tests.test_api"
    assert suites[0].framework == "pytest"
    assert suites[0].passed_count == 1
    assert suites[0].failed_count == 1
    assert suites[0].skipped_count == 1
    assert suites[0].quarantined_count == 1
    assert suites[0].system_out == "suite stdout"
    assert suites[0].system_err == "suite stderr"

    failed_case = next(case for case in suites[0].cases if case.status == "failed")
    assert failed_case.failure_message == "assertion failed"
    assert failed_case.failure_type == "AssertionError"
    assert "Expected 200" in (failed_case.stack_trace or "")

    quarantined_case = next(
        case for case in suites[0].cases if case.status == "quarantined"
    )
    assert quarantined_case.failure_message == "quarantined on ci"

    error_case = next(case for case in suites[1].cases if case.status == "error")
    assert error_case.failure_type == "TypeError"
    assert error_case.system_err == "boom stderr"


@pytest.mark.asyncio
async def test_process_test_report_maps_rows_and_service_attribution() -> None:
    repo_id = uuid4()
    suite_rows, case_rows = await process_test_report(
        repo_id=repo_id,
        run_id="run-123",
        source=(FIXTURES_DIR / "sample_junit.xml").read_text(encoding="utf-8"),
        environment="linux-x64",
        org_id="chaos",
        service_path_prefixes={
            "services/api": "api-service",
            "packages/web": "web-service",
        },
    )

    assert len(suite_rows) == 2
    assert len(case_rows) == 5

    first_suite = suite_rows[0]
    assert first_suite["repo_id"] == repo_id
    assert first_suite["service_id"] == "api-service"
    assert first_suite["suite_id"]

    failed_case = next(case for case in case_rows if case["status"] == "failed")
    assert failed_case["org_id"] == "chaos"
    assert failed_case["case_id"]
    assert "Expected 200" in (failed_case["stack_trace"] or "")


def test_build_case_id_disambiguates_by_occurrence() -> None:
    """CHAOS-4392: the Go cicd/tests route hit ErrInvalidConfiguration
    ("duplicate natural key in test_case_results batch") when a suite
    contained two identically named test cases, because case_id hashed only
    (suite_id, case_name). Python shares this exact hash shape
    (_hash_identifier(suite_id, case_name)), so the same collision existed
    here too -- silently, since nothing in the Python insert path rejects a
    duplicate case_id (ReplacingMergeTree FINAL-dedups instead of erroring).
    """
    first = build_case_id("suite-1", "flaky")
    second = build_case_id("suite-1", "flaky")
    assert first == second, "same inputs must still hash identically with occurrence=0"

    disambiguated = build_case_id("suite-1", "flaky", occurrence=1)
    assert disambiguated != first, "a duplicate occurrence must get a distinct case_id"
    # occurrence=0 (the default) must be byte-identical to the pre-fix hash so
    # every non-colliding case_id already persisted keeps its exact value.
    assert build_case_id("suite-1", "flaky", occurrence=0) == first


@pytest.mark.asyncio
async def test_process_test_report_disambiguates_within_suite_duplicate_case_names() -> (
    None
):
    """CHAOS-4392 regression: two <testcase> elements sharing a name inside
    one suite must both be retained with distinct case_id values, matching
    the fix applied to the Go cicd/tests route (github_tests_reports.go's
    caseOccurrence)."""
    xml = (
        '<testsuite name="matrix">'
        '<testcase name="flaky" classname="pkg.TestA"/>'
        '<testcase name="flaky" classname="pkg.TestB">'
        '<failure message="boom" type="AssertionError">trace</failure>'
        "</testcase>"
        "</testsuite>"
    )
    _, case_rows = await process_test_report(
        repo_id=uuid4(), run_id="run-4392", source=xml, org_id="chaos"
    )

    assert len(case_rows) == 2, (
        "both duplicate-named cases must be retained, not folded or dropped"
    )
    case_ids = {case["case_id"] for case in case_rows}
    assert len(case_ids) == 2, "duplicate-named cases must get distinct case_id values"
    assert all(case["case_name"] == "flaky" for case in case_rows)
