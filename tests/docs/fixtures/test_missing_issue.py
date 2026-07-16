from __future__ import annotations

from tests.docs.test_coverage_matrix import EXPECTED_ISSUES, coverage_errors


def test_omitted_issue_fixture_reports_chaos_2887() -> None:
    actual_issues = EXPECTED_ISSUES - {"CHAOS-2887"}

    assert coverage_errors(actual_issues) == []
