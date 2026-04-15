from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.metrics.ff_validation import (
    CheckStatus,
    ValidationReport,
    check_confidence_distribution,
    check_coverage,
    check_dedup,
    check_drift,
    check_join_integrity,
    check_org_isolation,
    check_schema_completeness,
    format_report,
    validate_flag_pipeline,
)


class _FakeQueryResult:
    def __init__(self, column_names: list[str], result_rows: list[list[Any]]):
        self.column_names = column_names
        self.result_rows = result_rows


def _make_client(responses: list[_FakeQueryResult]) -> MagicMock:
    client = MagicMock()
    client.query = MagicMock(side_effect=responses)
    return client


class TestCheckCoverage:
    def test_skip_when_no_releases(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total_releases", "covered_releases"], [[0, 0]]),
            ]
        )
        result = check_coverage(client, "acme")
        assert result.status == CheckStatus.skip

    def test_ok_when_high_coverage(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total_releases", "covered_releases"], [[10, 8]]),
            ]
        )
        result = check_coverage(client, "acme")
        assert result.status == CheckStatus.ok
        assert result.detail[0]["ratio"] == pytest.approx(0.8)

    def test_warn_when_medium_coverage(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total_releases", "covered_releases"], [[10, 6]]),
            ]
        )
        result = check_coverage(client, "acme")
        assert result.status == CheckStatus.warn

    def test_critical_when_low_coverage(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total_releases", "covered_releases"], [[10, 3]]),
            ]
        )
        result = check_coverage(client, "acme")
        assert result.status == CheckStatus.critical


class TestCheckDedup:
    def test_ok_when_no_duplicates(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "distinct_keys"], [[100, 100]]),
                _FakeQueryResult(["total", "distinct_keys"], [[200, 200]]),
            ]
        )
        result = check_dedup(client, "acme")
        assert result.status == CheckStatus.ok

    def test_warn_when_minor_duplicates(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "distinct_keys"], [[100, 98]]),
                _FakeQueryResult(["total", "distinct_keys"], [[200, 200]]),
            ]
        )
        result = check_dedup(client, "acme")
        assert result.status == CheckStatus.warn
        assert len(result.detail) == 1
        assert result.detail[0]["table"] == "feature_flag_event"

    def test_critical_when_heavy_duplicates(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "distinct_keys"], [[100, 90]]),
                _FakeQueryResult(["total", "distinct_keys"], [[200, 200]]),
            ]
        )
        result = check_dedup(client, "acme")
        assert result.status == CheckStatus.critical


class TestCheckSchemaCompleteness:
    def test_skip_when_no_flags(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "complete"], [[0, 0]]),
            ]
        )
        result = check_schema_completeness(client, "acme")
        assert result.status == CheckStatus.skip

    def test_ok_when_all_complete(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "complete"], [[50, 50]]),
            ]
        )
        result = check_schema_completeness(client, "acme")
        assert result.status == CheckStatus.ok

    def test_warn_when_some_incomplete(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "complete"], [[50, 46]]),
            ]
        )
        result = check_schema_completeness(client, "acme")
        assert result.status == CheckStatus.warn

    def test_critical_when_many_incomplete(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "complete"], [[50, 30]]),
            ]
        )
        result = check_schema_completeness(client, "acme")
        assert result.status == CheckStatus.critical


class TestCheckDrift:
    def test_skip_when_insufficient_data(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["day", "bucket_count"], [["2026-03-15", 100]]),
            ]
        )
        result = check_drift(client, "acme")
        assert result.status == CheckStatus.skip

    def test_ok_when_stable(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(
                    ["day", "bucket_count"],
                    [
                        ["2026-03-13", 100],
                        ["2026-03-14", 110],
                        ["2026-03-15", 105],
                    ],
                ),
            ]
        )
        result = check_drift(client, "acme")
        assert result.status == CheckStatus.ok

    def test_warn_when_spike(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(
                    ["day", "bucket_count"],
                    [
                        ["2026-03-13", 100],
                        ["2026-03-14", 100],
                        ["2026-03-15", 300],
                    ],
                ),
            ]
        )
        result = check_drift(client, "acme")
        assert result.status == CheckStatus.warn
        assert len(result.detail) == 1
        assert result.detail[0]["change_ratio"] == 3.0

    def test_warn_when_drop(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(
                    ["day", "bucket_count"],
                    [
                        ["2026-03-13", 200],
                        ["2026-03-14", 200],
                        ["2026-03-15", 50],
                    ],
                ),
            ]
        )
        result = check_drift(client, "acme")
        assert result.status == CheckStatus.warn
        assert result.detail[0]["change_ratio"] == 0.25


class TestCheckOrgIsolation:
    def test_ok_when_single_org(self) -> None:
        empty = _FakeQueryResult(["org_count"], [[0]])
        client = _make_client([empty, empty, empty, empty])
        result = check_org_isolation(client, "acme")
        assert result.status == CheckStatus.ok
        assert not result.detail

    def test_ok_with_info_when_multi_tenant(self) -> None:
        has_others = _FakeQueryResult(["org_count"], [[2]])
        empty = _FakeQueryResult(["org_count"], [[0]])
        client = _make_client([has_others, empty, empty, empty])
        result = check_org_isolation(client, "acme")
        assert result.status == CheckStatus.ok
        assert len(result.detail) == 1


class TestCheckJoinIntegrity:
    def test_skip_when_no_impact_rows(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "matched"], [[0, 0]]),
            ]
        )
        result = check_join_integrity(client, "acme")
        assert result.status == CheckStatus.skip

    def test_ok_when_high_join_rate(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "matched"], [[20, 18]]),
            ]
        )
        result = check_join_integrity(client, "acme")
        assert result.status == CheckStatus.ok

    def test_warn_when_medium_join_rate(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "matched"], [[20, 12]]),
            ]
        )
        result = check_join_integrity(client, "acme")
        assert result.status == CheckStatus.warn

    def test_critical_when_low_join_rate(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["total", "matched"], [[20, 5]]),
            ]
        )
        result = check_join_integrity(client, "acme")
        assert result.status == CheckStatus.critical


class TestCheckConfidenceDistribution:
    def test_skip_when_no_scores(self) -> None:
        client = _make_client(
            [
                _FakeQueryResult(["score"], []),
            ]
        )
        result = check_confidence_distribution(client, "acme")
        assert result.status == CheckStatus.skip

    def test_ok_with_healthy_distribution(self) -> None:
        scores = [[0.7], [0.8], [0.9], [0.6], [0.85]]
        client = _make_client(
            [
                _FakeQueryResult(["score"], scores),
            ]
        )
        result = check_confidence_distribution(client, "acme")
        assert result.status == CheckStatus.ok
        assert len(result.detail) == 5

    def test_warn_when_mostly_very_low(self) -> None:
        scores = [[0.1], [0.05], [0.15], [0.1], [0.8]]
        client = _make_client(
            [
                _FakeQueryResult(["score"], scores),
            ]
        )
        result = check_confidence_distribution(client, "acme")
        assert result.status == CheckStatus.warn


class TestValidationReport:
    def test_has_critical(self) -> None:
        from dev_health_ops.metrics.ff_validation import CheckResult

        report = ValidationReport(
            org_id="acme",
            checks=[
                CheckResult("a", CheckStatus.ok, "fine"),
                CheckResult("b", CheckStatus.critical, "bad"),
            ],
        )
        assert report.has_critical is True
        assert report.has_warnings is False

    def test_has_warnings(self) -> None:
        from dev_health_ops.metrics.ff_validation import CheckResult

        report = ValidationReport(
            org_id="acme",
            checks=[
                CheckResult("a", CheckStatus.ok, "fine"),
                CheckResult("b", CheckStatus.warn, "meh"),
            ],
        )
        assert report.has_critical is False
        assert report.has_warnings is True

    def test_all_ok(self) -> None:
        from dev_health_ops.metrics.ff_validation import CheckResult

        report = ValidationReport(
            org_id="acme",
            checks=[CheckResult("a", CheckStatus.ok, "fine")],
        )
        assert report.has_critical is False
        assert report.has_warnings is False


class TestFormatReport:
    def test_format_includes_all_checks(self) -> None:
        from dev_health_ops.metrics.ff_validation import CheckResult

        report = ValidationReport(
            org_id="acme",
            checks=[
                CheckResult("coverage", CheckStatus.ok, "8/10 (80%)"),
                CheckResult("dedup", CheckStatus.warn, "minor dups"),
            ],
        )
        output = format_report(report)
        assert "coverage" in output
        assert "dedup" in output
        assert "WARNING" in output

    def test_format_critical_result(self) -> None:
        from dev_health_ops.metrics.ff_validation import CheckResult

        report = ValidationReport(
            org_id="acme",
            checks=[CheckResult("join", CheckStatus.critical, "broken")],
        )
        output = format_report(report)
        assert "CRITICAL" in output


@pytest.mark.asyncio
async def test_validate_flag_pipeline_runs_all_checks() -> None:
    responses = [
        _FakeQueryResult(["total_releases", "covered_releases"], [[10, 8]]),
        _FakeQueryResult(["total", "distinct_keys"], [[100, 100]]),
        _FakeQueryResult(["total", "distinct_keys"], [[200, 200]]),
        _FakeQueryResult(["total", "complete"], [[50, 50]]),
        _FakeQueryResult(
            ["day", "bucket_count"],
            [["2026-03-14", 100], ["2026-03-15", 110]],
        ),
        _FakeQueryResult(["org_count"], [[0]]),
        _FakeQueryResult(["org_count"], [[0]]),
        _FakeQueryResult(["org_count"], [[0]]),
        _FakeQueryResult(["org_count"], [[0]]),
        _FakeQueryResult(["total", "matched"], [[20, 18]]),
        _FakeQueryResult(["score"], [[0.7], [0.8], [0.9]]),
    ]
    client = _make_client(responses)
    report = await validate_flag_pipeline(client, "acme", lookback_days=30)
    assert len(report.checks) == 7
    assert not report.has_critical
