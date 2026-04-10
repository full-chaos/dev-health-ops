from __future__ import annotations

import logging
from datetime import UTC, date, datetime

from dev_health_ops.metrics.testops_schemas import ChartSpec, InsightBlock
from dev_health_ops.reports.audit import ReportAuditRecord, log_report_audit
from dev_health_ops.reports.charts import ChartResult
from dev_health_ops.reports.confidence import (
    ConfidenceLevel,
    assess_confidence,
    filter_by_confidence,
)
from dev_health_ops.reports.narrative import NarrativeSection
from dev_health_ops.reports.provenance import (
    validate_insight_provenance,
    validate_narrative_provenance,
)
from dev_health_ops.reports.unsupported import (
    check_metric_availability,
    generate_availability_notice,
)

PLAN_ID = "plan-001"
NOW = datetime(2025, 4, 10, 12, 0, 0, tzinfo=UTC)


def _chart_spec(metric: str) -> ChartSpec:
    return ChartSpec(
        chart_id=f"chart-{metric}",
        plan_id=PLAN_ID,
        chart_type="timeseries",
        metric=metric,
        group_by="day",
        filter_teams=[],
        filter_repos=[],
        time_range_start=date(2025, 4, 1),
        time_range_end=date(2025, 4, 10),
        title=f"{metric} chart",
        org_id="org-1",
    )


def _chart_result(metric: str, empty: bool = False) -> ChartResult:
    return ChartResult(
        spec=_chart_spec(metric),
        data_points=[] if empty else [{"x": "2025-04-10", "y": 0.95, "group": None}],
        title=f"{metric} chart",
        empty=empty,
    )


def _insight(metric: str, insight_type: str = "trend_delta") -> InsightBlock:
    return InsightBlock(
        insight_id=f"ins-{metric}",
        plan_id=PLAN_ID,
        insight_type=insight_type,
        confidence="direct_fact",
        summary=f"{metric} appears to have changed.",
        supporting_metrics=[metric],
        supporting_values={metric: 0.95},
        severity="medium",
    )


def test_narrative_provenance_valid():
    charts = [_chart_result("success_rate"), _chart_result("flake_rate")]
    sections = [
        NarrativeSection(
            section_type="testops",
            title="TestOps",
            body="Pipeline success rate appears stable.",
            supporting_metrics=["success_rate"],
        )
    ]
    result = validate_narrative_provenance(sections, charts)
    assert result.valid
    assert result.violations == []


def test_narrative_provenance_catches_ungrounded():
    charts = [_chart_result("success_rate")]
    sections = [
        NarrativeSection(
            section_type="quality",
            title="Quality",
            body="Coverage appears lower.",
            supporting_metrics=["line_coverage_pct"],
        )
    ]
    result = validate_narrative_provenance(sections, charts)
    assert not result.valid
    assert len(result.violations) == 1
    assert "line_coverage_pct" in result.violations[0]


def test_insight_provenance_catches_missing_metric():
    charts = [_chart_result("success_rate")]
    insights = [_insight("nonexistent_metric")]
    result = validate_insight_provenance(insights, charts)
    assert not result.valid
    assert len(result.violations) == 1


def test_confidence_direct_fact():
    insight = _insight("success_rate", "trend_delta")
    level = assess_confidence(insight, data_points_count=1)
    assert level == ConfidenceLevel.DIRECT_FACT


def test_confidence_inferred():
    insight = _insight("success_rate", "trend_delta")
    level = assess_confidence(insight, data_points_count=5)
    assert level == ConfidenceLevel.INFERRED


def test_confidence_hypothesis():
    insight = _insight("success_rate", "correlation")
    level = assess_confidence(insight, data_points_count=10)
    assert level == ConfidenceLevel.HYPOTHESIS


def test_filter_by_confidence_direct_fact_threshold():
    insights = [
        _insight("success_rate", "trend_delta"),
        _insight("flake_rate", "correlation"),
    ]
    filtered = filter_by_confidence(
        insights,
        threshold="direct_fact",
        data_points_counts={"success_rate": 1, "flake_rate": 10},
    )
    assert len(filtered) == 1
    assert filtered[0].supporting_metrics[0] == "success_rate"


def test_filter_by_confidence_inferred_threshold():
    insights = [
        _insight("success_rate", "trend_delta"),
        _insight("flake_rate", "correlation"),
        _insight("pass_rate", "regression"),
    ]
    filtered = filter_by_confidence(
        insights,
        threshold="inferred",
        data_points_counts={"success_rate": 5, "flake_rate": 10, "pass_rate": 4},
    )
    assert len(filtered) == 2
    metrics = {i.supporting_metrics[0] for i in filtered}
    assert "success_rate" in metrics
    assert "pass_rate" in metrics


def test_metric_availability_mixed():
    charts = [_chart_result("success_rate"), _chart_result("flake_rate", empty=True)]
    result = check_metric_availability(
        ["success_rate", "flake_rate", "nonexistent"],
        charts,
    )
    assert result[0].available is True
    assert result[1].available is False
    assert result[1].reason == "time_range_empty"
    assert result[2].available is False
    assert result[2].reason == "not_computed"


def test_availability_notice_format():
    from dev_health_ops.reports.unsupported import MetricAvailability

    unavailable = [
        MetricAvailability(
            metric="flake_rate", available=False, reason="time_range_empty"
        ),
    ]
    notice = generate_availability_notice(unavailable)
    assert "flake_rate" in notice
    assert "no data in selected time range" in notice


def test_availability_notice_empty():
    assert generate_availability_notice([]) == ""


def test_audit_record_logs(caplog):
    record = ReportAuditRecord(
        audit_id="aud-1",
        plan_id="plan-1",
        org_id="org-1",
        report_type="weekly_health",
        metrics_requested=["success_rate", "flake_rate"],
        metrics_available=["success_rate"],
        metrics_unavailable=["flake_rate"],
        insights_generated=5,
        insights_filtered=2,
        provenance_violations=0,
        confidence_threshold="direct_fact",
        generated_at=NOW,
        duration_seconds=1.5,
    )
    with caplog.at_level(logging.INFO, logger="dev_health_ops.reports.audit"):
        log_report_audit(record)
    assert "plan_id=plan-1" in caplog.text
    assert "metrics_req=2" in caplog.text
