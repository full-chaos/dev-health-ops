from __future__ import annotations

import importlib
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

schemas = importlib.import_module("dev_health_ops.metrics.testops_schemas")
charts = importlib.import_module("dev_health_ops.reports.charts")
engine = importlib.import_module("dev_health_ops.reports.engine")
insights_module = importlib.import_module("dev_health_ops.reports.insights")
narrative_module = importlib.import_module("dev_health_ops.reports.narrative")
renderer_module = importlib.import_module("dev_health_ops.reports.renderer")

ChartSpec = schemas.ChartSpec
ReportPlan = schemas.ReportPlan
build_chart_query = charts.build_chart_query
execute_chart = charts.execute_chart
execute_report = engine.execute_report
generate_insights = insights_module.generate_insights
generate_narrative = narrative_module.generate_narrative
render_report_markdown = renderer_module.render_report_markdown


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []
        self.closed = False

    def query_dicts(self, query, params):
        self.calls.append((query, params))
        return self.rows

    def close(self):
        self.closed = True


def _plan() -> ReportPlan:
    return ReportPlan(
        plan_id="plan-1",
        report_type="weekly_health",
        audience="team_lead",
        scope_teams=["team-a"],
        scope_repos=["repo-a"],
        time_range_start=date(2026, 1, 1),
        time_range_end=date(2026, 1, 7),
        comparison_period="prior_week",
        sections=["summary", "quality", "testops"],
        requested_metrics=["success_rate", "line_coverage_pct"],
        requested_charts=["chart-1", "chart-2"],
        created_at=datetime(2026, 1, 8, tzinfo=UTC),
        org_id="org-1",
    )


def _chart_spec(
    metric: str, *, chart_id: str, group_by: str | None = "day"
) -> ChartSpec:
    return ChartSpec(
        chart_id=chart_id,
        plan_id="plan-1",
        chart_type="line" if group_by else "scorecard",
        metric=metric,
        group_by=group_by,
        filter_teams=["team-a"],
        filter_repos=["repo-a"],
        time_range_start=date(2026, 1, 1),
        time_range_end=date(2026, 1, 7),
        title=f"{metric} title",
        org_id="org-1",
    )


def test_build_chart_query_with_day_grouping_and_filters():
    spec = _chart_spec("flake_rate", chart_id="chart-1", group_by="day")
    query, params = build_chart_query(spec)

    assert "FROM testops_test_metrics_daily" in query
    assert "toDate(day) AS x" in query
    assert "avg(flake_rate) AS y" in query
    assert "team_id IN {filter_teams:Array(String)}" in query
    assert "repo_id IN {filter_repos:Array(String)}" in query
    assert params["org_id"] == "org-1"
    assert params["time_range_start"] == date(2026, 1, 1)
    assert params["time_range_end"] == date(2026, 1, 7)


def test_build_chart_query_with_month_grouping_uses_time_bucket():
    spec = _chart_spec("line_coverage_pct", chart_id="chart-2", group_by="month")
    query, _ = build_chart_query(spec)

    assert "toStartOfMonth(day) AS x" in query
    assert "FROM testops_coverage_metrics_daily" in query


@pytest.mark.asyncio
async def test_execute_chart_returns_structured_points():
    client = FakeClient([{"x": date(2026, 1, 1), "y": 0.96, "group_value": None}])
    result = await execute_chart(
        _chart_spec("success_rate", chart_id="chart-1"), client
    )

    assert not result.empty
    assert result.data_points == [{"x": "2026-01-01", "y": 0.96, "group": None}]


def test_generate_insights_creates_trend_regression_and_provenance():
    chart_results = [
        charts.ChartResult(
            spec=_chart_spec("line_coverage_pct", chart_id="chart-2"),
            data_points=[
                {"x": "2026-01-01", "y": 82.0, "group": None},
                {"x": "2026-01-02", "y": 79.0, "group": None},
                {"x": "2026-01-03", "y": 70.0, "group": None},
            ],
            title="coverage",
            empty=False,
        )
    ]

    insights, provenance = generate_insights(_plan(), chart_results)

    assert any(insight.insight_type == "trend_delta" for insight in insights)
    assert any(insight.insight_type == "regression" for insight in insights)
    assert all(record.artifact_type == "insight" for record in provenance)
    assert {record.artifact_id for record in provenance} == {
        insight.insight_id for insight in insights
    }


def test_generate_narrative_stays_grounded_to_available_metrics():
    chart_results = [
        charts.ChartResult(
            spec=_chart_spec("success_rate", chart_id="chart-1"),
            data_points=[
                {"x": "2026-01-01", "y": 0.91, "group": None},
                {"x": "2026-01-07", "y": 0.95, "group": None},
            ],
            title="Success rate",
            empty=False,
        )
    ]
    insights, _ = generate_insights(_plan(), chart_results)

    sections = generate_narrative(_plan(), chart_results, insights)
    summary = next(section for section in sections if section.section_type == "summary")

    assert "Success Rate appears near 95.0%" in summary.body
    assert "line coverage" not in summary.body.lower()
    assert summary.supporting_metrics == ["success_rate"]


def test_render_report_markdown_outputs_expected_sections():
    chart_result = charts.ChartResult(
        spec=_chart_spec("success_rate", chart_id="chart-1"),
        data_points=[{"x": "2026-01-01", "y": 0.95, "group": None}],
        title="Success rate",
        empty=False,
    )
    insights, provenance = generate_insights(_plan(), [chart_result])
    narrative_sections = generate_narrative(_plan(), [chart_result], insights)

    markdown = render_report_markdown(
        plan=_plan(),
        chart_results=[chart_result],
        insights=insights,
        narrative_sections=narrative_sections,
        provenance=provenance,
    )

    assert "# Weekly Health Report" in markdown
    assert "## Summary" in markdown
    assert "### Insights" in markdown
    assert "### Charts" in markdown
    assert "## Provenance" in markdown
    assert "Generated at 2026-01-08T00:00:00+00:00" in markdown


def test_empty_data_handling_produces_empty_chart_and_limited_narrative():
    chart_result = charts.ChartResult(
        spec=_chart_spec("success_rate", chart_id="chart-1"),
        data_points=[],
        title="Success rate",
        empty=True,
    )
    insights, provenance = generate_insights(_plan(), [chart_result])
    sections = generate_narrative(_plan(), [chart_result], insights)
    markdown = render_report_markdown(
        _plan(), [chart_result], insights, sections, provenance
    )

    assert insights == []
    assert provenance == []
    assert "Available evidence appears limited" in markdown
    assert (
        "_No data returned for this chart._" in markdown
        or "No charts linked" in markdown
    )


@pytest.mark.asyncio
async def test_execute_report_creates_provenance_for_all_artifacts(monkeypatch):

    fake_sink = FakeClient(
        [
            {"x": date(2026, 1, 1), "y": 0.91, "group_value": None},
            {"x": date(2026, 1, 7), "y": 0.95, "group_value": None},
        ]
    )

    monkeypatch.setattr(
        "dev_health_ops.reports.engine.ClickHouseMetricsSink",
        lambda dsn: fake_sink,
    )

    report = await execute_report(
        _plan(), [_chart_spec("success_rate", chart_id="chart-1")], "clickhouse://test"
    )

    artifact_types = {record.artifact_type for record in report.provenance}
    assert {"chart", "narrative", "report"}.issubset(artifact_types)
    assert report.chart_results[0].data_points[-1]["y"] == 0.95
    assert report.rendered_markdown.startswith("# Weekly Health Report")
    assert fake_sink.closed
