from __future__ import annotations

from datetime import date
from importlib import import_module

get_metric_definition = import_module(
    "dev_health_ops.reports.metric_registry"
).get_metric_definition
parse_prompt = import_module("dev_health_ops.reports.parser").parse_prompt


def test_parse_prompt_extracts_weekly_scope_metrics_and_comparison():
    parsed = parse_prompt(
        "Create a weekly report for team platform and repo org/api on flaky tests and cycle time compared to last month by repo",
        today=date(2026, 4, 10),
    )

    assert parsed.report_type == "weekly_health"
    assert parsed.scope.teams == ["platform"]
    assert "org/api" in parsed.scope.repos
    assert parsed.metric_terms == ["cycle time", "flaky tests"]
    assert parsed.group_by == "repo"
    assert parsed.comparison_period == "prior_month"


def test_parse_prompt_handles_quarter_ranges():
    parsed = parse_prompt(
        "Need a quality trend report for service checkout for Q1 2026",
        today=date(2026, 4, 10),
    )

    assert parsed.report_type == "quality_trend"
    assert parsed.time_range_start == date(2026, 1, 1)
    assert parsed.time_range_end == date(2026, 3, 31)


def test_parse_prompt_handles_named_month_without_year():
    parsed = parse_prompt(
        "Monthly review for repo org/web in March",
        today=date(2026, 4, 10),
    )

    assert parsed.time_range_start == date(2026, 3, 1)
    assert parsed.time_range_end == date(2026, 3, 31)


def test_parse_prompt_marks_invalid_explicit_range():
    parsed = parse_prompt(
        "CI report for service checkout from 2026-04-10 to 2026-04-01",
        today=date(2026, 4, 10),
    )

    assert parsed.invalid_reasons == ["invalid_time_range"]


def test_metric_registry_includes_core_and_testops_metrics():
    assert get_metric_definition("cycle_time_p50_hours") is not None
    assert get_metric_definition("flake_rate") is not None
    assert get_metric_definition("avg_queue_seconds") is not None
