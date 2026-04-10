from __future__ import annotations

from datetime import date
from importlib import import_module

build_report_plan = import_module("dev_health_ops.reports.planner").build_report_plan
resolver = import_module("dev_health_ops.reports.resolver")
EntityCatalog = resolver.EntityCatalog
EntityDefinition = resolver.EntityDefinition

CATALOG = EntityCatalog(
    teams=(EntityDefinition("team-platform", "platform", "team"),),
    repos=(
        EntityDefinition("repo-api", "org/api", "repo", aliases=("api",)),
        EntityDefinition("repo-web", "org/web", "repo", aliases=("web",)),
    ),
    services=(EntityDefinition("svc-checkout", "checkout", "service"),),
)


def test_build_report_plan_uses_template_and_resolves_entities_and_metrics():
    result = build_report_plan(
        "Create a weekly report for team platform and repo org/api on flaky tests and cycle time compared to last month by repo",
        org_id="org-1",
        entity_catalog=CATALOG,
        today=date(2026, 4, 10),
    )

    assert result.ok is True
    assert result.report_plan is not None
    assert result.report_plan.report_type == "weekly_health"
    assert result.report_plan.scope_teams == ["team-platform"]
    assert result.report_plan.scope_repos == ["repo-api"]
    assert result.report_plan.comparison_period == "prior_month"
    assert "cycle_time_p50_hours" in result.report_plan.requested_metrics
    assert "flake_rate" in result.report_plan.requested_metrics
    assert result.chart_specs
    assert all(chart.group_by == "repo" for chart in result.chart_specs)


def test_build_report_plan_rejects_unsupported_metric_requests():
    result = build_report_plan(
        "Create a weekly report for team platform on blast radius",
        org_id="org-1",
        entity_catalog=CATALOG,
        today=date(2026, 4, 10),
    )

    assert result.ok is False
    assert [error.code for error in result.validation.errors] == ["unsupported_metric"]


def test_build_report_plan_rejects_empty_scope():
    result = build_report_plan(
        "Create a monthly report on cycle time for leadership",
        org_id="org-1",
        entity_catalog=CATALOG,
        today=date(2026, 4, 10),
    )

    assert result.ok is False
    assert [error.code for error in result.validation.errors] == ["empty_scope"]


def test_build_report_plan_rejects_invalid_time_range():
    result = build_report_plan(
        "Create a CI report for service checkout from 2026-04-10 to 2026-04-01",
        org_id="org-1",
        entity_catalog=CATALOG,
        today=date(2026, 4, 10),
    )

    assert result.ok is False
    assert [error.code for error in result.validation.errors] == ["invalid_time_range"]


def test_build_report_plan_selects_ci_stability_template_defaults():
    result = build_report_plan(
        "Create a CI stability report for service checkout over the past 30 days",
        org_id="org-1",
        entity_catalog=CATALOG,
        today=date(2026, 4, 10),
    )

    assert result.ok is True
    assert result.report_plan is not None
    assert result.report_plan.report_type == "ci_stability"
    assert result.report_plan.scope_services == ["svc-checkout"]
    assert "success_rate" in result.report_plan.requested_metrics
    assert "median_duration_seconds" in result.report_plan.requested_metrics
