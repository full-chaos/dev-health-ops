"""Canonical metric registry for deterministic report planning."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields

from dev_health_ops.metrics import schemas as metric_schemas
from dev_health_ops.metrics import testops_schemas

DIMENSION_FIELD_NAMES = {
    "repo_id",
    "day",
    "computed_at",
    "org_id",
    "team_id",
    "team_name",
    "service_id",
    "provider",
    "work_scope_id",
    "identity_id",
    "user_identity",
    "as_of_day",
    "author_email",
    "status",
    "type",
    "path",
    "file_path",
    "ref",
    "language",
    "metric_name",
}

UNIT_OVERRIDES = {
    "active_hours": "hours",
    "avg_commit_size_loc": "loc",
    "avg_queue_minutes": "minutes",
    "avg_queue_seconds": "seconds",
    "avg_duration_minutes": "minutes",
    "blame_concentration": "ratio",
    "branch_coverage_pct": "percent",
    "change_failure_rate": "ratio",
    "code_ownership_gini": "ratio",
    "coverage_delta_pct": "percent",
    "cycle_time_p50_hours": "hours",
    "cycle_time_p90_hours": "hours",
    "defect_intro_rate": "ratio",
    "delivery_units": "count",
    "deploy_time_p50_hours": "hours",
    "failure_rate": "ratio",
    "failure_recurrence_score": "score",
    "flake_rate": "ratio",
    "flow_efficiency": "ratio",
    "lead_time_p50_hours": "hours",
    "lead_time_p90_hours": "hours",
    "line_coverage_pct": "percent",
    "loc_added": "loc",
    "loc_deleted": "loc",
    "loc_total": "loc",
    "loc_touched": "loc",
    "median_duration_seconds": "seconds",
    "median_pr_cycle_hours": "hours",
    "mttr_hours": "hours",
    "mttr_p50_hours": "hours",
    "mttr_p90_hours": "hours",
    "p90_duration_minutes": "minutes",
    "p95_duration_seconds": "seconds",
    "p95_queue_seconds": "seconds",
    "pass_rate": "ratio",
    "pr_comments_per_100_loc": "count_per_100_loc",
    "pr_cycle_p75_hours": "hours",
    "pr_cycle_p90_hours": "hours",
    "pr_first_review_p50_hours": "hours",
    "pr_first_review_p90_hours": "hours",
    "pr_pickup_time_p50_hours": "hours",
    "pr_review_time_p50_hours": "hours",
    "pr_reviews_per_100_loc": "count_per_100_loc",
    "predictability_score": "score",
    "rerun_rate": "ratio",
    "review_load_top_reviewer_ratio": "ratio",
    "review_reciprocity": "ratio",
    "rework_churn_ratio_30d": "ratio",
    "retry_dependency_rate": "ratio",
    "risk_score": "score",
    "single_owner_file_ratio_30d": "ratio",
    "story_points_completed": "story_points",
    "success_rate": "ratio",
    "suite_duration_p50_seconds": "seconds",
    "suite_duration_p95_seconds": "seconds",
    "value": "unitless",
    "weekend_commit_ratio": "ratio",
    "wip_age_p50_hours": "hours",
    "wip_age_p90_hours": "hours",
    "wip_congestion_ratio": "ratio",
}

DESCRIPTION_OVERRIDES = {
    "after_hours_commit_ratio": "Share of team commits made outside normal working hours.",
    "avg_queue_seconds": "Average time pipeline runs spent waiting before execution.",
    "change_failure_rate": "Share of changes that led to failed deployments or incidents.",
    "coverage_delta_pct": "Change in line coverage versus the prior coverage snapshot.",
    "cycle_time_p50_hours": "Median time from work start to completion.",
    "cycle_time_p90_hours": "90th percentile time from work start to completion.",
    "failure_recurrence_score": "Share of failures that repeated from prior runs.",
    "flake_rate": "Share of test cases that flipped outcome within the same run window.",
    "lead_time_p50_hours": "Median time from work creation to completion.",
    "line_coverage_pct": "Percentage of lines covered by automated tests.",
    "median_duration_seconds": "Median duration of pipeline runs.",
    "median_pr_cycle_hours": "Median pull request cycle time in hours.",
    "p95_duration_seconds": "95th percentile pipeline duration.",
    "p95_queue_seconds": "95th percentile pipeline queue time.",
    "pass_rate": "Share of executed test cases that passed.",
    "rerun_rate": "Share of pipeline runs that were retries or reruns.",
    "retry_dependency_rate": "Share of cases that only passed after one or more retries.",
    "success_rate": "Share of executions that completed successfully.",
    "weekend_commit_ratio": "Share of team commits made on weekends.",
    "wip_count_end_of_day": "Number of items still in progress at the end of the day.",
}

ALIASES = {
    "after hours": "after_hours_commit_ratio",
    "after hours work": "after_hours_commit_ratio",
    "build duration": "median_duration_seconds",
    "build queue": "avg_queue_seconds",
    "build success rate": "success_rate",
    "ci duration": "median_duration_seconds",
    "ci queue": "avg_queue_seconds",
    "ci success": "success_rate",
    "ci success rate": "success_rate",
    "commit volume": "commits_count",
    "coverage": "line_coverage_pct",
    "coverage regression": "coverage_regression_count",
    "cycle time": "cycle_time_p50_hours",
    "deployment failures": "change_failure_rate",
    "failure rate": "failure_rate",
    "flake rate": "flake_rate",
    "flaky tests": "flake_rate",
    "lead time": "lead_time_p50_hours",
    "pipeline duration": "median_duration_seconds",
    "pipeline health": "success_rate",
    "pipeline queue": "avg_queue_seconds",
    "pipeline reruns": "rerun_rate",
    "pipeline success rate": "success_rate",
    "pr cycle time": "median_pr_cycle_hours",
    "pr pickup time": "pr_pickup_time_p50_hours",
    "quality": "failure_rate",
    "queue time": "avg_queue_seconds",
    "reruns": "rerun_rate",
    "review latency": "pr_first_review_p50_hours",
    "success count": "success_count",
    "success rate": "success_rate",
    "test coverage": "line_coverage_pct",
    "test failures": "failed_count",
    "test pass rate": "pass_rate",
    "test reliability": "flake_rate",
    "throughput": "items_completed",
    "wip": "wip_count_end_of_day",
    "work in progress": "wip_count_end_of_day",
}


@dataclass(frozen=True)
class MetricDefinition:
    canonical_name: str
    display_name: str
    description: str
    unit: str
    dimensions: tuple[str, ...]
    source_table: str


@dataclass(frozen=True)
class RegistrySource:
    record_type: type
    source_table: str
    dimensions: tuple[str, ...]


REGISTRY_SOURCES = (
    RegistrySource(
        metric_schemas.CommitMetricsRecord, "commit_metrics", ("repo", "author", "day")
    ),
    RegistrySource(
        metric_schemas.UserMetricsDailyRecord,
        "user_metrics_daily",
        ("repo", "team", "author", "day"),
    ),
    RegistrySource(
        metric_schemas.ICLandscapeRollingRecord,
        "ic_landscape_rolling",
        ("repo", "team", "identity", "day"),
    ),
    RegistrySource(
        metric_schemas.RepoMetricsDailyRecord, "repo_metrics_daily", ("repo", "day")
    ),
    RegistrySource(
        metric_schemas.FileMetricsRecord, "file_metrics_daily", ("repo", "file", "day")
    ),
    RegistrySource(
        metric_schemas.TeamMetricsDailyRecord, "team_metrics_daily", ("team", "day")
    ),
    RegistrySource(
        metric_schemas.WorkItemCycleTimeRecord,
        "work_item_cycle_times",
        ("team", "work_scope", "provider", "day"),
    ),
    RegistrySource(
        metric_schemas.WorkItemMetricsDailyRecord,
        "work_item_metrics_daily",
        ("team", "work_scope", "provider", "day"),
    ),
    RegistrySource(
        metric_schemas.WorkItemUserMetricsDailyRecord,
        "work_item_user_metrics_daily",
        ("team", "user", "work_scope", "provider", "day"),
    ),
    RegistrySource(
        metric_schemas.WorkItemStateDurationDailyRecord,
        "work_item_state_duration_daily",
        ("team", "status", "work_scope", "provider", "day"),
    ),
    RegistrySource(
        metric_schemas.ReviewEdgeDailyRecord, "review_edges_daily", ("repo", "day")
    ),
    RegistrySource(
        metric_schemas.CICDMetricsDailyRecord, "cicd_metrics_daily", ("repo", "day")
    ),
    RegistrySource(
        metric_schemas.DeployMetricsDailyRecord, "deploy_metrics_daily", ("repo", "day")
    ),
    RegistrySource(
        metric_schemas.IncidentMetricsDailyRecord,
        "incident_metrics_daily",
        ("repo", "day"),
    ),
    RegistrySource(
        metric_schemas.DORAMetricsRecord,
        "dora_metrics_daily",
        ("repo", "metric_name", "day"),
    ),
    RegistrySource(
        metric_schemas.FileComplexitySnapshot,
        "file_complexity_snapshots",
        ("repo", "file", "day"),
    ),
    RegistrySource(
        metric_schemas.RepoComplexityDaily, "repo_complexity_daily", ("repo", "day")
    ),
    RegistrySource(
        metric_schemas.FileHotspotDaily, "file_hotspot_daily", ("repo", "file", "day")
    ),
    RegistrySource(
        metric_schemas.InvestmentMetricsRecord,
        "investment_metrics_daily",
        ("repo", "team", "investment_area", "day"),
    ),
    RegistrySource(
        metric_schemas.IssueTypeMetricsRecord,
        "issue_type_metrics_daily",
        ("repo", "team", "issue_type", "provider", "day"),
    ),
    RegistrySource(
        testops_schemas.PipelineMetricsDailyRecord,
        "testops_pipeline_metrics_daily",
        ("repo", "team", "service", "day"),
    ),
    RegistrySource(
        testops_schemas.TestMetricsDailyRecord,
        "testops_test_metrics_daily",
        ("repo", "team", "service", "day"),
    ),
    RegistrySource(
        testops_schemas.CoverageMetricsDailyRecord,
        "testops_coverage_metrics_daily",
        ("repo", "team", "service", "day"),
    ),
)


def _normalize(text: str) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in text).split())


def _display_name(field_name: str) -> str:
    tokens = field_name.replace("_", " ").split()
    if not tokens:
        return field_name
    return " ".join(
        token.upper()
        if token in {"pr", "ci", "mttr", "loc", "wip", "p50", "p75", "p90", "p95"}
        else token.capitalize()
        for token in tokens
    )


def _infer_unit(field_name: str) -> str:
    if field_name in UNIT_OVERRIDES:
        return UNIT_OVERRIDES[field_name]
    if field_name.endswith("_hours"):
        return "hours"
    if field_name.endswith("_minutes"):
        return "minutes"
    if field_name.endswith("_seconds"):
        return "seconds"
    if field_name.endswith("_pct"):
        return "percent"
    if field_name.endswith(("_ratio", "_rate", "_efficiency", "_gini")):
        return "ratio"
    if field_name.endswith("_score"):
        return "score"
    if "loc" in field_name:
        return "loc"
    if field_name.endswith("_count") or field_name.startswith(("count_", "total_")):
        return "count"
    return "unitless"


def _description(field_name: str, source_table: str) -> str:
    if field_name in DESCRIPTION_OVERRIDES:
        return DESCRIPTION_OVERRIDES[field_name]
    return f"{_display_name(field_name)} from {source_table}."


def _metric_field_names(record_type: type) -> Iterable[str]:
    for field in dataclass_fields(record_type):
        if field.name in DIMENSION_FIELD_NAMES:
            continue
        yield field.name


def build_metric_registry() -> dict[str, MetricDefinition]:
    registry: dict[str, MetricDefinition] = {}
    for source in REGISTRY_SOURCES:
        for field_name in _metric_field_names(source.record_type):
            registry.setdefault(
                field_name,
                MetricDefinition(
                    canonical_name=field_name,
                    display_name=_display_name(field_name),
                    description=_description(field_name, source.source_table),
                    unit=_infer_unit(field_name),
                    dimensions=source.dimensions,
                    source_table=source.source_table,
                ),
            )
    return registry


METRIC_REGISTRY = build_metric_registry()
NORMALIZED_ALIASES = {
    _normalize(alias): canonical_name for alias, canonical_name in ALIASES.items()
}


def get_metric_definition(metric_name: str) -> MetricDefinition | None:
    return METRIC_REGISTRY.get(metric_name)


def resolve_metric_alias(term: str) -> str | None:
    normalized = _normalize(term)
    if not normalized:
        return None
    if normalized in NORMALIZED_ALIASES:
        return NORMALIZED_ALIASES[normalized]
    compact = normalized.replace(" ", "_")
    if compact in METRIC_REGISTRY:
        return compact
    return None


def list_metric_names() -> list[str]:
    return sorted(METRIC_REGISTRY)
