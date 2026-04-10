"""Canonical TestOps schemas for Phase 0 foundations.

This module defines the v1 data contracts for:
- CI/CD pipeline and job execution events (extends existing ci_pipeline_runs)
- Test execution results (suite and case level)
- Code coverage snapshots
- TestOps daily metric records
- AI report DSL structures (report_plan, chart_spec, insight_block, provenance_record)

These schemas are the interface contract between ingestion, metrics, and visualization
layers. Changes must be coordinated across all downstream consumers.

Existing infrastructure this extends:
- metrics/schemas.py: PipelineRunRow, CICDMetricsDailyRecord, DORAMetricsRecord
- migrations/clickhouse/000_raw_tables.sql: ci_pipeline_runs, deployments, incidents
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TypedDict

from typing_extensions import NotRequired

# ---------------------------------------------------------------------------
# 1. CI/CD Pipeline and Job Event Schemas (CHAOS-1106)
# ---------------------------------------------------------------------------


class PipelineRunExtendedRow(TypedDict):
    """Extended pipeline run with TestOps-specific fields.

    Extends the existing PipelineRunRow with retry, trigger, and linkage data.
    The original PipelineRunRow (repo_id, run_id, status, queued_at, started_at,
    finished_at) remains canonical for backward compatibility.
    """

    repo_id: uuid.UUID
    run_id: str
    pipeline_name: NotRequired[str | None]
    provider: str  # github_actions | gitlab_ci | jenkins | buildkite
    status: str | None  # success | failure | cancelled | timeout | running | queued
    queued_at: datetime | None
    started_at: datetime
    finished_at: datetime | None
    duration_seconds: NotRequired[float | None]
    queue_seconds: NotRequired[float | None]
    retry_count: NotRequired[int]
    cancel_reason: NotRequired[str | None]
    trigger_source: NotRequired[str | None]  # push | pr | schedule | manual | api
    commit_hash: NotRequired[str | None]
    branch: NotRequired[str | None]
    pr_number: NotRequired[int | None]
    # Entity linkage (resolved at ingestion time).
    team_id: NotRequired[str | None]
    service_id: NotRequired[str | None]
    org_id: NotRequired[str]


class JobRunRow(TypedDict):
    """Individual job/stage within a pipeline run.

    Jobs are the unit of execution within a pipeline. A pipeline run contains
    one or more jobs that may run sequentially or in parallel.
    """

    repo_id: uuid.UUID
    run_id: str  # FK to pipeline run
    job_id: str
    job_name: str
    stage: NotRequired[str | None]  # stage/phase grouping if provider supports it
    status: str | None  # success | failure | cancelled | skipped | running
    started_at: datetime | None
    finished_at: datetime | None
    duration_seconds: NotRequired[float | None]
    runner_type: NotRequired[str | None]  # hosted | self-hosted | container
    retry_attempt: NotRequired[int]
    org_id: NotRequired[str]


# ---------------------------------------------------------------------------
# 2. Test Execution Schemas (CHAOS-1106)
# ---------------------------------------------------------------------------


class TestSuiteResultRow(TypedDict):
    """Aggregated result for a test suite within a pipeline run.

    A test suite is a collection of test cases, typically corresponding to
    a single test file, class, or logical grouping.
    """

    repo_id: uuid.UUID
    run_id: str  # FK to pipeline run
    suite_id: str  # deterministic ID: hash(run_id + suite_name + environment)
    suite_name: str
    framework: NotRequired[str | None]  # junit | pytest | jest | playwright | cypress
    environment: NotRequired[str | None]  # e.g., linux-x64, node-18, chrome
    total_count: int
    passed_count: int
    failed_count: int
    skipped_count: int
    error_count: NotRequired[int]
    quarantined_count: NotRequired[int]
    retried_count: NotRequired[int]
    duration_seconds: float | None
    started_at: datetime | None
    finished_at: datetime | None
    # Entity linkage.
    team_id: NotRequired[str | None]
    service_id: NotRequired[str | None]
    org_id: NotRequired[str]


class TestCaseResultRow(TypedDict):
    """Individual test case result within a suite.

    Test cases are the atomic unit of test execution. Flakiness, retry behavior,
    and failure patterns are tracked at this level.
    """

    repo_id: uuid.UUID
    run_id: str  # FK to pipeline run
    suite_id: str  # FK to test suite
    case_id: str  # deterministic ID: hash(suite_id + case_name + parameters)
    case_name: str
    class_name: NotRequired[str | None]
    status: str  # passed | failed | skipped | error | quarantined
    duration_seconds: float | None
    retry_attempt: NotRequired[int]  # 0 = first attempt
    failure_message: NotRequired[str | None]
    failure_type: NotRequired[
        str | None
    ]  # assertion | timeout | error | infrastructure
    stack_trace: NotRequired[str | None]  # truncated to 4KB
    is_quarantined: NotRequired[bool]
    # Flake detection fields (populated by metrics layer, not ingestion).
    # These are NOT set during ingestion — they are computed post-hoc.
    org_id: NotRequired[str]


# ---------------------------------------------------------------------------
# 3. Coverage Schemas (CHAOS-1106)
# ---------------------------------------------------------------------------


class CoverageSnapshotRow(TypedDict):
    """Code coverage snapshot associated with a pipeline run.

    Coverage data is ingested from lcov, Cobertura, JaCoCo, or similar formats.
    v1 supports aggregate and per-file coverage. Changed-code coverage is v2.
    """

    repo_id: uuid.UUID
    run_id: str  # FK to pipeline run
    snapshot_id: str  # deterministic ID: hash(run_id + report_type)
    report_format: NotRequired[str | None]  # lcov | cobertura | jacoco | clover
    # Aggregate metrics.
    lines_total: int | None
    lines_covered: int | None
    line_coverage_pct: float | None
    branches_total: NotRequired[int | None]
    branches_covered: NotRequired[int | None]
    branch_coverage_pct: NotRequired[float | None]
    functions_total: NotRequired[int | None]
    functions_covered: NotRequired[int | None]
    # Linkage.
    commit_hash: NotRequired[str | None]
    branch: NotRequired[str | None]
    pr_number: NotRequired[int | None]
    team_id: NotRequired[str | None]
    service_id: NotRequired[str | None]
    org_id: NotRequired[str]


# ---------------------------------------------------------------------------
# 4. AI Report DSL Schemas (CHAOS-1107)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReportPlan:
    """Structured plan compiled from a natural-language report prompt.

    The report planner converts user intent into this canonical structure.
    The rendering engine consumes only ReportPlan — never raw prompts.
    """

    plan_id: str
    report_type: str  # weekly_health | monthly_review | quality_trend | custom
    audience: str | None  # executive | team_lead | developer
    scope_teams: list[str] = field(default_factory=list)
    scope_repos: list[str] = field(default_factory=list)
    scope_services: list[str] = field(default_factory=list)
    time_range_start: date | None = None
    time_range_end: date | None = None
    comparison_period: str | None = None  # prior_week | prior_month | none
    sections: list[str] = field(
        default_factory=list
    )  # summary | delivery | quality | testops | wellbeing
    requested_metrics: list[str] = field(default_factory=list)
    requested_charts: list[str] = field(default_factory=list)  # chart_spec IDs
    include_insights: bool = True
    include_anomalies: bool = True
    confidence_threshold: str = "direct_fact"  # direct_fact | inferred | hypothesis
    created_at: datetime | None = None
    org_id: str = ""


@dataclass(frozen=True)
class ChartSpec:
    """Specification for a chart to be rendered in a report.

    Charts are generated from validated metric queries only. The chart_type
    and metric combination determines the rendering strategy.
    """

    chart_id: str
    plan_id: str  # FK to ReportPlan
    chart_type: (
        str  # line | bar | stacked_bar | heatmap | table | scorecard | trend_delta
    )
    metric: str  # canonical metric name from registry
    group_by: str | None = None  # team | repo | service | week | month
    filter_teams: list[str] = field(default_factory=list)
    filter_repos: list[str] = field(default_factory=list)
    time_range_start: date | None = None
    time_range_end: date | None = None
    title: str | None = None
    org_id: str = ""


@dataclass(frozen=True)
class InsightBlock:
    """Structured insight generated from metric analysis.

    Every insight must reference the specific metrics and data that support it.
    Freeform claims without metric backing are forbidden.
    """

    insight_id: str
    plan_id: str  # FK to ReportPlan
    insight_type: str  # trend_delta | anomaly | regression | correlation | top_risk
    confidence: str  # direct_fact | inferred | hypothesis
    summary: str  # Max 2 sentences.
    supporting_metrics: list[str] = field(default_factory=list)  # metric names
    supporting_values: dict[str, float] = field(default_factory=dict)  # metric→value
    severity: str = "info"  # info | warning | critical
    org_id: str = ""


@dataclass(frozen=True)
class ProvenanceRecord:
    """Audit trail for every generated report artifact.

    Every report, chart, and insight must have a provenance record showing
    exactly which data, time range, and filters produced it.
    """

    provenance_id: str
    artifact_type: str  # report | chart | insight | narrative
    artifact_id: str
    plan_id: str  # FK to ReportPlan
    data_sources: list[str] = field(default_factory=list)  # table/metric names queried
    metrics_used: list[str] = field(default_factory=list)
    time_range_start: date | None = None
    time_range_end: date | None = None
    filters_applied: dict[str, str] = field(default_factory=dict)
    generated_at: datetime | None = None
    generator_version: str = ""
    org_id: str = ""


# ---------------------------------------------------------------------------
# 5. TestOps Metric Records (CHAOS-1109)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineMetricsDailyRecord:
    """Daily pipeline health metrics per repo.

    Extends existing CICDMetricsDailyRecord with finer-grained TestOps metrics.
    """

    repo_id: uuid.UUID
    day: date
    pipelines_count: int
    success_count: int
    failure_count: int
    cancelled_count: int
    success_rate: float
    failure_rate: float
    cancel_rate: float
    rerun_rate: float  # proportion of runs that are retries
    median_duration_seconds: float | None
    p95_duration_seconds: float | None
    avg_queue_seconds: float | None
    p95_queue_seconds: float | None
    computed_at: datetime
    team_id: str | None = None
    service_id: str | None = None
    org_id: str = ""


@dataclass(frozen=True)
class TestMetricsDailyRecord:
    """Daily test reliability metrics per repo.

    Computed from test_case_results and test_suite_results tables.
    """

    repo_id: uuid.UUID
    day: date
    total_cases: int
    passed_count: int
    failed_count: int
    skipped_count: int
    quarantined_count: int
    pass_rate: float
    failure_rate: float
    flake_rate: float  # cases that flipped pass↔fail in same run window
    retry_dependency_rate: float  # cases that only pass after retry
    total_suites: int
    suite_duration_p50_seconds: float | None
    suite_duration_p95_seconds: float | None
    failure_recurrence_score: float  # proportion of failures seen in prior N runs
    computed_at: datetime
    team_id: str | None = None
    service_id: str | None = None
    org_id: str = ""


@dataclass(frozen=True)
class CoverageMetricsDailyRecord:
    """Daily coverage metrics per repo.

    v1: aggregate and per-file coverage. Changed-code coverage deferred to v2.
    """

    repo_id: uuid.UUID
    day: date
    line_coverage_pct: float | None
    branch_coverage_pct: float | None
    lines_total: int | None
    lines_covered: int | None
    coverage_delta_pct: float | None  # change from prior snapshot
    uncovered_files_count: int
    coverage_regression_count: int  # files where coverage decreased
    computed_at: datetime
    team_id: str | None = None
    service_id: str | None = None
    org_id: str = ""


# ---------------------------------------------------------------------------
# Risk Model Records (CHAOS-1079)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReleaseConfidenceRecord:
    """Composite release confidence score combining pipeline, test, and coverage signals."""

    repo_id: uuid.UUID
    day: date
    confidence_score: float  # 0.0-1.0
    pipeline_success_factor: float
    test_pass_factor: float
    coverage_factor: float
    flake_penalty: float
    regression_penalty: float
    factors_json: str  # JSON explainability payload
    computed_at: datetime
    team_id: str | None = None
    service_id: str | None = None
    org_id: str = ""


@dataclass(frozen=True)
class QualityDragRecord:
    """Estimated hours wasted due to CI/test quality issues."""

    repo_id: uuid.UUID
    day: date
    drag_hours: float
    failure_rework_hours: float
    flake_investigation_hours: float
    queue_wait_hours: float
    retry_overhead_hours: float
    factors_json: str
    computed_at: datetime
    team_id: str | None = None
    service_id: str | None = None
    org_id: str = ""


@dataclass(frozen=True)
class PipelineStabilityRecord:
    """Rolling pipeline stability index with trend analysis."""

    repo_id: uuid.UUID
    day: date
    stability_index: float  # 0.0-1.0 (1.0 = perfectly stable)
    success_rate_7d: float
    success_rate_trend: float  # positive = improving
    failure_clustering_score: float  # 0=random, 1=clustered
    median_recovery_time_seconds: float | None
    computed_at: datetime
    team_id: str | None = None
    service_id: str | None = None
    org_id: str = ""
