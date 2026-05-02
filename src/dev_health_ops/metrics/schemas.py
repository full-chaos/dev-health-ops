from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TypedDict
from uuid import UUID

from typing_extensions import NotRequired


class CommitStatRow(TypedDict):
    repo_id: uuid.UUID
    commit_hash: str
    author_email: str | None
    author_name: str | None
    committer_when: datetime
    file_path: str | None
    additions: int
    deletions: int
    old_file_mode: NotRequired[str | None]
    new_file_mode: NotRequired[str | None]


class PullRequestRow(TypedDict):
    """PR data from loaders. NotRequired fields: always populated by all loaders, default to 0 or None when DB null."""

    repo_id: uuid.UUID
    number: int
    author_email: str | None
    author_name: str | None
    created_at: datetime
    merged_at: datetime | None
    first_review_at: NotRequired[datetime | None]
    first_comment_at: NotRequired[datetime | None]
    reviews_count: NotRequired[int]
    changes_requested_count: NotRequired[int]
    comments_count: NotRequired[int]
    additions: NotRequired[int]
    deletions: NotRequired[int]
    changed_files: NotRequired[int]


class PullRequestReviewRow(TypedDict):
    repo_id: uuid.UUID
    number: int
    reviewer: str
    submitted_at: datetime
    state: str  # APPROVED|CHANGES_REQUESTED|COMMENTED|DISMISSED|...


class PullRequestCommentRow(TypedDict):
    repo_id: uuid.UUID
    number: int
    commenter: str
    created_at: datetime


class PipelineRunRow(TypedDict):
    repo_id: uuid.UUID
    run_id: str
    status: str | None
    queued_at: datetime | None
    started_at: datetime
    finished_at: datetime | None


class DeploymentRow(TypedDict):
    """Deployment data from loaders. NotRequired fields: populated when PR linkage exists in git_pull_requests."""

    repo_id: uuid.UUID
    deployment_id: str
    status: str | None
    environment: str | None
    started_at: datetime | None
    finished_at: datetime | None
    deployed_at: datetime | None
    merged_at: NotRequired[datetime | None]
    pull_request_number: NotRequired[int | None]
    release_ref: NotRequired[str]
    release_ref_confidence: NotRequired[float]


class IncidentRow(TypedDict):
    repo_id: uuid.UUID
    incident_id: str
    status: str | None
    started_at: datetime
    resolved_at: datetime | None


@dataclass(frozen=True)
class CommitMetricsRecord:
    repo_id: uuid.UUID
    commit_hash: str
    day: date
    author_email: str
    total_loc: int
    files_changed: int
    size_bucket: str  # small|medium|large
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class UserMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    author_email: str
    commits_count: int
    loc_added: int
    loc_deleted: int
    files_changed: int
    large_commits_count: int
    avg_commit_size_loc: float
    prs_authored: int
    prs_merged: int
    avg_pr_cycle_hours: float
    median_pr_cycle_hours: float
    computed_at: datetime

    # PR cycle time distribution (merged PRs, by merged day).
    pr_cycle_p75_hours: float = 0.0
    pr_cycle_p90_hours: float = 0.0

    # Review / collaboration signals (best-effort, requires review/comment facts).
    prs_with_first_review: int = 0
    pr_first_review_p50_hours: float | None = None
    pr_first_review_p90_hours: float | None = None
    pr_review_time_p50_hours: float | None = None
    pr_pickup_time_p50_hours: float | None = None
    reviews_given: int = 0
    changes_requested_given: int = 0
    reviews_received: int = 0
    review_reciprocity: float = 0.0

    # Burnout / Activity signals.
    active_hours: float = 0.0
    weekend_days: int = 0  # 1 if this day is a weekend and user was active, else 0

    # Team dimension (optional).
    team_id: str | None = None
    team_name: str | None = None

    # New IC/Landscape fields
    identity_id: str = ""
    loc_touched: int = 0
    prs_opened: int = 0
    work_items_completed: int = 0
    work_items_active: int = 0
    delivery_units: int = 0
    cycle_p50_hours: float = 0.0
    cycle_p90_hours: float = 0.0
    org_id: str = ""


@dataclass(frozen=True)
class ICLandscapeRollingRecord:
    repo_id: uuid.UUID
    as_of_day: date
    identity_id: str
    team_id: str | None
    map_name: str
    x_raw: float
    y_raw: float
    x_norm: float
    y_norm: float
    churn_loc_30d: int
    delivery_units_30d: int
    cycle_p50_30d_hours: float
    wip_max_30d: int
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class RepoMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    commits_count: int
    total_loc_touched: int
    avg_commit_size_loc: float
    large_commit_ratio: float
    prs_merged: int
    median_pr_cycle_hours: float
    computed_at: datetime

    # PR cycle time distribution (merged PRs).
    pr_cycle_p75_hours: float = 0.0
    pr_cycle_p90_hours: float = 0.0

    # Review / collaboration signals.
    prs_with_first_review: int = 0
    pr_first_review_p50_hours: float | None = None
    pr_first_review_p90_hours: float | None = None
    pr_review_time_p50_hours: float | None = None
    pr_pickup_time_p50_hours: float | None = None

    # Quality signals.
    large_pr_ratio: float = 0.0
    pr_rework_ratio: float = 0.0
    pr_size_p50_loc: float | None = None
    pr_size_p90_loc: float | None = None
    pr_comments_per_100_loc: float | None = None
    pr_reviews_per_100_loc: float | None = None
    rework_churn_ratio_30d: float = 0.0
    single_owner_file_ratio_30d: float = 0.0
    review_load_top_reviewer_ratio: float = 0.0

    # Knowledge / Risk signals
    bus_factor: int = 0
    code_ownership_gini: float = 0.0

    # DORA proxies.
    mttr_hours: float | None = None
    change_failure_rate: float = 0.0
    org_id: str = ""


@dataclass(frozen=True)
class FileMetricsRecord:
    repo_id: uuid.UUID
    day: date
    path: str
    churn: int
    contributors: int
    commits_count: int
    hotspot_score: float
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class TeamMetricsDailyRecord:
    day: date
    team_id: str
    team_name: str
    commits_count: int
    after_hours_commits_count: int
    weekend_commits_count: int
    after_hours_commit_ratio: float
    weekend_commit_ratio: float
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class WorkItemCycleTimeRecord:
    work_item_id: str
    provider: str
    day: date  # completed day (UTC) when completed_at is present, else created day
    work_scope_id: str
    team_id: str | None
    team_name: str | None
    assignee: str | None
    type: str
    status: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    cycle_time_hours: float | None
    lead_time_hours: float | None
    active_time_hours: float | None
    wait_time_hours: float | None
    flow_efficiency: float | None
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class WorkItemMetricsDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    team_id: str | None
    team_name: str | None
    items_started: int
    items_completed: int
    items_started_unassigned: int
    items_completed_unassigned: int
    wip_count_end_of_day: int
    wip_unassigned_end_of_day: int
    cycle_time_p50_hours: float | None
    cycle_time_p90_hours: float | None
    lead_time_p50_hours: float | None
    lead_time_p90_hours: float | None
    wip_age_p50_hours: float | None
    wip_age_p90_hours: float | None
    bug_completed_ratio: float
    story_points_completed: float
    computed_at: datetime
    # Phase 2 metrics
    new_bugs_count: int = 0
    new_items_count: int = 0
    defect_intro_rate: float = 0.0
    wip_congestion_ratio: float = 0.0
    predictability_score: float = 0.0
    org_id: str = ""


@dataclass(frozen=True)
class WorkItemUserMetricsDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    user_identity: str
    team_id: str | None
    team_name: str | None
    items_started: int
    items_completed: int
    wip_count_end_of_day: int
    cycle_time_p50_hours: float | None
    cycle_time_p90_hours: float | None
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class WorkItemStateDurationDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    team_id: str
    team_name: str
    status: str  # normalized status category
    duration_hours: float
    items_touched: int
    computed_at: datetime
    avg_wip: float = 0.0
    org_id: str = ""


@dataclass(frozen=True)
class ReviewEdgeDailyRecord:
    repo_id: uuid.UUID
    day: date
    reviewer: str
    author: str
    reviews_count: int
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class CICDMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    pipelines_count: int
    success_rate: float
    avg_duration_minutes: float | None
    p90_duration_minutes: float | None
    avg_queue_minutes: float | None
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class DeployMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    deployments_count: int
    failed_deployments_count: int
    deploy_time_p50_hours: float | None
    lead_time_p50_hours: float | None
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class IncidentMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    incidents_count: int
    mttr_p50_hours: float | None
    mttr_p90_hours: float | None
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class DORAMetricsRecord:
    repo_id: uuid.UUID
    day: date
    metric_name: str
    value: float
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class FileComplexitySnapshot:
    repo_id: uuid.UUID
    as_of_day: date
    ref: str
    file_path: str
    language: str
    loc: int
    functions_count: int
    cyclomatic_total: int
    cyclomatic_avg: float
    high_complexity_functions: int
    very_high_complexity_functions: int
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class RepoComplexityDaily:
    repo_id: uuid.UUID
    day: date
    loc_total: int
    cyclomatic_total: int
    cyclomatic_per_kloc: float
    high_complexity_functions: int
    very_high_complexity_functions: int
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class FileHotspotDaily:
    repo_id: uuid.UUID
    day: date
    file_path: str
    churn_loc_30d: int
    churn_commits_30d: int
    cyclomatic_total: int
    cyclomatic_avg: float
    blame_concentration: float | None
    risk_score: float
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class InvestmentClassificationRecord:
    repo_id: uuid.UUID | None
    day: date
    artifact_type: str
    artifact_id: str
    provider: str
    investment_area: str
    project_stream: str | None
    confidence: float
    rule_id: str
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class InvestmentMetricsRecord:
    repo_id: uuid.UUID | None
    day: date
    team_id: str | None
    investment_area: str
    project_stream: str | None
    delivery_units: int
    work_items_completed: int
    prs_merged: int
    churn_loc: int
    cycle_p50_hours: float
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class IssueTypeMetricsRecord:
    repo_id: uuid.UUID | None
    day: date
    provider: str
    team_id: str
    issue_type_norm: str
    created_count: int
    completed_count: int
    active_count: int
    cycle_p50_hours: float
    cycle_p90_hours: float
    lead_p50_hours: float
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class FeatureFlagRecord:
    provider: str
    flag_key: str
    project_key: str | None
    repo_id: UUID | None
    environment: str
    flag_type: str | None
    created_at: datetime | None
    archived_at: datetime | None
    last_synced: datetime
    org_id: str = ""


@dataclass(frozen=True)
class FeatureFlagEventRecord:
    event_type: str
    flag_key: str
    environment: str
    repo_id: UUID | None
    actor_type: str | None
    prev_state: str | None
    next_state: str | None
    event_ts: datetime
    ingested_at: datetime
    source_event_id: str | None
    dedupe_key: str
    org_id: str = ""


@dataclass(frozen=True)
class FeatureFlagLinkRecord:
    flag_key: str
    target_type: str
    target_id: str
    provider: str
    link_source: str
    link_type: str
    evidence_type: str | None
    confidence: float
    valid_from: datetime
    valid_to: datetime | None
    last_synced: datetime
    org_id: str = ""


@dataclass(frozen=True)
class TelemetrySignalBucketRecord:
    signal_type: str
    signal_count: int
    session_count: int
    unique_pseudonymous_count: int | None
    endpoint_group: str | None
    environment: str
    repo_id: UUID | None
    release_ref: str | None
    bucket_start: datetime
    bucket_end: datetime
    ingested_at: datetime
    is_sampled: bool
    schema_version: str
    dedupe_key: str
    org_id: str = ""


@dataclass(frozen=True)
class ReleaseImpactDailyRecord:
    day: date
    release_ref: str
    environment: str
    repo_id: UUID | None
    release_user_friction_delta: float | None
    release_post_friction_rate: float | None
    release_error_rate_delta: float | None
    release_post_error_rate: float | None
    time_to_first_user_issue_after_release: float | None
    release_impact_confidence_score: float | None
    release_impact_coverage_ratio: float | None
    flag_exposure_rate: float | None
    flag_activation_rate: float | None
    flag_reliability_guardrail: float | None
    flag_friction_delta: float | None
    flag_rollout_half_life: float | None
    flag_churn_rate: float | None
    issue_to_release_impact_link_rate: float | None
    rollback_or_disable_after_impact_spike: int | None
    coverage_ratio: float | None
    missing_required_fields_count: int = 0
    instrumentation_change_flag: bool = False
    data_completeness: float = 1.0
    concurrent_deploy_count: int = 0
    computed_at: datetime | None = None
    org_id: str = ""


@dataclass(frozen=True)
class WorkGraphEdgeRecord:
    edge_id: str
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    edge_type: str
    repo_id: UUID | None
    provider: str | None
    provenance: str
    confidence: float
    evidence: str
    discovered_at: datetime
    last_synced: datetime
    event_ts: datetime
    day: date
    org_id: str = ""


@dataclass(frozen=True)
class WorkGraphIssuePRRecord:
    repo_id: UUID
    work_item_id: str
    pr_number: int
    confidence: float
    provenance: str
    evidence: str
    last_synced: datetime
    org_id: str = ""


@dataclass(frozen=True)
class WorkGraphPRCommitRecord:
    repo_id: UUID
    pr_number: int
    commit_hash: str
    confidence: float
    provenance: str
    evidence: str
    last_synced: datetime
    org_id: str = ""


@dataclass(frozen=True)
class WorkUnitInvestmentRecord:
    work_unit_id: str
    work_unit_type: str | None
    work_unit_name: str | None
    from_ts: datetime
    to_ts: datetime
    repo_id: uuid.UUID | None
    provider: str | None
    effort_metric: str
    effort_value: float
    theme_distribution_json: dict[str, float]
    subcategory_distribution_json: dict[str, float]
    structural_evidence_json: str
    evidence_quality: float
    evidence_quality_band: str
    categorization_status: str
    categorization_errors_json: str
    categorization_model_version: str
    categorization_input_hash: str
    categorization_run_id: str
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class WorkUnitInvestmentEvidenceQuoteRecord:
    work_unit_id: str
    quote: str
    source_type: str
    source_id: str
    computed_at: datetime
    categorization_run_id: str
    org_id: str = ""


@dataclass(frozen=True)
class InvestmentExplanationRecord:
    """Cached LLM-generated explanation for investment mix views."""

    cache_key: str  # Hash of (filter_context + theme + subcategory)
    explanation_json: str  # Full JSON of InvestmentMixExplanation
    llm_provider: str
    llm_model: str | None
    computed_at: datetime
    org_id: str = ""


@dataclass(frozen=True)
class DailyMetricsResult:
    day: date
    repo_metrics: list[RepoMetricsDailyRecord]
    user_metrics: list[UserMetricsDailyRecord]
    commit_metrics: list[CommitMetricsRecord]

    # Optional expanded outputs (may be empty depending on available inputs).
    team_metrics: list[TeamMetricsDailyRecord] = field(default_factory=list)
    file_metrics: list[FileMetricsRecord] = field(default_factory=list)
    work_item_metrics: list[WorkItemMetricsDailyRecord] = field(default_factory=list)
    work_item_user_metrics: list[WorkItemUserMetricsDailyRecord] = field(
        default_factory=list
    )
    work_item_cycle_times: list[WorkItemCycleTimeRecord] = field(default_factory=list)
    work_item_state_durations: list[WorkItemStateDurationDailyRecord] = field(
        default_factory=list
    )
    review_edges: list[ReviewEdgeDailyRecord] = field(default_factory=list)


@dataclass(frozen=True)
class CapacityForecastRecord:
    forecast_id: str
    computed_at: datetime
    team_id: str | None
    work_scope_id: str | None
    backlog_size: int
    target_items: int | None
    target_date: date | None
    history_days: int
    simulation_count: int
    p50_days: int | None
    p85_days: int | None
    p95_days: int | None
    p50_date: date | None
    p85_date: date | None
    p95_date: date | None
    p50_items: int | None
    p85_items: int | None
    p95_items: int | None
    throughput_mean: float
    throughput_stddev: float
    insufficient_history: bool = False
    high_variance: bool = False
    org_id: str = ""
