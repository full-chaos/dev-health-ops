from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from uuid import UUID
from datetime import date, datetime
from typing import Dict, List, Optional, TypedDict
from typing_extensions import NotRequired


class CommitStatRow(TypedDict):
    repo_id: uuid.UUID
    commit_hash: str
    author_email: Optional[str]
    author_name: Optional[str]
    committer_when: datetime
    file_path: Optional[str]
    additions: int
    deletions: int


class PullRequestRow(TypedDict):
    repo_id: uuid.UUID
    number: int
    author_email: Optional[str]
    author_name: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime]
    # Optional PR facts when available from the synced store or derived joins.
    first_review_at: NotRequired[Optional[datetime]]
    first_comment_at: NotRequired[Optional[datetime]]
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
    status: Optional[str]
    queued_at: Optional[datetime]
    started_at: datetime
    finished_at: Optional[datetime]


class DeploymentRow(TypedDict):
    repo_id: uuid.UUID
    deployment_id: str
    status: Optional[str]
    environment: Optional[str]
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    deployed_at: Optional[datetime]
    merged_at: NotRequired[Optional[datetime]]
    pull_request_number: NotRequired[Optional[int]]


class IncidentRow(TypedDict):
    repo_id: uuid.UUID
    incident_id: str
    status: Optional[str]
    started_at: datetime
    resolved_at: Optional[datetime]


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
    pr_first_review_p50_hours: Optional[float] = None
    pr_first_review_p90_hours: Optional[float] = None
    pr_review_time_p50_hours: Optional[float] = None
    pr_pickup_time_p50_hours: Optional[float] = None
    reviews_given: int = 0
    changes_requested_given: int = 0
    reviews_received: int = 0
    review_reciprocity: float = 0.0

    # Burnout / Activity signals.
    active_hours: float = 0.0
    weekend_days: int = 0  # 1 if this day is a weekend and user was active, else 0

    # Team dimension (optional).
    team_id: Optional[str] = None
    team_name: Optional[str] = None

    # New IC/Landscape fields
    identity_id: str = ""
    loc_touched: int = 0
    prs_opened: int = 0
    work_items_completed: int = 0
    work_items_active: int = 0
    delivery_units: int = 0
    cycle_p50_hours: float = 0.0
    cycle_p90_hours: float = 0.0


@dataclass(frozen=True)
class ICLandscapeRollingRecord:
    repo_id: uuid.UUID
    as_of_day: date
    identity_id: str
    team_id: Optional[str]
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
    pr_first_review_p50_hours: Optional[float] = None
    pr_first_review_p90_hours: Optional[float] = None
    pr_review_time_p50_hours: Optional[float] = None
    pr_pickup_time_p50_hours: Optional[float] = None

    # Quality signals.
    large_pr_ratio: float = 0.0
    pr_rework_ratio: float = 0.0
    pr_size_p50_loc: Optional[float] = None
    pr_size_p90_loc: Optional[float] = None
    pr_comments_per_100_loc: Optional[float] = None
    pr_reviews_per_100_loc: Optional[float] = None
    rework_churn_ratio_30d: float = 0.0
    single_owner_file_ratio_30d: float = 0.0
    review_load_top_reviewer_ratio: float = 0.0

    # Knowledge / Risk signals
    bus_factor: int = 0
    code_ownership_gini: float = 0.0

    # DORA proxies.
    mttr_hours: Optional[float] = None
    change_failure_rate: float = 0.0


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


@dataclass(frozen=True)
class WorkItemCycleTimeRecord:
    work_item_id: str
    provider: str
    day: date  # completed day (UTC) when completed_at is present, else created day
    work_scope_id: str
    team_id: Optional[str]
    team_name: Optional[str]
    assignee: Optional[str]
    type: str
    status: str
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    cycle_time_hours: Optional[float]
    lead_time_hours: Optional[float]
    active_time_hours: Optional[float]
    wait_time_hours: Optional[float]
    flow_efficiency: Optional[float]
    computed_at: datetime


@dataclass(frozen=True)
class WorkItemMetricsDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    team_id: Optional[str]
    team_name: Optional[str]
    items_started: int
    items_completed: int
    items_started_unassigned: int
    items_completed_unassigned: int
    wip_count_end_of_day: int
    wip_unassigned_end_of_day: int
    cycle_time_p50_hours: Optional[float]
    cycle_time_p90_hours: Optional[float]
    lead_time_p50_hours: Optional[float]
    lead_time_p90_hours: Optional[float]
    wip_age_p50_hours: Optional[float]
    wip_age_p90_hours: Optional[float]
    bug_completed_ratio: float
    story_points_completed: float
    computed_at: datetime
    # Phase 2 metrics
    new_bugs_count: int = 0
    new_items_count: int = 0
    defect_intro_rate: float = 0.0
    wip_congestion_ratio: float = 0.0
    predictability_score: float = 0.0


@dataclass(frozen=True)
class WorkItemUserMetricsDailyRecord:
    day: date
    provider: str
    work_scope_id: str
    user_identity: str
    team_id: Optional[str]
    team_name: Optional[str]
    items_started: int
    items_completed: int
    wip_count_end_of_day: int
    cycle_time_p50_hours: Optional[float]
    cycle_time_p90_hours: Optional[float]
    computed_at: datetime


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


@dataclass(frozen=True)
class ReviewEdgeDailyRecord:
    repo_id: uuid.UUID
    day: date
    reviewer: str
    author: str
    reviews_count: int
    computed_at: datetime


@dataclass(frozen=True)
class CICDMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    pipelines_count: int
    success_rate: float
    avg_duration_minutes: Optional[float]
    p90_duration_minutes: Optional[float]
    avg_queue_minutes: Optional[float]
    computed_at: datetime


@dataclass(frozen=True)
class DeployMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    deployments_count: int
    failed_deployments_count: int
    deploy_time_p50_hours: Optional[float]
    lead_time_p50_hours: Optional[float]
    computed_at: datetime


@dataclass(frozen=True)
class IncidentMetricsDailyRecord:
    repo_id: uuid.UUID
    day: date
    incidents_count: int
    mttr_p50_hours: Optional[float]
    mttr_p90_hours: Optional[float]
    computed_at: datetime


@dataclass(frozen=True)
class DORAMetricsRecord:
    repo_id: uuid.UUID
    day: date
    metric_name: str
    value: float
    computed_at: datetime


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


@dataclass(frozen=True)
class FileHotspotDaily:
    repo_id: uuid.UUID
    day: date
    file_path: str
    churn_loc_30d: int
    churn_commits_30d: int
    cyclomatic_total: int
    cyclomatic_avg: float
    blame_concentration: Optional[float]
    risk_score: float
    computed_at: datetime


@dataclass(frozen=True)
class InvestmentClassificationRecord:
    repo_id: Optional[uuid.UUID]
    day: date
    artifact_type: str
    artifact_id: str
    provider: str
    investment_area: str
    project_stream: Optional[str]
    confidence: float
    rule_id: str
    computed_at: datetime


@dataclass(frozen=True)
class InvestmentMetricsRecord:
    repo_id: Optional[uuid.UUID]
    day: date
    team_id: Optional[str]
    investment_area: str
    project_stream: Optional[str]
    delivery_units: int
    work_items_completed: int
    prs_merged: int
    churn_loc: int
    cycle_p50_hours: float
    computed_at: datetime


@dataclass(frozen=True)
class IssueTypeMetricsRecord:
    repo_id: Optional[uuid.UUID]
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


@dataclass(frozen=True)
class WorkGraphEdgeRecord:
    edge_id: str
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    edge_type: str
    repo_id: Optional[UUID]
    provider: Optional[str]
    provenance: str
    confidence: float
    evidence: str
    discovered_at: datetime
    last_synced: datetime
    event_ts: datetime
    day: date


@dataclass(frozen=True)
class WorkGraphIssuePRRecord:
    repo_id: UUID
    work_item_id: str
    pr_number: int
    confidence: float
    provenance: str
    evidence: str
    last_synced: datetime


@dataclass(frozen=True)
class WorkGraphPRCommitRecord:
    repo_id: UUID
    pr_number: int
    commit_hash: str
    confidence: float
    provenance: str
    evidence: str
    last_synced: datetime


@dataclass(frozen=True)
class WorkUnitInvestmentRecord:
    work_unit_id: str
    work_unit_type: Optional[str]
    work_unit_name: Optional[str]
    from_ts: datetime
    to_ts: datetime
    repo_id: Optional[uuid.UUID]
    provider: Optional[str]
    effort_metric: str
    effort_value: float
    theme_distribution_json: Dict[str, float]
    subcategory_distribution_json: Dict[str, float]
    structural_evidence_json: str
    evidence_quality: float
    evidence_quality_band: str
    categorization_status: str
    categorization_errors_json: str
    categorization_model_version: str
    categorization_input_hash: str
    categorization_run_id: str
    computed_at: datetime


@dataclass(frozen=True)
class WorkUnitInvestmentEvidenceQuoteRecord:
    work_unit_id: str
    quote: str
    source_type: str
    source_id: str
    computed_at: datetime
    categorization_run_id: str


@dataclass(frozen=True)
class InvestmentExplanationRecord:
    """Cached LLM-generated explanation for investment mix views."""

    cache_key: str  # Hash of (filter_context + theme + subcategory)
    explanation_json: str  # Full JSON of InvestmentMixExplanation
    llm_provider: str
    llm_model: Optional[str]
    computed_at: datetime


@dataclass(frozen=True)
class DailyMetricsResult:
    day: date
    repo_metrics: List[RepoMetricsDailyRecord]
    user_metrics: List[UserMetricsDailyRecord]
    commit_metrics: List[CommitMetricsRecord]

    # Optional expanded outputs (may be empty depending on available inputs).
    team_metrics: List[TeamMetricsDailyRecord] = field(default_factory=list)
    file_metrics: List[FileMetricsRecord] = field(default_factory=list)
    work_item_metrics: List[WorkItemMetricsDailyRecord] = field(default_factory=list)
    work_item_user_metrics: List[WorkItemUserMetricsDailyRecord] = field(
        default_factory=list
    )
    work_item_cycle_times: List[WorkItemCycleTimeRecord] = field(default_factory=list)
    work_item_state_durations: List[WorkItemStateDurationDailyRecord] = field(
        default_factory=list
    )
    review_edges: List[ReviewEdgeDailyRecord] = field(default_factory=list)


@dataclass(frozen=True)
class CapacityForecastRecord:
    forecast_id: str
    computed_at: datetime
    team_id: Optional[str]
    work_scope_id: Optional[str]
    backlog_size: int
    target_items: Optional[int]
    target_date: Optional[date]
    history_days: int
    simulation_count: int
    p50_days: Optional[int]
    p85_days: Optional[int]
    p95_days: Optional[int]
    p50_date: Optional[date]
    p85_date: Optional[date]
    p95_date: Optional[date]
    p50_items: Optional[int]
    p85_items: Optional[int]
    p95_items: Optional[int]
    throughput_mean: float
    throughput_stddev: float
    insufficient_history: bool = False
    high_variance: bool = False
