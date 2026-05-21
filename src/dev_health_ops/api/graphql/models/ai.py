"""Strawberry GraphQL input and output types for AI workflow analytics.

These contracts expose the data produced by the AI Workflow Intelligence
milestone (attribution ingestion, impact metrics, work-graph evidence, and
governance) to API clients.

Design notes
------------
- Outputs are flat, stable, and JSON-serialisable.  They mirror persisted
  ClickHouse rows so the frontend never has to mix in unrelated entities.
- Bucket values and rule identifiers come from canonical, hard-coded
  registries — clients can switch on them safely.
- Every result type carries a ``data_available`` flag so the UI can render
  an empty, partial, or populated state without ad-hoc null probing.
- Drilldown identifiers point at Work-Graph evidence (PRs, issues, runs)
  rather than at synthetic UUIDs that the UI cannot resolve.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

import strawberry

# =============================================================================
# Inputs
# =============================================================================


@strawberry.enum
class AIAttributionBucketInput(Enum):
    """Canonical AI attribution buckets exposed to API clients."""

    AI_ASSISTED = "ai_assisted"
    AGENT_CREATED = "agent_created"
    AI_REVIEW = "ai_review"
    HUMAN = "human"
    UNKNOWN = "unknown"


@strawberry.enum
class AIWorkflowRootTypeInput(Enum):
    """Root entity types for AI workflow drilldown."""

    ISSUE = "issue"
    PR = "pr"
    WORK_UNIT = "work_unit"


@strawberry.input
class AIDateRangeInput:
    """Inclusive date range for AI analytics queries."""

    start_date: date
    end_date: date


@strawberry.input
class AIScopeInput:
    """Optional scope filters for AI analytics queries.

    All filters are AND-combined; ``None`` means "no filter at this level".
    """

    repo_id: str | None = None
    team_id: str | None = None
    work_type: str | None = None
    buckets: list[AIAttributionBucketInput] | None = None


# =============================================================================
# Outputs
# =============================================================================


@strawberry.type
class AILeverageComponents:
    """Decomposed Operating Leverage components.

    Each component is the contribution (positive = lift, negative = drag) of
    that dimension to the leverage executive metric.  Components are nullable
    when the underlying inputs were missing.
    """

    prs_component: float
    cycle_time_component: float | None = None
    review_component: float | None = None
    rework_component: float | None = None
    test_component: float | None = None
    incident_component: float | None = None


@strawberry.type
class AIImpactBucketRow:
    """Aggregated metric row for one (bucket, day) slice."""

    bucket: str
    prs_total: int
    prs_merged: int
    cycle_time_avg_hours: float | None
    reviews_per_pr: float | None
    changes_requested_per_pr: float | None
    rework_prs: int
    rework_rate: float | None
    revert_prs: int
    revert_rate: float | None
    incidents_count: int
    incident_rate: float | None
    test_gap_prs: int
    test_gap_rate: float | None


@strawberry.type
class AIImpactBucketTotals:
    """Totals across the requested time window for one bucket."""

    bucket: str
    prs_total: int
    prs_merged: int
    ai_assisted_pr_ratio: float | None
    agent_created_pr_count: int
    cycle_time_avg_hours: float | None
    ai_cycle_time_delta_hours: float | None
    ai_review_amplification: float | None
    rework_drag_rate: float | None
    revert_rate: float | None
    incident_drag_rate: float | None
    test_gap_rate: float | None
    leverage: AILeverageComponents


@strawberry.type
class AIImpactSummary:
    """Summary of AI workflow impact across a requested time range."""

    org_id: str
    start_date: date
    end_date: date
    total_prs: int
    ai_assisted_prs: int
    agent_created_prs: int
    human_prs: int
    unknown_prs: int
    ai_assisted_pr_ratio: float | None
    by_bucket: list[AIImpactBucketTotals]
    daily: list[AIImpactBucketRow]
    data_available: bool
    computed_at: datetime | None = None


@strawberry.type
class AIComparisonSide:
    """One side of an AI-assisted vs non-AI comparison."""

    bucket: str
    prs_total: int
    prs_merged: int
    cycle_time_avg_hours: float | None
    reviews_per_pr: float | None
    rework_rate: float | None
    revert_rate: float | None
    test_gap_rate: float | None
    incident_rate: float | None


@strawberry.type
class AIComparisonDelta:
    """AI side minus baseline side; null where inputs are missing."""

    cycle_time_delta_hours: float | None = None
    reviews_per_pr_delta: float | None = None
    rework_rate_delta: float | None = None
    revert_rate_delta: float | None = None
    test_gap_rate_delta: float | None = None
    incident_rate_delta: float | None = None


@strawberry.type
class AIComparison:
    """Side-by-side AI-assisted vs baseline comparison."""

    org_id: str
    start_date: date
    end_date: date
    ai_side: AIComparisonSide
    baseline_side: AIComparisonSide
    delta: AIComparisonDelta
    data_available: bool


@strawberry.type
class AIReviewLoadRow:
    """Per-bucket review-load row."""

    bucket: str
    prs_total: int
    reviews_total: int
    reviews_per_pr: float | None
    changes_requested_per_pr: float | None
    review_amplification: float | None


@strawberry.type
class AIReviewLoadResult:
    """Review-load breakdown across buckets and days."""

    org_id: str
    start_date: date
    end_date: date
    by_bucket: list[AIReviewLoadRow]
    daily: list[AIReviewLoadRow]
    data_available: bool


@strawberry.type
class AIRiskBreakdownRow:
    """Per-bucket risk row."""

    bucket: str
    prs_total: int
    rework_prs: int
    rework_rate: float | None
    revert_prs: int
    revert_rate: float | None
    test_gap_prs: int
    test_gap_rate: float | None
    incidents_count: int
    incident_rate: float | None


@strawberry.type
class AIRiskBreakdownResult:
    """Risk breakdown across buckets."""

    org_id: str
    start_date: date
    end_date: date
    by_bucket: list[AIRiskBreakdownRow]
    data_available: bool


@strawberry.enum
class AIOpportunityKind(Enum):
    """Canonical AI automation opportunity kinds.

    These are the targets recognised by the opportunity detector
    (delivered by CHAOS-1586). Listing them here keeps the contract
    stable even before the detector ships.
    """

    REPETITIVE_CHANGE = "repetitive_change"
    HIGH_REVIEW_LOAD = "high_review_load"
    HIGH_REWORK = "high_rework"
    SLOW_CYCLE = "slow_cycle"
    UNCOVERED_TEST_AREA = "uncovered_test_area"


@strawberry.type
class AIOpportunity:
    """A single AI automation candidate."""

    opportunity_id: str
    kind: AIOpportunityKind
    repo_id: str | None
    team_id: str | None
    title: str
    rationale: str
    score: float
    evidence_refs: list[str]


@strawberry.type
class AIOpportunitiesResult:
    """List of AI automation candidates.

    ``detector_ready`` is ``False`` until the opportunity detector
    (CHAOS-1586) lands; the contract is stable and returns an empty
    recommendation list so clients can render an empty state today.
    """

    org_id: str
    recommendations: list[AIOpportunity]
    detector_ready: bool


@strawberry.type
class AIGovernanceCoverageRow:
    """One day of governance coverage for a (team, repo) cell."""

    day: date
    team_id: str | None
    repo_id: str | None
    ai_artifacts: int
    declared_artifacts: int
    human_reviewed_prs: int
    security_scanned_prs: int
    in_policy_artifacts: int
    declaration_coverage: float
    human_review_coverage: float
    security_scan_coverage: float
    in_policy_coverage: float


@strawberry.type
class AIGovernanceViolationRow:
    """A persisted AI policy violation event."""

    rule_id: str
    severity: str
    subject_type: str
    subject_id: str
    team_id: str | None
    repo_id: str | None
    observed_at: datetime
    evidence: str


@strawberry.type
class AIGovernanceSummary:
    """Coverage + recent violations for the requested window."""

    org_id: str
    start_date: date
    end_date: date
    coverage: list[AIGovernanceCoverageRow]
    recent_violations: list[AIGovernanceViolationRow]
    data_available: bool


@strawberry.type
class AIWorkflowGraphNodeOut:
    """A node returned by AI workflow drilldown traversal."""

    node_type: str
    node_id: str


@strawberry.type
class AIWorkflowGraphEdgeOut:
    """A typed edge in an AI workflow traversal result.

    Edges always carry provenance (``source``), strength (``confidence``)
    and short evidence references so the UI can render explanations
    without re-querying.
    """

    edge_id: str
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    edge_type: str
    confidence: float
    source: str
    evidence: str
    provider: str | None = None
    repo_id: str | None = None


@strawberry.type
class AIWorkflowDrilldownResult:
    """Partial AI workflow graph rooted at an issue/PR/work-unit."""

    org_id: str
    root_type: str
    root_id: str
    nodes: list[AIWorkflowGraphNodeOut]
    edges: list[AIWorkflowGraphEdgeOut]
    partial: bool
    data_available: bool


@strawberry.type
class AiAttributedPr:
    """A single AI-attributed pull request candidate for drilldown selection.

    Rows are sourced directly from ``ai_attribution_resolved`` joined to
    ``git_pull_requests``. No aggregation, no fabrication — just the PRs that
    have an AI attribution signal in the requested window.
    """

    repo_id: strawberry.ID
    number: int
    title: str | None = None
    kind: str | None = None
    work_type: str | None = None
    team_id: str | None = None
    merged_at: datetime | None = None


@strawberry.type
class AiAttributedPrsResult:
    """Paginated list of AI-attributed PRs in the requested window."""

    org_id: str
    start_date: date
    end_date: date
    rows: list[AiAttributedPr]
    total: int
    has_more: bool
    data_available: bool
