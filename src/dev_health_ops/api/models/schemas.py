from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class Coverage(BaseModel):
    repos_covered_pct: float
    prs_linked_to_issues_pct: float
    issues_with_cycle_states_pct: float


class Freshness(BaseModel):
    last_ingested_at: datetime | None
    sources: dict[str, str]
    coverage: Coverage


class SparkPoint(BaseModel):
    ts: datetime
    value: float


class MetricDelta(BaseModel):
    metric: str
    label: str
    value: float
    unit: str
    delta_pct: float
    spark: list[SparkPoint]


class SummarySentence(BaseModel):
    id: str
    text: str
    evidence_link: str


class ConstraintEvidence(BaseModel):
    label: str
    link: str


class ConstraintCard(BaseModel):
    title: str
    claim: str
    evidence: list[ConstraintEvidence]
    experiments: list[str]


class EventItem(BaseModel):
    ts: datetime
    type: str
    text: str
    link: str


class HomeResponse(BaseModel):
    freshness: Freshness
    deltas: list[MetricDelta]
    summary: list[SummarySentence]
    tiles: dict[str, Any]
    constraint: ConstraintCard
    events: list[EventItem]


class Contributor(BaseModel):
    id: str
    label: str
    value: float
    delta_pct: float
    evidence_link: str


class ExplainResponse(BaseModel):
    metric: str
    label: str
    unit: str
    value: float
    delta_pct: float
    drivers: list[Contributor]
    contributors: list[Contributor]
    drilldown_links: dict[str, str]


class PullRequestRow(BaseModel):
    repo_id: str
    number: int
    title: str | None
    author: str | None
    created_at: datetime
    merged_at: datetime | None
    first_review_at: datetime | None
    review_latency_hours: float | None
    link: str | None


class IssueRow(BaseModel):
    work_item_id: str
    provider: str
    status: str
    team_id: str | None
    cycle_time_hours: float | None
    lead_time_hours: float | None
    started_at: datetime | None
    completed_at: datetime | None
    link: str | None


class DrilldownResponse(BaseModel):
    items: list[Any]


class OpportunityCard(BaseModel):
    id: str
    title: str
    rationale: str
    evidence_links: list[str]
    suggested_experiments: list[str]


class OpportunitiesResponse(BaseModel):
    items: list[OpportunityCard]


class HealthResponse(BaseModel):
    status: str
    services: dict[str, str]


class MetaResponse(BaseModel):
    """Backend metadata for /api/v1/meta endpoint."""

    backend: str
    version: str
    last_ingest_at: datetime | None
    coverage: dict[str, Any]
    limits: dict[str, int]
    supported_endpoints: list[str]


class InvestmentCategory(BaseModel):
    key: str
    name: str
    value: float


class InvestmentSubtype(BaseModel):
    name: str
    value: float
    parent_key: str = Field(alias="parentKey")

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)


class InvestmentResponse(BaseModel):
    theme_distribution: dict[str, float]
    subcategory_distribution: dict[str, float]
    evidence_quality_distribution: dict[str, float] | None = None
    evidence_quality_stats: EvidenceQualityStats | None = None
    unit: str | None = None
    edges: list[dict[str, Any]] | None = None


class InvestmentFindingEvidence(BaseModel):
    """Evidence backing a single finding."""

    theme: str
    subcategory: str | None = None
    share_pct: float
    delta_pct_points: float | None = None
    evidence_quality_mean: float | None = None
    evidence_quality_band: str | None = None


class InvestmentFinding(BaseModel):
    """A single finding from the investment mix analysis."""

    finding: str
    evidence: InvestmentFindingEvidence


class InvestmentConfidence(BaseModel):
    """Confidence metadata for the explanation."""

    level: Literal["high", "moderate", "low", "unknown"]
    quality_mean: float | None = None
    quality_stddev: float | None = None
    band_mix: dict[str, int] = Field(default_factory=dict)
    drivers: list[str] = Field(default_factory=list)


class InvestmentActionItem(BaseModel):
    """A suggested action item for follow-up."""

    action: str
    why: str
    where: str


class InvestmentMixExplanation(BaseModel):
    """Structured explanation for an investment mix view."""

    summary: str
    top_findings: list[InvestmentFinding] = Field(default_factory=list)
    confidence: InvestmentConfidence
    what_to_check_next: list[InvestmentActionItem] = Field(default_factory=list)
    anti_claims: list[str] = Field(default_factory=list)
    status: (
        Literal["valid", "invalid_json", "invalid_llm_output", "llm_unavailable"] | None
    ) = None


class WorkUnitTimeRange(BaseModel):
    start: datetime
    end: datetime


class WorkUnitEffort(BaseModel):
    metric: Literal["churn_loc", "active_hours"]
    value: float


class EvidenceQuality(BaseModel):
    value: float | None = None
    band: Literal["high", "moderate", "low", "very_low", "unknown"] | None = None


class EvidenceQualityStats(BaseModel):
    """Aggregated evidence quality statistics for a slice."""

    mean: float | None = None
    stddev: float | None = None
    band_counts: dict[str, int] = Field(default_factory=dict)
    quality_drivers: list[str] = Field(default_factory=list)


class WorkUnitEvidence(BaseModel):
    textual: list[dict[str, Any]] = Field(default_factory=list)
    structural: list[dict[str, Any]] = Field(default_factory=list)
    contextual: list[dict[str, Any]] = Field(default_factory=list)


class InvestmentBreakdown(BaseModel):
    themes: dict[str, float]
    subcategories: dict[str, float]


class WorkUnitInvestment(BaseModel):
    work_unit_id: str
    work_unit_type: str | None = None
    work_unit_name: str | None = None
    time_range: WorkUnitTimeRange
    effort: WorkUnitEffort
    investment: InvestmentBreakdown
    evidence_quality: EvidenceQuality
    evidence: WorkUnitEvidence


class WorkUnitExplanation(BaseModel):
    """LLM-generated explanation for a work unit's precomputed investment view."""

    work_unit_id: str
    ai_generated: bool = True
    summary: str  # Plain text explanation narrative
    category_rationale: dict[str, str]  # Why each category leans that way
    evidence_highlights: list[str]  # Which evidence mattered most
    uncertainty_disclosure: str  # Where uncertainty exists
    evidence_quality_limits: str  # Evidence quality statement


class InvestmentSunburstSlice(BaseModel):
    theme: str
    subcategory: str
    scope: str
    value: float


class PersonIdentity(BaseModel):
    provider: str
    handle: str


class PersonSummaryPerson(BaseModel):
    person_id: str
    display_name: str
    identities: list[PersonIdentity]


class PersonSearchResult(PersonSummaryPerson):
    active: bool


class PersonDelta(BaseModel):
    metric: str
    label: str
    value: float
    unit: str
    delta_pct: float
    spark: list[SparkPoint]


class WorkMixItem(BaseModel):
    key: str
    name: str
    value: float


class FlowStageItem(BaseModel):
    stage: str
    value: float
    unit: str


class CollaborationItem(BaseModel):
    label: str
    value: float


class CollaborationSection(BaseModel):
    review_load: list[CollaborationItem]
    handoff_points: list[CollaborationItem]


class PersonSummarySections(BaseModel):
    work_mix: list[WorkMixItem]
    flow_breakdown: list[FlowStageItem]
    collaboration: CollaborationSection


class PersonSummaryResponse(BaseModel):
    person: PersonSummaryPerson
    freshness: Freshness
    identity_coverage_pct: float
    deltas: list[PersonDelta]
    narrative: list[SummarySentence]
    sections: PersonSummarySections


class MetricDefinition(BaseModel):
    description: str
    interpretation: str


class MetricTimeseriesPoint(BaseModel):
    day: date
    value: float


class MetricBreakdownItem(BaseModel):
    label: str
    value: float


class PersonMetricBreakdowns(BaseModel):
    by_repo: list[MetricBreakdownItem]
    by_work_type: list[MetricBreakdownItem]
    by_stage: list[MetricBreakdownItem]


class DriverStatement(BaseModel):
    text: str
    link: str


class PersonMetricResponse(BaseModel):
    metric: str
    label: str
    definition: MetricDefinition
    timeseries: list[MetricTimeseriesPoint]
    breakdowns: PersonMetricBreakdowns
    drivers: list[DriverStatement]


class PersonDrilldownResponse(BaseModel):
    items: list[Any]
    next_cursor: datetime | None = None


class HeatmapAxes(BaseModel):
    x: list[str]
    y: list[str]


class HeatmapCell(BaseModel):
    x: str
    y: str
    value: float


class HeatmapLegend(BaseModel):
    unit: str
    scale: str


class HeatmapResponse(BaseModel):
    axes: HeatmapAxes
    cells: list[HeatmapCell]
    legend: HeatmapLegend
    evidence: list[dict[str, Any]] | None = None


class FlameTimeline(BaseModel):
    start: datetime
    end: datetime


class FlameFrame(BaseModel):
    id: str
    parent_id: str | None
    label: str
    start: datetime
    end: datetime
    state: str
    category: str


class FlameResponse(BaseModel):
    entity: dict[str, Any]
    timeline: FlameTimeline
    frames: list[FlameFrame]


class QuadrantAxis(BaseModel):
    metric: str
    label: str
    unit: str


class QuadrantAxes(BaseModel):
    x: QuadrantAxis
    y: QuadrantAxis


class QuadrantPointTrajectory(BaseModel):
    x: float
    y: float
    window: str


class QuadrantPoint(BaseModel):
    entity_id: str
    entity_label: str
    x: float
    y: float
    window_start: date
    window_end: date
    evidence_link: str
    trajectory: list[QuadrantPointTrajectory] | None = None


class QuadrantAnnotation(BaseModel):
    type: str
    description: str
    x_range: list[float]
    y_range: list[float]


class QuadrantResponse(BaseModel):
    axes: QuadrantAxes
    points: list[QuadrantPoint]
    annotations: list[QuadrantAnnotation]


class SankeyNode(BaseModel):
    name: str
    group: str | None = None
    value: float | None = None


class SankeyLink(BaseModel):
    source: str
    target: str
    value: float


class SankeyResponse(BaseModel):
    mode: Literal["investment", "expense", "state", "hotspot"]
    nodes: list[SankeyNode]
    links: list[SankeyLink]
    unit: str | None = None
    label: str | None = None
    description: str | None = None
    team_coverage: float | None = None
    repo_coverage: float | None = None
    distinct_team_targets: int | None = None
    distinct_repo_targets: int | None = None
    chosen_mode: str | None = None
    coverage: dict[str, float] | None = None
    unassigned_reasons: dict[str, int] | None = None
    flow_mode: str | None = None
    drill_category: str | None = None
    top_n_repos: int | None = None


# Aggregated flame graph models (hierarchical tree format)


class AggregatedFlameNode(BaseModel):
    """A node in a hierarchical flame graph tree."""

    name: str
    value: float
    children: list[AggregatedFlameNode] = []


class ApproximationInfo(BaseModel):
    """Info about data approximation when exact data unavailable."""

    used: bool = False
    method: str | None = None


class AggregatedFlameMeta(BaseModel):
    """Metadata for an aggregated flame response."""

    window_start: date
    window_end: date
    filters: dict[str, Any] = {}
    notes: list[str] = []
    approximation: ApproximationInfo = Field(default_factory=ApproximationInfo)


class AggregatedFlameResponse(BaseModel):
    """Response for aggregated flame graph modes."""

    mode: Literal["cycle_breakdown", "code_hotspots", "throughput"]
    unit: str
    root: AggregatedFlameNode
    meta: AggregatedFlameMeta
