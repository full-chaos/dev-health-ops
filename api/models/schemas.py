from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field

try:
    from pydantic import ConfigDict
except ImportError:  # pragma: no cover - pydantic v1 fallback
    ConfigDict = None


class Coverage(BaseModel):
    repos_covered_pct: float
    prs_linked_to_issues_pct: float
    issues_with_cycle_states_pct: float


class Freshness(BaseModel):
    last_ingested_at: Optional[datetime]
    sources: Dict[str, str]
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
    spark: List[SparkPoint]


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
    evidence: List[ConstraintEvidence]
    experiments: List[str]


class EventItem(BaseModel):
    ts: datetime
    type: str
    text: str
    link: str


class HomeResponse(BaseModel):
    freshness: Freshness
    deltas: List[MetricDelta]
    summary: List[SummarySentence]
    tiles: Dict[str, Any]
    constraint: ConstraintCard
    events: List[EventItem]


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
    drivers: List[Contributor]
    contributors: List[Contributor]
    drilldown_links: Dict[str, str]


class PullRequestRow(BaseModel):
    repo_id: str
    number: int
    title: Optional[str]
    author: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime]
    first_review_at: Optional[datetime]
    review_latency_hours: Optional[float]
    link: Optional[str]


class IssueRow(BaseModel):
    work_item_id: str
    provider: str
    status: str
    team_id: Optional[str]
    cycle_time_hours: Optional[float]
    lead_time_hours: Optional[float]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    link: Optional[str]


class DrilldownResponse(BaseModel):
    items: List[Any]


class OpportunityCard(BaseModel):
    id: str
    title: str
    rationale: str
    evidence_links: List[str]
    suggested_experiments: List[str]


class OpportunitiesResponse(BaseModel):
    items: List[OpportunityCard]


class HealthResponse(BaseModel):
    status: str
    services: Dict[str, str]


class MetaResponse(BaseModel):
    """Backend metadata for /api/v1/meta endpoint."""

    backend: str
    version: str
    last_ingest_at: Optional[datetime]
    coverage: Dict[str, Any]
    limits: Dict[str, int]
    supported_endpoints: List[str]


class InvestmentCategory(BaseModel):
    key: str
    name: str
    value: float


class InvestmentSubtype(BaseModel):
    name: str
    value: float
    parent_key: str = Field(alias="parentKey")

    if ConfigDict is not None:
        model_config = ConfigDict(validate_by_name=True)
    else:

        class Config:
            allow_population_by_field_name = True


class InvestmentResponse(BaseModel):
    theme_distribution: Dict[str, float]
    subcategory_distribution: Dict[str, float]
    evidence_quality_distribution: Optional[Dict[str, float]] = None
    evidence_quality_stats: Optional["EvidenceQualityStats"] = None
    unit: Optional[str] = None
    edges: Optional[List[Dict[str, Any]]] = None


class InvestmentFindingEvidence(BaseModel):
    """Evidence backing a single finding."""

    theme: str
    subcategory: Optional[str] = None
    share_pct: float
    delta_pct_points: Optional[float] = None
    evidence_quality_mean: Optional[float] = None
    evidence_quality_band: Optional[str] = None


class InvestmentFinding(BaseModel):
    """A single finding from the investment mix analysis."""

    finding: str
    evidence: InvestmentFindingEvidence


class InvestmentConfidence(BaseModel):
    """Confidence metadata for the explanation."""

    level: Literal["high", "moderate", "low", "unknown"]
    quality_mean: Optional[float] = None
    quality_stddev: Optional[float] = None
    band_mix: Dict[str, int] = Field(default_factory=dict)
    drivers: List[str] = Field(default_factory=list)


class InvestmentActionItem(BaseModel):
    """A suggested action item for follow-up."""

    action: str
    why: str
    where: str


class InvestmentMixExplanation(BaseModel):
    """Structured explanation for an investment mix view."""

    summary: str
    top_findings: List[InvestmentFinding] = Field(default_factory=list)
    confidence: InvestmentConfidence
    what_to_check_next: List[InvestmentActionItem] = Field(default_factory=list)
    anti_claims: List[str] = Field(default_factory=list)
    status: Optional[Literal["valid", "invalid_json", "invalid_llm_output"]] = None


class WorkUnitTimeRange(BaseModel):
    start: datetime
    end: datetime


class WorkUnitEffort(BaseModel):
    metric: Literal["churn_loc", "active_hours"]
    value: float


class EvidenceQuality(BaseModel):
    value: Optional[float] = None
    band: Optional[Literal["high", "moderate", "low", "very_low", "unknown"]] = None


class EvidenceQualityStats(BaseModel):
    """Aggregated evidence quality statistics for a slice."""

    mean: Optional[float] = None
    stddev: Optional[float] = None
    band_counts: Dict[str, int] = Field(default_factory=dict)
    quality_drivers: List[str] = Field(default_factory=list)


class WorkUnitEvidence(BaseModel):
    textual: List[Dict[str, Any]] = Field(default_factory=list)
    structural: List[Dict[str, Any]] = Field(default_factory=list)
    contextual: List[Dict[str, Any]] = Field(default_factory=list)


class InvestmentBreakdown(BaseModel):
    themes: Dict[str, float]
    subcategories: Dict[str, float]


class WorkUnitInvestment(BaseModel):
    work_unit_id: str
    work_unit_type: Optional[str] = None
    work_unit_name: Optional[str] = None
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
    category_rationale: Dict[str, str]  # Why each category leans that way
    evidence_highlights: List[str]  # Which evidence mattered most
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
    identities: List[PersonIdentity]


class PersonSearchResult(PersonSummaryPerson):
    active: bool


class PersonDelta(BaseModel):
    metric: str
    label: str
    value: float
    unit: str
    delta_pct: float
    spark: List[SparkPoint]


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
    review_load: List[CollaborationItem]
    handoff_points: List[CollaborationItem]


class PersonSummarySections(BaseModel):
    work_mix: List[WorkMixItem]
    flow_breakdown: List[FlowStageItem]
    collaboration: CollaborationSection


class PersonSummaryResponse(BaseModel):
    person: PersonSummaryPerson
    freshness: Freshness
    identity_coverage_pct: float
    deltas: List[PersonDelta]
    narrative: List[SummarySentence]
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
    by_repo: List[MetricBreakdownItem]
    by_work_type: List[MetricBreakdownItem]
    by_stage: List[MetricBreakdownItem]


class DriverStatement(BaseModel):
    text: str
    link: str


class PersonMetricResponse(BaseModel):
    metric: str
    label: str
    definition: MetricDefinition
    timeseries: List[MetricTimeseriesPoint]
    breakdowns: PersonMetricBreakdowns
    drivers: List[DriverStatement]


class PersonDrilldownResponse(BaseModel):
    items: List[Any]
    next_cursor: Optional[datetime] = None


class HeatmapAxes(BaseModel):
    x: List[str]
    y: List[str]


class HeatmapCell(BaseModel):
    x: str
    y: str
    value: float


class HeatmapLegend(BaseModel):
    unit: str
    scale: str


class HeatmapResponse(BaseModel):
    axes: HeatmapAxes
    cells: List[HeatmapCell]
    legend: HeatmapLegend
    evidence: Optional[List[Dict[str, Any]]] = None


class FlameTimeline(BaseModel):
    start: datetime
    end: datetime


class FlameFrame(BaseModel):
    id: str
    parent_id: Optional[str]
    label: str
    start: datetime
    end: datetime
    state: str
    category: str


class FlameResponse(BaseModel):
    entity: Dict[str, Any]
    timeline: FlameTimeline
    frames: List[FlameFrame]


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
    trajectory: Optional[List[QuadrantPointTrajectory]] = None


class QuadrantAnnotation(BaseModel):
    type: str
    description: str
    x_range: List[float]
    y_range: List[float]


class QuadrantResponse(BaseModel):
    axes: QuadrantAxes
    points: List[QuadrantPoint]
    annotations: List[QuadrantAnnotation]


class SankeyNode(BaseModel):
    name: str
    group: Optional[str] = None
    value: Optional[float] = None


class SankeyLink(BaseModel):
    source: str
    target: str
    value: float


class SankeyResponse(BaseModel):
    mode: Literal["investment", "expense", "state", "hotspot"]
    nodes: List[SankeyNode]
    links: List[SankeyLink]
    unit: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    team_coverage: Optional[float] = None
    repo_coverage: Optional[float] = None
    distinct_team_targets: Optional[int] = None
    distinct_repo_targets: Optional[int] = None
    chosen_mode: Optional[str] = None
    coverage: Optional[Dict[str, float]] = None
    unassigned_reasons: Optional[Dict[str, int]] = None
    flow_mode: Optional[str] = None
    drill_category: Optional[str] = None
    top_n_repos: Optional[int] = None


# Aggregated flame graph models (hierarchical tree format)


class AggregatedFlameNode(BaseModel):
    """A node in a hierarchical flame graph tree."""

    name: str
    value: float
    children: List["AggregatedFlameNode"] = []


class ApproximationInfo(BaseModel):
    """Info about data approximation when exact data unavailable."""

    used: bool = False
    method: Optional[str] = None


class AggregatedFlameMeta(BaseModel):
    """Metadata for an aggregated flame response."""

    window_start: date
    window_end: date
    filters: Dict[str, Any] = {}
    notes: List[str] = []
    approximation: ApproximationInfo = Field(default_factory=ApproximationInfo)


class AggregatedFlameResponse(BaseModel):
    """Response for aggregated flame graph modes."""

    mode: Literal["cycle_breakdown", "code_hotspots", "throughput"]
    unit: str
    root: AggregatedFlameNode
    meta: AggregatedFlameMeta
