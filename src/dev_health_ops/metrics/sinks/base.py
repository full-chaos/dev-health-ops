"""
Base interface for metrics sinks.

All sink implementations must derive from BaseMetricsSink and implement the
abstract methods. This ensures consistent behavior across analytics sink
implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from dev_health_ops.metrics.schemas import (
    CapacityForecastRecord,
    CICDMetricsDailyRecord,
    CommitMetricsRecord,
    DeployMetricsDailyRecord,
    DORAMetricsRecord,
    FeatureFlagEventRecord,
    FeatureFlagLinkRecord,
    FeatureFlagRecord,
    FileComplexitySnapshot,
    FileHotspotDaily,
    FileMetricsRecord,
    ICLandscapeRollingRecord,
    IncidentMetricsDailyRecord,
    InvestmentClassificationRecord,
    InvestmentExplanationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
    ReleaseImpactDailyRecord,
    RepoComplexityDaily,
    RepoMetricsDailyRecord,
    ReviewEdgeDailyRecord,
    TeamMetricsDailyRecord,
    TelemetrySignalBucketRecord,
    WorkGraphEdgeRecord,
    WorkGraphIssuePRRecord,
    WorkGraphPRCommitRecord,
    WorkItemCycleTimeRecord,
    WorkItemMetricsDailyRecord,
    WorkItemStateDurationDailyRecord,
    WorkItemUserMetricsDailyRecord,
    WorkUnitInvestmentEvidenceQuoteRecord,
    WorkUnitInvestmentRecord,
)
from dev_health_ops.metrics.testops_schemas import (
    BenchmarkAnomalyRecord,
    BenchmarkBaselineRecord,
    BenchmarkInsightRecord,
    CoverageMetricsDailyRecord,
    MaturityBandRecord,
    MetricCorrelationRecord,
    PeriodComparisonRecord,
    PipelineMetricsDailyRecord,
    PipelineStabilityRecord,
    QualityDragRecord,
    ReleaseConfidenceRecord,
    TestMetricsDailyRecord,
)
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
    Worklog,
)


class BaseMetricsSink(ABC):
    """
    Abstract base class for metrics sinks.

    Sinks are responsible for persisting derived metrics data. Each backend
    (ClickHouse) implements this interface
    with backend-specific optimizations (e.g., bulk inserts, upserts).

    Lifecycle:
        1. Create sink instance with connection string/config
        2. Call ensure_schema() to create tables/indexes
        3. Call write_*() methods to persist metrics
        4. Call close() when done

    Example:
        sink = create_sink("clickhouse://localhost:8123/default")
        try:
            sink.ensure_schema()
            sink.write_repo_metrics(rows)
        finally:
            sink.close()
    """

    @abstractmethod
    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries."""
        raise NotImplementedError(
            "BaseMetricsSink.query_dicts() must be implemented by subclasses."
        )

    async def __aenter__(self) -> BaseMetricsSink:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.close()

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Return the backend type identifier (clickhouse)."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close connections and release resources."""
        ...

    @abstractmethod
    def ensure_schema(self) -> None:
        """
        Create tables and indexes if they don't exist.

        For ClickHouse: runs SQL migration files.
        """
        ...

    # -------------------------------------------------------------------------
    # Core metrics write methods
    # -------------------------------------------------------------------------

    @abstractmethod
    def write_repo_metrics(self, rows: Sequence[RepoMetricsDailyRecord]) -> None:
        """Write daily repo-level metrics."""
        ...

    @abstractmethod
    def write_commit_metrics(self, rows: Sequence[CommitMetricsRecord]) -> None:
        """Write per-commit metrics."""
        ...

    @abstractmethod
    def write_file_metrics(self, rows: Sequence[FileMetricsRecord]) -> None:
        """Write daily file-level metrics (churn, hotspots)."""
        ...

    @abstractmethod
    def write_team_metrics(self, rows: Sequence[TeamMetricsDailyRecord]) -> None:
        """Write daily team-level metrics."""
        ...

    # -------------------------------------------------------------------------
    # Work item metrics
    # -------------------------------------------------------------------------

    @abstractmethod
    def write_work_item_metrics(
        self, rows: Sequence[WorkItemMetricsDailyRecord]
    ) -> None:
        """Write daily aggregate work item metrics."""
        ...

    @abstractmethod
    def write_work_item_user_metrics(
        self, rows: Sequence[WorkItemUserMetricsDailyRecord]
    ) -> None:
        """Write daily per-user work item metrics."""
        ...

    @abstractmethod
    def write_work_item_cycle_times(
        self, rows: Sequence[WorkItemCycleTimeRecord]
    ) -> None:
        """Write individual work item cycle time records."""
        ...

    @abstractmethod
    def write_work_item_state_durations(
        self, rows: Sequence[WorkItemStateDurationDailyRecord]
    ) -> None:
        """Write work item state duration records."""
        ...

    # -------------------------------------------------------------------------
    # Collaboration / review metrics
    # -------------------------------------------------------------------------

    @abstractmethod
    def write_review_edges(self, rows: Sequence[ReviewEdgeDailyRecord]) -> None:
        """Write daily review relationship edges (author->reviewer)."""
        ...

    @abstractmethod
    def write_ic_landscape_rolling(
        self, rows: Sequence[ICLandscapeRollingRecord]
    ) -> None:
        """Write rolling IC landscape metrics (30-day windows)."""
        ...

    # -------------------------------------------------------------------------
    # DORA / CI-CD metrics
    # -------------------------------------------------------------------------

    @abstractmethod
    def write_cicd_metrics(self, rows: Sequence[CICDMetricsDailyRecord]) -> None:
        """Write daily CI/CD pipeline metrics."""
        ...

    @abstractmethod
    def write_deploy_metrics(self, rows: Sequence[DeployMetricsDailyRecord]) -> None:
        """Write daily deployment metrics."""

    @abstractmethod
    def write_incident_metrics(
        self, rows: Sequence[IncidentMetricsDailyRecord]
    ) -> None:
        """Write daily incident metrics."""

    @abstractmethod
    def write_dora_metrics(self, rows: Sequence[DORAMetricsRecord]) -> None:
        """Write pre-computed DORA metrics from providers."""

    @abstractmethod
    def write_testops_pipeline_metrics(
        self, rows: Sequence[PipelineMetricsDailyRecord]
    ) -> None:
        """Write daily TestOps pipeline health metrics."""

    @abstractmethod
    def write_testops_test_metrics(
        self, rows: Sequence[TestMetricsDailyRecord]
    ) -> None:
        """Write daily TestOps test reliability metrics."""

    @abstractmethod
    def write_testops_coverage_metrics(
        self, rows: Sequence[CoverageMetricsDailyRecord]
    ) -> None:
        """Write daily TestOps coverage metrics."""

    def write_release_confidence(self, rows: Sequence[ReleaseConfidenceRecord]) -> None:
        pass

    def write_quality_drag(self, rows: Sequence[QualityDragRecord]) -> None:
        pass

    def write_pipeline_stability(self, rows: Sequence[PipelineStabilityRecord]) -> None:
        pass

    def write_period_comparisons(self, rows: Sequence[PeriodComparisonRecord]) -> None:
        pass

    def write_benchmark_baselines(
        self, rows: Sequence[BenchmarkBaselineRecord]
    ) -> None:
        pass

    def write_maturity_bands(self, rows: Sequence[MaturityBandRecord]) -> None:
        pass

    def write_benchmark_anomalies(self, rows: Sequence[BenchmarkAnomalyRecord]) -> None:
        pass

    def write_metric_correlations(
        self, rows: Sequence[MetricCorrelationRecord]
    ) -> None:
        pass

    def write_benchmark_insights(self, rows: Sequence[BenchmarkInsightRecord]) -> None:
        pass

    # -------------------------------------------------------------------------
    # Complexity / hotspot metrics
    # -------------------------------------------------------------------------

    @abstractmethod
    def write_file_complexity_snapshots(
        self, rows: Sequence[FileComplexitySnapshot]
    ) -> None:
        """Write file-level complexity snapshots."""

    @abstractmethod
    def write_repo_complexity_daily(self, rows: Sequence[RepoComplexityDaily]) -> None:
        """Write daily repo-level complexity aggregates."""

    @abstractmethod
    def write_file_hotspot_daily(self, rows: Sequence[FileHotspotDaily]) -> None:
        """Write daily file hotspot records."""

    # -------------------------------------------------------------------------
    # Investment / issue type metrics
    # -------------------------------------------------------------------------

    @abstractmethod
    def write_investment_classifications(
        self, rows: Sequence[InvestmentClassificationRecord]
    ) -> None:
        """Write investment area classifications for artifacts."""
        pass

    @abstractmethod
    def write_investment_metrics(self, rows: Sequence[InvestmentMetricsRecord]) -> None:
        """Write aggregated investment metrics by area/team."""
        pass

    @abstractmethod
    def write_issue_type_metrics(self, rows: Sequence[IssueTypeMetricsRecord]) -> None:
        """Write aggregated metrics by issue type."""
        pass

    @abstractmethod
    def write_feature_flags(self, rows: Sequence[FeatureFlagRecord]) -> None:
        """Write feature flag registry records."""
        pass

    @abstractmethod
    def write_feature_flag_events(self, rows: Sequence[FeatureFlagEventRecord]) -> None:
        """Write feature flag lifecycle event records."""
        pass

    @abstractmethod
    def write_feature_flag_links(self, rows: Sequence[FeatureFlagLinkRecord]) -> None:
        """Write feature flag linkage records."""
        pass

    @abstractmethod
    def write_telemetry_signal_buckets(
        self, rows: Sequence[TelemetrySignalBucketRecord]
    ) -> None:
        """Write aggregated telemetry signal bucket records."""
        pass

    @abstractmethod
    def write_release_impact_daily(
        self, rows: Sequence[ReleaseImpactDailyRecord]
    ) -> None:
        """Write daily release impact rollup records."""
        pass

    # -------------------------------------------------------------------------
    # Work unit investment materialization
    # -------------------------------------------------------------------------

    @abstractmethod
    def write_work_unit_investments(
        self, rows: Sequence[WorkUnitInvestmentRecord]
    ) -> None:
        """Write work unit-level investment materializations."""
        pass

    @abstractmethod
    def write_work_unit_investment_quotes(
        self, rows: Sequence[WorkUnitInvestmentEvidenceQuoteRecord]
    ) -> None:
        """Write extractive evidence quotes for work unit investment records."""
        pass

    # -------------------------------------------------------------------------
    # Investment explanation caching
    # -------------------------------------------------------------------------

    def write_investment_explanation(self, record: InvestmentExplanationRecord) -> None:
        """Write or replace an investment explanation to the cache."""
        pass

    def read_investment_explanation(
        self, cache_key: str
    ) -> InvestmentExplanationRecord | None:
        """Read a cached investment explanation by cache_key."""
        return None

    # -------------------------------------------------------------------------
    # Work graph (derived relationships)
    # -------------------------------------------------------------------------

    def write_work_graph_edges(self, rows: Sequence[WorkGraphEdgeRecord]) -> None:
        """Write derived work graph edges."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support work graph edges"
        )

    def write_work_graph_issue_pr(self, rows: Sequence[WorkGraphIssuePRRecord]) -> None:
        """Write derived issue↔PR link rows."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support work graph issue↔PR links"
        )

    def write_work_graph_pr_commit(
        self, rows: Sequence[WorkGraphPRCommitRecord]
    ) -> None:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support work graph PR↔commit links"
        )

    # -------------------------------------------------------------------------
    # Capacity planning forecasts
    # -------------------------------------------------------------------------

    def write_capacity_forecasts(self, rows: Sequence[CapacityForecastRecord]) -> None:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support capacity forecasts"
        )

    # -------------------------------------------------------------------------
    # Team resolution / identity support
    # -------------------------------------------------------------------------

    async def get_all_teams(self) -> list[Any]:
        """Fetch all teams from the database for identity resolution."""
        return []

    async def insert_teams(self, teams: list[Any]) -> None:
        """Insert or update teams in the database."""
        pass

    # -------------------------------------------------------------------------
    # Raw collection write methods (optional per sink)
    # -------------------------------------------------------------------------

    def write_work_items(self, work_items: Sequence[WorkItem]) -> None:
        """Write raw work items."""
        pass

    def write_work_item_transitions(
        self, transitions: Sequence[WorkItemStatusTransition]
    ) -> None:
        """Write raw work item status transitions."""
        pass

    def write_work_item_dependencies(self, rows: Sequence[WorkItemDependency]) -> None:
        """Write raw work item dependencies."""
        pass

    def write_work_item_reopen_events(
        self, rows: Sequence[WorkItemReopenEvent]
    ) -> None:
        """Write raw work item reopen events."""
        pass

    def write_work_item_interactions(
        self, rows: Sequence[WorkItemInteractionEvent]
    ) -> None:
        """Write raw work item interaction events."""
        pass

    def write_sprints(self, rows: Sequence[Sprint]) -> None:
        """Write raw sprint records."""
        pass

    def write_worklogs(self, rows: Sequence[Worklog]) -> None:
        """Write raw worklog records."""
        pass
