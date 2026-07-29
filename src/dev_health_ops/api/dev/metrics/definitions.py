"""Code-owned Ask Dev V1 metric definitions.

This registry is deliberately separate from the broader reporting catalog.
Ask Dev may expose exactly these eight definitions and must fail closed for
every other Dev Health measure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from ..contracts import DirectScope, MetricID

MAX_RANGE_DAYS: Final[int] = 366
MAX_SERIES_POINTS: Final[int] = 366


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    metric_id: MetricID
    label: str
    owner: str
    description: str
    definition_version: str
    source_table: str
    source_version: str
    query_version: str
    unit: str
    aggregation: str
    display_precision: int
    null_semantics: str
    zero_semantics: str
    supported_scopes: tuple[DirectScope, ...]
    supports_team_filter: bool
    supported_dimensions: tuple[str, ...]
    min_range_days: int
    max_range_days: int
    supported_presets: tuple[int, ...]
    supported_time_grains: tuple[str, ...]
    comparison_rule: str
    freshness_policy: str
    expected_materialization: str
    upstream_sources: tuple[str, ...]
    sensitivity: str
    entitlement: str


_ALL_REPO_SCOPES = (
    DirectScope.ORGANIZATION,
    DirectScope.REPOSITORY,
    DirectScope.PROJECT,
    DirectScope.WORK_UNIT,
    DirectScope.ISSUE,
    DirectScope.PULL_REQUEST,
)
_WORK_SCOPES = (
    DirectScope.ORGANIZATION,
    DirectScope.PROJECT,
    DirectScope.WORK_UNIT,
)


def _definition(
    metric_id: MetricID,
    *,
    label: str,
    description: str,
    source_table: str,
    unit: str,
    aggregation: str,
    display_precision: int,
    null_semantics: str,
    zero_semantics: str,
    supported_scopes: tuple[DirectScope, ...],
    supports_team_filter: bool = False,
    supported_dimensions: tuple[str, ...] = (),
    upstream_sources: tuple[str, ...] = (),
    supported_time_grains: tuple[str, ...] = ("window", "day"),
) -> MetricDefinition:
    return MetricDefinition(
        metric_id=metric_id,
        label=label,
        owner="product-analytics",
        description=description,
        definition_version=f"{metric_id.value}.v1",
        source_table=source_table,
        source_version=f"{source_table}.v1",
        query_version=f"query-{metric_id.value}.v1",
        unit=unit,
        aggregation=aggregation,
        display_precision=display_precision,
        null_semantics=null_semantics,
        zero_semantics=zero_semantics,
        supported_scopes=supported_scopes,
        supports_team_filter=supports_team_filter,
        supported_dimensions=supported_dimensions,
        min_range_days=1,
        max_range_days=MAX_RANGE_DAYS,
        supported_presets=(7, 30, 90),
        supported_time_grains=supported_time_grains,
        comparison_rule="immediately_preceding_equal_duration",
        freshness_policy=f"{source_table}.daily.v1",
        expected_materialization="latest_completed_utc_day",
        upstream_sources=upstream_sources,
        sensitivity="authorized_organization_analytics",
        entitlement="ask_dev",
    )


METRIC_REGISTRY: Final[dict[MetricID, MetricDefinition]] = {
    MetricID.ITEMS_COMPLETED: _definition(
        MetricID.ITEMS_COMPLETED,
        label="Items completed",
        description="Completed work items in the selected window.",
        source_table="work_item_metrics_daily",
        unit="items",
        aggregation="sum_latest_daily_scope_rows",
        display_precision=0,
        null_semantics="No current rows means no match, not zero throughput.",
        zero_semantics="Zero means current source rows explicitly sum to no completions.",
        supported_scopes=_WORK_SCOPES,
        supports_team_filter=True,
        upstream_sources=("work_items",),
    ),
    MetricID.CYCLE_TIME_P50_HOURS: _definition(
        MetricID.CYCLE_TIME_P50_HOURS,
        label="Cycle time p50",
        description=(
            "Average of persisted daily/scope median work-item cycle times; "
            "it is never replaced with an arithmetic mean of raw cycle times."
        ),
        source_table="work_item_metrics_daily",
        unit="hours",
        aggregation="average_latest_daily_scope_p50",
        display_precision=1,
        null_semantics="No non-null persisted p50 values means no match.",
        zero_semantics="Zero is a measured zero-hour median, not missing data.",
        supported_scopes=_WORK_SCOPES,
        supports_team_filter=True,
        upstream_sources=("work_items",),
    ),
    MetricID.AVG_WIP: _definition(
        MetricID.AVG_WIP,
        label="Average WIP",
        description="Average persisted work-in-progress across daily status snapshots.",
        source_table="work_item_state_durations_daily",
        unit="items",
        aggregation="average_latest_daily_status_snapshots",
        display_precision=1,
        null_semantics="No persisted status snapshots means no match.",
        zero_semantics="Zero means measured snapshots contain no work in progress.",
        supported_scopes=_WORK_SCOPES,
        supports_team_filter=True,
        upstream_sources=("work_items",),
    ),
    MetricID.DEPLOYMENTS_COUNT: _definition(
        MetricID.DEPLOYMENTS_COUNT,
        label="Deployments",
        description="Deployments recorded by deployment sources in the selected window.",
        source_table="deploy_metrics_daily",
        unit="deployments",
        aggregation="sum_latest_daily_repository_rows",
        display_precision=0,
        null_semantics="No deployment-source rows means no match, not no deployments.",
        zero_semantics="Zero means available deployment rows explicitly contain no deployments.",
        supported_scopes=_ALL_REPO_SCOPES,
        upstream_sources=("deployments",),
    ),
    MetricID.CHANGE_FAILURE_RATE: _definition(
        MetricID.CHANGE_FAILURE_RATE,
        label="Change failure rate",
        description=(
            "DORA failed deployments divided by total deployments across the "
            "selected window and authorized repository scope."
        ),
        source_table="deploy_metrics_daily",
        unit="ratio",
        aggregation="sum_failed_deployments_divided_by_sum_deployments",
        display_precision=3,
        null_semantics=(
            "No deployment rows or a zero deployment denominator is insufficient "
            "evidence; no PR-revert or incident fallback is allowed."
        ),
        zero_semantics="Zero means deployments exist and none are recorded as failed.",
        supported_scopes=_ALL_REPO_SCOPES,
        upstream_sources=("deployments",),
    ),
    MetricID.INVESTMENT_ALLOCATION_PCT: _definition(
        MetricID.INVESTMENT_ALLOCATION_PCT,
        label="Investment allocation",
        description=(
            "Completed-work allocation across the five canonical investment themes; "
            "noncanonical and unclassified rows are excluded from the denominator."
        ),
        source_table="investment_metrics_daily",
        unit="percent",
        aggregation="canonical_theme_completed_work_share",
        display_precision=1,
        null_semantics=(
            "No canonical completed-work denominator means no match; unclassified "
            "work is reported through coverage rather than a fabricated theme."
        ),
        zero_semantics="A canonical theme absent from a non-empty denominator is zero percent.",
        supported_scopes=_ALL_REPO_SCOPES,
        supports_team_filter=True,
        supported_dimensions=("investment_theme",),
        upstream_sources=("work_items", "investment_classification"),
        supported_time_grains=("window",),
    ),
    MetricID.CYCLOMATIC_PER_KLOC: _definition(
        MetricID.CYCLOMATIC_PER_KLOC,
        label="Cyclomatic complexity per KLOC",
        description="Average persisted repository cyclomatic-complexity density.",
        source_table="repo_complexity_daily",
        unit="cyclomatic_per_kloc",
        aggregation="average_latest_daily_repository_density",
        display_precision=2,
        null_semantics="No repository complexity rows means no match.",
        zero_semantics="Zero is a persisted zero-density snapshot, not missing analysis.",
        supported_scopes=_ALL_REPO_SCOPES,
        upstream_sources=("git", "complexity_analysis"),
    ),
    MetricID.COMPOUNDING_RISK_SCORE: _definition(
        MetricID.COMPOUNDING_RISK_SCORE,
        label="Compounding risk score",
        description=(
            "Latest persisted inspectable composite risk score using the weights and "
            "components recorded at compute time."
        ),
        source_table="compounding_risk_daily",
        unit="score_0_to_1",
        aggregation="latest_snapshot_mean_across_scope",
        display_precision=2,
        null_semantics="A missing required component keeps the persisted score null.",
        zero_semantics="Zero is a complete measured low-risk score, not missing inputs.",
        supported_scopes=_ALL_REPO_SCOPES,
        supports_team_filter=True,
        upstream_sources=(
            "repo_metrics",
            "complexity_analysis",
            "ownership",
            "reviews",
        ),
        supported_time_grains=("snapshot",),
    ),
}


def list_metrics() -> tuple[MetricDefinition, ...]:
    """Return the exact V1 registry in canonical MetricID declaration order."""
    return tuple(METRIC_REGISTRY[metric_id] for metric_id in MetricID)


def get_metric(metric_id: MetricID | str) -> MetricDefinition:
    """Return one registered metric or fail closed for deferred/unknown IDs."""
    try:
        canonical = (
            metric_id if isinstance(metric_id, MetricID) else MetricID(metric_id)
        )
        return METRIC_REGISTRY[canonical]
    except (KeyError, ValueError) as exc:
        raise ValueError("Unsupported Ask Dev V1 metric") from exc
