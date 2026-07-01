"""Resolvers for AI workflow analytics GraphQL queries.

This resolver layer is purely **read-only** and never performs persistence.
Each function:

1. Validates ``org_id`` and the optional scope.
2. Loads pre-computed AI rows from ClickHouse via the existing
   ``AIImpactClickHouseLoader`` and ``AIGovernanceLoader`` helpers.
3. Aggregates, projects, and returns Strawberry types.

Categorisation and metric computation never happen at request time — the
loaders only read from ``ai_impact_metrics_daily``, ``ai_governance_*`` and
``ai_workflow_*`` tables that the metrics/governance jobs already populate.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict
from datetime import datetime, time, timezone
from typing import Any

import strawberry

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.api.utils.numeric import safe_optional_float
from dev_health_ops.metrics.ai_impact import (
    AI_BUCKETS as AI_ATTRIBUTION_BUCKETS,
)
from dev_health_ops.metrics.ai_impact import (
    AttributionBucket,
)
from dev_health_ops.metrics.loaders.ai_attribution import AIAttributionClickHouseLoader
from dev_health_ops.metrics.loaders.ai_impact import AIImpactClickHouseLoader
from dev_health_ops.metrics.opportunities.ai_detector import AIOpportunityDetector

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.ai import (
    AiAttributedPr,
    AiAttributedPrsResult,
    AIAttributionEvidenceRow,
    AIAttributionMixRow,
    AIAttributionOverviewResult,
    AIAttributionScopeInput,
    AIComparison,
    AIComparisonDelta,
    AIComparisonSide,
    AIComplexityOverlapRow,
    AIDateRangeInput,
    AIGovernanceCoverageRow,
    AIGovernanceSummary,
    AIGovernanceViolationRow,
    AIHotspotOverlapRow,
    AIImpactBucketRow,
    AIImpactBucketTotals,
    AIImpactScopeRollupRow,
    AIImpactSummary,
    AILeverageComponents,
    AIMissingState,
    AIOpportunitiesResult,
    AIReviewerConcentrationSummary,
    AIReviewLoadResult,
    AIReviewLoadRow,
    AIRiskBreakdownResult,
    AIRiskBreakdownRow,
    AIScopeInput,
    AIWorkflowDrilldownResult,
    AIWorkflowGraphEdgeOut,
    AIWorkflowGraphNodeOut,
    AIWorkflowRootTypeInput,
)

logger = logging.getLogger(__name__)

_BASELINE_BUCKET: AttributionBucket = AttributionBucket.HUMAN


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for AI analytics resolver")
    return context.client


def _parse_uuid(value: str | None) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(value)
    except (TypeError, ValueError) as exc:
        logger.debug("Invalid UUID %r in AI analytics scope: %s", value, exc)
        return None


async def _resolve_repo_ref(
    client: Any, org_id: str, raw_repo_id: str | None
) -> uuid.UUID | None:
    if not raw_repo_id:
        return None
    try:
        return uuid.UUID(raw_repo_id)
    except (TypeError, ValueError):
        pass
    rows = await query_dicts(
        client,
        """
        SELECT id
        FROM repos
        WHERE org_id = {org_id:String}
          AND repo = {slug:String}
        ORDER BY toString(id)
        """,
        {"org_id": org_id, "slug": raw_repo_id},
    )
    if not rows:
        return None
    value = rows[0].get("id")
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


async def _normalize_scope(
    context: GraphQLContext,
    org_id: str,
    scope: AIScopeInput | None,
) -> tuple[uuid.UUID | None, str | None, str | None, bool]:
    if scope is None:
        return None, None, None, False
    repo_id = await _resolve_repo_ref(_require_client(context), org_id, scope.repo_id)
    return (
        repo_id,
        scope.team_id or None,
        scope.work_type or None,
        bool(scope.repo_id) and repo_id is None,
    )


async def _normalize_attribution_scope(
    context: GraphQLContext,
    org_id: str,
    scope: AIAttributionScopeInput | None,
) -> tuple[uuid.UUID | None, str | None, list[str] | None, bool]:
    """Normalize the narrower attribution-overview scope (no ``work_type``).

    ``ai_attribution_resolved`` has no ``work_type`` column, so that
    dimension is not exposed on :class:`AIAttributionScopeInput` at all
    (CHAOS-2744) -- there is nothing to silently drop here. ``buckets``
    maps 1:1 onto the resolved view's own ``kind`` column.
    """
    if scope is None:
        return None, None, None, False
    kinds = [bucket.value for bucket in scope.buckets] if scope.buckets else None
    repo_id = await _resolve_repo_ref(_require_client(context), org_id, scope.repo_id)
    return (
        repo_id,
        scope.team_id or None,
        kinds,
        bool(scope.repo_id) and repo_id is None,
    )


async def _normalize_opportunity_scope(
    context: GraphQLContext, org_id: str, scope: AIScopeInput | None
) -> tuple[AIScopeInput | None, bool]:
    if scope is None or not scope.repo_id:
        return scope, False
    repo_id = await _resolve_repo_ref(_require_client(context), org_id, scope.repo_id)
    if repo_id is None:
        return None, True
    if scope.repo_id == str(repo_id):
        return scope, False
    return (
        AIScopeInput(
            repo_id=str(repo_id),
            team_id=scope.team_id,
            work_type=scope.work_type,
            buckets=scope.buckets,
        ),
        False,
    )


def _validate_date_range(date_range: AIDateRangeInput) -> None:
    if date_range.end_date < date_range.start_date:
        raise ValueError(
            "AI analytics date range end_date must be >= start_date "
            f"(got start={date_range.start_date}, "
            f"end={date_range.end_date})"
        )


async def _load_daily_records(
    context: GraphQLContext,
    org_id: str,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None,
) -> list[Any]:
    """Load daily AI impact records honoring the scope filter."""

    _validate_date_range(date_range)
    repo_id, team_id, work_type, repo_unresolved = await _normalize_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return []
    loader = AIImpactClickHouseLoader(_require_client(context), org_id=org_id)
    return await loader.load_ai_impact_metrics(
        start_day=date_range.start_date,
        end_day=date_range.end_date,
        repo_id=repo_id,
        team_id=team_id,
        work_type=work_type,
    )


def _ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _weighted_avg(
    pairs: list[tuple[float | None, float]],
) -> float | None:
    """Combine per-day averages weighted by sample size."""
    total_weight = 0.0
    total_value = 0.0
    for value, weight in pairs:
        if value is None or weight <= 0:
            continue
        total_weight += weight
        total_value += value * weight
    if total_weight == 0:
        return None
    return total_value / total_weight


def _bucket_filter(
    rows: list[Any], buckets: list[AttributionBucket] | None
) -> list[Any]:
    """Drop rows whose ``attribution_bucket`` is outside ``buckets``."""
    if not buckets:
        return list(rows)
    bucket_set = {bucket.value for bucket in buckets}
    return [row for row in rows if row.attribution_bucket in bucket_set]


def _empty_leverage() -> AILeverageComponents:
    return AILeverageComponents(prs_component=0.0)


def _missing_state(key: str, title: str, guidance: str) -> AIMissingState:
    return AIMissingState(key=key, title=title, guidance=guidance)


def _unknown_attribution_missing_state(unknown_prs: int) -> AIMissingState | None:
    if unknown_prs <= 0:
        return None
    return _missing_state(
        "unknown_attribution",
        "Unknown attribution needs follow-up",
        (
            "Some PRs could not be attributed to AI-assisted, agent-created, "
            "AI-reviewed, or human buckets. Treat this as a coverage gap for "
            "labels, trailers, bot identities, or CI annotations, not as a "
            "person-level usage signal."
        ),
    )


def _row_to_impact_daily(row: Any) -> AIImpactBucketRow:
    test_gap_rate = row.test_gap_rate
    return AIImpactBucketRow(
        bucket=row.attribution_bucket,
        prs_total=row.prs_total,
        prs_merged=row.prs_merged,
        cycle_time_avg_hours=row.cycle_time_avg_hours,
        reviews_per_pr=row.reviews_per_pr,
        changes_requested_per_pr=row.changes_requested_per_pr,
        rework_prs=row.rework_prs,
        rework_rate=row.rework_drag_rate,
        revert_prs=row.revert_prs,
        revert_rate=row.revert_rate,
        incidents_count=row.incidents_count,
        incident_rate=row.incident_drag_rate,
        test_gap_prs=row.test_gap_prs,
        test_gap_rate=test_gap_rate,
    )


def _aggregate_bucket_totals(rows: list[Any]) -> dict[str, AIImpactBucketTotals]:
    by_bucket: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        by_bucket[row.attribution_bucket].append(row)

    totals: dict[str, AIImpactBucketTotals] = {}
    for bucket, bucket_rows in by_bucket.items():
        prs_total = sum(r.prs_total for r in bucket_rows)
        prs_merged = sum(r.prs_merged for r in bucket_rows)

        agent_created_prs = sum(r.agent_created_prs for r in bucket_rows)

        cycle_pairs = [(r.cycle_time_avg_hours, r.prs_merged) for r in bucket_rows]
        cycle_delta_pairs = [
            (r.ai_cycle_time_delta_hours, r.prs_merged) for r in bucket_rows
        ]
        review_amp_pairs = [
            (r.ai_review_amplification, r.prs_total) for r in bucket_rows
        ]
        rework_pairs = [(r.rework_drag_rate, r.prs_total) for r in bucket_rows]
        revert_pairs = [(r.revert_rate, r.prs_total) for r in bucket_rows]
        incident_pairs = [(r.incident_drag_rate, r.prs_total) for r in bucket_rows]
        test_pairs = [(r.test_gap_rate, r.prs_total) for r in bucket_rows]
        ratio_pairs = [(r.ai_assisted_pr_ratio, r.prs_total) for r in bucket_rows]

        # Aggregate leverage components by averaging per-day values weighted
        # by PR volume — the underlying metric is already a ratio.
        prs_component = (
            _weighted_avg(
                [(r.leverage.prs_component, r.prs_total) for r in bucket_rows]
            )
            or 0.0
        )
        leverage = AILeverageComponents(
            prs_component=prs_component,
            cycle_time_component=_weighted_avg(
                [(r.leverage.cycle_time_component, r.prs_merged) for r in bucket_rows]
            ),
            review_component=_weighted_avg(
                [(r.leverage.review_component, r.prs_total) for r in bucket_rows]
            ),
            rework_component=_weighted_avg(
                [(r.leverage.rework_component, r.prs_total) for r in bucket_rows]
            ),
            test_component=_weighted_avg(
                [(r.leverage.test_component, r.prs_total) for r in bucket_rows]
            ),
            incident_component=_weighted_avg(
                [(r.leverage.incident_component, r.prs_total) for r in bucket_rows]
            ),
        )

        totals[bucket] = AIImpactBucketTotals(
            bucket=bucket,
            prs_total=prs_total,
            prs_merged=prs_merged,
            ai_assisted_pr_ratio=_weighted_avg(ratio_pairs),
            # Sum of agent-created PRs that landed inside this bucket's group.
            agent_created_pr_count=agent_created_prs,
            cycle_time_avg_hours=_weighted_avg(cycle_pairs),
            ai_cycle_time_delta_hours=_weighted_avg(cycle_delta_pairs),
            ai_review_amplification=_weighted_avg(review_amp_pairs),
            rework_drag_rate=_weighted_avg(rework_pairs),
            revert_rate=_weighted_avg(revert_pairs),
            incident_drag_rate=_weighted_avg(incident_pairs),
            test_gap_rate=_weighted_avg(test_pairs),
            leverage=leverage,
        )
    return totals


# =============================================================================
# resolve_ai_impact_summary
# =============================================================================

_SCOPE_BREAKDOWN_LIMIT = 10


def _scope_rollups(
    rows: list[Any],
    *,
    key_fn: Any,
    labels: dict[str, str],
) -> list[AIImpactScopeRollupRow]:
    """Aggregate daily records into per-scope AI rollups (CHAOS-2186).

    ``key_fn`` extracts the scope id (repo uuid string or team id) from a
    record; records with no scope id are skipped. Scopes with zero
    AI-attributed PRs are dropped — an empty list is the honest "no AI
    activity ranked" state, not a fabricated zero row.
    """
    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        key = key_fn(row)
        if not key:
            continue
        grouped[str(key)].append(row)

    rollups: list[AIImpactScopeRollupRow] = []
    for scope_id, scope_rows in grouped.items():
        ai_rows = [
            r for r in scope_rows if r.attribution_bucket in AI_ATTRIBUTION_BUCKETS
        ]
        human_rows = [r for r in scope_rows if r.attribution_bucket == _BASELINE_BUCKET]
        ai_prs_total = sum(r.prs_total for r in ai_rows)
        if ai_prs_total <= 0:
            continue
        all_prs_total = sum(r.prs_total for r in scope_rows)
        ai_rework = _weighted_avg([(r.rework_drag_rate, r.prs_total) for r in ai_rows])
        human_rework = _weighted_avg(
            [(r.rework_drag_rate, r.prs_total) for r in human_rows]
        )
        rollups.append(
            AIImpactScopeRollupRow(
                scope_id=scope_id,
                scope_label=labels.get(scope_id, scope_id),
                ai_prs_total=ai_prs_total,
                ai_assisted_pr_ratio=_ratio(ai_prs_total, all_prs_total),
                rework_rate_delta=_delta(ai_rework, human_rework),
            )
        )
    rollups.sort(key=lambda r: (-r.ai_prs_total, r.scope_id))
    return rollups[:_SCOPE_BREAKDOWN_LIMIT]


async def _load_scope_breakdowns(
    context: GraphQLContext,
    org_id: str,
    rows: list[Any],
) -> tuple[list[AIImpactScopeRollupRow], list[AIImpactScopeRollupRow]]:
    """Build per-repo and per-team rollups with resolved display labels."""
    if not rows:
        return [], []
    loader = AIImpactClickHouseLoader(_require_client(context), org_id=org_id)

    repo_ids = sorted({str(r.repo_id) for r in rows if r.repo_id})
    try:
        repo_labels = await loader.load_repo_labels(repo_ids)
    except Exception as exc:
        logger.warning("Could not resolve repo labels for AI breakdown: %s", exc)
        repo_labels = {}

    team_ids = sorted({str(r.team_id) for r in rows if r.team_id})
    try:
        team_labels = await loader.load_team_labels(team_ids)
    except Exception as exc:
        logger.warning("Could not resolve team labels for AI breakdown: %s", exc)
        team_labels = {}

    repo_breakdown = _scope_rollups(
        rows,
        key_fn=lambda r: str(r.repo_id) if r.repo_id else None,
        labels=repo_labels,
    )
    team_breakdown = _scope_rollups(
        rows,
        key_fn=lambda r: r.team_id or None,
        labels=team_labels,
    )
    return repo_breakdown, team_breakdown


async def resolve_ai_impact_summary(
    context: GraphQLContext,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None = None,
) -> AIImpactSummary:
    org_id = require_org_id(context)
    rows = await _load_daily_records(context, org_id, date_range, scope)
    rows = _bucket_filter(
        rows,
        [AttributionBucket(b.value) for b in scope.buckets]
        if scope and scope.buckets
        else None,
    )

    by_bucket = _aggregate_bucket_totals(rows)
    daily = [_row_to_impact_daily(row) for row in rows]
    repo_breakdown, team_breakdown = await _load_scope_breakdowns(context, org_id, rows)

    total_prs = sum(r.prs_total for r in rows)
    ai_assisted = sum(r.ai_assisted_prs for r in rows)
    agent_created = sum(r.agent_created_prs for r in rows)
    human = sum(r.human_prs for r in rows)
    unknown = sum(r.unknown_prs for r in rows)

    computed_at = max((row.computed_at for row in rows), default=None)
    missing_states = [
        state
        for state in [_unknown_attribution_missing_state(unknown)]
        if state is not None
    ]
    if rows and not repo_breakdown and not team_breakdown:
        missing_states.append(
            _missing_state(
                "scope_breakdown",
                "No repo or team rollups with AI activity",
                (
                    "No repos or teams in this scope have AI-attributed PRs "
                    "in the window, so ranked rollups stay empty. This is a "
                    "no-data state, not a zero-impact signal."
                ),
            )
        )

    return AIImpactSummary(
        org_id=org_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
        total_prs=total_prs,
        ai_assisted_prs=ai_assisted,
        agent_created_prs=agent_created,
        human_prs=human,
        unknown_prs=unknown,
        ai_assisted_pr_ratio=_ratio(ai_assisted, total_prs),
        by_bucket=sorted(by_bucket.values(), key=lambda r: r.bucket),
        daily=daily,
        repo_breakdown=repo_breakdown,
        team_breakdown=team_breakdown,
        missing_states=missing_states,
        data_available=bool(rows),
        computed_at=computed_at,
    )


# =============================================================================
# resolve_ai_comparison
# =============================================================================


def _empty_side(bucket: str) -> AIComparisonSide:
    return AIComparisonSide(
        bucket=bucket,
        prs_total=0,
        prs_merged=0,
        cycle_time_avg_hours=None,
        reviews_per_pr=None,
        rework_rate=None,
        revert_rate=None,
        test_gap_rate=None,
        incident_rate=None,
    )


def _aggregate_side(rows: list[Any], bucket_label: str) -> AIComparisonSide:
    if not rows:
        return _empty_side(bucket_label)
    prs_total = sum(r.prs_total for r in rows)
    prs_merged = sum(r.prs_merged for r in rows)
    return AIComparisonSide(
        bucket=bucket_label,
        prs_total=prs_total,
        prs_merged=prs_merged,
        cycle_time_avg_hours=_weighted_avg(
            [(r.cycle_time_avg_hours, r.prs_merged) for r in rows]
        ),
        reviews_per_pr=_weighted_avg([(r.reviews_per_pr, r.prs_total) for r in rows]),
        rework_rate=_weighted_avg([(r.rework_drag_rate, r.prs_total) for r in rows]),
        revert_rate=_weighted_avg([(r.revert_rate, r.prs_total) for r in rows]),
        test_gap_rate=_weighted_avg([(r.test_gap_rate, r.prs_total) for r in rows]),
        incident_rate=_weighted_avg(
            [(r.incident_drag_rate, r.prs_total) for r in rows]
        ),
    )


def _delta(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return a - b


async def resolve_ai_comparison(
    context: GraphQLContext,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None = None,
) -> AIComparison:
    org_id = require_org_id(context)
    all_rows = await _load_daily_records(context, org_id, date_range, scope)

    ai_rows = [r for r in all_rows if r.attribution_bucket in AI_ATTRIBUTION_BUCKETS]
    baseline_rows = [r for r in all_rows if r.attribution_bucket == _BASELINE_BUCKET]

    ai_side = _aggregate_side(ai_rows, "ai")  # synthetic label for the aggregated side
    baseline_side = _aggregate_side(baseline_rows, _BASELINE_BUCKET.value)
    delta = AIComparisonDelta(
        cycle_time_delta_hours=_delta(
            ai_side.cycle_time_avg_hours, baseline_side.cycle_time_avg_hours
        ),
        reviews_per_pr_delta=_delta(
            ai_side.reviews_per_pr, baseline_side.reviews_per_pr
        ),
        rework_rate_delta=_delta(ai_side.rework_rate, baseline_side.rework_rate),
        revert_rate_delta=_delta(ai_side.revert_rate, baseline_side.revert_rate),
        test_gap_rate_delta=_delta(ai_side.test_gap_rate, baseline_side.test_gap_rate),
        incident_rate_delta=_delta(ai_side.incident_rate, baseline_side.incident_rate),
    )
    return AIComparison(
        org_id=org_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
        ai_side=ai_side,
        baseline_side=baseline_side,
        delta=delta,
        data_available=bool(ai_rows or baseline_rows),
    )


# =============================================================================
# resolve_ai_review_load
# =============================================================================


def _review_total_for_row(row: Any) -> int:
    reviews_per_pr = row.reviews_per_pr or 0.0
    return int(round(reviews_per_pr * row.prs_total))


async def _load_reviewer_concentration(
    context: GraphQLContext,
    org_id: str,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None,
) -> AIReviewerConcentrationSummary:
    repo_id, team_id, _work_type, repo_unresolved = await _normalize_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return AIReviewerConcentrationSummary(
            data_available=False, reviewer_count=0, reviewer_gini=None
        )
    loader = AIImpactClickHouseLoader(_require_client(context), org_id=org_id)
    reviewer_gini, reviewer_count = await loader.load_reviewer_concentration(
        start_day=date_range.start_date,
        end_day=date_range.end_date,
        repo_id=repo_id,
        team_id=team_id,
    )
    return AIReviewerConcentrationSummary(
        data_available=reviewer_gini is not None,
        reviewer_count=reviewer_count,
        reviewer_gini=reviewer_gini,
    )


class _EngagementSlice:
    """Accumulated engagement inputs for one (bucket, day) or one bucket."""

    __slots__ = ("latency_pairs", "comments_total", "loc_total")

    def __init__(self) -> None:
        self.latency_pairs: list[tuple[float | None, float]] = []
        self.comments_total = 0
        self.loc_total = 0

    def add(self, raw: dict[str, Any]) -> None:
        latency = raw.get("pickup_latency_hours")
        weight = float(raw.get("prs_with_first_review") or 0)
        self.latency_pairs.append(
            (float(latency) if latency is not None else None, weight)
        )
        self.comments_total += int(raw.get("review_comments_total") or 0)
        self.loc_total += int(raw.get("loc_total") or 0)

    @property
    def pickup_latency_hours(self) -> float | None:
        return _weighted_avg(self.latency_pairs)

    @property
    def review_comments_per_loc(self) -> float | None:
        if self.loc_total <= 0:
            return None
        return self.comments_total / self.loc_total


async def _load_engagement_slices(
    context: GraphQLContext,
    org_id: str,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None,
) -> tuple[dict[tuple[str, Any], _EngagementSlice], dict[str, _EngagementSlice]]:
    """Load CHAOS-2194 engagement metrics keyed by (bucket, day) and bucket.

    A ``work_type`` scope cannot be applied to the raw-PR engagement query
    (work types live on the linkage path only), so engagement is reported as
    unavailable rather than silently mis-scoped. Team scopes are translated
    to repo ids first; an unresolvable team also yields unavailable.
    """
    repo_id, team_id, work_type, repo_unresolved = await _normalize_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return {}, {}
    if work_type:
        return {}, {}

    team_repo_ids: list[uuid.UUID] | None = None
    if team_id:
        team_repo_ids = await _resolve_team_repo_ids(
            _require_client(context), org_id=org_id, team_id=team_id
        )
        if team_repo_ids is None or not team_repo_ids:
            return {}, {}

    start_dt = datetime.combine(date_range.start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(date_range.end_date, time.max, tzinfo=timezone.utc)
    loader = AIImpactClickHouseLoader(_require_client(context), org_id=org_id)
    try:
        raw_rows = await loader.load_review_engagement(
            start=start_dt,
            end=end_dt,
            repo_id=repo_id,
            repo_ids=team_repo_ids,
        )
    except Exception as exc:
        logger.warning("AI review engagement query failed: %s", exc)
        return {}, {}

    by_day: dict[tuple[str, Any], _EngagementSlice] = {}
    by_bucket: dict[str, _EngagementSlice] = {}
    for raw in raw_rows:
        bucket = str(raw.get("bucket") or "unknown")
        day_key = (bucket, raw.get("day"))
        by_day.setdefault(day_key, _EngagementSlice()).add(raw)
        by_bucket.setdefault(bucket, _EngagementSlice()).add(raw)
    return by_day, by_bucket


async def resolve_ai_review_load(
    context: GraphQLContext,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None = None,
) -> AIReviewLoadResult:
    org_id = require_org_id(context)
    rows = await _load_daily_records(context, org_id, date_range, scope)
    engagement_by_day, engagement_by_bucket = await _load_engagement_slices(
        context, org_id, date_range, scope
    )

    def _day_engagement(bucket: str, day: Any) -> _EngagementSlice | None:
        return engagement_by_day.get((bucket, day))

    daily = []
    for row in rows:
        slice_ = _day_engagement(row.attribution_bucket, row.day)
        daily.append(
            AIReviewLoadRow(
                bucket=row.attribution_bucket,
                prs_total=row.prs_total,
                reviews_total=_review_total_for_row(row),
                reviews_per_pr=row.reviews_per_pr,
                changes_requested_per_pr=row.changes_requested_per_pr,
                review_amplification=row.ai_review_amplification,
                post_first_review_pushes_count=row.followup_commits_count,
                post_first_review_pushes_per_pr=_ratio(
                    row.followup_commits_count, row.prs_total
                ),
                pickup_latency_hours=(slice_.pickup_latency_hours if slice_ else None),
                review_comments_per_loc=(
                    slice_.review_comments_per_loc if slice_ else None
                ),
            )
        )

    by_bucket_acc: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        by_bucket_acc[row.attribution_bucket].append(row)

    by_bucket: list[AIReviewLoadRow] = []
    for bucket, bucket_rows in by_bucket_acc.items():
        prs_total = sum(r.prs_total for r in bucket_rows)
        reviews_total = sum(_review_total_for_row(r) for r in bucket_rows)
        post_first_review_pushes = sum(r.followup_commits_count for r in bucket_rows)
        bucket_slice = engagement_by_bucket.get(bucket)
        by_bucket.append(
            AIReviewLoadRow(
                bucket=bucket,
                prs_total=prs_total,
                reviews_total=reviews_total,
                reviews_per_pr=_weighted_avg(
                    [(r.reviews_per_pr, r.prs_total) for r in bucket_rows]
                ),
                changes_requested_per_pr=_weighted_avg(
                    [(r.changes_requested_per_pr, r.prs_total) for r in bucket_rows]
                ),
                review_amplification=_weighted_avg(
                    [(r.ai_review_amplification, r.prs_total) for r in bucket_rows]
                ),
                post_first_review_pushes_count=post_first_review_pushes,
                post_first_review_pushes_per_pr=_ratio(
                    post_first_review_pushes, prs_total
                ),
                pickup_latency_hours=(
                    bucket_slice.pickup_latency_hours if bucket_slice else None
                ),
                review_comments_per_loc=(
                    bucket_slice.review_comments_per_loc if bucket_slice else None
                ),
            )
        )
    by_bucket.sort(key=lambda r: r.bucket)

    reviewer_concentration = await _load_reviewer_concentration(
        context, org_id, date_range, scope
    )
    missing_states = []
    if rows and not engagement_by_bucket:
        missing_states.append(
            _missing_state(
                "review_engagement",
                "Pickup latency and comment density unavailable",
                (
                    "Pickup latency and review comments per changed line could "
                    "not be computed for this scope — the raw pull-request "
                    "review timestamps or size fields are missing, or the "
                    "scope (work type, unresolved team) cannot be applied to "
                    "raw PR rows. Treat as no-data, not as zero."
                ),
            )
        )
    if not reviewer_concentration.data_available:
        missing_states.append(
            _missing_state(
                "reviewer_concentration",
                "Reviewer concentration needs aggregate coverage",
                (
                    "Reviewer concentration is only shown as an aggregate "
                    "distribution signal. No reviewer names, rankings, or "
                    "person-level review counts are exposed."
                ),
            )
        )

    return AIReviewLoadResult(
        org_id=org_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
        by_bucket=by_bucket,
        daily=daily,
        reviewer_concentration=reviewer_concentration,
        missing_states=missing_states,
        data_available=bool(rows),
    )


# =============================================================================
# resolve_ai_risk_breakdown
# =============================================================================


async def resolve_ai_risk_breakdown(
    context: GraphQLContext,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None = None,
) -> AIRiskBreakdownResult:
    org_id = require_org_id(context)
    rows = await _load_daily_records(context, org_id, date_range, scope)

    by_bucket_acc: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        by_bucket_acc[row.attribution_bucket].append(row)

    by_bucket: list[AIRiskBreakdownRow] = []
    for bucket, bucket_rows in by_bucket_acc.items():
        prs_total = sum(r.prs_total for r in bucket_rows)
        rework_prs = sum(r.rework_prs for r in bucket_rows)
        revert_prs = sum(r.revert_prs for r in bucket_rows)
        test_gap_prs = sum(r.test_gap_prs for r in bucket_rows)
        incidents = sum(r.incidents_count for r in bucket_rows)
        by_bucket.append(
            AIRiskBreakdownRow(
                bucket=bucket,
                prs_total=prs_total,
                rework_prs=rework_prs,
                rework_rate=_ratio(rework_prs, prs_total),
                revert_prs=revert_prs,
                revert_rate=_ratio(revert_prs, prs_total),
                test_gap_prs=test_gap_prs,
                test_gap_rate=_ratio(test_gap_prs, prs_total),
                incidents_count=incidents,
                incident_rate=_ratio(incidents, prs_total),
            )
        )
    by_bucket.sort(key=lambda r: r.bucket)

    hotspot_overlap, complexity_overlap = await _load_overlap_rows(
        context, org_id, date_range, scope
    )

    missing_states: list[AIMissingState] = []
    if not hotspot_overlap:
        missing_states.append(
            _missing_state(
                "hotspot_overlap",
                "Hotspot overlap has no assessable AI PRs",
                (
                    "No AI-attributed PRs in this window could be joined to "
                    "hotspot files — either no AI PRs have commit/file "
                    "linkage yet or no hotspot snapshots exist for the "
                    "window. Treat missing overlap as no data, not as no "
                    "risk."
                ),
            )
        )
    if not complexity_overlap:
        missing_states.append(
            _missing_state(
                "complexity_overlap",
                "Complexity overlap has no assessable AI PRs",
                (
                    "No AI-attributed PRs in this window could be joined to "
                    "high-complexity files. Drill into PR and Work Graph "
                    "evidence when available; do not infer person-level "
                    "quality from this gap."
                ),
            )
        )

    return AIRiskBreakdownResult(
        org_id=org_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
        by_bucket=by_bucket,
        hotspot_overlap=hotspot_overlap,
        complexity_overlap=complexity_overlap,
        missing_states=missing_states,
        data_available=bool(rows),
    )


async def _load_overlap_rows(
    context: GraphQLContext,
    org_id: str,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None,
) -> tuple[list[AIHotspotOverlapRow], list[AIComplexityOverlapRow]]:
    """Load CHAOS-2185 hotspot/complexity overlap rows (best effort).

    Query failures and unresolvable scopes degrade to empty lists, which the
    caller renders as explicit missing states — never as fabricated zeros.
    A ``work_type`` scope cannot be applied to the raw-PR join, so overlap
    is reported unavailable for work-type-scoped requests.
    """
    repo_id, team_id, work_type, repo_unresolved = await _normalize_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return [], []
    if work_type:
        return [], []

    team_repo_ids: list[uuid.UUID] | None = None
    if team_id:
        team_repo_ids = await _resolve_team_repo_ids(
            _require_client(context), org_id=org_id, team_id=team_id
        )
        if team_repo_ids is None or not team_repo_ids:
            return [], []

    start_dt = datetime.combine(date_range.start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(date_range.end_date, time.max, tzinfo=timezone.utc)
    loader = AIImpactClickHouseLoader(_require_client(context), org_id=org_id)

    hotspot_rows: list[AIHotspotOverlapRow] = []
    try:
        raw_hotspots = await loader.load_hotspot_overlap(
            start=start_dt,
            end=end_dt,
            start_day=date_range.start_date,
            end_day=date_range.end_date,
            repo_id=repo_id,
            repo_ids=team_repo_ids,
        )
    except Exception as exc:
        logger.warning("AI hotspot overlap query failed: %s", exc)
        raw_hotspots = []
    for raw in raw_hotspots:
        prs_total = int(raw.get("prs_total") or 0)
        if prs_total <= 0:
            continue
        touching = int(raw.get("prs_touching_hotspots") or 0)
        avg_risk = raw.get("avg_hotspot_risk_score")
        hotspot_rows.append(
            AIHotspotOverlapRow(
                bucket=str(raw.get("bucket") or "unknown"),
                prs_total=prs_total,
                prs_touching_hotspots=touching,
                hotspot_overlap_rate=_ratio(touching, prs_total),
                avg_hotspot_risk_score=safe_optional_float(avg_risk),
            )
        )
    hotspot_rows.sort(key=lambda r: r.bucket)

    complexity_rows: list[AIComplexityOverlapRow] = []
    try:
        raw_complexity = await loader.load_complexity_overlap(
            start=start_dt,
            end=end_dt,
            end_day=date_range.end_date,
            repo_id=repo_id,
            repo_ids=team_repo_ids,
        )
    except Exception as exc:
        logger.warning("AI complexity overlap query failed: %s", exc)
        raw_complexity = []
    for raw in raw_complexity:
        prs_total = int(raw.get("prs_total") or 0)
        if prs_total <= 0:
            continue
        touching = int(raw.get("prs_touching_high_complexity") or 0)
        complexity_rows.append(
            AIComplexityOverlapRow(
                bucket=str(raw.get("bucket") or "unknown"),
                prs_total=prs_total,
                prs_touching_high_complexity=touching,
                complexity_overlap_rate=_ratio(touching, prs_total),
            )
        )
    complexity_rows.sort(key=lambda r: r.bucket)

    return hotspot_rows, complexity_rows


# =============================================================================
# resolve_ai_opportunities
# =============================================================================


async def resolve_ai_opportunities(
    context: GraphQLContext,
    scope: AIScopeInput | None = None,
    limit: int = 25,
) -> AIOpportunitiesResult:
    """Return rule-based AI automation opportunities.

    First release decision: inline detection. The resolver reads existing
    ClickHouse rollups synchronously via ``AIOpportunityDetector`` and does not
    persist recommendations yet, keeping the detector pure-read and avoiding a
    second materialization path until noisy-recommendation dismissal lands.
    """

    org_id = require_org_id(context)
    client = _require_client(context)
    resolved_scope, repo_unresolved = await _normalize_opportunity_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return AIOpportunitiesResult(
            org_id=org_id, recommendations=[], detector_ready=True
        )
    detector = AIOpportunityDetector(client)
    recommendations = await detector.detect(
        org_id=org_id, scope=resolved_scope, limit=limit
    )
    # detector_ready signals that the detector is wired and ran successfully,
    # NOT that it found candidates.  An empty result is valid (no opportunities
    # right now); False would mislead the frontend into showing "not connected".
    return AIOpportunitiesResult(
        org_id=org_id,
        recommendations=recommendations,
        detector_ready=True,
    )


# =============================================================================
# resolve_ai_governance_summary
# =============================================================================


async def resolve_ai_governance_summary(
    context: GraphQLContext,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None = None,
    violation_limit: int = 100,
) -> AIGovernanceSummary:
    from dev_health_ops.audit.ai_governance.loaders import AIGovernanceLoader

    org_id = require_org_id(context)
    _validate_date_range(date_range)
    repo_id, team_id, _work_type, repo_unresolved = await _normalize_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return AIGovernanceSummary(
            org_id=org_id,
            start_date=date_range.start_date,
            end_date=date_range.end_date,
            coverage=[],
            recent_violations=[],
            data_available=False,
        )
    client = _require_client(context)

    loader = AIGovernanceLoader(client)
    coverage = await loader.load_coverage(
        org_id=org_id,
        start_day=date_range.start_date,
        end_day=date_range.end_date,
        team_id=team_id,
        repo_id=repo_id,
    )
    violations = await loader.load_violations(
        org_id=org_id,
        start_day=date_range.start_date,
        end_day=date_range.end_date,
        team_id=team_id,
        repo_id=repo_id,
        limit=max(0, int(violation_limit)),
    )

    coverage_rows = [
        AIGovernanceCoverageRow(
            day=row.day,
            team_id=row.team_id,
            repo_id=str(row.repo_id) if row.repo_id is not None else None,
            ai_artifacts=row.ai_artifacts,
            declared_artifacts=row.declared_artifacts,
            human_reviewed_prs=row.human_reviewed_prs,
            security_scanned_prs=row.security_scanned_prs,
            in_policy_artifacts=row.in_policy_artifacts,
            declaration_coverage=row.declaration_coverage,
            human_review_coverage=row.human_review_coverage,
            security_scan_coverage=row.security_scan_coverage,
            in_policy_coverage=row.in_policy_coverage,
        )
        for row in coverage
    ]

    violation_rows = [
        AIGovernanceViolationRow(
            rule_id=v.rule_id,
            severity=v.severity,
            subject_type=v.subject_type,
            subject_id=v.subject_id,
            team_id=v.team_id,
            repo_id=str(v.repo_id) if v.repo_id is not None else None,
            observed_at=_to_aware(v.observed_at),
            evidence=v.evidence,
        )
        for v in violations
    ]

    return AIGovernanceSummary(
        org_id=org_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
        coverage=coverage_rows,
        recent_violations=violation_rows,
        data_available=bool(coverage_rows or violation_rows),
    )


def _to_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


# =============================================================================
# resolve_ai_workflow_drilldown
# =============================================================================


async def resolve_ai_workflow_drilldown(
    context: GraphQLContext,
    root_type: AIWorkflowRootTypeInput,
    root_id: str,
    depth: int = 3,
    limit: int = 100,
) -> AIWorkflowDrilldownResult:
    from dev_health_ops.work_graph.ai_workflow import load_ai_workflow_graph

    org_id = require_org_id(context)
    if not root_id:
        raise ValueError("root_id is required for AI workflow drilldown")

    traversal = await load_ai_workflow_graph(
        _require_client(context),
        org_id,
        root_type.value,
        root_id,
        depth=max(0, int(depth)),
        limit=max(0, int(limit)),
    )

    nodes = [
        AIWorkflowGraphNodeOut(node_type=node.node_type, node_id=node.node_id)
        for node in traversal.nodes
    ]
    edges = [
        AIWorkflowGraphEdgeOut(
            edge_id=edge.edge_id,
            source_type=edge.source_type,
            source_id=edge.source_id,
            target_type=edge.target_type,
            target_id=edge.target_id,
            edge_type=edge.edge_type,
            confidence=edge.confidence,
            source=edge.source,
            evidence=edge.evidence,
            provider=edge.provider,
            repo_id=edge.repo_id,
        )
        for edge in traversal.edges
    ]
    return AIWorkflowDrilldownResult(
        org_id=org_id,
        root_type=traversal.root_type,
        root_id=traversal.root_id,
        nodes=nodes,
        edges=edges,
        partial=traversal.partial,
        data_available=bool(edges),
    )


# =============================================================================
# resolve_ai_attributed_prs
# =============================================================================


async def _resolve_repo_team_map(
    client: Any,
    org_id: str,
    repo_ids: list[str],
) -> dict[str, str | None]:
    """Return {repo_id_str → team_id} using RepoPatternTeamResolver.

    ``teams.repo_patterns`` is an Array(String) of fnmatch glob patterns over
    repo full-names (e.g. ``"acme/*"``, ``"backend/api"``), so team membership
    cannot be resolved with a SQL JOIN on repo UUIDs.  Instead we:

    1. Load teams + patterns from ClickHouse.
    2. Build a :class:`RepoPatternTeamResolver` from the patterns.
    3. Load each repo's ``full_name`` (the ``repo`` column in ``repos``).
    4. Resolve team by matching full_name against the patterns.

    This mirrors the approach used by ``job_daily.py`` (build_repo_pattern_resolver).
    Returns an empty dict on any query error so callers degrade gracefully.
    """
    from dev_health_ops.api.queries.client import query_dicts
    from dev_health_ops.providers.teams import build_repo_pattern_resolver

    if not repo_ids or not org_id:
        return {}

    try:
        team_rows = await query_dicts(
            client,
            "SELECT id, name, repo_patterns FROM teams WHERE org_id = {org_id:String}",
            {"org_id": org_id},
        )
    except Exception as exc:
        logger.warning("Could not load teams for AI attributed PR resolution: %s", exc)
        return {}

    resolver = build_repo_pattern_resolver(team_rows)

    try:
        name_rows = await query_dicts(
            client,
            """
            SELECT toString(id) AS repo_id, repo AS full_name
            FROM repos
            WHERE org_id = {org_id:String}
              AND toString(id) IN {repo_ids:Array(String)}
            """,
            {"org_id": org_id, "repo_ids": repo_ids},
        )
    except Exception as exc:
        logger.warning("Could not load repo names for team resolution: %s", exc)
        return {}

    repo_id_to_name: dict[str, str] = {
        r["repo_id"]: str(r.get("full_name") or r["repo_id"]) for r in name_rows
    }

    result: dict[str, str | None] = {}
    for repo_id in repo_ids:
        full_name = repo_id_to_name.get(repo_id)
        team_id_resolved, _ = resolver.resolve(full_name)
        result[repo_id] = team_id_resolved or None
    return result


async def _resolve_team_repo_ids(
    client: Any,
    org_id: str,
    team_id: str,
) -> list[uuid.UUID] | None:
    """Resolve a team id to the repo UUIDs whose names match its patterns.

    Returns the (possibly empty) list of repo UUIDs that
    ``RepoPatternTeamResolver`` assigns to ``team_id``, or ``None`` when the
    team/repo catalogs cannot be loaded — callers must then fall back to
    in-memory filtering rather than treating the team as empty.
    """
    from dev_health_ops.api.queries.client import query_dicts
    from dev_health_ops.providers.teams import build_repo_pattern_resolver

    if not org_id or not team_id:
        return None

    try:
        team_rows = await query_dicts(
            client,
            "SELECT id, name, repo_patterns FROM teams WHERE org_id = {org_id:String}",
            {"org_id": org_id},
        )
        repo_rows = await query_dicts(
            client,
            """
            SELECT toString(id) AS repo_id, repo AS full_name
            FROM repos
            WHERE org_id = {org_id:String}
            """,
            {"org_id": org_id},
        )
    except Exception as exc:
        logger.warning("Could not resolve team %r to repo ids: %s", team_id, exc)
        return None

    resolver = build_repo_pattern_resolver(team_rows)
    repo_ids: list[uuid.UUID] = []
    for row in repo_rows:
        full_name = str(row.get("full_name") or "")
        resolved_team, _ = resolver.resolve(full_name)
        if (resolved_team or None) == team_id:
            parsed = _parse_uuid(str(row.get("repo_id")))
            if parsed is not None:
                repo_ids.append(parsed)
    return repo_ids


_MAX_AI_ATTRIBUTED_PRS_PAGE = 200


async def resolve_ai_attributed_prs(
    context: GraphQLContext,
    date_range: AIDateRangeInput,
    scope: AIScopeInput | None = None,
    limit: int = 50,
    offset: int = 0,
) -> AiAttributedPrsResult:
    """List AI-attributed pull requests for drilldown selection.

    Reads ``ai_attribution_resolved`` joined to ``git_pull_requests`` and
    returns the unaggregated PRs that drive the AI Review Load and AI Risk
    dashboards. The UI passes a chosen ``(repo_id, number)`` to
    ``aiWorkflowDrilldown`` to retrieve evidence graphs.

    No fabrication: every returned row corresponds to a persisted attribution.
    """

    org_id = require_org_id(context)
    _validate_date_range(date_range)
    repo_id, team_id, work_type, repo_unresolved = await _normalize_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return AiAttributedPrsResult(
            org_id=org_id,
            start_date=date_range.start_date,
            end_date=date_range.end_date,
            rows=[],
            total=0,
            has_more=False,
            data_available=False,
        )

    page_size = max(1, min(int(limit), _MAX_AI_ATTRIBUTED_PRS_PAGE))
    page_offset = max(0, int(offset))

    start_dt = datetime.combine(date_range.start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(date_range.end_date, time.max, tzinfo=timezone.utc)

    # Resolve the team scope to repo UUIDs BEFORE the SQL LIMIT so pages stay
    # dense (CHAOS-2180 Wave 2 — Wave 1 filtered in memory after LIMIT, which
    # returned sparse pages for team-scoped requests). ``None`` means the
    # catalogs could not be loaded; fall back to in-memory filtering then.
    team_repo_ids: list[uuid.UUID] | None = None
    if team_id:
        team_repo_ids = await _resolve_team_repo_ids(
            _require_client(context), org_id=org_id, team_id=team_id
        )
        if team_repo_ids is not None and not team_repo_ids:
            # The team resolves to no repos: honestly empty, not an error.
            return AiAttributedPrsResult(
                org_id=org_id,
                start_date=date_range.start_date,
                end_date=date_range.end_date,
                rows=[],
                total=0,
                has_more=False,
                data_available=False,
            )

    loader = AIImpactClickHouseLoader(_require_client(context), org_id=org_id)
    # Fetch one extra row so we can report has_more without a COUNT(*) round-trip.
    raw_rows = await loader.load_ai_pr_attributions(
        start=start_dt,
        end=end_dt,
        repo_id=repo_id,
        repo_ids=team_repo_ids,
        limit=page_size + 1,
        offset=page_offset,
    )

    has_more = len(raw_rows) > page_size
    page_rows = raw_rows[:page_size]

    # Resolve team IDs via RepoPatternTeamResolver before filtering.
    # The SQL loader returns an empty team_id because teams.repo_patterns
    # holds fnmatch glob patterns over repo full-names (e.g. "acme/*"), not
    # repo UUIDs — a SQL JOIN on UUID would never match.  We resolve in
    # app-code by fetching the repo name from the repos table and running the
    # pattern matcher here.
    if org_id:
        distinct_repo_ids = list({str(row["repo_id"]) for row in page_rows})
        team_map = await _resolve_repo_team_map(
            _require_client(context), org_id=org_id, repo_ids=distinct_repo_ids
        )
        if team_map:
            for row in page_rows:
                row["team_id"] = team_map.get(str(row["repo_id"]))

    # team_id / work_type filtering happens here because the loader treats
    # them as projections, not WHERE-clause inputs. The PR universe is bounded
    # by date_range + repo_id so in-memory filtering is safe at this scale.
    if team_id:
        page_rows = [row for row in page_rows if (row.get("team_id") or "") == team_id]
    if work_type:
        page_rows = [
            row for row in page_rows if (row.get("work_type") or "") == work_type
        ]

    rows: list[AiAttributedPr] = []
    for row in page_rows:
        merged_at_raw = row.get("merged_at")
        rows.append(
            AiAttributedPr(
                repo_id=strawberry.ID(str(row["repo_id"])),
                number=int(row["number"]),
                title=row.get("title"),
                kind=row.get("kind"),
                work_type=row.get("work_type"),
                team_id=row.get("team_id") or None,
                merged_at=_to_aware(merged_at_raw) if merged_at_raw else None,
            )
        )

    return AiAttributedPrsResult(
        org_id=org_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
        rows=rows,
        total=len(rows),
        has_more=has_more,
        data_available=bool(rows),
    )


# =============================================================================
# resolve_ai_attribution_overview
# =============================================================================

_MAX_AI_ATTRIBUTION_EVIDENCE_PAGE = 200


async def resolve_ai_attribution_overview(
    context: GraphQLContext,
    date_range: AIDateRangeInput,
    scope: AIAttributionScopeInput | None = None,
    limit: int = 50,
    offset: int = 0,
) -> AIAttributionOverviewResult:
    """Attribution mix + provenance evidence for the dedicated AI Attribution page.

    Reads ``ai_attribution_resolved`` only — the highest-precedence,
    non-superseded signal per subject. Every returned row corresponds to a
    persisted signal; source/confidence/evidence are never fabricated or
    recomputed at request time.
    """

    org_id = require_org_id(context)
    _validate_date_range(date_range)
    repo_id, team_id, kinds, repo_unresolved = await _normalize_attribution_scope(
        context, org_id, scope
    )
    if repo_unresolved:
        return AIAttributionOverviewResult(
            org_id=org_id,
            start_date=date_range.start_date,
            end_date=date_range.end_date,
            mix=[],
            total_attributed=0,
            rows=[],
            has_more=False,
            data_available=False,
        )

    page_size = max(1, min(int(limit), _MAX_AI_ATTRIBUTION_EVIDENCE_PAGE))
    page_offset = max(0, int(offset))

    start_dt = datetime.combine(date_range.start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(date_range.end_date, time.max, tzinfo=timezone.utc)

    # Resolve the team scope to repo UUIDs BEFORE the SQL LIMIT, mirroring
    # resolve_ai_attributed_prs (CHAOS-2180 Wave 2), so team-scoped pages
    # stay dense. ``None`` means the catalogs could not be loaded; callers
    # then fall back to in-memory filtering below.
    team_repo_ids: list[uuid.UUID] | None = None
    if team_id:
        team_repo_ids = await _resolve_team_repo_ids(
            _require_client(context), org_id=org_id, team_id=team_id
        )
        if team_repo_ids is not None and not team_repo_ids:
            # The team resolves to no repos: honestly empty, not an error.
            return AIAttributionOverviewResult(
                org_id=org_id,
                start_date=date_range.start_date,
                end_date=date_range.end_date,
                mix=[],
                total_attributed=0,
                rows=[],
                has_more=False,
                data_available=False,
            )

    loader = AIAttributionClickHouseLoader(_require_client(context), org_id=org_id)

    mix_raw = await loader.load_mix(
        start=start_dt,
        end=end_dt,
        repo_id=repo_id,
        repo_ids=team_repo_ids,
        kinds=kinds,
    )
    total_mix = sum(int(row.get("count") or 0) for row in mix_raw)
    mix = [
        AIAttributionMixRow(
            kind=str(row.get("kind") or "unknown"),
            count=int(row.get("count") or 0),
            share=(int(row.get("count") or 0) / total_mix) if total_mix else 0.0,
        )
        for row in mix_raw
    ]

    # Fetch one extra row so we can report has_more without a COUNT(*) round-trip.
    raw_rows = await loader.load_evidence(
        start=start_dt,
        end=end_dt,
        repo_id=repo_id,
        repo_ids=team_repo_ids,
        kinds=kinds,
        limit=page_size + 1,
        offset=page_offset,
    )
    has_more = len(raw_rows) > page_size
    page_rows = raw_rows[:page_size]

    # Resolve team IDs via RepoPatternTeamResolver before filtering, mirroring
    # resolve_ai_attributed_prs: teams.repo_patterns holds fnmatch glob
    # patterns over repo full-names, not repo UUIDs, so a SQL JOIN never
    # matches — team membership is resolved in app code.
    if org_id:
        distinct_repo_ids = list(
            {str(row["repo_id"]) for row in page_rows if row.get("repo_id") is not None}
        )
        team_map = await _resolve_repo_team_map(
            _require_client(context), org_id=org_id, repo_ids=distinct_repo_ids
        )
        if team_map:
            for row in page_rows:
                repo_id_val = row.get("repo_id")
                row["team_id"] = (
                    team_map.get(str(repo_id_val)) if repo_id_val is not None else None
                )

    if team_id:
        page_rows = [row for row in page_rows if (row.get("team_id") or "") == team_id]

    rows: list[AIAttributionEvidenceRow] = []
    for row in page_rows:
        rows.append(
            AIAttributionEvidenceRow(
                subject_type=str(row.get("subject_type") or ""),
                subject_id=str(row.get("subject_id") or ""),
                repo_id=str(row["repo_id"]) if row.get("repo_id") is not None else None,
                provider=str(row.get("provider") or ""),
                kind=str(row.get("kind") or "unknown"),
                source=str(row.get("source") or ""),
                confidence=float(row.get("confidence") or 0.0),
                actor=row.get("actor"),
                evidence=str(row.get("evidence") or "{}"),
                observed_at=_to_aware(row["observed_at"]),
                team_id=row.get("team_id") or None,
            )
        )

    return AIAttributionOverviewResult(
        org_id=org_id,
        start_date=date_range.start_date,
        end_date=date_range.end_date,
        mix=mix,
        total_attributed=total_mix,
        rows=rows,
        has_more=has_more,
        data_available=bool(mix or rows),
    )
