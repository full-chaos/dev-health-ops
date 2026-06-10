"""Resolvers for the Improve area — Experiments (CHAOS-2219) + Automations (CHAOS-2220).

This module holds BOTH Improve-area resolvers:

- ``resolve_experiments``: DERIVED resolver that assembles experiments at
  query-time from ``OpportunityCard.suggested_experiments`` (CHAOS-2219).
  No persistence table is needed for v1 — the resolver contract is stable so
  persistence can be added in v2 without a schema change.

  ID stability: experiment IDs are derived from (metric, suggestion_text)
  via SHA-256 so the same logical experiment always receives the same ID
  regardless of the opportunity card's daily rank position.

- ``resolve_improve_opportunities``: Wires ``FlowOpportunityDetector`` into
  the GraphQL schema (CHAOS-2220).  Follows the same read-only pattern as
  ``resolvers/ai.py``.  ``org_id`` is always injected via ``require_org_id``
  — the resolver never serves cross-org rows.  An optional scope (repo_id /
  team_id) narrows the ClickHouse queries inside the detector.

Both resolvers share the module-level ``logger`` and the ``GraphQLContext``
dependency; they are independent and do not call each other.
"""

from __future__ import annotations

import hashlib
import logging
import uuid

from dev_health_ops.metrics.opportunities.flow_detector import FlowOpportunityDetector
from dev_health_ops.metrics.opportunities.models import (
    FlowScopeInput,
)
from dev_health_ops.metrics.opportunities.models import (
    ImproveOpportunity as _DomainOpportunity,
)
from dev_health_ops.metrics.opportunities.models import (
    ImproveOpportunityKind as _DomainKind,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.ai import AIScopeInput
from ..models.improve import (
    Experiment,
    ExperimentsResult,
    ExperimentStatus,
    ImproveOpportunitiesResult,
    ImproveOpportunity,
    ImproveOpportunityKind,
)

logger = logging.getLogger(__name__)


# ── Experiments (CHAOS-2219) ──────────────────────────────────────────────────


def _stable_experiment_id(metric: str, suggestion: str) -> str:
    """Return a 16-hex-char content-stable ID for a derived experiment.

    The ID is derived from (metric, suggestion_text) so it is invariant to
    daily rank-order shifts of the parent opportunity card.  The same
    suggestion text for the same metric always produces the same ID, which
    makes client-side caching and deduplication safe.

    Uses the same SHA-256-prefix approach as
    ``dev_health_ops.metrics.opportunities.scoring.stable_opportunity_id``.
    """
    raw = f"{metric}:{suggestion}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


async def resolve_experiments(
    context: GraphQLContext,
    filters: object | None = None,
) -> ExperimentsResult:
    """Return derived experiments assembled from opportunity suggested_experiments.

    Calls ``build_opportunities_response`` via the same filter path used by the
    REST ``/api/v1/opportunities`` endpoint so results are consistent with the
    Opportunities page.

    Args:
        context: GraphQL request context (carries org_id, db_url, ClickHouse client).
        filters:  Optional ``FilterInput``; forwarded to the opportunities service.

    Returns:
        ``ExperimentsResult`` with ``derived_from_opportunities=True`` when the
        opportunities service returned data, ``False`` on failure (empty items).
    """
    from dev_health_ops.api.models.filters import MetricFilter, ScopeFilter, TimeFilter
    from dev_health_ops.api.services.cache import TTLCache
    from dev_health_ops.api.services.opportunities import build_opportunities_response

    # Build a MetricFilter from the GraphQL FilterInput (mirrors the REST path).
    try:
        if (
            filters is not None
            and hasattr(filters, "scope")
            and filters.scope is not None
        ):
            scope_in = filters.scope
            # ScopeFilterInput.level may be a Strawberry enum (ScopeLevelInput)
            # whose .value is already the lowercase string, or a plain str.
            raw_level = getattr(scope_in, "level", "org")
            level_str = (
                raw_level.value if hasattr(raw_level, "value") else str(raw_level)
            ).lower() or "org"
            scope = ScopeFilter(
                level=level_str,
                ids=list(getattr(scope_in, "ids", None) or []),
            )
        else:
            scope = ScopeFilter(level="org", ids=[])

        if (
            filters is not None
            and hasattr(filters, "time")
            and filters.time is not None
        ):
            time_in = filters.time
            time_filter = TimeFilter(
                range_days=int(getattr(time_in, "range_days", 30)),
                compare_days=int(getattr(time_in, "compare_days", 30)),
            )
        else:
            time_filter = TimeFilter(range_days=30, compare_days=30)

        metric_filter = MetricFilter(scope=scope, time=time_filter)
    except Exception:
        logger.exception(
            "Failed to build MetricFilter from GraphQL FilterInput; using defaults"
        )
        metric_filter = MetricFilter(
            scope=ScopeFilter(level="org", ids=[]),
            time=TimeFilter(range_days=30, compare_days=30),
        )

    try:
        # Re-use the module-level cache injected by the GraphQL app (warm across
        # requests).  Fall back to a fresh in-memory instance only when running
        # outside the normal app lifecycle (e.g. tests that don't wire a cache).
        cache = context.cache or TTLCache(ttl_seconds=60)
        response = await build_opportunities_response(
            db_url=context.db_url,
            filters=metric_filter,
            cache=cache,
            org_id=context.org_id,
        )
    except Exception:
        logger.exception("Experiments resolver: opportunities service failed")
        return ExperimentsResult(items=[], derived_from_opportunities=False)

    experiments: list[Experiment] = []
    for card in response.items:
        for suggestion in card.suggested_experiments:
            metric = _metric_from_card(card)
            experiments.append(
                Experiment(
                    id=_stable_experiment_id(metric, suggestion),
                    opportunity_id=card.id,
                    hypothesis=suggestion,
                    metric=metric,
                    owner="",
                    stop_condition="",
                    status=ExperimentStatus.SUGGESTED,
                    start_date=None,
                    stop_date=None,
                    outcome=None,
                )
            )

    return ExperimentsResult(items=experiments, derived_from_opportunities=True)


def _metric_from_card(card: object) -> str:
    """Infer the metric key from the opportunity card title.

    The opportunities service currently sets ``title`` as ``"Reduce <metric_label>"``.
    For the baseline "Maintain steady flow" card (no specific metric) we return
    an empty string.

    NOTE: this title-reversal is intentionally lightweight for v1.  The correct
    fix for v2 is to thread a ``metric_key`` field onto ``OpportunityCard``
    directly so callers never need to reverse-engineer it from display text.
    """
    title: str = str(getattr(card, "title", ""))
    if title.startswith("Reduce "):
        # Strip the leading verb and normalise to a metric-key-style slug.
        label = title.split(" ", 1)[1].lower().replace(" ", "_")
        return label
    return ""


# ── Automations / Flow opportunities (CHAOS-2220) ─────────────────────────────

# Map domain enum → Strawberry enum (same string values, but distinct objects).
_KIND_MAP: dict[_DomainKind, ImproveOpportunityKind] = {
    _DomainKind.HIGH_REVIEW_LATENCY: ImproveOpportunityKind.HIGH_REVIEW_LATENCY,
    _DomainKind.SLOW_CYCLE_TIME: ImproveOpportunityKind.SLOW_CYCLE_TIME,
    _DomainKind.HIGH_REWORK: ImproveOpportunityKind.HIGH_REWORK,
    _DomainKind.HIGH_WIP: ImproveOpportunityKind.HIGH_WIP,
    _DomainKind.LOW_THROUGHPUT: ImproveOpportunityKind.LOW_THROUGHPUT,
    _DomainKind.HIGH_CHURN: ImproveOpportunityKind.HIGH_CHURN,
    _DomainKind.HIGH_CHANGE_FAILURE: ImproveOpportunityKind.HIGH_CHANGE_FAILURE,
}


def _project(domain: _DomainOpportunity) -> ImproveOpportunity:
    """Convert a domain ImproveOpportunity to its Strawberry counterpart."""
    return ImproveOpportunity(
        opportunity_id=domain.opportunity_id,
        kind=_KIND_MAP[domain.kind],
        entity_type=domain.entity_type,
        entity_id=domain.entity_id,
        title=domain.title,
        rationale=domain.rationale,
        score=domain.score,
        severity=domain.severity,
        evidence_refs=list(domain.evidence_refs),
        recommended_action=domain.recommended_action,
    )


def _parse_scope(scope: AIScopeInput | None) -> FlowScopeInput | None:
    if scope is None:
        return None
    repo_id: uuid.UUID | None = None
    if scope.repo_id:
        try:
            repo_id = uuid.UUID(scope.repo_id)
        except (TypeError, ValueError):
            logger.debug(
                "Invalid repo UUID %r in improve scope, ignoring", scope.repo_id
            )
    team_id = scope.team_id or None
    if repo_id is None and team_id is None:
        return None
    return FlowScopeInput(repo_id=repo_id, team_id=team_id)


async def resolve_improve_opportunities(
    context: GraphQLContext,
    scope: AIScopeInput | None = None,
    limit: int = 10,
    window_days: int = 30,
) -> ImproveOpportunitiesResult:
    """Return rule-based non-AI flow opportunities for the Improve surface.

    Decision (CHAOS-2220): inline detection — no persisted recommendations.
    The detector reads pre-computed ClickHouse rows; it never writes.
    An empty list is the valid "all green" state.
    """
    org_id = require_org_id(context)

    if context.client is None:
        logger.error(
            "resolve_improve_opportunities: DB client unavailable for org=%s", org_id
        )
        return ImproveOpportunitiesResult(
            org_id=org_id,
            opportunities=[],
            detector_ready=False,
            total_count=0,
        )

    bounded_limit = max(1, min(int(limit), 100))
    bounded_window = max(1, min(int(window_days), 365))

    detector = FlowOpportunityDetector(context.client)
    flow_scope = _parse_scope(scope)

    domain_opps = await detector.detect(
        org_id=org_id,
        scope=flow_scope,
        limit=bounded_limit,
        window_days=bounded_window,
    )

    projected = [_project(o) for o in domain_opps]

    return ImproveOpportunitiesResult(
        org_id=org_id,
        opportunities=projected,
        detector_ready=True,
        total_count=len(projected),
    )
