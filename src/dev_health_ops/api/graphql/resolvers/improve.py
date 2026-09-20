"""Resolver for the Improve area's flow opportunities.

``resolve_improve_opportunities`` wires ``FlowOpportunityDetector`` into the
GraphQL schema.  It follows the same read-only pattern as ``resolvers/ai.py``.
``org_id`` is always injected via ``require_org_id`` -- the resolver never
serves cross-org rows.  An optional scope (repo_id / team_id) narrows the
ClickHouse queries inside the detector.

The experiments field is served by query-api; its resolver is not defined here.
"""

from __future__ import annotations

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
    ImproveOpportunitiesResult,
    ImproveOpportunity,
    ImproveOpportunityKind,
)

logger = logging.getLogger(__name__)


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
