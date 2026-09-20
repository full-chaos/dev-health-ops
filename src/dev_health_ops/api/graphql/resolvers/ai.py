"""Resolver for the AI automation opportunities GraphQL query.

The other AI analytics operations (impact summary, comparison, review load,
risk breakdown, governance summary, workflow drilldown, attributed PRs,
attribution overview) are served by query-api and have no Python body; their
Strawberry fields raise through ``_raise_served_by_query_api``.

This resolver is purely **read-only** and never performs persistence.
"""

from __future__ import annotations

import uuid
from typing import Any

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.metrics.opportunities.ai_detector import AIOpportunityDetector

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.ai import (
    AIOpportunitiesResult,
    AIScopeInput,
)


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for AI analytics resolver")
    return context.client


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
