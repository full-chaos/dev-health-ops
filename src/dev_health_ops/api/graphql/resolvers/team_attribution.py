"""GraphQL resolver for team-attribution provenance (CHAOS-2600 CS4).

Surfaces the per-candidate provenance the precedence resolver persists to
``work_item_team_attributions`` so the web can explain *why* a work item maps to a
team (and stop deriving attribution client-side). Mirrors the work_graph edge
resolver: org-scoped, FINAL-deduped (ReplacingMergeTree), enum-mapped with a safe
fallback so a future enum value degrades instead of 500ing the query.
"""

from __future__ import annotations

import logging
from typing import Any

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.outputs import (
    TeamAttributionConfidence,
    TeamAttributionSource,
    WorkItemTeamAttribution,
)

logger = logging.getLogger(__name__)

# Cap a single provenance read so an unfiltered call can't top-sort a large
# tenant's entire attribution set.
_MAX_ROWS = 5000


def _map_source(value: str) -> TeamAttributionSource:
    try:
        return TeamAttributionSource(str(value).lower())
    except ValueError:
        # A source the API predates degrades to the floor rather than 500ing.
        return TeamAttributionSource.UNASSIGNED


def _map_confidence(value: str) -> TeamAttributionConfidence:
    try:
        return TeamAttributionConfidence(str(value).lower())
    except ValueError:
        return TeamAttributionConfidence.NONE


def _row_to_attribution(row: dict[str, Any]) -> WorkItemTeamAttribution:
    return WorkItemTeamAttribution(
        work_item_id=str(row.get("work_item_id", "")),
        provider=str(row.get("provider", "")),
        team_id=str(row["team_id"]) if row.get("team_id") else None,
        team_name=str(row["team_name"]) if row.get("team_name") else None,
        source=_map_source(str(row.get("source", "unassigned"))),
        confidence=_map_confidence(str(row.get("confidence", "none"))),
        is_primary=bool(int(row.get("is_primary", 0) or 0)),
        evidence=str(row.get("evidence", "")),
    )


async def resolve_work_item_team_attributions(
    context: GraphQLContext,
    work_item_ids: list[str] | None = None,
    team_id: str | None = None,
) -> list[WorkItemTeamAttribution]:
    """Return team-attribution candidates (with provenance) for work items.

    Org-scoped and FINAL-deduped. ``work_item_ids`` bounds the read to the items a
    view is rendering (the expected call shape); ``team_id`` optionally filters to
    one team's attributions. Rows are ordered primary-first per work item.
    """
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client
    if client is None:
        raise RuntimeError("Database client not available")

    # The snapshot subquery scopes by org + work_item_ids only — NOT team_id: a
    # work item's latest compute spans all candidate teams, and the team filter is
    # applied afterward so a re-org that moved the item to another team can't be
    # masked by an old same-team row.
    base_where = ["org_id = %(org_id)s"]
    params: dict[str, Any] = {"org_id": org_id, "limit": _MAX_ROWS}
    if work_item_ids:
        base_where.append("work_item_id IN %(work_item_ids)s")
        params["work_item_ids"] = list(work_item_ids)
    snapshot_where = " AND ".join(base_where)

    outer_where = list(base_where)
    if team_id:
        outer_where.append("team_id = %(team_id)s")
        params["team_id"] = team_id

    # compute_work_item_team_attributions stamps EVERY candidate of one compute
    # with a single computed_at and APPENDS them (never deletes prior ones). The
    # RMT key is (org_id, repo_id, work_item_id, team_id, source), so a re-org that
    # drops a candidate or moves a work item to another team leaves the old
    # (team_id, source) rows alive — FINAL can't retire them (different key). Read
    # back raw, they would surface as a second is_primary=true row / a stale
    # higher-precedence source. Constrain the read to each work item's LATEST
    # compute snapshot (max computed_at) so retired candidates drop out. (FINAL
    # still collapses exact-key duplicates within that snapshot.)
    query = f"""
        SELECT
            work_item_id,
            provider,
            team_id,
            team_name,
            source,
            confidence,
            is_primary,
            evidence
        FROM work_item_team_attributions FINAL
        WHERE {" AND ".join(outer_where)}
          AND (work_item_id, computed_at) IN (
              SELECT work_item_id, max(computed_at)
              FROM work_item_team_attributions
              WHERE {snapshot_where}
              GROUP BY work_item_id
          )
        ORDER BY work_item_id, is_primary DESC, source
        LIMIT %(limit)s
    """

    rows = await query_dicts(client, query, params)
    return [_row_to_attribution(row) for row in rows]
