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
    WorkUnitTeamAttribution,
)
from ._membership_run_scope import (
    LATEST_COMPLETE_RUN_SUBQUERY,
    LEGACY_NODE_MAX_JOIN,
    RUN_SCOPE_PREDICATE,
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


# CHAOS-2600 CS7: staged source precedence as a SQL CASE, mirroring
# compute_work_items._SOURCE_ORDER. A unit's owning team is the one backed by the
# strongest (lowest-rank) member-item source; an unrecognised source degrades to
# the floor (8) so it never out-ranks a real signal. Keep in lockstep with the
# enum in models/outputs.py and the Python map in compute_work_items.py.
_SOURCE_RANK_SQL = (
    "multiIf("
    "a.source='native_team',0,"
    "a.source='issue_project',1,"
    "a.source='project_ownership',2,"
    "a.source='repo_ownership',3,"
    "a.source='assignee_membership',4,"
    "a.source='linked_issue',5,"
    "a.source='manual_fallback',6,"
    "a.source='unassigned',7,"
    "8)"
)


def _row_to_unit_attribution(row: dict[str, Any]) -> WorkUnitTeamAttribution:
    team_id = str(row["team_id"]) if row.get("team_id") else None
    team_name = str(row["team_name"]) if row.get("team_name") else None
    source = _map_source(str(row.get("source", "unassigned")))
    member_count = int(row.get("member_count", 0) or 0)
    # Synthesised here (not in SQL) so the evidence string stays a presentation
    # concern: how many member items back the team and via which signal.
    target = team_name or team_id or "no team"
    evidence = (
        f"{member_count} member work item(s) attributed to {target} via {source.value}"
    )
    return WorkUnitTeamAttribution(
        work_unit_id=str(row.get("work_unit_id", "")),
        team_id=team_id,
        team_name=team_name,
        source=source,
        confidence=_map_confidence(str(row.get("confidence", "none"))),
        is_primary=True,
        member_count=member_count,
        evidence=evidence,
    )


async def resolve_work_unit_team_attributions(
    context: GraphQLContext,
    work_unit_ids: list[str] | None = None,
    team_id: str | None = None,
) -> list[WorkUnitTeamAttribution]:
    """Return the ONE owning team per work unit, with provenance.

    A work unit is an aggregation of work items; this collapses each unit's member
    ``work_item_team_attributions`` to a single team by the staged source
    precedence (strongest member-item source wins; ties → most member items, then
    team_id for determinism). ``work_unit_ids`` bounds the read to the units a view
    is rendering (the expected call shape); ``team_id`` optionally filters to units
    that roll up to one team.

    The join key is ``work_unit_membership.node_id = work_item_team_attributions``
    ``.work_item_id``: both live in the provider-qualified id space (``linear:`` /
    ``ghpr:`` / ``gh:``). Cross-provider ``extkey:`` reference nodes never match —
    correctly, since they carry no attribution of their own and are covered by the
    sibling PR/issue nodes in the same unit.
    """
    from dev_health_ops.api.queries.client import query_dicts

    org_id = require_org_id(context)
    client = context.client
    if client is None:
        raise RuntimeError("Database client not available")

    params: dict[str, Any] = {"org_id": org_id, "limit": _MAX_ROWS}

    # Member nodes are scoped to the latest COMPLETE membership run via the SHARED
    # protocol (LATEST_COMPLETE_RUN_SUBQUERY + LEGACY_NODE_MAX_JOIN +
    # RUN_SCOPE_PREDICATE) — identical to the work-graph reader. This handles the
    # seeded '__legacy__' marker (migration 048): a migrated/idle org whose latest
    # marker is '__legacy__' keeps its pre-migration (run_id='') rows readable,
    # mapped to each node's latest legacy row. The empty-string guard
    # (latest_run.latest_run_id != '') makes an org with NO complete run resolve to
    # "no membership" rather than over-matching empty-run_id rows.
    # work_unit_ids bounds the unit set when the caller supplies it.
    work_unit_filter = ""
    if work_unit_ids:
        work_unit_filter = "AND m.work_unit_id IN %(work_unit_ids)s"
        params["work_unit_ids"] = list(work_unit_ids)

    # team_id filters the FINAL per-unit winner (a unit whose owning team is
    # team_id), not the candidate set — mirrors the per-item "filter after pick"
    # rule. It must wrap the winner subquery: team_id is an aggregate (argMin), so
    # it can't be filtered with a WHERE/HAVING at the GROUP BY level.
    team_filter = ""
    if team_id:
        team_filter = "WHERE team_id = %(team_id)s"
        params["team_id"] = team_id

    # See work_item_team_attributions resolver for the latest-snapshot rationale:
    # candidates are appended (never deleted), so a re-org's retired rows survive
    # FINAL and must be excluded by constraining to each item's max(computed_at).
    query = f"""
        WITH latest_run AS ({LATEST_COMPLETE_RUN_SUBQUERY})
        SELECT
            work_unit_id,
            team_id,
            team_name,
            source,
            confidence,
            member_count
        FROM (
            SELECT
                work_unit_id,
                argMin(team_id, sort_key) AS team_id,
                argMin(team_name, sort_key) AS team_name,
                argMin(source, sort_key) AS source,
                argMin(confidence, sort_key) AS confidence,
                argMin(member_count, sort_key) AS member_count
            FROM (
                SELECT
                    work_unit_id,
                    team_id,
                    argMin(team_name, src_rank) AS team_name,
                    argMin(source, src_rank) AS source,
                    argMin(confidence, src_rank) AS confidence,
                    count() AS member_count,
                    (min(src_rank), -toInt64(count()), team_id) AS sort_key
                FROM (
                    SELECT
                        m.work_unit_id AS work_unit_id,
                        a.team_id AS team_id,
                        a.team_name AS team_name,
                        a.source AS source,
                        a.confidence AS confidence,
                        {_SOURCE_RANK_SQL} AS src_rank
                    FROM (
                        SELECT DISTINCT m.work_unit_id AS work_unit_id, m.node_id AS node_id
                        FROM work_unit_membership AS m
                        INNER JOIN latest_run ON 1 = 1
                        {LEGACY_NODE_MAX_JOIN}
                        WHERE m.org_id = %(org_id)s
                          {work_unit_filter}
                          AND latest_run.latest_run_id != ''
                          AND ({RUN_SCOPE_PREDICATE})
                    ) AS m
                    INNER JOIN (
                        SELECT work_item_id, team_id, team_name, source, confidence
                        FROM work_item_team_attributions FINAL
                        WHERE org_id = %(org_id)s
                          AND is_primary = 1
                          AND (work_item_id, computed_at) IN (
                              SELECT work_item_id, max(computed_at)
                              FROM work_item_team_attributions
                              WHERE org_id = %(org_id)s
                              GROUP BY work_item_id
                          )
                    ) AS a
                    ON m.node_id = a.work_item_id
                )
                GROUP BY work_unit_id, team_id
            )
            GROUP BY work_unit_id
        )
        {team_filter}
        ORDER BY member_count DESC, work_unit_id
        LIMIT %(limit)s
    """

    rows = await query_dicts(client, query, params)
    return [_row_to_unit_attribution(row) for row in rows]
