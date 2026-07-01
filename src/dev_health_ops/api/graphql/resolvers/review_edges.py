"""Review Edges GraphQL resolver (CHAOS-2077).

Reads from the append-only ClickHouse table ``review_edges_daily``.
Returns reviewer-to-author collaboration edges filtered by org, date range,
and optionally by repo.  All reads are org-scoped via ``require_org_id``.
No data is written or recomputed.
"""

from __future__ import annotations

import logging
from typing import Any

from dev_health_ops.api.queries.client import query_dicts

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.review_edges import (
    ReviewEdgeRow,
    ReviewEdgesInput,
    ReviewEdgesResult,
)

logger = logging.getLogger(__name__)

#: Hard cap on returned rows to protect against pathological date ranges.
MAX_REVIEW_EDGES_ROWS: int = 2000


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for ReviewEdges resolver")
    return context.client


# ---------------------------------------------------------------------------
# ClickHouse fetch helper
# ---------------------------------------------------------------------------


async def _fetch_review_edges(
    client: Any,
    *,
    org_id: str,
    since_date: str,
    until_date: str,
    repo_ids: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Read ``review_edges_daily`` filtered by org + date range.

    ``review_edges_daily`` is append-only (plain MergeTree, NOT
    ReplacingMergeTree): a recompute / backfill writes a duplicate row for the
    same ``(org_id, repo_id, reviewer, author, day)`` key (live data shows 49
    duplicate-key rows). The inner subquery collapses each key to its latest
    row via ``argMax(reviews_count, computed_at)`` so a backfilled day is not
    counted twice. Only after deduplication do we ``ORDER BY reviews_count
    DESC`` (heaviest collaboration pairs first) and apply the row cap.

    An optional ``repo_ids`` filter narrows to specific repositories.
    ``repo_id`` is stored as a UUID; input repo refs may be UUID strings or
    ``repos.repo`` slugs, so the predicate resolves them through the org-scoped
    catalog before comparing UUID-to-UUID.
    """
    inner_where = """
            WHERE org_id = {org_id:String}
              AND day >= {since_date:Date}
              AND day <= {until_date:Date}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "since_date": since_date,
        "until_date": until_date,
    }
    if repo_ids:
        inner_where += """
              AND repo_id IN (
                  SELECT id FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
              )"""
        params["repo_ids"] = list(repo_ids)

    query = f"""
        SELECT
            reviewer,
            author,
            reviews_count,
            day,
            toString(repo_id) AS repo_id
        FROM (
            SELECT
                repo_id,
                reviewer,
                author,
                day,
                argMax(reviews_count, computed_at) AS reviews_count
            FROM review_edges_daily
            {inner_where}
            GROUP BY repo_id, reviewer, author, day
        )
        ORDER BY reviews_count DESC
        LIMIT {limit}
    """
    return await query_dicts(client, query, params)


# ---------------------------------------------------------------------------
# Public resolver
# ---------------------------------------------------------------------------


async def resolve_review_edges(
    context: GraphQLContext,
    input: ReviewEdgesInput,
) -> ReviewEdgesResult:
    """Serve review-edge rows from ClickHouse (read-only).

    Org-gate is enforced via ``require_org_id``; any mismatch between the
    JWT org and the GraphQL ``orgId`` argument is logged and the JWT org wins.
    """
    authorized_org_id = require_org_id(context)
    if input.org_id != authorized_org_id:
        logger.debug(
            "Ignoring GraphQL orgId %r in favor of authorized org %r",
            input.org_id,
            authorized_org_id,
        )

    client = _require_client(context)

    raw_limit = input.limit if input.limit is not None else 500
    effective_limit = max(1, min(raw_limit, MAX_REVIEW_EDGES_ROWS))

    since_date = input.since_date.isoformat()
    until_date = input.until_date.isoformat()

    raw_rows = await _fetch_review_edges(
        client,
        org_id=authorized_org_id,
        since_date=since_date,
        until_date=until_date,
        repo_ids=input.repo_ids,
        limit=effective_limit,
    )

    edges: list[ReviewEdgeRow] = []
    for row in raw_rows:
        edges.append(
            ReviewEdgeRow(
                reviewer=str(row.get("reviewer") or ""),
                author=str(row.get("author") or ""),
                reviews_count=int(row.get("reviews_count") or 0),
                day=row["day"],
                repo_id=str(row["repo_id"]) if row.get("repo_id") else None,
            )
        )

    return ReviewEdgesResult(edges=edges, total_count=len(edges))
