"""Resolver for security alert queries.

Repo name lookup strategy:
  The `repos` table IS present in ClickHouse (migration 000_raw_tables.sql)
  and has an `org_id` column (migration 024_add_org_id.sql). The column for
  the repo name is `repo` (not `name`). The `url` column is absent from the
  CH repos table, so `repo_url` is always returned as None from the JOIN.
  If a repo URL is needed in future, the Postgres-side `Repo` model has a
  `url`-equivalent that could be surfaced via a separate lookup or by adding
  the column to the CH table.

Cursor convention:
  Matches `work_graph_edges` which uses a plain opaque string (the edge_id
  value) for start_cursor / end_cursor. Because security alerts have a
  composite primary key (repo_id, alert_id) and a sort order based on
  severity_rank + created_at, we use offset-as-string (e.g. "50") — the same
  opaque-string convention, just offset-based. The `after` cursor is decoded
  as an integer offset. This is the simplest cursor form that survives the
  severity-rank sort without requiring composite base64 encoding.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.inputs import SecurityAlertFilterInput, SecurityPaginationInput
from ..models.outputs import (
    PageInfo,
    SecurityAlertConnection,
    SecurityAlertEdge,
    SecurityAlertNode,
)

logger = logging.getLogger(__name__)

# States that count as "open" for the open_only shorthand.
_OPEN_STATES = ("open", "detected", "confirmed")

# Severity rank expression for ORDER BY (critical=4, high=3, medium=2, low=1, unknown=0).
_SEVERITY_RANK_EXPR = (
    "multiIf("
    "sa.severity = 'critical', 4, "
    "sa.severity = 'high', 3, "
    "sa.severity = 'medium', 2, "
    "sa.severity = 'low', 1, "
    "0)"
)


def _decode_cursor(cursor: str | None) -> int:
    """Decode an offset cursor string to an integer offset. Returns 0 on failure."""
    if cursor is None:
        return 0
    try:
        offset = int(cursor)
        return max(0, offset)
    except (ValueError, TypeError):
        return 0


def _encode_cursor(offset: int) -> str:
    """Encode an integer offset as a cursor string."""
    return str(offset)


def _build_filter_clauses(
    filters: SecurityAlertFilterInput | None,
    params: dict[str, Any],
) -> list[str]:
    """Build WHERE clause fragments from the filter input.

    The `sa` alias refers to the security_alerts table.
    The `r` alias refers to the repos table.
    org_id scoping is always injected via r.org_id.
    """
    clauses: list[str] = ["r.org_id = %(org_id)s"]

    if filters is None:
        return clauses

    # open_only overrides states when both are provided (spec requirement).
    if filters.open_only:
        clauses.append("sa.state IN %(open_states)s")
        params["open_states"] = list(_OPEN_STATES)
    elif filters.states:
        clauses.append("sa.state IN %(states)s")
        params["states"] = [s.value for s in filters.states]

    if filters.repo_ids:
        clauses.append("toString(sa.repo_id) IN %(repo_ids)s")
        params["repo_ids"] = filters.repo_ids

    if filters.severities:
        clauses.append("sa.severity IN %(severities)s")
        params["severities"] = [s.value for s in filters.severities]

    if filters.sources:
        clauses.append("sa.source IN %(sources)s")
        params["sources"] = [s.value for s in filters.sources]

    if filters.since is not None:
        clauses.append("sa.created_at >= %(since)s")
        params["since"] = (
            datetime(
                filters.since.year,
                filters.since.month,
                filters.since.day,
                tzinfo=timezone.utc,
            )
            if isinstance(filters.since, date)
            and not isinstance(filters.since, datetime)
            else filters.since
        )

    if filters.until is not None:
        clauses.append("sa.created_at <= %(until)s")
        params["until"] = (
            datetime(
                filters.until.year,
                filters.until.month,
                filters.until.day,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            )
            if isinstance(filters.until, date)
            and not isinstance(filters.until, datetime)
            else filters.until
        )

    if filters.search:
        # ILIKE against title, package_name, cve_id — no string concatenation.
        clauses.append(
            "(ilike(sa.title, %(search_pattern)s)"
            " OR ilike(sa.package_name, %(search_pattern)s)"
            " OR ilike(sa.cve_id, %(search_pattern)s))"
        )
        params["search_pattern"] = f"%{filters.search}%"

    return clauses


def _row_to_node(row: dict[str, Any]) -> SecurityAlertNode:
    """Convert a ClickHouse result row to a SecurityAlertNode."""

    def _to_dt(val: Any) -> datetime | None:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        # ClickHouse may return a string; parse it.
        try:
            return datetime.fromisoformat(str(val))
        except (ValueError, TypeError):
            return None

    created_at_raw = row.get("created_at")
    created_at = _to_dt(created_at_raw) or datetime(1970, 1, 1, tzinfo=timezone.utc)

    return SecurityAlertNode(
        alert_id=str(row.get("alert_id", "")),
        repo_id=str(row.get("repo_id", "")),
        repo_name=str(row.get("repo_name", "")),
        repo_url=str(row["repo_url"]) if row.get("repo_url") else None,
        source=str(row.get("source", "")),
        severity=str(row.get("severity", "unknown")),
        state=str(row.get("state", "")),
        package_name=str(row["package_name"]) if row.get("package_name") else None,
        cve_id=str(row["cve_id"]) if row.get("cve_id") else None,
        url=str(row["url"]) if row.get("url") else None,
        title=str(row["title"]) if row.get("title") else None,
        description=str(row["description"]) if row.get("description") else None,
        created_at=created_at,
        fixed_at=_to_dt(row.get("fixed_at")),
        dismissed_at=_to_dt(row.get("dismissed_at")),
    )


async def resolve_security_alerts(
    context: GraphQLContext,
    org_id: str,
    filters: SecurityAlertFilterInput | None = None,
    pagination: SecurityPaginationInput | None = None,
) -> SecurityAlertConnection:
    """Resolve a paginated list of security alerts for an org."""
    from dev_health_ops.api.queries.client import query_dicts

    require_org_id(context)
    client = context.client

    if client is None:
        raise RuntimeError("Database client not available")

    first = pagination.first if pagination else 50
    offset = _decode_cursor(pagination.after if pagination else None)

    params: dict[str, Any] = {
        "org_id": org_id,
        "limit": int(first) + 1,
        "offset": int(offset),
    }
    where_clauses = _build_filter_clauses(filters, params)
    where_sql = f"WHERE {' AND '.join(where_clauses)}"

    # Fetch one extra row to determine has_next_page without a separate COUNT query.
    query = f"""
        SELECT
            toString(sa.repo_id) AS repo_id,
            sa.alert_id,
            r.repo AS repo_name,
            NULL AS repo_url,
            sa.source,
            coalesce(sa.severity, 'unknown') AS severity,
            coalesce(sa.state, 'open') AS state,
            sa.package_name,
            sa.cve_id,
            sa.url,
            sa.title,
            sa.description,
            sa.created_at,
            sa.fixed_at,
            sa.dismissed_at
        FROM security_alerts sa
        INNER JOIN repos r ON sa.repo_id = r.id
        {where_sql}
        ORDER BY {_SEVERITY_RANK_EXPR} DESC, sa.created_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """

    raw_rows = await query_dicts(client, query, params)
    has_next = len(raw_rows) > first
    nodes = [_row_to_node(row) for row in raw_rows[:first]]

    edges = [
        SecurityAlertEdge(
            node=node,
            cursor=_encode_cursor(offset + i + 1),
        )
        for i, node in enumerate(nodes)
    ]

    return SecurityAlertConnection(
        edges=edges,
        total_count=offset + len(edges),  # lower-bound; avoids a COUNT(*) round-trip
        page_info=PageInfo(
            has_next_page=has_next,
            has_previous_page=offset > 0,
            start_cursor=edges[0].cursor if edges else None,
            end_cursor=edges[-1].cursor if edges else None,
        ),
    )
