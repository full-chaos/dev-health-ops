"""Cognitive Load GraphQL resolver (CHAOS-2077).

Reads from two append-only ClickHouse tables:
- ``user_metrics_daily``  — per-developer load signals (SUM across developers)
- ``team_metrics_daily``  — per-team commit-timing ratios (AVG across teams)

Both tables are plain ``MergeTree`` (NOT ``ReplacingMergeTree``): a recompute /
backfill appends a NEW row for the same logical key rather than replacing the
old one. Live data confirms duplicates (user_metrics_daily: 2344 duplicate-key
rows; team_metrics_daily: 66). We therefore collapse to the latest row per key
via ``argMax(<col>, computed_at)`` BEFORE aggregating — mirroring the
established ``resolvers/complexity.py`` convention. Without this, SUM/AVG would
double-count backfilled rows (naive SUM of pr_interruption_load was 3296 vs the
correct 1744 in the demo org).

The two result sets are merged on ``day`` (over the UNION of days) in Python
before being returned. All reads are org-scoped via ``require_org_id`` +
parametrized ``org_id``. No data is written or recomputed — pure surface of
persisted metrics.
"""

from __future__ import annotations

import logging
from typing import Any

from dev_health_ops.api.queries.client import query_dicts

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.cognitive_load import (
    CognitiveLoadInput,
    CognitiveLoadResult,
    CognitiveLoadSignal,
)

logger = logging.getLogger(__name__)


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for CognitiveLoad resolver")
    return context.client


def _nfloat(row: dict[str, Any], key: str) -> float | None:
    """Return ``float(row[key])`` or ``None`` when absent/null."""
    v = row.get(key)
    return float(v) if v is not None else None


# ---------------------------------------------------------------------------
# ClickHouse fetch helpers
# ---------------------------------------------------------------------------


async def _fetch_user_metrics(
    client: Any,
    *,
    org_id: str,
    since_date: str,
    until_date: str,
    team_id: str | None,
    repo_id: str | None,
) -> list[dict[str, Any]]:
    """SUM of latest-per-developer cognitive load columns, grouped by day.

    ``user_metrics_daily`` is append-only (plain MergeTree), so a backfill
    writes a duplicate row for the same ``(org_id, repo_id, author_email, day)``
    key. The inner subquery selects the latest row per key via
    ``argMax(<col>, computed_at)``; the outer query SUMs those deduplicated
    rows by day. This prevents double-counting from re-computation passes.

    Filters by ``org_id`` (always), date range, and optionally ``team_id`` /
    ``repo_id`` (the latter is valid here since ``user_metrics_daily`` carries
    a ``repo_id`` column per row; ``team_metrics_daily`` does not, so
    ``repo_id`` is never applied to the team-metrics query). ``repo_id`` is a
    ``UUID``-typed column, so the predicate casts it via ``toString(...)``
    before comparing — mirroring ``resolvers/complexity.py``'s
    ``toString(repo_id) IN {repo_ids:Array(String)}`` convention — rather than
    comparing the column directly against the ``String`` parameter, which
    would force ClickHouse to parse the parameter as a UUID and raise
    ``CANNOT_PARSE_UUID`` for any non-UUID value.
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
    if team_id:
        inner_where += "\n              AND team_id = {team_id:String}"
        params["team_id"] = team_id
    if repo_id:
        inner_where += "\n              AND toString(repo_id) = {repo_id:String}"
        params["repo_id"] = repo_id

    query = f"""
        SELECT
            day,
            SUM(pr_interruption_load) AS pr_interruption_load,
            SUM(context_spread_count) AS context_spread_count,
            SUM(review_request_load)  AS review_request_load
        FROM (
            SELECT
                day,
                repo_id,
                author_email,
                argMax(pr_interruption_load, computed_at) AS pr_interruption_load,
                argMax(context_spread_count, computed_at) AS context_spread_count,
                argMax(review_request_load,  computed_at) AS review_request_load
            FROM user_metrics_daily
            {inner_where}
            GROUP BY day, repo_id, author_email
        )
        GROUP BY day
        ORDER BY day
    """
    return await query_dicts(client, query, params)


async def _fetch_team_metrics(
    client: Any,
    *,
    org_id: str,
    since_date: str,
    until_date: str,
    team_id: str | None,
) -> list[dict[str, Any]]:
    """AVG of latest-per-team after-hours / weekend commit ratios, by day.

    ``team_metrics_daily`` is append-only (plain MergeTree) and team-scoped
    (no repo_id). The inner subquery collapses each
    ``(org_id, team_id, day)`` key to its latest row via
    ``argMax(<col>, computed_at)``; the outer query AVGs those deduplicated
    rows by day. When ``team_id`` is supplied we filter to that team;
    otherwise we average across all teams to produce an org-wide signal.
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
    if team_id:
        inner_where += "\n              AND team_id = {team_id:String}"
        params["team_id"] = team_id

    query = f"""
        SELECT
            day,
            AVG(after_hours_commit_ratio) AS after_hours_commit_ratio,
            AVG(weekend_commit_ratio)     AS weekend_commit_ratio
        FROM (
            SELECT
                day,
                team_id,
                argMax(after_hours_commit_ratio, computed_at) AS after_hours_commit_ratio,
                argMax(weekend_commit_ratio,     computed_at) AS weekend_commit_ratio
            FROM team_metrics_daily
            {inner_where}
            GROUP BY day, team_id
        )
        GROUP BY day
        ORDER BY day
    """
    return await query_dicts(client, query, params)


# ---------------------------------------------------------------------------
# Public resolver
# ---------------------------------------------------------------------------


async def resolve_cognitive_load(
    context: GraphQLContext,
    input: CognitiveLoadInput,
) -> CognitiveLoadResult:
    """Serve cognitive-load signals from ClickHouse (read-only).

    Org-gate is enforced via ``require_org_id``; any mismatch between the
    JWT org and the GraphQL ``orgId`` argument is logged and the JWT org wins.

    The resolver fires two queries (user + team), each of which deduplicates
    append-only rows via ``argMax(..., computed_at)`` before aggregating, then
    merges them over the UNION of days. A day present only in
    ``team_metrics_daily`` (e.g. a weekend with commit-timing data but no
    per-developer load rows) is still emitted with zero user-side signals.
    """
    authorized_org_id = require_org_id(context)
    if input.org_id != authorized_org_id:
        logger.debug(
            "Ignoring GraphQL orgId %r in favor of authorized org %r",
            input.org_id,
            authorized_org_id,
        )

    client = _require_client(context)

    since_date = input.since_date.isoformat()
    until_date = input.until_date.isoformat()

    # Fire both queries
    user_rows = await _fetch_user_metrics(
        client,
        org_id=authorized_org_id,
        since_date=since_date,
        until_date=until_date,
        team_id=input.team_id,
        repo_id=input.repo_id,
    )
    team_rows = await _fetch_team_metrics(
        client,
        org_id=authorized_org_id,
        since_date=since_date,
        until_date=until_date,
        team_id=input.team_id,
    )

    # Index both result sets by day for an O(1) outer-join merge.
    user_by_day: dict[Any, dict[str, Any]] = {row["day"]: row for row in user_rows}
    team_by_day: dict[Any, dict[str, Any]] = {row["day"]: row for row in team_rows}

    # Merge over the UNION of days: a day may appear in team_metrics_daily
    # without a matching user_metrics_daily row (and vice versa). Sort so the
    # output is deterministic regardless of dict insertion order.
    all_days = sorted(set(user_by_day) | set(team_by_day))

    signals: list[CognitiveLoadSignal] = []
    for day_val in all_days:
        user_row = user_by_day.get(day_val, {})
        team_row = team_by_day.get(day_val, {})

        signals.append(
            CognitiveLoadSignal(
                day=day_val,
                pr_interruption_load=float(user_row.get("pr_interruption_load") or 0.0),
                context_spread_count=float(user_row.get("context_spread_count") or 0.0),
                review_request_load=float(user_row.get("review_request_load") or 0.0),
                after_hours_commit_ratio=_nfloat(team_row, "after_hours_commit_ratio"),
                weekend_commit_ratio=_nfloat(team_row, "weekend_commit_ratio"),
            )
        )

    return CognitiveLoadResult(
        org_id=authorized_org_id,
        team_id=input.team_id,
        signals=signals,
        total_days=len(signals),
    )
