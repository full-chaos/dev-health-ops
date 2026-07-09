"""Complexity GraphQL resolvers (CHAOS-1756).

Reads from three append-only ClickHouse tables:
- ``repo_complexity_daily``      — repo-scope timeseries
- ``file_complexity_snapshots``  — file-scope timeseries
- ``file_hotspot_daily``         — hotspot ranking

All reads use ``argMax(<col>, computed_at)`` to surface the latest compute
pass per ``(org_id, day, scope_key)``.  The resolvers are read-only and never
recompute any metric — they surface persisted data as-is per the inspectability
contract.
"""

from __future__ import annotations

import logging
from datetime import date as PyDate
from datetime import timedelta, timezone
from typing import Any
from urllib.parse import quote

from dev_health_ops.api.queries.client import query_dicts

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.complexity import (
    ComplexityPoint,
    ComplexityScope,
    ComplexityTimeseriesInput,
    ComplexityTimeseriesResult,
    HotspotRow,
    HotspotsInput,
    HotspotsResult,
    TimeGranularity,
)

logger = logging.getLogger(__name__)

#: Hard cap on timeseries rows; protects against pathological scopes.
MAX_ROWS: int = 1000
MAX_TIMESERIES_POINTS: int = 1000
#: Default row limit for timeseries queries.
DEFAULT_TIMESERIES_LIMIT: int = 500
#: Default row limit for hotspot queries.
DEFAULT_HOTSPOTS_LIMIT: int = 50
#: Hard cap for hotspot rows.
MAX_HOTSPOTS_ROWS: int = 500


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for Complexity resolver")
    return context.client


def _nint(row: dict[str, Any], key: str) -> int | None:
    """Return ``int(row[key])`` or ``None`` when the value is absent/null."""
    v = row.get(key)
    return int(v) if v is not None else None


def _nfloat(row: dict[str, Any], key: str) -> float | None:
    """Return ``float(row[key])`` or ``None`` when the value is absent/null."""
    v = row.get(key)
    return float(v) if v is not None else None


def _bucket_count(
    since_day: PyDate, until_day: PyDate, granularity: TimeGranularity
) -> int:
    if until_day < since_day:
        return 1
    if granularity == TimeGranularity.WEEK:
        since_bucket = since_day - timedelta(days=since_day.weekday())
        until_bucket = until_day - timedelta(days=until_day.weekday())
        return ((until_bucket - since_bucket).days // 7) + 1
    return (until_day - since_day).days + 1


async def _load_repo_labels(
    client: Any, org_id: str, repo_ids: list[str]
) -> dict[str, str]:
    """Return ``{repo_id: repo_full_name}`` for the given repo IDs.

    Falls back to the repo_id string when the catalog row is missing so the
    resolver always returns a non-empty ``scopeName``.
    """
    if not repo_ids:
        return {}
    rows = await query_dicts(
        client,
        """
        SELECT toString(id) AS repo_id, repo AS full_name
        FROM repos
        WHERE org_id = {org_id:String}
          AND toString(id) IN {repo_ids:Array(String)}
        """,
        {"org_id": org_id, "repo_ids": repo_ids},
    )
    return {row["repo_id"]: row.get("full_name") or row["repo_id"] for row in rows}


# ---------------------------------------------------------------------------
# ClickHouse fetch helpers
# ---------------------------------------------------------------------------


async def _fetch_repo_timeseries(
    client: Any,
    *,
    org_id: str,
    since_day: str,
    until_day: str,
    repo_ids: list[str] | None,
    limit: int,
    granularity: TimeGranularity,
) -> list[dict[str, Any]]:
    """Latest-compute read from ``repo_complexity_daily`` for the date window.

    ``argMax`` over ``computed_at`` returns the most-recent ingestion for each
    ``(repo_id, day)`` pair, consistent with the append-only sink contract.
    """
    day_expr = "toStartOfWeek(day, 1)" if granularity == TimeGranularity.WEEK else "day"
    computed_expr = (
        "(day, computed_at)" if granularity == TimeGranularity.WEEK else "computed_at"
    )
    query = f"""
        SELECT
            {day_expr} AS day,
            toString(repo_id) AS repo_id,
            argMax(loc_total,                      {computed_expr}) AS loc_total,
            argMax(cyclomatic_total,               {computed_expr}) AS cyclomatic_total,
            argMax(cyclomatic_per_kloc,            {computed_expr}) AS cyclomatic_per_kloc,
            argMax(high_complexity_functions,      {computed_expr}) AS high_complexity_functions,
            argMax(very_high_complexity_functions, {computed_expr}) AS very_high_complexity_functions
        FROM repo_complexity_daily
        WHERE org_id = {{org_id:String}}
          AND day >= {{since_day:Date}}
          AND day <= {{until_day:Date}}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "since_day": since_day,
        "until_day": until_day,
    }
    if repo_ids:
        bounded = list(repo_ids)[:limit]
        query += """
          AND repo_id IN (
              SELECT id FROM repos
              WHERE org_id = {org_id:String}
                AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
          )
        """
        params["repo_ids"] = bounded
    else:
        query += """
          AND toString(repo_id) IN (
              SELECT repo_id
              FROM (
                  SELECT
                      toString(repo_id) AS repo_id,
                      argMax(cyclomatic_per_kloc, (day, computed_at)) AS latest_complexity
                  FROM repo_complexity_daily
                  WHERE org_id = {org_id:String}
                    AND day >= {since_day:Date}
                    AND day <= {until_day:Date}
                  GROUP BY repo_id
                  ORDER BY latest_complexity DESC NULLS LAST, repo_id
                  LIMIT {limit:UInt32}
              )
          )
        """
        params["limit"] = limit
    query += "\nGROUP BY day, repo_id\nORDER BY day, repo_id"
    return await query_dicts(client, query, params)


async def _fetch_file_timeseries(
    client: Any,
    *,
    org_id: str,
    since_day: str,
    until_day: str,
    repo_ids: list[str] | None,
    limit: int,
    granularity: TimeGranularity,
) -> list[dict[str, Any]]:
    """Latest-compute read from ``file_complexity_snapshots`` for the date window.

    The table uses ``as_of_day`` (not ``day``) as the snapshot column.
    """
    day_expr = (
        "toStartOfWeek(as_of_day, 1)"
        if granularity == TimeGranularity.WEEK
        else "as_of_day"
    )
    computed_expr = (
        "(as_of_day, computed_at)"
        if granularity == TimeGranularity.WEEK
        else "computed_at"
    )
    query = f"""
        SELECT
            {day_expr} AS day,
            toString(repo_id) AS repo_id,
            file_path,
            argMax(cyclomatic_total,               {computed_expr}) AS cyclomatic_total,
            argMax(cyclomatic_avg,                 {computed_expr}) AS cyclomatic_avg,
            argMax(high_complexity_functions,      {computed_expr}) AS high_complexity_functions,
            argMax(very_high_complexity_functions, {computed_expr}) AS very_high_complexity_functions
        FROM file_complexity_snapshots
        WHERE org_id = {{org_id:String}}
          AND as_of_day >= {{since_day:Date}}
          AND as_of_day <= {{until_day:Date}}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "since_day": since_day,
        "until_day": until_day,
    }
    if repo_ids:
        bounded = list(repo_ids)[:limit]
        query += """
          AND repo_id IN (
              SELECT id FROM repos
              WHERE org_id = {org_id:String}
                AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
          )
        """
        params["repo_ids"] = bounded
    query += f"\nGROUP BY day, repo_id, file_path\nORDER BY day, repo_id\nLIMIT {limit}"
    return await query_dicts(client, query, params)


async def _fetch_hotspot_rows(
    client: Any,
    *,
    org_id: str,
    since_day: str,
    until_day: str,
    repo_ids: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Latest-compute read from ``file_hotspot_daily``, ranked by risk_score DESC."""
    query = """
        SELECT
            toString(repo_id) AS repo_id,
            file_path,
            argMax(churn_loc_30d,       computed_at) AS churn_loc_30d,
            argMax(churn_commits_30d,   computed_at) AS churn_commits_30d,
            argMax(cyclomatic_total,    computed_at) AS cyclomatic_total,
            argMax(cyclomatic_avg,      computed_at) AS cyclomatic_avg,
            argMax(blame_concentration, computed_at) AS blame_concentration,
            argMax(risk_score,          computed_at) AS risk_score
        FROM file_hotspot_daily
        WHERE org_id = {org_id:String}
          AND day >= {since_day:Date}
          AND day <= {until_day:Date}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "since_day": since_day,
        "until_day": until_day,
    }
    if repo_ids:
        bounded = list(repo_ids)[:MAX_ROWS]
        query += """
          AND repo_id IN (
              SELECT id FROM repos
              WHERE org_id = {org_id:String}
                AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
          )
        """
        params["repo_ids"] = bounded
    query += (
        f"\nGROUP BY repo_id, file_path"
        f"\nORDER BY risk_score DESC NULLS LAST\nLIMIT {limit}"
    )
    return await query_dicts(client, query, params)


# ---------------------------------------------------------------------------
# Public resolver functions (called from schema.py)
# ---------------------------------------------------------------------------


async def resolve_complexity_timeseries(
    context: GraphQLContext,
    input: ComplexityTimeseriesInput,
) -> ComplexityTimeseriesResult:
    """Serve complexity timeseries from ClickHouse (read-only, append-only reads).

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

    raw_limit = input.limit if input.limit is not None else DEFAULT_TIMESERIES_LIMIT
    effective_limit = max(1, min(raw_limit, MAX_ROWS))

    since_date = input.since_utc.astimezone(timezone.utc).date()
    until_date = input.until_utc.astimezone(timezone.utc).date()
    bucket_count = _bucket_count(since_date, until_date, input.granularity)
    effective_limit = min(
        effective_limit, max(1, MAX_TIMESERIES_POINTS // bucket_count)
    )
    since_day = since_date.isoformat()
    until_day = until_date.isoformat()

    points: list[ComplexityPoint] = []

    if input.scope == ComplexityScope.REPO:
        rows = await _fetch_repo_timeseries(
            client,
            org_id=authorized_org_id,
            since_day=since_day,
            until_day=until_day,
            repo_ids=input.repo_ids,
            limit=effective_limit,
            granularity=input.granularity,
        )
        repo_ids_seen = list({str(r["repo_id"]) for r in rows})
        labels = await _load_repo_labels(client, authorized_org_id, repo_ids_seen)

        for row in rows:
            repo_id = str(row["repo_id"])
            points.append(
                ComplexityPoint(
                    point_date=row["day"],
                    scope_id=repo_id,
                    scope_name=labels.get(repo_id, repo_id),
                    loc_total=_nint(row, "loc_total"),
                    cyclomatic_per_kloc=_nfloat(row, "cyclomatic_per_kloc"),
                    cyclomatic_total=_nint(row, "cyclomatic_total"),
                    cyclomatic_avg=None,  # not stored per repo row in v1 table
                    high_complexity_functions=_nint(row, "high_complexity_functions"),
                    very_high_complexity_functions=_nint(
                        row, "very_high_complexity_functions"
                    ),
                )
            )

    else:  # FILE scope — reads from file_complexity_snapshots
        rows = await _fetch_file_timeseries(
            client,
            org_id=authorized_org_id,
            since_day=since_day,
            until_day=until_day,
            repo_ids=input.repo_ids,
            limit=effective_limit,
            granularity=input.granularity,
        )
        # FILE-scope scopeName is derived from file_path — no repo-label join needed.
        for row in rows:
            repo_id = str(row["repo_id"])
            file_path = str(row.get("file_path") or "")
            # Composite scopeId encodes both repo and file path for uniqueness.
            scope_id = f"{repo_id}/{file_path}"
            points.append(
                ComplexityPoint(
                    point_date=row["day"],
                    scope_id=scope_id,
                    scope_name=file_path or scope_id,
                    loc_total=None,  # not stored per-file in v1 schema
                    cyclomatic_per_kloc=None,  # not stored per-file in v1 schema
                    cyclomatic_total=_nint(row, "cyclomatic_total"),
                    cyclomatic_avg=_nfloat(row, "cyclomatic_avg"),
                    high_complexity_functions=_nint(row, "high_complexity_functions"),
                    very_high_complexity_functions=_nint(
                        row, "very_high_complexity_functions"
                    ),
                )
            )

    total_scope = len({p.scope_id for p in points})
    return ComplexityTimeseriesResult(points=points, total_scope=total_scope)


async def resolve_hotspots(
    context: GraphQLContext,
    input: HotspotsInput,
) -> HotspotsResult:
    """Serve hotspot rows from ClickHouse (read-only, append-only reads).

    Rows are ordered by ``risk_score DESC NULLS LAST`` at the database level.
    ``evidenceUrl`` is a deterministic deeplink built from ``file_path`` —
    no external service is queried.
    """
    authorized_org_id = require_org_id(context)
    if input.org_id != authorized_org_id:
        logger.debug(
            "Ignoring GraphQL orgId %r in favor of authorized org %r",
            input.org_id,
            authorized_org_id,
        )

    client = _require_client(context)

    raw_limit = input.limit if input.limit is not None else DEFAULT_HOTSPOTS_LIMIT
    effective_limit = max(1, min(raw_limit, MAX_HOTSPOTS_ROWS))

    since_day = input.since_utc.date().isoformat()
    until_day = input.until_utc.date().isoformat()

    raw_rows = await _fetch_hotspot_rows(
        client,
        org_id=authorized_org_id,
        since_day=since_day,
        until_day=until_day,
        repo_ids=input.repo_ids,
        limit=effective_limit,
    )

    repo_ids_seen = list({str(r["repo_id"]) for r in raw_rows})
    labels = await _load_repo_labels(client, authorized_org_id, repo_ids_seen)

    rows: list[HotspotRow] = []
    for row in raw_rows:
        repo_id = str(row["repo_id"])
        file_path = str(row.get("file_path") or "")
        blame = row.get("blame_concentration")
        risk = row.get("risk_score")

        rows.append(
            HotspotRow(
                file_path=file_path,
                repo_id=repo_id,
                repo_name=labels.get(repo_id, repo_id),
                churn_loc_30d=int(row.get("churn_loc_30d") or 0),
                churn_commits_30d=int(row.get("churn_commits_30d") or 0),
                cyclomatic_total=int(row.get("cyclomatic_total") or 0),
                cyclomatic_avg=float(row.get("cyclomatic_avg") or 0.0),
                blame_concentration=float(blame) if blame is not None else None,
                risk_score=float(risk) if risk is not None else 0.0,
                # Deterministic deeplink — never calls an external service.
                evidence_url=f"/code?file={quote(file_path)}" if file_path else None,
            )
        )

    return HotspotsResult(rows=rows)
