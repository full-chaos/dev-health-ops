"""Compounding Risk GraphQL resolver (CHAOS-1642).

Reads from the append-only ``compounding_risk_daily`` table populated by the
daily metrics job (CHAOS-1641). The resolver is read-only and never
recomputes the composite — it surfaces persisted distributions exactly as
they were computed, honoring the inspectability contract.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from dev_health_ops.api.queries.client import query_dicts

from ..authz import require_org_id
from ..context import GraphQLContext
from ..types.compounding_risk import (
    CompoundingRiskComponents,
    CompoundingRiskFilterInput,
    CompoundingRiskPoint,
    CompoundingRiskResult,
    CompoundingRiskScope,
    CompoundingRiskScopeEntity,
    CompoundingRiskSeverity,
    CompoundingRiskThresholds,
    CompoundingRiskTrendPoint,
    CompoundingRiskWeights,
)

logger = logging.getLogger(__name__)

#: Hard limit on rows returned; protects against pathological scopes.
MAX_ROWS: int = 500
#: Hard limit on trend window in days.
MAX_TREND_DAYS: int = 365


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError(
            "Database client not available for Compounding Risk resolver"
        )
    return context.client


def _severity_from_str(value: Any) -> CompoundingRiskSeverity:
    raw = str(value or "unknown").lower()
    try:
        return CompoundingRiskSeverity(raw)
    except ValueError:
        return CompoundingRiskSeverity.UNKNOWN


# NOTE: ClickHouse access goes through the canonical async helper
# ``query_dicts(sink, query, params)`` from ``api.queries.client`` —
# ``context.client`` is a ``ClickHouseMetricsSink`` wrapper, not a raw
# ``clickhouse_connect`` client. Earlier revisions of this resolver
# carried a private ``_query_dicts(client, ...)`` that called
# ``client.query(...)`` directly; that path raised
# ``AttributeError: 'ClickHouseMetricsSink' object has no attribute 'query'``
# at runtime against the live sink and is replaced with the canonical helper.


async def _latest_day_for_org(
    client: Any,
    org_id: str,
    *,
    scope: str,
    scope_ids: list[str] | None,
    start_day: date | None = None,
    end_day: date | None = None,
) -> date | None:
    if scope_ids == []:
        return None

    bounded_end_day = end_day or datetime.now(timezone.utc).date()
    bounded_start_day = start_day or bounded_end_day - timedelta(days=29)

    query = """
        SELECT maxOrNull(day) AS day
        FROM (
            SELECT
                day,
                countIf(tupleElement(latest_row, 1) IS NOT NULL) AS scored_rows
            FROM (
                SELECT
                    day,
                    scope_id,
                    argMax(tuple(compounding_risk), computed_at) AS latest_row
                FROM compounding_risk_daily
                WHERE org_id = {org_id:String}
                  AND scope = {scope:String}
                  AND day >= {start_day:Date}
                  AND day <= {end_day:Date}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "scope": scope,
        "start_day": bounded_start_day,
        "end_day": bounded_end_day,
    }
    if scope_ids:
        bounded = list(scope_ids)[:MAX_ROWS]
        if scope == "repo":
            query += """
                  AND scope_id IN (
                      SELECT toString(id) FROM repos
                      WHERE org_id = {org_id:String}
                        AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
                  )
            """
            params["repo_ids"] = bounded
        else:
            query += "\n                  AND scope_id IN {scope_ids:Array(String)}"
            params["scope_ids"] = bounded
    query += """
                GROUP BY day, scope_id
            )
            GROUP BY day
        )
        WHERE scored_rows > 0
    """
    rows = await query_dicts(client, query, params)
    if not rows:
        return None
    value = rows[0].get("day")
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    return None


async def _fetch_latest_rows(
    client: Any,
    *,
    org_id: str,
    day: date,
    scope: str,
    scope_ids: list[str] | None,
) -> list[dict[str, Any]]:
    """Latest per-(scope_id) row at the given day, for one scope value.

    ``argMax(tuple(...), computed_at)`` returns the most-recent compute for
    each scope_id on that day without ClickHouse skipping nullable score
    values. All selected fields are unpacked from the same latest tuple so the
    UI cannot mix an older score with newer audit fields.
    """
    if scope_ids == []:
        return []

    query = """
        SELECT
            scope_id,
            tupleElement(latest_row, 1)  AS score,
            tupleElement(latest_row, 2)  AS severity,
            tupleElement(latest_row, 3)  AS churn_norm,
            tupleElement(latest_row, 4)  AS complexity_norm,
            tupleElement(latest_row, 5)  AS ownership_norm,
            tupleElement(latest_row, 6)  AS review_norm,
            tupleElement(latest_row, 7)  AS rework_churn,
            tupleElement(latest_row, 8)  AS complexity_delta,
            tupleElement(latest_row, 9)  AS bus_factor,
            tupleElement(latest_row, 10) AS ownership_gini,
            tupleElement(latest_row, 11) AS single_owner_ratio,
            tupleElement(latest_row, 12) AS review_latency_p90h,
            tupleElement(latest_row, 13) AS w_churn,
            tupleElement(latest_row, 14) AS w_complexity,
            tupleElement(latest_row, 15) AS w_ownership,
            tupleElement(latest_row, 16) AS w_review,
            tupleElement(latest_row, 17) AS threshold_elevated,
            tupleElement(latest_row, 18) AS threshold_high,
            tupleElement(latest_row, 19) AS latest_computed_at
        FROM (
            SELECT
                scope_id,
                argMax(
                    tuple(
                        compounding_risk,
                        severity,
                        churn_norm,
                        complexity_norm,
                        ownership_norm,
                        review_norm,
                        rework_churn,
                        complexity_delta,
                        bus_factor,
                        ownership_gini,
                        single_owner_ratio,
                        review_latency_p90h,
                        w_churn,
                        w_complexity,
                        w_ownership,
                        w_review,
                        threshold_elevated,
                        threshold_high,
                        computed_at
                    ),
                    computed_at
                ) AS latest_row
            FROM compounding_risk_daily
            WHERE org_id = {org_id:String}
              AND scope = {scope:String}
              AND day = {day:Date}
    """
    params: dict[str, Any] = {"org_id": org_id, "day": day, "scope": scope}
    if scope_ids:
        bounded = list(scope_ids)[:MAX_ROWS]
        if scope == "repo":
            query += """
          AND scope_id IN (
              SELECT toString(id) FROM repos
              WHERE org_id = {org_id:String}
                AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
          )
        """
            params["repo_ids"] = bounded
        else:
            query += "\n  AND scope_id IN {scope_ids:Array(String)}"
            params["scope_ids"] = bounded
    query += (
        f"\n    GROUP BY scope_id\n)\nORDER BY score DESC NULLS LAST\nLIMIT {MAX_ROWS}"
    )
    return await query_dicts(client, query, params)


async def _fetch_repo_trend(
    client: Any,
    org_id: str,
    end_day: date,
    trend_days: int,
    repo_ids: list[str] | None,
) -> list[dict[str, Any]]:
    if repo_ids == []:
        return []

    start_day = end_day - timedelta(days=max(0, trend_days - 1))
    query = """
        SELECT day,
               avg(score) AS avg_score
        FROM (
            SELECT
                day,
                scope_id,
                tupleElement(argMax(tuple(compounding_risk), computed_at), 1) AS score
            FROM compounding_risk_daily
            WHERE org_id = {org_id:String}
              AND scope = 'repo'
              AND day >= {start:Date} AND day <= {end:Date}
    """
    params: dict[str, Any] = {
        "org_id": org_id,
        "start": start_day,
        "end": end_day,
    }
    if repo_ids:
        bounded = list(repo_ids)[:MAX_ROWS]
        query += """
              AND scope_id IN (
                  SELECT toString(id) FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
              )"""
        params["repo_ids"] = bounded
    query += """
            GROUP BY day, scope_id
        )
        GROUP BY day ORDER BY day
    """
    return await query_dicts(client, query, params)


async def _repo_scope_ids_for_team_fallback(
    client: Any,
    org_id: str,
    *,
    repo_ids: list[str] | None,
    team_ids: list[str] | None,
) -> list[str] | None:
    if not team_ids:
        return repo_ids

    repo_to_team, _ = await _load_team_assignments(client, org_id)
    team_filter = set(team_ids)
    repos_for_teams = {
        repo_id for repo_id, team_id in repo_to_team.items() if team_id in team_filter
    }
    if repo_ids:
        repo_filter = set(repo_ids)
        return sorted(repo_id for repo_id in repos_for_teams if repo_id in repo_filter)
    return sorted(repos_for_teams)


def _components_from_row(row: dict[str, Any]) -> CompoundingRiskComponents:
    def _nf(key: str) -> float | None:
        v = row.get(key)
        return float(v) if v is not None else None

    return CompoundingRiskComponents(
        churn_norm=_nf("churn_norm"),
        complexity_norm=_nf("complexity_norm"),
        ownership_norm=_nf("ownership_norm"),
        review_norm=_nf("review_norm"),
        rework_churn=_nf("rework_churn"),
        complexity_delta=_nf("complexity_delta"),
        bus_factor=_nf("bus_factor"),
        ownership_gini=_nf("ownership_gini"),
        single_owner_ratio=_nf("single_owner_ratio"),
        review_latency_p90h=_nf("review_latency_p90h"),
    )


def _weights_from_row(row: dict[str, Any]) -> CompoundingRiskWeights:
    return CompoundingRiskWeights(
        churn=float(row.get("w_churn") or 0.0),
        complexity=float(row.get("w_complexity") or 0.0),
        ownership=float(row.get("w_ownership") or 0.0),
        review=float(row.get("w_review") or 0.0),
    )


def _thresholds_from_row(row: dict[str, Any]) -> CompoundingRiskThresholds:
    return CompoundingRiskThresholds(
        elevated=float(row.get("threshold_elevated") or 0.0),
        high=float(row.get("threshold_high") or 0.0),
    )


def _point_from_repo_row(
    row: dict[str, Any], day: date, label_resolver: dict[str, str]
) -> CompoundingRiskPoint:
    scope_id = str(row["scope_id"])
    score_val = row.get("score")
    return CompoundingRiskPoint(
        day=day,
        scope=CompoundingRiskScope.REPO,
        scope_id=scope_id,
        scope_label=label_resolver.get(scope_id, scope_id),
        score=float(score_val) if score_val is not None else None,
        severity=_severity_from_str(row.get("severity")),
        components=_components_from_row(row),
        weights=_weights_from_row(row),
        thresholds=_thresholds_from_row(row),
        computed_at=row.get("latest_computed_at") or datetime.now(timezone.utc),
        scope_entity=CompoundingRiskScopeEntity(
            id=scope_id,
            display_name=label_resolver.get(scope_id, scope_id),
        ),
    )


def _point_from_team_row(
    row: dict[str, Any], day: date, team_labels: dict[str, str]
) -> CompoundingRiskPoint:
    scope_id = str(row["scope_id"])
    score_val = row.get("score")
    return CompoundingRiskPoint(
        day=day,
        scope=CompoundingRiskScope.TEAM,
        scope_id=scope_id,
        scope_label=team_labels.get(scope_id, scope_id),
        score=float(score_val) if score_val is not None else None,
        severity=_severity_from_str(row.get("severity")),
        components=_components_from_row(row),
        weights=_weights_from_row(row),
        thresholds=_thresholds_from_row(row),
        computed_at=row.get("latest_computed_at") or datetime.now(timezone.utc),
        scope_entity=CompoundingRiskScopeEntity(
            id=scope_id,
            display_name=team_labels.get(scope_id, scope_id),
        ),
    )


def _aggregate_repo_rows_to_team(
    repo_rows: list[dict[str, Any]],
    *,
    repo_to_team: dict[str, str],
    team_labels: dict[str, str],
    team_id_filter: list[str] | None,
    day: date,
) -> list[CompoundingRiskPoint]:
    """Read-time aggregation of repo-scope rows into team-scope points.

    v1 strategy: unweighted mean of repo scores per team. Component values
    are similarly averaged. Audit fields (weights, thresholds) carry from
    the first repo in the team — these are constant across repos in the
    same compute pass so the mean is a no-op.

    Team-scope persistence is a follow-up optimisation (see plan).
    """
    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in repo_rows:
        team = repo_to_team.get(str(row["scope_id"]))
        if not team:
            continue
        if team_id_filter and team not in team_id_filter:
            continue
        by_team[team].append(row)

    out: list[CompoundingRiskPoint] = []
    for team_id, rows in by_team.items():
        score_values: list[float] = [
            float(r["score"]) for r in rows if r.get("score") is not None
        ]
        avg_score: float | None = (
            sum(score_values) / len(score_values) if score_values else None
        )

        def _avg(key: str, source_rows: list[dict[str, Any]] = rows) -> float | None:
            vals: list[float] = [
                float(r[key]) for r in source_rows if r.get(key) is not None
            ]
            return sum(vals) / len(vals) if vals else None

        first = rows[0]
        components = CompoundingRiskComponents(
            churn_norm=_avg("churn_norm"),
            complexity_norm=_avg("complexity_norm"),
            ownership_norm=_avg("ownership_norm"),
            review_norm=_avg("review_norm"),
            rework_churn=_avg("rework_churn"),
            complexity_delta=_avg("complexity_delta"),
            bus_factor=_avg("bus_factor"),
            ownership_gini=_avg("ownership_gini"),
            single_owner_ratio=_avg("single_owner_ratio"),
            review_latency_p90h=_avg("review_latency_p90h"),
        )
        # Severity is recomputed from the averaged score using the same
        # thresholds the underlying rows were computed with.
        thresholds = _thresholds_from_row(first)
        if avg_score is None:
            severity = CompoundingRiskSeverity.UNKNOWN
        elif avg_score >= thresholds.high:
            severity = CompoundingRiskSeverity.HIGH
        elif avg_score >= thresholds.elevated:
            severity = CompoundingRiskSeverity.ELEVATED
        else:
            severity = CompoundingRiskSeverity.LOW

        out.append(
            CompoundingRiskPoint(
                day=day,
                scope=CompoundingRiskScope.TEAM,
                scope_id=team_id,
                scope_label=team_labels.get(team_id, team_id),
                score=avg_score,
                severity=severity,
                components=components,
                weights=_weights_from_row(first),
                thresholds=thresholds,
                computed_at=first.get("latest_computed_at")
                or datetime.now(timezone.utc),
                scope_entity=CompoundingRiskScopeEntity(
                    id=team_id,
                    display_name=team_labels.get(team_id, team_id),
                ),
            )
        )
    # Sort by score desc, nulls last.
    out.sort(key=lambda p: (p.score is None, -(p.score or 0.0)))
    return out


async def _load_repo_labels(
    client: Any, org_id: str, repo_ids: list[str]
) -> dict[str, str]:
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


async def _load_team_assignments(
    client: Any, org_id: str
) -> tuple[dict[str, str], dict[str, str]]:
    """Return ``(repo_id → team_id, team_id → team_name)`` mappings.

    The mapping is best-effort. If the ``teams`` table cannot be reached,
    the resolver simply returns no team rows rather than erroring.
    """
    try:
        team_rows = await query_dicts(
            client,
            """
            SELECT id, name, repo_patterns
            FROM teams
            WHERE org_id = {org_id:String}
            """,
            {"org_id": org_id},
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not load teams for compounding risk: %s", exc)
        return {}, {}

    repo_to_team: dict[str, str] = {}
    team_labels: dict[str, str] = {}
    for row in team_rows:
        team_id = str(row["id"])
        team_labels[team_id] = row.get("name") or team_id
        for repo_id in row.get("repo_patterns") or []:
            repo_to_team[str(repo_id)] = team_id
    return repo_to_team, team_labels


async def resolve_compounding_risk(
    context: GraphQLContext,
    org_id: str,
    filter: CompoundingRiskFilterInput | None = None,  # noqa: A002
) -> CompoundingRiskResult:
    """Read latest Compounding Risk rows for the org, with optional breakout."""
    authorized_org_id = require_org_id(context)
    if org_id != authorized_org_id:
        logger.debug(
            "Ignoring GraphQL orgId %r in favor of authorized org %r",
            org_id,
            authorized_org_id,
        )

    client = _require_client(context)
    filt = filter or CompoundingRiskFilterInput()
    breakout = filt.breakout
    trend_days = max(1, min(filt.trend_days, MAX_TREND_DAYS))
    latest_end_day = datetime.now(timezone.utc).date()
    latest_start_day = latest_end_day - timedelta(days=max(0, trend_days - 1))
    fallback_repo_scope_ids: list[str] | None = filt.repo_ids
    team_filtered_repo_scope_ids: list[str] | None = None

    day: date | None
    if filt.day is not None:
        day = filt.day
    elif breakout == CompoundingRiskScope.TEAM:
        day = await _latest_day_for_org(
            client,
            authorized_org_id,
            scope="team",
            scope_ids=filt.team_ids,
            start_day=latest_start_day,
            end_day=latest_end_day,
        )
        if day is None:
            team_filtered_repo_scope_ids = await _repo_scope_ids_for_team_fallback(
                client,
                authorized_org_id,
                repo_ids=filt.repo_ids,
                team_ids=filt.team_ids,
            )
            fallback_repo_scope_ids = team_filtered_repo_scope_ids
            day = await _latest_day_for_org(
                client,
                authorized_org_id,
                scope="repo",
                scope_ids=fallback_repo_scope_ids,
                start_day=latest_start_day,
                end_day=latest_end_day,
            )
    else:
        day = await _latest_day_for_org(
            client,
            authorized_org_id,
            scope="repo",
            scope_ids=filt.repo_ids,
            start_day=latest_start_day,
            end_day=latest_end_day,
        )
    if day is None:
        return CompoundingRiskResult(
            org_id=authorized_org_id,
            breakout=breakout,
            rows=[],
            trend=[],
            generated_at=datetime.now(timezone.utc),
        )

    points: list[CompoundingRiskPoint]
    if breakout == CompoundingRiskScope.REPO:
        repo_rows = await _fetch_latest_rows(
            client,
            org_id=authorized_org_id,
            day=day,
            scope="repo",
            scope_ids=filt.repo_ids,
        )
        repo_ids = [str(r["scope_id"]) for r in repo_rows]
        labels = await _load_repo_labels(client, authorized_org_id, repo_ids)
        points = [_point_from_repo_row(r, day, labels) for r in repo_rows]
    else:
        # Prefer persisted team-scope rows. Fall back to read-time
        # aggregation only when the team rows are missing (back-compat
        # with deployments that haven't yet caught up to the orchestrator
        # change).
        team_rows = await _fetch_latest_rows(
            client,
            org_id=authorized_org_id,
            day=day,
            scope="team",
            scope_ids=filt.team_ids,
        )
        if team_rows:
            _, team_labels = await _load_team_assignments(client, authorized_org_id)
            points = [_point_from_team_row(r, day, team_labels) for r in team_rows]
        else:
            if filt.team_ids and team_filtered_repo_scope_ids is None:
                team_filtered_repo_scope_ids = await _repo_scope_ids_for_team_fallback(
                    client,
                    authorized_org_id,
                    repo_ids=filt.repo_ids,
                    team_ids=filt.team_ids,
                )
                fallback_repo_scope_ids = team_filtered_repo_scope_ids
            # Fallback path: aggregate from the repo rows.
            repo_rows = await _fetch_latest_rows(
                client,
                org_id=authorized_org_id,
                day=day,
                scope="repo",
                scope_ids=fallback_repo_scope_ids,
            )
            repo_to_team, team_labels = await _load_team_assignments(
                client, authorized_org_id
            )
            points = _aggregate_repo_rows_to_team(
                repo_rows,
                repo_to_team=repo_to_team,
                team_labels=team_labels,
                team_id_filter=filt.team_ids,
                day=day,
            )

    if breakout == CompoundingRiskScope.TEAM:
        if filt.team_ids and team_filtered_repo_scope_ids is None:
            team_filtered_repo_scope_ids = await _repo_scope_ids_for_team_fallback(
                client,
                authorized_org_id,
                repo_ids=filt.repo_ids,
                team_ids=filt.team_ids,
            )
        trend_repo_scope_ids = (
            team_filtered_repo_scope_ids if filt.team_ids else fallback_repo_scope_ids
        )
    else:
        trend_repo_scope_ids = filt.repo_ids
    trend_rows = await _fetch_repo_trend(
        client, authorized_org_id, day, trend_days, trend_repo_scope_ids
    )
    trend = [
        CompoundingRiskTrendPoint(
            day=row["day"],
            score=float(row["avg_score"]) if row.get("avg_score") is not None else None,
            severity=_severity_from_str(None),  # severity not aggregated for trend
        )
        for row in trend_rows
    ]

    return CompoundingRiskResult(
        org_id=authorized_org_id,
        breakout=breakout,
        rows=points,
        trend=trend,
        generated_at=datetime.now(timezone.utc),
    )
