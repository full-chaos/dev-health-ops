from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal, cast

from fastapi import HTTPException

from dev_health_ops.metrics.sinks.base import BaseMetricsSink

from ..models.filters import MetricFilter, ScopeFilter, TimeFilter
from ..models.schemas import (
    QuadrantAnnotation,
    QuadrantAxes,
    QuadrantAxis,
    QuadrantPoint,
    QuadrantPointTrajectory,
    QuadrantResponse,
)
from ..queries.client import clickhouse_client, query_dicts
from ..queries.people import fetch_person_team_id, resolve_person_identity
from ..queries.quadrant import (
    fetch_quadrant_metric,
    fetch_work_item_team_quadrant_metric,
)
from ..utils import build_reverse_alias_map, normalize_alias
from .filtering import time_window
from .people_identity import (
    display_name_for_identity,
    identity_variants,
    load_identity_aliases,
    person_id_for_identity,
)


@dataclass(frozen=True)
class MetricSpec:
    metric: str
    label: str
    unit: str
    table: str
    value_expr: str
    entity_expr: str
    label_expr: str
    join_clause: str = ""
    where_clause: str = ""
    transform: Callable[[float], float] = lambda value: value
    use_primary_team_attribution: bool = False


@dataclass(frozen=True)
class AxisDefinition:
    metric: str
    label: str
    unit: str


@dataclass(frozen=True)
class QuadrantDefinition:
    type: str
    x: AxisDefinition
    y: AxisDefinition
    evidence_metric: str


TEAM_LABEL = "ifNull(nullIf(m.team_name, ''), m.team_id)"
logger = logging.getLogger(__name__)

TEAM_METRICS: dict[str, MetricSpec] = {
    "churn": MetricSpec(
        metric="churn",
        label="Churn",
        unit="loc",
        table="user_metrics_daily AS m",
        value_expr="sum(m.loc_touched)",
        entity_expr="m.team_id",
        label_expr=TEAM_LABEL,
        where_clause="AND m.team_id != ''",
    ),
    "throughput": MetricSpec(
        metric="throughput",
        label="Throughput",
        unit="items",
        table="work_item_metrics_daily AS m",
        value_expr="sum(m.items_completed)",
        entity_expr="m.team_id",
        label_expr=TEAM_LABEL,
        where_clause="AND m.team_id != ''",
        use_primary_team_attribution=True,
    ),
    "cycle_time": MetricSpec(
        metric="cycle_time",
        label="Cycle Time",
        unit="days",
        table="work_item_metrics_daily AS m",
        value_expr="avg(m.cycle_time_p50_hours)",
        entity_expr="m.team_id",
        label_expr=TEAM_LABEL,
        where_clause="AND m.cycle_time_p50_hours IS NOT NULL AND m.team_id != ''",
        transform=lambda value: value / 24.0,
        use_primary_team_attribution=True,
    ),
    "wip": MetricSpec(
        metric="wip",
        label="WIP",
        unit="items",
        table="work_item_metrics_daily AS m",
        value_expr="avg(m.wip_count_end_of_day)",
        entity_expr="m.team_id",
        label_expr=TEAM_LABEL,
        where_clause="AND m.team_id != ''",
    ),
    "review_load": MetricSpec(
        metric="review_load",
        label="Review Load",
        unit="reviews",
        table="user_metrics_daily AS m",
        value_expr="sum(m.reviews_given) / nullIf(countDistinct(m.identity_id), 0)",
        entity_expr="m.team_id",
        label_expr=TEAM_LABEL,
        where_clause="AND m.team_id != ''",
    ),
    "review_latency": MetricSpec(
        metric="review_latency",
        label="Review Latency",
        unit="hours",
        table="user_metrics_daily AS m",
        value_expr="avg(m.pr_first_review_p50_hours)",
        entity_expr="m.team_id",
        label_expr=TEAM_LABEL,
        where_clause="AND m.team_id != '' AND m.pr_first_review_p50_hours IS NOT NULL",
    ),
}

REPO_METRICS: dict[str, MetricSpec] = {
    "churn": MetricSpec(
        metric="churn",
        label="Churn",
        unit="loc",
        table="repo_metrics_daily AS m",
        value_expr="sum(m.total_loc_touched)",
        entity_expr="repos.repo",
        label_expr="repos.repo",
        join_clause="INNER JOIN repos ON repos.id = m.repo_id",
        where_clause="AND repos.repo != ''",
    ),
    "throughput": MetricSpec(
        metric="throughput",
        label="Throughput",
        unit="items",
        table="repo_metrics_daily AS m",
        value_expr="sum(m.prs_merged)",
        entity_expr="repos.repo",
        label_expr="repos.repo",
        join_clause="INNER JOIN repos ON repos.id = m.repo_id",
        where_clause="AND repos.repo != ''",
    ),
    "cycle_time": MetricSpec(
        metric="cycle_time",
        label="Cycle Time",
        unit="days",
        table="repo_metrics_daily AS m",
        value_expr="avg(m.median_pr_cycle_hours)",
        entity_expr="repos.repo",
        label_expr="repos.repo",
        join_clause="INNER JOIN repos ON repos.id = m.repo_id",
        where_clause="AND m.median_pr_cycle_hours > 0 AND repos.repo != ''",
        transform=lambda value: value / 24.0,
    ),
    "wip": MetricSpec(
        metric="wip",
        label="WIP",
        unit="items",
        table="work_item_metrics_daily AS m",
        value_expr="avg(m.wip_count_end_of_day)",
        entity_expr="repos.repo",
        label_expr="repos.repo",
        join_clause="INNER JOIN repos ON repos.repo = m.work_scope_id",
        where_clause="AND m.work_scope_id != ''",
    ),
    "review_load": MetricSpec(
        metric="review_load",
        label="Review Load",
        unit="reviews",
        table="user_metrics_daily AS m",
        value_expr="sum(m.reviews_given) / nullIf(countDistinct(m.identity_id), 0)",
        entity_expr="repos.repo",
        label_expr="repos.repo",
        join_clause="INNER JOIN repos ON repos.id = m.repo_id",
        where_clause="AND repos.repo != ''",
    ),
    "review_latency": MetricSpec(
        metric="review_latency",
        label="Review Latency",
        unit="hours",
        table="user_metrics_daily AS m",
        value_expr="avg(m.pr_first_review_p50_hours)",
        entity_expr="repos.repo",
        label_expr="repos.repo",
        join_clause="INNER JOIN repos ON repos.id = m.repo_id",
        where_clause="AND repos.repo != '' AND m.pr_first_review_p50_hours IS NOT NULL",
    ),
}

PERSON_METRICS: dict[str, MetricSpec] = {
    "churn": MetricSpec(
        metric="churn",
        label="Churn",
        unit="loc",
        table="user_metrics_daily AS m",
        value_expr="sum(m.loc_touched)",
        entity_expr="m.identity_id",
        label_expr="m.identity_id",
        where_clause="AND m.identity_id != ''",
    ),
    "throughput": MetricSpec(
        metric="throughput",
        label="Throughput",
        unit="items",
        table="work_item_user_metrics_daily AS m",
        value_expr="sum(m.items_completed)",
        entity_expr="m.user_identity",
        label_expr="m.user_identity",
        where_clause="AND m.user_identity != ''",
    ),
    "cycle_time": MetricSpec(
        metric="cycle_time",
        label="Cycle Time",
        unit="days",
        table="work_item_user_metrics_daily AS m",
        value_expr="avg(m.cycle_time_p50_hours)",
        entity_expr="m.user_identity",
        label_expr="m.user_identity",
        where_clause="AND m.cycle_time_p50_hours IS NOT NULL AND m.user_identity != ''",
        transform=lambda value: value / 24.0,
    ),
    "wip": MetricSpec(
        metric="wip",
        label="WIP",
        unit="items",
        table="work_item_user_metrics_daily AS m",
        value_expr="avg(m.wip_count_end_of_day)",
        entity_expr="m.user_identity",
        label_expr="m.user_identity",
        where_clause="AND m.user_identity != ''",
    ),
    "review_load": MetricSpec(
        metric="review_load",
        label="Review Load",
        unit="reviews",
        table="user_metrics_daily AS m",
        value_expr="sum(m.reviews_given)",
        entity_expr="m.identity_id",
        label_expr="m.identity_id",
        where_clause="AND m.identity_id != ''",
    ),
    "review_latency": MetricSpec(
        metric="review_latency",
        label="Review Latency",
        unit="hours",
        table="user_metrics_daily AS m",
        value_expr="avg(m.pr_first_review_p50_hours)",
        entity_expr="m.identity_id",
        label_expr="m.identity_id",
        where_clause="AND m.identity_id != '' AND m.pr_first_review_p50_hours IS NOT NULL",
    ),
}

METRICS_BY_SCOPE = {
    "team": TEAM_METRICS,
    "repo": REPO_METRICS,
    "person": PERSON_METRICS,
}

QUADRANT_DEFINITIONS: dict[str, QuadrantDefinition] = {
    "churn_throughput": QuadrantDefinition(
        type="churn_throughput",
        x=AxisDefinition(metric="churn", label="Churn", unit="loc"),
        y=AxisDefinition(metric="throughput", label="Throughput", unit="items"),
        evidence_metric="throughput",
    ),
    "cycle_throughput": QuadrantDefinition(
        type="cycle_throughput",
        x=AxisDefinition(metric="cycle_time", label="Cycle Time", unit="days"),
        y=AxisDefinition(metric="throughput", label="Throughput", unit="items"),
        evidence_metric="cycle_time",
    ),
    "wip_throughput": QuadrantDefinition(
        type="wip_throughput",
        x=AxisDefinition(metric="wip", label="WIP", unit="items"),
        y=AxisDefinition(metric="throughput", label="Throughput", unit="items"),
        evidence_metric="throughput",
    ),
    "review_load_latency": QuadrantDefinition(
        type="review_load_latency",
        x=AxisDefinition(metric="review_load", label="Review Load", unit="reviews"),
        y=AxisDefinition(metric="review_latency", label="Review Latency", unit="hours"),
        evidence_metric="review_latency",
    ),
}


def _normalize_scope(scope_type: str) -> str:
    if scope_type in {"developer", "person"}:
        return "person"
    return scope_type


def _group_scope(scope_type: str) -> str:
    if scope_type in {"org", "team"}:
        return "team"
    if scope_type == "repo":
        return "repo"
    if scope_type == "person":
        return "person"
    return "team"


def _normalize_range_days(range_days: int) -> int:
    try:
        value = int(range_days)
    except Exception:
        value = 30
    return max(1, min(value, 180))


async def _resolve_identity_variants(
    sink: BaseMetricsSink,
    *,
    person_id: str,
    org_id: str,
) -> list[str]:
    aliases = load_identity_aliases()
    reverse = build_reverse_alias_map(aliases)

    identity = await resolve_person_identity(sink, person_id=person_id, org_id=org_id)
    if identity:
        normalized = normalize_alias(identity)
        canonical = reverse.get(normalized, identity)
        alias_list = list(aliases.get(canonical, []))
        if identity not in alias_list and identity != canonical:
            alias_list.append(identity)
        return identity_variants(canonical, alias_list)

    for canonical, alias_list in aliases.items():
        if person_id_for_identity(canonical) == person_id:
            return identity_variants(canonical, alias_list)
        for alias in alias_list:
            if person_id_for_identity(alias) == person_id:
                return identity_variants(canonical, alias_list)

    return []


def _bucket_start(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return None


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _bucket_window_end(bucket_start: date, bucket: str) -> date:
    if bucket == "month":
        return _next_month(bucket_start)
    return bucket_start + timedelta(days=7)


def _cohort_scope_filter(team_id: str | None) -> tuple[str, dict[str, Any]]:
    if not team_id:
        return "", {}
    return "AND m.team_id = %(team_id)s", {"team_id": team_id}


def _row_entity(
    row: dict[str, Any],
    *,
    group_scope: str,
) -> tuple[str, str]:
    entity_id = str(row.get("entity_id") or "").strip()
    entity_label = str(row.get("entity_label") or entity_id).strip()
    if group_scope != "person":
        return entity_id, entity_label or entity_id
    if not entity_id:
        return "", ""
    person_id = person_id_for_identity(entity_id)
    label = display_name_for_identity(entity_id)
    return person_id, label


async def _resolve_team_labels(
    sink: BaseMetricsSink,
    *,
    team_ids: set[str],
    org_id: str,
) -> dict[str, str]:
    if not team_ids:
        return {}
    try:
        rows = await query_dicts(
            sink,
            """
            SELECT toString(id) AS team_id, name AS team_name
            FROM teams FINAL
            WHERE org_id = %(org_id)s
              AND toString(id) IN %(team_ids)s
            """,
            {"org_id": org_id, "team_ids": sorted(team_ids)},
        )
    except Exception as exc:
        logger.warning("Could not resolve quadrant team labels: %s", exc)
        return {}
    return {
        str(row.get("team_id") or ""): str(row.get("team_name") or "").strip()
        for row in rows
        if row.get("team_id") and str(row.get("team_name") or "").strip()
    }


def _build_axes(definition: QuadrantDefinition) -> QuadrantAxes:
    return QuadrantAxes(
        x=QuadrantAxis(
            metric=definition.x.metric,
            label=definition.x.label,
            unit=definition.x.unit,
        ),
        y=QuadrantAxis(
            metric=definition.y.metric,
            label=definition.y.label,
            unit=definition.y.unit,
        ),
    )


def _metric_spec(metric_key: str, group_scope: str) -> MetricSpec:
    metric_set = METRICS_BY_SCOPE.get(group_scope)
    if not metric_set or metric_key not in metric_set:
        raise HTTPException(status_code=400, detail="Metric not supported for scope")
    return metric_set[metric_key]


def _evidence_link(metric: str) -> str:
    return f"/api/v1/explain?metric={metric}"


def _trajectory(
    windows: list[dict[str, Any]],
) -> list[QuadrantPointTrajectory]:
    return [
        QuadrantPointTrajectory(
            x=window["x"],
            y=window["y"],
            window=window["window_start"].isoformat(),
        )
        for window in windows
    ]


async def build_quadrant_response(
    *,
    db_url: str,
    org_id: str = "",
    type: str,
    scope_type: str,
    scope_id: str,
    range_days: int,
    bucket: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> QuadrantResponse:
    definition = QUADRANT_DEFINITIONS.get(type)
    if definition is None:
        raise HTTPException(status_code=404, detail="Unknown quadrant type")

    normalized_scope = _normalize_scope(scope_type)
    # Churn is repo-attributed at ingest; user_metrics_daily.team_id is sparse, so
    # team/org-grain churn collapses to ~0 and the scatter degenerates onto the y-axis
    # (CHAOS-2079). For churn_throughput, always enumerate repos regardless of the
    # caller's filter scope so the quadrant plots real per-repo churn. Person grain
    # (the individual view) is left intact, and cycle/wip/review quadrants are
    # unaffected because the branch is gated on the quadrant type. The scope id does
    # not filter entities at team/repo grain (scope_filter is only set for person
    # scope below), so an inbound team id here is harmless.
    if type == "churn_throughput" and normalized_scope in {"org", "team"}:
        normalized_scope = "repo"
    group_scope = _group_scope(normalized_scope)
    filter_scope = cast(
        Literal["org", "team", "repo", "service", "developer"],
        "developer" if normalized_scope == "person" else normalized_scope,
    )
    if group_scope == "person" and not scope_id:
        raise HTTPException(
            status_code=400, detail="Individual quadrants require a person id"
        )

    if bucket not in {"week", "month"}:
        raise HTTPException(status_code=400, detail="Bucket must be week or month")

    range_days = _normalize_range_days(range_days)
    try:
        filters = MetricFilter(
            time=TimeFilter(
                range_days=range_days,
                compare_days=range_days,
                start_date=start_date,
                end_date=end_date,
            ),
            scope=ScopeFilter(
                level=filter_scope,
                ids=[scope_id] if scope_id else [],
            ),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid scope filter") from exc

    start_day, end_day, _, _ = time_window(filters)

    async with clickhouse_client(db_url) as sink:
        team_filter: str | None = None
        scope_filter = ""
        scope_params: dict[str, Any] = {}

        if group_scope == "person":
            identities = await _resolve_identity_variants(
                sink, person_id=scope_id, org_id=org_id
            )
            if not identities:
                raise HTTPException(status_code=404, detail="Individual not found")
            team_filter = await fetch_person_team_id(
                sink, identities=identities, org_id=org_id
            )
            scope_filter, scope_params = _cohort_scope_filter(team_filter)

        x_spec = _metric_spec(definition.x.metric, group_scope)
        y_spec = _metric_spec(definition.y.metric, group_scope)

        async def _fetch_metric_rows(spec: MetricSpec) -> list[dict[str, Any]]:
            if (
                definition.type == "cycle_throughput"
                and group_scope == "team"
                and spec.use_primary_team_attribution
            ):
                return await fetch_work_item_team_quadrant_metric(
                    sink,
                    metric=spec.metric,
                    start_day=start_day,
                    end_day=end_day,
                    bucket=bucket,
                    org_id=org_id,
                )
            return await fetch_quadrant_metric(
                sink,
                table=spec.table,
                value_expr=spec.value_expr,
                start_day=start_day,
                end_day=end_day,
                bucket=bucket,
                entity_expr=spec.entity_expr,
                label_expr=spec.label_expr,
                join_clause=spec.join_clause,
                where_clause=spec.where_clause,
                scope_filter=scope_filter,
                scope_params=scope_params,
                org_id=org_id,
            )

        x_rows, y_rows = await asyncio.gather(
            _fetch_metric_rows(x_spec),
            _fetch_metric_rows(y_spec),
        )

        team_labels = (
            await _resolve_team_labels(
                sink,
                team_ids={
                    str(row.get("entity_id") or "").strip()
                    for row in [*x_rows, *y_rows]
                    if str(row.get("entity_id") or "").strip()
                },
                org_id=org_id,
            )
            if group_scope == "team"
            else {}
        )

    x_map: dict[tuple[str, date], dict[str, Any]] = {}
    for row in x_rows:
        bucket_start = _bucket_start(row.get("bucket"))
        if not bucket_start:
            continue
        entity_id, label = _row_entity(row, group_scope=group_scope)
        if not entity_id:
            continue
        label = team_labels.get(entity_id, label)
        value = x_spec.transform(float(row.get("value") or 0.0))
        x_map[(entity_id, bucket_start)] = {
            "entity_id": entity_id,
            "label": label or entity_id,
            "x": value,
        }

    points_by_entity: dict[str, list[dict[str, Any]]] = {}
    for row in y_rows:
        bucket_start = _bucket_start(row.get("bucket"))
        if not bucket_start:
            continue
        entity_id, label = _row_entity(row, group_scope=group_scope)
        if not entity_id:
            continue
        label = team_labels.get(entity_id, label)
        key = (entity_id, bucket_start)
        x_entry = x_map.get(key)
        if not x_entry:
            continue
        y_value = y_spec.transform(float(row.get("value") or 0.0))
        window_start = bucket_start
        window_end = _bucket_window_end(bucket_start, bucket)
        points_by_entity.setdefault(entity_id, []).append(
            {
                "entity_id": entity_id,
                "label": label or x_entry["label"],
                "x": x_entry["x"],
                "y": y_value,
                "window_start": window_start,
                "window_end": window_end,
            }
        )

    points: list[QuadrantPoint] = []
    for entity_id, windows in points_by_entity.items():
        ordered = sorted(windows, key=lambda item: item["window_start"])
        if not ordered:
            continue
        latest = ordered[-1]
        points.append(
            QuadrantPoint(
                entity_id=entity_id,
                entity_label=latest["label"],
                x=latest["x"],
                y=latest["y"],
                window_start=latest["window_start"],
                window_end=latest["window_end"],
                evidence_link=_evidence_link(definition.evidence_metric),
                trajectory=_trajectory(ordered) if len(ordered) > 1 else None,
            )
        )

    axes = _build_axes(definition)
    annotations: list[QuadrantAnnotation] = []
    return QuadrantResponse(axes=axes, points=points, annotations=annotations)
