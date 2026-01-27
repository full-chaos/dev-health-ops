from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from fastapi import HTTPException

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from ..models.schemas import HeatmapAxes, HeatmapCell, HeatmapLegend, HeatmapResponse
from ..models.filters import MetricFilter, ScopeFilter, TimeFilter
from ..queries.client import clickhouse_client
from ..queries.heatmap import (
    fetch_hotspot_evidence,
    fetch_hotspot_risk,
    fetch_individual_active_evidence,
    fetch_individual_active_hours,
    fetch_repo_touchpoints,
    fetch_review_wait_density,
    fetch_review_wait_evidence,
)
from ..queries.people import resolve_person_identity
from .filtering import scope_filter_for_metric, time_window
from .people_identity import (
    identity_variants,
    load_identity_aliases,
    person_id_for_identity,
)


WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
STATUS_ORDER = [
    "backlog",
    "todo",
    "in_progress",
    "in_review",
    "blocked",
    "done",
    "canceled",
    "unknown",
]


@dataclass(frozen=True)
class HeatmapMetric:
    type: str
    metric: str
    unit: str
    scale: str
    x_axis: str
    y_axis: str
    x_field: str
    y_field: str
    scope: str


HEATMAP_METRICS: List[HeatmapMetric] = [
    HeatmapMetric(
        type="temporal_load",
        metric="review_wait_density",
        unit="hours",
        scale="linear",
        x_axis="hour",
        y_axis="weekday",
        x_field="hour",
        y_field="weekday",
        scope="repo",
    ),
    HeatmapMetric(
        type="context_switch",
        metric="repo_touchpoints",
        unit="commits",
        scale="linear",
        x_axis="day",
        y_axis="repo",
        x_field="day",
        y_field="repo",
        scope="repo",
    ),
    HeatmapMetric(
        type="risk",
        metric="hotspot_risk",
        unit="hotspot score",
        scale="log",
        x_axis="week",
        y_axis="file",
        x_field="week",
        y_field="file_key",
        scope="repo",
    ),
    HeatmapMetric(
        type="individual",
        metric="active_hours",
        unit="commits",
        scale="linear",
        x_axis="hour",
        y_axis="weekday",
        x_field="hour",
        y_field="weekday",
        scope="developer",
    ),
]


def _metric_for(type_value: str, metric_value: str) -> Optional[HeatmapMetric]:
    for metric in HEATMAP_METRICS:
        if metric.type == type_value and metric.metric == metric_value:
            return metric
    return None


def _hour_labels() -> List[str]:
    return [f"{hour:02d}" for hour in range(24)]


def _weekday_labels() -> List[str]:
    return WEEKDAY_LABELS[:]


def _axis_order(
    kind: str, values: Iterable[str], totals: Dict[str, float]
) -> List[str]:
    values_list = list(dict.fromkeys([v for v in values if v is not None]))
    if kind == "hour":
        return _hour_labels()
    if kind == "weekday":
        return [
            label for label in _weekday_labels() if label in values_list
        ] or _weekday_labels()
    if kind in {"day", "week"}:
        return sorted(values_list)
    if kind == "status":
        return [status for status in STATUS_ORDER if status in values_list]
    if kind == "age_bucket":
        buckets = ["0-1d", "1-3d", "3-7d", "7-14d", "14-30d", "30d+"]
        return [bucket for bucket in buckets if bucket in values_list]
    return sorted(values_list, key=lambda v: totals.get(v, 0.0), reverse=True)


def _format_axis_value(kind: str, value: Any) -> str:
    if value is None:
        return ""
    if kind == "hour":
        try:
            return f"{int(value):02d}"
        except Exception:
            return str(value)
    if kind == "weekday":
        try:
            idx = int(value)
        except Exception:
            return str(value)
        if 1 <= idx <= 7:
            return WEEKDAY_LABELS[idx - 1]
        return str(value)
    if kind in {"day", "week"}:
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, datetime):
            return value.date().isoformat()
    return str(value)


def _totals_by_axis(rows: List[Dict[str, Any]], axis_key: str) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for row in rows:
        key = row.get(axis_key)
        label = str(key) if key is not None else ""
        value = float(row.get("value") or 0.0)
        totals[label] = totals.get(label, 0.0) + value
    return totals


def _axis_values(rows: List[Dict[str, Any]], key: str, kind: str) -> List[str]:
    values = [_format_axis_value(kind, row.get(key)) for row in rows]
    totals = _totals_by_axis(rows, key)
    return _axis_order(kind, values, totals)


def _cells_from_rows(
    rows: List[Dict[str, Any]],
    *,
    x_key: str,
    y_key: str,
    x_kind: str,
    y_kind: str,
) -> List[HeatmapCell]:
    cells: List[HeatmapCell] = []
    for row in rows:
        x_label = _format_axis_value(x_kind, row.get(x_key))
        y_label = _format_axis_value(y_kind, row.get(y_key))
        value = float(row.get("value") or 0.0)
        if x_label and y_label:
            cells.append(HeatmapCell(x=x_label, y=y_label, value=value))
    return cells


def _normalize_range_days(range_days: int) -> int:
    return max(1, min(int(range_days or 14), 180))


def _reverse_aliases(aliases: Dict[str, List[str]]) -> Dict[str, str]:
    reverse: Dict[str, str] = {}
    for canonical, alias_list in aliases.items():
        for alias in alias_list:
            key = (alias or "").strip().lower()
            if key:
                reverse[key] = canonical
    return reverse


async def _resolve_identity_variants(
    sink: BaseMetricsSink,
    *,
    person_id: str,
) -> List[str]:
    aliases = load_identity_aliases()
    reverse = _reverse_aliases(aliases)

    identity = await resolve_person_identity(sink, person_id=person_id)
    if identity:
        normalized = (identity or "").strip().lower()
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


async def build_heatmap_response(
    *,
    db_url: str,
    type: str,
    metric: str,
    scope_type: str,
    scope_id: str,
    range_days: int,
    start_date: date | None = None,
    end_date: date | None = None,
    x: Optional[str] = None,
    y: Optional[str] = None,
    limit: int = 50,
) -> HeatmapResponse:
    definition = _metric_for(type, metric)
    if definition is None:
        raise HTTPException(status_code=404, detail="Unknown heatmap metric")

    if definition.type == "individual" and scope_type != "developer":
        raise HTTPException(
            status_code=400, detail="Individual heatmaps require developer scope"
        )

    if scope_type == "developer" and definition.type != "individual":
        raise HTTPException(
            status_code=400,
            detail="Developer scope is only supported for individual heatmaps",
        )

    if definition.type == "individual" and not scope_id:
        raise HTTPException(
            status_code=400, detail="Individual heatmaps require a person id"
        )

    if definition.x_axis == "person" and definition.y_axis == "person":
        raise HTTPException(
            status_code=400, detail="Person comparisons are not supported"
        )

    range_days = _normalize_range_days(range_days)
    try:
        filters = MetricFilter(
            time=TimeFilter(
                range_days=range_days,
                compare_days=range_days,
                start_date=start_date,
                end_date=end_date,
            ),
            scope=ScopeFilter(level=scope_type, ids=[scope_id] if scope_id else []),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid scope filter") from exc
    start_day, end_day, _, _ = time_window(filters)
    start_ts = datetime.combine(start_day, datetime.min.time())
    end_ts = datetime.combine(end_day, datetime.min.time())

    async with clickhouse_client(db_url) as sink:
        identities: List[str] = []
        if definition.type == "individual":
            identities = await _resolve_identity_variants(sink, person_id=scope_id)
            if not identities:
                raise HTTPException(status_code=404, detail="Individual not found")

        rows: List[Dict[str, Any]]
        evidence: Optional[List[Dict[str, Any]]] = None

        if definition.metric == "review_wait_density":
            scope_filter, scope_params = await scope_filter_for_metric(
                sink,
                metric_scope="repo",
                filters=filters,
                team_column="team_id",
                repo_column="repo_id",
            )
            rows = await fetch_review_wait_density(
                sink,
                start_ts=start_ts,
                end_ts=end_ts,
                scope_filter=scope_filter,
                scope_params=scope_params,
            )
            if x and y:
                try:
                    hour = int(x)
                    weekday = WEEKDAY_LABELS.index(y) + 1
                except Exception:
                    hour = -1
                    weekday = -1
                if 0 <= hour <= 23 and 1 <= weekday <= 7:
                    evidence = await fetch_review_wait_evidence(
                        sink,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        weekday=weekday,
                        hour=hour,
                        scope_filter=scope_filter,
                        scope_params=scope_params,
                        limit=min(max(limit, 1), 200),
                    )
        elif definition.metric == "repo_touchpoints":
            scope_filter, scope_params = await scope_filter_for_metric(
                sink,
                metric_scope="repo",
                filters=filters,
                team_column="team_id",
                repo_column="repo_id",
            )
            rows = await fetch_repo_touchpoints(
                sink,
                start_ts=start_ts,
                end_ts=end_ts,
                scope_filter=scope_filter,
                scope_params=scope_params,
                limit=20,
            )
        elif definition.metric == "hotspot_risk":
            scope_filter, scope_params = await scope_filter_for_metric(
                sink,
                metric_scope="repo",
                filters=filters,
                team_column="team_id",
                repo_column="repo_id",
            )
            rows = await fetch_hotspot_risk(
                sink,
                start_day=start_day,
                end_day=end_day,
                scope_filter=scope_filter,
                scope_params=scope_params,
                limit=20,
            )
            if x and y:
                try:
                    week_start = date.fromisoformat(x)
                    week_end = week_start + timedelta(days=7)
                except Exception:
                    week_start = None
                    week_end = None
                if week_start and week_end:
                    evidence = await fetch_hotspot_evidence(
                        sink,
                        week_start=week_start,
                        week_end=week_end,
                        file_key=y,
                        scope_filter=scope_filter,
                        scope_params=scope_params,
                        limit=min(max(limit, 1), 200),
                    )
        elif definition.metric == "active_hours":
            rows = await fetch_individual_active_hours(
                sink,
                start_ts=start_ts,
                end_ts=end_ts,
                identities=identities,
            )
            if x and y:
                try:
                    hour = int(x)
                    weekday = WEEKDAY_LABELS.index(y) + 1
                except Exception:
                    hour = -1
                    weekday = -1
                if 0 <= hour <= 23 and 1 <= weekday <= 7:
                    evidence = await fetch_individual_active_evidence(
                        sink,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        weekday=weekday,
                        hour=hour,
                        identities=identities,
                        limit=min(max(limit, 1), 200),
                    )
        else:
            rows = []

    x_axis = _axis_values(rows, definition.x_field, definition.x_axis)
    y_axis = _axis_values(rows, definition.y_field, definition.y_axis)

    axes = HeatmapAxes(x=x_axis, y=y_axis)
    cells = _cells_from_rows(
        rows,
        x_key=definition.x_field,
        y_key=definition.y_field,
        x_kind=definition.x_axis,
        y_kind=definition.y_axis,
    )

    legend = HeatmapLegend(unit=definition.unit, scale=definition.scale)
    return HeatmapResponse(axes=axes, cells=cells, legend=legend, evidence=evidence)
