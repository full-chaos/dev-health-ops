from __future__ import annotations

from datetime import date, datetime, timedelta
import math
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dev_health_ops.metrics.sinks.base import BaseMetricsSink
from ..models.schemas import (
    CollaborationItem,
    CollaborationSection,
    Coverage,
    DriverStatement,
    FlowStageItem,
    Freshness,
    MetricBreakdownItem,
    MetricDefinition,
    MetricTimeseriesPoint,
    PersonDelta,
    PersonDrilldownResponse,
    PersonIdentity,
    PersonMetricBreakdowns,
    PersonMetricResponse,
    PersonSearchResult,
    PersonSummaryPerson,
    PersonSummaryResponse,
    PersonSummarySections,
    PullRequestRow,
    IssueRow,
    SparkPoint,
    SummarySentence,
    WorkMixItem,
)
from ..queries.client import clickhouse_client
from ..queries.freshness import fetch_coverage, fetch_last_ingested_at
from ..queries.people import (
    fetch_identity_coverage,
    fetch_person_breakdown,
    fetch_person_collaboration,
    fetch_person_flow_breakdown,
    fetch_person_issues,
    fetch_person_metric_series,
    fetch_person_metric_value,
    fetch_person_pull_requests,
    fetch_person_work_mix,
    resolve_person_identity,
    search_people as query_people,
)
from .people_identity import (
    display_name_for_identity,
    identities_for_person,
    identity_variants,
    load_identity_aliases,
    person_id_for_identity,
)

_MAX_SEARCH_LIMIT = 50
_MAX_DRILLDOWN_LIMIT = 200


_PERSON_METRICS: List[Dict[str, Any]] = [
    {
        "metric": "cycle_time",
        "label": "Cycle Time",
        "unit": "days",
        "table": "work_item_user_metrics_daily",
        "column": "cycle_time_p50_hours",
        "aggregator": "avg",
        "identity_column": "user_identity",
        "extra_where": "AND cycle_time_p50_hours IS NOT NULL",
        "transform": lambda v: v / 24.0,
        "definition": {
            "description": "Time from work start to completion for your items.",
            "interpretation": "Lower values indicate faster delivery.",
        },
        "breakdowns": {
            "by_work_type": {
                "table": "work_item_cycle_times",
                "column": "cycle_time_hours",
                "aggregator": "avg",
                "identity_column": "assignee",
                "group_expr": "if(type = '' OR type IS NULL, 'unknown', type)",
                "extra_where": "AND cycle_time_hours IS NOT NULL",
                "transform": lambda v: v / 24.0,
            },
            "by_stage": {
                "table": "work_item_cycle_times",
                "column": "cycle_time_hours",
                "aggregator": "avg",
                "identity_column": "assignee",
                "group_expr": "if(status = '' OR status IS NULL, 'unknown', status)",
                "extra_where": "AND cycle_time_hours IS NOT NULL",
                "transform": lambda v: v / 24.0,
            },
        },
    },
    {
        "metric": "review_latency",
        "label": "Review Latency",
        "unit": "hours",
        "table": "user_metrics_daily",
        "column": "pr_first_review_p50_hours",
        "aggregator": "avg",
        "identity_column": "identity_id",
        "extra_where": "AND pr_first_review_p50_hours IS NOT NULL",
        "transform": lambda v: v,
        "definition": {
            "description": "Time from pull request creation to first review.",
            "interpretation": "Lower values mean reviews start sooner.",
        },
        "breakdowns": {
            "by_repo": {
                "table": "user_metrics_daily",
                "column": "pr_first_review_p50_hours",
                "aggregator": "avg",
                "identity_column": "identity_id",
                "group_expr": "repos.repo",
                "join_clause": "INNER JOIN repos ON repos.id = user_metrics_daily.repo_id",
                "extra_where": "AND pr_first_review_p50_hours IS NOT NULL",
                "transform": lambda v: v,
            }
        },
    },
    {
        "metric": "throughput",
        "label": "Throughput",
        "unit": "items",
        "table": "work_item_user_metrics_daily",
        "column": "items_completed",
        "aggregator": "sum",
        "identity_column": "user_identity",
        "extra_where": "",
        "transform": lambda v: v,
        "definition": {
            "description": "Count of completed work items in the period.",
            "interpretation": "Higher counts indicate more delivery throughput.",
        },
        "breakdowns": {
            "by_work_type": {
                "table": "work_item_cycle_times",
                "column": "if(completed_at IS NULL, 0, 1)",
                "aggregator": "sum",
                "identity_column": "assignee",
                "group_expr": "if(type = '' OR type IS NULL, 'unknown', type)",
                "extra_where": "",
                "transform": lambda v: v,
            }
        },
    },
    {
        "metric": "churn",
        "label": "Code Churn",
        "unit": "loc",
        "table": "user_metrics_daily",
        "column": "loc_touched",
        "aggregator": "sum",
        "identity_column": "identity_id",
        "extra_where": "",
        "transform": lambda v: v,
        "definition": {
            "description": "Lines of code touched in the period.",
            "interpretation": "Higher values can reflect refactors or rework.",
        },
        "breakdowns": {
            "by_repo": {
                "table": "user_metrics_daily",
                "column": "loc_touched",
                "aggregator": "sum",
                "identity_column": "identity_id",
                "group_expr": "repos.repo",
                "join_clause": "INNER JOIN repos ON repos.id = user_metrics_daily.repo_id",
                "extra_where": "",
                "transform": lambda v: v,
            }
        },
    },
    {
        "metric": "wip_overlap",
        "label": "WIP Overlap",
        "unit": "items",
        "table": "work_item_user_metrics_daily",
        "column": "wip_count_end_of_day",
        "aggregator": "avg",
        "identity_column": "user_identity",
        "extra_where": "",
        "transform": lambda v: v,
        "definition": {
            "description": "Average count of items active in parallel.",
            "interpretation": "Lower values indicate less multitasking.",
        },
        "breakdowns": {},
    },
    {
        "metric": "blocked_work",
        "label": "Blocked Work",
        "unit": "items",
        "table": "work_item_cycle_times",
        "column": "if(status = 'blocked', 1, 0)",
        "aggregator": "sum",
        "identity_column": "assignee",
        "extra_where": "",
        "transform": lambda v: v,
        "definition": {
            "description": "Count of assigned items marked blocked in the period.",
            "interpretation": "Lower values indicate fewer blockers.",
        },
        "breakdowns": {
            "by_work_type": {
                "table": "work_item_cycle_times",
                "column": "if(status = 'blocked', 1, 0)",
                "aggregator": "sum",
                "identity_column": "assignee",
                "group_expr": "if(type = '' OR type IS NULL, 'unknown', type)",
                "extra_where": "",
                "transform": lambda v: v,
            }
        },
    },
]


def _metric_config(metric: str) -> Dict[str, Any]:
    return next((cfg for cfg in _PERSON_METRICS if cfg["metric"] == metric), {})


def _time_window(range_days: int, compare_days: int) -> Tuple[date, date, date, date]:
    end_day = date.today() + timedelta(days=1)
    range_days = max(1, range_days)
    compare_days = max(1, compare_days)
    start_day = end_day - timedelta(days=range_days)
    compare_end = start_day
    compare_start = compare_end - timedelta(days=compare_days)
    return start_day, end_day, compare_start, compare_end


def _delta_pct(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0
    return (current - previous) / previous * 100.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _safe_optional_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_transform(transform, value: float) -> float:
    return _safe_float(transform(value))


def _spark_points(rows: List[Dict[str, Any]], transform) -> List[SparkPoint]:
    points: List[SparkPoint] = []
    for row in rows:
        value = _safe_float(row.get("value"))
        points.append(
            SparkPoint(ts=row["day"], value=_safe_transform(transform, value))
        )
    return points


def _bounded_limit(limit: int, max_limit: int) -> int:
    limit = limit or 0
    if limit <= 0:
        return min(50, max_limit)
    return min(max(limit, 1), max_limit)


def _normalize_alias(value: str) -> str:
    return (value or "").strip().lower()


def _reverse_aliases(aliases: Dict[str, List[str]]) -> Dict[str, str]:
    reverse: Dict[str, str] = {}
    for canonical, alias_list in aliases.items():
        for alias in alias_list:
            key = _normalize_alias(alias)
            if key:
                reverse[key] = canonical
    return reverse


async def _resolve_identity_context(
    sink: BaseMetricsSink,
    *,
    person_id: str,
    aliases: Dict[str, List[str]],
) -> Tuple[str, List[str]]:
    identity = await resolve_person_identity(sink, person_id=person_id)
    reverse = _reverse_aliases(aliases)

    if identity:
        canonical = reverse.get(_normalize_alias(identity), identity)
        alias_list = list(aliases.get(canonical, []))
        if identity != canonical and identity not in alias_list:
            alias_list.append(identity)
        return canonical, alias_list

    for canonical, alias_list in aliases.items():
        if person_id_for_identity(canonical) == person_id:
            return canonical, list(alias_list)
        for alias in alias_list:
            if person_id_for_identity(alias) == person_id:
                return canonical, list(alias_list)

    return "", []


def _identity_inputs(identity: str, aliases: Iterable[str]) -> List[str]:
    variants = identity_variants(identity, aliases)
    return list(dict.fromkeys([v for v in variants if v]))


def _person_model(identity: str, aliases: Iterable[str]) -> PersonSummaryPerson:
    display_name = display_name_for_identity(identity)
    identity_items = identities_for_person(identity, aliases)
    return PersonSummaryPerson(
        person_id=person_id_for_identity(identity),
        display_name=display_name,
        identities=[PersonIdentity(**item) for item in identity_items],
    )


def _metric_link(
    person_id: str, metric: str, range_days: int, compare_days: int
) -> str:
    return (
        f"/api/v1/people/{person_id}/metric"
        f"?metric={metric}&range_days={range_days}&compare_days={compare_days}"
    )


def _drilldown_link(person_id: str, metric: str) -> str:
    if metric in {"review_latency", "churn"}:
        return f"/api/v1/people/{person_id}/drilldown/prs?metric={metric}"
    return f"/api/v1/people/{person_id}/drilldown/issues?metric={metric}"


def _narrative_for_deltas(
    deltas: List[PersonDelta],
    *,
    person_id: str,
    range_days: int,
    compare_days: int,
) -> List[SummarySentence]:
    narrative: List[SummarySentence] = []
    ranked = sorted(deltas, key=lambda d: abs(d.delta_pct), reverse=True)

    for idx, delta in enumerate(ranked[:2], start=1):
        direction = "increased" if delta.delta_pct > 0 else "decreased"
        if delta.delta_pct == 0:
            direction = "held steady"

        if delta.metric == "review_latency":
            text = f"Review latency {direction} over the last {range_days} days."
        elif delta.metric == "cycle_time":
            text = f"Cycle time {direction} over the last {range_days} days."
        elif delta.metric == "throughput":
            text = f"Throughput {direction} compared to the prior period."
        elif delta.metric == "churn":
            text = f"Code churn {direction} in this period."
        elif delta.metric == "wip_overlap":
            text = f"WIP overlap {direction} in this period."
        elif delta.metric == "blocked_work":
            text = f"Blocked work {direction} in this period."
        else:
            text = f"{delta.label} {direction} in this period."

        narrative.append(
            SummarySentence(
                id=f"n{idx}",
                text=text,
                evidence_link=_metric_link(
                    person_id, delta.metric, range_days, compare_days
                ),
            )
        )

    return narrative


def _driver_from_breakdowns(
    *,
    metric: str,
    breakdowns: PersonMetricBreakdowns,
    person_id: str,
) -> List[DriverStatement]:
    for items in [
        breakdowns.by_repo,
        breakdowns.by_work_type,
        breakdowns.by_stage,
    ]:
        if not items:
            continue
        total = sum(item.value for item in items) or 0.0
        head = items[0]
        if total > 0:
            pct = head.value / total * 100.0
            text = f"{head.label} accounts for {pct:.0f}% of this period."
        else:
            text = f"{head.label} contributes the largest share this period."
        return [DriverStatement(text=text, link=_drilldown_link(person_id, metric))]
    return []


async def search_people_response(
    *,
    db_url: str,
    query: str,
    limit: int,
) -> List[PersonSearchResult]:
    trimmed = (query or "").strip()
    if not trimmed:
        return []

    aliases = load_identity_aliases()
    reverse_aliases = _reverse_aliases(aliases)
    limit = _bounded_limit(limit, _MAX_SEARCH_LIMIT)

    async with clickhouse_client(db_url) as sink:
        rows = await query_people(
            sink,
            query=f"%{trimmed.lower()}%",
            limit=limit,
        )

    results: List[PersonSearchResult] = []
    today = date.today()
    for row in rows:
        identity = str(row.get("identity_id") or "").strip()
        if not identity:
            continue
        canonical = reverse_aliases.get(_normalize_alias(identity), identity)
        alias_list = list(aliases.get(canonical, []))
        if identity != canonical and identity not in alias_list:
            alias_list.append(identity)
        display_name = display_name_for_identity(canonical)
        identity_items = identities_for_person(canonical, alias_list)
        last_seen = row.get("last_seen")
        active = True
        if last_seen:
            seen_day = (
                last_seen.date() if isinstance(last_seen, datetime) else last_seen
            )
            if isinstance(seen_day, date):
                active = (today - seen_day).days <= 90
        results.append(
            PersonSearchResult(
                person_id=person_id_for_identity(canonical),
                display_name=display_name,
                identities=[PersonIdentity(**item) for item in identity_items],
                active=active,
            )
        )

    return results


async def build_person_summary_response(
    *,
    db_url: str,
    person_id: str,
    range_days: int,
    compare_days: int,
) -> PersonSummaryResponse:
    start_day, end_day, compare_start, compare_end = _time_window(
        range_days, compare_days
    )
    aliases = load_identity_aliases()

    async with clickhouse_client(db_url) as sink:
        identity, alias_list = await _resolve_identity_context(
            sink, person_id=person_id, aliases=aliases
        )
        if not identity:
            raise ValueError("person not found")

        identity_inputs = _identity_inputs(identity, alias_list)
        person = _person_model(identity, alias_list)
        last_ingested = await fetch_last_ingested_at(sink)
        coverage = await fetch_coverage(sink, start_day=start_day, end_day=end_day)
        sources = {
            "github": "ok" if last_ingested else "down",
            "gitlab": "ok" if last_ingested else "down",
            "jira": "ok" if last_ingested else "down",
            "ci": "ok" if last_ingested else "down",
        }

        coverage_sources = await fetch_identity_coverage(
            sink, identities=identity_inputs
        )
        identity_coverage_pct = _safe_float(_safe_float(coverage_sources) / 2.0 * 100.0)

        deltas: List[PersonDelta] = []
        for metric in _PERSON_METRICS:
            current_value = await fetch_person_metric_value(
                sink,
                table=metric["table"],
                column=metric["column"],
                aggregator=metric["aggregator"],
                identity_column=metric["identity_column"],
                identities=identity_inputs,
                start_day=start_day,
                end_day=end_day,
                extra_where=metric["extra_where"],
            )
            previous_value = await fetch_person_metric_value(
                sink,
                table=metric["table"],
                column=metric["column"],
                aggregator=metric["aggregator"],
                identity_column=metric["identity_column"],
                identities=identity_inputs,
                start_day=compare_start,
                end_day=compare_end,
                extra_where=metric["extra_where"],
            )
            series = await fetch_person_metric_series(
                sink,
                table=metric["table"],
                column=metric["column"],
                aggregator=metric["aggregator"],
                identity_column=metric["identity_column"],
                identities=identity_inputs,
                start_day=start_day,
                end_day=end_day,
                extra_where=metric["extra_where"],
            )

            transform = metric["transform"]
            current_value = _safe_float(current_value)
            previous_value = _safe_float(previous_value)
            delta_pct = _safe_float(_delta_pct(current_value, previous_value))
            deltas.append(
                PersonDelta(
                    metric=metric["metric"],
                    label=metric["label"],
                    value=_safe_transform(transform, current_value),
                    unit=metric["unit"],
                    delta_pct=delta_pct,
                    spark=_spark_points(series, transform),
                )
            )

        work_mix_rows = await fetch_person_work_mix(
            sink, identities=identity_inputs, start_day=start_day, end_day=end_day
        )
        work_mix = [
            WorkMixItem(
                key=str(row.get("key") or ""),
                name=str(row.get("name") or ""),
                value=_safe_float(row.get("value")),
            )
            for row in work_mix_rows
        ]

        flow_rows = await fetch_person_flow_breakdown(
            sink, identities=identity_inputs, start_day=start_day, end_day=end_day
        )
        flow_breakdown = [
            FlowStageItem(
                stage=str(row.get("stage") or ""),
                value=_safe_float(row.get("value")),
                unit=str(row.get("unit") or "hours"),
            )
            for row in flow_rows
        ]

        collab_rows = await fetch_person_collaboration(
            sink, identities=identity_inputs, start_day=start_day, end_day=end_day
        )
        review_load: List[CollaborationItem] = []
        handoff_points: List[CollaborationItem] = []
        for row in collab_rows:
            item = CollaborationItem(
                label=str(row.get("label") or ""),
                value=_safe_float(row.get("value")),
            )
            if row.get("section") == "handoff_points":
                handoff_points.append(item)
            else:
                review_load.append(item)

    narrative = _narrative_for_deltas(
        deltas,
        person_id=person.person_id,
        range_days=range_days,
        compare_days=compare_days,
    )

    return PersonSummaryResponse(
        person=person,
        freshness=Freshness(
            last_ingested_at=last_ingested,
            sources=sources,
            coverage=Coverage(**coverage),
        ),
        identity_coverage_pct=identity_coverage_pct,
        deltas=deltas,
        narrative=narrative,
        sections=PersonSummarySections(
            work_mix=work_mix,
            flow_breakdown=flow_breakdown,
            collaboration=CollaborationSection(
                review_load=review_load,
                handoff_points=handoff_points,
            ),
        ),
    )


async def build_person_metric_response(
    *,
    db_url: str,
    person_id: str,
    metric: str,
    range_days: int,
    compare_days: int,
) -> PersonMetricResponse:
    config = _metric_config(metric)
    if not config:
        raise ValueError("metric not supported")

    start_day, end_day, _, _ = _time_window(range_days, compare_days)
    aliases = load_identity_aliases()

    async with clickhouse_client(db_url) as sink:
        identity, alias_list = await _resolve_identity_context(
            sink, person_id=person_id, aliases=aliases
        )
        if not identity:
            raise ValueError("person not found")

        identity_inputs = _identity_inputs(identity, alias_list)

        series_rows = await fetch_person_metric_series(
            sink,
            table=config["table"],
            column=config["column"],
            aggregator=config["aggregator"],
            identity_column=config["identity_column"],
            identities=identity_inputs,
            start_day=start_day,
            end_day=end_day,
            extra_where=config.get("extra_where", ""),
        )
        transform = config["transform"]
        timeseries = [
            MetricTimeseriesPoint(
                day=row["day"],
                value=_safe_transform(transform, _safe_float(row.get("value"))),
            )
            for row in series_rows
        ]

        breakdowns = {"by_repo": [], "by_work_type": [], "by_stage": []}
        for key in breakdowns.keys():
            detail = config.get("breakdowns", {}).get(key)
            if not detail:
                continue
            rows = await fetch_person_breakdown(
                sink,
                table=detail["table"],
                column=detail["column"],
                aggregator=detail["aggregator"],
                identity_column=detail["identity_column"],
                identities=identity_inputs,
                group_expr=detail["group_expr"],
                join_clause=detail.get("join_clause", ""),
                start_day=start_day,
                end_day=end_day,
                extra_where=detail.get("extra_where", ""),
            )
            breakdowns[key] = [
                MetricBreakdownItem(
                    label=str(row.get("label") or ""),
                    value=_safe_transform(
                        detail["transform"], _safe_float(row.get("value"))
                    ),
                )
                for row in rows
            ]

    breakdown_models = PersonMetricBreakdowns(
        by_repo=breakdowns["by_repo"],
        by_work_type=breakdowns["by_work_type"],
        by_stage=breakdowns["by_stage"],
    )

    drivers = _driver_from_breakdowns(
        metric=metric, breakdowns=breakdown_models, person_id=person_id
    )

    return PersonMetricResponse(
        metric=metric,
        label=config["label"],
        definition=MetricDefinition(**config["definition"]),
        timeseries=timeseries,
        breakdowns=breakdown_models,
        drivers=drivers,
    )


async def build_person_drilldown_prs_response(
    *,
    db_url: str,
    person_id: str,
    range_days: int,
    limit: int,
    cursor: Optional[datetime] = None,
) -> PersonDrilldownResponse:
    start_day, end_day, _, _ = _time_window(range_days, range_days)
    aliases = load_identity_aliases()
    limit = _bounded_limit(limit, _MAX_DRILLDOWN_LIMIT)

    async with clickhouse_client(db_url) as sink:
        identity, alias_list = await _resolve_identity_context(
            sink, person_id=person_id, aliases=aliases
        )
        if not identity:
            raise ValueError("person not found")

        identity_inputs = _identity_inputs(identity, alias_list)
        rows = await fetch_person_pull_requests(
            sink,
            identities=identity_inputs,
            start_day=start_day,
            end_day=end_day,
            limit=limit,
            cursor=cursor,
        )

    items: List[PullRequestRow] = []
    for row in rows:
        items.append(
            PullRequestRow(
                repo_id=str(row.get("repo_id") or ""),
                number=int(row.get("number") or 0),
                title=row.get("title"),
                author=row.get("author") or row.get("author_name"),
                created_at=row.get("created_at"),
                merged_at=row.get("merged_at"),
                first_review_at=row.get("first_review_at"),
                review_latency_hours=_safe_optional_float(
                    row.get("review_latency_hours")
                ),
                link=None,
            )
        )

    next_cursor = rows[-1].get("created_at") if rows else None
    return PersonDrilldownResponse(items=items, next_cursor=next_cursor)


async def build_person_drilldown_issues_response(
    *,
    db_url: str,
    person_id: str,
    range_days: int,
    limit: int,
    cursor: Optional[datetime] = None,
) -> PersonDrilldownResponse:
    start_day, end_day, _, _ = _time_window(range_days, range_days)
    aliases = load_identity_aliases()
    limit = _bounded_limit(limit, _MAX_DRILLDOWN_LIMIT)

    async with clickhouse_client(db_url) as sink:
        identity, alias_list = await _resolve_identity_context(
            sink, person_id=person_id, aliases=aliases
        )
        if not identity:
            raise ValueError("person not found")

        identity_inputs = _identity_inputs(identity, alias_list)
        rows = await fetch_person_issues(
            sink,
            identities=identity_inputs,
            start_day=start_day,
            end_day=end_day,
            limit=limit,
            cursor=cursor,
        )

    items: List[IssueRow] = []
    for row in rows:
        items.append(
            IssueRow(
                work_item_id=str(row.get("work_item_id") or ""),
                provider=str(row.get("provider") or ""),
                status=str(row.get("status") or ""),
                team_id=row.get("team_id"),
                cycle_time_hours=_safe_optional_float(row.get("cycle_time_hours")),
                lead_time_hours=_safe_optional_float(row.get("lead_time_hours")),
                started_at=row.get("started_at"),
                completed_at=row.get("completed_at"),
                link=None,
            )
        )

    next_cursor = rows[-1].get("completed_at") if rows else None
    return PersonDrilldownResponse(items=items, next_cursor=next_cursor)
