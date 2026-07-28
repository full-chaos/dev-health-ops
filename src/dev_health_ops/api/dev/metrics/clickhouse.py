"""Bounded ClickHouse adapter for the canonical Ask Dev metric service."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timezone
from typing import Any

from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.api.queries.metrics import canonical_investment_theme_sql
from dev_health_ops.investment_taxonomy import THEMES

from ..contracts import DevScope, DirectScope, MetricID
from .definitions import MetricDefinition
from .service import (
    MetricSeriesPoint,
    MetricSourceRef,
    MetricSourceState,
    MetricSourceUnavailableError,
    RawMetricResult,
    RawMetricRow,
)

SourceStateResolver = Callable[[str, MetricDefinition], Awaitable[MetricSourceState]]

_THEME_ORDER = (
    "feature_delivery",
    "operational",
    "maintenance",
    "quality",
    "risk",
)
_QUERY_TIMEOUT_SECONDS = 15


async def _available_source(
    _org_id: str, _definition: MetricDefinition
) -> MetricSourceState:
    return MetricSourceState.AVAILABLE


class ClickHouseMetricSource:
    """Execute only allowlisted, parameterized V1 metric projections."""

    def __init__(
        self,
        client: Any,
        *,
        source_state_resolver: SourceStateResolver | None = None,
    ) -> None:
        self._client = client
        self._source_state_resolver = source_state_resolver or _available_source

    async def watermark(
        self, org_id: str, definition: MetricDefinition, scope: DevScope
    ) -> str:
        source_state = await self._source_state_resolver(org_id, definition)
        if source_state is not MetricSourceState.AVAILABLE:
            return hashlib.sha256(source_state.value.encode()).hexdigest()
        filters, params = self._watermark_filter(definition, scope)
        start = (
            scope.comparison_range.start
            if scope.comparison_range is not None
            else scope.time_range.start
        ).astimezone(timezone.utc)
        end = scope.time_range.end.astimezone(timezone.utc)
        query = f"""
            SELECT maxOrNull(day) AS latest_day,
                   maxOrNull(computed_at) AS watermark
            FROM {definition.source_table}
            WHERE org_id = %(org_id)s
              AND day >= %(watermark_start)s AND day < %(watermark_end)s
            {filters}
        """
        try:
            rows = await self._query(
                query,
                {
                    "org_id": org_id,
                    "watermark_start": start.date(),
                    "watermark_end": end.date(),
                    **params,
                },
            )
        except Exception as exc:
            raise MetricSourceUnavailableError(definition.source_table) from exc
        row = rows[0] if rows else {}
        raw = json.dumps(
            {
                "state": source_state.value,
                "latest_day": str(row.get("latest_day") or ""),
                "watermark": self._iso(row.get("watermark")),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(raw.encode()).hexdigest()

    async def query(
        self,
        org_id: str,
        definition: MetricDefinition,
        scope: DevScope,
        *,
        comparison: bool,
        include_series: bool,
        max_series_points: int,
    ) -> RawMetricResult:
        source_state = await self._source_state_resolver(org_id, definition)
        if source_state is not MetricSourceState.AVAILABLE:
            return self._empty(definition, scope, comparison, source_state)
        start, end = self._window(scope, comparison)
        try:
            if definition.metric_id is MetricID.INVESTMENT_ALLOCATION_PCT:
                result = await self._investment(
                    org_id, definition, scope, start, end, include_series
                )
            elif definition.metric_id is MetricID.COMPOUNDING_RISK_SCORE:
                result = await self._compounding_risk(
                    org_id, definition, scope, start, end, include_series
                )
            else:
                result = await self._scalar(
                    org_id,
                    definition,
                    scope,
                    start,
                    end,
                    include_series,
                    max_series_points,
                )
        except MetricSourceUnavailableError:
            raise
        except Exception as exc:
            raise MetricSourceUnavailableError(definition.source_table) from exc
        return result

    async def _scalar(
        self,
        org_id: str,
        definition: MetricDefinition,
        scope: DevScope,
        start: datetime,
        end: datetime,
        include_series: bool,
        max_series_points: int,
    ) -> RawMetricResult:
        inner, aggregate = self._scalar_expressions(definition.metric_id)
        group_by = self._scalar_group_by(definition.metric_id)
        cfr_columns = (
            ", sum(deployments_count) AS deployments, "
            "sum(failed_deployments_count) AS failed_deployments"
            if definition.metric_id is MetricID.CHANGE_FAILURE_RATE
            else ""
        )
        filters, scope_params = self._scope_filter(definition, scope)
        query = f"""
            SELECT {aggregate} AS value,
                   count() AS matched_rows,
                   countDistinct(day) AS covered_days,
                   maxOrNull(day) AS latest_day,
                   maxOrNull(latest_computed_at) AS watermark
                   {cfr_columns}
            FROM (
                {inner}
                WHERE org_id = %(org_id)s
                  AND day >= %(start)s AND day < %(end)s
                {filters}
                GROUP BY {group_by}
            )
        """
        params = {
            "org_id": org_id,
            "start": start.date(),
            "end": end.date(),
            **scope_params,
        }
        rows = await self._query(query, params)
        row = rows[0] if rows else {}
        matched_rows = int(row.get("matched_rows") or 0)
        value = row.get("value") if matched_rows > 0 else None
        if isinstance(value, float) and not math.isfinite(value):
            value = None
        metadata: tuple[tuple[str, str], ...] = ()
        if definition.metric_id is MetricID.CHANGE_FAILURE_RATE:
            deployments = float(row.get("deployments") or 0.0)
            failed = float(row.get("failed_deployments") or 0.0)
            value = (
                failed / deployments if matched_rows > 0 and deployments > 0 else None
            )
            metadata_items = [
                ("deployments", self._number(deployments)),
                ("failed_deployments", self._number(failed)),
            ]
            if matched_rows > 0 and deployments == 0:
                metadata_items.append(("empty_reason", "zero_denominator"))
            metadata = tuple(metadata_items)
        series = (
            await self._scalar_series(
                org_id,
                definition,
                inner,
                aggregate,
                group_by,
                filters,
                params,
                max_series_points,
            )
            if include_series and value is not None
            else ()
        )
        watermark = self._datetime(row.get("watermark"))
        result_rows = (
            (
                RawMetricRow(
                    dimensions=(),
                    value=float(value),
                    series=series,
                ),
            )
            if value is not None
            else ()
        )
        return RawMetricResult(
            rows=result_rows,
            watermark=watermark,
            latest_materialized_day=self._date(row.get("latest_day")),
            source_state=MetricSourceState.AVAILABLE,
            covered_days=int(row.get("covered_days") or 0),
            expected_days=self._expected_days(start, end),
            source_refs=self._source_refs(definition, scope, start, end, watermark),
            metadata=metadata,
        )

    async def _scalar_series(
        self,
        org_id: str,
        definition: MetricDefinition,
        inner: str,
        aggregate: str,
        group_by: str,
        filters: str,
        params: dict[str, Any],
        max_series_points: int,
    ) -> tuple[MetricSeriesPoint, ...]:
        if definition.metric_id is MetricID.CHANGE_FAILURE_RATE:
            daily_value = (
                "if(sum(deployments_count) > 0, "
                "sum(failed_deployments_count) / sum(deployments_count), NULL)"
            )
        else:
            daily_value = aggregate
        query = f"""
            SELECT day, {daily_value} AS value
            FROM (
                {inner}
                WHERE org_id = %(org_id)s
                  AND day >= %(start)s AND day < %(end)s
                {filters}
                GROUP BY {group_by}
            )
            GROUP BY day
            ORDER BY day
            LIMIT {max_series_points}
        """
        rows = await self._query(query, params)
        return tuple(
            MetricSeriesPoint(
                timestamp=datetime.combine(
                    self._date(row["day"]) or date.min,
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                ),
                value=float(row["value"]),
            )
            for row in rows
            if row.get("value") is not None
        )

    @staticmethod
    def _scalar_expressions(metric_id: MetricID) -> tuple[str, str]:
        if metric_id is MetricID.ITEMS_COMPLETED:
            return (
                """SELECT day, provider, work_scope_id, team_id,
                           argMax(items_completed, computed_at) AS items_completed,
                           max(computed_at) AS latest_computed_at
                    FROM work_item_metrics_daily""",
                "sum(items_completed)",
            )
        if metric_id is MetricID.CYCLE_TIME_P50_HOURS:
            return (
                """SELECT day, provider, work_scope_id, team_id,
                           argMax(cycle_time_p50_hours, computed_at) AS cycle_time_p50_hours,
                           max(computed_at) AS latest_computed_at
                    FROM work_item_metrics_daily""",
                "avg(cycle_time_p50_hours)",
            )
        if metric_id is MetricID.AVG_WIP:
            return (
                """SELECT day, provider, work_scope_id, team_id, status,
                           argMax(avg_wip, computed_at) AS avg_wip,
                           max(computed_at) AS latest_computed_at
                    FROM work_item_state_durations_daily""",
                "avg(avg_wip)",
            )
        if metric_id is MetricID.DEPLOYMENTS_COUNT:
            return (
                """SELECT day, repo_id,
                           argMax(deployments_count, computed_at) AS deployments_count,
                           max(computed_at) AS latest_computed_at
                    FROM deploy_metrics_daily""",
                "sum(deployments_count)",
            )
        if metric_id is MetricID.CHANGE_FAILURE_RATE:
            return (
                """SELECT day, repo_id,
                           argMax(deployments_count, computed_at) AS deployments_count,
                           argMax(failed_deployments_count, computed_at) AS failed_deployments_count,
                           max(computed_at) AS latest_computed_at
                    FROM deploy_metrics_daily""",
                "NULL",
            )
        if metric_id is MetricID.CYCLOMATIC_PER_KLOC:
            return (
                """SELECT day, repo_id,
                           argMax(cyclomatic_per_kloc, computed_at) AS cyclomatic_per_kloc,
                           max(computed_at) AS latest_computed_at
                    FROM repo_complexity_daily""",
                "avg(cyclomatic_per_kloc)",
            )
        raise ValueError("Unsupported scalar metric")

    @staticmethod
    def _scalar_group_by(metric_id: MetricID) -> str:
        if metric_id in {MetricID.ITEMS_COMPLETED, MetricID.CYCLE_TIME_P50_HOURS}:
            return "day, provider, work_scope_id, team_id"
        if metric_id is MetricID.AVG_WIP:
            return "day, provider, work_scope_id, team_id, status"
        if metric_id in {
            MetricID.DEPLOYMENTS_COUNT,
            MetricID.CHANGE_FAILURE_RATE,
            MetricID.CYCLOMATIC_PER_KLOC,
        }:
            return "day, repo_id"
        raise ValueError("Unsupported scalar metric")

    async def _investment(
        self,
        org_id: str,
        definition: MetricDefinition,
        scope: DevScope,
        start: datetime,
        end: datetime,
        include_series: bool,
    ) -> RawMetricResult:
        theme_expr = canonical_investment_theme_sql("investment_area")
        filters, scope_params = self._scope_filter(definition, scope)
        query = f"""
            SELECT canonical_theme AS theme,
                   sum(work_items_completed) AS allocation,
                   max(latest_computed_at) AS watermark,
                   max(day) AS latest_day,
                   countDistinct(day) AS covered_days
            FROM (
                SELECT day, repo_id, team_id,
                       {theme_expr} AS canonical_theme,
                       project_stream,
                       argMax(work_items_completed, computed_at) AS work_items_completed,
                       max(computed_at) AS latest_computed_at
                FROM investment_metrics_daily
                WHERE org_id = %(org_id)s
                  AND day >= %(start)s AND day < %(end)s
                {filters}
                GROUP BY day, repo_id, team_id, canonical_theme, project_stream
            )
            GROUP BY canonical_theme
            ORDER BY canonical_theme
            LIMIT 6
        """
        params = {
            "org_id": org_id,
            "start": start.date(),
            "end": end.date(),
            **scope_params,
        }
        rows = await self._query(query, params)
        allocations = {
            str(row.get("theme")): float(row.get("allocation") or 0.0)
            for row in rows
            if str(row.get("theme")) in THEMES
        }
        denominator = sum(allocations.values())
        total_allocation = sum(float(row.get("allocation") or 0.0) for row in rows)
        classification_coverage = (
            denominator / total_allocation if total_allocation > 0 else 0.0
        )
        watermark = max(
            (self._datetime(row.get("watermark")) for row in rows),
            default=None,
            key=lambda item: item or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest_day = max(
            (self._date(row.get("latest_day")) for row in rows),
            default=None,
            key=lambda item: item or date.min,
        )
        covered_days = max(
            (int(row.get("covered_days") or 0) for row in rows), default=0
        )
        result_rows = (
            tuple(
                RawMetricRow(
                    dimensions=(("investment_theme", theme),),
                    value=allocations.get(theme, 0.0) / denominator * 100.0,
                )
                for theme in _THEME_ORDER
            )
            if denominator > 0
            else ()
        )
        metadata_items = [
            ("denominator", self._number(denominator)),
            ("total_completed_work", self._number(total_allocation)),
            ("classification_coverage", self._number(classification_coverage)),
            ("denominator_field", "work_items_completed"),
            ("unclassified_treatment", "excluded_from_denominator"),
        ]
        if rows and denominator == 0:
            metadata_items.append(("empty_reason", "zero_denominator"))
        metadata = tuple(metadata_items)
        return RawMetricResult(
            rows=result_rows,
            watermark=watermark,
            latest_materialized_day=latest_day,
            source_state=MetricSourceState.AVAILABLE,
            covered_days=covered_days,
            expected_days=self._expected_days(start, end),
            source_refs=self._source_refs(definition, scope, start, end, watermark),
            metadata=metadata,
            coverage=(
                min(1.0, covered_days / self._expected_days(start, end))
                * classification_coverage
            ),
        )

    async def _compounding_risk(
        self,
        org_id: str,
        definition: MetricDefinition,
        scope: DevScope,
        start: datetime,
        end: datetime,
        include_series: bool,
    ) -> RawMetricResult:
        repo_ids = self._repository_ids(scope)
        risk_scope = "team" if scope.team_ids else "repo"
        id_filter = ""
        params: dict[str, Any] = {
            "org_id": org_id,
            "start": start.date(),
            "end": end.date(),
            "risk_scope": risk_scope,
        }
        if scope.team_ids:
            id_filter = "AND scope_id IN %(scope_ids)s"
            params["scope_ids"] = list(scope.team_ids)
        elif repo_ids:
            id_filter = "AND scope_id IN %(scope_ids)s"
            params["scope_ids"] = repo_ids
        query = f"""
            WITH (
                SELECT max(day) FROM compounding_risk_daily
                WHERE org_id = %(org_id)s AND scope = %(risk_scope)s
                  AND day >= %(start)s AND day < %(end)s {id_filter}
            ) AS selected_day
            SELECT scope_id,
                   tupleElement(latest, 1) AS score,
                   tupleElement(latest, 2) AS churn_norm,
                   tupleElement(latest, 3) AS complexity_norm,
                   tupleElement(latest, 4) AS ownership_norm,
                   tupleElement(latest, 5) AS review_norm,
                   tupleElement(latest, 6) AS w_churn,
                   tupleElement(latest, 7) AS w_complexity,
                   tupleElement(latest, 8) AS w_ownership,
                   tupleElement(latest, 9) AS w_review,
                   tupleElement(latest, 10) AS threshold_elevated,
                   tupleElement(latest, 11) AS threshold_high,
                   tupleElement(latest, 12) AS watermark,
                   selected_day AS latest_day
            FROM (
                SELECT scope_id,
                       argMax(tuple(compounding_risk, churn_norm, complexity_norm,
                                    ownership_norm, review_norm, w_churn, w_complexity,
                                    w_ownership, w_review, threshold_elevated,
                                    threshold_high, computed_at), computed_at) AS latest
                FROM compounding_risk_daily
                WHERE org_id = %(org_id)s AND scope = %(risk_scope)s
                  AND day = selected_day {id_filter}
                GROUP BY scope_id
            )
            ORDER BY scope_id
            LIMIT 20
        """
        rows = await self._query(query, params)
        scores = [float(row["score"]) for row in rows if row.get("score") is not None]
        latest_day = self._date(rows[0].get("latest_day")) if rows else None
        watermark = max(
            (self._datetime(row.get("watermark")) for row in rows),
            default=None,
            key=lambda item: item or datetime.min.replace(tzinfo=timezone.utc),
        )
        persisted = [
            {
                key: float(row[key]) if row.get(key) is not None else None
                for key in (
                    "churn_norm",
                    "complexity_norm",
                    "ownership_norm",
                    "review_norm",
                    "w_churn",
                    "w_complexity",
                    "w_ownership",
                    "w_review",
                    "threshold_elevated",
                    "threshold_high",
                )
            }
            | {"scope_id": str(row["scope_id"])}
            for row in rows
        ]
        version_payload = sorted(
            {
                json.dumps(
                    {
                        key: item[key]
                        for key in (
                            "w_churn",
                            "w_complexity",
                            "w_ownership",
                            "w_review",
                            "threshold_elevated",
                            "threshold_high",
                        )
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                for item in persisted
            }
        )
        version = hashlib.sha256(
            json.dumps(version_payload, separators=(",", ":")).encode()
        ).hexdigest()[:32]
        result_rows = (
            (RawMetricRow(dimensions=(), value=sum(scores) / len(scores)),)
            if scores
            else ()
        )
        return RawMetricResult(
            rows=result_rows,
            watermark=watermark,
            latest_materialized_day=latest_day,
            source_state=MetricSourceState.AVAILABLE,
            covered_days=1 if latest_day else 0,
            expected_days=1,
            source_refs=self._source_refs(definition, scope, start, end, watermark),
            metadata=(
                ("component_weight_version", f"sha256:{version}"),
                (
                    "persisted_components_weights_thresholds",
                    json.dumps(persisted, sort_keys=True, separators=(",", ":")),
                ),
            ),
        )

    def _scope_filter(
        self, definition: MetricDefinition, scope: DevScope
    ) -> tuple[str, dict[str, Any]]:
        clauses: list[str] = []
        params: dict[str, Any] = {}
        if definition.source_table in {
            "work_item_metrics_daily",
            "work_item_state_durations_daily",
        }:
            if scope.direct_scope in {DirectScope.PROJECT, DirectScope.WORK_UNIT}:
                clauses.append("AND work_scope_id = %(work_scope_id)s")
                params["work_scope_id"] = scope.entity_refs[0].entity_id
            if scope.team_ids:
                clauses.append("AND team_id IN %(team_ids)s")
                params["team_ids"] = list(scope.team_ids)
        elif definition.source_table != "compounding_risk_daily":
            repo_ids = self._repository_ids(scope)
            if repo_ids:
                clauses.append("AND toString(repo_id) IN %(repo_ids)s")
                params["repo_ids"] = repo_ids
            if scope.team_ids and definition.source_table == "investment_metrics_daily":
                clauses.append("AND team_id IN %(team_ids)s")
                params["team_ids"] = list(scope.team_ids)
        return "\n".join(clauses), params

    def _watermark_filter(
        self, definition: MetricDefinition, scope: DevScope
    ) -> tuple[str, dict[str, Any]]:
        if definition.source_table != "compounding_risk_daily":
            return self._scope_filter(definition, scope)
        repo_ids = self._repository_ids(scope)
        risk_scope = "team" if scope.team_ids else "repo"
        clauses = ["AND scope = %(risk_scope)s"]
        params: dict[str, Any] = {"risk_scope": risk_scope}
        scope_ids = list(scope.team_ids) if scope.team_ids else repo_ids
        if scope_ids:
            clauses.append("AND scope_id IN %(watermark_scope_ids)s")
            params["watermark_scope_ids"] = scope_ids
        return "\n".join(clauses), params

    async def _query(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Keep every metric read within the TRD's hard query deadline."""
        async with asyncio.timeout(_QUERY_TIMEOUT_SECONDS):
            return await query_dicts(self._client, query, params)

    @staticmethod
    def _repository_ids(scope: DevScope) -> list[str]:
        return sorted(
            set(scope.repositories)
            | {
                ref.repository_id
                for ref in scope.entity_refs
                if ref.repository_id is not None
            }
        )

    @staticmethod
    def _window(scope: DevScope, comparison: bool) -> tuple[datetime, datetime]:
        window = scope.comparison_range if comparison else scope.time_range
        if window is None:
            raise ValueError("Resolved comparison window is required")
        return window.start.astimezone(timezone.utc), window.end.astimezone(
            timezone.utc
        )

    def _empty(
        self,
        definition: MetricDefinition,
        scope: DevScope,
        comparison: bool,
        source_state: MetricSourceState,
    ) -> RawMetricResult:
        start, end = self._window(scope, comparison)
        return RawMetricResult(
            rows=(),
            watermark=None,
            latest_materialized_day=None,
            source_state=source_state,
            covered_days=0,
            expected_days=self._expected_days(start, end),
            source_refs=(),
        )

    @staticmethod
    def _source_refs(
        definition: MetricDefinition,
        scope: DevScope,
        start: datetime,
        end: datetime,
        watermark: datetime | None,
    ) -> tuple[MetricSourceRef, ...]:
        digest = hashlib.sha256(
            json.dumps(
                {
                    "source": definition.source_table,
                    "scope": scope.model_dump(mode="json"),
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "watermark": ClickHouseMetricSource._iso(watermark),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()[:32]
        return (
            MetricSourceRef(
                ref_id=f"metric-source:{digest}",
                source_table=definition.source_table,
                source_version=definition.source_version,
                watermark=watermark,
                query_version=definition.query_version,
            ),
        )

    @staticmethod
    def _expected_days(start: datetime, end: datetime) -> int:
        return max(1, (end.date() - start.date()).days)

    @staticmethod
    def _date(value: Any) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value:
            return date.fromisoformat(value[:10])
        return None

    @staticmethod
    def _datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str) and value:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    @staticmethod
    def _iso(value: Any) -> str:
        parsed = ClickHouseMetricSource._datetime(value)
        return parsed.isoformat() if parsed else ""

    @staticmethod
    def _number(value: float) -> str:
        return format(value, ".15g")
