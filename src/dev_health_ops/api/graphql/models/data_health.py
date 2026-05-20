"""Strawberry GraphQL types for the data-health surface."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

import strawberry
from strawberry.types import Info

logger = logging.getLogger(__name__)


@strawberry.type
class ConnectorFailure:
    occurred_at: datetime
    message: str
    stage: str | None = None


@strawberry.type
class ConnectorStatus:
    provider: str
    scope: str
    last_sync_at: datetime | None = None
    rows_ingested: int = 0
    last_failure: ConnectorFailure | None = None


@strawberry.type
class UnmappedIdentity:
    provider: str
    email: str | None = None
    display_name: str | None = None
    observed_count: int | None = None


@strawberry.type
class AliasSuggestion:
    unmapped_identity: UnmappedIdentity
    suggested_canonical_id: str
    confidence: float


@strawberry.type
class IdentityMappingHealth:
    unmapped_count: int
    unmapped_identities: list[UnmappedIdentity]
    suggested_aliases: list[AliasSuggestion]


@strawberry.type
class MissingMapping:
    repo_name: str
    reason: str


@strawberry.type
class CoverageStat:
    total_repos: int
    covered_repos: int
    coverage_pct: float
    missing: list[MissingMapping]


@strawberry.type
class MappingCoverage:
    deployments: CoverageStat
    work_items: CoverageStat


@strawberry.type
class WindowSpec:
    kind: str
    duration_days: int | None = None


METRIC_LINEAGE_REGISTRY: dict[str, tuple[list[str], WindowSpec]] = {
    "throughput": (["work_item_metrics_daily"], WindowSpec(kind="daily")),
    "cycle_time": (["work_item_metrics_daily"], WindowSpec(kind="daily")),
    "lead_time": (["work_item_metrics_daily"], WindowSpec(kind="daily")),
    "wip": (["work_item_metrics_daily"], WindowSpec(kind="daily")),
    "review_load": (["repo_metrics_daily"], WindowSpec(kind="daily")),
    "review_latency": (["repo_metrics_daily"], WindowSpec(kind="daily")),
    "deployment_frequency": (["repo_metrics_daily"], WindowSpec(kind="daily")),
    "change_failure_rate": (["repo_metrics_daily"], WindowSpec(kind="daily")),
    "after_hours_ratio": (["team_metrics_daily"], WindowSpec(kind="daily")),
    "weekend_ratio": (["team_metrics_daily"], WindowSpec(kind="daily")),
    "investment_mix": (
        ["work_unit_investments"],
        WindowSpec(kind="rolling", duration_days=30),
    ),
}


@strawberry.type
class MetricLineage:
    metric_id: strawberry.ID
    source_tables: list[str]
    compute_window: WindowSpec
    computed_at: datetime
    row_count: int | None = None


async def compute_metric_lineage(
    context: Any, team: str, metric_id: str
) -> MetricLineage | None:
    from dev_health_ops.api.graphql.authz import require_org_id

    org_id = require_org_id(context)
    registry_entry = METRIC_LINEAGE_REGISTRY.get(metric_id)
    if registry_entry is None:
        return None
    source_tables, window = registry_entry
    computed_at, row_count = await _lineage_freshness(
        context, org_id=org_id, team=team, tables=source_tables
    )
    if computed_at is None:
        return None
    return MetricLineage(
        metric_id=strawberry.ID(metric_id),
        source_tables=source_tables,
        compute_window=window,
        computed_at=computed_at,
        row_count=row_count,
    )


async def _lineage_freshness(
    context: Any, *, org_id: str, team: str, tables: Sequence[str]
) -> tuple[datetime | None, int | None]:
    computed_values: list[datetime] = []
    total_rows = 0
    for table in tables:
        if not _safe_table_name(table):
            continue
        sql = f"""
            SELECT argMax(computed_at, computed_at) AS computed_at, count() AS row_count
            FROM {table}
            WHERE org_id = %(org_id)s
        """
        rows = await _query_dicts(context, sql, {"org_id": org_id, "team": team})
        if not rows:
            continue
        row = rows[0]
        computed = row.get("computed_at")
        if isinstance(computed, datetime):
            computed_values.append(computed)
        elif computed:
            try:
                computed_values.append(datetime.fromisoformat(str(computed)))
            except ValueError:
                logger.debug(
                    "Ignoring unparsable computed_at %r for %s", computed, table
                )
        total_rows += _int(row.get("row_count"))
    if not computed_values:
        return None, None
    return max(computed_values), total_rows


async def _query_dicts(
    context: Any, sql: str, params: Mapping[str, Any]
) -> list[Mapping[str, Any]]:
    if context.client is None:
        return []
    from dev_health_ops.api.queries.client import query_dicts

    try:
        return list(await query_dicts(context.client, sql, dict(params)))
    except Exception:
        logger.exception("Data health query failed")
        return []


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_table_name(table: str) -> bool:
    return table.replace("_", "").isalnum()


@strawberry.type
class DataHealth:
    connectors: list[ConnectorStatus]
    identity_mapping: IdentityMappingHealth
    mapping_coverage: MappingCoverage
    team: strawberry.Private[str]
    context: strawberry.Private[Any]

    @strawberry.field
    async def metric_lineage(
        self, info: Info, metric_id: strawberry.ID
    ) -> MetricLineage | None:
        # Keep this resolver with the type to avoid a models<->resolvers import cycle.
        context = info.context if info is not None else self.context
        return await compute_metric_lineage(context, self.team, str(metric_id))
