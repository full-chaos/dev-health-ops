"""Strawberry GraphQL types for the data-health surface."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import strawberry
from strawberry.types import Info


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


@strawberry.type
class MetricLineage:
    metric_id: strawberry.ID
    source_tables: list[str]
    compute_window: WindowSpec
    computed_at: datetime
    row_count: int | None = None


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
        from dev_health_ops.api.graphql.resolvers.data_health import (
            resolve_metric_lineage,
        )

        context = info.context if info is not None else self.context
        return await resolve_metric_lineage(context, self.team, str(metric_id))
