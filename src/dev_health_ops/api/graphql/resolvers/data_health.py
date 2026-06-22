"""Resolvers for the operator-facing data-health GraphQL surface."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from dev_health_ops.api.services.configuration.clickhouse_identity_admin import (
    ClickHouseIdentity,
    _decode_provider_identities,
)
from dev_health_ops.models.settings import (
    JobRun,
    JobRunStatus,
    ScheduledJob,
    SyncConfiguration,
)

from ..authz import require_org_id
from ..context import GraphQLContext
from ..errors import AuthorizationError
from ..models.data_health import (
    AliasSuggestion,
    ConnectorFailure,
    ConnectorStatus,
    CoverageStat,
    DataHealth,
    IdentityMappingHealth,
    MappingCoverage,
    MetricLineage,
    MissingMapping,
    UnmappedIdentity,
    compute_metric_lineage,
)

logger = logging.getLogger(__name__)

MAX_UNMAPPED_IDENTITIES = 25
MAX_ALIAS_SUGGESTIONS = 25

# TODO(CHAOS-1631): promote sync_configurations/job_runs into a dedicated
# sync-run read model with rows-ingested/stage fields for connector health.


async def resolve_data_health(context: GraphQLContext, team: str) -> DataHealth:
    """Return connector, identity, mapping, and lineage health for a team."""

    _require_operator(context)
    require_org_id(context)
    connectors = await resolve_connectors(context)
    identity_mapping = await resolve_identity_mapping(context, team)
    mapping_coverage = await resolve_mapping_coverage(context, team)
    return DataHealth(
        connectors=connectors,
        identity_mapping=identity_mapping,
        mapping_coverage=mapping_coverage,
        team=team,
        context=context,
    )


def _require_operator(context: GraphQLContext) -> None:
    user = context.user
    if user is None:
        raise AuthorizationError("Authentication required")
    role = str(getattr(user, "role", "") or "").lower()
    if getattr(user, "is_superuser", False) or role in {"admin", "owner", "operator"}:
        return
    raise AuthorizationError("Data health requires operator access")


async def resolve_connectors(context: GraphQLContext) -> list[ConnectorStatus]:
    """Read connector status from existing sync configuration/job-run tables."""

    org_id = require_org_id(context)
    session = _db_session(context)
    if session is None:
        return []

    stmt = (
        select(SyncConfiguration)
        .where(
            SyncConfiguration.org_id == org_id, SyncConfiguration.is_active.is_(True)
        )
        .options(selectinload(SyncConfiguration.children))
        .order_by(SyncConfiguration.provider, SyncConfiguration.name)
    )
    configs = list((await session.execute(stmt)).scalars().all())
    statuses: list[ConnectorStatus] = []
    for config in configs:
        latest_run = await _latest_job_run(session, config.id)
        stats = _as_mapping(config.last_sync_stats) or _as_mapping(
            getattr(latest_run, "result", None)
        )
        rows_ingested = _rows_ingested(stats)
        last_sync_at = config.last_sync_at or getattr(latest_run, "completed_at", None)
        failure = _connector_failure(config, latest_run)
        statuses.append(
            ConnectorStatus(
                provider=str(config.provider),
                scope=_connector_scope(config),
                last_sync_at=last_sync_at,
                rows_ingested=rows_ingested,
                last_failure=failure,
            )
        )
    return statuses


async def _latest_job_run(session: Any, sync_config_id: Any) -> JobRun | None:
    stmt = (
        select(JobRun)
        .join(ScheduledJob, ScheduledJob.id == JobRun.job_id)
        .where(ScheduledJob.sync_config_id == sync_config_id)
        .order_by(JobRun.created_at.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalars().first()


def _connector_scope(config: SyncConfiguration) -> str:
    targets = config.sync_targets or []
    if targets:
        return ", ".join(str(target) for target in targets[:3])
    return str(config.name)


def _connector_failure(
    config: SyncConfiguration, latest_run: JobRun | None
) -> ConnectorFailure | None:
    message = config.last_sync_error or getattr(latest_run, "error", None)
    failed_run = latest_run is not None and getattr(latest_run, "status", None) in {
        JobRunStatus.FAILED.value,
        JobRunStatus.CANCELLED.value,
    }
    if not message and config.last_sync_success is not False and not failed_run:
        return None
    occurred_at = (
        getattr(latest_run, "completed_at", None)
        or getattr(latest_run, "started_at", None)
        or config.last_sync_at
        or config.updated_at
        or datetime.now(UTC)
    )
    result = _as_mapping(getattr(latest_run, "result", None))
    return ConnectorFailure(
        occurred_at=occurred_at,
        message=str(message or "Last sync failed"),
        stage=str(result.get("stage")) if result and result.get("stage") else None,
    )


async def resolve_identity_mapping(
    context: GraphQLContext, team: str
) -> IdentityMappingHealth:
    org_id = require_org_id(context)
    observed = await _observed_identities(context, org_id=org_id, team=team)
    mapped = await _mapped_identities(context, org_id=org_id, team=team)
    mapped_keys = _identity_keys(mapped)

    unmapped: list[UnmappedIdentity] = []
    for row in observed:
        identity = _unmapped_identity(row)
        if not _is_mapped(identity, mapped_keys):
            unmapped.append(identity)

    unmapped.sort(key=lambda item: item.observed_count or 0, reverse=True)
    suggestions = _alias_suggestions(unmapped, mapped)[:MAX_ALIAS_SUGGESTIONS]
    return IdentityMappingHealth(
        unmapped_count=len(unmapped),
        unmapped_identities=unmapped[:MAX_UNMAPPED_IDENTITIES],
        suggested_aliases=suggestions,
    )


async def _observed_identities(
    context: GraphQLContext, *, org_id: str, team: str
) -> list[Mapping[str, Any]]:
    client = context.client
    if client is None:
        return []
    sql = """
        SELECT provider, identity, display_name, sum(observed_count) AS observed_count
        FROM (
            SELECT 'git' AS provider, lower(author_email) AS identity,
                   any(author_name) AS display_name, count() AS observed_count
            FROM git_commits
            WHERE org_id = %(org_id)s AND lower(author_email) != ''
            GROUP BY identity
            UNION ALL
            SELECT provider, arrayJoin(assignees) AS identity,
                   identity AS display_name, count() AS observed_count
            FROM work_items
            WHERE org_id = %(org_id)s AND has(assignees, '') = 0
            GROUP BY provider, identity
        )
        GROUP BY provider, identity, display_name
        ORDER BY observed_count DESC
        LIMIT 100
    """
    return await _query_dicts(context, sql, {"org_id": org_id, "team": team})


async def _mapped_identities(
    context: GraphQLContext, *, org_id: str, team: str
) -> list[ClickHouseIdentity]:
    """Read the org's active identities from the ClickHouse ``identities`` table.

    ClickHouse is the system of record for identities (CHAOS-2600 CS5/CS6). This
    goes through the same ``_query_dicts`` path the rest of this resolver uses,
    which creates a fresh per-thread clickhouse-connect client per call — the
    process-wide shared client is not thread-safe, so we must not reuse it.
    """
    sql = """
        SELECT canonical_id, email, display_name, provider_identities,
               team_ids, is_active, org_id
        FROM identities FINAL
        WHERE org_id = %(org_id)s AND is_active = 1
    """
    rows = await _query_dicts(context, sql, {"org_id": org_id})
    identities = [_row_to_identity(row, org_id) for row in rows]
    if not team:
        return identities
    return [
        identity
        for identity in identities
        if not identity.team_ids or team in identity.team_ids
    ]


def _row_to_identity(row: Mapping[str, Any], org_id: str) -> ClickHouseIdentity:
    canonical_id = str(row.get("canonical_id") or "")
    row_org_id = str(row.get("org_id") or org_id)
    return ClickHouseIdentity(
        canonical_id=canonical_id,
        identity_uuid=uuid.uuid5(
            uuid.NAMESPACE_URL, f"identity:{row_org_id}:{canonical_id}"
        ),
        display_name=row.get("display_name") or None,
        email=row.get("email") or None,
        provider_identities=_decode_provider_identities(row.get("provider_identities")),
        team_ids=[str(t) for t in (row.get("team_ids") or [])],
        is_active=bool(row.get("is_active", 1)),
        updated_at=datetime.now(UTC),
        org_id=row_org_id,
    )


def _unmapped_identity(row: Mapping[str, Any]) -> UnmappedIdentity:
    identity = str(row.get("identity") or "")
    email = identity if "@" in identity else None
    return UnmappedIdentity(
        provider=str(row.get("provider") or "unknown"),
        email=email,
        display_name=str(row.get("display_name") or identity) or None,
        observed_count=_int(row.get("observed_count")),
    )


def _identity_keys(mapped: Sequence[ClickHouseIdentity]) -> set[str]:
    keys: set[str] = set()
    for row in mapped:
        for value in [row.canonical_id, row.email, row.display_name]:
            if value:
                keys.add(_norm(value))
        for values in (row.provider_identities or {}).values():
            keys.update(_norm(str(value)) for value in values if value)
    return keys


def _is_mapped(identity: UnmappedIdentity, mapped_keys: set[str]) -> bool:
    values = [identity.email, identity.display_name]
    return any(value and _norm(value) in mapped_keys for value in values)


def _alias_suggestions(
    unmapped: Sequence[UnmappedIdentity], mapped: Sequence[ClickHouseIdentity]
) -> list[AliasSuggestion]:
    by_local: dict[str, ClickHouseIdentity] = {}
    for row in mapped:
        for value in [row.email, row.canonical_id]:
            local = _email_local(value)
            if local:
                by_local.setdefault(local, row)

    suggestions: list[AliasSuggestion] = []
    for identity in unmapped:
        local = _email_local(identity.email) or _email_local(identity.display_name)
        if not local or local not in by_local:
            continue
        target = by_local[local]
        suggestions.append(
            AliasSuggestion(
                unmapped_identity=identity,
                suggested_canonical_id=target.canonical_id,
                confidence=0.82,
            )
        )
    return suggestions


async def resolve_mapping_coverage(
    context: GraphQLContext, team: str
) -> MappingCoverage:
    org_id = require_org_id(context)
    deployments, work_items = await _coverage_rows(context, org_id=org_id, team=team)
    return MappingCoverage(
        deployments=_coverage_stat(deployments, "No deployment-to-release/PR mapping"),
        work_items=_coverage_stat(work_items, "No work-item project/provider mapping"),
    )


async def _coverage_rows(
    context: GraphQLContext, *, org_id: str, team: str
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    deployments_sql = """
        SELECT coalesce(nullIf(r.repo, ''), toString(d.repo_id)) AS repo_name,
               count() AS total,
               countIf(d.pull_request_number IS NOT NULL OR d.release_ref != '') AS covered
        FROM deployments d
        LEFT JOIN repos r ON r.id = d.repo_id
        WHERE d.org_id = %(org_id)s
        GROUP BY repo_name
        ORDER BY repo_name
    """
    work_items_sql = """
        SELECT coalesce(nullIf(r.repo, ''), toString(w.repo_id)) AS repo_name,
               count() AS total,
               countIf(w.provider != '' AND w.project_key != '') AS covered
        FROM work_items w
        LEFT JOIN repos r ON r.id = w.repo_id
        WHERE w.org_id = %(org_id)s
        GROUP BY repo_name
        ORDER BY repo_name
    """
    deployments = await _query_dicts(
        context, deployments_sql, {"org_id": org_id, "team": team}
    )
    work_items = await _query_dicts(
        context, work_items_sql, {"org_id": org_id, "team": team}
    )
    return deployments, work_items


def _coverage_stat(rows: Iterable[Mapping[str, Any]], reason: str) -> CoverageStat:
    materialized = list(rows)
    total = len(materialized)
    covered = sum(1 for row in materialized if _int(row.get("covered")) > 0)
    missing = [
        MissingMapping(repo_name=str(row.get("repo_name") or "unknown"), reason=reason)
        for row in materialized
        if _int(row.get("covered")) <= 0
    ]
    pct = (covered / total * 100.0) if total else 100.0
    return CoverageStat(
        total_repos=total,
        covered_repos=covered,
        coverage_pct=pct,
        missing=missing,
    )


async def resolve_metric_lineage(
    context: GraphQLContext, team: str, metric_id: str
) -> MetricLineage | None:
    return await compute_metric_lineage(context, team, metric_id)


async def _query_dicts(
    context: GraphQLContext, sql: str, params: Mapping[str, Any]
) -> list[Mapping[str, Any]]:
    if context.client is None:
        return []
    from dev_health_ops.api.queries.client import query_dicts

    try:
        return list(await query_dicts(context.client, sql, dict(params)))
    except Exception:
        logger.exception("Data health query failed")
        return []


def _db_session(context: GraphQLContext) -> Any | None:
    return getattr(context, "db_session", None) or getattr(context, "session", None)


def _rows_ingested(stats: Mapping[str, Any] | None) -> int:
    if not stats:
        return 0
    for key in ("rows_ingested", "rows", "items_synced", "items", "count"):
        if key in stats:
            return _int(stats.get(key))
    return 0


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _norm(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _email_local(value: str | None) -> str | None:
    if not value:
        return None
    normalized = _norm(value)
    if "@" in normalized:
        return normalized.split("@", 1)[0]
    return normalized or None
