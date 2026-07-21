"""ClickHouse reader and writer for canonical operational migration backfills."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, replace
from datetime import datetime
from uuid import UUID

from dev_health_ops.backfill.operational import (
    LegacyIncidentRepositoryRow,
    map_legacy_issue_incident_batches,
)
from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsAlert,
    AtlassianOpsIncident,
    AtlassianOpsSchedule,
)
from dev_health_ops.models.operational import OperationalBatch
from dev_health_ops.models.operational_identity import (
    normalized_operational_provider_instance,
)
from dev_health_ops.providers.operational_migration import (
    AtlassianOpsRows,
    AtlassianOpsSource,
    map_atlassian_ops_batch,
    write_operational_batch,
)
from dev_health_ops.storage.clickhouse import ClickHouseStore
from dev_health_ops.storage.operational_current import current_operational_rows_sql


@dataclass(frozen=True, slots=True)
class OperationalBackfillResult:
    """Counts written by one canonical operational migration invocation."""

    services: int
    incidents: int
    alerts: int
    schedules: int
    service_repository_mappings: int


async def run_canonical_operational_backfill(
    *,
    clickhouse_uri: str,
    org_id: str,
    github_provider_instance_id: str | None = None,
    gitlab_provider_instance_id: str | None = None,
    atlassian_provider_instance_id: str = "atlassian-ops",
) -> OperationalBackfillResult:
    """Join legacy rows and persist their deterministic canonical replacements."""
    async with ClickHouseStore(clickhouse_uri) as store:
        store.org_id = org_id
        legacy_rows = await _load_legacy_incident_repository_rows(
            store,
            org_id=org_id,
            github_provider_instance_id=github_provider_instance_id,
            gitlab_provider_instance_id=gitlab_provider_instance_id,
        )
        issue_batches = await _without_existing_incidents(
            store, map_legacy_issue_incident_batches(legacy_rows)
        )
        atlassian_batch = await _load_atlassian_ops_batch(
            store,
            org_id=org_id,
            provider_instance_id=atlassian_provider_instance_id,
        )
        all_batches = (*issue_batches, atlassian_batch)
        for batch in all_batches:
            await write_operational_batch(store, batch)
    return OperationalBackfillResult(
        services=sum(len(batch.services) for batch in all_batches),
        incidents=sum(len(batch.incidents) for batch in all_batches),
        alerts=sum(len(batch.alerts) for batch in all_batches),
        schedules=sum(len(batch.on_call_schedules) for batch in all_batches),
        service_repository_mappings=sum(
            len(batch.service_repository_mappings) for batch in all_batches
        ),
    )


async def _without_existing_incidents(
    store: ClickHouseStore, batches: tuple[OperationalBatch, ...]
) -> tuple[OperationalBatch, ...]:
    assert store.client is not None
    result = await asyncio.to_thread(
        store.client.query,
        f"SELECT id FROM {current_operational_rows_sql('operational_incidents')}",
        parameters={"org_id": store.org_id},
    )
    existing_ids = {str(row[0]) for row in result.result_rows}
    return tuple(
        replace(
            batch,
            incidents=tuple(
                incident
                for incident in batch.incidents
                if incident.id not in existing_ids
            ),
        )
        for batch in batches
    )


async def _load_legacy_incident_repository_rows(
    store: ClickHouseStore,
    *,
    org_id: str,
    github_provider_instance_id: str | None,
    gitlab_provider_instance_id: str | None,
) -> tuple[LegacyIncidentRepositoryRow, ...]:
    assert store.client is not None
    result = await asyncio.to_thread(
        store.client.query,
        """
        SELECT
            i.repo_id, i.incident_id, i.status, i.started_at, i.resolved_at,
            r.repo, r.provider, r.settings
        FROM incidents AS i FINAL
        INNER JOIN repos AS r FINAL ON i.repo_id = r.id
        WHERE i.org_id = {org_id:String}
          AND r.org_id = {org_id:String}
          AND r.provider IN ('github', 'gitlab')
        """,
        parameters={"org_id": org_id},
    )
    rows: list[LegacyIncidentRepositoryRow] = []
    for row in result.result_rows:
        provider = str(row[6])
        settings = _repo_settings(row[7])
        provider_instance_id = _recover_provider_instance_id(
            provider,
            settings,
            github_provider_instance_id
            if provider == "github"
            else gitlab_provider_instance_id,
        )
        if provider_instance_id is None:
            logging.warning(
                "Skipping operational incident backfill without recoverable %s host for repo %s",
                provider,
                row[5],
            )
            continue
        started_at = row[3]
        if not isinstance(started_at, datetime):
            continue
        rows.append(
            LegacyIncidentRepositoryRow(
                org_id=org_id,
                repo_id=UUID(str(row[0])),
                repo_full_name=str(row[5]),
                provider=provider,
                provider_instance_id=provider_instance_id,
                incident_id=str(row[1]),
                status=str(row[2]) if row[2] is not None else None,
                started_at=started_at,
                resolved_at=row[4] if isinstance(row[4], datetime) else None,
                source_version_at=(
                    row[4] if isinstance(row[4], datetime) else started_at
                ),
            )
        )
    return tuple(rows)


def _recover_provider_instance_id(
    provider: str, settings: dict[str, str], configured_instance: str | None
) -> str | None:
    candidates = (
        settings.get(f"{provider}_instance_url"),
        settings.get("html_url"),
        settings.get("api_url"),
        settings.get("url"),
        configured_instance,
    )
    for candidate in candidates:
        if candidate:
            normalized = normalized_operational_provider_instance(provider, candidate)
            if normalized is not None:
                return normalized
    return None


def _repo_settings(value: object) -> dict[str, str]:
    if isinstance(value, dict):
        return {
            str(key): item.strip()
            for key, item in value.items()
            if isinstance(item, str) and item.strip()
        }
    if not isinstance(value, str) or not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        str(key): item.strip()
        for key, item in parsed.items()
        if isinstance(item, str) and item.strip()
    }


async def _load_atlassian_ops_batch(
    store: ClickHouseStore,
    *,
    org_id: str,
    provider_instance_id: str,
):
    assert store.client is not None
    incidents_result = await asyncio.to_thread(
        store.client.query,
        "SELECT id, url, summary, description, status, severity, created_at, "
        "provider_id, last_synced FROM atlassian_ops_incidents FINAL "
        "WHERE org_id = {org_id:String}",
        parameters={"org_id": org_id},
    )
    alerts_result = await asyncio.to_thread(
        store.client.query,
        "SELECT id, status, priority, created_at, acknowledged_at, snoozed_at, "
        "closed_at, last_synced FROM atlassian_ops_alerts FINAL "
        "WHERE org_id = {org_id:String}",
        parameters={"org_id": org_id},
    )
    schedules_result = await asyncio.to_thread(
        store.client.query,
        "SELECT id, name, timezone, last_synced FROM atlassian_ops_schedules FINAL "
        "WHERE org_id = {org_id:String}",
        parameters={"org_id": org_id},
    )
    incidents = tuple(
        AtlassianOpsIncident(
            id=str(row[0]),
            url=str(row[1]) if row[1] is not None else None,
            summary=str(row[2]),
            description=str(row[3]) if row[3] is not None else None,
            status=str(row[4]),
            severity=str(row[5]),
            created_at=row[6],
            provider_id=str(row[7]) if row[7] is not None else None,
            last_synced=row[8],
        )
        for row in incidents_result.result_rows
    )
    alerts = tuple(
        AtlassianOpsAlert(
            id=str(row[0]),
            status=str(row[1]),
            priority=str(row[2]),
            created_at=row[3],
            acknowledged_at=row[4],
            snoozed_at=row[5],
            closed_at=row[6],
            last_synced=row[7],
        )
        for row in alerts_result.result_rows
    )
    schedules = tuple(
        AtlassianOpsSchedule(
            id=str(row[0]), name=str(row[1]), timezone=row[2], last_synced=row[3]
        )
        for row in schedules_result.result_rows
    )
    return map_atlassian_ops_batch(
        AtlassianOpsSource(
            org_id=org_id,
            provider_instance_id=provider_instance_id,
            rows=AtlassianOpsRows(
                incidents=incidents,
                alerts=alerts,
                schedules=schedules,
            ),
        )
    )
