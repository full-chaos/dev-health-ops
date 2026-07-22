"""ClickHouse reader and writer for canonical operational migration backfills."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
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
    expected_incidents: int
    verified_incidents: int
    expected_service_repository_mappings: int
    verified_service_repository_mappings: int

    @property
    def parity_verified(self) -> bool:
        """Return whether every expected migration identity was observed."""
        return (
            self.verified_incidents == self.expected_incidents
            and self.verified_service_repository_mappings
            == self.expected_service_repository_mappings
        )


@dataclass(frozen=True, slots=True)
class LegacyIncidentProviderIdentityFailure:
    """Legacy incident that cannot be assigned a canonical provider identity."""

    provider: str
    repo_full_name: str
    incident_id: str


class OperationalBackfillPreflightError(RuntimeError):
    """Raised before writes when legacy rows cannot be migrated losslessly."""

    def __init__(
        self, failures: tuple[LegacyIncidentProviderIdentityFailure, ...]
    ) -> None:
        self.failures = failures
        examples = ", ".join(
            f"{failure.provider}:{failure.repo_full_name}:{failure.incident_id}"
            for failure in failures[:5]
        )
        if len(failures) > 5:
            examples = f"{examples}, ..."
        super().__init__(
            "canonical operational backfill preflight failed: "
            f"{len(failures)} legacy GitHub/GitLab incident(s) have no "
            f"recoverable provider identity ({examples}); legacy incidents "
            "must not be dropped"
        )


class OperationalBackfillParityError(RuntimeError):
    """Raised after writes when expected canonical identities are absent."""

    def __init__(
        self,
        *,
        missing_incident_ids: tuple[str, ...],
        missing_service_repository_mapping_ids: tuple[str, ...],
    ) -> None:
        self.missing_incident_ids = missing_incident_ids
        self.missing_service_repository_mapping_ids = (
            missing_service_repository_mapping_ids
        )
        super().__init__(
            "canonical operational backfill parity verification failed: "
            f"missing incidents={len(missing_incident_ids)}, "
            "missing service_repository_mappings="
            f"{len(missing_service_repository_mapping_ids)}; legacy incidents "
            "must not be dropped"
        )


@dataclass(frozen=True, slots=True)
class _OperationalBackfillParity:
    expected_incidents: int
    verified_incidents: int
    expected_service_repository_mappings: int
    verified_service_repository_mappings: int


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
        expected_issue_batches = map_legacy_issue_incident_batches(legacy_rows)
        issue_batches = await _without_existing_incidents(store, expected_issue_batches)
        atlassian_batch = await _load_atlassian_ops_batch(
            store,
            org_id=org_id,
            provider_instance_id=atlassian_provider_instance_id,
        )
        all_batches = (*issue_batches, atlassian_batch)
        for batch in all_batches:
            await write_operational_batch(store, batch)
        parity = await _verify_expected_canonical_identities(
            store, (*expected_issue_batches, atlassian_batch)
        )
    return OperationalBackfillResult(
        services=sum(len(batch.services) for batch in all_batches),
        incidents=sum(len(batch.incidents) for batch in all_batches),
        alerts=sum(len(batch.alerts) for batch in all_batches),
        schedules=sum(len(batch.on_call_schedules) for batch in all_batches),
        service_repository_mappings=sum(
            len(batch.service_repository_mappings) for batch in all_batches
        ),
        expected_incidents=parity.expected_incidents,
        verified_incidents=parity.verified_incidents,
        expected_service_repository_mappings=(
            parity.expected_service_repository_mappings
        ),
        verified_service_repository_mappings=(
            parity.verified_service_repository_mappings
        ),
    )


async def _verify_expected_canonical_identities(
    store: ClickHouseStore, batches: tuple[OperationalBatch, ...]
) -> _OperationalBackfillParity:
    """Prove all deterministic incident and service-mapping IDs are current."""
    expected_incident_ids = {
        incident.id for batch in batches for incident in batch.incidents
    }
    expected_mapping_ids = {
        mapping.id for batch in batches for mapping in batch.service_repository_mappings
    }
    verified_incident_ids = await _load_current_identity_ids(
        store, "operational_incidents", expected_incident_ids
    )
    verified_mapping_ids = await _load_current_identity_ids(
        store,
        "operational_service_repository_mappings",
        expected_mapping_ids,
    )
    missing_incident_ids = tuple(sorted(expected_incident_ids - verified_incident_ids))
    missing_mapping_ids = tuple(sorted(expected_mapping_ids - verified_mapping_ids))
    if missing_incident_ids or missing_mapping_ids:
        raise OperationalBackfillParityError(
            missing_incident_ids=missing_incident_ids,
            missing_service_repository_mapping_ids=missing_mapping_ids,
        )
    return _OperationalBackfillParity(
        expected_incidents=len(expected_incident_ids),
        verified_incidents=len(verified_incident_ids),
        expected_service_repository_mappings=len(expected_mapping_ids),
        verified_service_repository_mappings=len(verified_mapping_ids),
    )


async def _load_current_identity_ids(
    store: ClickHouseStore, table: str, expected_ids: set[str]
) -> set[str]:
    if not expected_ids:
        return set()
    assert store.client is not None
    result = await asyncio.to_thread(
        store.client.query,
        f"SELECT id FROM {current_operational_rows_sql(table, ('id IN {ids:Array(String)}',))}",
        parameters={"org_id": store.org_id, "ids": sorted(expected_ids)},
    )
    return {str(row[0]) for row in result.result_rows}


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
    identity_failures: list[LegacyIncidentProviderIdentityFailure] = []
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
            identity_failures.append(
                LegacyIncidentProviderIdentityFailure(
                    provider=provider,
                    repo_full_name=str(row[5]),
                    incident_id=str(row[1]),
                )
            )
            continue
        started_at = row[3]
        if not isinstance(started_at, datetime):
            continue
        started_at = _utc_datetime(started_at)
        resolved_at = _utc_datetime(row[4]) if isinstance(row[4], datetime) else None
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
                resolved_at=resolved_at,
                source_version_at=resolved_at or started_at,
            )
        )
    if identity_failures:
        logging.error(
            "Canonical operational backfill preflight rejected %d legacy "
            "incident(s) without provider identity",
            len(identity_failures),
        )
        raise OperationalBackfillPreflightError(tuple(identity_failures))
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


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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
            created_at=_utc_datetime(row[6]),
            provider_id=str(row[7]) if row[7] is not None else None,
            last_synced=_utc_datetime(row[8]),
        )
        for row in incidents_result.result_rows
    )
    alerts = tuple(
        AtlassianOpsAlert(
            id=str(row[0]),
            status=str(row[1]),
            priority=str(row[2]),
            created_at=_utc_datetime(row[3]),
            acknowledged_at=_utc_datetime(row[4]) if row[4] is not None else None,
            snoozed_at=_utc_datetime(row[5]) if row[5] is not None else None,
            closed_at=_utc_datetime(row[6]) if row[6] is not None else None,
            last_synced=_utc_datetime(row[7]),
        )
        for row in alerts_result.result_rows
    )
    schedules = tuple(
        AtlassianOpsSchedule(
            id=str(row[0]),
            name=str(row[1]),
            timezone=row[2],
            last_synced=_utc_datetime(row[3]),
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
