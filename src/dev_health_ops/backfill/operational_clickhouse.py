"""ClickHouse reader and writer for canonical operational migration backfills."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsAlert,
    AtlassianOpsIncident,
    AtlassianOpsSchedule,
)
from dev_health_ops.models.operational import OperationalBatch
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
            f"{len(missing_service_repository_mapping_ids)}; canonical "
            "backfill is incomplete"
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
    atlassian_provider_instance_id: str = "atlassian-ops",
) -> OperationalBackfillResult:
    """Persist deterministic canonical replacements for Atlassian Ops rows."""
    async with ClickHouseStore(clickhouse_uri) as store:
        store.org_id = org_id
        atlassian_batch = await _load_atlassian_ops_batch(
            store,
            org_id=org_id,
            provider_instance_id=atlassian_provider_instance_id,
        )
        all_batches = (atlassian_batch,)
        for batch in all_batches:
            await write_operational_batch(store, batch)
        parity = await _verify_expected_canonical_identities(store, all_batches)
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
