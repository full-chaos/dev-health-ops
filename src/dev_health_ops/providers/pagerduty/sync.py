"""Bounded PagerDuty REST reconciliation through canonical operational sinks."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import TypeVar

from dev_health_ops.models.operational import (
    EscalationPolicy,
    OnCallSchedule,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
)
from dev_health_ops.providers.pagerduty.client import PagerDutyClient
from dev_health_ops.providers.pagerduty.degradation import (
    DATASET_FETCH_ERRORS,
    PagerDutyDatasetDegradedError,
)
from dev_health_ops.providers.pagerduty.enrichment import PagerDutyEnrichmentToggles
from dev_health_ops.providers.pagerduty.incident_cursor import (
    incident_source_time,
    iter_resumable_incidents,
)
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer
from dev_health_ops.providers.pagerduty.operational_store import (
    PagerDutyOperationalStore,
)


@dataclass(frozen=True, slots=True)
class PagerDutySyncOptions:
    dataset_key: str
    window_start: datetime | None
    window_end: datetime | None
    resume_after: datetime | None = None
    batch_size: int = 100
    enrichment_cap: int = 100
    enrichment: PagerDutyEnrichmentToggles = field(
        default_factory=PagerDutyEnrichmentToggles
    )

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("PagerDuty batch_size must be positive")
        if self.enrichment_cap < 0:
            raise ValueError("PagerDuty enrichment_cap must not be negative")


@dataclass(frozen=True, slots=True)
class PagerDutySyncResult:
    dataset_key: str
    persisted: int
    watermark_at: datetime | None
    degraded: bool
    observations: tuple[dict[str, object], ...]


_Source = TypeVar("_Source")
_Destination = TypeVar("_Destination")
_ReferenceEntity = TypeVar(
    "_ReferenceEntity",
    OperationalService,
    EscalationPolicy,
    OnCallSchedule,
    OperationalTeam,
    OperationalUser,
)


class PagerDutyOperationalSync:
    """Sync one PagerDuty dataset without crossing account or sink boundaries."""

    def __init__(
        self,
        *,
        client: PagerDutyClient,
        store: PagerDutyOperationalStore,
        normalizer: PagerDutyNormalizer,
    ) -> None:
        self._client = client
        self._store = store
        self._normalizer = normalizer

    async def run(self, options: PagerDutySyncOptions) -> PagerDutySyncResult:
        try:
            return await self._run(options)
        except DATASET_FETCH_ERRORS as exc:
            raise PagerDutyDatasetDegradedError(options.dataset_key, exc) from exc

    async def _run(self, options: PagerDutySyncOptions) -> PagerDutySyncResult:
        watermark_at: datetime | None = None
        match options.dataset_key:
            case (
                "incident-alerts" | "incident-log-entries" | "incident-notes"
            ) as dataset_key if not options.enrichment.enabled(dataset_key):
                persisted = 0
            case "services":
                persisted = await self._sync_reference(
                    self._client.list_services,
                    self._normalizer.service,
                    self._store.insert_operational_services,
                    options.batch_size,
                    OperationalService,
                    "service",
                )
            case "business-services":
                persisted = await self._sync_reference(
                    self._client.list_business_services,
                    self._normalizer.business_service,
                    self._store.insert_operational_services,
                    options.batch_size,
                    OperationalService,
                    "business_service",
                )
            case "escalation-policies":
                persisted = await self._sync_reference(
                    self._client.list_escalation_policies,
                    self._normalizer.escalation_policy,
                    self._store.insert_operational_escalation_policies,
                    options.batch_size,
                    EscalationPolicy,
                    "escalation_policy",
                )
            case "schedules":
                persisted = await self._sync_reference(
                    self._client.list_schedules,
                    self._normalizer.schedule,
                    self._store.insert_operational_on_call_schedules,
                    options.batch_size,
                    OnCallSchedule,
                    "schedule",
                )
            case "on-calls":
                persisted = await self._sync(
                    self._client.list_oncalls,
                    self._normalizer.oncall,
                    self._store.insert_operational_on_call_assignments,
                    options.batch_size,
                )
            case "users":
                persisted = await self._sync_reference(
                    self._client.list_users,
                    self._normalizer.user,
                    self._store.insert_operational_users,
                    options.batch_size,
                    OperationalUser,
                    "user",
                )
            case "teams":
                persisted = await self._sync_reference(
                    self._client.list_teams,
                    self._normalizer.team,
                    self._store.insert_operational_teams,
                    options.batch_size,
                    OperationalTeam,
                    "team",
                )
            case "incidents":
                persisted, watermark_at = await self._sync_incidents(options)
            case "incident-alerts":
                persisted, watermark_at = await self._sync_enrichment(
                    options,
                    self._client.iter_incident_alert_pages,
                    self._normalizer.alert,
                    self._store.insert_operational_alerts,
                )
            case "incident-log-entries":
                persisted, watermark_at = await self._sync_enrichment(
                    options,
                    self._client.iter_incident_log_entry_pages,
                    self._normalizer.log_entry,
                    self._store.insert_operational_incident_timeline_events,
                )
            case "incident-notes":
                persisted, watermark_at = await self._sync_enrichment(
                    options,
                    self._client.iter_incident_note_pages,
                    self._normalizer.note,
                    self._store.insert_operational_incident_notes,
                )
            case unsupported:
                raise ValueError(f"Unsupported PagerDuty dataset {unsupported!r}")
        return PagerDutySyncResult(
            dataset_key=options.dataset_key,
            persisted=persisted,
            watermark_at=watermark_at,
            degraded=False,
            observations=tuple(self._client.drain_usage_observations()),
        )

    async def _sync(
        self,
        fetch: Callable[[], Awaitable[list[_Source]]],
        normalize: Callable[[_Source], _Destination],
        persist: Callable[[list[_Destination]], Awaitable[None]],
        batch_size: int,
    ) -> int:
        rows = await fetch()
        values = [normalize(row) for row in rows]
        for start in range(0, len(values), batch_size):
            await persist(values[start : start + batch_size])
        return len(values)

    async def _sync_reference(
        self,
        fetch: Callable[[], Awaitable[list[_Source]]],
        normalize: Callable[[_Source], _ReferenceEntity],
        persist: Callable[[list[_ReferenceEntity]], Awaitable[None]],
        batch_size: int,
        entity_type: type[_ReferenceEntity],
        source_entity_type: str,
    ) -> int:
        rows = await fetch()
        values = [normalize(row) for row in rows]
        for start in range(0, len(values), batch_size):
            await persist(values[start : start + batch_size])

        active_entities = await self._store.load_active_operational_entities(
            entity_type,
            org_id=self._normalizer.org_id,
            provider="pagerduty",
            provider_instance_id=self._normalizer.provider_instance_id,
            source_entity_type=source_entity_type,
        )
        seen_ids = {value.id for value in values}
        tombstones = [
            _reference_tombstone(entity, self._normalizer.observed_at)
            for entity in active_entities
            if entity.id not in seen_ids
        ]
        if tombstones:
            await persist(tombstones)
        return len(values)

    async def _sync_incidents(
        self, options: PagerDutySyncOptions
    ) -> tuple[int, datetime | None]:
        values: list[OperationalIncident] = []
        persisted = 0
        watermark_at: datetime | None = None
        try:
            async for incident in iter_resumable_incidents(self._client, options):
                source_time = incident_source_time(incident)
                if source_time is not None and (
                    watermark_at is None or source_time > watermark_at
                ):
                    watermark_at = source_time
                values.append(self._normalizer.incident(incident))
                if len(values) == options.batch_size:
                    await self._store.insert_operational_incidents(values)
                    persisted += len(values)
                    values = []
        except DATASET_FETCH_ERRORS:
            if values:
                await self._store.insert_operational_incidents(values)
            raise
        if values:
            await self._store.insert_operational_incidents(values)
            persisted += len(values)
        return persisted, watermark_at

    async def _sync_enrichment(
        self,
        options: PagerDutySyncOptions,
        fetch: Callable[[str], AsyncIterator[list[_Source]]],
        normalize: Callable[[_Source, str], _Destination],
        persist: Callable[[list[_Destination]], Awaitable[None]],
    ) -> tuple[int, datetime | None]:
        values: list[_Destination] = []
        persisted = 0
        watermark_at = options.resume_after or options.window_start
        if options.enrichment_cap == 0:
            async for incident in iter_resumable_incidents(self._client, options):
                source_time = incident_source_time(incident)
                if source_time is not None and (
                    watermark_at is None or source_time > watermark_at
                ):
                    watermark_at = source_time
            return persisted, watermark_at
        earliest_undrained_at: datetime | None = None
        async for incident in iter_resumable_incidents(self._client, options):
            incident_id = self._normalizer.incident(incident).id
            child_count = 0
            async for page in fetch(incident.id):
                remaining = options.enrichment_cap - child_count
                for row in page[:remaining]:
                    values.append(normalize(row, incident_id))
                    child_count += 1
                    if len(values) == options.batch_size:
                        await persist(values)
                        persisted += len(values)
                        values = []
                if child_count == options.enrichment_cap:
                    break
            if child_count == options.enrichment_cap:
                source_time = incident_source_time(incident)
                if source_time is not None and (
                    earliest_undrained_at is None or source_time < earliest_undrained_at
                ):
                    earliest_undrained_at = source_time
            source_time = incident_source_time(incident)
            if source_time is not None and (
                watermark_at is None or source_time > watermark_at
            ):
                watermark_at = source_time
        if earliest_undrained_at is not None and (
            watermark_at is None or earliest_undrained_at < watermark_at
        ):
            watermark_at = earliest_undrained_at
        if values:
            await persist(values)
            persisted += len(values)
        return persisted, watermark_at


def _reference_tombstone(
    entity: _ReferenceEntity, observed_at: datetime
) -> _ReferenceEntity:
    source_version_at = max(
        observed_at, entity.source_version_at + timedelta(microseconds=1)
    )
    return replace(
        entity,
        source_version_at=source_version_at,
        observed_at=source_version_at,
        last_synced=source_version_at,
        is_deleted=True,
        deleted_at=source_version_at,
    )
