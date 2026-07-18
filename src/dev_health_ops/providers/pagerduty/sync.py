"""Bounded PagerDuty REST reconciliation through canonical operational sinks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, TypeVar

from dev_health_ops.models.operational import (
    EscalationPolicy,
    IncidentNote,
    IncidentTimelineEvent,
    OnCallAssignment,
    OnCallSchedule,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
)
from dev_health_ops.providers.pagerduty.client import PagerDutyClient
from dev_health_ops.providers.pagerduty.incident_cursor import (
    incident_source_time,
    iter_resumable_incidents,
)
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer


class PagerDutyOperationalStore(Protocol):
    async def insert_operational_services(
        self, values: list[OperationalService]
    ) -> None: ...
    async def insert_operational_incidents(
        self, values: list[OperationalIncident]
    ) -> None: ...
    async def insert_operational_alerts(
        self, values: list[OperationalAlert]
    ) -> None: ...
    async def insert_operational_incident_timeline_events(
        self, values: list[IncidentTimelineEvent]
    ) -> None: ...
    async def insert_operational_incident_notes(
        self, values: list[IncidentNote]
    ) -> None: ...
    async def insert_operational_escalation_policies(
        self, values: list[EscalationPolicy]
    ) -> None: ...
    async def insert_operational_on_call_schedules(
        self, values: list[OnCallSchedule]
    ) -> None: ...
    async def insert_operational_on_call_assignments(
        self, values: list[OnCallAssignment]
    ) -> None: ...
    async def insert_operational_teams(self, values: list[OperationalTeam]) -> None: ...
    async def insert_operational_users(self, values: list[OperationalUser]) -> None: ...


@dataclass(frozen=True, slots=True)
class PagerDutySyncOptions:
    dataset_key: str
    window_start: datetime | None
    window_end: datetime | None
    resume_after: datetime | None = None
    incident_cap: int = 100
    batch_size: int = 100
    enrichment_cap: int = 100

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("PagerDuty batch_size must be positive")
        if self.incident_cap < 0 or self.enrichment_cap < 0:
            raise ValueError("PagerDuty caps must not be negative")


@dataclass(frozen=True, slots=True)
class PagerDutySyncResult:
    dataset_key: str
    persisted: int
    watermark_at: datetime | None
    degraded: bool
    observations: tuple[dict[str, object], ...]


_Source = TypeVar("_Source")
_Destination = TypeVar("_Destination")


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
        watermark_at: datetime | None = None
        match options.dataset_key:
            case "services":
                persisted = await self._sync(
                    self._client.list_services,
                    self._normalizer.service,
                    self._store.insert_operational_services,
                    options.batch_size,
                )
            case "business-services":
                persisted = await self._sync(
                    self._client.list_business_services,
                    self._normalizer.business_service,
                    self._store.insert_operational_services,
                    options.batch_size,
                )
            case "escalation-policies":
                persisted = await self._sync(
                    self._client.list_escalation_policies,
                    self._normalizer.escalation_policy,
                    self._store.insert_operational_escalation_policies,
                    options.batch_size,
                )
            case "schedules":
                persisted = await self._sync(
                    self._client.list_schedules,
                    self._normalizer.schedule,
                    self._store.insert_operational_on_call_schedules,
                    options.batch_size,
                )
            case "on-calls":
                persisted = await self._sync(
                    self._client.list_oncalls,
                    self._normalizer.oncall,
                    self._store.insert_operational_on_call_assignments,
                    options.batch_size,
                )
            case "users":
                persisted = await self._sync(
                    self._client.list_users,
                    self._normalizer.user,
                    self._store.insert_operational_users,
                    options.batch_size,
                )
            case "teams":
                persisted = await self._sync(
                    self._client.list_teams,
                    self._normalizer.team,
                    self._store.insert_operational_teams,
                    options.batch_size,
                )
            case "incidents":
                persisted, watermark_at = await self._sync_incidents(options)
            case "incident-alerts":
                persisted, watermark_at = await self._sync_enrichment(
                    options,
                    self._client.list_incident_alerts,
                    self._normalizer.alert,
                    self._store.insert_operational_alerts,
                )
            case "incident-log-entries":
                persisted, watermark_at = await self._sync_enrichment(
                    options,
                    self._client.list_incident_log_entries,
                    self._normalizer.log_entry,
                    self._store.insert_operational_incident_timeline_events,
                )
            case "incident-notes":
                persisted, watermark_at = await self._sync_enrichment(
                    options,
                    self._client.list_incident_notes,
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

    async def _sync_incidents(
        self, options: PagerDutySyncOptions
    ) -> tuple[int, datetime | None]:
        values: list[OperationalIncident] = []
        persisted = 0
        watermark_at: datetime | None = None
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
        if values:
            await self._store.insert_operational_incidents(values)
            persisted += len(values)
        return persisted, watermark_at

    async def _sync_enrichment(
        self,
        options: PagerDutySyncOptions,
        fetch: Callable[[str], Awaitable[list[_Source]]],
        normalize: Callable[[_Source, str], _Destination],
        persist: Callable[[list[_Destination]], Awaitable[None]],
    ) -> tuple[int, datetime | None]:
        values: list[_Destination] = []
        persisted = 0
        watermark_at: datetime | None = None
        async for incident in iter_resumable_incidents(self._client, options):
            source_time = incident_source_time(incident)
            if source_time is not None and (
                watermark_at is None or source_time > watermark_at
            ):
                watermark_at = source_time
            incident_id = self._normalizer.incident(incident).id
            for row in (await fetch(incident.id))[: options.enrichment_cap]:
                values.append(normalize(row, incident_id))
                if len(values) == options.batch_size:
                    await persist(values)
                    persisted += len(values)
                    values = []
        if values:
            await persist(values)
            persisted += len(values)
        return persisted, watermark_at
