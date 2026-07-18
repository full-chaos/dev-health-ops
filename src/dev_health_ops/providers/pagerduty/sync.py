"""Bounded PagerDuty REST reconciliation through canonical operational sinks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
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
from dev_health_ops.providers.pagerduty.models import (
    Incident,
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
    incident_cap: int = 100
    batch_size: int = 100


@dataclass(frozen=True, slots=True)
class PagerDutySyncResult:
    dataset_key: str
    persisted: int
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
                persisted = await self._sync_incidents(options)
            case "incident-alerts":
                persisted = await self._sync_enrichment(
                    options,
                    self._client.list_incident_alerts,
                    self._normalizer.alert,
                    self._store.insert_operational_alerts,
                )
            case "incident-log-entries":
                persisted = await self._sync_enrichment(
                    options,
                    self._client.list_incident_log_entries,
                    self._normalizer.log_entry,
                    self._store.insert_operational_incident_timeline_events,
                )
            case "incident-notes":
                persisted = await self._sync_enrichment(
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

    async def _sync_incidents(self, options: PagerDutySyncOptions) -> int:
        incidents = await self._incidents(options)
        values = [self._normalizer.incident(row) for row in incidents]
        for start in range(0, len(values), options.batch_size):
            await self._store.insert_operational_incidents(
                values[start : start + options.batch_size]
            )
        return len(values)

    async def _sync_enrichment(
        self,
        options: PagerDutySyncOptions,
        fetch: Callable[[str], Awaitable[list[_Source]]],
        normalize: Callable[[_Source, str], _Destination],
        persist: Callable[[list[_Destination]], Awaitable[None]],
    ) -> int:
        values: list[_Destination] = []
        persisted = 0
        for incident in await self._incidents(options):
            incident_id = self._normalizer.incident(incident).id
            values.extend(
                normalize(row, incident_id) for row in await fetch(incident.id)
            )
            if len(values) >= options.batch_size:
                await persist(values)
                persisted += len(values)
                values = []
        if values:
            await persist(values)
            persisted += len(values)
        return persisted

    async def _incidents(self, options: PagerDutySyncOptions) -> Sequence[Incident]:
        params: dict[str, str] = {}
        if options.window_start is not None:
            params["since"] = options.window_start.isoformat()
        if options.window_end is not None:
            params["until"] = options.window_end.isoformat()
        return (await self._client.list_incidents(params=params))[
            : options.incident_cap
        ]
