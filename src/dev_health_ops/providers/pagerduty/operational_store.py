from __future__ import annotations

from typing import Protocol, TypeVar

from dev_health_ops.models.operational import (
    CanonicalOperationalEntity,
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

T = TypeVar("T", bound=CanonicalOperationalEntity)


class PagerDutyOperationalStore(Protocol):
    async def load_active_operational_entities(
        self,
        entity_type: type[T],
        *,
        org_id: str,
        provider: str,
        provider_instance_id: str,
        source_entity_type: str,
    ) -> list[T]: ...

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
