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
    ) -> list[T]:
        """Load active canonical operational entities for the requested provider source."""
        raise NotImplementedError

    async def insert_operational_services(
        self, values: list[OperationalService]
    ) -> None:
        """Persist operational services."""

    async def insert_operational_incidents(
        self, values: list[OperationalIncident]
    ) -> None:
        """Persist operational incidents."""

    async def insert_operational_alerts(self, values: list[OperationalAlert]) -> None:
        """Persist operational alerts."""

    async def insert_operational_incident_timeline_events(
        self, values: list[IncidentTimelineEvent]
    ) -> None:
        """Persist incident timeline events."""

    async def insert_operational_incident_notes(
        self, values: list[IncidentNote]
    ) -> None:
        """Persist incident notes."""

    async def insert_operational_escalation_policies(
        self, values: list[EscalationPolicy]
    ) -> None:
        """Persist escalation policies."""

    async def insert_operational_on_call_schedules(
        self, values: list[OnCallSchedule]
    ) -> None:
        """Persist on-call schedules."""

    async def insert_operational_on_call_assignments(
        self, values: list[OnCallAssignment]
    ) -> None:
        """Persist on-call assignments."""

    async def insert_operational_teams(self, values: list[OperationalTeam]) -> None:
        """Persist operational teams."""

    async def insert_operational_users(self, values: list[OperationalUser]) -> None:
        """Persist operational users."""
