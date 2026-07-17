"""Provider-neutral operational entities persisted in ClickHouse.

The idempotency key for every entity is ``(org_id, id)``. ``id`` is a
deterministic SHA-256 digest of the immutable source identity seed
``(org_id, provider_instance_id, source_entity_type, external_id)``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import Final


def canonical_operational_id(
    org_id: str,
    provider_instance_id: str,
    source_entity_type: str,
    external_id: str,
) -> str:
    """Return the stable internal id for an immutable provider identity seed."""
    seed = json.dumps(
        [org_id, provider_instance_id, source_entity_type, external_id],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class CanonicalOperationalEntity:
    """Common immutable source identity and normalization contract."""

    org_id: str
    provider: str
    provider_instance_id: str
    source_entity_type: str
    external_id: str
    id: str = field(init=False)
    source_url: str | None = None
    source_event_at: datetime | None = None
    observed_at: datetime = field(default_factory=_utcnow)
    last_synced: datetime = field(default_factory=_utcnow)
    raw_status: str | None = None
    raw_severity: str | None = None
    raw_priority: str | None = None
    normalized_status: str | None = None
    normalized_severity: str | None = None
    normalized_priority: str | None = None
    relationship_provenance: str | None = None
    relationship_confidence: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "id",
            canonical_operational_id(
                self.org_id,
                self.provider_instance_id,
                self.source_entity_type,
                self.external_id,
            ),
        )


@dataclass(frozen=True, slots=True)
class OperationalService(CanonicalOperationalEntity):
    name: str = ""
    description: str | None = None
    service_type: str | None = None
    owning_team_id: str | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperationalIncident(CanonicalOperationalEntity):
    service_id: str | None = None
    service_external_id: str | None = None
    title: str = ""
    description: str | None = None
    started_at: datetime | None = None
    resolved_at: datetime | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperationalAlert(CanonicalOperationalEntity):
    service_id: str | None = None
    incident_id: str | None = None
    title: str = ""
    description: str | None = None
    triggered_at: datetime | None = None
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class IncidentTimelineEvent(CanonicalOperationalEntity):
    incident_id: str = ""
    event_type: str = ""
    body: str | None = None
    actor_user_id: str | None = None
    occurred_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class IncidentNote(CanonicalOperationalEntity):
    incident_id: str = ""
    body: str = ""
    author_user_id: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class IncidentResponder(CanonicalOperationalEntity):
    incident_id: str = ""
    user_id: str | None = None
    responder_name: str | None = None
    role: str | None = None
    responded_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EscalationPolicy(CanonicalOperationalEntity):
    name: str = ""
    description: str | None = None
    escalation_level: int = 0


@dataclass(frozen=True, slots=True)
class OnCallSchedule(CanonicalOperationalEntity):
    name: str = ""
    description: str | None = None
    timezone: str | None = None


@dataclass(frozen=True, slots=True)
class OnCallAssignment(CanonicalOperationalEntity):
    schedule_id: str = ""
    user_id: str | None = None
    starts_at: datetime | None = None
    ends_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperationalTeam(CanonicalOperationalEntity):
    name: str = ""
    description: str | None = None


@dataclass(frozen=True, slots=True)
class OperationalUser(CanonicalOperationalEntity):
    display_name: str = ""
    email: str | None = None


@dataclass(frozen=True, slots=True)
class ServiceRepositoryMapping(CanonicalOperationalEntity):
    service_id: str = ""
    repo_id: str | None = None
    repo_full_name: str | None = None
    mapping_kind: str | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class OperationalBatch:
    """Separate ingestion envelope for canonical operational entities."""

    org_id: str
    provider: str
    provider_instance_id: str
    observed_at: datetime = field(default_factory=_utcnow)
    services: tuple[OperationalService, ...] = ()
    incidents: tuple[OperationalIncident, ...] = ()
    alerts: tuple[OperationalAlert, ...] = ()
    incident_timeline_events: tuple[IncidentTimelineEvent, ...] = ()
    incident_notes: tuple[IncidentNote, ...] = ()
    incident_responders: tuple[IncidentResponder, ...] = ()
    escalation_policies: tuple[EscalationPolicy, ...] = ()
    on_call_schedules: tuple[OnCallSchedule, ...] = ()
    on_call_assignments: tuple[OnCallAssignment, ...] = ()
    teams: tuple[OperationalTeam, ...] = ()
    users: tuple[OperationalUser, ...] = ()
    service_repository_mappings: tuple[ServiceRepositoryMapping, ...] = ()


OPERATIONAL_ENTITY_TABLES: Final[dict[type[CanonicalOperationalEntity], str]] = {
    OperationalService: "operational_services",
    OperationalIncident: "operational_incidents",
    OperationalAlert: "operational_alerts",
    IncidentTimelineEvent: "operational_incident_timeline_events",
    IncidentNote: "operational_incident_notes",
    IncidentResponder: "operational_incident_responders",
    EscalationPolicy: "operational_escalation_policies",
    OnCallSchedule: "operational_on_call_schedules",
    OnCallAssignment: "operational_on_call_assignments",
    OperationalTeam: "operational_teams",
    OperationalUser: "operational_users",
    ServiceRepositoryMapping: "operational_service_repository_mappings",
}


def operational_columns(
    entity_type: type[CanonicalOperationalEntity],
) -> tuple[str, ...]:
    """Return the ClickHouse insert columns in dataclass declaration order."""
    return tuple(item.name for item in fields(entity_type))
