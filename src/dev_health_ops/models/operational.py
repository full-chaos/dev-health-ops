"""Provider-neutral operational entities persisted in ClickHouse.

The idempotency key for every entity is ``(org_id, id)``. ``id`` is a
deterministic SHA-256 digest of the immutable provider identity seed.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import ClassVar, Final
from uuid import UUID


def canonical_operational_id(
    org_id: str,
    provider: str,
    provider_instance_id: str,
    entity_family: str,
    external_id: str,
) -> str:
    """Return the stable internal id for an immutable provider identity seed."""
    for component, value in (
        ("org_id", org_id),
        ("provider", provider),
        ("provider_instance_id", provider_instance_id),
        ("entity_family", entity_family),
        ("external_id", external_id),
    ):
        if not value:
            raise OperationalContractError(component, "non-empty", value)
    seed = json.dumps(
        [org_id, provider, provider_instance_id, entity_family, external_id],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class OperationalContractError(ValueError):
    """Raised when a canonical operational row violates its immutable contract."""

    def __init__(self, field_name: str, expected: str, actual: str) -> None:
        super().__init__(f"{field_name} must be {expected}, got {actual!r}")


CANONICAL_STATUSES: Final[frozenset[str]] = frozenset(
    {"active", "open", "acknowledged", "resolved", "closed", "suppressed"}
)
CANONICAL_SEVERITIES: Final[frozenset[str]] = frozenset(
    {"critical", "high", "medium", "low", "info"}
)
CANONICAL_PRIORITIES: Final[frozenset[str]] = frozenset(
    {"critical", "high", "medium", "low"}
)


@dataclass(frozen=True, slots=True)
class CanonicalOperationalEntity:
    """Common immutable source identity and normalization contract."""

    entity_family: ClassVar[str] = ""

    org_id: str
    provider: str
    provider_instance_id: str
    source_entity_type: str
    external_id: str
    source_version_at: datetime
    id: str = field(init=False)
    source_id: UUID | None = None
    source_url: str | None = None
    source_event_at: datetime | None = None
    source_event_id: str | None = None
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
        for value, vocabulary, field_name in (
            (self.normalized_status, CANONICAL_STATUSES, "normalized_status"),
            (self.normalized_severity, CANONICAL_SEVERITIES, "normalized_severity"),
            (self.normalized_priority, CANONICAL_PRIORITIES, "normalized_priority"),
        ):
            if value is not None and value not in vocabulary:
                raise OperationalContractError(
                    field_name, "a canonical vocabulary value", value
                )
        if (
            self.relationship_confidence is not None
            and not 0 <= self.relationship_confidence <= 1
        ):
            raise OperationalContractError(
                "relationship_confidence",
                "between 0 and 1",
                str(self.relationship_confidence),
            )
        object.__setattr__(
            self,
            "id",
            canonical_operational_id(
                self.org_id,
                self.provider,
                self.provider_instance_id,
                self.entity_family,
                self.external_id,
            ),
        )


@dataclass(frozen=True, slots=True)
class OperationalService(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_service"
    name: str = ""
    description: str | None = None
    service_type: str | None = None
    owning_team_id: str | None = None
    escalation_policy_id: str | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperationalIncident(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_incident"
    service_id: str | None = None
    service_external_id: str | None = None
    escalation_policy_id: str | None = None
    title: str = ""
    description: str | None = None
    started_at: datetime | None = None
    resolved_at: datetime | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperationalAlert(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_alert"
    service_id: str | None = None
    incident_id: str | None = None
    title: str = ""
    description: str | None = None
    triggered_at: datetime | None = None
    acknowledged_at: datetime | None = None
    resolved_at: datetime | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class IncidentTimelineEvent(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_incident_timeline_event"
    incident_id: str = ""
    event_type: str = ""
    body: str | None = None
    actor_type: str | None = None
    actor_id: str | None = None
    occurred_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class IncidentNote(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_incident_note"
    incident_id: str = ""
    body: str = ""
    author_user_id: str | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class IncidentResponder(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_incident_responder"
    incident_id: str = ""
    user_id: str | None = None
    responder_name: str | None = None
    role: str | None = None
    responder_assignment_id: str | None = None
    requested_at: datetime | None = None
    assigned_at: datetime | None = None
    acknowledged_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EscalationPolicy(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_escalation_policy"
    name: str = ""
    description: str | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OnCallSchedule(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_on_call_schedule"
    name: str = ""
    description: str | None = None
    timezone: str | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OnCallAssignment(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_on_call_assignment"

    schedule_id: str | None = None
    user_id: str | None = None
    escalation_policy_id: str | None = None
    escalation_level: int | None = None
    starts_at: datetime | None = None
    ends_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperationalTeam(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_team"
    name: str = ""
    description: str | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OperationalUser(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_user"
    display_name: str = ""
    email: str | None = None
    is_deleted: bool = False
    deleted_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ServiceRepositoryMapping(CanonicalOperationalEntity):
    entity_family: ClassVar[str] = "operational_service_repository_mapping"

    service_id: str = ""
    repo_id: UUID | None = None
    repo_full_name: str | None = None
    repo_provider: str | None = None
    mapping_kind: str | None = None
    rule_id: str | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    is_active: bool = True

    def __post_init__(self) -> None:
        CanonicalOperationalEntity.__post_init__(self)
        if self.repo_id is None and (not self.repo_provider or not self.repo_full_name):
            raise OperationalContractError(
                "repository_mapping",
                "a repo_id or repo_provider plus repo_full_name",
                "unresolved",
            )


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

    def __post_init__(self) -> None:
        for entities in (
            self.services,
            self.incidents,
            self.alerts,
            self.incident_timeline_events,
            self.incident_notes,
            self.incident_responders,
            self.escalation_policies,
            self.on_call_schedules,
            self.on_call_assignments,
            self.teams,
            self.users,
            self.service_repository_mappings,
        ):
            for entity in entities:
                if entity.org_id != self.org_id:
                    raise OperationalContractError("org_id", self.org_id, entity.org_id)
                if entity.provider != self.provider:
                    raise OperationalContractError(
                        "provider", self.provider, entity.provider
                    )
                if entity.provider_instance_id != self.provider_instance_id:
                    raise OperationalContractError(
                        "provider_instance_id",
                        self.provider_instance_id,
                        entity.provider_instance_id,
                    )


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
