"""Normalize PagerDuty REST resources into canonical operational entities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict

from dev_health_ops.models.operational import (
    CanonicalOperationalEntity,
    IncidentNote,
    IncidentTimelineEvent,
    OnCallAssignment,
    OnCallSchedule,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
    canonical_operational_id,
)
from dev_health_ops.models.operational import (
    EscalationPolicy as CanonicalEscalationPolicy,
)
from dev_health_ops.models.operational_identity import operational_source_coordinates
from dev_health_ops.providers.pagerduty.models import (
    Alert,
    BusinessService,
    EscalationPolicy,
    Incident,
    LogEntry,
    Note,
    Oncall,
    PagerDutyModel,
    Schedule,
    Service,
    Team,
    User,
)


class OperationalCommonKwargs(TypedDict):
    org_id: str
    provider: str
    provider_instance_id: str
    source_entity_type: str
    external_id: str
    source_version_at: datetime
    source_url: str | None
    observed_at: datetime
    last_synced: datetime


@dataclass(frozen=True, slots=True)
class PagerDutyNormalizer:
    """Map one PagerDuty account's resources using canonical source coordinates."""

    org_id: str
    provider_instance_id: str
    observed_at: datetime

    def service(self, row: Service) -> OperationalService:
        return OperationalService(
            **self._common(OperationalService, row, "service"),
            name=row.name or row.summary or row.id,
            service_type="technical",
            escalation_policy_id=self._reference_id(
                CanonicalEscalationPolicy, row.escalation_policy
            ),
        )

    def business_service(self, row: BusinessService) -> OperationalService:
        return OperationalService(
            **self._common(OperationalService, row, "business_service"),
            name=row.name or row.summary or row.id,
            description=row.description,
            service_type="business",
        )

    def escalation_policy(self, row: EscalationPolicy) -> CanonicalEscalationPolicy:
        return CanonicalEscalationPolicy(
            **self._common(CanonicalEscalationPolicy, row, "escalation_policy"),
            name=row.name or row.summary or row.id,
        )

    def schedule(self, row: Schedule) -> OnCallSchedule:
        return OnCallSchedule(
            **self._common(OnCallSchedule, row, "schedule"),
            name=row.name or row.summary or row.id,
            timezone=row.time_zone,
        )

    def oncall(self, row: Oncall) -> OnCallAssignment:
        return OnCallAssignment(
            **self._common(OnCallAssignment, row, "oncall"),
            schedule_id=self._reference_id(OnCallSchedule, row.schedule),
            user_id=self._reference_id(OperationalUser, row.user),
            escalation_policy_id=self._reference_id(
                CanonicalEscalationPolicy, row.escalation_policy
            ),
            starts_at=row.start,
            ends_at=row.end,
        )

    def user(self, row: User) -> OperationalUser:
        return OperationalUser(
            **self._common(OperationalUser, row, "user"),
            display_name=row.name or row.summary or row.id,
            email=row.email,
        )

    def team(self, row: Team) -> OperationalTeam:
        return OperationalTeam(
            **self._common(OperationalTeam, row, "team"),
            name=row.name or row.summary or row.id,
            description=row.description,
        )

    def incident(self, row: Incident) -> OperationalIncident:
        return OperationalIncident(
            **self._common(OperationalIncident, row, "incident"),
            source_event_at=row.created_at,
            source_event_id=str(row.incident_number) if row.incident_number else None,
            raw_status=row.status,
            raw_severity=row.urgency,
            normalized_status=_incident_status(row.status),
            normalized_severity=_urgency_severity(row.urgency),
            service_id=self._reference_id(OperationalService, row.service),
            service_external_id=row.service.id if row.service else None,
            title=row.title or row.summary or row.id,
            started_at=row.created_at,
            resolved_at=self._source_time(row) if row.status == "resolved" else None,
        )

    def alert(self, row: Alert, incident_id: str) -> OperationalAlert:
        return OperationalAlert(
            **self._common(OperationalAlert, row, "alert"),
            raw_status=row.status,
            raw_severity=row.severity,
            normalized_status=_alert_status(row.status),
            normalized_severity=_severity(row.severity),
            incident_id=incident_id,
            title=row.summary or row.id,
            triggered_at=row.created_at,
            resolved_at=self._source_time(row) if row.status == "resolved" else None,
        )

    def log_entry(self, row: LogEntry, incident_id: str) -> IncidentTimelineEvent:
        return IncidentTimelineEvent(
            **self._common(IncidentTimelineEvent, row, "log_entry"),
            incident_id=incident_id,
            event_type=row.type or "pagerduty_log_entry",
            body=row.summary,
            actor_type="pagerduty",
            occurred_at=row.created_at,
        )

    def note(self, row: Note, incident_id: str) -> IncidentNote:
        return IncidentNote(
            **self._common(IncidentNote, row, "note"),
            incident_id=incident_id,
            body=row.content or "",
            author_user_id=self._reference_id(OperationalUser, row.user),
            created_at=row.created_at,
        )

    def _common(
        self,
        entity_type: type[CanonicalOperationalEntity],
        row: PagerDutyModel,
        source_entity_type: str,
    ) -> OperationalCommonKwargs:
        coordinates = operational_source_coordinates(
            entity_type,
            provider="pagerduty",
            provider_instance_id=self.provider_instance_id,
            external_id=row.id,
        )
        canonical_operational_id(
            self.org_id,
            coordinates.provider,
            coordinates.provider_instance_id,
            coordinates.entity_family,
            coordinates.external_id,
        )
        return {
            "org_id": self.org_id,
            "provider": coordinates.provider,
            "provider_instance_id": coordinates.provider_instance_id,
            "source_entity_type": source_entity_type,
            "external_id": coordinates.external_id,
            "source_version_at": self._source_time(row),
            "source_url": row.html_url or row.self_url,
            "observed_at": self.observed_at,
            "last_synced": self.observed_at,
        }

    def _reference_id(
        self,
        entity_type: type[
            OperationalService
            | CanonicalEscalationPolicy
            | OnCallSchedule
            | OperationalUser
        ],
        row: PagerDutyModel | None,
    ) -> str | None:
        if row is None:
            return None
        coordinates = operational_source_coordinates(
            entity_type,
            provider="pagerduty",
            provider_instance_id=self.provider_instance_id,
            external_id=row.id,
        )
        return canonical_operational_id(
            self.org_id,
            coordinates.provider,
            coordinates.provider_instance_id,
            coordinates.entity_family,
            coordinates.external_id,
        )

    def _source_time(self, row: PagerDutyModel) -> datetime:
        return row.updated_at or row.created_at or self.observed_at


def _incident_status(raw_status: str | None) -> str | None:
    return {
        "triggered": "open",
        "acknowledged": "acknowledged",
        "resolved": "resolved",
    }.get(raw_status or "")


def _alert_status(raw_status: str | None) -> str | None:
    return {
        "triggered": "open",
        "acknowledged": "acknowledged",
        "resolved": "resolved",
    }.get(raw_status or "")


def _urgency_severity(raw_urgency: str | None) -> str | None:
    return {"high": "high", "low": "low"}.get(raw_urgency or "")


def _severity(raw_severity: str | None) -> str | None:
    return {
        "critical": "critical",
        "error": "high",
        "warning": "medium",
        "info": "info",
    }.get(raw_severity or "")
