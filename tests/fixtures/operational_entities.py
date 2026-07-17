from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from dev_health_ops.models.operational import (
    CanonicalOperationalEntity,
    EscalationPolicy,
    IncidentNote,
    IncidentResponder,
    IncidentTimelineEvent,
    OnCallAssignment,
    OnCallSchedule,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
    ServiceRepositoryMapping,
)

_AT = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def all_operational_entities() -> tuple[CanonicalOperationalEntity, ...]:
    """Return every canonical entity plus native and external-push service paths."""
    service = OperationalService(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="native.service",
        external_id="payments",
        source_version_at=_AT,
        name="Payments",
        escalation_policy_id="policy-1",
    )
    pushed_service = OperationalService(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="external_push.service",
        external_id="payments",
        source_version_at=_AT,
        source_id=UUID("00000000-0000-0000-0000-000000000001"),
        name="Payments",
        escalation_policy_id="policy-1",
    )
    incident = OperationalIncident(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="incident",
        external_id="incident-1",
        source_version_at=_AT,
        service_id=service.id,
        escalation_policy_id="policy-1",
        title="Payments incident",
        started_at=_AT,
    )
    alert = OperationalAlert(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="alert",
        external_id="alert-1",
        source_version_at=_AT,
        service_id=service.id,
        incident_id=incident.id,
        title="Payments alert",
        triggered_at=_AT,
    )
    event = IncidentTimelineEvent(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="timeline_event",
        external_id="event-1",
        source_version_at=_AT,
        incident_id=incident.id,
        event_type="status_changed",
        actor_type="user",
        actor_id="user-1",
        occurred_at=_AT,
    )
    note = IncidentNote(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="note",
        external_id="note-1",
        source_version_at=_AT,
        incident_id=incident.id,
        body="Sanitized incident note",
        created_at=_AT,
    )
    responder = IncidentResponder(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="responder",
        external_id="responder-1",
        source_version_at=_AT,
        incident_id=incident.id,
        user_id="user-1",
        requested_at=_AT,
        assigned_at=_AT,
    )
    policy = EscalationPolicy(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="escalation_policy",
        external_id="policy-1",
        source_version_at=_AT,
        name="Primary",
    )
    schedule = OnCallSchedule(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="schedule",
        external_id="schedule-1",
        source_version_at=_AT,
        name="Primary",
        timezone="UTC",
    )
    assignment = OnCallAssignment(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="assignment",
        external_id="assignment-1",
        source_version_at=_AT,
        escalation_policy_id=policy.id,
        escalation_level=1,
        starts_at=_AT,
    )
    team = OperationalTeam(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="team",
        external_id="team-1",
        source_version_at=_AT,
        name="Operations",
    )
    user = OperationalUser(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="user",
        external_id="user-1",
        source_version_at=_AT,
        display_name="Responder",
    )
    mapping = ServiceRepositoryMapping(
        org_id="org-example",
        provider="pagerduty",
        provider_instance_id="pd-example",
        source_entity_type="service_repository_mapping",
        external_id="payments-repository",
        source_version_at=_AT,
        service_id=service.id,
        repo_id=UUID("00000000-0000-0000-0000-000000000002"),
        rule_id="catalog-link",
    )
    return (
        service,
        pushed_service,
        incident,
        alert,
        event,
        note,
        responder,
        policy,
        schedule,
        assignment,
        team,
        user,
        mapping,
    )
