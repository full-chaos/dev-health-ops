from __future__ import annotations

from datetime import datetime, timezone

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
from dev_health_ops.providers.pagerduty.models import (
    Alert,
    BusinessService,
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
from dev_health_ops.providers.pagerduty.models import (
    EscalationPolicy as PagerDutyEscalationPolicy,
)
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer

SOURCE_TIME = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def _normalizer() -> PagerDutyNormalizer:
    return PagerDutyNormalizer(
        org_id="org-1",
        provider_instance_id="acme",
        observed_at=SOURCE_TIME,
    )


def test_normalizer_maps_every_pagerduty_dataset_to_canonical_entities() -> None:
    normalizer = _normalizer()
    service = normalizer.service(
        Service(id="service-1", name="Payments", updated_at=SOURCE_TIME)
    )
    incident = normalizer.incident(
        Incident(
            id="incident-1",
            title="Payments unavailable",
            status="triggered",
            urgency="high",
            created_at=SOURCE_TIME,
            updated_at=SOURCE_TIME,
            service=PagerDutyModel(id="service-1"),
        )
    )

    assert isinstance(service, OperationalService)
    assert isinstance(
        normalizer.business_service(
            BusinessService(
                id="business-service-1", name="Checkout", updated_at=SOURCE_TIME
            )
        ),
        OperationalService,
    )
    assert isinstance(
        normalizer.escalation_policy(
            PagerDutyEscalationPolicy(
                id="policy-1", name="Primary", updated_at=SOURCE_TIME
            )
        ),
        EscalationPolicy,
    )
    assert isinstance(
        normalizer.schedule(
            Schedule(id="schedule-1", name="Primary", updated_at=SOURCE_TIME)
        ),
        OnCallSchedule,
    )
    assert isinstance(
        normalizer.oncall(
            Oncall(
                id="oncall-1",
                start=SOURCE_TIME,
                end=SOURCE_TIME,
                schedule=PagerDutyModel(id="schedule-1"),
                user=PagerDutyModel(id="user-1"),
            )
        ),
        OnCallAssignment,
    )
    assert isinstance(
        normalizer.user(User(id="user-1", name="Ada", updated_at=SOURCE_TIME)),
        OperationalUser,
    )
    assert isinstance(
        normalizer.team(Team(id="team-1", name="SRE", updated_at=SOURCE_TIME)),
        OperationalTeam,
    )
    assert isinstance(incident, OperationalIncident)
    assert isinstance(
        normalizer.alert(
            Alert(id="alert-1", created_at=SOURCE_TIME, updated_at=SOURCE_TIME),
            incident.id,
        ),
        OperationalAlert,
    )
    assert isinstance(
        normalizer.log_entry(
            LogEntry(id="entry-1", created_at=SOURCE_TIME, updated_at=SOURCE_TIME),
            incident.id,
        ),
        IncidentTimelineEvent,
    )
    assert isinstance(
        normalizer.note(
            Note(
                id="note-1",
                content="Evidence",
                created_at=SOURCE_TIME,
                updated_at=SOURCE_TIME,
            ),
            incident.id,
        ),
        IncidentNote,
    )
    assert service.id != incident.id
