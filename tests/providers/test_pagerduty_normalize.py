from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

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
_PAYLOADS_PATH = Path(__file__).parents[1] / "fixtures/pagerduty/rest_v2_payloads.json"
REAL_PAGERDUTY_PAYLOADS = json.loads(_PAYLOADS_PATH.read_text())


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
                escalation_policy=PagerDutyModel(id="policy-1"),
                escalation_level=1,
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


@pytest.mark.parametrize(
    ("model", "payload_key", "unknown_key"),
    [
        (Service, "services", "auto_resolve_timeout"),
        (BusinessService, "business_services", "point_of_contact"),
        (PagerDutyEscalationPolicy, "escalation_policies", "escalation_rules"),
        (Schedule, "schedules", "schedule_layers"),
        (Oncall, "oncalls", "override"),
        (User, "users", "contact_methods"),
        (Team, "teams", "parent"),
        (Incident, "incidents", "assignments"),
        (Alert, "incident_alerts", "integration"),
        (LogEntry, "log_entries", "agent"),
        (Note, "notes", "audit_trail"),
    ],
)
def test_models_retain_real_pagerduty_wire_payloads(
    model: type[PagerDutyModel], payload_key: str, unknown_key: str
) -> None:
    payload = REAL_PAGERDUTY_PAYLOADS[payload_key]

    parsed = model.model_validate(payload)

    assert parsed.raw == payload
    assert parsed.raw[unknown_key] == payload[unknown_key]


def test_oncall_without_global_id_uses_stable_composite_external_id() -> None:
    oncall = Oncall.model_validate(REAL_PAGERDUTY_PAYLOADS["oncalls"])

    first = _normalizer().oncall(oncall)
    second = _normalizer().oncall(oncall)

    assert first.external_id == (
        "PESCAL1|PSCHED1|PUSER01|1|2026-07-17T08:00:00+00:00|2026-07-17T16:00:00+00:00"
    )
    assert first.id == second.id


def test_oncall_rejects_missing_stable_composite_dimension() -> None:
    payload = {
        **REAL_PAGERDUTY_PAYLOADS["oncalls"],
        "escalation_policy": {
            **REAL_PAGERDUTY_PAYLOADS["oncalls"]["escalation_policy"],
            "id": "",
        },
    }

    with pytest.raises(ValueError, match="requires stable dimensions"):
        _normalizer().oncall(Oncall.model_validate(payload))


def test_incident_preserves_resolution_time_and_raw_priority() -> None:
    incident = _normalizer().incident(
        Incident.model_validate(REAL_PAGERDUTY_PAYLOADS["incidents"])
    )

    assert incident.resolved_at == datetime(2026, 7, 17, 11, 0, tzinfo=timezone.utc)
    assert incident.resolved_at != datetime(2026, 7, 17, 12, 30, tzinfo=timezone.utc)
    assert incident.raw_status == "resolved"
    assert incident.raw_severity == "high"
    assert incident.raw_priority == "P1"
    assert incident.normalized_priority == "high"


def test_service_normalization_does_not_compute_discarded_canonical_id() -> None:
    service = Service.model_validate(
        {
            "id": "service-1",
            "name": "Payments",
            "updated_at": "2026-07-17T12:00:00Z",
        }
    )

    with patch(
        "dev_health_ops.providers.pagerduty.normalize.canonical_operational_id"
    ) as canonical_id:
        _normalizer().service(service)

    canonical_id.assert_not_called()
