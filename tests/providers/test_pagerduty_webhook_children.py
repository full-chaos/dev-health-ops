from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dev_health_ops.api.webhooks.pagerduty_models import PagerDutyV3Webhook
from dev_health_ops.models.operational import (
    CanonicalOperationalEntity,
    IncidentNote,
    IncidentResponder,
    IncidentTimelineEvent,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
)
from dev_health_ops.providers.pagerduty.models import Incident
from dev_health_ops.providers.pagerduty.webhooks import reconcile_pagerduty_webhook


class _Store:
    def __init__(self) -> None:
        self.incidents: list[OperationalIncident] = []
        self.notes: list[IncidentNote] = []
        self.responders: list[IncidentResponder] = []
        self.timeline_events: list[IncidentTimelineEvent] = []
        self.users: list[OperationalUser] = []
        self.teams: list[OperationalTeam] = []

    async def load_active_operational_entities(
        self,
        entity_type: type[CanonicalOperationalEntity],
        *,
        org_id: str,
        provider: str,
        provider_instance_id: str,
        source_entity_type: str,
        include_deleted: bool = False,
    ) -> list[CanonicalOperationalEntity]:
        return []

    async def insert_operational_services(
        self, values: list[OperationalService]
    ) -> None:
        return None

    async def insert_operational_incidents(
        self, values: list[OperationalIncident]
    ) -> None:
        self.incidents.extend(values)

    async def insert_operational_alerts(self, values: list[OperationalAlert]) -> None:
        return None

    async def insert_operational_incident_timeline_events(
        self, values: list[IncidentTimelineEvent]
    ) -> None:
        self.timeline_events.extend(values)

    async def insert_operational_incident_notes(
        self, values: list[IncidentNote]
    ) -> None:
        self.notes.extend(values)

    async def insert_operational_incident_responders(
        self, values: list[IncidentResponder]
    ) -> None:
        self.responders.extend(values)

    async def insert_operational_users(self, values: list[OperationalUser]) -> None:
        self.users.extend(values)

    async def insert_operational_teams(self, values: list[OperationalTeam]) -> None:
        self.teams.extend(values)


class _Client:
    async def get_incident(self, incident_id: str) -> Incident:
        raise AssertionError(incident_id)


_OCCURRED_AT = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)


def _webhook(event_type: str, data: dict[str, object]) -> PagerDutyV3Webhook:
    return PagerDutyV3Webhook.model_validate(
        {
            "event": {
                "id": f"event-{event_type}",
                "event_type": event_type,
                "occurred_at": _OCCURRED_AT.isoformat(),
                "data": data,
            }
        }
    )


def _incident() -> dict[str, str]:
    return {
        "id": "incident-1",
        "title": "Payments unavailable",
        "status": "triggered",
        "created_at": _OCCURRED_AT.isoformat(),
    }


@pytest.mark.anyio
async def test_responder_webhook_materializes_responder_user_and_team() -> None:
    store = _Store()
    webhook = _webhook(
        "incident.responder.added",
        {
            "incident": _incident(),
            "responder": {
                "id": "responder-1",
                "name": "Ava",
                "role": "responder",
                "user": {"id": "user-1", "name": "Ava"},
                "team": {"id": "team-1", "name": "Operations"},
            },
        },
    )

    processed = await reconcile_pagerduty_webhook(
        webhook=webhook,
        org_id="org-1",
        provider_instance_id="acme",
        received_at=_OCCURRED_AT,
        store=store,
        client=_Client(),
    )

    assert processed is True
    assert store.responders[0].source_event_id == webhook.event.id
    assert store.responders[0].source_version_at == _OCCURRED_AT
    assert store.users[0].source_event_id == webhook.event.id
    assert store.teams[0].source_event_id == webhook.event.id


@pytest.mark.anyio
async def test_annotation_webhook_materializes_incident_note() -> None:
    store = _Store()
    webhook = _webhook(
        "incident.annotated",
        {
            "incident": _incident(),
            "note": {
                "id": "note-1",
                "content": "Investigating the outage",
                "created_at": _OCCURRED_AT.isoformat(),
                "user": {"id": "user-1", "name": "Ava"},
            },
        },
    )

    processed = await reconcile_pagerduty_webhook(
        webhook=webhook,
        org_id="org-1",
        provider_instance_id="acme",
        received_at=_OCCURRED_AT,
        store=store,
        client=_Client(),
    )

    assert processed is True
    assert store.notes[0].source_event_id == webhook.event.id
    assert store.notes[0].source_version_at == _OCCURRED_AT
    assert store.notes[0].body == "Investigating the outage"


@pytest.mark.anyio
async def test_status_update_webhook_materializes_timeline_event() -> None:
    store = _Store()
    webhook = _webhook(
        "incident.status_update_published",
        {
            "incident": _incident(),
            "status_update": {
                "id": "status-update-1",
                "summary": "Mitigation is in progress",
                "created_at": _OCCURRED_AT.isoformat(),
            },
        },
    )

    processed = await reconcile_pagerduty_webhook(
        webhook=webhook,
        org_id="org-1",
        provider_instance_id="acme",
        received_at=_OCCURRED_AT,
        store=store,
        client=_Client(),
    )

    assert processed is True
    assert store.timeline_events[0].source_event_id == webhook.event.id
    assert store.timeline_events[0].source_version_at == _OCCURRED_AT
    assert store.timeline_events[0].body == "Mitigation is in progress"
