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


class Store:
    def __init__(self, current: CanonicalOperationalEntity) -> None:
        self.current = current
        self.services: list[OperationalService] = []
        self.incidents: list[OperationalIncident] = []

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
        return [self.current] if type(self.current) is entity_type else []

    async def insert_operational_services(
        self, values: list[OperationalService]
    ) -> None:
        self.services.extend(values)

    async def insert_operational_incidents(
        self, values: list[OperationalIncident]
    ) -> None:
        self.incidents.extend(values)

    async def insert_operational_alerts(self, values: list[OperationalAlert]) -> None:
        return None

    async def insert_operational_incident_timeline_events(
        self, values: list[IncidentTimelineEvent]
    ) -> None:
        return None

    async def insert_operational_incident_notes(
        self, values: list[IncidentNote]
    ) -> None:
        return None

    async def insert_operational_incident_responders(
        self, values: list[IncidentResponder]
    ) -> None:
        return None

    async def insert_operational_users(self, values: list[OperationalUser]) -> None:
        return None

    async def insert_operational_teams(self, values: list[OperationalTeam]) -> None:
        return None


class Client:
    async def get_incident(self, incident_id: str) -> Incident:
        raise AssertionError(incident_id)


def _incident_event(occurred_at: datetime) -> PagerDutyV3Webhook:
    return PagerDutyV3Webhook.model_validate(
        {
            "event": {
                "id": "event-1",
                "event_type": "incident.triggered",
                "occurred_at": occurred_at.isoformat(),
                "data": {
                    "id": "incident-1",
                    "title": "Payments unavailable",
                    "status": "triggered",
                    "created_at": occurred_at.isoformat(),
                },
            }
        }
    )


@pytest.mark.anyio
async def test_out_of_order_incident_does_not_regress_current_entity() -> None:
    occurred_at = datetime(2026, 7, 17, tzinfo=UTC)
    store = Store(
        OperationalIncident(
            org_id="org-1",
            provider="pagerduty",
            provider_instance_id="acme",
            source_entity_type="incident",
            external_id="incident-1",
            source_version_at=datetime(2026, 7, 18, tzinfo=UTC),
            title="Newer incident",
        )
    )

    processed = await reconcile_pagerduty_webhook(
        webhook=_incident_event(occurred_at),
        org_id="org-1",
        provider_instance_id="acme",
        received_at=datetime(2026, 7, 19, tzinfo=UTC),
        store=store,
        client=Client(),
    )

    assert processed is False
    assert store.incidents == []


@pytest.mark.anyio
async def test_service_delete_writes_versioned_tombstone() -> None:
    occurred_at = datetime(2026, 7, 18, tzinfo=UTC)
    store = Store(
        OperationalService(
            org_id="org-1",
            provider="pagerduty",
            provider_instance_id="acme",
            source_entity_type="service",
            external_id="service-1",
            source_version_at=datetime(2026, 7, 17, tzinfo=UTC),
            name="Payments",
        )
    )
    webhook = PagerDutyV3Webhook.model_validate(
        {
            "event": {
                "id": "event-2",
                "event_type": "service.deleted",
                "occurred_at": occurred_at.isoformat(),
                "data": {"id": "service-1", "name": "Payments"},
            }
        }
    )

    processed = await reconcile_pagerduty_webhook(
        webhook=webhook,
        org_id="org-1",
        provider_instance_id="acme",
        received_at=occurred_at,
        store=store,
        client=Client(),
    )

    assert processed is True
    assert store.services[0].is_deleted is True
    assert store.services[0].source_version_at == occurred_at


@pytest.mark.anyio
async def test_stale_service_update_cannot_resurrect_a_tombstone() -> None:
    store = Store(
        OperationalService(
            org_id="org-1",
            provider="pagerduty",
            provider_instance_id="acme",
            source_entity_type="service",
            external_id="service-1",
            source_version_at=datetime(2026, 7, 18, tzinfo=UTC),
            name="Payments",
            is_deleted=True,
            deleted_at=datetime(2026, 7, 18, tzinfo=UTC),
        )
    )
    webhook = PagerDutyV3Webhook.model_validate(
        {
            "event": {
                "id": "event-3",
                "event_type": "service.updated",
                "occurred_at": "2026-07-17T00:00:00Z",
                "data": {"id": "service-1", "name": "Payments"},
            }
        }
    )

    processed = await reconcile_pagerduty_webhook(
        webhook=webhook,
        org_id="org-1",
        provider_instance_id="acme",
        received_at=datetime(2026, 7, 19, tzinfo=UTC),
        store=store,
        client=Client(),
    )

    assert processed is False
    assert store.services == []
