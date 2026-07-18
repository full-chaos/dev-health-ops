"""Canonical PagerDuty webhook reconciliation."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Protocol, TypeVar

from pydantic import JsonValue

from dev_health_ops.api.webhooks.pagerduty_models import (
    PagerDutyEventType,
    PagerDutyV3Webhook,
)
from dev_health_ops.models.operational import (
    CanonicalOperationalEntity,
    IncidentNote,
    IncidentTimelineEvent,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
)
from dev_health_ops.providers.pagerduty.models import Incident, Service
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer


class PagerDutyWebhookStore(Protocol):
    async def load_active_operational_entities(
        self,
        entity_type: type[CanonicalOperationalEntity],
        *,
        org_id: str,
        provider: str,
        provider_instance_id: str,
        source_entity_type: str,
    ) -> list[CanonicalOperationalEntity]: ...

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


class PagerDutyIncidentClient(Protocol):
    async def get_incident(self, incident_id: str) -> Incident: ...


TEntity = TypeVar("TEntity", bound=CanonicalOperationalEntity)


def _payload_mapping(value: JsonValue | None) -> dict[str, JsonValue]:
    return value if isinstance(value, dict) else {}


def _incident_payload(webhook: PagerDutyV3Webhook) -> dict[str, JsonValue]:
    nested = _payload_mapping(webhook.event.data.get("incident"))
    return nested or webhook.event.data


def _needs_incident_hydration(incident: Incident) -> bool:
    return not incident.title or not incident.status or incident.created_at is None


def _versioned(
    entity: TEntity, *, occurred_at: datetime, received_at: datetime
) -> TEntity:
    return replace(
        entity,
        source_version_at=occurred_at,
        observed_at=received_at,
        last_synced=received_at,
    )


async def _is_newer(
    store: PagerDutyWebhookStore, entity: CanonicalOperationalEntity
) -> bool:
    active = await store.load_active_operational_entities(
        type(entity),
        org_id=entity.org_id,
        provider=entity.provider,
        provider_instance_id=entity.provider_instance_id,
        source_entity_type=entity.source_entity_type,
    )
    for current in active:
        if current.external_id == entity.external_id:
            return entity.source_version_at > current.source_version_at
    return True


async def _insert_if_newer(
    store: PagerDutyWebhookStore, entity: CanonicalOperationalEntity
) -> bool:
    if not await _is_newer(store, entity):
        return False
    match entity:
        case OperationalService():
            await store.insert_operational_services([entity])
        case OperationalIncident():
            await store.insert_operational_incidents([entity])
        case OperationalAlert():
            await store.insert_operational_alerts([entity])
        case IncidentTimelineEvent():
            await store.insert_operational_incident_timeline_events([entity])
        case IncidentNote():
            await store.insert_operational_incident_notes([entity])
        case unreachable:
            raise TypeError(
                f"Unsupported PagerDuty webhook entity: {type(unreachable)!r}"
            )
    return True


async def _tombstone_service(
    *,
    store: PagerDutyWebhookStore,
    service: OperationalService,
) -> bool:
    active = await store.load_active_operational_entities(
        OperationalService,
        org_id=service.org_id,
        provider=service.provider,
        provider_instance_id=service.provider_instance_id,
        source_entity_type="service",
    )
    matching = next(
        (row for row in active if row.external_id == service.external_id), None
    )
    match matching:
        case OperationalService() if (
            service.source_version_at <= matching.source_version_at
        ):
            return False
        case OperationalService():
            tombstone = replace(
                matching,
                source_version_at=service.source_version_at,
                observed_at=service.observed_at,
                last_synced=service.last_synced,
                is_deleted=True,
                deleted_at=service.source_version_at,
            )
        case None:
            tombstone = replace(
                service, is_deleted=True, deleted_at=service.source_version_at
            )
        case unexpected:
            raise TypeError(f"Operational service lookup returned {type(unexpected)!r}")
    await store.insert_operational_services([tombstone])
    return True


async def reconcile_pagerduty_webhook(
    *,
    webhook: PagerDutyV3Webhook,
    org_id: str,
    provider_instance_id: str,
    received_at: datetime,
    store: PagerDutyWebhookStore,
    client: PagerDutyIncidentClient,
) -> bool:
    normalizer = PagerDutyNormalizer(org_id, provider_instance_id, received_at)
    occurred_at = webhook.event.occurred_at
    match webhook.event.event_type:
        case (
            PagerDutyEventType.SERVICE_CREATED
            | PagerDutyEventType.SERVICE_UPDATED
            | PagerDutyEventType.SERVICE_UPDATED_V3
        ):
            service_entity = _versioned(
                normalizer.service(Service.model_validate(webhook.event.data)),
                occurred_at=occurred_at,
                received_at=received_at,
            )
            return await _insert_if_newer(store, service_entity)
        case PagerDutyEventType.SERVICE_DELETED:
            service_entity = _versioned(
                normalizer.service(Service.model_validate(webhook.event.data)),
                occurred_at=occurred_at,
                received_at=received_at,
            )
            return await _tombstone_service(store=store, service=service_entity)
        case (
            PagerDutyEventType.INCIDENT_TRIGGERED
            | PagerDutyEventType.INCIDENT_ACKNOWLEDGED
            | PagerDutyEventType.INCIDENT_UNACKNOWLEDGED
            | PagerDutyEventType.INCIDENT_ESCALATED
            | PagerDutyEventType.INCIDENT_REASSIGNED
            | PagerDutyEventType.INCIDENT_DELEGATED
            | PagerDutyEventType.INCIDENT_PRIORITY_UPDATED
            | PagerDutyEventType.INCIDENT_RESOLVED
            | PagerDutyEventType.INCIDENT_REOPENED
            | PagerDutyEventType.INCIDENT_ANNOTATED
            | PagerDutyEventType.RESPONDER_ADDED
            | PagerDutyEventType.RESPONDER_REPLIED
            | PagerDutyEventType.STATUS_UPDATE_PUBLISHED
        ):
            incident = Incident.model_validate(_incident_payload(webhook))
            if _needs_incident_hydration(incident):
                incident = await client.get_incident(incident.id)
            incident_entity = _versioned(
                normalizer.incident(incident),
                occurred_at=occurred_at,
                received_at=received_at,
            )
            return await _insert_if_newer(store, incident_entity)
        case unreachable:
            raise AssertionError(f"Unhandled PagerDuty event type: {unreachable!r}")
