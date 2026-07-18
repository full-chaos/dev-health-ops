from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, JsonValue


class PagerDutyEventType(StrEnum):
    INCIDENT_TRIGGERED = "incident.triggered"
    INCIDENT_ACKNOWLEDGED = "incident.acknowledged"
    INCIDENT_UNACKNOWLEDGED = "incident.unacknowledged"
    INCIDENT_ESCALATED = "incident.escalated"
    INCIDENT_REASSIGNED = "incident.reassigned"
    INCIDENT_DELEGATED = "incident.delegated"
    INCIDENT_PRIORITY_UPDATED = "incident.priority_updated"
    INCIDENT_RESOLVED = "incident.resolved"
    INCIDENT_REOPENED = "incident.reopened"
    INCIDENT_ANNOTATED = "incident.annotated"
    RESPONDER_ADDED = "responder.added"
    RESPONDER_REPLIED = "responder.replied"
    SERVICE_UPDATED = "service_updated"
    STATUS_UPDATE_PUBLISHED = "status_update_published"
    SERVICE_CREATED = "service.created"
    SERVICE_DELETED = "service.deleted"
    SERVICE_UPDATED_V3 = "service.updated"


class PagerDutyEvent(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    id: str
    event_type: PagerDutyEventType
    occurred_at: datetime
    data: dict[str, JsonValue]


class PagerDutyV3Webhook(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    event: PagerDutyEvent


class PagerDutyWebhookResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: str
    event_id: str | None = None
    message: str


class PagerDutyWebhookConfiguration(BaseModel):
    model_config = ConfigDict(frozen=True)

    configured: bool
    org_id: str | None
    provider_instance_id: str | None
