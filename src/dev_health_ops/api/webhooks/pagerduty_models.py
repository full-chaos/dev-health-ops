from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


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
    RESPONDER_ADDED = "incident.responder.added"
    RESPONDER_REPLIED = "incident.responder.replied"
    SERVICE_UPDATED = "incident.service_updated"
    STATUS_UPDATE_PUBLISHED = "incident.status_update_published"
    SERVICE_CREATED = "service.created"
    SERVICE_DELETED = "service.deleted"
    SERVICE_UPDATED_V3 = "service.updated"


class PagerDutyEvent(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    id: str = Field(max_length=512)
    event_type: PagerDutyEventType | Literal["pagey.ping"]
    occurred_at: datetime
    data: dict[str, JsonValue]

    @field_validator("occurred_at")
    @classmethod
    def occurred_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone offset")
        return value.astimezone(UTC)


class PagerDutyV3Webhook(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    event: PagerDutyEvent
