"""Typed PagerDuty V2 response models."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class PagerDutyModel(BaseModel):
    """Strict API model retaining explicitly supplied forward-compatible values."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    type: str | None = None
    summary: str | None = None
    self_url: str | None = Field(default=None, alias="self")
    html_url: str | None = None
    raw: dict[str, JsonValue] = Field(default_factory=dict)


class Service(PagerDutyModel):
    name: str | None = None
    status: str | None = None
    escalation_policy: PagerDutyModel | None = None


class BusinessService(PagerDutyModel):
    name: str | None = None
    description: str | None = None


class Incident(PagerDutyModel):
    incident_number: int | None = None
    title: str | None = None
    status: str | None = None
    urgency: str | None = None
    created_at: datetime | None = None
    service: PagerDutyModel | None = None


class Alert(PagerDutyModel):
    status: str | None = None
    severity: str | None = None
    created_at: datetime | None = None
    body: dict[str, JsonValue] | None = None


class LogEntry(PagerDutyModel):
    created_at: datetime | None = None
    channel: dict[str, JsonValue] | None = None
    summary: str | None = None


class Note(PagerDutyModel):
    content: str | None = None
    created_at: datetime | None = None
    user: PagerDutyModel | None = None


class EscalationPolicy(PagerDutyModel):
    name: str | None = None
    num_loops: int | None = None


class Schedule(PagerDutyModel):
    name: str | None = None
    time_zone: str | None = None


class Oncall(PagerDutyModel):
    start: datetime | None = None
    end: datetime | None = None
    user: PagerDutyModel | None = None
    schedule: PagerDutyModel | None = None
    escalation_policy: PagerDutyModel | None = None


class User(PagerDutyModel):
    name: str | None = None
    email: str | None = None
    role: str | None = None


class Team(PagerDutyModel):
    name: str | None = None
    description: str | None = None
