"""Typed boundary models for Jira Service Management incident search rows."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class JsmPayloadError(ValueError):
    """Raised when a JSM search result cannot satisfy the incident contract."""


class JsmStatusCategory(BaseModel):
    """Jira's stable status category key."""

    model_config = ConfigDict(frozen=True)
    key: str


class JsmStatus(BaseModel):
    """Raw Jira status retained alongside its stable category."""

    model_config = ConfigDict(frozen=True)
    name: str
    status_category: JsmStatusCategory = Field(alias="statusCategory")


class JsmPriority(BaseModel):
    """Raw Jira priority displayed by the source system."""

    model_config = ConfigDict(frozen=True)
    name: str


class JsmIncidentFields(BaseModel):
    """The incident fields selected by the enhanced JQL request."""

    model_config = ConfigDict(frozen=True)
    summary: str
    created: datetime
    updated: datetime
    resolution_date: datetime | None = Field(default=None, alias="resolutiondate")
    status: JsmStatus
    priority: JsmPriority | None = None


class JsmIncidentIssue(BaseModel):
    """A validated JSM incident issue returned by enhanced JQL search."""

    model_config = ConfigDict(frozen=True)
    id: str
    key: str
    fields: JsmIncidentFields


class JsmNativeIncident(BaseModel):
    """A successfully parsed response from the native JSM Incidents API."""

    model_config = ConfigDict(extra="allow", frozen=True)


def parse_jsm_incident(value: dict[str, object]) -> JsmIncidentIssue:
    """Parse one untrusted enhanced-JQL issue response at the provider boundary."""
    try:
        return JsmIncidentIssue.model_validate(value)
    except ValidationError as error:
        raise JsmPayloadError(
            "JSM incident response failed schema validation"
        ) from error


def parse_jsm_native_incident(value: dict[str, object]) -> JsmNativeIncident:
    """Parse an authoritative JSM Incidents API response at the transport boundary."""
    try:
        return JsmNativeIncident.model_validate(value)
    except ValidationError as error:
        raise JsmPayloadError(
            "JSM native incident response failed schema validation"
        ) from error
