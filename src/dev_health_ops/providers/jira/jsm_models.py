"""Typed boundary models for Jira Service Management incident search rows."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


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

    @field_validator("created", "updated", "resolution_date")
    @classmethod
    def _require_utc(cls, value: datetime | None) -> datetime | None:
        """Pin every incident timestamp to UTC at the provider boundary.

        Jira Cloud returns the reporter's local offset -- ``+0200`` as readily
        as ``+0000`` -- and pydantic preserves whichever it is parsed. Two
        things then break downstream, both silently:

        * ``operational_ordering_codec.canonical_datetime`` requires a
          zero-offset datetime and raises ``OperationalOrderingEncodingError``
          on anything else, so a single non-UTC incident aborts the whole
          batch's conflict-key construction.
        * The Go incidents route already normalizes
          (``parseJiraIncidentTime`` -> ``parsed.UTC().Truncate(microsecond)``),
          so leaving Python on the source offset makes the two runtimes emit
          different rows for the same payload -- exactly the divergence the
          live-Python oracle exists to catch.

        A naive value is rejected rather than assumed to be UTC, matching Go,
        whose layouts all require an explicit offset: guessing a zone here
        would move a timestamp by hours with no error anywhere.
        """

        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("JSM incident timestamps require an explicit offset")
        return value.astimezone(UTC)


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
