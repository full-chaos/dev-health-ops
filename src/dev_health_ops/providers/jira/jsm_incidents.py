"""Jira Service Management incident-only enumeration and normalization."""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from dev_health_ops.models.operational import OperationalBatch, OperationalIncident
from dev_health_ops.models.operational_identity import operational_source_coordinates
from dev_health_ops.providers.jira.jsm_models import parse_jsm_incident


class JsmIncidentClient(Protocol):
    """The small public Jira client surface required by the JSM producer."""

    def iter_service_desks(self) -> AsyncIterable[str]: ...

    def iter_jsm_incident_issues(
        self,
        *,
        project_keys: tuple[str, ...],
        window_start: datetime,
        window_end: datetime,
    ) -> AsyncIterable[dict[str, object]]: ...

    async def admit_jsm_incident(self, *, issue_id: str) -> bool: ...


class JsmIncidentScopeError(ValueError):
    """Raised when a configured JSM source scope cannot be verified."""


@dataclass(frozen=True, slots=True)
class JsmIncidentProducer:
    """Enumerate JSM incident issues and emit only canonical incidents."""

    client: JsmIncidentClient
    org_id: str
    provider_instance_id: str
    base_url: str
    window_start: datetime | None = None
    window_end: datetime | None = None
    observed_at: datetime | None = None
    allowed_project_keys: tuple[str, ...] = ()

    async def collect(self) -> OperationalBatch:
        """Fetch service project incidents using the required two-step contract."""
        if not self.allowed_project_keys:
            raise JsmIncidentScopeError(
                "JSM incident ingestion requires a service project"
            )
        if self.window_start is None or self.window_end is None:
            raise JsmIncidentScopeError(
                "JSM incident ingestion requires a bounded updated window"
            )
        if self.window_start >= self.window_end:
            raise JsmIncidentScopeError(
                "JSM incident ingestion requires a non-empty updated window"
            )
        available_project_keys = {key async for key in self.client.iter_service_desks()}
        unavailable_project_keys = set(self.allowed_project_keys).difference(
            available_project_keys
        )
        if unavailable_project_keys:
            raise JsmIncidentScopeError(
                "Configured Jira source is not a JSM service project"
            )
        rows = []
        async for row in self.client.iter_jsm_incident_issues(
            project_keys=self.allowed_project_keys,
            window_start=self.window_start,
            window_end=self.window_end,
        ):
            issue = parse_jsm_incident(row)
            if await self.client.admit_jsm_incident(issue_id=issue.id):
                rows.append(row)
        return self.normalize(rows)

    def normalize(self, rows: Iterable[dict[str, object]]) -> OperationalBatch:
        """Normalize validated JSM issue rows into an incident-only batch."""
        incidents = tuple(self._incident(row) for row in rows)
        return OperationalBatch(
            org_id=self.org_id,
            provider="jira",
            provider_instance_id=self.provider_instance_id,
            observed_at=self.observed_at or datetime.now(UTC),
            incidents=incidents,
        )

    def _incident(self, row: dict[str, object]) -> OperationalIncident:
        issue = parse_jsm_incident(row)
        coordinates = operational_source_coordinates(
            OperationalIncident,
            provider="jira",
            provider_instance_id=self.provider_instance_id,
            external_id=issue.id,
        )
        status_category = issue.fields.status.status_category.key.casefold()
        return OperationalIncident(
            org_id=self.org_id,
            provider=coordinates.provider,
            provider_instance_id=coordinates.provider_instance_id,
            source_entity_type="jsm_incident",
            external_id=coordinates.external_id,
            source_version_at=issue.fields.updated,
            source_url=f"{self.base_url.rstrip('/')}/browse/{issue.key}",
            source_event_at=issue.fields.created,
            source_event_id=issue.key,
            observed_at=self.observed_at or datetime.now(UTC),
            last_synced=self.observed_at or datetime.now(UTC),
            raw_status=issue.fields.status.name,
            raw_priority=issue.fields.priority.name if issue.fields.priority else None,
            normalized_status=_normalized_status(status_category),
            title=issue.fields.summary,
            started_at=issue.fields.created,
            resolved_at=issue.fields.resolution_date
            if status_category == "done"
            else None,
        )


def _normalized_status(status_category: str) -> str:
    """Map Jira's stable categories to the canonical incident vocabulary."""
    match status_category:
        case "new" | "open":
            return "open"
        case "indeterminate":
            return "active"
        case "done":
            return "resolved"
        case _:
            return "active"
