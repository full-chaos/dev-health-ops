"""Provider-neutral mappings for legacy operational producers.

The mappers in this module preserve the legacy write path while supplying the
canonical ClickHouse operational contract during the reversible migration.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsAlert,
    AtlassianOpsIncident,
    AtlassianOpsSchedule,
)
from dev_health_ops.models.operational import (
    OnCallSchedule,
    OperationalAlert,
    OperationalBatch,
    OperationalIncident,
    OperationalService,
    ServiceRepositoryMapping,
)
from dev_health_ops.models.operational_identity import operational_source_coordinates


@dataclass(frozen=True, slots=True)
class IssueIncidentSource:
    """Provider issue data retained until its canonical incident is persisted."""

    org_id: str
    provider: str
    provider_instance_id: str
    repo_id: UUID
    repo_full_name: str
    external_id: str
    issue_number: str | None
    source_url: str | None
    labels: tuple[str, ...]
    raw_status: str | None
    title: str
    description: str | None
    created_at: datetime
    resolved_at: datetime | None
    source_version_at: datetime


@dataclass(frozen=True, slots=True)
class AtlassianOpsRows:
    """Legacy Atlassian Ops rows for one provider instance."""

    incidents: tuple[AtlassianOpsIncident, ...] = ()
    alerts: tuple[AtlassianOpsAlert, ...] = ()
    schedules: tuple[AtlassianOpsSchedule, ...] = ()


@dataclass(frozen=True, slots=True)
class AtlassianOpsSource:
    """Canonical identity context for an Atlassian Ops legacy batch."""

    org_id: str
    provider_instance_id: str
    rows: AtlassianOpsRows


class OperationalWriteStore(Protocol):
    """Canonical operational sink capability used by producer orchestration."""

    async def insert_operational_services(
        self, services: list[OperationalService]
    ) -> None: ...

    async def insert_operational_incidents(
        self, incidents: list[OperationalIncident]
    ) -> None: ...

    async def insert_operational_alerts(
        self, alerts: list[OperationalAlert]
    ) -> None: ...

    async def insert_operational_on_call_schedules(
        self, schedules: list[OnCallSchedule]
    ) -> None: ...

    async def insert_operational_service_repository_mappings(
        self, mappings: list[ServiceRepositoryMapping]
    ) -> None: ...


def operational_dual_write_enabled() -> bool:
    """Return whether additive canonical operational writes are enabled."""
    return os.getenv("OPERATIONAL_INCIDENT_DUAL_WRITE", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _normalized_status(raw_status: str | None) -> str | None:
    normalized = {
        "active": "active",
        "acknowledged": "acknowledged",
        "closed": "resolved",
        "open": "open",
        "opened": "open",
        "resolved": "resolved",
        "suppressed": "suppressed",
    }
    return normalized.get((raw_status or "").strip().lower())


def _normalized_priority(raw_priority: str | None) -> str | None:
    normalized = {
        "critical": "critical",
        "high": "high",
        "low": "low",
        "medium": "medium",
        "p1": "critical",
        "p2": "high",
        "p3": "medium",
        "p4": "low",
    }
    return normalized.get((raw_priority or "").strip().lower())


def _normalized_severity(raw_severity: str | None) -> str | None:
    normalized = {
        "critical": "critical",
        "high": "high",
        "info": "info",
        "low": "low",
        "medium": "medium",
        "sev1": "critical",
        "sev2": "high",
        "sev3": "medium",
        "sev4": "low",
    }
    return normalized.get((raw_severity or "").strip().lower().replace("-", ""))


def map_issue_incidents(sources: Sequence[IssueIncidentSource]) -> OperationalBatch:
    """Map labelled GitHub or GitLab issues into canonical incident records."""
    if not sources:
        raise ValueError("at least one issue incident source is required")

    first = sources[0]
    first_coordinates = operational_source_coordinates(
        OperationalIncident,
        provider=first.provider,
        provider_instance_id=first.provider_instance_id,
        external_id=first.external_id,
        repo_full_name=first.repo_full_name,
        issue_number=first.issue_number,
    )
    services: dict[str, OperationalService] = {}
    mappings: dict[str, ServiceRepositoryMapping] = {}
    incidents: dict[str, OperationalIncident] = {}
    for source in sources:
        source_coordinates = operational_source_coordinates(
            OperationalIncident,
            provider=source.provider,
            provider_instance_id=source.provider_instance_id,
            external_id=source.external_id,
            repo_full_name=source.repo_full_name,
            issue_number=source.issue_number,
        )
        if source.org_id != first.org_id or (
            source_coordinates.provider,
            source_coordinates.provider_instance_id,
        ) != (
            first_coordinates.provider,
            first_coordinates.provider_instance_id,
        ):
            raise ValueError(
                "issue incident sources must share canonical identity context"
            )

        service_coordinates = operational_source_coordinates(
            OperationalService,
            provider=source.provider,
            provider_instance_id=source.provider_instance_id,
            external_id=source.repo_full_name,
        )
        incident_coordinates = source_coordinates
        service = services.setdefault(
            source.repo_full_name,
            OperationalService(
                org_id=source.org_id,
                provider=service_coordinates.provider,
                provider_instance_id=service_coordinates.provider_instance_id,
                source_entity_type="repository",
                external_id=service_coordinates.external_id,
                source_version_at=source.source_version_at,
                source_url=None,
                name=source.repo_full_name,
                service_type="repository",
            ),
        )
        mapping = ServiceRepositoryMapping(
            org_id=source.org_id,
            provider=service_coordinates.provider,
            provider_instance_id=service_coordinates.provider_instance_id,
            source_entity_type="repository_mapping",
            external_id=f"{source.repo_full_name}:{source.repo_id}",
            source_version_at=source.source_version_at,
            service_id=service.id,
            repo_id=source.repo_id,
            repo_full_name=source.repo_full_name,
            repo_provider=source.provider,
            mapping_kind="repository_derived",
            relationship_provenance="native_repository_context",
            relationship_confidence=1.0,
        )
        mappings.setdefault(mapping.id, mapping)
        incident = OperationalIncident(
            org_id=source.org_id,
            provider=incident_coordinates.provider,
            provider_instance_id=incident_coordinates.provider_instance_id,
            source_entity_type="issue",
            external_id=incident_coordinates.external_id,
            source_version_at=source.source_version_at,
            source_url=source.source_url,
            source_event_id=source.issue_number,
            raw_status=source.raw_status,
            normalized_status=_normalized_status(source.raw_status),
            service_id=service.id,
            service_external_id=source.repo_full_name,
            title=source.title,
            description=source.description,
            started_at=source.created_at,
            resolved_at=source.resolved_at,
        )
        incidents.setdefault(incident.id, incident)

    return OperationalBatch(
        org_id=first.org_id,
        provider=first_coordinates.provider,
        provider_instance_id=first_coordinates.provider_instance_id,
        services=tuple(services.values()),
        incidents=tuple(incidents.values()),
        service_repository_mappings=tuple(mappings.values()),
    )


def map_atlassian_ops_batch(source: AtlassianOpsSource) -> OperationalBatch:
    """Map one legacy Atlassian Ops provider instance into canonical entities."""
    incidents = tuple(
        OperationalIncident(
            org_id=source.org_id,
            provider="atlassian",
            provider_instance_id=source.provider_instance_id,
            source_entity_type="atlassian_ops_incident",
            external_id=row.id,
            source_version_at=row.last_synced,
            source_url=row.url,
            raw_status=row.status,
            raw_severity=row.severity,
            normalized_status=_normalized_status(row.status),
            normalized_severity=_normalized_severity(row.severity),
            title=row.summary,
            description=row.description,
            started_at=row.created_at,
            resolved_at=row.last_synced
            if _normalized_status(row.status) == "resolved"
            else None,
        )
        for row in source.rows.incidents
    )
    alerts = tuple(
        OperationalAlert(
            org_id=source.org_id,
            provider="atlassian",
            provider_instance_id=source.provider_instance_id,
            source_entity_type="atlassian_ops_alert",
            external_id=row.id,
            source_version_at=row.last_synced,
            raw_status=row.status,
            raw_priority=row.priority,
            normalized_status=_normalized_status(row.status),
            normalized_priority=_normalized_priority(row.priority),
            triggered_at=row.created_at,
            acknowledged_at=row.acknowledged_at,
            resolved_at=row.closed_at,
        )
        for row in source.rows.alerts
    )
    schedules = tuple(
        OnCallSchedule(
            org_id=source.org_id,
            provider="atlassian",
            provider_instance_id=source.provider_instance_id,
            source_entity_type="atlassian_ops_schedule",
            external_id=row.id,
            source_version_at=row.last_synced,
            name=row.name,
            timezone=row.timezone,
        )
        for row in source.rows.schedules
    )
    return OperationalBatch(
        org_id=source.org_id,
        provider="atlassian",
        provider_instance_id=source.provider_instance_id,
        services=(),
        incidents=incidents,
        alerts=alerts,
        on_call_schedules=schedules,
    )


async def write_operational_batch(
    store: OperationalWriteStore, batch: OperationalBatch
) -> None:
    """Persist a homogeneous canonical batch through typed ClickHouse writers."""
    if batch.services:
        await store.insert_operational_services(list(batch.services))
    if batch.service_repository_mappings:
        await store.insert_operational_service_repository_mappings(
            list(batch.service_repository_mappings)
        )
    if batch.incidents:
        await store.insert_operational_incidents(list(batch.incidents))
    if batch.alerts:
        await store.insert_operational_alerts(list(batch.alerts))
    if batch.on_call_schedules:
        await store.insert_operational_on_call_schedules(list(batch.on_call_schedules))
