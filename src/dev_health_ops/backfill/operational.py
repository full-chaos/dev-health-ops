"""Pure mappings for migrating legacy incident rows into canonical operations."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from dev_health_ops.models.operational import OperationalBatch
from dev_health_ops.providers.operational_migration import (
    IssueIncidentSource,
    map_issue_incidents,
)


@dataclass(frozen=True, slots=True)
class LegacyIncidentRepositoryRow:
    """A legacy incident row enriched with its source repository identity."""

    org_id: str
    repo_id: UUID
    repo_full_name: str
    provider: str
    provider_instance_id: str
    incident_id: str
    status: str | None
    started_at: datetime
    resolved_at: datetime | None
    source_version_at: datetime


def map_legacy_issue_incident_batches(
    rows: Iterable[LegacyIncidentRepositoryRow],
) -> tuple[OperationalBatch, ...]:
    """Group repository-enriched legacy incident rows into canonical batches."""
    grouped: dict[tuple[str, str, str], list[IssueIncidentSource]] = defaultdict(list)
    for row in rows:
        grouped[(row.org_id, row.provider, row.provider_instance_id)].append(
            IssueIncidentSource(
                org_id=row.org_id,
                provider=row.provider,
                provider_instance_id=row.provider_instance_id,
                repo_id=row.repo_id,
                repo_full_name=row.repo_full_name,
                external_id=row.incident_id,
                issue_number=None,
                source_url=None,
                labels=(),
                raw_status=row.status,
                title="",
                description=None,
                created_at=row.started_at,
                resolved_at=row.resolved_at,
                source_version_at=row.source_version_at - timedelta(seconds=1),
            )
        )
    return tuple(map_issue_incidents(sources) for sources in grouped.values())
