"""Repository-scoped incident projection for analytics consumers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Any, TypeVar

from dev_health_ops.storage.operational_current import current_operational_rows_sql

ProjectedIncidentRow = TypeVar("ProjectedIncidentRow", bound=Mapping[str, Any])


class IncidentWindow(StrEnum):
    """Canonical incident lifecycle timestamp used to bound a projection."""

    RESOLVED = "resolved"
    STARTED = "started"


def active_incidents_query(
    *,
    window: IncidentWindow,
    org_id: str,
    repo_filter: str,
) -> str:
    """Build the canonical repository incident projection query.

    PagerDuty incidents have no repository directly. They become repository-scoped
    only through a currently active service mapping in the same organization.
    """
    if not org_id:
        raise ValueError("canonical incident projection requires org_id")

    # Initialize before the match so control-flow analysis sees a definite
    # assignment; an unhandled window then yields an empty filter and a loud SQL
    # error rather than a silent wrong result.
    canonical_time_filter = ""
    match window:
        case IncidentWindow.RESOLVED:
            canonical_time_filter = (
                "resolved_at IS NOT NULL "
                "AND resolved_at >= {start:DateTime64(3, 'UTC')} "
                "AND resolved_at < {end:DateTime64(3, 'UTC')}"
            )
        case IncidentWindow.STARTED:
            canonical_time_filter = (
                "started_at >= {start:DateTime64(3, 'UTC')} "
                "AND started_at < {end:DateTime64(3, 'UTC')}"
            )

    current_incidents = current_operational_rows_sql(
        "operational_incidents", ("is_deleted = 0", canonical_time_filter)
    )
    current_mappings = current_operational_rows_sql(
        "operational_service_repository_mappings",
        (
            "repo_id IS NOT NULL",
            "is_active = 1",
            "valid_from <= {as_of:DateTime64(6, 'UTC')}",
            "(valid_to IS NULL OR valid_to > {as_of:DateTime64(6, 'UTC')})",
        ),
    )
    return f"""
        SELECT repo_id, incident_id, status, started_at, resolved_at, last_synced
        FROM (
            SELECT
                mapping.repo_id AS repo_id,
                incident.id AS incident_id,
                incident.normalized_status AS status,
                incident.started_at,
                incident.resolved_at,
                incident.last_synced AS last_synced
            FROM {current_incidents} AS incident
            INNER JOIN {current_mappings} AS mapping
                ON incident.org_id = mapping.org_id
               AND incident.service_id = mapping.service_id
            INNER JOIN repos AS repo FINAL
                ON mapping.org_id = repo.org_id
               AND mapping.repo_id = repo.id
            WHERE mapping.repo_id IS NOT NULL{repo_filter}
            ORDER BY mapping.repo_id, incident.id, incident.last_synced DESC
            LIMIT 1 BY mapping.repo_id, incident.id
        )
        ORDER BY repo_id, incident_id
    """


def deduplicate_active_incidents(
    rows: Sequence[ProjectedIncidentRow],
) -> list[ProjectedIncidentRow]:
    """Keep the first row for each repository-local stable incident identity."""
    seen: set[tuple[str, str]] = set()
    deduplicated: list[ProjectedIncidentRow] = []
    for row in rows:
        key = (str(row["repo_id"]), str(row["incident_id"]))
        if key in seen:
            continue
        seen.add(key)
        deduplicated.append(row)
    return deduplicated
