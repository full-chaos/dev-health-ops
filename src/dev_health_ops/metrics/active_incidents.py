"""Repository-scoped incident projection for analytics consumers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Any, TypeVar

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
    """Build the legacy-plus-canonical repository incident projection query.

    PagerDuty incidents have no repository directly. They become repository-scoped
    only through a currently active service mapping in the same organization.
    Legacy rows appear first and win a same-repository identity collision until the
    legacy backfill is retired, preserving existing GitHub/GitLab behavior.
    """
    match window:
        case IncidentWindow.RESOLVED:
            legacy_time_filter = (
                "resolved_at IS NOT NULL "
                "AND resolved_at >= {start:DateTime64(3, 'UTC')} "
                "AND resolved_at < {end:DateTime64(3, 'UTC')}"
            )
            canonical_time_filter = (
                "incident.resolved_at IS NOT NULL "
                "AND incident.resolved_at >= {start:DateTime64(3, 'UTC')} "
                "AND incident.resolved_at < {end:DateTime64(3, 'UTC')}"
            )
        case IncidentWindow.STARTED:
            legacy_time_filter = (
                "started_at >= {start:DateTime64(3, 'UTC')} "
                "AND started_at < {end:DateTime64(3, 'UTC')}"
            )
            canonical_time_filter = (
                "incident.started_at >= {start:DateTime64(3, 'UTC')} "
                "AND incident.started_at < {end:DateTime64(3, 'UTC')}"
            )

    legacy_org_filter = "org_id = {org_id:String} AND" if org_id else ""
    legacy_projection = f"""
        SELECT repo_id, incident_id, status, started_at, resolved_at, last_synced AS synced_at, 1 AS source_priority
        FROM incidents FINAL
        WHERE {legacy_org_filter}
              {legacy_time_filter}
    """
    if not org_id:
        return f"""
            SELECT repo_id, incident_id, status, started_at, resolved_at, synced_at AS last_synced
            FROM ({legacy_projection})
            WHERE 1 = 1{repo_filter}
        """

    canonical_projection = f"""
        SELECT
            mapping.repo_id,
            incident.id AS incident_id,
            incident.normalized_status AS status,
            incident.started_at,
            incident.resolved_at,
            incident.last_synced AS synced_at,
            0 AS source_priority
        FROM operational_incidents AS incident FINAL
        INNER JOIN operational_service_repository_mappings AS mapping FINAL
            ON incident.org_id = mapping.org_id
           AND incident.service_id = mapping.service_id
        INNER JOIN repos AS repo FINAL
            ON mapping.org_id = repo.org_id
           AND mapping.repo_id = repo.id
        WHERE incident.org_id = {{org_id:String}}
          AND mapping.org_id = {{org_id:String}}
          AND incident.is_deleted = 0
          AND mapping.repo_id IS NOT NULL
          AND mapping.is_active = 1
          AND mapping.valid_from <= {{as_of:DateTime64(6, 'UTC')}}
          AND (mapping.valid_to IS NULL OR mapping.valid_to > {{as_of:DateTime64(6, 'UTC')}})
          AND {canonical_time_filter}
    """
    return f"""
        WITH projected_incidents AS (
            {legacy_projection}
            UNION ALL
            {canonical_projection}
        )
        SELECT
            repo_id,
            incident_id,
            argMax(status, (source_priority, synced_at)) AS status,
            argMax(started_at, (source_priority, synced_at)) AS started_at,
            argMax(resolved_at, (source_priority, synced_at)) AS resolved_at,
            argMax(synced_at, (source_priority, synced_at)) AS last_synced
        FROM projected_incidents
        WHERE 1 = 1{repo_filter}
        GROUP BY repo_id, incident_id
        HAVING repo_id IS NOT NULL
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
