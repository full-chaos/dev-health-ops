from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from dev_health_ops.backfill.operational import (
    LegacyIncidentRepositoryRow,
    map_legacy_issue_incident_batches,
)

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)


def test_legacy_incident_backfill_preserves_distinct_global_issue_ids() -> None:
    # Given: distinct provider-global issue ids from separate repositories in one organization.
    rows = (
        LegacyIncidentRepositoryRow(
            org_id="org-a",
            repo_id=UUID("00000000-0000-0000-0000-000000000101"),
            repo_full_name="acme/api",
            provider="github",
            provider_instance_id="github.com",
            incident_id="incident-2",
            status="closed",
            started_at=_AT,
            resolved_at=_AT,
            source_version_at=_AT,
        ),
        LegacyIncidentRepositoryRow(
            org_id="org-a",
            repo_id=UUID("00000000-0000-0000-0000-000000000102"),
            repo_full_name="acme/worker",
            provider="github",
            provider_instance_id="github.com",
            incident_id="incident-1",
            status="closed",
            started_at=_AT,
            resolved_at=_AT,
            source_version_at=_AT,
        ),
    )

    # When: legacy incident and repository rows are mapped into canonical batches.
    batches = map_legacy_issue_incident_batches(rows)

    # Then: provider-global incident identities remain distinct.
    assert len(batches) == 1
    assert len(batches[0].incidents) == 2
    assert len(batches[0].services) == 2
    assert {
        mapping.repo_full_name for mapping in batches[0].service_repository_mappings
    } == {
        "acme/api",
        "acme/worker",
    }


def test_legacy_incident_backfill_separates_organizations_and_provider_instances() -> (
    None
):
    # Given: matching legacy identity fields across organization and provider-instance boundaries.
    rows = (
        LegacyIncidentRepositoryRow(
            org_id="org-a",
            repo_id=UUID("00000000-0000-0000-0000-000000000101"),
            repo_full_name="acme/api",
            provider="gitlab",
            provider_instance_id="https://gitlab.com",
            incident_id="incident-1",
            status="opened",
            started_at=_AT,
            resolved_at=None,
            source_version_at=_AT,
        ),
        LegacyIncidentRepositoryRow(
            org_id="org-b",
            repo_id=UUID("00000000-0000-0000-0000-000000000101"),
            repo_full_name="acme/api",
            provider="gitlab",
            provider_instance_id="https://gitlab.example.com",
            incident_id="incident-1",
            status="opened",
            started_at=_AT,
            resolved_at=None,
            source_version_at=_AT,
        ),
    )

    # When: rows are grouped into homogeneous canonical batches.
    batches = map_legacy_issue_incident_batches(rows)

    # Then: each canonical identity remains isolated by source context.
    assert len(batches) == 2
    assert {batch.org_id for batch in batches} == {"org-a", "org-b"}
    assert {batch.provider_instance_id for batch in batches} == {
        "gitlab.com",
        "gitlab.example.com",
    }
    assert len({batch.incidents[0].id for batch in batches}) == 2
