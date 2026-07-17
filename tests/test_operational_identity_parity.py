from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from dev_health_ops.api.external_ingest.schemas import RecordEnvelope
from dev_health_ops.backfill.operational import (
    LegacyIncidentRepositoryRow,
    map_legacy_issue_incident_batches,
)
from dev_health_ops.external_ingest.normalize import normalize_batch
from dev_health_ops.providers.operational_migration import (
    IssueIncidentSource,
    map_issue_incidents,
)

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)
_REPO_ID = UUID("00000000-0000-0000-0000-000000000101")


@pytest.mark.parametrize(
    ("provider", "instance"),
    (("github", "https://github.com"), ("gitlab", "https://gitlab.example.com")),
)
def test_operational_incident_identity_matches_native_backfill_and_push(
    provider: str, instance: str
) -> None:
    # Given: one logical issue incident represented by all three ingestion paths.
    native = IssueIncidentSource(
        org_id="org-a",
        provider=provider,
        provider_instance_id=instance,
        repo_id=_REPO_ID,
        repo_full_name="acme/api",
        external_id="17",
        issue_number="17",
        source_url=None,
        labels=("incident",),
        raw_status="open",
        title="Database unavailable",
        description=None,
        created_at=_AT,
        resolved_at=None,
        source_version_at=_AT,
    )
    backfill = LegacyIncidentRepositoryRow(
        org_id="org-a",
        repo_id=_REPO_ID,
        repo_full_name="acme/api",
        provider=provider,
        provider_instance_id=instance,
        incident_id="17",
        status="open",
        started_at=_AT,
        resolved_at=None,
        source_version_at=_AT,
    )
    push_record = RecordEnvelope(
        kind="operational_incident.v1",
        external_id="17",
        payload={
            "externalId": "17",
            "sourceVersionAt": _AT.isoformat(),
            "sourceEventId": "17",
            "serviceExternalId": "acme/api",
            "title": "Database unavailable",
        },
    )

    # When: each representation is mapped into canonical operational records.
    native_id = map_issue_incidents((native,)).incidents[0].id
    backfill_id = map_legacy_issue_incident_batches((backfill,))[0].incidents[0].id
    push_id = (
        normalize_batch(
            org_id="org-a",
            source_id=uuid4(),
            source_system=provider,
            source_instance=instance,
            ingestion_id=uuid4(),
            records=[push_record],
        )
        .batch.operational_incidents[0]
        .id
    )

    # Then: all producer paths use the same canonical source coordinates.
    assert native_id == backfill_id == push_id
