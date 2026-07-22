from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from dev_health_ops.api.external_ingest.schemas import RecordEnvelope
from dev_health_ops.external_ingest.normalize import normalize_batch
from dev_health_ops.providers.operational_migration import (
    IssueIncidentSource,
    map_issue_incidents,
)

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)
_REPO_ID = UUID("00000000-0000-0000-0000-000000000101")


def test_gitlab_incident_identity_matches_native_and_push() -> None:
    # Given: one logical issue incident represented by both supported ingestion paths.
    provider = "gitlab"
    native_instance = "https://GitLab.Acme.test:8443/api/v4"
    push_instance = "https://gitlab.acme.test:8443/api/v4"
    global_issue_id = "200000001"
    native = IssueIncidentSource(
        org_id="org-a",
        provider=provider,
        provider_instance_id=native_instance,
        repo_id=_REPO_ID,
        repo_full_name="AcMe/API",
        external_id=global_issue_id,
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
    push_record = RecordEnvelope(
        kind="operational_incident.v1",
        external_id=global_issue_id,
        payload={
            "externalId": global_issue_id,
            "sourceVersionAt": _AT.isoformat(),
            "sourceEventId": "17",
            "serviceExternalId": "AcMe/API",
            "title": "Database unavailable",
        },
    )

    # When: each representation is mapped into canonical operational records.
    native_id = map_issue_incidents((native,)).incidents[0].id
    push_id = (
        normalize_batch(
            org_id="org-a",
            source_id=uuid4(),
            source_system=provider,
            source_instance=push_instance,
            ingestion_id=uuid4(),
            records=[push_record],
        )
        .batch.operational_incidents[0]
        .id
    )

    # Then: all producer paths use the same canonical source coordinates.
    assert native_id == push_id
