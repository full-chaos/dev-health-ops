from datetime import datetime, timezone
from uuid import UUID

import pytest

from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsAlert,
    AtlassianOpsIncident,
    AtlassianOpsSchedule,
)
from dev_health_ops.providers.operational_migration import (
    AtlassianOpsRows,
    AtlassianOpsSource,
    IssueIncidentSource,
    map_atlassian_ops_batch,
    map_issue_incidents,
)

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)
_REPO_ID = UUID("00000000-0000-0000-0000-000000000101")


def test_issue_incident_mapper_creates_repository_service_and_mapping() -> None:
    # Given: an issue-labelled incident from a GitHub repository.
    source = IssueIncidentSource(
        org_id="org-a",
        provider="github",
        provider_instance_id="github.com",
        repo_id=_REPO_ID,
        repo_full_name="acme/api",
        external_id="12345",
        issue_number="17",
        source_url="https://github.com/acme/api/issues/17",
        labels=("incident", "sev-1"),
        raw_status="closed",
        title="Database unavailable",
        description="Primary database was unavailable.",
        created_at=_AT,
        resolved_at=_AT,
        source_version_at=_AT,
    )

    # When: the source is normalized into the canonical envelope.
    batch = map_issue_incidents((source,))

    # Then: incident provenance and repository linkage are explicit.
    assert len(batch.incidents) == 1
    assert batch.incidents[0].source_entity_type == "issue"
    assert batch.incidents[0].external_id == "acme/api#17"
    assert batch.incidents[0].normalized_status == "resolved"
    assert batch.incidents[0].service_id == batch.services[0].id
    assert batch.incidents[0].service_external_id == "acme/api"
    assert batch.services[0].name == "acme/api"
    assert batch.service_repository_mappings[0].repo_id == _REPO_ID
    assert batch.service_repository_mappings[0].service_id == batch.services[0].id


def test_issue_incident_mapper_isolated_by_provider_and_organization() -> None:
    # Given: equivalent issue ids in distinct provider and organization scopes.
    github = IssueIncidentSource(
        org_id="org-a",
        provider="github",
        provider_instance_id="github.com",
        repo_id=_REPO_ID,
        repo_full_name="acme/api",
        external_id="12345",
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
    gitlab = IssueIncidentSource(
        org_id="org-a",
        provider="gitlab",
        provider_instance_id="gitlab.com",
        repo_id=_REPO_ID,
        repo_full_name="acme/api",
        external_id="12345",
        issue_number="17",
        source_url=None,
        labels=("incident",),
        raw_status="opened",
        title="Database unavailable",
        description=None,
        created_at=_AT,
        resolved_at=None,
        source_version_at=_AT,
    )
    other_org = IssueIncidentSource(
        org_id="org-b",
        provider="github",
        provider_instance_id="github.com",
        repo_id=_REPO_ID,
        repo_full_name="acme/api",
        external_id="12345",
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

    # When: each source is mapped within its own canonical envelope.
    github_incident = map_issue_incidents((github,)).incidents[0]
    gitlab_incident = map_issue_incidents((gitlab,)).incidents[0]
    other_org_incident = map_issue_incidents((other_org,)).incidents[0]

    # Then: deterministic identities do not cross tenant or provider boundaries.
    assert github_incident.id != gitlab_incident.id
    assert github_incident.id != other_org_incident.id
    assert gitlab_incident.source_entity_type == "issue"


def test_atlassian_mapper_preserves_incident_alert_and_schedule_lifecycles() -> None:
    # Given: legacy Atlassian Ops rows.
    incident = AtlassianOpsIncident(
        id="incident-1",
        url="https://acme.atlassian.net/ops/incident-1",
        summary="Database unavailable",
        description="Primary database was unavailable.",
        status="closed",
        severity="critical",
        created_at=_AT,
        provider_id="acme-atlassian",
        last_synced=_AT,
    )
    alert = AtlassianOpsAlert(
        id="alert-1",
        status="closed",
        priority="high",
        created_at=_AT,
        acknowledged_at=_AT,
        closed_at=_AT,
        last_synced=_AT,
    )
    schedule = AtlassianOpsSchedule(
        id="schedule-1",
        name="Primary response",
        timezone="UTC",
        last_synced=_AT,
    )

    # When: legacy rows are mapped for canonical backfill.
    batch = map_atlassian_ops_batch(
        AtlassianOpsSource(
            org_id="org-a",
            provider_instance_id="acme-atlassian",
            rows=AtlassianOpsRows(
                incidents=(incident,),
                alerts=(alert,),
                schedules=(schedule,),
            ),
        )
    )

    # Then: each canonical family retains its lifecycle timestamps and provenance.
    assert batch.provider == "atlassian"
    assert batch.incidents[0].source_entity_type == "atlassian_ops_incident"
    assert batch.incidents[0].resolved_at == _AT
    assert batch.alerts[0].acknowledged_at == _AT
    assert batch.alerts[0].resolved_at == _AT
    assert batch.on_call_schedules[0].timezone == "UTC"


@pytest.mark.parametrize(
    ("provider", "provider_instance_id"),
    (("github", "github.com"), ("gitlab", "https://gitlab.com")),
)
def test_issue_mappers_preserve_equivalent_incident_lifecycle(
    provider: str, provider_instance_id: str
) -> None:
    # Given: equivalent issue incidents from each repository provider.
    source = IssueIncidentSource(
        org_id="org-a",
        provider=provider,
        provider_instance_id=provider_instance_id,
        repo_id=_REPO_ID,
        repo_full_name="acme/api",
        external_id="12345",
        issue_number="17",
        source_url=None,
        labels=("incident",),
        raw_status="closed",
        title="Database unavailable",
        description=None,
        created_at=_AT,
        resolved_at=_AT,
        source_version_at=_AT,
    )

    # When: each issue is mapped through the common canonical family mapper.
    batch = map_issue_incidents((source,))

    # Then: canonical count and lifecycle semantics are provider-neutral.
    assert (
        len(batch.services)
        == len(batch.incidents)
        == len(batch.service_repository_mappings)
        == 1
    )
    assert batch.incidents[0].started_at == _AT
    assert batch.incidents[0].resolved_at == _AT
    assert batch.incidents[0].source_entity_type == "issue"
