from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import UUID

import pytest

from dev_health_ops.processors.gitlab import _fetch_gitlab_incidents_sync
from dev_health_ops.providers.operational_migration import (
    IssueIncidentSource,
    map_issue_incidents,
)

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)
_REPO_ID = UUID("00000000-0000-0000-0000-000000000101")


def test_gitlab_incident_fetch_retains_full_issue_for_canonical_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a live GitLab issue with metadata absent from legacy incidents.
    connector = Mock()
    connector.rest_client.get_issues.return_value = [
        {
            "id": "gitlab-incident-1",
            "iid": 17,
            "state": "closed",
            "created_at": "2026-07-17T00:00:00Z",
            "closed_at": "2026-07-17T00:00:00Z",
            "updated_at": "2026-07-17T00:00:00Z",
            "web_url": "https://gitlab.com/acme/api/-/issues/17",
            "title": "Database unavailable",
            "description": "The primary database was unavailable.",
            "labels": ["incident", "severity::high"],
            "issue_type": "incident",
            "severity": "HIGH",
        }
    ]
    sources: list[IssueIncidentSource] = []

    # When: the native producer fetches its canonical incident source.
    incidents = _fetch_gitlab_incidents_sync(
        connector,
        123,
        _REPO_ID,
        10,
        None,
        canonical_sources=sources,
        canonical_org_id="org-a",
        canonical_provider_instance_id="https://gitlab.com",
        repo_full_name="acme/api",
    )

    # Then: the canonical source retains lifecycle and issue metadata.
    assert [incident.incident_id for incident in incidents] == ["gitlab-incident-1"]
    assert len(sources) == 1
    assert sources[0].source_url == "https://gitlab.com/acme/api/-/issues/17"
    assert sources[0].issue_number == "17"
    assert sources[0].labels == ("incident", "severity::high")
    assert sources[0].resolved_at == _AT
    assert sources[0].source_entity_type == "incident"
    assert sources[0].raw_severity == "HIGH"
    mapped = map_issue_incidents(sources).incidents[0]
    assert mapped.source_entity_type == "incident"
    assert mapped.raw_severity == "HIGH"
    assert mapped.normalized_severity == "high"
