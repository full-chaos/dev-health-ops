from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from dev_health_ops.processors.github import _fetch_github_incidents_async
from dev_health_ops.processors.gitlab import _fetch_gitlab_incidents_sync
from dev_health_ops.providers.operational_migration import IssueIncidentSource

_AT = datetime(2026, 7, 17, tzinfo=timezone.utc)
_REPO_ID = UUID("00000000-0000-0000-0000-000000000101")


class _GitHubIssueClient:
    def __init__(self, issue: SimpleNamespace) -> None:
        self._issue = issue
        self.close = AsyncMock()

    async def iter_issues(
        self, _owner: str, _repo: str, **_kwargs: object
    ) -> list[SimpleNamespace]:
        return [self._issue]

    def drain_usage_observations(self) -> list[dict[str, str]]:
        return []


@pytest.mark.asyncio
async def test_github_incident_fetch_retains_full_issue_for_canonical_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a live GitHub issue with the metadata absent from legacy incidents.
    from dev_health_ops.processors import github

    issue = SimpleNamespace(
        issue_id="github-incident-1",
        number=17,
        state="closed",
        created_at=_AT,
        closed_at=_AT,
        updated_at=_AT,
        source_url="https://github.com/acme/api/issues/17",
        title="Database unavailable",
        description="The primary database was unavailable.",
        labels=("incident", "sev-1"),
    )
    client = _GitHubIssueClient(issue)
    monkeypatch.setattr(github, "resolve_incident_labels", lambda: ["incident"])
    monkeypatch.setattr(
        github, "_github_code_client_from_connector", lambda _connector: client
    )
    sources: list[IssueIncidentSource] = []

    # When: the legacy producer fetches its incident row.
    incidents = await _fetch_github_incidents_async(
        Mock(),
        "acme",
        "api",
        _REPO_ID,
        10,
        None,
        canonical_sources=sources,
        canonical_org_id="org-a",
        canonical_provider_instance_id="github.com",
    )

    # Then: the additive canonical source retains lifecycle and issue metadata.
    assert [incident.incident_id for incident in incidents] == ["github-incident-1"]
    assert len(sources) == 1
    assert sources[0].source_url == "https://github.com/acme/api/issues/17"
    assert sources[0].issue_number == "17"
    assert sources[0].labels == ("incident", "sev-1")
    assert sources[0].resolved_at == _AT


def test_gitlab_incident_fetch_retains_full_issue_for_canonical_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: a live GitLab issue with metadata absent from legacy incidents.
    from dev_health_ops.processors import gitlab

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
        }
    ]
    monkeypatch.setattr(gitlab, "resolve_incident_labels", lambda: ["incident"])
    sources: list[IssueIncidentSource] = []

    # When: the legacy producer fetches its incident row.
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

    # Then: the additive canonical source retains lifecycle and issue metadata.
    assert [incident.incident_id for incident in incidents] == ["gitlab-incident-1"]
    assert len(sources) == 1
    assert sources[0].source_url == "https://gitlab.com/acme/api/-/issues/17"
    assert sources[0].issue_number == "17"
    assert sources[0].labels == ("incident", "severity::high")
    assert sources[0].resolved_at == _AT
