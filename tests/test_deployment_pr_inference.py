"""CHAOS-2368/CHAOS-2369: deployment→PR inference + configurable incident labels.

Deployment syncs previously ignored the deployed commit SHA, leaving
``deployments.pull_request_number`` NULL everywhere — so the PR↔deployment
work-graph edges (#886) had no native source data. Incident syncs hardcoded
the exact label ``incident`` with no diagnosability.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.connectors.utils.rest import GitLabRESTClient
from dev_health_ops.processors.base_git import resolve_incident_labels
from dev_health_ops.processors.github import (
    _fetch_github_deployments_async,
    _fetch_github_incidents_sync,
)
from dev_health_ops.processors.gitlab import (
    _fetch_gitlab_incidents_sync,
    _resolve_gitlab_deployment_mr_from_items,
)

REPO_ID = uuid.uuid4()
NOW = datetime(2026, 6, 12, tzinfo=timezone.utc)


class _NoWaitGate:
    def __init__(self) -> None:
        self.waits = 0

    def wait_sync(self) -> None:
        self.waits += 1


class TestGitHubDeploymentPRInference:
    def test_deployments_carry_pr_attribution(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        asyncio.run(self._test_deployments_carry_pr_attribution(monkeypatch))

    async def _test_deployments_carry_pr_attribution(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dev_health_ops.providers.github.code_client import GitHubDeploymentData

        class FakeDeploymentClient:
            def __init__(self) -> None:
                self.closed = False

            async def get_deployment_releases(self, owner, repo_name, *, max_releases):
                return []

            async def get_deployments(self, owner, repo_name, *, max_deployments):
                return [
                    GitHubDeploymentData(
                        deployment_id="99",
                        state="success",
                        environment="prod",
                        created_at=NOW,
                        sha="abc123",
                        ref=None,
                        tag=None,
                        tag_name=None,
                        payload=None,
                    )
                ]

            async def get_deployment_pull_request(self, owner, repo_name, sha):
                return 42, NOW

            def drain_usage_observations(self):
                return [{"route_family": "deployments", "request_count": 3}]

            async def close(self) -> None:
                self.closed = True

        fake_client = FakeDeploymentClient()
        monkeypatch.setattr(
            "dev_health_ops.processors.github._github_code_client_from_connector",
            lambda connector: fake_client,
        )
        usage_sink: list[dict[str, object]] = []

        deployments = await _fetch_github_deployments_async(
            Mock(), "acme", "widgets", REPO_ID, 10, None, usage_sink=usage_sink
        )

        assert len(deployments) == 1
        assert deployments[0].pull_request_number == 42
        assert deployments[0].merged_at == NOW
        assert usage_sink == [{"route_family": "deployments", "request_count": 3}]
        assert fake_client.closed is True


class TestGitLabDeploymentMRInference:
    def test_resolves_merged_mr(self) -> None:
        merge_requests = [
            {"iid": 5, "state": "opened", "merged_at": None},
            {"iid": 6, "state": "merged", "merged_at": "2026-06-12T00:00:00Z"},
        ]

        number, merged_at = _resolve_gitlab_deployment_mr_from_items(merge_requests)

        assert number == 6
        assert merged_at is not None

    def test_no_sha_or_failure_is_soft(self) -> None:
        assert _resolve_gitlab_deployment_mr_from_items([]) == (None, None)
        assert _resolve_gitlab_deployment_mr_from_items(object()) == (None, None)

    def test_non_numeric_iid_is_soft(self) -> None:
        merge_requests = [
            {"iid": "abc", "state": "merged", "merged_at": "2026-06-12T00:00:00Z"}
        ]
        number, merged_at = _resolve_gitlab_deployment_mr_from_items(merge_requests)
        assert number is None
        assert merged_at is not None


class TestIncidentLabels:
    def test_default_label(self, monkeypatch) -> None:
        monkeypatch.delenv("INCIDENT_LABELS", raising=False)
        assert resolve_incident_labels() == ["incident"]

    def test_env_override_and_whitespace(self, monkeypatch) -> None:
        monkeypatch.setenv("INCIDENT_LABELS", " incident , outage ,sev1,")
        assert resolve_incident_labels() == ["incident", "outage", "sev1"]

    def test_empty_env_falls_back(self, monkeypatch) -> None:
        monkeypatch.setenv("INCIDENT_LABELS", " , ")
        assert resolve_incident_labels() == ["incident"]

    def test_github_queries_each_label_and_dedupes(self, monkeypatch) -> None:
        monkeypatch.setenv("INCIDENT_LABELS", "incident,outage")
        shared = Mock(id=1, state="closed", created_at=NOW, closed_at=NOW)
        shared.pull_request = None
        outage_only = Mock(id=2, state="open", created_at=NOW, closed_at=None)
        outage_only.pull_request = None
        gh_repo = Mock()
        gh_repo.get_issues.side_effect = [[shared], [shared, outage_only]]

        incidents = _fetch_github_incidents_sync(gh_repo, REPO_ID, 10, None)

        assert {i.incident_id for i in incidents} == {"1", "2"}
        assert gh_repo.get_issues.call_count == 2
        labels_queried = [
            call.kwargs["labels"] for call in gh_repo.get_issues.call_args_list
        ]
        assert labels_queried == [["incident"], ["outage"]]

    def test_github_filters_prs_carrying_incident_label(self, monkeypatch) -> None:
        monkeypatch.delenv("INCIDENT_LABELS", raising=False)
        real_issue = Mock(id=1, state="closed", created_at=NOW, closed_at=NOW)
        real_issue.pull_request = None
        labeled_pr = Mock(id=2, state="open", created_at=NOW, closed_at=None)
        labeled_pr.pull_request = Mock()  # the issues API includes PRs
        gh_repo = Mock()
        gh_repo.get_issues.return_value = [real_issue, labeled_pr]

        incidents = _fetch_github_incidents_sync(gh_repo, REPO_ID, 10, None)

        assert [i.incident_id for i in incidents] == ["1"]

    def test_gitlab_queries_each_label_and_dedupes(self, monkeypatch) -> None:
        """rest_client is spec'd to the real GitLabRESTClient so a missing
        method (the original silent-no-op bug) fails loudly here."""
        monkeypatch.setenv("INCIDENT_LABELS", "incident,outage")
        shared = {"id": 1, "state": "closed", "created_at": "2026-06-12T00:00:00Z"}
        outage_only = {"id": 2, "state": "opened", "created_at": "2026-06-12T01:00:00Z"}
        connector = Mock()
        connector.rest_client = Mock(spec=GitLabRESTClient)
        connector.rest_client.get_issues.side_effect = [[shared], [shared, outage_only]]

        incidents = _fetch_gitlab_incidents_sync(connector, 123, REPO_ID, 10, None)

        assert {i.incident_id for i in incidents} == {"1", "2"}
        assert connector.rest_client.get_issues.call_count == 2
        first_call = connector.rest_client.get_issues.call_args_list[0]
        assert first_call.kwargs["labels"] == "incident"
