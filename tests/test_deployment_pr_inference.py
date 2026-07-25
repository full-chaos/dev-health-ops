"""Deployment-to-PR inference and native GitLab incident selection.

Deployment syncs previously ignored the deployed commit SHA, leaving
``deployments.pull_request_number`` NULL everywhere — so the PR↔deployment
work-graph edges (#886) had no native source data.
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
from dev_health_ops.processors.github import _fetch_github_deployments_async
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


class TestGitLabNativeIncidents:
    def test_gitlab_queries_native_incidents(self, monkeypatch) -> None:
        """rest_client is spec'd to the real GitLabRESTClient so a missing
        method (the original silent-no-op bug) fails loudly here."""
        incident = {
            "id": 1,
            "issue_type": "incident",
            "state": "closed",
            "created_at": "2026-06-12T00:00:00Z",
        }
        connector = Mock()
        connector.rest_client = Mock(spec=GitLabRESTClient)
        connector.rest_client.get_issues.return_value = [incident]

        incidents = _fetch_gitlab_incidents_sync(connector, 123, REPO_ID, 10, None)

        assert [i.incident_id for i in incidents] == ["1"]
        connector.rest_client.get_issues.assert_called_once()
        call = connector.rest_client.get_issues.call_args
        assert call.kwargs["issue_type"] == "incident"
        assert "labels" not in call.kwargs
