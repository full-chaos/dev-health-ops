"""CHAOS-2368/CHAOS-2369: deployment→PR inference + configurable incident labels.

Deployment syncs previously ignored the deployed commit SHA, leaving
``deployments.pull_request_number`` NULL everywhere — so the PR↔deployment
work-graph edges (#886) had no native source data. Incident syncs hardcoded
the exact label ``incident`` with no diagnosability.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import Mock

# Initialize the connectors package before processors to avoid the
# pre-existing providers._base <-> connectors circular import when this file
# is collected in isolation.
import dev_health_ops.connectors  # noqa: F401
from dev_health_ops.connectors.utils.rest import GitLabRESTClient
from dev_health_ops.processors.base_git import resolve_incident_labels
from dev_health_ops.processors.github import (
    _fetch_github_deployments_sync,
    _fetch_github_incidents_sync,
    _resolve_github_deployment_pr,
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
    def test_resolves_merged_pr(self) -> None:
        merged_pr = Mock(number=42, merged_at=NOW, merge_commit_sha="abc123")
        open_pr = Mock(number=43, merged_at=None)
        commit = Mock()
        commit.get_pulls.return_value = [open_pr, merged_pr]
        gh_repo = Mock()
        gh_repo.get_commit.return_value = commit
        gate = _NoWaitGate()

        number, merged_at = _resolve_github_deployment_pr(gh_repo, "abc123", gate)

        assert (number, merged_at) == (42, NOW)
        gh_repo.get_commit.assert_called_once_with("abc123")
        assert gate.waits == 2

    def test_prefers_pr_that_directly_merged_the_sha(self) -> None:
        """get_pulls() also returns PRs that merely contain the SHA
        (stacked merges); the direct merger must win regardless of order."""
        containing_pr = Mock(number=100, merged_at=NOW, merge_commit_sha="other")
        direct_pr = Mock(number=42, merged_at=NOW, merge_commit_sha="abc123")
        commit = Mock()
        commit.get_pulls.return_value = [containing_pr, direct_pr]
        gh_repo = Mock()
        gh_repo.get_commit.return_value = commit

        number, _ = _resolve_github_deployment_pr(gh_repo, "abc123", _NoWaitGate())

        assert number == 42

    def test_falls_back_to_first_pr_when_none_merged(self) -> None:
        open_pr = Mock(number=7, merged_at=None)
        commit = Mock()
        commit.get_pulls.return_value = [open_pr]
        gh_repo = Mock()
        gh_repo.get_commit.return_value = commit

        number, merged_at = _resolve_github_deployment_pr(
            gh_repo, "abc123", _NoWaitGate()
        )

        assert (number, merged_at) == (7, None)

    def test_no_sha_skips_lookup(self) -> None:
        gh_repo = Mock()
        assert _resolve_github_deployment_pr(gh_repo, None, _NoWaitGate()) == (
            None,
            None,
        )
        gh_repo.get_commit.assert_not_called()

    def test_lookup_failure_is_soft(self) -> None:
        gh_repo = Mock()
        gh_repo.get_commit.side_effect = RuntimeError("boom")
        assert _resolve_github_deployment_pr(gh_repo, "abc", _NoWaitGate()) == (
            None,
            None,
        )

    def test_deployments_carry_pr_attribution(self) -> None:
        dep = Mock(id=99, state="success", environment="prod", created_at=NOW)
        dep.sha = "abc123"
        merged_pr = Mock(number=42, merged_at=NOW)
        commit = Mock()
        commit.get_pulls.return_value = [merged_pr]
        gh_repo = Mock()
        gh_repo.get_deployments.return_value = [dep]
        gh_repo.get_releases.return_value = []
        gh_repo.get_commit.return_value = commit

        deployments = _fetch_github_deployments_sync(
            gh_repo, REPO_ID, 10, None, _NoWaitGate()
        )

        assert len(deployments) == 1
        assert deployments[0].pull_request_number == 42
        assert deployments[0].merged_at == NOW


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
