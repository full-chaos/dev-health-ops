"""Provider matrix for the CHAOS-2467 team/project field split.

Asserts that every provider normalizer populates the new WorkItem fields
correctly:

* ``native_team_key`` is the raw provider team key for Linear (``issue.team.key``)
  and ``None`` for GitHub / GitLab / Jira (their work items have no native team).
* ``project_key`` / ``project_id`` / ``project_name`` are no longer conflated:
  for Linear ``project_key`` is no longer the team key and ``project_id`` is the
  real project id (not the project name).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from dev_health_ops.models.work_items import WorkItem, WorkItemProvider
from dev_health_ops.providers.github.normalize import (
    enrich_work_item_with_priority as github_enrich,
)
from dev_health_ops.providers.github.normalize import github_issue_to_work_item
from dev_health_ops.providers.gitlab.normalize import (
    enrich_work_item_with_priority as gitlab_enrich,
)
from dev_health_ops.providers.gitlab.normalize import gitlab_issue_to_work_item
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.jira.normalize import jira_issue_to_work_item
from dev_health_ops.providers.linear.normalize import linear_issue_to_work_item
from dev_health_ops.providers.status_mapping import load_status_mapping


def _identity() -> IdentityResolver:
    return IdentityResolver(alias_to_canonical={})


def _github_issue() -> MagicMock:
    issue = MagicMock()
    issue.number = 7
    issue.title = "GH issue"
    issue.state = "open"
    issue.body = ""
    issue.created_at = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    issue.updated_at = issue.created_at
    issue.closed_at = None
    issue.html_url = "https://github.com/owner/repo/issues/7"
    issue.pull_request = None
    issue.labels = []
    issue.assignees = []
    issue.user = None
    return issue


def test_linear_sets_native_team_key_and_splits_project() -> None:
    status_mapping = load_status_mapping()
    issue = {
        "identifier": "CHAOS-1",
        "title": "Linear in-project issue",
        "state": {"name": "In Progress", "type": "started"},
        "team": {"key": "CHAOS"},
        "project": {"id": "proj-uuid-123", "name": "Q1 Platform Revamp"},
    }

    wi, _ = linear_issue_to_work_item(
        issue=issue, status_mapping=status_mapping, identity=_identity()
    )

    # Team key now lives in native_team_key, NOT project_key.
    assert wi.native_team_key == "CHAOS"
    assert wi.project_key is None
    # Project id is the real id; the name moved to project_name.
    assert wi.project_id == "proj-uuid-123"
    assert wi.project_name == "Q1 Platform Revamp"


def test_jira_sets_project_name_and_no_native_team() -> None:
    status_mapping = load_status_mapping()
    issue = {
        "key": "ABC-1",
        "fields": {
            "project": {"key": "ABC", "id": "10001", "name": "Backend"},
            "summary": "Jira issue",
            "status": {"name": "Done", "statusCategory": {"key": "done"}},
            "issuetype": {"name": "Task"},
            "labels": [],
            "created": "2025-12-01T10:00:00.000+0000",
            "updated": "2025-12-02T10:00:00.000+0000",
        },
    }

    wi, _ = jira_issue_to_work_item(
        issue=issue, status_mapping=status_mapping, identity=_identity(), repo_id=None
    )

    assert wi.native_team_key is None
    assert wi.project_key == "ABC"
    assert wi.project_id == "10001"
    assert wi.project_name == "Backend"


def test_github_work_item_has_no_native_team() -> None:
    status_mapping = load_status_mapping()
    wi, _ = github_issue_to_work_item(
        issue=_github_issue(),
        repo_full_name="owner/repo",
        repo_id=None,
        status_mapping=status_mapping,
        identity=_identity(),
    )

    assert wi.native_team_key is None
    # Repo carries scope; no project name is threaded for GitHub.
    assert wi.project_name is None


def test_gitlab_work_item_has_no_native_team() -> None:
    status_mapping = load_status_mapping()
    issue = {
        "iid": 42,
        "title": "GL issue",
        "state": "opened",
        "created_at": "2025-01-01T10:00:00Z",
        "updated_at": "2025-01-15T12:00:00Z",
        "labels": [],
        "assignees": [],
        "author": {"username": "reporter1", "name": "Reporter One"},
        "web_url": "https://gitlab.com/group/project/-/issues/42",
    }

    wi, _ = gitlab_issue_to_work_item(
        issue=issue,
        project_full_path="group/project",
        repo_id=None,
        status_mapping=status_mapping,
        identity=_identity(),
    )

    assert wi.native_team_key is None
    assert wi.project_name is None


def _sentinel_work_item(provider: WorkItemProvider) -> WorkItem:
    """A work item with EVERY optional field set to a sentinel.

    Used to prove the priority-enrichment copy preserves all fields instead of
    silently dropping any (the dataclasses.replace contract).
    """
    return WorkItem(
        work_item_id=f"{provider}:owner/repo#1",
        provider=provider,
        title="t",
        type="task",
        status="todo",
        status_raw="open",
        native_team_key="SENTINEL-TEAM",
        project_key="pk",
        project_id="pid",
        project_name="Sentinel Project",
        parent_id="parent-1",
        epic_id="epic-1",
        due_at=datetime(2025, 6, 1, tzinfo=timezone.utc),
        org_id="org-9",
    )


def test_github_enrichment_preserves_all_fields() -> None:
    wi = _sentinel_work_item("github")
    enriched = github_enrich(wi, ["critical"])

    assert enriched.priority_raw is not None  # the copy path actually ran
    assert enriched.native_team_key == "SENTINEL-TEAM"
    assert enriched.project_name == "Sentinel Project"
    assert enriched.parent_id == "parent-1"
    assert enriched.epic_id == "epic-1"
    assert enriched.due_at == wi.due_at
    assert enriched.org_id == "org-9"


def test_gitlab_enrichment_preserves_all_fields() -> None:
    wi = _sentinel_work_item("gitlab")
    enriched = gitlab_enrich(wi, ["critical"])

    assert enriched.priority_raw is not None
    assert enriched.native_team_key == "SENTINEL-TEAM"
    assert enriched.project_name == "Sentinel Project"
    assert enriched.parent_id == "parent-1"
    assert enriched.epic_id == "epic-1"
    assert enriched.due_at == wi.due_at
    assert enriched.org_id == "org-9"
