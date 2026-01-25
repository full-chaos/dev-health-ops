from __future__ import annotations

from datetime import datetime, timezone

from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.jira.normalize import jira_issue_to_work_item
from dev_health_ops.providers.status_mapping import load_status_mapping


def test_jira_issue_dict_parses_status_category_and_changelog() -> None:
    status_mapping = load_status_mapping()
    identity = IdentityResolver(alias_to_canonical={})

    issue = {
        "key": "ABC-1",
        "self": "https://example.atlassian.net/rest/api/3/issue/10000",
        "fields": {
            "project": {"key": "ABC", "id": "10001"},
            "summary": "Test",
            "status": {"name": "Custom Done", "statusCategory": {"key": "done"}},
            "issuetype": {"name": "Task"},
            "labels": [],
            "created": "2025-12-01T10:00:00.000+0000",
            "updated": "2025-12-02T10:00:00.000+0000",
            "resolutiondate": "2025-12-02T10:00:00.000+0000",
        },
        "changelog": {
            "histories": [
                {
                    "created": "2025-12-01T12:00:00.000+0000",
                    "author": {"displayName": "Alice", "accountId": "a1"},
                    "items": [
                        {
                            "field": "status",
                            "fromString": "To Do",
                            "toString": "In Progress",
                        }
                    ],
                },
                {
                    "created": "2025-12-02T10:00:00.000+0000",
                    "author": {"displayName": "Alice", "accountId": "a1"},
                    "items": [
                        {
                            "field": "status",
                            "fromString": "In Progress",
                            "toString": "Custom Done",
                        }
                    ],
                },
            ]
        },
    }

    wi, transitions = jira_issue_to_work_item(
        issue=issue, status_mapping=status_mapping, identity=identity, repo_id=None
    )

    assert wi.work_item_id == "jira:ABC-1"
    assert wi.project_key == "ABC"
    # statusCategory=done should force normalized done even if status name is custom.
    assert wi.status == "done"
    assert wi.started_at == datetime(2025, 12, 1, 12, 0, tzinfo=timezone.utc)
    assert wi.completed_at == datetime(2025, 12, 2, 10, 0, tzinfo=timezone.utc)
    assert len(transitions) == 2
