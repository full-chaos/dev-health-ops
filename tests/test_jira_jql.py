from __future__ import annotations

from dev_health_ops.providers.jira.client import build_jira_jql


def test_build_jira_jql_formats_order_by() -> None:
    assert (
        build_jira_jql(updated_since="2025-11-19")
        == "updated >= '2025-11-19' ORDER BY updated DESC"
    )
    assert build_jira_jql(project_key="ABC") == "project = 'ABC' ORDER BY updated DESC"
    assert (
        build_jira_jql(project_key="ABC", updated_since="2025-11-19")
        == "project = 'ABC' AND updated >= '2025-11-19' ORDER BY updated DESC"
    )
    assert (
        build_jira_jql(updated_since="2025-09-10", active_until="2025-12-18")
        == "(updated >= '2025-09-10' OR (statusCategory != Done AND created <= '2025-12-18')) ORDER BY updated DESC"
    )
