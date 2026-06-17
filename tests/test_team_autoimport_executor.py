from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader


@pytest.mark.asyncio
async def test_loader_builds_team_attribution_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)
    calls: list[str] = []

    async def fake_query(_client: object, query: str, _params: dict[str, object]):
        calls.append(query)
        if "team_project_ownership" in query:
            return [
                {
                    "provider": "linear",
                    "team_id": "team-project",
                    "team_name": "Project Team",
                    "project_id": "project-1",
                    "project_key": "PROJ",
                    "is_primary": 1,
                    "specificity": 80,
                    "priority": 5,
                    "updated_at": now,
                }
            ]
        if "team_repo_ownership" in query:
            return [
                {
                    "provider": "github",
                    "team_id": "team-repo",
                    "team_name": "Repo Team",
                    "repo_id": None,
                    "repo_full_name": "full-chaos/dev-health",
                    "is_primary": 1,
                    "specificity": 60,
                    "priority": 10,
                    "updated_at": now,
                }
            ]
        return [
            {
                "provider": "jira",
                "team_id": "team-member",
                "team_name": "Member Team",
                "member_id": "member-1",
                "raw_provider_user_id": "jira-user-1",
                "raw_email": "ADA@EXAMPLE.COM",
                "is_primary": 1,
                "specificity": 50,
                "priority": 20,
                "updated_at": now,
            }
        ]

    monkeypatch.setattr(
        "dev_health_ops.metrics.loaders.clickhouse._clickhouse_query_dicts",
        fake_query,
    )

    context = await ClickHouseDataLoader(
        object(), org_id="org-1"
    ).load_team_attribution_context(as_of=now)

    assert len(calls) == 3
    assert context.project_by_id[("linear", "project-1")][0].team_id == "team-project"
    assert context.project_by_key[("linear", "PROJ")][0].team_id == "team-project"
    assert (
        context.repo_by_name[("github", "full-chaos/dev-health")][0].team_id
        == "team-repo"
    )
    assert (
        context.member_by_identity[("jira", "ada@example.com")][0].team_id
        == "team-member"
    )
