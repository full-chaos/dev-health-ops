from __future__ import annotations

from typing import TypedDict

import pytest

from dev_health_ops.api.graphql.loaders.repo_loader import RepoByNameLoader, RepoLoader
from dev_health_ops.api.graphql.loaders.team_loader import TeamByNameLoader, TeamLoader


class _CapturedQuery(TypedDict):
    sql: str
    params: dict[str, object]


@pytest.mark.asyncio
async def test_repo_loader_scopes_query_by_org_id(monkeypatch):
    captured: _CapturedQuery = {"sql": "", "params": {}}

    async def fake_query_dicts(_client, sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    loader = RepoLoader(client=object(), org_id="org-A")
    await loader.batch_load(["repo-1"])

    assert "AND org_id = %(org_id)s" in str(captured["sql"])
    assert captured["params"]["org_id"] == "org-A"


@pytest.mark.asyncio
async def test_repo_by_name_loader_scopes_query_by_org_id(monkeypatch):
    captured: _CapturedQuery = {"sql": "", "params": {}}

    async def fake_query_dicts(_client, sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    loader = RepoByNameLoader(client=object(), org_id="org-A")
    await loader.batch_load(["Repo-One"])

    assert "AND org_id = %(org_id)s" in str(captured["sql"])
    assert captured["params"]["org_id"] == "org-A"


@pytest.mark.asyncio
async def test_team_loader_scopes_query_by_org_id(monkeypatch):
    captured: _CapturedQuery = {"sql": "", "params": {}}

    async def fake_query_dicts(_client, sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    loader = TeamLoader(client=object(), org_id="org-A")
    await loader.batch_load(["team-1"])

    assert "AND org_id = %(org_id)s" in str(captured["sql"])
    assert captured["params"]["org_id"] == "org-A"


@pytest.mark.asyncio
async def test_team_by_name_loader_scopes_query_by_org_id(monkeypatch):
    captured: _CapturedQuery = {"sql": "", "params": {}}

    async def fake_query_dicts(_client, sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.query_dicts", fake_query_dicts
    )

    loader = TeamByNameLoader(client=object(), org_id="org-A")
    await loader.batch_load(["Core Team"])

    assert "AND org_id = %(org_id)s" in str(captured["sql"])
    assert captured["params"]["org_id"] == "org-A"
