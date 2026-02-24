from __future__ import annotations

import pytest

import dev_health_ops.api.queries.scopes as scopes


@pytest.mark.asyncio
async def test_resolve_repo_id_uuid_returns_none_when_org_does_not_own_repo(
    monkeypatch,
):
    repo_id = "11111111-1111-1111-1111-111111111111"

    async def _fake_query_dicts(_sink, query: str, params):
        assert "WHERE id = %(repo_id)s" in query
        assert params == {"repo_id": repo_id, "org_id": "org-b"}
        return []

    monkeypatch.setattr(scopes, "query_dicts", _fake_query_dicts)

    resolved = await scopes.resolve_repo_id(object(), repo_id, org_id="org-b")

    assert resolved is None


@pytest.mark.asyncio
async def test_resolve_repo_id_uuid_returns_id_when_org_owns_repo(monkeypatch):
    repo_id = "22222222-2222-2222-2222-222222222222"

    async def _fake_query_dicts(_sink, query: str, params):
        assert "WHERE id = %(repo_id)s" in query
        assert params == {"repo_id": repo_id, "org_id": "org-a"}
        return [{"id": repo_id}]

    monkeypatch.setattr(scopes, "query_dicts", _fake_query_dicts)

    resolved = await scopes.resolve_repo_id(object(), repo_id, org_id="org-a")

    assert resolved == repo_id


@pytest.mark.asyncio
async def test_resolve_repo_ids_mixed_refs_are_org_scoped(monkeypatch):
    owned_uuid = "33333333-3333-3333-3333-333333333333"
    foreign_uuid = "44444444-4444-4444-4444-444444444444"
    calls = []

    async def _fake_query_dicts(_sink, query: str, params):
        calls.append({"query": query, "params": params})
        if "WHERE id = %(repo_id)s" in query:
            if params == {"repo_id": owned_uuid, "org_id": "org-a"}:
                return [{"id": owned_uuid}]
            return []
        if "WHERE repo = %(repo_name)s" in query:
            if params == {"repo_name": "org-a/repo-1", "org_id": "org-a"}:
                return [{"id": "repo-name-id"}]
            return []
        return []

    monkeypatch.setattr(scopes, "query_dicts", _fake_query_dicts)

    resolved = await scopes.resolve_repo_ids(
        object(),
        [owned_uuid, foreign_uuid, "org-a/repo-1", "org-b/repo-2"],
        org_id="org-a",
    )

    assert resolved == [owned_uuid, "repo-name-id"]
    assert all(call["params"]["org_id"] == "org-a" for call in calls)
