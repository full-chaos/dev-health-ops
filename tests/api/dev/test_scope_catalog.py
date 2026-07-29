from __future__ import annotations

from typing import Any, cast

import pytest

from dev_health_ops.api.dev.scope_catalog import ClickHouseAuthorizedEntityCatalog
from dev_health_ops.api.dev.scope_service import EntityKind, ScopeRef


@pytest.mark.asyncio
async def test_exact_repository_lookup_is_parameterized_and_tenant_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    async def fake_query_dicts(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.update(sql=sql, params=params)
        return [
            {
                "canonical_id": "repo-a",
                "label": "full-chaos/dev-health",
                "repository_id": "repo-a",
            }
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )

    async def fake_resolve_repo_id(
        _client: object, repo_ref: str, *, org_id: str
    ) -> str:
        assert repo_ref == "full-chaos/dev-health"
        assert org_id == "org-a"
        return "repo-a"

    async def fake_display_names(
        _client: object, *, org_id: str, scope: str, ids: list[str]
    ) -> dict[str, str]:
        assert (org_id, scope, ids) == ("org-a", "repo", ["repo-a"])
        return {"repo-a": "full-chaos/dev-health"}

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.resolve_repo_id", fake_resolve_repo_id
    )
    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.resolve_scope_display_names",
        fake_display_names,
    )
    catalog = ClickHouseAuthorizedEntityCatalog(object())

    result = await catalog.exact(
        "org-a",
        ScopeRef(EntityKind.REPOSITORY, "full-chaos/dev-health"),
        limit=25,
    )

    assert "FROM repos FINAL" in observed["sql"]
    assert "org_id = {org_id:String}" in observed["sql"]
    assert observed["params"] == {
        "org_id": "org-a",
        "query": "full-chaos/dev-health",
        "limit": 25,
    }
    assert result[0].canonical_id == "repo-a"


@pytest.mark.asyncio
async def test_search_queries_only_requested_approved_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sqls: list[str] = []

    async def fake_query_dicts(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert params["org_id"] == "org-a"
        sqls.append(sql)
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )
    catalog = ClickHouseAuthorizedEntityCatalog(object())

    await catalog.search(
        "org-a",
        "ask",
        (EntityKind.PROJECT, EntityKind.ISSUE),
        limit=25,
    )

    assert len(sqls) == 2
    assert any("FROM projects FINAL" in sql for sql in sqls)
    assert any("FROM work_items FINAL" in sql for sql in sqls)
    assert all("org_id = {org_id:String}" in sql for sql in sqls)


def test_catalog_rejects_supporting_evidence_kind() -> None:
    with pytest.raises(ValueError, match="not catalog-searchable"):
        ClickHouseAuthorizedEntityCatalog._query_for(
            cast(EntityKind, "deployment"), exact=False
        )
