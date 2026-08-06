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


#: The live shape CHAOS-3422 reports, reduced to what the ranking can see: a
#: page bound of five, five work items whose titles hold the query as an
#: incidental substring and casefold ahead of the one real project, and that
#: project. Kind-blind, the project sorts sixth and is truncated off the page
#: entirely — the user asked for a project and is offered five things that are
#: not one.
_PREFERRED_KIND_PROJECT_ROW = {
    "canonical_id": "60997592-f9f4-462b-87c3-ef82671df270",
    "label": "Zulu Agent Context Runtime",
}
_PREFERRED_KIND_ISSUE_ROWS = [
    {"canonical_id": f"linear:CHAOS-{2911 + index}", "label": f"{index} runtime task"}
    for index in range(5)
]


def _runtime_rows() -> Any:
    async def fake_query_dicts(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        assert params["org_id"] == "org-a"
        if "FROM work_items FINAL" in sql:
            return list(_PREFERRED_KIND_ISSUE_ROWS)
        if "FROM projects FINAL" in sql:
            return [dict(_PREFERRED_KIND_PROJECT_ROW)]
        return []

    return fake_query_dicts


@pytest.mark.asyncio
async def test_the_production_search_ranks_the_preferred_kind_before_its_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHAOS-3422 at the real catalog, not at a stand-in for it.

    Codex review, confirmed by mutation: every other CHAOS-3422 test drives
    ``SeededCatalog``, which calls ``merge_search_candidates`` itself — so
    dropping ``preferred_kinds`` at *this* call site left the whole suite
    green while the live clarification regressed. The first shape written for
    this test could not fail either: with the project arriving as an *alias*
    hit, CHAOS-3388's own alias-first key already put it first, so removing
    the kind key changed nothing. The project matches by plain substring here,
    which is the only shape where the kind key is the deciding one.
    """

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", _runtime_rows()
    )
    catalog = ClickHouseAuthorizedEntityCatalog(object())

    result = await catalog.search(
        "org-a",
        "runtime",
        (EntityKind.PROJECT, EntityKind.ISSUE),
        limit=5,
        preferred_kinds=frozenset({EntityKind.PROJECT}),
    )

    assert len(result) == 5, "the page bound is unchanged"
    assert result[0].kind is EntityKind.PROJECT
    assert result[0].canonical_id == _PREFERRED_KIND_PROJECT_ROW["canonical_id"]
    assert [entity.kind for entity in result[1:]] == [EntityKind.ISSUE] * 4, (
        "ranking must not filter the other kinds off the page"
    )


@pytest.mark.asyncio
async def test_the_production_search_without_a_preference_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same rows, no preference: today's label-ordered page, verbatim.

    Also the before/after witness for the ticket — with no kind key the real
    project is not merely ranked last, it is off the page.
    """

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", _runtime_rows()
    )
    catalog = ClickHouseAuthorizedEntityCatalog(object())

    result = await catalog.search(
        "org-a",
        "runtime",
        (EntityKind.PROJECT, EntityKind.ISSUE),
        limit=5,
    )

    assert [entity.kind for entity in result] == [EntityKind.ISSUE] * 5
    assert [entity.label for entity in result] == [
        row["label"] for row in _PREFERRED_KIND_ISSUE_ROWS
    ]


def test_catalog_rejects_supporting_evidence_kind() -> None:
    with pytest.raises(ValueError, match="not catalog-searchable"):
        ClickHouseAuthorizedEntityCatalog._query_for(
            cast(EntityKind, "deployment"), exact=False
        )


@pytest.mark.asyncio
async def test_organization_repository_ids_is_tenant_scoped_and_reports_true_total(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[dict[str, Any]] = []

    async def fake_query_dicts(
        _client: object, sql: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        observed.append({"sql": sql, "params": params})
        assert "org_id = {org_id:String}" in sql
        assert params["org_id"] == "org-a"
        assert params["limit"] == 20
        # A single query carries both the page and the true total (via a
        # window function) so a concurrent insert/delete between two
        # separate queries can never desync a truncated page from the count.
        return [
            {"repository_id": f"repo-{index:02}", "total_authorized": 25}
            for index in range(20)
        ]

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )
    catalog = ClickHouseAuthorizedEntityCatalog(object())

    ids, total = await catalog.organization_repository_ids("org-a", limit=20)

    assert total == 25
    assert len(ids) == 20
    assert ids == sorted(ids)
    assert len(observed) == 1
    assert "count() OVER ()" in observed[0]["sql"]
    assert "FROM repos FINAL" in observed[0]["sql"]


@pytest.mark.asyncio
async def test_organization_repository_ids_empty_org_reports_zero_total(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_query_dicts(
        _client: object, _sql: str, _params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return []

    monkeypatch.setattr(
        "dev_health_ops.api.dev.scope_catalog.query_dicts", fake_query_dicts
    )
    catalog = ClickHouseAuthorizedEntityCatalog(object())

    ids, total = await catalog.organization_repository_ids("org-a", limit=20)

    assert ids == []
    assert total == 0
