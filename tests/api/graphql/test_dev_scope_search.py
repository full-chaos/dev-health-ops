from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest

from dev_health_ops.api.dev.scope_catalog import merge_search_candidates
from dev_health_ops.api.dev.scope_service import AuthorizedEntity, EntityKind
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers import dev_entitlement
from dev_health_ops.api.graphql.schema import schema
from dev_health_ops.api.services.auth import AuthenticatedUser

_QUERY = """
query ScopeSearch($orgId: String!, $input: DevScopeSearchInput!) {
  devScopeSearch(orgId: $orgId, input: $input) {
    queryVersion
    catalogWatermark
    candidates { kind canonicalId label repositoryId }
  }
}
"""
ORG_ID = "70d529e0-3c06-4597-8480-794fd02328b6"
OTHER_ORG_ID = "80d529e0-3c06-4597-8480-794fd02328b6"


class FakeCatalog:
    calls = 0

    def __init__(self, _client: object) -> None:
        pass

    async def watermark(self, org_id: str, kinds: tuple[EntityKind, ...]) -> str:
        assert org_id == ORG_ID
        return "2026-07-28T12:00:00+00:00"

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
        preferred_kinds: frozenset[EntityKind] = frozenset(),
    ) -> list[AuthorizedEntity]:
        type(self).calls += 1
        assert org_id == ORG_ID
        assert query == "ask"
        assert kinds == (EntityKind.ISSUE, EntityKind.PROJECT)
        assert limit == 25
        # Ranked and truncated by the production helper, not by hand: the
        # service now preserves the catalog's order instead of re-sorting it
        # (CHAOS-3422), so a double that returned rows in seeded order would
        # be asserting an order the real catalog never emits.
        return merge_search_candidates(
            alias_hits=(),
            substring_hits=[
                AuthorizedEntity(EntityKind.PROJECT, "project-z", "Ask Dev"),
                AuthorizedEntity(EntityKind.ISSUE, "issue-a", "ask dev"),
            ],
            preferred_kinds=preferred_kinds,
            limit=limit,
        )

    async def exact(self, *_args: Any, **_kwargs: Any) -> list[AuthorizedEntity]:
        raise AssertionError("GraphQL scope search must use the shared search service")


def _context(org_id: str = ORG_ID, *, authenticated: bool = True) -> GraphQLContext:
    user = None
    if authenticated:
        user = AuthenticatedUser(
            user_id="user-a",
            email="member@example.com",
            org_id=org_id,
            role="member",
            token_version=7,
        )
    return GraphQLContext(
        org_id=org_id,
        db_url="clickhouse://test",
        client=object(),
        user=user,
    )


@pytest.fixture
def _allow_ask_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def fake_session():
        yield object()

    class AllowedEntitlement:
        async def require(self, _org_id: str) -> None:
            return None

    monkeypatch.setattr(dev_entitlement, "get_postgres_session", fake_session)
    monkeypatch.setattr(
        dev_entitlement,
        "CanonicalAskDevEntitlementAuthorizer",
        lambda _session: AllowedEntitlement(),
    )


@pytest.mark.asyncio
async def test_graphql_scope_search_uses_shared_authorized_service(
    monkeypatch: pytest.MonkeyPatch,
    _allow_ask_dev,
) -> None:
    FakeCatalog.calls = 0
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_scope.ClickHouseAuthorizedEntityCatalog",
        FakeCatalog,
    )

    result = await schema.execute(
        _QUERY,
        variable_values={
            "orgId": ORG_ID,
            "input": {"query": "ask", "kinds": ["PROJECT", "ISSUE"]},
        },
        context_value=_context(),
    )

    assert result.errors is None
    assert FakeCatalog.calls == 1
    assert result.data == {
        "devScopeSearch": {
            "queryVersion": "resolve-scope.v1",
            "catalogWatermark": "2026-07-28T12:00:00+00:00",
            "candidates": [
                {
                    "kind": "ISSUE",
                    "canonicalId": "issue-a",
                    "label": "ask dev",
                    "repositoryId": None,
                },
                {
                    "kind": "PROJECT",
                    "canonicalId": "project-z",
                    "label": "Ask Dev",
                    "repositoryId": None,
                },
            ],
        }
    }


@pytest.mark.asyncio
async def test_graphql_scope_search_rejects_cross_tenant_before_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakeCatalog.calls = 0
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_scope.ClickHouseAuthorizedEntityCatalog",
        FakeCatalog,
    )

    result = await schema.execute(
        _QUERY,
        variable_values={
            "orgId": OTHER_ORG_ID,
            "input": {"query": "ask", "kinds": ["PROJECT"]},
        },
        context_value=_context(),
    )

    assert result.errors is not None
    assert result.data is None
    assert FakeCatalog.calls == 0


@pytest.mark.asyncio
async def test_graphql_scope_search_requires_authenticated_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakeCatalog.calls = 0
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_scope.ClickHouseAuthorizedEntityCatalog",
        FakeCatalog,
    )

    result = await schema.execute(
        _QUERY,
        variable_values={
            "orgId": ORG_ID,
            "input": {"query": "ask", "kinds": ["PROJECT"]},
        },
        context_value=_context(authenticated=False),
    )

    assert result.errors is not None
    assert FakeCatalog.calls == 0


@pytest.mark.asyncio
async def test_graphql_scope_search_does_not_expose_supporting_evidence_kinds() -> None:
    result = await schema.execute(
        _QUERY,
        variable_values={
            "orgId": ORG_ID,
            "input": {"query": "prod", "kinds": ["DEPLOYMENT"]},
        },
        context_value=_context(),
    )

    assert result.errors is not None


@pytest.mark.asyncio
async def test_graphql_scope_search_requires_canonical_ask_dev_entitlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    FakeCatalog.calls = 0

    @asynccontextmanager
    async def fake_session():
        yield object()

    class DeniedEntitlement:
        async def require(self, _org_id: str) -> None:
            raise RuntimeError("denied")

    monkeypatch.setattr(dev_entitlement, "get_postgres_session", fake_session)
    monkeypatch.setattr(
        dev_entitlement,
        "CanonicalAskDevEntitlementAuthorizer",
        lambda _session: DeniedEntitlement(),
    )
    monkeypatch.setattr(
        "dev_health_ops.api.graphql.resolvers.dev_scope.ClickHouseAuthorizedEntityCatalog",
        FakeCatalog,
    )

    result = await schema.execute(
        _QUERY,
        variable_values={
            "orgId": ORG_ID,
            "input": {"query": "ask", "kinds": ["PROJECT"]},
        },
        context_value=_context(),
    )

    assert result.errors is not None
    assert result.data is None
    assert FakeCatalog.calls == 0
