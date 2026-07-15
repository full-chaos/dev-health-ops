from __future__ import annotations

import uuid

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.schema import schema
from dev_health_ops.api.services.auth import AuthenticatedUser

_QUERY = """
query ACRRepositoryScopes($orgId: String!) {
  catalog(orgId: $orgId, dimension: REPO) {
    values { value count }
  }
}
"""

_ARBITRARY_VARIABLE_QUERY = """
query ACRRepositoryScopes($target: String!) {
  catalog(orgId: $target, dimension: REPO) {
    values { value count }
  }
}
"""

_INLINE_QUERY = """
query ACRRepositoryScopes {
  catalog(orgId: "org-b", dimension: REPO) {
    values { value count }
  }
}
"""

_TRAILING_FRAGMENT_QUERY = """
query ACRRepositoryScopes($orgId: String!) {
  catalogA: catalog(orgId: $orgId, dimension: REPO) {
    values { value count }
  }
  ...CrossOrganizationCatalog
}

fragment CrossOrganizationCatalog on Query {
  catalogB: catalog(orgId: "org-b", dimension: REPO) {
    values { value count }
  }
}
"""


def _context(
    org_id: str,
    *,
    is_superuser: bool = False,
    is_superuser_verified: bool = True,
    client: object | None = None,
) -> GraphQLContext:
    user = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="member@example.com",
        org_id=org_id,
        role="member",
        is_superuser=is_superuser,
    )
    user.is_superuser_verified = is_superuser_verified
    return GraphQLContext(
        org_id=org_id,
        db_url="clickhouse://test",
        client=client or object(),
        user=user,
    )


@pytest.mark.asyncio
async def test_acr_repository_scopes_returns_sorted_unique_canonical_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_org_ids: list[str] = []

    async def _query_dicts(_client: object, _sql: str, params: dict[str, object]):
        assert "FROM repos FINAL" in _sql
        observed_org_ids.append(str(params["org_id"]))
        return [
            {"value": "Zeta/Api", "count": 2},
            {"value": " Acme/API ", "count": 3},
            {"value": "ACME/API", "count": 4},
            {"value": "not-a-slug", "count": 9},
            {"value": "other//extra", "count": 5},
            {"value": "group/subgroup/repository", "count": 6},
            {"value": "acme/trailing-", "count": 7},
            {"value": "acme/leading_", "count": 8},
            {"value": f"acme/{'a' * 101}", "count": 9},
        ]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _QUERY,
        variable_values={"orgId": "org-a"},
        context_value=_context("org-a"),
    )

    assert result.errors is None
    assert observed_org_ids == ["org-a"]
    assert result.data == {
        "catalog": {
            "values": [
                {"value": "acme/api", "count": 7},
                {"value": "zeta/api", "count": 2},
            ]
        }
    }


@pytest.mark.asyncio
async def test_acr_repository_scopes_rejects_cross_organization_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def _query_dicts(_client: object, _sql: str, _params: dict[str, object]):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _QUERY,
        variable_values={"orgId": "org-b"},
        context_value=_context("org-a"),
    )

    assert result.data is None
    assert result.errors is not None
    assert called is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "variables"),
    [
        (_ARBITRARY_VARIABLE_QUERY, {"target": "org-b"}),
        (_INLINE_QUERY, {}),
    ],
)
async def test_acr_repository_scopes_rejects_noncanonical_cross_organization_syntax(
    monkeypatch: pytest.MonkeyPatch,
    query: str,
    variables: dict[str, str],
) -> None:
    called = False

    async def _query_dicts(_client: object, _sql: str, _params: dict[str, object]):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        query,
        variable_values=variables,
        context_value=_context("org-a"),
    )

    assert result.data is None
    assert result.errors is not None
    assert called is False


@pytest.mark.asyncio
async def test_acr_repository_scopes_rejects_cross_organization_in_trailing_fragment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def _query_dicts(_client: object, _sql: str, _params: dict[str, object]):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _TRAILING_FRAGMENT_QUERY,
        variable_values={"orgId": "org-a"},
        context_value=_context("org-a"),
    )

    assert result.data is None
    assert result.errors is not None
    assert called is False


@pytest.mark.asyncio
async def test_acr_repository_scopes_rejects_blank_organization_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def _query_dicts(_client: object, _sql: str, _params: dict[str, object]):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _QUERY,
        variable_values={"orgId": ""},
        context_value=_context("org-a"),
    )

    assert result.data is None
    assert result.errors is not None
    assert called is False


@pytest.mark.asyncio
async def test_acr_repository_scopes_rejects_superuser_cross_organization_during_impersonation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dev_health_ops.api.services.auth import (
        _impersonation_ctx,
        set_impersonation_context,
    )

    async def _query_dicts(_client: object, _sql: str, _params: dict[str, object]):
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)
    token = set_impersonation_context(
        target_user_id="target-user",
        target_org_id="org-a",
        target_role="member",
        real_user_id="admin-user",
    )
    try:
        result = await schema.execute(
            _QUERY,
            variable_values={"orgId": "org-b"},
            context_value=_context("org-a", is_superuser=True),
        )
    finally:
        _impersonation_ctx.reset(token)

    assert result.data is None
    assert result.errors is not None


@pytest.mark.asyncio
async def test_acr_repository_scopes_rejects_demoted_superuser_stale_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def _query_dicts(_client: object, _sql: str, _params: dict[str, object]):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _QUERY,
        variable_values={"orgId": "org-b"},
        context_value=_context("org-a", is_superuser=True, is_superuser_verified=False),
    )

    assert result.data is None
    assert result.errors is not None
    assert called is False


@pytest.mark.asyncio
async def test_acr_repository_scopes_allows_superuser_cross_organization_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_org_ids: list[str] = []

    async def _query_dicts(_client: object, _sql: str, params: dict[str, object]):
        observed_org_ids.append(str(params["org_id"]))
        return [{"value": "other/repository", "count": 1}]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _QUERY,
        variable_values={"orgId": "org-b"},
        context_value=_context("org-a", is_superuser=True),
    )

    assert result.errors is None
    assert observed_org_ids == ["org-b"]
    assert result.data == {
        "catalog": {"values": [{"value": "other/repository", "count": 1}]}
    }


@pytest.mark.asyncio
async def test_acr_repository_scopes_rejects_catalog_exceeding_supported_scope_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _query_dicts(_client: object, _sql: str, params: dict[str, object]):
        assert params["limit"] == 101
        return [
            {"value": f"owner/repository-{index:03}", "count": 1}
            for index in range(130)
        ] + [
            {"value": " OWNER/REPOSITORY-000 ", "count": 1},
            {"value": "owner/trailing-", "count": 1},
            {"value": "group/subgroup/repository", "count": 1},
        ]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _QUERY,
        variable_values={"orgId": "org-a"},
        context_value=_context("org-a"),
    )

    assert result.data is None
    assert result.errors is not None
    assert (
        result.errors[0].message == "repository catalog exceeds supported scope limit"
    )


@pytest.mark.asyncio
async def test_acr_repository_scopes_keeps_query_wrapper_in_superuser_scope() -> None:
    from dev_health_ops.api.services.auth import _current_org_id, set_current_org_id

    class Sink:
        def __init__(self) -> None:
            self.params: list[dict[str, object]] = []

        def query_dicts(self, _sql: str, params: dict[str, object]):
            self.params.append(params)
            return [{"value": "other/repository", "count": 1}]

    sink = Sink()
    token = set_current_org_id("org-a")
    try:
        result = await schema.execute(
            _QUERY,
            variable_values={"orgId": "org-b"},
            context_value=_context("org-a", is_superuser=True, client=sink),
        )
    finally:
        _current_org_id.reset(token)

    assert result.errors is None
    assert sink.params == [{"limit": 101, "timeout": 30, "org_id": "org-b"}]


@pytest.mark.asyncio
async def test_acr_repository_scopes_returns_empty_catalog_for_effective_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _query_dicts(_client: object, _sql: str, params: dict[str, object]):
        assert params["org_id"] == "org-a"
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)

    result = await schema.execute(
        _QUERY,
        variable_values={"orgId": "org-a"},
        context_value=_context("org-a"),
    )

    assert result.errors is None
    assert result.data == {"catalog": {"values": []}}


@pytest.mark.asyncio
async def test_acr_repository_scopes_uses_persisted_catalog_for_empty_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _query_dicts(_client: object, sql: str, _params: dict[str, object]):
        assert "FROM repos FINAL" in sql
        return [{"value": "acme/repository", "count": 1}]

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)
    result = await schema.execute(
        """
        query ACRRepositoryScopes($orgId: String!) {
          catalog(orgId: $orgId, dimension: REPO, filters: {}) {
            values { value count }
          }
        }
        """,
        variable_values={"orgId": "org-a"},
        context_value=_context("org-a"),
    )

    assert result.errors is None
    assert result.data == {
        "catalog": {"values": [{"value": "acme/repository", "count": 1}]}
    }


@pytest.mark.asyncio
async def test_acr_repository_scopes_rejects_active_filters_instead_of_emptying_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def _query_dicts(_client: object, _sql: str, _params: dict[str, object]):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr("dev_health_ops.api.queries.client.query_dicts", _query_dicts)
    result = await schema.execute(
        """
        query ACRRepositoryScopes($orgId: String!) {
          catalog(
            orgId: $orgId,
            dimension: REPO,
            filters: {what: {repos: ["acme/repository"]}}
          ) {
            values { value count }
          }
        }
        """,
        variable_values={"orgId": "org-a"},
        context_value=_context("org-a"),
    )

    assert result.data is None
    assert result.errors is not None
    assert called is False
