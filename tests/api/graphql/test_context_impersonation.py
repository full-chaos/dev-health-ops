"""Regression tests for GraphQL get_context org resolution under impersonation.

CHAOS-2303: the analytics web UI fetches via GraphQL. During impersonation the
admin JWT is never re-issued, so `user.org_id` stays the admin's org. get_context
must prefer the active impersonation target org so that context scoping, loader
cache keys, and SQL params are all consistently scoped to the impersonated org
(otherwise query_dicts overrides only the raw SQL params, poisoning loader caches
keyed by the admin org).

These tests exercise get_context directly with a fake request and a stubbed auth
service, so no DB or real JWT is required.
"""

from __future__ import annotations

import types
from contextlib import asynccontextmanager

import pytest

from dev_health_ops.api.graphql import app as gql_app
from dev_health_ops.api.services.auth import (
    _impersonation_ctx,
    set_impersonation_context,
)


class _FakeRequest:
    """Minimal duck-typed stand-in for starlette Request used by get_context."""

    def __init__(
        self, headers: dict[str, str], query_params: dict[str, str] | None = None
    ) -> None:
        self.headers = headers
        self.query_params = query_params or {}


def _stub_admin_auth(monkeypatch: pytest.MonkeyPatch, *, admin_org: str) -> None:
    """Make get_auth_service() return a superuser admin whose JWT org = admin_org."""
    fake_admin = types.SimpleNamespace(
        user_id="admin-1",
        email="admin@example.com",
        org_id=admin_org,
        role="owner",
        is_superuser=True,
    )

    async def _authenticate_access_token(
        _token: str, _db: object
    ) -> types.SimpleNamespace:
        return fake_admin

    fake_service = types.SimpleNamespace(
        authenticate_access_token=_authenticate_access_token
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.auth.get_auth_service",
        lambda: fake_service,
    )

    async def _no_client(_dsn: str) -> None:
        # Skip ClickHouse client creation; get_context tolerates client=None.
        return None

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.get_global_client",
        _no_client,
    )
    monkeypatch.setenv("GRAPHQL_AUTH_REQUIRED", "true")

    @asynccontextmanager
    async def _fake_session():
        yield object()

    monkeypatch.setattr("dev_health_ops.db.get_postgres_session", _fake_session)


@pytest.mark.asyncio
async def test_get_context_prefers_impersonation_target_org(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An active impersonation session overrides the admin JWT org."""
    admin_org = "org-admin-gql"
    target_org = "org-target-gql"
    _stub_admin_auth(monkeypatch, admin_org=admin_org)

    request = _FakeRequest(headers={"Authorization": "Bearer faketoken"})

    token = set_impersonation_context(
        target_user_id="target-user",
        target_org_id=target_org,
        target_role="member",
        real_user_id="admin-1",
    )
    try:
        ctx = await gql_app.get_context(request)  # type: ignore[arg-type]
    finally:
        _impersonation_ctx.reset(token)

    assert ctx.org_id == target_org, (
        f"GraphQL context must scope to the impersonated org; "
        f"expected {target_org!r}, got {ctx.org_id!r}"
    )


@pytest.mark.asyncio
async def test_get_context_uses_jwt_org_without_impersonation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without impersonation, get_context still uses the admin JWT org."""
    admin_org = "org-admin-gql-2"
    _stub_admin_auth(monkeypatch, admin_org=admin_org)

    # Ensure no impersonation context leaks in from another test.
    _impersonation_ctx.set(None)

    request = _FakeRequest(headers={"Authorization": "Bearer faketoken"})
    ctx = await gql_app.get_context(request)  # type: ignore[arg-type]

    assert ctx.org_id == admin_org


def _superuser_graphql_context(org_id: str = "org-x"):
    """Build a minimal GraphQLContext whose user is a real superuser."""
    from dev_health_ops.api.graphql.context import GraphQLContext
    from dev_health_ops.api.services.auth import AuthenticatedUser

    user = AuthenticatedUser(
        user_id="admin-1",
        email="admin@example.com",
        org_id=org_id,
        role="owner",
        is_superuser=True,
    )
    return GraphQLContext(org_id=org_id, db_url="", user=user)


def test_require_platform_admin_denied_during_impersonation() -> None:
    """A superuser impersonating a non-platform user cannot reach platform-admin
    GraphQL operations -- the impersonated identity is not a platform admin."""
    from dev_health_ops.api.graphql.authz import require_platform_admin
    from dev_health_ops.api.graphql.errors import AuthorizationError

    ctx = _superuser_graphql_context()
    token = set_impersonation_context(
        target_user_id="target-user",
        target_org_id="org-target",
        target_role="viewer",
        real_user_id="admin-1",
    )
    try:
        with pytest.raises(AuthorizationError):
            require_platform_admin(ctx)
    finally:
        _impersonation_ctx.reset(token)


def test_require_platform_admin_allowed_for_superuser_without_impersonation() -> None:
    """Without an active impersonation session a real superuser still passes."""
    from dev_health_ops.api.graphql.authz import require_platform_admin

    _impersonation_ctx.set(None)
    ctx = _superuser_graphql_context()
    # Must not raise.
    require_platform_admin(ctx)
