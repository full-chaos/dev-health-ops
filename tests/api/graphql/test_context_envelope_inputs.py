"""get_context wiring for the two effective-principal-envelope inputs
(CHAOS-4697 prerequisite: tier, licensed_features).

Nothing consumes GraphQLContext.tier/licensed_features on a live request yet
-- the dispatcher that would is CHAOS-4697 itself, deliberately a separate
lane. These tests prove the wiring works end-to-end at the real seam
(get_context, with a real async Postgres-shaped session) rather than only at
the underlying service functions: an authenticated request for an org with
license/feature rows ends up with both fields populated on the context, an
unauthenticated/org-less request leaves both None (never a fabricated
tier/[] guess), and rebind_org_id (the superuser cross-org path) clears both
rather than leaking a stale org's values into a new tenant scope.
"""

from __future__ import annotations

import types
import uuid
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dev_health_ops.api.graphql import app as gql_app
from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.services.auth import _current_org_id, _impersonation_ctx
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization
from tests._helpers import tables_of

_TABLES = tables_of(Organization, OrgLicense, FeatureFlag, OrgFeatureOverride)


class _FakeRequest:
    def __init__(
        self, headers: dict[str, str], query_params: dict[str, str] | None = None
    ) -> None:
        self.headers = headers
        self.query_params = query_params or {}


async def _seed_org(
    maker: async_sessionmaker[AsyncSession],
    org_id: uuid.UUID,
    *,
    tier: str = "enterprise",
    feature_key: str | None = "ai_review",
) -> None:
    async with maker() as session:
        session.add(
            Organization(id=org_id, slug=f"org-{org_id.hex[:8]}", name="T", tier=tier)
        )
        if feature_key:
            session.add(
                FeatureFlag(
                    key=feature_key,
                    name=feature_key,
                    category="analytics",
                    min_tier="community",
                )
            )
        await session.commit()


def _stub_auth(
    monkeypatch: pytest.MonkeyPatch,
    maker: async_sessionmaker[AsyncSession],
    *,
    org_id: str,
) -> None:
    fake_user = types.SimpleNamespace(
        user_id="user-1",
        email="dev@example.com",
        org_id=org_id,
        role="admin",
        is_superuser=False,
    )

    async def _authenticate_access_token(
        _token: str, _db: object
    ) -> types.SimpleNamespace:
        return fake_user

    fake_service = types.SimpleNamespace(
        authenticate_access_token=_authenticate_access_token
    )
    monkeypatch.setattr(
        "dev_health_ops.api.services.auth.get_auth_service", lambda: fake_service
    )

    async def _no_client(_dsn: str) -> None:
        return None

    monkeypatch.setattr(
        "dev_health_ops.api.queries.client.get_global_client", _no_client
    )
    monkeypatch.setenv("GRAPHQL_AUTH_REQUIRED", "true")

    @asynccontextmanager
    async def _session():
        async with maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    monkeypatch.setattr("dev_health_ops.db.get_postgres_session", _session)


@pytest_asyncio.fixture
async def db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ctx.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_get_context_populates_tier_and_licensed_features(
    db, monkeypatch: pytest.MonkeyPatch
) -> None:
    org_id = uuid.uuid4()
    await _seed_org(db, org_id, tier="enterprise", feature_key="ai_review")
    _stub_auth(monkeypatch, db, org_id=str(org_id))

    request = _FakeRequest(headers={"Authorization": "Bearer faketoken"})
    ctx = await gql_app.get_context(request)  # type: ignore[arg-type]

    assert ctx.tier is LicenseTier.ENTERPRISE
    assert ctx.licensed_features == ["ai_review"]


@pytest.mark.asyncio
async def test_get_context_leaves_envelope_inputs_none_when_unauthenticated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Unauthenticated, but org_id still present via query params (e.g. the
    # GraphiQL playground) -- proves the gate is "no authenticated user",
    # not "no org_id", since the latter alone wouldn't reach the envelope
    # inputs block at all.
    monkeypatch.setenv("GRAPHQL_AUTH_REQUIRED", "false")
    _impersonation_ctx.set(None)
    _current_org_id.set(None)
    request = _FakeRequest(headers={}, query_params={"org_id": str(uuid.uuid4())})

    ctx = await gql_app.get_context(request)  # type: ignore[arg-type]

    assert ctx.user is None
    assert ctx.tier is None
    assert ctx.licensed_features is None


def test_rebind_org_id_clears_tier_and_licensed_features() -> None:
    """The superuser cross-org path must never leak a stale org's
    tier/licensed_features into the rebound tenant scope."""
    ctx = GraphQLContext(
        org_id="org-a",
        db_url="clickhouse://localhost/test",
        tier=LicenseTier.ENTERPRISE,
        licensed_features=["ai_review"],
    )

    ctx.rebind_org_id("org-b")

    assert ctx.tier is None
    assert ctx.licensed_features is None
