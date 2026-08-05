"""RED-first regression for CHAOS-3411.

Production evidence: ``POST /api/v1/admin/orgs/{org_id}/members`` returned a
500 with asyncpg ``ForeignKeyViolationError`` (``memberships_user_id_fkey``)
for a user whose ``POST /api/v1/admin/users`` call returned ``ok=true`` with
that same id one HTTP call earlier.

Root cause: ``UserService.create`` (create_user's write) and
``MembershipService.add_member`` (add_member's write) only call
``session.flush()`` -- they never commit. Both endpoints then return a
response built from data that is *not yet durable*, trusting
``get_postgres_session()``'s post-response ``session.commit()`` (a
dependency-with-yield cleanup) to make it so eventually. But FastAPI's own
``request_response()`` wrapper (see ``fastapi/routing.py``:
``await response(scope, receive, send)`` runs *inside* the same
``AsyncExitStack`` that owns dependency cleanup, and strictly *before* that
stack unwinds) sends the HTTP response to the client before that deferred
commit executes. A second, genuinely independent request -- its own
session/connection, exactly what ``get_postgres_session_dep`` hands out per
request -- that lands before the first request's deferred commit lands
cannot see the row the first request just reported as created.

This test reproduces that gap deterministically, with no reliance on wall
clock timing: it drives the real ``create_user`` and ``add_member`` router
functions directly, each given its own independent session against a live
Postgres schema (mirroring what two sequential HTTP requests actually get),
and never commits on the test's behalf. If the endpoint itself does not
guarantee durability before returning, the second session's FK-constrained
insert fails exactly like the production traceback -- proving the gap
without needing to race an actual clock.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dev_health_ops.api.admin.routers.orgs import add_member
from dev_health_ops.api.admin.routers.users import create_user
from dev_health_ops.api.admin.schemas_flat import MembershipCreate, UserCreate
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.users import MembershipService
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

_POSTGRES_URI_ENV = "DEV_HEALTH_POSTGRES_TEST_URI"


def _require_postgres_test_uri() -> None:
    """Same contract as ``tests/test_0066_celery_river_cutover_postgres.py``'s
    ``_require_postgres_test_uri``: skip locally when the URI isn't
    configured, but hard-``pytest.fail`` under CI. A plain ``skipif`` would
    let this module go quietly uncollected-in-substance on every PR (CHAOS-
    3411 Codex round 1: the PR-gated unit step never sets this URI, so the
    module always took the skip branch there and a stripped commit would
    still pass the required gate). Missing coverage under CI must be a loud
    failure, not a silent pass.
    """
    if os.getenv(_POSTGRES_URI_ENV):
        return
    if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
        pytest.fail(f"{_POSTGRES_URI_ENV} must be configured for CHAOS-3411 tests")
    pytest.skip(f"requires {_POSTGRES_URI_ENV}")


@pytest.fixture(autouse=True, scope="module")
def require_postgres_test_uri() -> None:
    _require_postgres_test_uri()


_SUPERUSER = AuthenticatedUser(
    user_id="00000000-0000-0000-0000-000000000099",
    email="super-admin@example.com",
    org_id="",
    role="owner",
    is_superuser=True,
)


@pytest_asyncio.fixture
async def org_schema() -> AsyncIterator[
    tuple[async_sessionmaker[AsyncSession], uuid.UUID]
]:
    """A live, isolated Postgres schema with the real users/orgs/memberships
    tables (FK constraints included) and one seeded Organization -- same
    per-test-schema pattern as ``tests/api/dev/test_persistence_postgres.py``.
    """
    configured_url = make_url(os.environ[_POSTGRES_URI_ENV])
    if configured_url.get_backend_name() != "postgresql":
        pytest.fail(f"{_POSTGRES_URI_ENV} must use PostgreSQL")
    async_url = configured_url.set(drivername="postgresql+asyncpg")
    schema = f"chaos_3411_{uuid.uuid4().hex}"
    admin_engine = create_async_engine(async_url)
    engine: AsyncEngine | None = None
    schema_created = False
    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            schema_created = True
        engine = create_async_engine(
            async_url,
            connect_args={"server_settings": {"search_path": schema}},
        )
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=tables_of(Organization, User, Membership),
                )
            )
        maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        org_id = uuid.uuid4()
        async with maker() as seed_session:
            seed_session.add(
                Organization(
                    id=org_id,
                    slug=f"chaos-3411-{org_id.hex}",
                    name="CHAOS-3411 org",
                )
            )
            await seed_session.commit()
        yield maker, org_id
    finally:
        if engine is not None:
            await engine.dispose()
        if schema_created:
            async with admin_engine.begin() as connection:
                await connection.execute(
                    text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                )
        await admin_engine.dispose()


async def _seed_committed_user(
    maker: async_sessionmaker[AsyncSession], *, email: str
) -> str:
    """Insert and COMMIT a user directly (bypassing create_user's own
    endpoint code entirely), so tests that aren't exercising create_user's
    commit don't depend on it -- keeps each test's mutation-kill isolated to
    the one endpoint it's actually about.
    """
    user_id = uuid.uuid4()
    async with maker() as session:
        session.add(User(id=user_id, email=email, is_active=True))
        await session.commit()
    return str(user_id)


@pytest.mark.asyncio
async def test_add_member_sees_a_user_created_one_http_call_earlier(
    org_schema: tuple[async_sessionmaker[AsyncSession], uuid.UUID],
) -> None:
    """Two separate sessions, driving the real endpoints directly -- exactly
    what two sequential HTTP requests get from ``get_postgres_session_dep``.
    ``add_member`` must be able to see the user ``create_user`` just
    reported as created, with no explicit commit from the test itself, AND
    the membership it reports creating must itself be durable -- not just
    flushed into a session this test happens to still be holding open.
    """
    maker, org_id = org_schema

    async with maker() as create_user_session:
        user_response = await create_user(
            payload=UserCreate(email="chaos-3411@example.com", password="hunter22"),
            session=create_user_session,
        )
    # `async with` has now exited: create_user_session is closed. Nothing in
    # this test committed it -- exactly like a router-level bug where the
    # endpoint returns its response without durably persisting first.

    async with maker() as add_member_session:
        membership_response = await add_member(
            org_id=str(org_id),
            payload=MembershipCreate(user_id=user_response.id),
            session=add_member_session,
            current_user=_SUPERUSER,
        )
    # Same discipline for add_member_session: no commit from this test.

    assert membership_response.user_id == user_response.id
    assert membership_response.org_id == str(org_id)

    # Durability double-check, from a brand-new fourth session: the user AND
    # the membership must both actually be on disk, not just visible within
    # a session that happens to still hold the write. Checking only the user
    # here would pass even if add_member's own commit (orgs.py) were deleted
    # -- add_member_session.close() rolls back its flush-only insert, but
    # nothing upstream of this assertion would notice, since the *response*
    # object was already built from the pre-rollback, in-memory ORM row.
    async with maker() as verify_session:
        seen_user = await verify_session.get(User, uuid.UUID(user_response.id))
        assert seen_user is not None, (
            "user_response reported ok, but the user row was never durably "
            "committed -- CHAOS-3411 (create_user returns before commit)"
        )
        seen_membership = await MembershipService(verify_session).get_membership(
            str(org_id), user_response.id
        )
        assert seen_membership is not None, (
            "add_member reported ok, but the membership row was never "
            "durably committed -- CHAOS-3411 (add_member returns before commit)"
        )
        assert str(seen_membership.user_id) == user_response.id


@pytest.mark.asyncio
async def test_create_organization_with_owner_is_durable_before_returning(
    org_schema: tuple[async_sessionmaker[AsyncSession], uuid.UUID],
) -> None:
    """Sibling of the confirmed bug: ``create_organization`` has the exact
    same create-and-flush-only shape as ``add_member``, and when given an
    ``owner_user_id`` it also inserts the owner membership in the same
    session (OrganizationService.create -> MembershipService.add_member).
    A subsequent, separate-session request must see both the org AND the
    owner membership row it just reported creating.
    """
    from dev_health_ops.api.admin.routers.orgs import create_organization
    from dev_health_ops.api.admin.schemas_flat import OrganizationCreate

    maker, _existing_org_id = org_schema
    owner_user_id = await _seed_committed_user(
        maker, email="chaos-3411-owner@example.com"
    )

    async with maker() as create_org_session:
        org_response = await create_organization(
            payload=OrganizationCreate(
                name="CHAOS-3411 sibling org", owner_user_id=owner_user_id
            ),
            session=create_org_session,
            current_user=_SUPERUSER,
        )
    # No commit from this test -- same discipline as the add_member test.

    async with maker() as verify_session:
        seen_org = await verify_session.get(Organization, uuid.UUID(org_response.id))
        assert seen_org is not None, (
            "create_organization reported ok, but the org row was never "
            "durably committed -- same emit-before-commit class as CHAOS-3411"
        )
        seen_owner_membership = await MembershipService(verify_session).get_membership(
            org_response.id, owner_user_id
        )
        assert seen_owner_membership is not None, (
            "create_organization reported ok with an owner_user_id, but the "
            "owner membership row was never durably committed"
        )
        assert str(seen_owner_membership.role) == "owner"
