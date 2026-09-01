"""Full-stack CHAOS-4722 regression tests: /api/v1/telemetry/* org identity.

RED-FIRST evidence (pasted in the PR body): run unmodified against
origin/main @ 512c4e77b (the parent commit, pre-fix) every "anonymous
rejected" test below FAILS -- ``get_org_id`` returned the raw ``X-Org-Id``
header verbatim with no authentication dependency at all, so the request
went through as if it were an authorized member (200/204, not 401), and an
anonymous ``/opt-in`` call actually flipped the target org's state. They
pass against the fix in this PR.

These hit the REAL app (``dev_health_ops.api.main.app``) through its real
ASGI stack -- no mock of ``get_org_id``, ``require_platform_role``,
``user_is_member_of_org``, or any other authorization decision. The
membership-boundary tests give only the caller's IDENTITY via FastAPI's
standard dependency-override mechanism (``get_current_user``), exactly like
every other authenticated-route test in this suite (e.g.
``tests/api/test_work_unit_explain_hardening.py``) -- the org-membership
check itself runs for real, against a real (sqlite-backed) session seeded
with real ``User``/``Organization``/``Membership`` rows, through the actual
``user_is_member_of_org`` query in ``api/middleware/__init__.py``.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, patch

os.environ.setdefault("CLICKHOUSE_URI", "clickhouse://localhost:8123/default")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-for-telemetry-auth")
os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dev_health_ops.api.auth.router import get_current_user  # noqa: E402
from dev_health_ops.api.main import app  # noqa: E402
from dev_health_ops.api.services.auth import AuthenticatedUser  # noqa: E402
from dev_health_ops.models.audit import AuditLog  # noqa: E402
from dev_health_ops.models.git import Base  # noqa: E402
from dev_health_ops.models.settings import Setting  # noqa: E402
from dev_health_ops.models.users import Membership, Organization, User  # noqa: E402
from tests._helpers import tables_of  # noqa: E402

STATUS = "/api/v1/telemetry/status"
OPT_IN = "/api/v1/telemetry/opt-in"
OPT_OUT = "/api/v1/telemetry/opt-out"
REPORT = "/api/v1/telemetry/report"

ARBITRARY_ORG = "00000000-0000-0000-0000-000000000000"

_TABLES = tables_of(User, Organization, Membership, Setting, AuditLog)


# ---------------------------------------------------------------------------
# Part 1 -- anonymous rejection. Zero mocking: real app, no Authorization
# header, no DB needed (get_current_user 401s before any session opens --
# the CHAOS-4722 fix also reorders each route's dependencies so auth
# resolves before the DB-session dependency is even entered).
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_status_anonymous_rejected(client):
    resp = await client.get(STATUS, headers={"X-Org-Id": ARBITRARY_ORG})
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_opt_in_anonymous_rejected(client):
    resp = await client.post(OPT_IN, headers={"X-Org-Id": ARBITRARY_ORG})
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_opt_out_anonymous_rejected(client):
    resp = await client.post(OPT_OUT, headers={"X-Org-Id": ARBITRARY_ORG})
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_report_anonymous_rejected(client):
    resp = await client.post(REPORT, headers={"X-Org-Id": ARBITRARY_ORG})
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_status_anonymous_rejected_even_without_header(client):
    """No X-Org-Id at all must still 401 -- never fall through to a default org."""
    resp = await client.get(STATUS)
    assert resp.status_code == 401, resp.text


# ---------------------------------------------------------------------------
# Part 2 -- authenticated membership boundary (real sqlite-backed session,
# real Membership rows, real user_is_member_of_org query).
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db(tmp_path):
    db_path = tmp_path / "telemetry-auth.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: Base.metadata.create_all(c, tables=_TABLES))
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    @asynccontextmanager
    async def _fake_get_postgres_session() -> AsyncIterator[AsyncSession]:
        async with maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    with (
        patch("dev_health_ops.db.get_postgres_session", _fake_get_postgres_session),
        patch(
            "dev_health_ops.api.dependencies.get_postgres_session",
            _fake_get_postgres_session,
        ),
    ):
        yield maker

    await engine.dispose()


async def _seed_user_and_orgs(maker, *, is_superuser: bool = False):
    """Seed one user, home org (a real Membership), and a foreign org."""
    user_id = uuid.uuid4()
    home_org_id = uuid.uuid4()
    foreign_org_id = uuid.uuid4()
    async with maker() as session:
        session.add_all(
            [
                Organization(id=home_org_id, slug=f"home-{home_org_id}", name="Home"),
                Organization(
                    id=foreign_org_id,
                    slug=f"foreign-{foreign_org_id}",
                    name="Foreign",
                ),
                User(
                    id=user_id,
                    email=f"{user_id}@example.com",
                    is_superuser=is_superuser,
                ),
            ]
        )
        await session.flush()
        session.add(Membership(user_id=user_id, org_id=home_org_id, role="member"))
        await session.commit()
    return user_id, home_org_id, foreign_org_id


def _fake_user(
    user_id: uuid.UUID, org_id: uuid.UUID, *, is_superuser: bool = False
) -> AuthenticatedUser:
    return AuthenticatedUser(
        user_id=str(user_id),
        email="u@example.com",
        org_id=str(org_id),
        role="owner" if is_superuser else "member",
        is_superuser=is_superuser,
    )


@pytest.mark.asyncio
async def test_status_own_org_allowed(db, client):
    user_id, home_org_id, _ = await _seed_user_and_orgs(db)
    app.dependency_overrides[get_current_user] = lambda: _fake_user(
        user_id, home_org_id
    )
    try:
        resp = await client.get(STATUS, headers={"X-Org-Id": str(home_org_id)})
    finally:
        app.dependency_overrides.pop(get_current_user, None)
    assert resp.status_code == 200, resp.text
    assert resp.json()["opted_in"] is False


@pytest.mark.asyncio
async def test_status_foreign_org_rejected(db, client):
    """CHAOS-4722 core acceptance criterion: an authenticated non-member
    supplying another org's id gets 403, not the foreign org's data."""
    user_id, _home_org_id, foreign_org_id = await _seed_user_and_orgs(db)
    app.dependency_overrides[get_current_user] = lambda: _fake_user(
        user_id, uuid.uuid4()
    )
    try:
        resp = await client.get(STATUS, headers={"X-Org-Id": str(foreign_org_id)})
    finally:
        app.dependency_overrides.pop(get_current_user, None)
    assert resp.status_code == 403, resp.text


@pytest.mark.asyncio
async def test_opt_in_foreign_org_rejected_and_state_untouched(db, client):
    user_id, _home_org_id, foreign_org_id = await _seed_user_and_orgs(db)
    app.dependency_overrides[get_current_user] = lambda: _fake_user(
        user_id, uuid.uuid4()
    )
    try:
        resp = await client.post(OPT_IN, headers={"X-Org-Id": str(foreign_org_id)})
    finally:
        app.dependency_overrides.pop(get_current_user, None)
    assert resp.status_code == 403, resp.text

    async with db() as session:
        result = await session.execute(
            select(Setting).where(Setting.org_id == str(foreign_org_id))
        )
        assert result.scalar_one_or_none() is None, (
            "a rejected request must never mutate the foreign org's telemetry state"
        )


@pytest.mark.asyncio
async def test_opt_in_then_report_self_escalation_still_blocked(db, client):
    """The pre-fix escalation chain: opt your OWN org in, then call /report
    to try to read instance-wide stats. Even a legitimate member of their
    own org must still 403 on /report without a platform role (G-32) -- the
    precondition for the disclosure must not be settable by the seeker."""
    user_id, home_org_id, _foreign_org_id = await _seed_user_and_orgs(db)
    app.dependency_overrides[get_current_user] = lambda: _fake_user(
        user_id, home_org_id
    )
    try:
        opt_in_resp = await client.post(OPT_IN, headers={"X-Org-Id": str(home_org_id)})
        assert opt_in_resp.status_code == 200, opt_in_resp.text
        report_resp = await client.post(REPORT, headers={"X-Org-Id": str(home_org_id)})
    finally:
        app.dependency_overrides.pop(get_current_user, None)
    assert report_resp.status_code == 403, report_resp.text


@pytest.mark.asyncio
async def test_report_platform_role_allowed(db, client, monkeypatch):
    """A superuser passes the CHAOS-4722 identity/authorization gate.

    ``collect_usage_stats`` is mocked here -- it is unrelated business logic,
    not part of the org-identity/platform-role check this ticket is about.
    """
    monkeypatch.delenv("TELEMETRY_ENDPOINT", raising=False)
    user_id, home_org_id, _ = await _seed_user_and_orgs(db, is_superuser=True)
    app.dependency_overrides[get_current_user] = lambda: _fake_user(
        user_id, home_org_id, is_superuser=True
    )
    try:
        opt_in_resp = await client.post(OPT_IN, headers={"X-Org-Id": str(home_org_id)})
        assert opt_in_resp.status_code == 200, opt_in_resp.text

        fake_stats = {
            "total_organizations": 1,
            "active_organizations": 1,
            "total_users": 1,
            "active_users": 1,
            "total_repos": 0,
            "total_sync_configs": 0,
            "active_syncs_24h": 0,
            "tier_distribution": {},
            "feature_usage": {},
        }
        with patch(
            "dev_health_ops.api.services.telemetry.TelemetryService.collect_usage_stats",
            AsyncMock(return_value=fake_stats),
        ):
            resp = await client.post(REPORT, headers={"X-Org-Id": str(home_org_id)})
    finally:
        app.dependency_overrides.pop(get_current_user, None)
    assert resp.status_code == 200, resp.text
    assert resp.json()["total_organizations"] == 1
