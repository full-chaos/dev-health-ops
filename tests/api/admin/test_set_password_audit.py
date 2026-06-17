"""CHAOS-2498: admin set_password must emit a PASSWORD_CHANGED audit row.

The emit lives in admin/routers/users.py (set_user_password). This test mirrors
tests/api/auth/test_password_reset.py::test_reset_password_valid_token_emits_audit_log
and asserts the audit row actually lands in the DB on the success path.
"""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.middleware.rate_limit import limiter as rate_limiter
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.audit import AuditAction, AuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
admin_router_module = importlib.import_module("dev_health_ops.api.admin")

_TABLES = tables_of(User, Organization, Membership, AuditLog, RefreshToken)

# Pre-computed bcrypt hash (cost=4) of ADMIN_PASSWORD to avoid hashing at import.
# This is a test fixture hash, not a real credential.
ADMIN_PASSWORD = "OldPassword@123"
# nosemgrep: generic.secrets.security.detected-bcrypt-hash.detected-bcrypt-hash
ADMIN_PASSWORD_HASH = "$2b$04$tgxalfE5Q58OGJE/0M0piOakqY90AzLsIFaz178yu6eMEkjMuYeJe"
# gitleaks:allow — test fixture password, not a credential. The literal trips
# gitleaks' generic-api-key entropy heuristic (16 alphanumerics, no symbols).
NEW_PASSWORD = "BrandNewPass1234"  # gitleaks:allow (>=12 chars, letter+digit)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "set-password-audit.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded_state(session_maker):
    org_id = uuid.uuid4()
    admin_id = uuid.uuid4()
    target_id = uuid.uuid4()

    org = Organization(id=org_id, slug="test-org", name="Test Org")
    admin = User(
        id=admin_id,
        email="admin@example.com",
        password_hash=ADMIN_PASSWORD_HASH,
        is_active=True,
    )
    target = User(id=target_id, email="target@example.com", is_active=True)

    async with session_maker() as session:
        session.add_all([org, admin, target])
        session.add(Membership(org_id=org_id, user_id=admin_id, role="owner"))
        session.add(Membership(org_id=org_id, user_id=target_id, role="member"))
        await session.commit()

    return {
        "org_id": str(org_id),
        "admin_id": str(admin_id),
        "target_id": str(target_id),
        "admin_email": "admin@example.com",
    }


@pytest_asyncio.fixture
async def client(monkeypatch: pytest.MonkeyPatch, session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    current_user = AuthenticatedUser(
        user_id=seeded_state["admin_id"],
        email=seeded_state["admin_email"],
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    monkeypatch.setattr(rate_limiter, "enabled", False)
    app.dependency_overrides[auth_router_module.get_current_user] = lambda: current_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_set_password_emits_audit_log(client, session_maker, seeded_state):
    resp = await client.post(
        f"/api/v1/admin/users/{seeded_state['target_id']}/password",
        json={"admin_password": ADMIN_PASSWORD, "password": NEW_PASSWORD},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["success"] is True

    async with session_maker() as session:
        result = await session.execute(
            select(AuditLog).where(AuditLog.resource_id == seeded_state["target_id"])
        )
        audit_log = result.scalar_one()

    assert audit_log.action == AuditAction.PASSWORD_CHANGED.value
    assert audit_log.org_id == uuid.UUID(seeded_state["org_id"])
    assert audit_log.user_id == uuid.UUID(seeded_state["admin_id"])
    assert audit_log.status == "success"


@pytest.mark.asyncio
async def test_set_password_wrong_admin_password_emits_no_audit(
    client, session_maker, seeded_state
):
    """A failed admin-password check must not write a PASSWORD_CHANGED row."""
    resp = await client.post(
        f"/api/v1/admin/users/{seeded_state['target_id']}/password",
        json={"admin_password": "WrongAdminPass99", "password": NEW_PASSWORD},
    )
    assert resp.status_code == 403

    async with session_maker() as session:
        result = await session.execute(select(AuditLog))
        rows = result.scalars().all()
    assert rows == []
