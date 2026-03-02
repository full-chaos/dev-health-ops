from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.ip_allowlist import OrgIPAllowlist
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.users import Membership, Organization, User

auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
admin_router_module = importlib.import_module("dev_health_ops.api.admin.router")

_TABLES = [
    User.__table__,
    Organization.__table__,
    Membership.__table__,
    OrgIPAllowlist.__table__,
    OrgLicense.__table__,
]


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "ip-allowlist.db"
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
    user_id = uuid.uuid4()

    # tier="enterprise" enables the ip_allowlist feature via _check_org_feature_async
    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="enterprise")
    user = User(id=user_id, email="admin@example.com", is_active=True)

    async with session_maker() as session:
        session.add_all([org, user])
        session.add(Membership(org_id=org_id, user_id=user_id, role="owner"))
        await session.commit()

    return {
        "org_id": str(org_id),
        "user_id": str(user_id),
        "user_email": "admin@example.com",
    }


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    current_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email=seeded_state["user_email"],
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = (
        lambda: current_user
    )
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_ip_allowlist_empty(client, seeded_state):
    resp = await client.get(
        f"/api/v1/admin/ip-allowlist",
        headers={"X-Org-Id": seeded_state["org_id"]},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["items"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_create_ip_allowlist_entry_single_ip(client, seeded_state):
    resp = await client.post(
        "/api/v1/admin/ip-allowlist",
        json={"ip_range": "192.168.1.1", "description": "Office IP"},
        headers={"X-User-Id": seeded_state["user_id"]},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["ip_range"] == "192.168.1.1"
    assert data["description"] == "Office IP"
    assert data["is_active"] is True
    assert data["org_id"] == seeded_state["org_id"]


@pytest.mark.asyncio
async def test_create_ip_allowlist_entry_cidr_range(client, seeded_state):
    resp = await client.post(
        "/api/v1/admin/ip-allowlist",
        json={"ip_range": "10.0.0.0/24", "description": "VPN range"},
        headers={"X-User-Id": seeded_state["user_id"]},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["ip_range"] == "10.0.0.0/24"
    assert data["description"] == "VPN range"


@pytest.mark.asyncio
async def test_create_ip_allowlist_entry_invalid_ip_returns_422(client, seeded_state):
    resp = await client.post(
        "/api/v1/admin/ip-allowlist",
        json={"ip_range": "not-an-ip", "description": "Bad entry"},
        headers={"X-User-Id": seeded_state["user_id"]},
    )
    # Invalid IP: service raises ValueError → router catches → 400
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_ip_allowlist_entry_by_id(client, seeded_state):
    # Create an entry first
    create_resp = await client.post(
        "/api/v1/admin/ip-allowlist",
        json={"ip_range": "172.16.0.1", "description": "Test get"},
        headers={"X-User-Id": seeded_state["user_id"]},
    )
    assert create_resp.status_code == 201
    entry_id = create_resp.json()["id"]

    # Now fetch it
    get_resp = await client.get(f"/api/v1/admin/ip-allowlist/{entry_id}")
    assert get_resp.status_code == 200
    data = get_resp.json()
    assert data["id"] == entry_id
    assert data["ip_range"] == "172.16.0.1"


@pytest.mark.asyncio
async def test_update_ip_allowlist_entry(client, seeded_state):
    # Create
    create_resp = await client.post(
        "/api/v1/admin/ip-allowlist",
        json={"ip_range": "10.1.1.1", "description": "Original"},
        headers={"X-User-Id": seeded_state["user_id"]},
    )
    assert create_resp.status_code == 201
    entry_id = create_resp.json()["id"]

    # Update description and deactivate
    patch_resp = await client.patch(
        f"/api/v1/admin/ip-allowlist/{entry_id}",
        json={"description": "Updated", "is_active": False},
    )
    assert patch_resp.status_code == 200
    data = patch_resp.json()
    assert data["description"] == "Updated"
    assert data["is_active"] is False


@pytest.mark.asyncio
async def test_delete_ip_allowlist_entry(client, seeded_state):
    # Create
    create_resp = await client.post(
        "/api/v1/admin/ip-allowlist",
        json={"ip_range": "10.2.2.2"},
        headers={"X-User-Id": seeded_state["user_id"]},
    )
    assert create_resp.status_code == 201
    entry_id = create_resp.json()["id"]

    # Delete
    del_resp = await client.delete(f"/api/v1/admin/ip-allowlist/{entry_id}")
    assert del_resp.status_code == 200
    assert del_resp.json()["deleted"] is True

    # Verify gone
    get_resp = await client.get(f"/api/v1/admin/ip-allowlist/{entry_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_check_ip_allowed(client, seeded_state):
    # No entries → all IPs allowed (empty allowlist = open)
    resp = await client.post(
        "/api/v1/admin/ip-allowlist/check",
        json={"ip_address": "1.2.3.4"},
    )
    assert resp.status_code == 200
    assert resp.json()["allowed"] is True

    # Add an entry for 192.168.0.0/24
    await client.post(
        "/api/v1/admin/ip-allowlist",
        json={"ip_range": "192.168.0.0/24"},
        headers={"X-User-Id": seeded_state["user_id"]},
    )

    # IP in range → allowed
    in_range = await client.post(
        "/api/v1/admin/ip-allowlist/check",
        json={"ip_address": "192.168.0.50"},
    )
    assert in_range.status_code == 200
    assert in_range.json()["allowed"] is True

    # IP outside range → blocked
    out_of_range = await client.post(
        "/api/v1/admin/ip-allowlist/check",
        json={"ip_address": "10.0.0.1"},
    )
    assert out_of_range.status_code == 200
    assert out_of_range.json()["allowed"] is False
