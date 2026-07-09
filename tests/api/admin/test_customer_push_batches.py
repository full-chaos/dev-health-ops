"""Tests for the CHAOS-2694 admin-plane batch/schema read proxies under
``/api/v1/admin/customer-push/*``.

Follows tests/api/admin/test_customer_push.py's direct-app fixture style.
Seeds ``external_ingest_batches``/``rejections`` via status.py directly (not
via HTTP -- the POST accept path is out of scope for this issue) alongside an
``IngestSource`` row for the source-scoped list endpoint.
"""

from __future__ import annotations

import importlib
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest import status as status_mod
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.external_ingest import (
    ExternalIngestBatch,
    ExternalIngestRejection,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.ingest_auth import IngestSource
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    IngestSource,
    FeatureFlag,
    OrgFeatureOverride,
    OrgLicense,
    ExternalIngestBatch,
    ExternalIngestRejection,
)

_CUSTOMER_PUSH_FEATURE = "customer_push_ingest"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "customer_push_batches.db"
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
    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="team")
    user = User(id=user_id, email="admin@example.com", is_active=True)
    feature = FeatureFlag(
        key=_CUSTOMER_PUSH_FEATURE,
        name="Customer Push Ingest",
        category="integrations",
        min_tier="team",
    )
    source = IngestSource(
        id=uuid.uuid4(),
        org_id=str(org_id),
        system="github",
        instance="acme/api",
        mode="customer_push",
        enabled=True,
    )

    async with session_maker() as session:
        session.add_all([org, user, feature, source])
        await session.commit()

    return {"org_id": str(org_id), "user_id": str(user_id), "source_id": str(source.id)}


@pytest_asyncio.fixture
async def client(session_maker, seeded_state):
    app = FastAPI()
    app.include_router(admin_router_module.router)

    admin_user = AuthenticatedUser(
        user_id=seeded_state["user_id"],
        email="admin@example.com",
        org_id=seeded_state["org_id"],
        role="owner",
        is_superuser=False,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, seeded_state

    app.dependency_overrides.clear()


async def _seed_batch(
    session_maker,
    *,
    org_id: str,
    idempotency_key: str = "key-1",
    source_system: str = "github",
    source_instance: str = "acme/api",
    producer: str | None = "dev-hops-cli",
    items_received: int = 3,
    complete: bool = True,
    rejections: list[status_mod.RejectedRecord] | None = None,
) -> uuid.UUID:
    async with session_maker() as session:
        batch = await status_mod.create_batch(
            session,
            ingestion_id=uuid.uuid4(),
            org_id=org_id,
            idempotency_key=idempotency_key,
            payload_hash="hash-1",
            source_system=source_system,
            source_instance=source_instance,
            producer=producer,
            producer_version="0.1.0",
            schema_version="external-ingest.v1",
            window_started_at=None,
            window_ended_at=None,
            items_received=items_received,
        )
        await session.commit()
        if complete:
            await status_mod.mark_processing(
                session, org_id=org_id, ingestion_id=batch.ingestion_id
            )
            rejections = rejections or []
            await status_mod.complete_batch(
                session,
                org_id=org_id,
                ingestion_id=batch.ingestion_id,
                items_accepted=items_received - len(rejections),
                items_rejected=len(rejections),
                rejections=rejections,
            )
            await session.commit()
        return batch.ingestion_id


# ---------------------------------------------------------------------------
# GET /customer-push/sources/{source_id}/batches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_source_batches_filters_by_status_and_producer(
    client, session_maker
):
    async_client, seeded_state = client
    org_id = seeded_state["org_id"]
    completed_id = await _seed_batch(
        session_maker, org_id=org_id, idempotency_key="k1", producer="dev-hops-cli"
    )
    await _seed_batch(
        session_maker,
        org_id=org_id,
        idempotency_key="k2",
        producer="other-producer",
        complete=False,
    )

    resp = await async_client.get(
        f"/api/v1/admin/customer-push/sources/{seeded_state['source_id']}/batches",
        params={"status": "completed"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["ingestion_id"] == str(completed_id)

    resp = await async_client.get(
        f"/api/v1/admin/customer-push/sources/{seeded_state['source_id']}/batches",
        params={"producer": "other-producer"},
    )
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["producer"] == "other-producer"


@pytest.mark.asyncio
async def test_list_source_batches_unknown_source_404s(client):
    async_client, _ = client
    resp = await async_client.get(
        f"/api/v1/admin/customer-push/sources/{uuid.uuid4()}/batches"
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /customer-push/batches/{ingestion_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_batch_detail_includes_rejected_records_and_record_counts(
    client, session_maker
):
    async_client, seeded_state = client
    org_id = seeded_state["org_id"]
    rejections = [
        status_mod.RejectedRecord(
            0, "commit.v1", "c0", "missing_required_field", "boom", "hash"
        ),
    ]
    ingestion_id = await _seed_batch(
        session_maker, org_id=org_id, items_received=2, rejections=rejections
    )

    resp = await async_client.get(f"/api/v1/admin/customer-push/batches/{ingestion_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["ingestion_id"] == str(ingestion_id)
    assert body["status"] == "partial"
    assert body["record_counts"] is None  # not populated by any v1 writer
    assert "recompute_status" not in body  # CC21: added by CHAOS-2699 in wave 3
    assert body["rejected_records_total"] == 1
    assert body["rejected_records"][0] == {
        "index": 0,
        "kind": "commit.v1",
        "external_id": "c0",
        "code": "missing_required_field",
        "message": "boom",
        "path": "hash",
    }


@pytest.mark.asyncio
async def test_get_batch_detail_nonexistent_and_malformed_both_404(client):
    async_client, _ = client
    resp_missing = await async_client.get(
        f"/api/v1/admin/customer-push/batches/{uuid.uuid4()}"
    )
    resp_malformed = await async_client.get(
        "/api/v1/admin/customer-push/batches/not-a-uuid"
    )

    assert resp_missing.status_code == 404
    assert resp_malformed.status_code == 404


@pytest.mark.asyncio
async def test_get_batch_detail_cross_org_404s(client, session_maker):
    async_client, _ = client
    ingestion_id = await _seed_batch(session_maker, org_id=str(uuid.uuid4()))

    resp = await async_client.get(f"/api/v1/admin/customer-push/batches/{ingestion_id}")

    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /customer-push/schemas*
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_list_schemas_passthrough(client):
    async_client, _ = client
    resp = await async_client.get("/api/v1/admin/customer-push/schemas")

    assert resp.status_code == 200
    body = resp.json()
    assert body["schemaVersions"] == ["external-ingest.v1"]
    assert "commit.v1" in body["recordKinds"]
    assert set(body["limits"]) == {"maxRecordsPerBatch", "maxBodyBytes"}


@pytest.mark.asyncio
async def test_admin_get_schema_unknown_version_404s(client):
    async_client, _ = client
    resp = await async_client.get(
        "/api/v1/admin/customer-push/schemas/external-ingest.v99"
    )
    assert resp.status_code == 404
