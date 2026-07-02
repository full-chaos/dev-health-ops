"""API tests for the CHAOS-2694 status-store GET endpoints.

Follows tests/api/test_external_ingest_router.py's fixture pattern: override
the bound per-scope dependency object (not the ``require_ingest_scope``
factory -- FastAPI's dependency_overrides matches by the exact callable
passed to ``Depends()``) plus the Postgres session dependency, against an
aiosqlite in-memory DB seeded directly via status.py (never via HTTP -- the
POST accept path is out of scope for this issue).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.external_ingest import status as status_mod
from dev_health_ops.api.external_ingest.auth import IngestAuthContext
from dev_health_ops.api.main import app
from dev_health_ops.models.external_ingest import (
    ExternalIngestBatch,
    ExternalIngestRejection,
)
from dev_health_ops.models.git import Base
from tests._helpers import tables_of

BASE = "/api/v1/external-ingest"
ORG_A = "test-org"
ORG_B = "other-org"

_TABLES = tables_of(ExternalIngestBatch, ExternalIngestRejection)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "external-ingest-status-api.db"
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
async def client(session_maker):
    ctx = IngestAuthContext(org_id=ORG_A, scopes=frozenset({"ingest:status"}))

    async def _session_override():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[status_mod._require_ingest_status] = lambda: ctx
    app.dependency_overrides[status_mod.get_postgres_session_dep] = _session_override

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c, session_maker
    finally:
        app.dependency_overrides.pop(status_mod._require_ingest_status, None)
        app.dependency_overrides.pop(status_mod.get_postgres_session_dep, None)


async def _seed_batch(
    session_maker,
    *,
    org_id: str = ORG_A,
    idempotency_key: str = "key-1",
    source_system: str = "github",
    source_instance: str = "acme/api",
    items_received: int = 3,
    complete: bool = True,
    rejections: list[status_mod.RejectedRecord] | None = None,
    items_accepted: int = 2,
    items_rejected: int = 1,
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
            producer="dev-hops-cli",
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
            await status_mod.complete_batch(
                session,
                org_id=org_id,
                ingestion_id=batch.ingestion_id,
                items_accepted=items_accepted,
                items_rejected=items_rejected,
                rejections=rejections or [],
            )
            await session.commit()
        return batch.ingestion_id


# ---------------------------------------------------------------------------
# GET /batches/{ingestion_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_batch_detail_happy_path(client):
    async_client, session_maker = client
    rejections = [
        status_mod.RejectedRecord(
            0, "commit.v1", "c0", "missing_required_field", "boom", "hash"
        ),
        status_mod.RejectedRecord(
            1, "commit.v1", "c1", "invalid_literal", "bad", "state"
        ),
        status_mod.RejectedRecord(
            2, "commit.v1", "c2", "invalid_literal", "bad", "state"
        ),
    ]
    ingestion_id = await _seed_batch(
        session_maker,
        items_received=5,
        items_accepted=2,
        items_rejected=3,
        rejections=rejections,
    )

    resp = await async_client.get(f"{BASE}/batches/{ingestion_id}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["ingestionId"] == str(ingestion_id)
    assert body["status"] == "partial"
    assert body["itemsReceived"] == 5
    assert body["itemsAccepted"] == 2
    assert body["itemsRejected"] == 3
    assert body["source"] == {"system": "github", "instance": "acme/api"}
    assert body["producer"] == "dev-hops-cli"
    assert body["producerVersion"] == "0.1.0"
    assert body["errorsTotal"] == 3
    assert body["errorsLimit"] == 50
    assert body["errorsOffset"] == 0
    assert len(body["errors"]) == 3
    assert body["errors"][0] == {
        "index": 0,
        "kind": "commit.v1",
        "externalId": "c0",
        "code": "missing_required_field",
        "message": "boom",
        "path": "hash",
    }
    assert body["errorSummary"]["total_rejected"] == 3


@pytest.mark.asyncio
async def test_get_batch_detail_pagination(client):
    async_client, session_maker = client
    rejections = [
        status_mod.RejectedRecord(i, "commit.v1", f"c{i}", "code", "msg", None)
        for i in range(3)
    ]
    ingestion_id = await _seed_batch(
        session_maker,
        items_received=3,
        items_accepted=0,
        items_rejected=3,
        rejections=rejections,
    )

    resp = await async_client.get(
        f"{BASE}/batches/{ingestion_id}", params={"errorLimit": 1, "errorOffset": 1}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["errorsTotal"] == 3
    assert body["errorsLimit"] == 1
    assert body["errorsOffset"] == 1
    assert len(body["errors"]) == 1
    assert body["errors"][0]["index"] == 1


@pytest.mark.asyncio
async def test_get_batch_detail_nonexistent_returns_404(client):
    async_client, _ = client
    resp = await async_client.get(f"{BASE}/batches/{uuid.uuid4()}")

    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "not_found"


@pytest.mark.asyncio
async def test_get_batch_detail_cross_org_returns_identical_404(client):
    async_client, session_maker = client
    ingestion_id = await _seed_batch(session_maker, org_id=ORG_B)

    resp = await async_client.get(f"{BASE}/batches/{ingestion_id}")
    nonexistent_resp = await async_client.get(f"{BASE}/batches/{uuid.uuid4()}")

    assert resp.status_code == 404
    assert resp.json() == nonexistent_resp.json()


# ---------------------------------------------------------------------------
# GET /batches (list)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_batches_filters_by_status_and_source(client):
    async_client, session_maker = client
    completed_id = await _seed_batch(
        session_maker,
        idempotency_key="k1",
        source_system="github",
        complete=True,
        items_accepted=3,
        items_rejected=0,
    )
    accepted_id = await _seed_batch(
        session_maker, idempotency_key="k2", source_system="gitlab", complete=False
    )

    resp = await async_client.get(f"{BASE}/batches", params={"status": "completed"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["ingestionId"] == str(completed_id)

    resp = await async_client.get(f"{BASE}/batches", params={"sourceSystem": "gitlab"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["ingestionId"] == str(accepted_id)


@pytest.mark.asyncio
async def test_list_batches_pagination_total_independent_of_limit(client):
    async_client, session_maker = client
    for i in range(5):
        await _seed_batch(session_maker, idempotency_key=f"page-{i}", complete=False)

    resp = await async_client.get(f"{BASE}/batches", params={"limit": 2, "offset": 0})
    body = resp.json()

    assert body["total"] == 5
    assert len(body["items"]) == 2
    assert body["limit"] == 2
    assert body["offset"] == 0


@pytest.mark.asyncio
async def test_list_batches_only_returns_requesting_org(client):
    async_client, session_maker = client
    await _seed_batch(session_maker, org_id=ORG_A, idempotency_key="mine")
    await _seed_batch(session_maker, org_id=ORG_B, idempotency_key="theirs")

    resp = await async_client.get(f"{BASE}/batches")
    body = resp.json()

    assert body["total"] == 1
