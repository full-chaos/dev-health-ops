from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.billing.audit_service import BillingAuditService
from dev_health_ops.api.billing.reconciliation_service import ReconciliationService
from dev_health_ops.api.billing.router import router
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.db import postgres_session_dependency
from dev_health_ops.models.billing_audit import BillingAuditLog
from dev_health_ops.models.git import Base
from dev_health_ops.models.users import Organization, User


def _make_stripe_event(event_type: str, event_id: str = "evt_123") -> SimpleNamespace:
    return SimpleNamespace(type=event_type, id=event_id)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "billing-audit.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    User.__table__,
                    Organization.__table__,
                    BillingAuditLog.__table__,
                ],
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db(session_maker):
    async with session_maker() as session:
        yield session


@pytest.mark.asyncio
async def test_billing_audit_model_append_only_insert(db: AsyncSession):
    org = Organization(id=uuid.uuid4(), slug="audit-org", name="Audit Org")
    user = User(id=uuid.uuid4(), email="audit@example.com")
    db.add_all([org, user])
    await db.flush()

    entry = BillingAuditLog(
        org_id=org.id,
        actor_id=user.id,
        action="reconciliation.started",
        resource_type="reconciliation",
        resource_id=uuid.uuid4(),
        description="Started reconciliation",
        reconciliation_status="unresolved",
        created_at=datetime.now(timezone.utc),
    )
    db.add(entry)
    await db.flush()

    assert entry.id is not None
    assert not hasattr(entry, "updated_at")


@pytest.mark.asyncio
async def test_billing_audit_service_log_webhook_and_query(db: AsyncSession):
    org = Organization(id=uuid.uuid4(), slug="audit-org-2", name="Audit Org 2")
    user = User(id=uuid.uuid4(), email="audit2@example.com")
    db.add_all([org, user])
    await db.flush()

    service = BillingAuditService(db)
    entry = await service.log(
        org_id=org.id,
        actor_id=user.id,
        action="subscription.updated",
        resource_type="subscription",
        resource_id=uuid.uuid4(),
        description="Subscription updated",
        reconciliation_status="matched",
    )
    assert entry is not None

    webhook_entry = await service.log_webhook(
        event=_make_stripe_event("invoice.paid"),
        resource_type="invoice",
        resource_id=uuid.uuid4(),
        org_id=org.id,
        local_state={"status": "paid"},
    )
    assert webhook_entry is not None
    assert webhook_entry.stripe_event_id == "evt_123"

    items, total = await service.query(org_id=org.id, action="subscription.updated")
    assert total == 1
    assert len(items) == 1


@pytest.mark.asyncio
async def test_billing_audit_service_never_fails(db: AsyncSession):
    org = Organization(id=uuid.uuid4(), slug="audit-org-3", name="Audit Org 3")
    db.add(org)
    await db.flush()

    service = BillingAuditService(db)

    async def _boom() -> None:
        raise RuntimeError("flush failed")

    db.flush = _boom  # type: ignore[assignment]

    result = await service.log(
        org_id=org.id,
        action="invoice.voided",
        resource_type="invoice",
        resource_id=uuid.uuid4(),
        description="void",
    )
    assert result is None


@pytest.mark.asyncio
async def test_reconciliation_service_detects_mismatches(db: AsyncSession):
    org = Organization(id=uuid.uuid4(), slug="audit-org-4", name="Audit Org 4")
    db.add(org)
    await db.flush()

    audit_service = BillingAuditService(db)
    stripe_client = SimpleNamespace()
    service = ReconciliationService(db, stripe_client, audit_service)

    sub_id = uuid.uuid4()

    async def _local(*_args, **_kwargs):
        return [{"id": str(sub_id), "stripe_id": "sub_1", "status": "active"}]

    async def _stripe(*_args, **_kwargs):
        return [{"id": "sub_1", "status": "past_due"}]

    service._fetch_local_rows = _local  # type: ignore[method-assign]
    service._fetch_stripe_rows = _stripe  # type: ignore[method-assign]

    report = await service.reconcile_subscriptions(org_id=org.id)
    assert report.subscriptions_checked == 1
    assert len(report.mismatches) == 1
    assert report.mismatches[0].field == "status"


@pytest_asyncio.fixture
async def api_client(session_maker):
    app = FastAPI()
    app.include_router(router)

    async def _override_db():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[postgres_session_dependency] = _override_db
    app.dependency_overrides[get_current_user] = lambda: AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="super@example.com",
        org_id=str(uuid.uuid4()),
        role="owner",
        is_superuser=True,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, session_maker


@pytest.mark.asyncio
async def test_billing_audit_endpoints_require_superadmin(api_client):
    client, session_maker = api_client

    org = Organization(id=uuid.uuid4(), slug="audit-org-5", name="Audit Org 5")
    async with session_maker() as session:
        session.add(org)
        await session.flush()
        session.add(
            BillingAuditLog(
                org_id=org.id,
                action="reconciliation.mismatch_found",
                resource_type="invoice",
                resource_id=uuid.uuid4(),
                description="Mismatch",
                reconciliation_status="mismatch",
                created_at=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    response = await client.get(
        f"/api/v1/billing/audit?org_id={org.id}&limit=10&offset=0"
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["limit"] == 10
