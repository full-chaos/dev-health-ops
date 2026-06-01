from __future__ import annotations

import importlib
import logging
import uuid
from typing import Any

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.org_deletion import OrganizationDeletionService
from dev_health_ops.models.audit import AuditLog
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.billing import BillingPlan, BillingPrice
from dev_health_ops.models.billing_audit import BillingAuditLog
from dev_health_ops.models.checkpoints import MetricCheckpoint
from dev_health_ops.models.git import Base
from dev_health_ops.models.impersonation import ImpersonationSession
from dev_health_ops.models.invoices import Invoice, InvoiceLineItem
from dev_health_ops.models.ip_allowlist import OrgIPAllowlist
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.org_invite import OrgInvite
from dev_health_ops.models.refresh_token import RefreshToken
from dev_health_ops.models.refunds import Refund
from dev_health_ops.models.reports import ReportRun, SavedReport
from dev_health_ops.models.retention import OrgRetentionPolicy
from dev_health_ops.models.settings import (
    IdentityMapping,
    IntegrationCredential,
    JobRun,
    ScheduledJob,
    Setting,
    SyncConfiguration,
    SyncWatermark,
    TeamMapping,
)
from dev_health_ops.models.sso import SSOProvider
from dev_health_ops.models.subscriptions import Subscription, SubscriptionEvent
from dev_health_ops.models.teams import Team
from dev_health_ops.models.users import Membership, Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")

_TABLES = tables_of(
    User,
    Organization,
    BillingPlan,
    BillingPrice,
    FeatureFlag,
    Membership,
    Setting,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
    IdentityMapping,
    TeamMapping,
    SyncWatermark,
    SavedReport,
    ReportRun,
    OrgRetentionPolicy,
    OrgInvite,
    BackfillJob,
    RefreshToken,
    MetricCheckpoint,
    ImpersonationSession,
    Team,
    Subscription,
    SubscriptionEvent,
    Invoice,
    InvoiceLineItem,
    Refund,
    AuditLog,
    BillingAuditLog,
    SSOProvider,
    OrgIPAllowlist,
    OrgFeatureOverride,
    OrgLicense,
)


class _ClickHouseResult:
    def __init__(self, rows: list[tuple[Any, ...]]):
        self.result_rows = rows


class _FakeClickHouseClient:
    def __init__(
        self, counts: dict[str, int], org_id_types: dict[str, str] | None = None
    ):
        self.counts = counts
        self.org_id_types = org_id_types or {}
        self.commands: list[tuple[str, dict[str, str] | None]] = []

    def query(self, query: str, parameters: dict[str, str] | None = None):
        params = parameters or {}
        if "system.columns" in query:
            table = params["table"]
            return _ClickHouseResult([(self.org_id_types.get(table, "String"),)])
        table = query.split("`")[1]
        return _ClickHouseResult([(self.counts.get(table, 0),)])

    def command(self, query: str, parameters: dict[str, str] | None = None) -> None:
        self.commands.append((query, parameters))


@pytest_asyncio.fixture
async def session_maker(tmp_path):
    db_path = tmp_path / "org-deletion.db"
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


async def _row_count(session: AsyncSession, model, predicate) -> int:
    count = await session.scalar(
        select(func.count()).select_from(model).where(predicate)
    )
    return int(count or 0)


async def _seed_org_pair(session: AsyncSession) -> tuple[str, str]:
    org1_id = uuid.uuid4()
    org2_id = uuid.uuid4()
    user_id = uuid.uuid4()
    report1 = SavedReport(org_id=str(org1_id), name="Org One Report")
    report2 = SavedReport(org_id=str(org2_id), name="Org Two Report")
    job1 = ScheduledJob(
        org_id=str(org1_id),
        name="org-one-report",
        job_type="report",
        schedule_cron="0 0 * * *",
    )
    job2 = ScheduledJob(
        org_id=str(org2_id),
        name="org-two-report",
        job_type="report",
        schedule_cron="0 0 * * *",
    )

    session.add_all(
        [
            User(id=user_id, email="admin@example.com", is_active=True),
            Organization(id=org1_id, slug="org-one", name="Org One"),
            Organization(id=org2_id, slug="org-two", name="Org Two"),
            Membership(org_id=org1_id, user_id=user_id, role="owner"),
            Membership(org_id=org2_id, user_id=user_id, role="owner"),
            Setting(
                org_id=str(org1_id),
                category="github",
                key="api_token",
                value="encrypted-secret-value",
                is_encrypted=True,
            ),
            Setting(org_id=str(org2_id), category="github", key="region", value="us"),
            IntegrationCredential(
                org_id=str(org1_id),
                provider="github",
                name="prod",
                credentials_encrypted="encrypted-token-body",
            ),
            IntegrationCredential(org_id=str(org2_id), provider="github", name="prod"),
            SyncConfiguration(
                org_id=str(org1_id), name="org-one-config", provider="github"
            ),
            SyncConfiguration(
                org_id=str(org2_id), name="org-two-config", provider="github"
            ),
            ScheduledJob(
                org_id=str(org1_id),
                name="org-one-sync",
                job_type="sync",
                schedule_cron="0 * * * *",
            ),
            ScheduledJob(
                org_id=str(org2_id),
                name="org-two-sync",
                job_type="sync",
                schedule_cron="0 * * * *",
            ),
            report1,
            report2,
            ReportRun(report_id=report1.id),
            ReportRun(report_id=report2.id),
            SSOProvider(
                org_id=org1_id,
                name="saml",
                protocol="saml",
                encrypted_secrets={"certificate": "encrypted-cert"},
            ),
            Team(id="team-one", org_id=str(org1_id), name="Team One"),
            Team(id="team-two", org_id=str(org2_id), name="Team Two"),
            job1,
            job2,
            JobRun(job_id=job1.id),
            JobRun(job_id=job2.id),
        ]
    )
    await session.flush()
    return str(org1_id), str(org2_id)


@pytest.mark.asyncio
async def test_org_deletion_sync_configs_removal(session_maker):
    async with session_maker() as session:
        org1_id, org2_id = await _seed_org_pair(session)

        assert (
            await _row_count(
                session, SyncConfiguration, SyncConfiguration.org_id == org1_id
            )
            == 1
        )
        assert (
            await _row_count(session, ScheduledJob, ScheduledJob.org_id == org1_id) == 2
        )

        service = OrganizationDeletionService(session)
        result = await service.delete(org1_id, dry_run=False)

        assert result.postgres.tables["sync_configurations"] == 1
        assert result.postgres.tables["scheduled_jobs"] == 2
        assert (
            await _row_count(
                session, SyncConfiguration, SyncConfiguration.org_id == org1_id
            )
            == 0
        )
        assert (
            await _row_count(session, ScheduledJob, ScheduledJob.org_id == org1_id) == 0
        )
        assert (
            await _row_count(
                session, SyncConfiguration, SyncConfiguration.org_id == org2_id
            )
            == 1
        )
        assert (
            await _row_count(session, ScheduledJob, ScheduledJob.org_id == org2_id) == 2
        )


@pytest.mark.asyncio
async def test_org_deletion_dry_run_returns_contract_without_deleting(session_maker):
    async with session_maker() as session:
        org1_id, org2_id = await _seed_org_pair(session)

        service = OrganizationDeletionService(session)
        result = await service.delete(org1_id, dry_run=True)

        payload = result.to_dict()
        assert payload["organization_id"] == org1_id
        assert payload["dry_run"] is True
        assert payload["timestamp"].endswith("Z")
        assert payload["postgres"]["tables"]["organizations"] == 1
        assert payload["postgres"]["tables"]["settings"] == 1
        assert payload["postgres"]["tables"]["scheduled_jobs"] == 2
        assert payload["clickhouse"] == {"total": 0, "tables": {}}
        assert payload["disabled_jobs"] == 2
        assert payload["credentials_deleted"] == 3
        # Warnings depend on whether ClickHouse is reachable in the environment
        # (unconfigured locally vs. configured-but-unmigrated in CI); assert shape.
        assert isinstance(payload["warnings"], list)

        assert (
            await _row_count(
                session, Organization, Organization.id == uuid.UUID(org1_id)
            )
            == 1
        )
        assert (
            await _row_count(
                session, Organization, Organization.id == uuid.UUID(org2_id)
            )
            == 1
        )
        assert await _row_count(session, Setting, Setting.org_id == org1_id) == 1


@pytest.mark.asyncio
async def test_org_deletion_deletes_only_target_org_and_sanitizes_logs(
    session_maker, caplog
):
    async with session_maker() as session:
        org1_id, org2_id = await _seed_org_pair(session)

        caplog.set_level(
            logging.INFO, logger="dev_health_ops.api.services.org_deletion"
        )
        service = OrganizationDeletionService(session)
        result = await service.delete(org1_id)

        assert result.dry_run is False
        assert result.postgres.tables["organizations"] == 1
        assert result.postgres.tables["integration_credentials"] == 1
        assert result.postgres.tables["scheduled_jobs"] == 2
        assert result.disabled_jobs == 2
        assert result.credentials_deleted == 3

        assert (
            await _row_count(
                session, Organization, Organization.id == uuid.UUID(org1_id)
            )
            == 0
        )
        assert await _row_count(session, Setting, Setting.org_id == org1_id) == 0
        assert (
            await _row_count(
                session, IntegrationCredential, IntegrationCredential.org_id == org1_id
            )
            == 0
        )
        assert (
            await _row_count(session, ScheduledJob, ScheduledJob.org_id == org1_id) == 0
        )
        assert await _row_count(session, Team, Team.org_id == org1_id) == 0

        assert (
            await _row_count(
                session, Organization, Organization.id == uuid.UUID(org2_id)
            )
            == 1
        )
        assert await _row_count(session, Setting, Setting.org_id == org2_id) == 1
        assert (
            await _row_count(
                session, IntegrationCredential, IntegrationCredential.org_id == org2_id
            )
            == 1
        )
        assert (
            await _row_count(session, ScheduledJob, ScheduledJob.org_id == org2_id) == 2
        )
        assert await _row_count(session, Team, Team.org_id == org2_id) == 1

    log_output = caplog.text
    assert org1_id in log_output
    assert "encrypted-secret-value" not in log_output
    assert "encrypted-token-body" not in log_output
    assert "encrypted-cert" not in log_output
    # CodeQL py/log-injection (#945): the dry_run query param is logged via a
    # constant literal selected by the boolean, not the tainted value itself.
    # Confirm the finished entry renders the expected boolean text on one line.
    assert "Organization deletion finished" in log_output
    assert "dry_run=False" in log_output


@pytest.mark.asyncio
async def test_org_deletion_credentials_removal(session_maker):
    async with session_maker() as session:
        org1_id, org2_id = await _seed_org_pair(session)

        assert (
            await _row_count(
                session, IntegrationCredential, IntegrationCredential.org_id == org1_id
            )
            == 1
        )

        service = OrganizationDeletionService(session)
        await service.delete(org1_id, dry_run=False)

        assert (
            await _row_count(
                session, IntegrationCredential, IntegrationCredential.org_id == org1_id
            )
            == 0
        )
        assert (
            await _row_count(
                session, IntegrationCredential, IntegrationCredential.org_id == org2_id
            )
            == 1
        )


@pytest.mark.asyncio
async def test_org_deletion_clickhouse_dry_run_counts_without_delete(session_maker):
    async with session_maker() as session:
        org1_id, _org2_id = await _seed_org_pair(session)
        clickhouse = _FakeClickHouseClient(
            counts={"repo_metrics_daily": 3, "ai_attribution": 2},
            org_id_types={"ai_attribution": "UUID"},
        )

        service = OrganizationDeletionService(session, clickhouse_client=clickhouse)
        result = await service.delete(org1_id, dry_run=True)

        assert result.clickhouse.tables["repo_metrics_daily"] == 3
        assert result.clickhouse.tables["ai_attribution"] == 2
        assert result.clickhouse.total == 5
        assert clickhouse.commands == []
        assert result.warnings == []


@pytest.mark.asyncio
async def test_org_deletion_clickhouse_delete_is_org_scoped(session_maker):
    async with session_maker() as session:
        org1_id, _org2_id = await _seed_org_pair(session)
        clickhouse = _FakeClickHouseClient(counts={"repo_metrics_daily": 4})

        service = OrganizationDeletionService(session, clickhouse_client=clickhouse)
        result = await service.delete(org1_id)

        assert result.clickhouse.tables["repo_metrics_daily"] == 4
        assert result.clickhouse.total == 4
        assert clickhouse.commands == [
            (
                "ALTER TABLE `repo_metrics_daily` DELETE WHERE org_id = {org_id:String}",
                {"org_id": org1_id},
            )
        ]


@pytest.mark.asyncio
async def test_org_delete_admin_api_returns_deletion_result(session_maker):
    async with session_maker() as session:
        org1_id, _org2_id = await _seed_org_pair(session)
        await session.commit()

    app = FastAPI()
    app.include_router(admin_router_module.router)
    admin_user = AuthenticatedUser(
        user_id=str(uuid.uuid4()),
        email="admin@example.com",
        org_id=org1_id,
        role="owner",
        is_superuser=True,
    )

    async def _session_override():
        async with session_maker() as session:
            yield session
            await session.commit()

    app.dependency_overrides[auth_router_module.get_current_user] = lambda: admin_user
    app.dependency_overrides[admin_router_module.get_session] = _session_override

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.delete(f"/api/v1/admin/orgs/{org1_id}?dry_run=true")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["organization_id"] == org1_id
    assert payload["dry_run"] is True
    assert payload["postgres"]["tables"]["organizations"] == 1
    assert payload["disabled_jobs"] == 2


@pytest.mark.asyncio
async def test_org_deletion_settings_access_removal(session_maker):
    """Verify settings and access are removed upon org deletion."""
    from dev_health_ops.models.settings import Setting
    from dev_health_ops.models.users import Membership

    async with session_maker() as session:
        org1_id, org2_id = await _seed_org_pair(session)

        # Verify settings and membership exist
        assert await _row_count(session, Setting, Setting.org_id == org1_id) == 1
        assert await _row_count(session, Membership, Membership.org_id == org1_id) == 1

        service = OrganizationDeletionService(session)
        await service.delete(org1_id, dry_run=False)

        # Verify settings and membership deleted for org1, remain for org2
        assert await _row_count(session, Setting, Setting.org_id == org1_id) == 0
        assert await _row_count(session, Membership, Membership.org_id == org1_id) == 0
        assert await _row_count(session, Setting, Setting.org_id == org2_id) == 1
        assert await _row_count(session, Membership, Membership.org_id == org2_id) == 1


@pytest.mark.asyncio
async def test_org_deletion_idempotency(session_maker):
    """Verify repeated deletion is safe."""
    async with session_maker() as session:
        org1_id, _ = await _seed_org_pair(session)
        service = OrganizationDeletionService(session)

        # First deletion
        await service.delete(org1_id, dry_run=False)
        # Second deletion should not raise an error
        await service.delete(org1_id, dry_run=False)

        # Verify org1 is still gone
        assert (
            await _row_count(
                session, Organization, Organization.id == uuid.UUID(org1_id)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_org_deletion_result_counts(session_maker):
    """Verify DeletionResult has useful counts."""
    async with session_maker() as session:
        org1_id, _ = await _seed_org_pair(session)
        service = OrganizationDeletionService(session)
        result = await service.delete(org1_id, dry_run=False)

        assert result.postgres.total >= 0
        assert "organizations" in result.postgres.tables
