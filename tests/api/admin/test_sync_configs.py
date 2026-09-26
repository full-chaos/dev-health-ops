from __future__ import annotations

import importlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncDispatchOutbox,
    SyncExecutedProofLedger,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
)
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobRunStatus,
    ScheduledJob,
    Setting,
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
from dev_health_ops.models.users import Organization, User
from dev_health_ops.sync.canonical_incident_gate import CANONICAL_INCIDENT_FEATURE_KEY
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
sync_router_module = importlib.import_module("dev_health_ops.api.admin.routers.sync")

_TABLES = tables_of(
    User,
    Organization,
    FeatureFlag,
    OrgFeatureOverride,
    OrgLicense,
    IntegrationCredential,
    SyncConfiguration,
    ScheduledJob,
    JobRun,
    Setting,
    Integration,
    IntegrationSource,
    IntegrationDataset,
    SyncDispatchOutbox,
    SyncRun,
    SyncRunReferenceDiscovery,
    SyncRunUnit,
    # CHAOS-4114: triggering a sync runs plan_sync_run, which records every
    # planned pair as ATTEMPTED in the executed-proof ledger inside the same
    # transaction. An explicit `tables=` list does not pull it in.
    SyncExecutedProofLedger,
    SyncWatermark,
    SyncCoverageProjection,
)


class _CroniterStub:
    def __init__(self, *_args, **_kwargs):
        self._next = 0.0

    def get_next(self, _type):
        self._next += 86400.0
        return self._next


def test_planner_source_rows_accepts_github_full_name():
    integration_id = uuid.uuid4()
    config_id = uuid.uuid4()
    payload = sync_router_module.SyncConfigBatchCreate(
        name="Full Chaos",
        provider="github",
        sync_options={"owner": "fallback-owner"},
        repos=["acme/web"],
    )

    rows = sync_router_module._planner_source_rows(
        payload,
        {},
        {},
        "org-test",
        integration_id,
        config_id,
    )

    assert len(rows) == 1
    source = rows[0]
    assert source.external_id == "acme/web"
    assert source.full_name == "acme/web"
    assert source.name == "web"
    assert source.metadata_ == {
        "owner": "acme",
        "planner_managed_sync_config_id": str(config_id),
    }


def test_planner_source_rows_keeps_gitlab_slash_path_unchanged():
    integration_id = uuid.uuid4()
    config_id = uuid.uuid4()
    payload = sync_router_module.SyncConfigBatchCreate(
        name="GitLab",
        provider="gitlab",
        sync_options={"group": "fallback-group"},
        repos=["group/subgroup/project"],
    )

    rows = sync_router_module._planner_source_rows(
        payload,
        {},
        {"group/subgroup/project": (42, "group/subgroup/project")},
        "org-test",
        integration_id,
        config_id,
    )

    assert len(rows) == 1
    source = rows[0]
    assert source.external_id == "42"
    assert source.full_name == "group/subgroup/project"
    assert source.name == "project"
    assert source.metadata_ == {
        "path_with_namespace": "group/subgroup/project",
        "planner_managed_sync_config_id": str(config_id),
    }


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "sync-configs.db"
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
    org = Organization(id=org_id, slug="test-org", name="Test Org", tier="pro")
    user = User(id=user_id, email="admin@example.com", is_active=True)
    feature = FeatureFlag(
        key=CANONICAL_INCIDENT_FEATURE_KEY,
        name="Canonical incident ingestion",
        category="integrations",
        min_tier="community",
        is_enabled=True,
    )

    async with session_maker() as session:
        session.add_all([org, user, feature])
        await session.flush()
        session.add(
            OrgFeatureOverride(
                org_id=org_id,
                feature_id=feature.id,
                is_enabled=True,
            )
        )
        await session.commit()

    return {
        "org_id": str(org_id),
        "user_id": str(user_id),
    }


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


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


async def _create_sync_config(ac, name: str = "my-sync", provider: str = "github"):
    # github/gitlab plain creates are token-wide (all_repos); non-git providers
    # ignore the flag and materialize a single source. Either way the config is
    # integration-native and triggerable.
    return await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": name,
            "provider": provider,
            "sync_targets": [],
            "sync_options": {"all_repos": True},
        },
    )


async def _create_migrated_config(
    session_maker,
    org_id: str,
    *,
    name: str = "migrated-sync",
    provider: str = "github",
    is_active: bool = True,
) -> str:
    """Insert an integration-linked SyncConfiguration + Integration/source/dataset.

    Seeds the integration, one enabled source and one enabled dataset so the
    trigger endpoint routes through the fan-out planner (plan_sync_run +
    dispatch_sync_run).
    """
    async with session_maker() as session:
        integration = Integration(
            org_id=org_id,
            provider=provider,
            name=f"{name}-integration",
            config={},
            is_active=True,
        )
        session.add(integration)
        await session.flush()
        source = IntegrationSource(
            org_id=org_id,
            integration_id=integration.id,
            provider=provider,
            source_type="repository",
            external_id="acme/repo",
            name="repo",
            full_name="acme/repo",
            metadata_={},
            is_enabled=True,
        )
        dataset = IntegrationDataset(
            org_id=org_id,
            integration_id=integration.id,
            dataset_key="commits",
            is_enabled=True,
            options={},
        )
        config = SyncConfiguration(
            org_id=org_id,
            name=name,
            provider=provider,
            sync_targets=["git"],
            sync_options={},
            is_active=is_active,
            integration_id=integration.id,
        )
        session.add_all([source, dataset, config])
        await session.flush()
        config_id = str(config.id)
        await session.commit()
    return config_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repo_limit_advisory_lock_key_is_deterministic_and_sqlite_noops(
    session_maker,
):
    org_uuid = str(uuid.uuid4())
    slug = "full-chaos/test-org"

    assert sync_router_module._repo_limit_advisory_lock_key(
        org_uuid
    ) == sync_router_module._repo_limit_advisory_lock_key(org_uuid)
    assert sync_router_module._repo_limit_advisory_lock_key(
        slug
    ) == sync_router_module._repo_limit_advisory_lock_key(slug)
    assert 0 <= sync_router_module._repo_limit_advisory_lock_key(slug) < 2**63
    assert sync_router_module._repo_limit_advisory_lock_key(
        org_uuid
    ) != sync_router_module._repo_limit_advisory_lock_key(slug)

    async with session_maker() as session:
        execute = AsyncMock()
        session.execute = execute

        await sync_router_module._acquire_repo_limit_create_lock(session, org_uuid)

    execute.assert_not_called()


async def _tests_dataset_row(session_maker, integration_id: uuid.UUID):
    async with session_maker() as session:
        return (
            await session.execute(
                select(IntegrationDataset).where(
                    IntegrationDataset.integration_id == integration_id,
                    IntegrationDataset.dataset_key == "tests",
                )
            )
        ).scalar_one_or_none()


@pytest.mark.asyncio
async def test_trigger_sync_config_returns_202_for_migrated_config(
    client, session_maker
):
    """A migrated, integration-linked config triggers through the fan-out planner
    and returns 202 with the planner sync_run_id."""
    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="trigger-migrated"
    )

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.return_value = MagicMock(id="fake-task-id")

    with patch(
        "dev_health_ops.workers.sync_units.dispatch_sync_run.apply_async",
        mock_dispatch.apply_async,
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["status"] == "triggered"
    assert data["config_id"] == config_id
    assert data["sync_run_id"]
    mock_dispatch.apply_async.assert_not_called()

    async with session_maker() as session:
        sync_run = await session.get(SyncRun, uuid.UUID(data["sync_run_id"]))
        outbox = (
            await session.execute(
                select(SyncDispatchOutbox).where(
                    SyncDispatchOutbox.sync_run_id == uuid.UUID(data["sync_run_id"]),
                    SyncDispatchOutbox.kind == "reference_discovery",
                )
            )
        ).scalar_one()

    assert sync_run is not None
    assert sync_run.status == "planned"
    assert outbox.status == "pending"


@pytest.mark.asyncio
async def test_trigger_sync_config_nonexistent_returns_404(client):
    ac, _ = client

    resp = await ac.post(f"/api/v1/admin/sync-configs/{uuid.uuid4()}/trigger")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_sync_config_does_not_depend_on_celery(client, session_maker):
    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="celery-fail-test"
    )

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.side_effect = Exception(
        "Celery broker connection refused"
    )

    with patch(
        "dev_health_ops.workers.sync_units.dispatch_sync_run.apply_async",
        mock_dispatch.apply_async,
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    mock_dispatch.apply_async.assert_not_called()

    data = resp.json()
    async with session_maker() as session:
        job_run = await session.get(JobRun, uuid.UUID(data["run_id"]))
        sync_run = await session.get(SyncRun, uuid.UUID(data["sync_run_id"]))
        outbox = (
            await session.execute(
                select(SyncDispatchOutbox).where(
                    SyncDispatchOutbox.sync_run_id == uuid.UUID(data["sync_run_id"]),
                    SyncDispatchOutbox.kind == "reference_discovery",
                )
            )
        ).scalar_one()

    assert job_run is not None
    assert job_run.status == JobRunStatus.PENDING.value
    assert sync_run is not None
    assert sync_run.status == "planned"
    assert outbox.status == "pending"


def test_sync_run_unit_range_normalizes_naive_datetimes_to_utc():
    source_id = uuid.uuid4()
    run_id = uuid.uuid4()
    unit = SyncRunUnit(
        org_id="org-1",
        sync_run_id=run_id,
        integration_id=uuid.uuid4(),
        source_id=source_id,
        provider="github",
        dataset_key="commits",
        cost_class="standard",
        mode="incremental",
        since_at=datetime(2026, 1, 1),
        before_at=datetime(2026, 1, 2),
        status="success",
        attempts=1,
    )

    coverage_range = sync_router_module._sync_run_unit_range([unit])

    assert coverage_range is not None
    assert coverage_range.since == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert coverage_range.before == datetime(2026, 1, 2, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Provider-scoped uniqueness tests (CHAOS-2243)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# CHAOS-2255: PENDING JobRun created at trigger time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_creates_pending_job_run(client, session_maker):
    """Trigger persists a PENDING JobRun without publishing a Celery task."""
    from dev_health_ops.models.settings import JobRun, JobRunStatus

    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="pending-run-test"
    )

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.return_value = MagicMock(id="pending-task-id")

    with patch(
        "dev_health_ops.workers.sync_units.dispatch_sync_run.apply_async",
        mock_dispatch.apply_async,
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["status"] == "triggered"
    assert "run_id" in data
    run_id = data["run_id"]
    mock_dispatch.apply_async.assert_not_called()

    # Verify a PENDING JobRun row was persisted.
    async with session_maker() as session:
        result = await session.execute(
            select(JobRun).where(JobRun.id == uuid.UUID(run_id))
        )
        run = result.scalar_one_or_none()

    assert run is not None
    assert run.status == JobRunStatus.PENDING.value
    assert run.triggered_by == "manual"


@pytest.mark.asyncio
async def test_trigger_commits_pending_job_run_with_durable_outbox(
    client, session_maker
):
    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="pre-enqueue-commit-test"
    )

    mock_dispatch = MagicMock()

    with patch(
        "dev_health_ops.workers.sync_units.dispatch_sync_run.apply_async",
        mock_dispatch.apply_async,
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    mock_dispatch.apply_async.assert_not_called()

    data = resp.json()
    async with session_maker() as session:
        job_run = await session.get(JobRun, uuid.UUID(data["run_id"]))
        sync_run = await session.get(SyncRun, uuid.UUID(data["sync_run_id"]))
        outbox = (
            await session.execute(
                select(SyncDispatchOutbox).where(
                    SyncDispatchOutbox.sync_run_id == uuid.UUID(data["sync_run_id"]),
                    SyncDispatchOutbox.kind == "reference_discovery",
                )
            )
        ).scalar_one()

    assert job_run is not None
    assert job_run.status == JobRunStatus.PENDING.value
    assert sync_run is not None
    assert sync_run.status == "planned"
    assert outbox.status == "pending"


@pytest.mark.asyncio
async def test_trigger_keeps_runs_pending_when_celery_is_unavailable(
    client, session_maker
):
    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="enqueue-failure-test"
    )

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.side_effect = RuntimeError("broker down")

    with patch(
        "dev_health_ops.workers.sync_units.dispatch_sync_run.apply_async",
        mock_dispatch.apply_async,
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    mock_dispatch.apply_async.assert_not_called()

    async with session_maker() as session:
        job_runs = list((await session.execute(select(JobRun))).scalars().all())
        sync_runs = list((await session.execute(select(SyncRun))).scalars().all())
        outboxes = list(
            (await session.execute(select(SyncDispatchOutbox))).scalars().all()
        )

    assert len(job_runs) == 1
    assert job_runs[0].status == JobRunStatus.PENDING.value
    assert job_runs[0].completed_at is None
    assert job_runs[0].error is None
    assert len(sync_runs) == 1
    assert sync_runs[0].status == "planned"
    assert len(outboxes) == 1
    assert outboxes[0].kind == "reference_discovery"
    assert outboxes[0].status == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger_child", [False, True])
async def test_trigger_inactive_config_returns_409_without_execution(
    client, session_maker, trigger_child
):
    ac, seeded_state = client
    org_id = seeded_state["org_id"]

    async with session_maker() as session:
        parent = SyncConfiguration(
            name="paused-parent",
            provider="github",
            org_id=org_id,
            sync_targets=["git"],
            is_active=False,
        )
        session.add(parent)
        await session.flush()
        target_id = parent.id
        if trigger_child:
            child = SyncConfiguration(
                name="paused-parent/repo",
                provider="github",
                org_id=org_id,
                sync_targets=["git"],
                sync_options={"owner": "paused-parent", "repo": "repo"},
                is_active=False,
                parent_id=parent.id,
            )
            session.add(child)
            await session.flush()
            target_id = child.id
        await session.commit()

    mock_dispatch = MagicMock()
    with patch(
        "dev_health_ops.workers.sync_units.dispatch_sync_run.apply_async",
        mock_dispatch.apply_async,
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{target_id}/trigger")

    assert resp.status_code == 409
    assert "paused" in resp.json()["detail"]
    mock_dispatch.apply_async.assert_not_called()

    async with session_maker() as session:
        job_runs = (await session.execute(select(JobRun))).scalars().all()
        sync_runs = (await session.execute(select(SyncRun))).scalars().all()
    assert job_runs == []
    assert sync_runs == []


# ---------------------------------------------------------------------------
# Batch create — provider-shaped child sync_options (CHAOS-2283)
# ---------------------------------------------------------------------------


def _gitlab_project(project_id: int, name: str, full_name: str):
    from dev_health_ops.connectors.models import Repository

    return Repository(
        id=project_id,
        name=name,
        full_name=full_name,
        default_branch="main",
    )


class _FakeGitLabCodeClient:
    """Async context manager stub for GitLabCodeClient (CHAOS-2817/CS15b).

    Batch-create name resolution now discovers group projects through the
    canonical httpx code client instead of ``GitLabConnector.list_repositories``.
    """

    def __init__(self, projects):
        self.list_projects = AsyncMock(return_value=projects)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return None


# ---------------------------------------------------------------------------
# CHAOS-2681: connecting an integration must NOT silently create+start a broad
# sync. The connect step alone yields no sync config (no enabled, no running);
# a sync only starts after explicit repository selection (/sync-configs/batch)
# followed by an explicit start (/sync-configs/{id}/trigger).
# ---------------------------------------------------------------------------


async def _simulate_connect(session_maker, org_id: str) -> None:
    """Mimic the GitHub App connect step: a credential, nothing else."""
    async with session_maker() as session:
        session.add(
            IntegrationCredential(
                provider="github",
                name="github-app",
                org_id=org_id,
                credentials_encrypted="enc",
                is_active=True,
            )
        )
        await session.commit()
