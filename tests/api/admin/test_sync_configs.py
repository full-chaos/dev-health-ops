from __future__ import annotations

import importlib
import uuid
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
    SyncRun,
    SyncRunUnit,
)
from dev_health_ops.models.licensing import OrgLicense
from dev_health_ops.models.settings import (
    IntegrationCredential,
    JobRun,
    JobRunStatus,
    JobStatus,
    ScheduledJob,
    Setting,
    SyncConfiguration,
    SyncWatermark,
)
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

admin_router_module = importlib.import_module("dev_health_ops.api.admin")
auth_router_module = importlib.import_module("dev_health_ops.api.auth.router")
sync_router_module = importlib.import_module("dev_health_ops.api.admin.routers.sync")

_TABLES = tables_of(
    User,
    Organization,
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
    SyncRunUnit,
    SyncWatermark,
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

    async with session_maker() as session:
        session.add_all([org, user])
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
async def test_list_sync_configs_empty(client):
    ac, _ = client

    resp = await ac.get("/api/v1/admin/sync-configs")

    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_create_sync_config_returns_201_shape(client):
    ac, _ = client

    resp = await _create_sync_config(ac, name="test-sync", provider="github")

    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "test-sync"
    assert data["provider"] == "github"
    assert data["is_active"] is True
    assert "id" in data
    assert "created_at" in data


@pytest.mark.asyncio
async def test_create_sync_config_persists_to_db(client, session_maker):
    ac, seeded_state = client

    resp = await _create_sync_config(ac, name="persist-test", provider="gitlab")

    assert resp.status_code == 201
    config_id = resp.json()["id"]

    async with session_maker() as session:
        result = await session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.id == uuid.UUID(config_id)
            )
        )
        config = result.scalar_one_or_none()

    assert config is not None
    assert config.name == "persist-test"
    assert config.provider == "gitlab"
    assert config.org_id == seeded_state["org_id"]


@pytest.mark.asyncio
async def test_create_sync_config_creates_scheduled_job(client, session_maker):
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "scheduled-linear",
            "provider": "linear",
            "sync_targets": ["work-items"],
            "sync_options": {},
        },
    )

    assert resp.status_code == 201
    config_id = uuid.UUID(resp.json()["id"])
    async with session_maker() as session:
        result = await session.execute(
            select(ScheduledJob).where(ScheduledJob.sync_config_id == config_id)
        )
        job = result.scalar_one_or_none()

    assert job is not None
    assert job.schedule_cron == "0 * * * *"


@pytest.mark.asyncio
async def test_create_sync_config_folds_top_level_schedule_fields(
    client, session_maker
):
    ac, _ = client

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs",
            json={
                "name": "top-level-schedule",
                "provider": "linear",
                "sync_targets": ["work-items"],
                "sync_options": {"backfill_days": 1},
                "schedule_cron": "30 2 * * *",
                "timezone": "America/Los_Angeles",
                "initial_sync_depth": 14,
            },
        )

    assert resp.status_code == 201, resp.text
    config_id = uuid.UUID(resp.json()["id"])
    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(SyncConfiguration.id == config_id)
            )
        ).scalar_one()
        job = (
            await session.execute(
                select(ScheduledJob).where(ScheduledJob.sync_config_id == config_id)
            )
        ).scalar_one()

    assert config.sync_options["schedule_cron"] == "30 2 * * *"
    assert config.sync_options["timezone"] == "America/Los_Angeles"
    assert config.sync_options["initial_sync_depth"] == 14
    assert job.provider == "linear"
    assert job.timezone == "America/Los_Angeles"
    assert job.job_config == {"provider": "linear", "sync_config_id": str(config_id)}


@pytest.mark.asyncio
async def test_create_sync_config_rejects_invalid_timezone(client):
    """Invalid selected timezone is rejected at write time (CHAOS-2689) instead
    of silently falling back to UTC at dispatch time."""
    ac, _ = client

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs",
            json={
                "name": "bad-tz",
                "provider": "linear",
                "sync_targets": ["work-items"],
                "sync_options": {"backfill_days": 1},
                "schedule_cron": "30 2 * * *",
                "timezone": "Not/AZone",
            },
        )

    assert resp.status_code == 422, resp.text
    assert "timezone" in resp.text.lower()


@pytest.mark.asyncio
async def test_batch_create_linear_without_repos_creates_active_config_and_job(
    client, session_maker
):
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "linear-work-items",
            "provider": "linear",
            "sync_targets": ["work-items"],
            "sync_options": {},
            "repos": [],
            "schedule_cron": "15 * * * *",
        },
    )

    assert resp.status_code == 201
    data = resp.json()
    assert data["children"] == []
    assert data["parent"]["is_active"] is True
    config_id = uuid.UUID(data["parent"]["id"])
    async with session_maker() as session:
        config_result = await session.execute(
            select(SyncConfiguration).where(SyncConfiguration.id == config_id)
        )
        config = config_result.scalar_one_or_none()
        job_result = await session.execute(
            select(ScheduledJob).where(ScheduledJob.sync_config_id == config_id)
        )
        job = job_result.scalar_one_or_none()

    assert config is not None
    assert config.is_active is True
    assert config.sync_options["schedule_cron"] == "15 * * * *"
    assert job is not None
    assert job.schedule_cron == "15 * * * *"


@pytest.mark.asyncio
async def test_batch_create_counts_cross_provider_planner_sources_for_repo_limit(
    client, monkeypatch
):
    ac, _ = client
    requested_counts = []

    class TierLimitStub:
        def __init__(self, _session):
            pass

        def check_repo_limit(self, _org_id, requested_count):
            requested_counts.append(requested_count)
            if requested_count > 3:
                return False, "Repo limit exceeded"
            return True, None

    monkeypatch.setattr(sync_router_module, "TierLimitService", TierLimitStub)

    first = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "repo-cap-one",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos"},
            "repos": ["one", "two"],
        },
    )
    second = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "repo-cap-gitlab",
            "provider": "gitlab",
            "sync_targets": ["git"],
            "sync_options": {"group": "full-chaos"},
            "repos": ["3", "4"],
        },
    )

    assert first.status_code == 201, first.text
    assert second.status_code == 403, second.text
    assert requested_counts == [2, 4]


@pytest.mark.asyncio
async def test_single_create_counts_existing_planner_sources_for_repo_limit(
    client, monkeypatch
):
    ac, _ = client
    requested_counts = []

    class TierLimitStub:
        def __init__(self, _session):
            pass

        def check_repo_limit(self, _org_id, requested_count):
            requested_counts.append(requested_count)
            if requested_count > 2:
                return False, "Repo limit exceeded"
            return True, None

    monkeypatch.setattr(sync_router_module, "TierLimitService", TierLimitStub)

    batch = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "repo-cap-batch",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos"},
            "repos": ["one", "two"],
        },
    )
    single = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "repo-cap-single",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos", "repo": "three"},
        },
    )

    assert batch.status_code == 201, batch.text
    assert single.status_code == 403, single.text
    assert requested_counts == [2, 3]


@pytest.mark.asyncio
async def test_create_sync_config_acquires_repo_limit_lock_before_count(
    client, monkeypatch
):
    ac, _ = client
    calls: list[str] = []

    async def acquire_lock(_session, _org_id):
        calls.append("lock")

    async def count_repos(_session, _org_id):
        calls.append("count")
        return 0

    monkeypatch.setattr(
        sync_router_module, "_acquire_repo_limit_create_lock", acquire_lock
    )
    monkeypatch.setattr(
        sync_router_module, "_active_repo_usage_count_for_limit", count_repos
    )

    resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "lock-before-count-single",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos", "all_repos": True},
        },
    )

    assert resp.status_code == 201, resp.text
    assert calls[:2] == ["lock", "count"]


@pytest.mark.asyncio
async def test_batch_create_sync_configs_acquires_repo_limit_lock_before_count(
    client, monkeypatch
):
    ac, _ = client
    calls: list[str] = []

    async def acquire_lock(_session, _org_id):
        calls.append("lock")

    async def count_repos(_session, _org_id):
        calls.append("count")
        return 0

    monkeypatch.setattr(
        sync_router_module, "_acquire_repo_limit_create_lock", acquire_lock
    )
    monkeypatch.setattr(
        sync_router_module, "_active_repo_usage_count_for_limit", count_repos
    )

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "lock-before-count-batch",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "full-chaos"},
            "repos": ["dev-health"],
        },
    )

    assert resp.status_code == 201, resp.text
    assert calls[:2] == ["lock", "count"]


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


@pytest.mark.asyncio
async def test_get_sync_config_by_id(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="get-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}")

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == config_id
    assert data["name"] == "get-test"


@pytest.mark.asyncio
async def test_get_sync_config_nonexistent_returns_404(client):
    ac, _ = client

    resp = await ac.get(f"/api/v1/admin/sync-configs/{uuid.uuid4()}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_sync_config_changes_is_active(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="update-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{config_id}",
        json={"is_active": False},
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["is_active"] is False
    assert data["id"] == config_id


@pytest.mark.asyncio
async def test_update_sync_config_merges_top_level_schedule_fields(
    client, session_maker
):
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": "merge-schedule",
            "provider": "linear",
            "sync_targets": ["work-items"],
            "sync_options": {"backfill_days": 7},
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        resp = await ac.patch(
            f"/api/v1/admin/sync-configs/{config_id}",
            json={
                "schedule_cron": "45 3 * * *",
                "timezone": "Europe/London",
                "initial_sync_depth": 21,
            },
        )

    assert resp.status_code == 200, resp.text
    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()

    assert config.sync_options["backfill_days"] == 7
    assert config.sync_options["schedule_cron"] == "45 3 * * *"
    assert config.sync_options["timezone"] == "Europe/London"
    assert config.sync_options["initial_sync_depth"] == 21
    assert job.schedule_cron == "45 3 * * *"
    assert job.provider == "linear"
    assert job.timezone == "Europe/London"
    # An explicit schedule on an active config keeps the job ACTIVE.
    assert job.status == JobStatus.ACTIVE.value


@pytest.mark.asyncio
async def test_update_sync_config_explicit_null_clears_schedule(client, session_maker):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="clear-schedule")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        set_resp = await ac.patch(
            f"/api/v1/admin/sync-configs/{config_id}",
            json={"schedule_cron": "0 0 * * *", "timezone": "America/Los_Angeles"},
        )
    assert set_resp.status_code == 200, set_resp.text

    # Clear the schedule. The payload also carries a stale nested copy of the
    # old cron (what a stale client sends); it must NOT resurrect the schedule.
    resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{config_id}",
        json={
            "schedule_cron": None,
            "timezone": "America/Los_Angeles",
            "sync_options": {
                "owner": "full-chaos",
                "schedule_cron": "0 0 * * *",
            },
        },
    )

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "schedule_cron" not in data["sync_options"]
    assert data["sync_options"]["owner"] == "full-chaos"
    assert data["sync_options"]["timezone"] == "America/Los_Angeles"
    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
    assert "schedule_cron" not in config.sync_options
    assert config.sync_options["owner"] == "full-chaos"
    # Clearing the schedule parks the ScheduledJob so the scheduler never
    # auto-dispatches a manual-only config (CHAOS-2297).
    assert job.status == JobStatus.PAUSED.value


@pytest.mark.asyncio
async def test_update_sync_config_omitted_schedule_fields_left_unchanged(
    client, session_maker
):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="omit-schedule")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    with (
        patch(
            "dev_health_ops.licensing.gating._check_org_feature_async",
            return_value=True,
        ),
        patch.object(sync_router_module, "Croniter", _CroniterStub),
    ):
        set_resp = await ac.patch(
            f"/api/v1/admin/sync-configs/{config_id}",
            json={"schedule_cron": "30 2 * * *", "timezone": "Europe/Berlin"},
        )
    assert set_resp.status_code == 200, set_resp.text

    # Omitting schedule fields entirely must leave the stored values untouched.
    resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{config_id}",
        json={"is_active": False},
    )

    assert resp.status_code == 200, resp.text
    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
    assert config.is_active is False
    assert config.sync_options["schedule_cron"] == "30 2 * * *"
    assert config.sync_options["timezone"] == "Europe/Berlin"


@pytest.mark.asyncio
async def test_create_sync_config_without_schedule_creates_paused_job(
    client, session_maker
):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="manual-only")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    async with session_maker() as session:
        job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()

    # Manual-only configs (no schedule_cron) keep a job row for manual-trigger
    # JobRun anchoring but must stay PAUSED (CHAOS-2297).
    assert job.status == JobStatus.PAUSED.value


@pytest.mark.asyncio
async def test_schedule_job_upsert_propagates_schedule_cron(
    client, session_maker, seeded_state
):
    ac, _ = client

    create_resp = await _create_sync_config(
        ac, name="schedule-update", provider="linear"
    )
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    async with session_maker() as session:
        config_result = await session.execute(
            select(SyncConfiguration).where(
                SyncConfiguration.id == uuid.UUID(config_id)
            )
        )
        config = config_result.scalar_one()
        config.sync_options = {"schedule_cron": "5 * * * *"}
        await sync_router_module._upsert_scheduled_job(
            session, config, seeded_state["org_id"]
        )
        await session.flush()
        result = await session.execute(
            select(ScheduledJob).where(
                ScheduledJob.sync_config_id == uuid.UUID(config_id)
            )
        )
        job = result.scalar_one_or_none()

    assert job is not None
    assert job.schedule_cron == "5 * * * *"


@pytest.mark.asyncio
async def test_schedule_job_upsert_sets_provider_timezone_and_job_config(
    client, session_maker, seeded_state
):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="schedule-meta", provider="linear")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        config.sync_options = {"schedule_cron": "10 * * * *", "timezone": "Asia/Tokyo"}
        await sync_router_module._upsert_scheduled_job(
            session, config, seeded_state["org_id"]
        )
        await session.flush()
        job = (
            await session.execute(
                select(ScheduledJob).where(
                    ScheduledJob.sync_config_id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()

    assert job.provider == "linear"
    assert job.timezone == "Asia/Tokyo"
    assert job.job_config == {"provider": "linear", "sync_config_id": config_id}


@pytest.mark.asyncio
async def test_delete_sync_config(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="delete-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    del_resp = await ac.delete(f"/api/v1/admin/sync-configs/{config_id}")

    assert del_resp.status_code == 204

    get_resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_nonexistent_sync_config_returns_404(client):
    ac, _ = client

    resp = await ac.delete(f"/api/v1/admin/sync-configs/{uuid.uuid4()}")

    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider, sync_targets, sync_options",
    [
        ("jira", ["work-items"], {"project_key": "ENG"}),
        ("linear", ["work-items"], {"team_id": "team-uuid"}),
        ("launchdarkly", ["feature-flags"], {"project_key": "default"}),
    ],
)
async def test_create_non_git_sync_config_is_integration_native_and_triggerable(
    client, session_maker, provider, sync_targets, sync_options
):
    """Provider matrix (non-git): POST /sync-configs materializes an
    integration-linked, planner-managed config with exactly one planner-tagged
    source for jira/linear/launchdarkly, so they trigger real units instead of
    the old "no linked integration" 400. These providers have no repo list and
    previously fell through to a bare, unroutable config.
    """
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": f"{provider}-matrix",
            "provider": provider,
            "sync_targets": sync_targets,
            "sync_options": sync_options,
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]

    async with session_maker() as session:
        config = await session.get(SyncConfiguration, uuid.UUID(config_id))
        assert config is not None
        assert config.integration_id is not None
        assert config.planner_managed is True
        integration = await session.get(Integration, config.integration_id)
        assert integration is not None and integration.provider == provider
        enabled_sources = (
            (
                await session.execute(
                    select(IntegrationSource).where(
                        IntegrationSource.integration_id == config.integration_id,
                        IntegrationSource.is_enabled.is_(True),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(enabled_sources) == 1
        assert (enabled_sources[0].metadata_ or {}).get(
            "planner_managed_sync_config_id"
        ) == config_id

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.return_value = MagicMock(id="fake-task-id")
    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    assert resp.json()["total_units"] >= 1
    mock_dispatch.apply_async.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider, repo", [("github", "acme/repo"), ("gitlab", "123")])
async def test_batch_create_git_sync_config_is_triggerable_with_units(
    client, session_maker, provider, repo
):
    """Provider matrix (git): POST /sync-configs/batch with a concrete repo
    materializes one planner source and plans a non-empty run, so a single-repo
    git create is triggerable with units (not a silent zero-unit 202)."""
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": f"{provider}-batch",
            "provider": provider,
            "sync_targets": ["git"],
            "repos": [repo],
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["parent"]["id"]

    async with session_maker() as session:
        config = await session.get(SyncConfiguration, uuid.UUID(config_id))
        assert config is not None and config.integration_id is not None
        assert config.planner_managed is True
        enabled_sources = (
            (
                await session.execute(
                    select(IntegrationSource).where(
                        IntegrationSource.integration_id == config.integration_id,
                        IntegrationSource.is_enabled.is_(True),
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(enabled_sources) == 1

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.return_value = MagicMock(id="fake-task-id")
    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    assert resp.json()["total_units"] >= 1
    mock_dispatch.apply_async.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["github", "gitlab"])
async def test_create_git_sync_config_without_all_repos_is_rejected(client, provider):
    """git providers must select repos via /batch or set all_repos; a plain create
    with neither would be a zero-source, zero-unit no-op, so it is rejected with
    400 instead of silently acknowledging triggers."""
    ac, _ = client
    resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": f"{provider}-bare",
            "provider": provider,
            "sync_targets": ["git"],
        },
    )
    assert resp.status_code == 400, resp.text
    assert "all_repos" in resp.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["github", "gitlab"])
async def test_create_git_all_repos_sync_config_is_integration_native(
    client, session_maker, provider
):
    """git all_repos plain create is integration-native and planner-managed;
    sources are populated separately via POST /integrations/{id}/discover."""
    ac, _ = client
    create_resp = await ac.post(
        "/api/v1/admin/sync-configs",
        json={
            "name": f"{provider}-allrepos",
            "provider": provider,
            "sync_targets": ["git"],
            "sync_options": {"all_repos": True},
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["id"]
    async with session_maker() as session:
        config = await session.get(SyncConfiguration, uuid.UUID(config_id))
        assert config is not None
        assert config.integration_id is not None
        assert config.planner_managed is True


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
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["status"] == "triggered"
    assert data["config_id"] == config_id
    assert data["sync_run_id"]
    # Fan-out dispatch enqueues the planner run on the shared sync queue.
    mock_dispatch.apply_async.assert_called_once()
    assert mock_dispatch.apply_async.call_args.kwargs["queue"] == "sync"
    assert mock_dispatch.apply_async.call_args.kwargs["args"] == (data["sync_run_id"],)


@pytest.mark.asyncio
async def test_trigger_sync_config_nonexistent_returns_404(client):
    ac, _ = client

    resp = await ac.post(f"/api/v1/admin/sync-configs/{uuid.uuid4()}/trigger")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_sync_config_celery_unavailable_returns_503(
    client, session_maker
):
    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="celery-fail-test"
    )

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.side_effect = Exception(
        "Celery broker connection refused"
    )

    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 503
    assert "unavailable" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_list_sync_config_jobs_empty(client):
    ac, _ = client

    create_resp = await _create_sync_config(ac, name="jobs-test")
    assert create_resp.status_code == 201
    config_id = create_resp.json()["id"]

    resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/jobs")

    assert resp.status_code == 200
    assert resp.json() == []


# ---------------------------------------------------------------------------
# Provider-scoped uniqueness tests (CHAOS-2243)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_name_different_providers_can_coexist(client):
    """Two configs with the same name but different providers must both be created."""
    ac, _ = client

    resp_gh = await _create_sync_config(ac, name="chaos", provider="github")
    assert resp_gh.status_code == 201, resp_gh.text

    resp_lin = await _create_sync_config(ac, name="chaos", provider="linear")
    assert resp_lin.status_code == 201, resp_lin.text

    assert resp_gh.json()["id"] != resp_lin.json()["id"]

    list_resp = await ac.get("/api/v1/admin/sync-configs")
    assert list_resp.status_code == 200
    names = [c["name"] for c in list_resp.json()]
    assert names.count("chaos") == 2


@pytest.mark.asyncio
async def test_same_name_same_provider_is_rejected(client):
    """Two configs with the same (name, provider) must fail with a DB-level error."""
    from sqlalchemy.exc import IntegrityError

    ac, _ = client

    resp1 = await _create_sync_config(ac, name="dup-config", provider="github")
    assert resp1.status_code == 201, resp1.text

    # Second create with identical (name, provider) must raise a DB integrity error.
    # The ASGI test transport propagates the unhandled IntegrityError as an exception
    # rather than returning an HTTP response, so we catch it here.
    with pytest.raises((IntegrityError, Exception)) as exc_info:
        await _create_sync_config(ac, name="dup-config", provider="github")
    assert "UNIQUE" in str(exc_info.value) or "unique" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_delete_targets_correct_provider(client, session_maker):
    """Deleting a config by ID removes only the matching provider row."""
    ac, _ = client

    resp_gh = await _create_sync_config(ac, name="shared-name", provider="github")
    resp_lin = await _create_sync_config(ac, name="shared-name", provider="linear")
    assert resp_gh.status_code == 201
    assert resp_lin.status_code == 201

    gh_id = resp_gh.json()["id"]
    lin_id = resp_lin.json()["id"]

    del_resp = await ac.delete(f"/api/v1/admin/sync-configs/{gh_id}")
    assert del_resp.status_code == 204

    # GitHub config gone, Linear config still present.
    assert (await ac.get(f"/api/v1/admin/sync-configs/{gh_id}")).status_code == 404
    assert (await ac.get(f"/api/v1/admin/sync-configs/{lin_id}")).status_code == 200


@pytest.mark.asyncio
async def test_update_targets_correct_provider(client):
    """Updating a config by ID only modifies the matching provider row."""
    ac, _ = client

    resp_gh = await _create_sync_config(ac, name="upd-name", provider="github")
    resp_lin = await _create_sync_config(ac, name="upd-name", provider="linear")
    assert resp_gh.status_code == 201
    assert resp_lin.status_code == 201

    gh_id = resp_gh.json()["id"]
    lin_id = resp_lin.json()["id"]

    patch_resp = await ac.patch(
        f"/api/v1/admin/sync-configs/{gh_id}",
        json={"is_active": False},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["is_active"] is False

    # Linear config must remain active.
    lin_resp = await ac.get(f"/api/v1/admin/sync-configs/{lin_id}")
    assert lin_resp.status_code == 200
    assert lin_resp.json()["is_active"] is True


# ---------------------------------------------------------------------------
# CHAOS-2255: PENDING JobRun created at trigger time
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_creates_pending_job_run(client, session_maker):
    """Trigger must persist a PENDING JobRun before dispatching the fan-out run."""
    from dev_health_ops.models.settings import JobRun, JobRunStatus

    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="pending-run-test"
    )

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.return_value = MagicMock(id="pending-task-id")

    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text
    data = resp.json()
    assert data["status"] == "triggered"
    assert "run_id" in data
    run_id = data["run_id"]

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
async def test_trigger_commits_pending_job_run_before_enqueue(client, session_maker):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="pre-enqueue-commit-test"
    )
    sync_url = str(session_maker.kw["bind"].url).replace("sqlite+aiosqlite", "sqlite")

    def assert_pending_run_is_visible(*_args, **_kwargs):
        engine = create_engine(sync_url)
        try:
            with Session(engine) as session:
                assert session.query(JobRun).count() == 1
        finally:
            engine.dispose()
        return MagicMock(id="visible-task-id")

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.side_effect = assert_pending_run_is_visible

    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 202, resp.text


@pytest.mark.asyncio
async def test_trigger_marks_pending_job_run_failed_when_enqueue_fails(
    client, session_maker
):
    from dev_health_ops.models.settings import JobRunStatus

    ac, seeded_state = client
    config_id = await _create_migrated_config(
        session_maker, seeded_state["org_id"], name="enqueue-failure-test"
    )

    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.side_effect = RuntimeError("broker down")

    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert resp.status_code == 503
    assert "broker down" in resp.json()["detail"]

    async with session_maker() as session:
        result = await session.execute(select(JobRun))
        runs = list(result.scalars().all())

    assert len(runs) == 1
    run = runs[0]
    assert run.status == JobRunStatus.FAILED.value
    assert run.completed_at is not None
    assert run.error == "dispatch enqueue failed: broker down"


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
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
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


@pytest.mark.asyncio
async def test_batch_create_github_creates_planner_config_without_children(
    client, session_maker
):
    ac, seeded_state = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gh-batch",
            "provider": "github",
            "sync_targets": ["git", "prs"],
            "sync_options": {"owner": "acme"},
            "repos": ["alpha", "beta"],
            "schedule_cron": "0 3 * * *",
        },
    )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["total_created"] == 0
    assert data["children"] == []
    assert data["parent"]["is_active"] is True

    async with session_maker() as session:
        config_id = uuid.UUID(data["parent"]["id"])
        children = (
            (
                await session.execute(
                    select(SyncConfiguration).where(
                        SyncConfiguration.parent_id == config_id
                    )
                )
            )
            .scalars()
            .all()
        )
        integrations = (
            (
                await session.execute(
                    select(Integration).where(
                        Integration.org_id == seeded_state["org_id"]
                    )
                )
            )
            .scalars()
            .all()
        )
        sources = (await session.execute(select(IntegrationSource))).scalars().all()
        datasets = (await session.execute(select(IntegrationDataset))).scalars().all()
        jobs = (await session.execute(select(ScheduledJob))).scalars().all()

    assert children == []
    assert len(integrations) == 1
    assert str(data["parent"]["id"])
    assert len(sources) == 2
    assert {source.external_id for source in sources} == {"acme/alpha", "acme/beta"}
    assert {dataset.dataset_key for dataset in datasets} >= {
        "repo-metadata",
        "commits",
        "prs",
    }
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.ACTIVE.value


@pytest.mark.asyncio
async def test_get_sync_config_repositories_returns_planner_sources(client):
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gh-repo-read",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "acme"},
            "repos": ["alpha", "beta"],
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["parent"]["id"]

    resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/repositories")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "owner": "acme",
        "repos": ["acme/alpha", "acme/beta"],
        "sync_all_repos": False,
    }


@pytest.mark.asyncio
async def test_replace_sync_config_repositories_updates_planner_sources(
    client, session_maker
):
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gh-repo-replace",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "acme"},
            "repos": ["alpha", "beta"],
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["parent"]["id"]

    resp = await ac.put(
        f"/api/v1/admin/sync-configs/{config_id}/repositories",
        json={"owner": "acme", "repos": ["acme/beta", "acme/gamma"]},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "owner": "acme",
        "repos": ["acme/beta", "acme/gamma"],
        "sync_all_repos": False,
    }
    async with session_maker() as session:
        sources = (await session.execute(select(IntegrationSource))).scalars().all()
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()

    by_external_id = {source.external_id: source for source in sources}
    assert by_external_id["acme/alpha"].is_enabled is False
    assert by_external_id["acme/beta"].is_enabled is True
    assert by_external_id["acme/gamma"].is_enabled is True
    assert config.sync_options["owner"] == "acme"
    assert "all_repos" not in config.sync_options


@pytest.mark.asyncio
async def test_sync_config_repositories_ignore_untagged_sources(client, session_maker):
    ac, _ = client

    create_resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gh-repo-scoped",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "acme"},
            "repos": ["alpha", "beta"],
        },
    )
    assert create_resp.status_code == 201, create_resp.text
    config_id = create_resp.json()["parent"]["id"]

    async with session_maker() as session:
        config = (
            await session.execute(
                select(SyncConfiguration).where(
                    SyncConfiguration.id == uuid.UUID(config_id)
                )
            )
        ).scalar_one()
        session.add(
            IntegrationSource(
                org_id=config.org_id,
                integration_id=config.integration_id,
                provider="github",
                source_type="repository",
                external_id="acme/discovered",
                name="discovered",
                full_name="acme/discovered",
                metadata_={"owner": "acme"},
                is_enabled=True,
            )
        )
        await session.commit()

    get_resp = await ac.get(f"/api/v1/admin/sync-configs/{config_id}/repositories")

    assert get_resp.status_code == 200, get_resp.text
    assert get_resp.json()["repos"] == ["acme/alpha", "acme/beta"]

    put_resp = await ac.put(
        f"/api/v1/admin/sync-configs/{config_id}/repositories",
        json={"owner": "acme", "repos": ["acme/beta"]},
    )

    assert put_resp.status_code == 200, put_resp.text
    assert put_resp.json()["repos"] == ["acme/beta"]
    async with session_maker() as session:
        sources = (await session.execute(select(IntegrationSource))).scalars().all()

    by_external_id = {source.external_id: source for source in sources}
    assert by_external_id["acme/alpha"].is_enabled is False
    assert by_external_id["acme/beta"].is_enabled is True
    assert by_external_id["acme/discovered"].is_enabled is True


@pytest.mark.asyncio
async def test_batch_create_gitlab_children_get_project_id_and_group(
    client, session_maker
):
    """GitLab children carry int project_id + group (+ gitlab_url), not repo."""
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
        _gitlab_project(202, "beta", "acme-group/sub/beta"),
    ]
    mock_connector_cls = MagicMock(return_value=mock_connector)

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            mock_connector_cls,
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git", "prs"],
                "sync_options": {
                    "owner": "acme-group",
                    "gitlab_url": "https://gitlab.example.com",
                },
                "repos": ["alpha", "beta"],
                "schedule_cron": "0 3 * * *",
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["total_created"] == 0
    assert data["children"] == []

    mock_connector_cls.assert_called_once_with(
        url="https://gitlab.example.com", private_token="glpat-test"
    )
    mock_connector.list_repositories.assert_called_once_with(org_name="acme-group")

    async with session_maker() as session:
        sources = (await session.execute(select(IntegrationSource))).scalars().all()
    assert {source.external_id for source in sources} == {"101", "202"}
    assert {source.full_name for source in sources} == {
        "acme-group/alpha",
        "acme-group/sub/beta",
    }


@pytest.mark.asyncio
async def test_batch_create_gitlab_numeric_ids_skip_resolution(client):
    """Numeric repos entries are used as project ids without a credential."""
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gl-batch-ids",
            "provider": "gitlab",
            "sync_targets": ["git"],
            "sync_options": {"group": "acme-group"},
            "repos": ["123", "456"],
        },
    )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["total_created"] == 0
    assert data["children"] == []


@pytest.mark.asyncio
async def test_batch_create_gitlab_unknown_project_name_returns_400(client):
    ac, _ = client

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            MagicMock(return_value=mock_connector),
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-missing",
                "provider": "gitlab",
                "credential_id": str(uuid.uuid4()),
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["ghost"],
            },
        )

    assert resp.status_code == 400
    assert "ghost" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_batch_create_gitlab_names_without_credential_returns_400(client):
    ac, _ = client

    resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "gl-batch-nocred",
            "provider": "gitlab",
            "sync_targets": ["git"],
            "sync_options": {"owner": "acme-group"},
            "repos": ["alpha"],
        },
    )

    assert resp.status_code == 400
    assert "credential_id" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_batch_create_gitlab_credential_url_persisted_into_children(
    client, session_maker
):
    """Self-hosted URL from the credential lands in parent + child options.

    Regression: name resolution used the credential's self-hosted URL but
    children only inherited a gitlab_url already present in parent options,
    so they later synced against the gitlab.com runtime default.
    """
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=(
            {"token": "glpat-test", "url": "https://gitlab.internal.example.com"},
            MagicMock(config={}),
        )
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]
    mock_connector_cls = MagicMock(return_value=mock_connector)

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            mock_connector_cls,
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-selfhosted",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["alpha"],
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()

    # Resolution used the credential's self-hosted URL...
    mock_connector_cls.assert_called_once_with(
        url="https://gitlab.internal.example.com", private_token="glpat-test"
    )
    # ...and that URL is persisted into both parent and child options.
    assert (
        data["parent"]["sync_options"]["gitlab_url"]
        == "https://gitlab.internal.example.com"
    )
    assert data["children"] == []
    async with session_maker() as session:
        source = (await session.execute(select(IntegrationSource))).scalar_one()
    assert source.external_id == "101"
    assert source.full_name == "acme-group/alpha"


@pytest.mark.asyncio
async def test_batch_create_gitlab_numeric_entry_matching_name_resolves_as_name(
    client, session_maker
):
    """A numeric entry that matches a listed project NAME is a name, not an id."""
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(7007, "007", "acme-group/007"),
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            MagicMock(return_value=mock_connector),
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-numeric-name",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["007"],
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    mock_connector.list_repositories.assert_called_once_with(org_name="acme-group")

    assert data["children"] == []
    async with session_maker() as session:
        source = (await session.execute(select(IntegrationSource))).scalar_one()
    assert source.full_name == "acme-group/007"
    assert source.external_id == "7007"


@pytest.mark.asyncio
async def test_batch_create_gitlab_numeric_entry_not_in_listing_used_as_id(
    client, session_maker
):
    """A numeric entry matching no listed name keeps project-id semantics."""
    ac, _ = client
    credential_id = str(uuid.uuid4())

    mock_creds_svc = MagicMock()
    mock_creds_svc.get_decrypted_credentials_by_id = AsyncMock(
        return_value=({"token": "glpat-test"}, MagicMock(config={}))
    )
    mock_connector = MagicMock()
    mock_connector.list_repositories.return_value = [
        _gitlab_project(101, "alpha", "acme-group/alpha"),
    ]

    with (
        patch.object(
            sync_router_module,
            "IntegrationCredentialsService",
            return_value=mock_creds_svc,
        ),
        patch(
            "dev_health_ops.connectors.gitlab.GitLabConnector",
            MagicMock(return_value=mock_connector),
        ),
    ):
        resp = await ac.post(
            "/api/v1/admin/sync-configs/batch",
            json={
                "name": "gl-batch-numeric-id",
                "provider": "gitlab",
                "credential_id": credential_id,
                "sync_targets": ["git"],
                "sync_options": {"owner": "acme-group"},
                "repos": ["12345"],
            },
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    # The listing WAS consulted (credential + group present)...
    mock_connector.list_repositories.assert_called_once_with(org_name="acme-group")

    assert data["children"] == []
    async with session_maker() as session:
        source = (await session.execute(select(IntegrationSource))).scalar_one()
    assert source.external_id == "12345"
    assert source.full_name == "acme-group/12345"


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


@pytest.mark.asyncio
async def test_connect_only_creates_no_enabled_or_running_sync(client, session_maker):
    ac, seeded_state = client
    await _simulate_connect(session_maker, seeded_state["org_id"])

    # The connect step alone must not materialize any sync config or job run.
    list_resp = await ac.get("/api/v1/admin/sync-configs")
    assert list_resp.status_code == 200
    assert list_resp.json() == []

    async with session_maker() as session:
        configs = (await session.execute(select(SyncConfiguration))).scalars().all()
        jobs = (await session.execute(select(ScheduledJob))).scalars().all()
        runs = (await session.execute(select(JobRun))).scalars().all()

    assert configs == []
    assert jobs == []
    assert runs == []


@pytest.mark.asyncio
async def test_sync_starts_only_after_explicit_select_and_start(client, session_maker):
    ac, seeded_state = client
    await _simulate_connect(session_maker, seeded_state["org_id"])

    # Step 1 (explicit select): batch-create with concrete repos. This enables a
    # planner config but must NOT dispatch a run on its own.
    select_resp = await ac.post(
        "/api/v1/admin/sync-configs/batch",
        json={
            "name": "explicit-select",
            "provider": "github",
            "sync_targets": ["git"],
            "sync_options": {"owner": "acme"},
            "repos": ["web"],
        },
    )
    assert select_resp.status_code == 201, select_resp.text
    config_id = select_resp.json()["parent"]["id"]

    async with session_maker() as session:
        runs_after_select = (await session.execute(select(JobRun))).scalars().all()
    # Selecting repositories enables the config but starts no sync run.
    assert runs_after_select == []

    # Step 2 (explicit start): trigger dispatches a run and persists a PENDING
    # JobRun. dispatch is mocked so no Celery broker is needed.
    mock_dispatch = MagicMock()
    mock_dispatch.apply_async.return_value = MagicMock(id="task-id")
    with patch(
        "dev_health_ops.api.admin.routers.sync.dispatch_sync_run", mock_dispatch
    ):
        trigger_resp = await ac.post(f"/api/v1/admin/sync-configs/{config_id}/trigger")

    assert trigger_resp.status_code == 202, trigger_resp.text
    assert trigger_resp.json()["status"] == "triggered"
    mock_dispatch.apply_async.assert_called_once()

    async with session_maker() as session:
        runs_after_start = (await session.execute(select(JobRun))).scalars().all()
    assert len(runs_after_start) == 1
    assert runs_after_start[0].status == JobRunStatus.PENDING.value
