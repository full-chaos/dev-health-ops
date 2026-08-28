"""CHAOS-4393 regression: DB-level coverage projection rebuild through a fold.

``rebuild_sync_coverage_projection`` (the full orchestration path --
``resolve_effective_scope`` -> ``_stream_compact_unit_windows`` ->
``build_coverage_summary_payload``) had no end-to-end test: every existing
sync-coverage test either fed pre-expanded ``UnitWindow`` objects straight
into ``build_coverage_summary_payload`` or exercised
``_effective_dataset_keys``/``_query_dataset_keys_for_scope`` in isolation.
This seeds real ``IntegrationDataset``/``SyncRun``/``SyncRunUnit`` rows and
rebuilds the projection end to end, closing that gap for the CHAOS-4078
TestOps (cicd/tests) fold.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.sync_coverage import (
    _CoverageQueryBudget,
    rebuild_sync_coverage_projection,
    resolve_effective_scope,
)
from dev_health_ops.models import (
    Integration,
    IntegrationDataset,
    IntegrationSource,
    SyncConfiguration,
)
from dev_health_ops.models.backfill import BackfillJob
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import SyncRun, SyncRunUnit
from dev_health_ops.models.settings import ScheduledJob
from dev_health_ops.models.sync_coverage import SyncCoverageProjection
from tests._helpers import tables_of

ORG_ID = "org-chaos-4393"


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'coverage.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(
                    Integration,
                    IntegrationSource,
                    IntegrationDataset,
                    SyncConfiguration,
                    SyncRun,
                    SyncRunUnit,
                    SyncCoverageProjection,
                    BackfillJob,
                    ScheduledJob,
                ),
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


def _dt(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 8, day, hour, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_testops_fold_closes_tests_gap_via_cicd_success(db: AsyncSession):
    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    db.add(
        Integration(
            id=integration_id,
            org_id=ORG_ID,
            provider="github",
            name="acme",
            is_active=True,
        )
    )
    config = SyncConfiguration(
        org_id=ORG_ID,
        name="github",
        provider="github",
        sync_targets=["cicd"],
        sync_options={},
        is_active=True,
        planner_managed=True,
        integration_id=integration_id,
    )
    db.add(config)
    await db.flush()
    db.add(
        IntegrationSource(
            id=source_id,
            org_id=ORG_ID,
            integration_id=integration_id,
            provider="github",
            source_type="repository",
            external_id="acme/api",
            name="api",
            full_name="acme/api",
            metadata_={"planner_managed_sync_config_id": str(config.id)},
            is_enabled=True,
        )
    )
    # Both `tests` and `cicd` enabled -- the real-world prod shape from
    # CHAOS-4393: the org enabled the alias dataset, and the planner folds it
    # onto its canonical `cicd` writer.
    for key in ("tests", "cicd"):
        db.add(
            IntegrationDataset(
                id=uuid.uuid4(),
                org_id=ORG_ID,
                integration_id=integration_id,
                dataset_key=key,
                is_enabled=True,
                options={},
            )
        )
    await db.flush()

    # Old, pre-fold FAILED unit persisted directly under the alias key
    # "tests" -- the literal shape CHAOS-4125 describes (planned/failed under
    # the raw alias identity before the runtime learned to fold it).
    old_failed_run_id = uuid.uuid4()
    db.add(
        SyncRun(
            id=old_failed_run_id,
            org_id=ORG_ID,
            integration_id=integration_id,
            triggered_by="scheduler",
            mode="incremental",
            status="failed",
            total_units=1,
            completed_units=0,
            failed_units=1,
        )
    )
    db.add(
        SyncRunUnit(
            id=uuid.uuid4(),
            org_id=ORG_ID,
            sync_run_id=old_failed_run_id,
            integration_id=integration_id,
            source_id=source_id,
            provider="github",
            dataset_key="tests",
            cost_class="standard",
            mode="incremental",
            since_at=_dt(19),
            before_at=_dt(21),
            status="failed",
            attempts=5,
            processor_flags=None,
        )
    )

    # Later SUCCESS composite "cicd" unit, over the SAME window, carrying the
    # TestOps fold's completion flag for "tests".
    sync_run_id = uuid.uuid4()
    db.add(
        SyncRun(
            id=sync_run_id,
            org_id=ORG_ID,
            integration_id=integration_id,
            triggered_by="scheduler",
            mode="incremental",
            status="success",
            total_units=1,
            completed_units=1,
            failed_units=0,
        )
    )
    db.add(
        SyncRunUnit(
            id=uuid.uuid4(),
            org_id=ORG_ID,
            sync_run_id=sync_run_id,
            integration_id=integration_id,
            source_id=source_id,
            provider="github",
            dataset_key="cicd",
            cost_class="standard",
            mode="incremental",
            since_at=_dt(19),
            before_at=_dt(21),
            status="success",
            attempts=1,
            processor_flags={"family_dataset_tests": True},
        )
    )
    await db.flush()

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())
    assert "tests" in scope.dataset_keys
    assert "cicd" in scope.dataset_keys

    payload = await rebuild_sync_coverage_projection(
        db,
        ORG_ID,
        config,
        generated_at=_dt(28),
        scope=scope,
    )

    datasets = {d["dataset_key"]: d for d in payload["datasets"]}
    assert "tests" in datasets, datasets.keys()
    assert datasets["tests"]["gaps"] == [], datasets["tests"]
    assert datasets["tests"]["failed_ranges"] == [], datasets["tests"]
    assert datasets["tests"]["status"] != "failed", datasets["tests"]


@pytest.mark.asyncio
async def test_testops_fold_leaves_a_genuine_cicd_gap_alone(db: AsyncSession):
    """The inverse: a config with only `cicd` enabled and NO success unit at
    all must still show `cicd` as a real, unclosed gap. The fold must never
    manufacture coverage for the canonical key itself."""

    integration_id = uuid.uuid4()
    source_id = uuid.uuid4()
    db.add(
        Integration(
            id=integration_id,
            org_id=ORG_ID,
            provider="github",
            name="acme",
            is_active=True,
        )
    )
    config = SyncConfiguration(
        org_id=ORG_ID,
        name="github",
        provider="github",
        sync_targets=["cicd"],
        sync_options={"schedule_cron": "0 0 * * *", "timezone": "UTC"},
        is_active=True,
        planner_managed=True,
        integration_id=integration_id,
    )
    db.add(config)
    await db.flush()
    db.add(
        IntegrationSource(
            id=source_id,
            org_id=ORG_ID,
            integration_id=integration_id,
            provider="github",
            source_type="repository",
            external_id="acme/api",
            name="api",
            full_name="acme/api",
            metadata_={"planner_managed_sync_config_id": str(config.id)},
            is_enabled=True,
        )
    )
    db.add(
        IntegrationDataset(
            id=uuid.uuid4(),
            org_id=ORG_ID,
            integration_id=integration_id,
            dataset_key="cicd",
            is_enabled=True,
            options={},
        )
    )
    db.add(
        ScheduledJob(
            org_id=ORG_ID,
            name="cicd-sync",
            job_type="sync",
            provider="github",
            schedule_cron="0 0 * * *",
            sync_config_id=config.id,
            status=0,
        )
    )
    await db.flush()

    old_failed_run_id = uuid.uuid4()
    db.add(
        SyncRun(
            id=old_failed_run_id,
            org_id=ORG_ID,
            integration_id=integration_id,
            triggered_by="scheduler",
            mode="incremental",
            status="failed",
            total_units=1,
            completed_units=0,
            failed_units=1,
        )
    )
    db.add(
        SyncRunUnit(
            id=uuid.uuid4(),
            org_id=ORG_ID,
            sync_run_id=old_failed_run_id,
            integration_id=integration_id,
            source_id=source_id,
            provider="github",
            dataset_key="cicd",
            cost_class="standard",
            mode="incremental",
            since_at=_dt(19),
            before_at=_dt(21),
            status="failed",
            attempts=5,
            processor_flags=None,
        )
    )
    await db.flush()

    scope = await resolve_effective_scope(db, ORG_ID, config, _CoverageQueryBudget())
    payload = await rebuild_sync_coverage_projection(
        db,
        ORG_ID,
        config,
        generated_at=_dt(28),
        scope=scope,
    )

    datasets = {d["dataset_key"]: d for d in payload["datasets"]}
    assert datasets["cicd"]["failed_ranges"] != [], datasets["cicd"]
    assert datasets["cicd"]["status"] == "failed", datasets["cicd"]
