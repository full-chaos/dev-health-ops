from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.queries.sync_freshness import fetch_latest_successful_sync_at
from dev_health_ops.models.git import Base
from dev_health_ops.models.integrations import SyncRun, SyncRunStatus
from dev_health_ops.models.settings import JobRun, JobRunStatus, ScheduledJob
from tests._helpers import tables_of

pytestmark = pytest.mark.anyio

_TABLES = tables_of(ScheduledJob, JobRun, SyncRun)


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'sync-freshness.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection,
                tables=_TABLES,
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def test_latest_successful_sync_uses_run_history_and_org_scope(session_maker):
    org_id = "org-1"
    legacy_success_at = datetime(2026, 7, 13, 16, 11, tzinfo=UTC)

    async with session_maker() as session:
        job = ScheduledJob(
            name="legacy-sync",
            job_type="sync",
            schedule_cron="0 * * * *",
            org_id=org_id,
            provider="linear",
        )
        non_sync_job = ScheduledJob(
            name="daily-metrics",
            job_type="metrics",
            schedule_cron="0 * * * *",
            org_id=org_id,
        )
        other_org_job = ScheduledJob(
            name="other-org-sync",
            job_type="sync",
            schedule_cron="0 * * * *",
            org_id="org-2",
            provider="github",
        )
        session.add_all([job, non_sync_job, other_org_job])
        await session.flush()

        legacy_success = JobRun(
            job_id=job.id,
            status=JobRunStatus.SUCCESS.value,
        )
        legacy_success.completed_at = legacy_success_at
        legacy_failure = JobRun(
            job_id=job.id,
            status=JobRunStatus.FAILED.value,
        )
        legacy_failure.completed_at = datetime(2026, 7, 13, 16, 20, tzinfo=UTC)
        non_sync_success = JobRun(
            job_id=non_sync_job.id,
            status=JobRunStatus.SUCCESS.value,
        )
        non_sync_success.completed_at = datetime(2026, 7, 13, 16, 40, tzinfo=UTC)
        other_org_legacy_success = JobRun(
            job_id=other_org_job.id,
            status=JobRunStatus.SUCCESS.value,
        )
        other_org_legacy_success.completed_at = datetime(
            2026, 7, 13, 16, 45, tzinfo=UTC
        )

        planner_success = SyncRun(
            org_id=org_id,
            integration_id=uuid.uuid4(),
            triggered_by="manual",
            mode="incremental",
            status=SyncRunStatus.SUCCESS.value,
            total_units=1,
            completed_units=1,
            failed_units=0,
            completed_at=datetime(2026, 7, 13, 16, 5, tzinfo=UTC),
        )
        planner_failure = SyncRun(
            org_id=org_id,
            integration_id=uuid.uuid4(),
            triggered_by="manual",
            mode="incremental",
            status=SyncRunStatus.FAILED.value,
            total_units=1,
            completed_units=0,
            failed_units=1,
            completed_at=datetime(2026, 7, 13, 16, 25, tzinfo=UTC),
        )
        other_org_success = SyncRun(
            org_id="org-2",
            integration_id=uuid.uuid4(),
            triggered_by="manual",
            mode="incremental",
            status=SyncRunStatus.SUCCESS.value,
            total_units=1,
            completed_units=1,
            failed_units=0,
            completed_at=datetime(2026, 7, 13, 16, 30, tzinfo=UTC),
        )
        session.add_all(
            [
                legacy_success,
                legacy_failure,
                non_sync_success,
                other_org_legacy_success,
                planner_success,
                planner_failure,
                other_org_success,
            ]
        )
        await session.commit()

    async with session_maker() as session:
        latest = await fetch_latest_successful_sync_at(session, org_id=org_id)
        missing = await fetch_latest_successful_sync_at(
            session, org_id="org-without-runs"
        )

    assert latest == legacy_success_at
    assert missing is None


async def test_latest_successful_sync_uses_planner_history_without_legacy_runs(
    session_maker,
):
    planner_success_at = datetime(2026, 7, 13, 17, 5, tzinfo=UTC)
    async with session_maker() as session:
        session.add(
            SyncRun(
                org_id="org-1",
                integration_id=uuid.uuid4(),
                triggered_by="scheduler",
                mode="incremental",
                status=SyncRunStatus.SUCCESS.value,
                total_units=1,
                completed_units=1,
                failed_units=0,
                completed_at=planner_success_at,
            )
        )
        await session.commit()

    async with session_maker() as session:
        latest = await fetch_latest_successful_sync_at(session, org_id="org-1")

    assert latest == planner_success_at


async def test_latest_successful_sync_picks_newer_planner_run(session_maker):
    planner_success_at = datetime(2026, 7, 13, 18, 5, tzinfo=UTC)
    async with session_maker() as session:
        job = ScheduledJob(
            name="legacy-sync",
            job_type="sync",
            schedule_cron="0 * * * *",
            org_id="org-1",
            provider="linear",
        )
        session.add(job)
        await session.flush()

        legacy_success = JobRun(
            job_id=job.id,
            status=JobRunStatus.SUCCESS.value,
        )
        legacy_success.completed_at = datetime(2026, 7, 13, 17, 5, tzinfo=UTC)
        planner_success = SyncRun(
            org_id="org-1",
            integration_id=uuid.uuid4(),
            triggered_by="scheduler",
            mode="incremental",
            status=SyncRunStatus.SUCCESS.value,
            total_units=1,
            completed_units=1,
            failed_units=0,
            completed_at=planner_success_at,
        )
        session.add_all([legacy_success, planner_success])
        await session.commit()

    async with session_maker() as session:
        latest = await fetch_latest_successful_sync_at(session, org_id="org-1")

    assert latest == planner_success_at
