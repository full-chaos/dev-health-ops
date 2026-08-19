from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.git import Base
from dev_health_ops.models.reports import (
    ReportRun,
    ReportRunStatus,
    SavedReport,
    ScheduledReportOccurrence,
)
from dev_health_ops.models.settings import ScheduledJob
from dev_health_ops.models.worker_job_outbox import WorkerJobOutbox
from tests._helpers import tables_of


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "saved-reports-api.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(
                    SavedReport,
                    ReportRun,
                    ScheduledJob,
                    ScheduledReportOccurrence,
                    WorkerJobOutbox,
                ),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def seeded_reports(session_maker):
    org_id = "test-org"
    async with session_maker() as session:
        r1 = SavedReport(
            name="Weekly Health",
            org_id=org_id,
            report_plan={"report_type": "weekly_health", "plan_id": "p1"},
            parameters={"team": "backend"},
        )
        r2 = SavedReport(
            name="Monthly Review",
            org_id=org_id,
            report_plan={"report_type": "monthly_review", "plan_id": "p2"},
            is_template=True,
        )
        session.add_all([r1, r2])
        await session.commit()

        run1 = ReportRun(
            report_id=r1.id,
            triggered_by="manual",
            status=ReportRunStatus.SUCCESS.value,
        )
        run1.rendered_markdown = "# Weekly Health\nAll good."
        session.add(run1)
        await session.commit()

    return {
        "org_id": org_id,
        "report1_id": str(r1.id),
        "report2_id": str(r2.id),
        "run1_id": str(run1.id),
    }


def _make_mock_session(session_maker):
    @asynccontextmanager
    async def mock_get_postgres_session():
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    return mock_get_postgres_session


@pytest.mark.asyncio
async def test_resolve_saved_reports(monkeypatch, session_maker, seeded_reports):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    result = await reports_mod.resolve_saved_reports(
        org_id=seeded_reports["org_id"], limit=50, offset=0
    )
    assert result.total == 2
    assert len(result.items) == 2
    names = {r.name for r in result.items}
    assert "Weekly Health" in names
    assert "Monthly Review" in names


@pytest.mark.asyncio
async def test_resolve_saved_report_by_id(monkeypatch, session_maker, seeded_reports):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    result = await reports_mod.resolve_saved_report(
        org_id=seeded_reports["org_id"],
        report_id=seeded_reports["report1_id"],
    )
    assert result is not None
    assert result.name == "Weekly Health"

    missing = await reports_mod.resolve_saved_report(
        org_id=seeded_reports["org_id"],
        report_id=str(uuid.uuid4()),
    )
    assert missing is None


@pytest.mark.asyncio
async def test_resolve_report_runs(monkeypatch, session_maker, seeded_reports):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    result = await reports_mod.resolve_report_runs(
        org_id=seeded_reports["org_id"],
        report_id=seeded_reports["report1_id"],
        limit=10,
    )
    assert result.total == 1
    assert result.items[0].status == "success"
    assert result.items[0].rendered_markdown == "# Weekly Health\nAll good."


@pytest.mark.asyncio
async def test_trigger_report_creates_atomic_run_and_deferred_handoff(
    monkeypatch, session_maker, seeded_reports
):
    from unittest.mock import MagicMock

    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )
    dispatch = MagicMock()
    monkeypatch.setattr(
        "dev_health_ops.workers.report_task.execute_saved_report.apply_async", dispatch
    )

    run = await reports_mod.resolve_trigger_report(
        org_id=seeded_reports["org_id"], report_id=seeded_reports["report1_id"]
    )

    assert run is not None
    assert run.status == ReportRunStatus.PENDING.value
    dispatch.assert_called_once()
    async with session_maker() as session:
        outbox = await session.scalar(select(WorkerJobOutbox))
        assert outbox is not None
        assert outbox.dedupe_key == f"report.run:{run.id}"


@pytest.mark.asyncio
async def test_create_and_delete_saved_report(
    monkeypatch, session_maker, seeded_reports
):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    created = await reports_mod.resolve_create_saved_report(
        org_id=seeded_reports["org_id"],
        input=reports_mod.CreateSavedReportInput(
            name="New Report",
            description="Test creation",
            report_plan=cast(Any, {"report_type": "custom"}),
        ),
    )
    assert created.name == "New Report"
    assert created.description == "Test creation"

    deleted = await reports_mod.resolve_delete_saved_report(
        org_id=seeded_reports["org_id"],
        report_id=created.id,
    )
    assert deleted is True

    gone = await reports_mod.resolve_saved_report(
        org_id=seeded_reports["org_id"],
        report_id=created.id,
    )
    assert gone is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_cron",
    ["not-a-cron", "", "0 6 * * * *", "@daily", "99 * * * *"],
)
async def test_create_saved_report_rejects_unevaluable_cron(
    monkeypatch, session_maker, seeded_reports, bad_cron
):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    with pytest.raises(ValueError, match=r"Invalid report schedule cron expression"):
        await reports_mod.resolve_create_saved_report(
            org_id=seeded_reports["org_id"],
            input=reports_mod.CreateSavedReportInput(
                name="Invalid Schedule",
                report_plan=cast(Any, {"report_type": "custom"}),
                schedule_cron=bad_cron,
            ),
        )

    async with session_maker() as session:
        assert (
            await session.scalar(
                select(SavedReport).where(SavedReport.name == "Invalid Schedule")
            )
            is None
        )
        assert await session.scalar(select(ScheduledJob)) is None


@pytest.mark.asyncio
async def test_clone_saved_report(monkeypatch, session_maker, seeded_reports):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    cloned = await reports_mod.resolve_clone_saved_report(
        org_id=seeded_reports["org_id"],
        input=reports_mod.CloneSavedReportInput(
            source_report_id=seeded_reports["report1_id"],
            new_name="Cloned Weekly",
            parameter_overrides=cast(Any, {"team": "frontend"}),
        ),
    )
    assert cloned is not None
    assert cloned.name == "Cloned Weekly"
    assert cloned.template_source_id == seeded_reports["report1_id"]


@pytest.mark.asyncio
async def test_update_saved_report(monkeypatch, session_maker, seeded_reports):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    updated = await reports_mod.resolve_update_saved_report(
        org_id=seeded_reports["org_id"],
        report_id=seeded_reports["report1_id"],
        input=reports_mod.UpdateSavedReportInput(
            name="Updated Weekly Health",
            is_active=False,
        ),
    )
    assert updated is not None
    assert updated.name == "Updated Weekly Health"
    assert updated.is_active is False


@pytest.mark.asyncio
async def test_report_schedule_writes_the_exact_next_due_marker(
    monkeypatch, session_maker, seeded_reports
):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )
    base = datetime(2026, 7, 24, 6, 30, tzinfo=UTC)
    async with session_maker() as session:
        report = await session.get(SavedReport, uuid.UUID(seeded_reports["report1_id"]))
        assert report is not None
        report.last_run_at = base
        await session.commit()

    updated = await reports_mod.resolve_update_saved_report(
        org_id=seeded_reports["org_id"],
        report_id=seeded_reports["report1_id"],
        input=reports_mod.UpdateSavedReportInput(
            schedule_cron="0 6 * * *",
            schedule_timezone="UTC",
        ),
    )
    assert updated is not None

    async with session_maker() as session:
        job = await session.scalar(select(ScheduledJob))
        assert job is not None
        assert job.next_run_at == datetime(2026, 7, 25, 6, 0)
        original_job_id = job.id
        report = await session.get(SavedReport, uuid.UUID(seeded_reports["report1_id"]))
        assert report is not None
        report.last_run_at = datetime(2026, 7, 25, 6, 30, tzinfo=UTC)
        await session.commit()

    updated_again = await reports_mod.resolve_update_saved_report(
        org_id=seeded_reports["org_id"],
        report_id=seeded_reports["report1_id"],
        input=reports_mod.UpdateSavedReportInput(
            schedule_cron="30 7 * * *",
            schedule_timezone="UTC",
        ),
    )
    assert updated_again is not None

    async with session_maker() as session:
        jobs = (await session.scalars(select(ScheduledJob))).all()
        assert len(jobs) == 1
        assert jobs[0].id == original_job_id
        assert jobs[0].next_run_at == datetime(2026, 7, 25, 7, 30)


@pytest.mark.asyncio
async def test_update_saved_report_rejects_unevaluable_cron(
    monkeypatch, session_maker, seeded_reports
):
    from dev_health_ops.api.graphql.resolvers import reports as reports_mod

    monkeypatch.setattr(
        "dev_health_ops.db.get_postgres_session",
        _make_mock_session(session_maker),
    )

    created = await reports_mod.resolve_create_saved_report(
        org_id=seeded_reports["org_id"],
        input=reports_mod.CreateSavedReportInput(
            name="Valid Schedule",
            report_plan=cast(Any, {"report_type": "custom"}),
            schedule_cron="0 6 * * *",
        ),
    )

    with pytest.raises(ValueError, match=r"Invalid report schedule cron expression"):
        await reports_mod.resolve_update_saved_report(
            org_id=seeded_reports["org_id"],
            report_id=created.id,
            input=reports_mod.UpdateSavedReportInput(
                name="Invalid Update",
                schedule_cron="99 * * * *",
            ),
        )

    async with session_maker() as session:
        report = await session.scalar(
            select(SavedReport).where(SavedReport.id == uuid.UUID(created.id))
        )
        assert report is not None
        assert report.name == "Valid Schedule"
        job = await session.scalar(
            select(ScheduledJob).where(ScheduledJob.id == report.schedule_id)
        )
        assert job is not None
        assert job.schedule_cron == "0 6 * * *"
