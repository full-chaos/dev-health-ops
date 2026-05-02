from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.models.git import Base
from dev_health_ops.models.reports import ReportRun, ReportRunStatus, SavedReport
from dev_health_ops.models.settings import ScheduledJob
from tests._helpers import tables_of


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path):
    db_path = tmp_path / "reports-test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=tables_of(SavedReport, ReportRun, ScheduledJob),
            )
        )

    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_saved_report_creation(session_maker):
    async with session_maker() as session:
        report = SavedReport(
            name="Weekly Health",
            org_id="org-1",
            description="Weekly team health report",
            report_plan={"report_type": "weekly_health", "plan_id": "p1"},
            is_template=False,
            parameters={"team": "backend"},
        )
        session.add(report)
        await session.commit()

        assert report.id is not None
        assert report.name == "Weekly Health"
        assert report.org_id == "org-1"
        assert report.report_plan["report_type"] == "weekly_health"
        assert report.is_active is True
        assert report.is_template is False


@pytest.mark.asyncio
async def test_saved_report_clone(session_maker):
    async with session_maker() as session:
        original = SavedReport(
            name="Monthly Review",
            org_id="org-1",
            report_plan={"report_type": "monthly_review", "plan_id": "p2"},
            parameters={"team": "platform", "date_range": "last_month"},
        )
        session.add(original)
        await session.commit()

        cloned = original.clone(
            new_name="Monthly Review (Q1)",
            parameter_overrides={"team": "frontend"},
        )
        session.add(cloned)
        await session.commit()

        assert cloned.id != original.id
        assert cloned.name == "Monthly Review (Q1)"
        assert cloned.template_source_id == original.id
        assert cloned.is_template is False
        assert cloned.parameters["team"] == "frontend"
        assert cloned.parameters["date_range"] == "last_month"
        assert cloned.report_plan["report_type"] == "monthly_review"


@pytest.mark.asyncio
async def test_saved_report_clone_default_name(session_maker):
    async with session_maker() as session:
        original = SavedReport(name="Sprint Report", org_id="org-1")
        session.add(original)
        await session.commit()

        cloned = original.clone()
        assert cloned.name == "Sprint Report (Copy)"


@pytest.mark.asyncio
async def test_report_run_creation(session_maker):
    async with session_maker() as session:
        report = SavedReport(name="Test Report", org_id="org-1")
        session.add(report)
        await session.commit()

        run = ReportRun(
            report_id=report.id,
            triggered_by="manual",
        )
        session.add(run)
        await session.commit()

        assert run.id is not None
        assert run.report_id == report.id
        assert run.status == ReportRunStatus.PENDING.value
        assert run.triggered_by == "manual"
        assert run.rendered_markdown is None
        assert run.error is None


@pytest.mark.asyncio
async def test_report_run_status_enum():
    assert ReportRunStatus.PENDING.value == "pending"
    assert ReportRunStatus.RUNNING.value == "running"
    assert ReportRunStatus.SUCCESS.value == "success"
    assert ReportRunStatus.FAILED.value == "failed"


@pytest.mark.asyncio
async def test_saved_report_template_flag(session_maker):
    async with session_maker() as session:
        template = SavedReport(
            name="Template: Quality Trend",
            org_id="org-1",
            report_plan={"report_type": "quality_trend"},
            is_template=True,
        )
        session.add(template)
        await session.commit()

        assert template.is_template is True

        cloned = template.clone(new_name="Q1 Quality Trend")
        assert cloned.is_template is False
        assert cloned.template_source_id == template.id
