from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
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
    db_path = tmp_path / "saved-reports-api.db"
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
            report_plan={"report_type": "custom"},
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
            parameter_overrides={"team": "frontend"},
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
