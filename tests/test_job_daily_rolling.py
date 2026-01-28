import math
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from dev_health_ops.metrics.job_daily import run_daily_metrics_job
from dev_health_ops.metrics.loaders.sqlalchemy import SqlAlchemyDataLoader
from dev_health_ops.metrics.schemas import UserMetricsDailyRecord
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink
from dev_health_ops.models.atlassian_ops import (AtlassianOpsAlert,
                                                 AtlassianOpsIncident,
                                                 AtlassianOpsSchedule)
from dev_health_ops.storage import SQLAlchemyStore


@pytest.mark.asyncio
async def test_job_daily_rolling_ic_landscape(tmp_path):
    db_path = tmp_path / "test_rolling.db"
    db_url = f"sqlite:///{db_path}"

    from dev_health_ops.models.git import Base

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    sink = SQLiteMetricsSink(db_url)
    sink.ensure_tables()

    repo_id = uuid.uuid4()
    today = date(2026, 1, 27)
    computed_at = datetime.now(timezone.utc)

    history_records = []
    for i in range(10):
        day = today - timedelta(days=i)
        history_records.append(
            UserMetricsDailyRecord(
                repo_id=repo_id,
                day=day,
                author_email="dev@example.com",
                commits_count=2,
                loc_added=100,
                loc_deleted=20,
                files_changed=5,
                large_commits_count=0,
                avg_commit_size_loc=60.0,
                prs_authored=1,
                prs_merged=1,
                avg_pr_cycle_hours=2.0,
                median_pr_cycle_hours=2.0,
                computed_at=computed_at,
                identity_id="dev@example.com",
                loc_touched=120,
                delivery_units=2,
                cycle_p50_hours=2.0,
                team_id="team-a",
            )
        )

    sink.write_user_metrics(history_records)
    sink.close()

    await run_daily_metrics_job(
        db_url=db_url, day=today, backfill_days=1, provider="none", sink="sqlite"
    )

    sink = SQLiteMetricsSink(db_url)
    with sink.engine.connect() as conn:
        rows = (
            conn.execute(text("SELECT * FROM ic_landscape_rolling_30d"))
            .mappings()
            .all()
        )
        assert len(rows) > 0

        user_rows = [r for r in rows if r["identity_id"] == "dev@example.com"]
        assert len(user_rows) == 3

        maps = {r["map_name"] for r in user_rows}
        assert "churn_throughput" in maps

        churn_row = next(r for r in user_rows if r["map_name"] == "churn_throughput")
        assert churn_row["churn_loc_30d"] == 1200
        assert churn_row["delivery_units_30d"] == 20
        assert math.isclose(churn_row["x_raw"], math.log1p(1200))
        assert churn_row["y_raw"] == 20.0

    sink.close()


@pytest.mark.asyncio
async def test_sqlalchemy_loader_atlassian_ops(tmp_path):
    db_path = tmp_path / "test_loaders.db"
    db_url = f"sqlite:///{db_path}"

    store = SQLAlchemyStore(f"sqlite+aiosqlite:///{db_path}")
    async with store:
        await store.ensure_tables()

        start = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc)

        await store.insert_atlassian_ops_incidents(
            [
                AtlassianOpsIncident(
                    id="inc-1",
                    url=None,
                    summary="Incident 1",
                    description=None,
                    status="OPEN",
                    severity="P1",
                    created_at=start + timedelta(hours=1),
                )
            ]
        )
        await store.insert_atlassian_ops_alerts(
            [
                AtlassianOpsAlert(
                    id="alert-1",
                    status="OPENED",
                    priority="P2",
                    created_at=start + timedelta(hours=2),
                )
            ]
        )
        await store.insert_atlassian_ops_schedules(
            [
                AtlassianOpsSchedule(
                    id="sch-1",
                    name="Schedule 1",
                    timezone="UTC",
                )
            ]
        )

    engine = create_engine(db_url)
    loader = SqlAlchemyDataLoader(engine)

    incidents = await loader.load_atlassian_ops_incidents(start, end)
    assert len(incidents) == 1
    assert incidents[0].id == "inc-1"

    alerts = await loader.load_atlassian_ops_alerts(start, end)
    assert len(alerts) == 1
    assert alerts[0].id == "alert-1"

    schedules = await loader.load_atlassian_ops_schedules()
    assert len(schedules) == 1
    assert schedules[0].id == "sch-1"
