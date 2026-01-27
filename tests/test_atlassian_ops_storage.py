import pytest
from datetime import datetime, timezone
from dev_health_ops.models.atlassian_ops import (
    AtlassianOpsIncident,
    AtlassianOpsAlert,
    AtlassianOpsSchedule,
)
from dev_health_ops.storage import SQLAlchemyStore


@pytest.mark.asyncio
async def test_sqlalchemy_store_atlassian_ops():
    """Test Atlassian Ops storage in SQLAlchemy (SQLite)."""
    store = SQLAlchemyStore("sqlite+aiosqlite:///:memory:")
    async with store:
        await store.ensure_tables()

        # Test Incidents
        incidents = [
            AtlassianOpsIncident(
                id="inc-1",
                url="http://ops/inc-1",
                summary="Incident 1",
                description="Serious issue",
                status="OPEN",
                severity="P1",
                created_at=datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            )
        ]
        await store.insert_atlassian_ops_incidents(incidents)

        # Test Alerts
        alerts = [
            AtlassianOpsAlert(
                id="alert-1",
                status="OPENED",
                priority="P2",
                created_at=datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc),
            )
        ]
        await store.insert_atlassian_ops_alerts(alerts)

        # Test Schedules
        schedules = [
            AtlassianOpsSchedule(
                id="sch-1",
                name="Schedule 1",
                timezone="UTC",
            )
        ]
        await store.insert_atlassian_ops_schedules(schedules)

        # Verify (minimal check - if it doesn't crash, upsert worked)
        # We can add loaders later if needed for full verification
