from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from dev_health_ops.models.operational import OperationalIncident
from dev_health_ops.storage.clickhouse import ClickHouseStore


def test_operational_insert_normalizes_aware_and_naive_datetimes(
    monkeypatch,
) -> None:
    aware_pacific = datetime(2026, 7, 17, 5, tzinfo=timezone(timedelta(hours=-7)))
    naive_utc = datetime(2026, 7, 17, 12)
    incident = OperationalIncident(
        org_id="test-org",
        provider="pagerduty",
        provider_instance_id="pagerduty-test",
        source_entity_type="incident",
        external_id="incident-1",
        source_version_at=aware_pacific,
        source_event_at=naive_utc,
        started_at=naive_utc,
    )
    store = ClickHouseStore("clickhouse://unused")
    captured_rows: list[dict[str, object]] = []

    async def capture_rows(
        _table: str, _columns: list[str], rows: list[dict[str, object]]
    ) -> None:
        captured_rows.extend(rows)

    monkeypatch.setattr(store, "_insert_rows", capture_rows)

    asyncio.run(store._insert_operational_rows("operational_incidents", [incident]))

    source_version_at = captured_rows[0]["source_version_at"]
    source_event_at = captured_rows[0]["source_event_at"]
    started_at = captured_rows[0]["started_at"]
    expected = datetime(2026, 7, 17, 12, tzinfo=timezone.utc)

    assert source_version_at == expected
    assert source_event_at == expected
    assert started_at == expected
