from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from dev_health_ops.models.operational import OperationalIncident
from dev_health_ops.storage.clickhouse import ClickHouseStore


def test_operational_insert_preserves_canonical_utc_datetimes(
    monkeypatch,
) -> None:
    canonical_utc = datetime(2026, 7, 17, 12, tzinfo=timezone.utc)
    incident = OperationalIncident(
        org_id="test-org",
        provider="pagerduty",
        provider_instance_id="pagerduty-test",
        source_entity_type="incident",
        external_id="incident-1",
        source_version_at=canonical_utc,
        source_event_at=canonical_utc,
        started_at=canonical_utc,
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
    assert source_version_at == canonical_utc
    assert source_event_at == canonical_utc
    assert started_at == canonical_utc
