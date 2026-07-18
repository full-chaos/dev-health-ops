from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest

from dev_health_ops.models.operational import OperationalIncident
from dev_health_ops.providers.pagerduty.models import Incident
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer
from dev_health_ops.providers.pagerduty.sync import (
    PagerDutyOperationalSync,
    PagerDutySyncOptions,
)

SOURCE_TIME = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_incident_sync_flushes_bounded_batches_without_boundary_loss() -> None:
    incidents = tuple(
        Incident(
            id=f"incident-{number}",
            title=f"Incident {number}",
            created_at=SOURCE_TIME,
            updated_at=SOURCE_TIME,
        )
        for number in range(5)
    )
    client = Mock()
    observed_params: dict[str, str] | None = None

    async def pages(*, params: dict[str, str] | None = None):
        nonlocal observed_params
        observed_params = params
        yield list(incidents[:2])
        yield list(incidents[2:])

    batches: list[list[OperationalIncident]] = []

    async def persist(values: list[OperationalIncident]) -> None:
        batches.append(values)

    client.iter_incident_pages = pages
    client.drain_usage_observations.return_value = []
    store = Mock()
    store.insert_operational_incidents = AsyncMock(side_effect=persist)
    sync = PagerDutyOperationalSync(
        client=client,
        store=store,
        normalizer=PagerDutyNormalizer("org-1", "acme", SOURCE_TIME),
    )

    result = await sync.run(
        PagerDutySyncOptions(
            dataset_key="incidents",
            window_start=SOURCE_TIME,
            window_end=SOURCE_TIME,
            incident_cap=5,
            batch_size=2,
        )
    )

    assert result.persisted == 5
    assert [len(batch) for batch in batches] == [2, 2, 1]
    assert [row.external_id for batch in batches for row in batch] == [
        "incident-0",
        "incident-1",
        "incident-2",
        "incident-3",
        "incident-4",
    ]
    assert observed_params == {
        "since": SOURCE_TIME.isoformat(),
        "until": SOURCE_TIME.isoformat(),
    }
