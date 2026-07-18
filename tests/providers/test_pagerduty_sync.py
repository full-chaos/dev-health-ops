from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.models.git import Base
from dev_health_ops.models.operational import OperationalIncident
from dev_health_ops.providers.pagerduty.models import Incident
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer
from dev_health_ops.providers.pagerduty.sync import (
    PagerDutyEnrichmentToggles,
    PagerDutyOperationalSync,
    PagerDutySyncOptions,
)
from dev_health_ops.sync.watermarks import get_watermark, set_watermark

SOURCE_TIME = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    engine.dispose()


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


@pytest.mark.asyncio
async def test_incident_sync_resumes_from_persisted_watermark_without_boundary_loss(
    db_session,
) -> None:
    # Given: a completed fixed window ending at an incident timestamp boundary.
    boundary = datetime(2026, 7, 17, 13, 0, tzinfo=timezone.utc)
    next_source_time = datetime(2026, 7, 17, 14, 0, tzinfo=timezone.utc)
    first_window = (
        Incident(id="incident-1", created_at=SOURCE_TIME, updated_at=SOURCE_TIME),
        Incident(id="incident-2", created_at=boundary, updated_at=boundary),
    )
    resumed_window = (
        first_window[1],
        Incident(
            id="incident-3",
            created_at=next_source_time,
            updated_at=next_source_time,
        ),
    )
    persisted: list[OperationalIncident] = []

    async def persist(values: list[OperationalIncident]) -> None:
        persisted.extend(values)

    async def first_pages(*, params: dict[str, str] | None = None):
        del params
        yield list(first_window)

    first_client = Mock()
    first_client.iter_incident_pages = first_pages
    first_client.drain_usage_observations.return_value = []
    first_store = Mock()
    first_store.insert_operational_incidents = AsyncMock(side_effect=persist)
    first_sync = PagerDutyOperationalSync(
        client=first_client,
        store=first_store,
        normalizer=PagerDutyNormalizer("org-1", "acme", next_source_time),
    )

    # When: the first window succeeds and advances the architecture's watermark.
    first_result = await first_sync.run(
        PagerDutySyncOptions(
            dataset_key="incidents",
            window_start=datetime(2026, 7, 17, 11, 0, tzinfo=timezone.utc),
            window_end=next_source_time,
        )
    )
    watermark_at = first_result.watermark_at
    assert watermark_at is not None
    assert watermark_at == boundary
    set_watermark(
        db_session,
        "org-1",
        "acme",
        "incidents",
        watermark_at,
    )
    resumed_from = get_watermark(db_session, "org-1", "acme", "incidents")
    assert resumed_from == boundary.replace(tzinfo=None)
    assert resumed_from is not None
    resumed_from = resumed_from.replace(tzinfo=timezone.utc)

    async def resumed_pages(*, params: dict[str, str] | None = None):
        assert params == {
            "since": boundary.isoformat(),
            "until": next_source_time.isoformat(),
        }
        yield list(resumed_window)

    resumed_client = Mock()
    resumed_client.iter_incident_pages = resumed_pages
    resumed_client.drain_usage_observations.return_value = []
    resumed_store = Mock()
    resumed_store.insert_operational_incidents = AsyncMock(side_effect=persist)
    resumed_sync = PagerDutyOperationalSync(
        client=resumed_client,
        store=resumed_store,
        normalizer=PagerDutyNormalizer("org-1", "acme", next_source_time),
    )

    await resumed_sync.run(
        PagerDutySyncOptions(
            dataset_key="incidents",
            window_start=resumed_from,
            window_end=next_source_time,
            resume_after=resumed_from,
        )
    )

    # Then: the boundary incident is retained once and never re-emitted on resume.
    assert [incident.external_id for incident in persisted] == [
        "incident-1",
        "incident-2",
        "incident-3",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dataset_key", "toggles", "fetch_name"),
    [
        ("incident-alerts", PagerDutyEnrichmentToggles(alerts=False), "alerts"),
        (
            "incident-log-entries",
            PagerDutyEnrichmentToggles(log_entries=False),
            "log_entries",
        ),
        ("incident-notes", PagerDutyEnrichmentToggles(notes=False), "notes"),
    ],
)
async def test_disabled_enrichment_dataset_makes_no_provider_calls(
    dataset_key: str,
    toggles: PagerDutyEnrichmentToggles,
    fetch_name: str,
) -> None:
    incidents = [
        Incident(id="incident-1", created_at=SOURCE_TIME, updated_at=SOURCE_TIME)
    ]

    async def pages(*, params: dict[str, str] | None = None):
        del params
        yield incidents

    client = Mock()
    client.iter_incident_pages = pages
    fetch = AsyncMock(return_value=[])
    setattr(client, f"list_incident_{fetch_name}", fetch)
    client.drain_usage_observations.return_value = []
    store = Mock()
    sync = PagerDutyOperationalSync(
        client=client,
        store=store,
        normalizer=PagerDutyNormalizer("org-1", "acme", SOURCE_TIME),
    )

    result = await sync.run(
        PagerDutySyncOptions(
            dataset_key=dataset_key,
            window_start=None,
            window_end=None,
            enrichment=toggles,
        )
    )

    assert result.persisted == 0
    assert fetch.await_count == 0


@pytest.mark.asyncio
async def test_enrichment_cap_bounds_provider_calls_across_incidents() -> None:
    incidents = [
        Incident(
            id=f"incident-{number}",
            created_at=SOURCE_TIME,
            updated_at=SOURCE_TIME,
        )
        for number in range(3)
    ]

    async def pages(*, params: dict[str, str] | None = None):
        del params
        yield incidents

    client = Mock()
    client.iter_incident_pages = pages
    client.list_incident_alerts = AsyncMock(return_value=[])
    client.drain_usage_observations.return_value = []
    store = Mock()
    store.insert_operational_alerts = AsyncMock()
    sync = PagerDutyOperationalSync(
        client=client,
        store=store,
        normalizer=PagerDutyNormalizer("org-1", "acme", SOURCE_TIME),
    )

    result = await sync.run(
        PagerDutySyncOptions(
            dataset_key="incident-alerts",
            window_start=None,
            window_end=None,
            enrichment_cap=2,
        )
    )

    assert result.persisted == 0
    assert client.list_incident_alerts.await_count == 2
