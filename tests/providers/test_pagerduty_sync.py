from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dev_health_ops.exceptions import APIException, PaginationException
from dev_health_ops.models.git import Base
from dev_health_ops.models.operational import OperationalIncident, OperationalService
from dev_health_ops.providers.pagerduty.models import Alert, Incident, Service
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer
from dev_health_ops.providers.pagerduty.sync import (
    PagerDutyDatasetDegradedError,
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
async def test_incident_sync_drains_window_beyond_legacy_cap_in_bounded_batches() -> (
    None
):
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

    # Then: the inclusive boundary is replayed and canonical FINAL state keeps one row.
    assert [incident.external_id for incident in persisted] == [
        "incident-1",
        "incident-2",
        "incident-2",
        "incident-3",
    ]
    final_incidents = {incident.external_id: incident for incident in persisted}
    assert list(final_incidents) == ["incident-1", "incident-2", "incident-3"]


@pytest.mark.asyncio
async def test_incident_cursor_uses_created_at_for_watermark_and_updated_at_for_version() -> (
    None
):
    # Given: a created-at window whose API response includes an older incident only
    # because this test double does not apply PagerDuty's server-side filtering.
    watermark = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    in_window_created_at = datetime(2026, 7, 17, 12, 1, tzinfo=timezone.utc)
    in_window_updated_at = datetime(2026, 7, 17, 12, 2, tzinfo=timezone.utc)
    incidents = [
        Incident(
            id="created-before-watermark",
            created_at=datetime(2026, 7, 17, 11, 59, tzinfo=timezone.utc),
            updated_at=datetime(2026, 7, 17, 12, 3, tzinfo=timezone.utc),
        ),
        Incident(
            id="created-within-window",
            created_at=in_window_created_at,
            updated_at=in_window_updated_at,
        ),
    ]

    async def pages(*, params: dict[str, str] | None = None):
        assert params == {
            "since": watermark.isoformat(),
            "until": datetime(2026, 7, 17, 13, 0, tzinfo=timezone.utc).isoformat(),
        }
        yield incidents

    client = Mock()
    client.iter_incident_pages = pages
    client.drain_usage_observations.return_value = []
    store = Mock()
    store.insert_operational_incidents = AsyncMock()
    sync = PagerDutyOperationalSync(
        client=client,
        store=store,
        normalizer=PagerDutyNormalizer("org-1", "acme", SOURCE_TIME),
    )

    # When: the incremental incident window resumes at its created-at watermark.
    result = await sync.run(
        PagerDutySyncOptions(
            dataset_key="incidents",
            window_start=watermark,
            window_end=datetime(2026, 7, 17, 13, 0, tzinfo=timezone.utc),
            resume_after=watermark,
        )
    )

    # Then: older-created incidents are outside the contract while an in-window
    # update keeps its later updated_at as the canonical row version.
    inserted = store.insert_operational_incidents.await_args.args[0]
    assert result.persisted == 1
    assert result.watermark_at == in_window_created_at
    assert [row.external_id for row in inserted] == ["created-within-window"]
    assert inserted[0].source_version_at == in_window_updated_at


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
async def test_enrichment_cap_stops_child_stream_without_advancing_past_undrained_incident() -> (
    None
):
    incidents = [
        Incident(
            id=f"incident-{number}",
            created_at=SOURCE_TIME,
            updated_at=datetime(2026, 7, 17, 12, number, tzinfo=timezone.utc),
        )
        for number in range(2)
    ]

    async def pages(*, params: dict[str, str] | None = None):
        del params
        yield incidents

    client = Mock()
    client.iter_incident_pages = pages

    child_page_requests: list[str] = []

    async def alert_pages(incident_id: str):
        child_page_requests.append(incident_id)
        yield [
            Alert(id=f"{incident_id}-alert-{number}", created_at=SOURCE_TIME)
            for number in range(2)
        ]
        child_page_requests.append(incident_id)
        yield [Alert(id=f"{incident_id}-alert-2", created_at=SOURCE_TIME)]

    client.iter_incident_alert_pages = alert_pages
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
            window_start=SOURCE_TIME,
            window_end=datetime(2026, 7, 17, 13, 0, tzinfo=timezone.utc),
            enrichment_cap=2,
        )
    )

    assert result.persisted == 4
    assert child_page_requests == ["incident-0", "incident-1"]
    assert result.watermark_at == SOURCE_TIME


@pytest.mark.asyncio
async def test_enrichment_cap_clamps_watermark_to_earliest_undrained_incident_out_of_order() -> (
    None
):
    older_source_time = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    newer_source_time = datetime(2026, 7, 17, 12, 1, tzinfo=timezone.utc)
    incidents = [
        Incident(
            id="newer-drained",
            created_at=newer_source_time,
            updated_at=newer_source_time,
        ),
        Incident(
            id="older-capped",
            created_at=older_source_time,
            updated_at=older_source_time,
        ),
    ]

    async def pages(*, params: dict[str, str] | None = None):
        del params
        yield incidents

    async def alert_pages(incident_id: str):
        if incident_id == "newer-drained":
            yield []
            return
        yield [
            Alert(id=f"{incident_id}-alert-{number}", created_at=SOURCE_TIME)
            for number in range(2)
        ]

    client = Mock()
    client.iter_incident_pages = pages
    client.iter_incident_alert_pages = alert_pages
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
            window_start=older_source_time,
            window_end=datetime(2026, 7, 17, 13, 0, tzinfo=timezone.utc),
            enrichment_cap=2,
        )
    )

    assert result.watermark_at == older_source_time


@pytest.mark.asyncio
async def test_zero_enrichment_cap_skips_child_calls_and_completes_window() -> None:
    latest_source_time = datetime(2026, 7, 17, 12, 1, tzinfo=timezone.utc)
    incidents = [
        Incident(id="incident-0", created_at=SOURCE_TIME, updated_at=SOURCE_TIME),
        Incident(
            id="incident-1",
            created_at=latest_source_time,
            updated_at=latest_source_time,
        ),
    ]

    async def pages(*, params: dict[str, str] | None = None):
        del params
        yield incidents

    child_page_requests: list[str] = []

    async def alert_pages(incident_id: str):
        child_page_requests.append(incident_id)
        yield []

    client = Mock()
    client.iter_incident_pages = pages
    client.iter_incident_alert_pages = alert_pages
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
            window_start=SOURCE_TIME,
            window_end=latest_source_time,
            enrichment_cap=0,
        )
    )

    assert result.persisted == 0
    assert child_page_requests == []
    assert result.watermark_at == latest_source_time


@pytest.mark.asyncio
async def test_complete_service_snapshot_tombstones_missing_reference() -> None:
    normalizer = PagerDutyNormalizer("org-1", "acme", SOURCE_TIME)
    missing = normalizer.service(
        Service(id="service-missing", name="Missing", updated_at=SOURCE_TIME)
    )
    client = Mock()
    client.list_services = AsyncMock(
        return_value=[
            Service(id="service-current", name="Current", updated_at=SOURCE_TIME)
        ]
    )
    client.drain_usage_observations.return_value = []
    store = Mock()
    store.load_active_operational_entities = AsyncMock(return_value=[missing])
    store.insert_operational_services = AsyncMock()

    await PagerDutyOperationalSync(
        client=client,
        store=store,
        normalizer=normalizer,
    ).run(PagerDutySyncOptions("services", None, None))

    inserted = [
        row
        for call in store.insert_operational_services.await_args_list
        for row in call.args[0]
    ]
    tombstone = next(row for row in inserted if row.external_id == "service-missing")
    assert isinstance(tombstone, OperationalService)
    assert tombstone.is_deleted is True
    assert tombstone.deleted_at == tombstone.source_version_at
    assert tombstone.source_version_at > missing.source_version_at


@pytest.mark.asyncio
async def test_failed_service_snapshot_emits_no_reference_tombstones() -> None:
    client = Mock()
    client.list_services = AsyncMock(side_effect=APIException("PagerDuty unavailable"))
    client.drain_usage_observations.return_value = []
    store = Mock()
    store.load_active_operational_entities = AsyncMock()
    store.insert_operational_services = AsyncMock()

    with pytest.raises(PagerDutyDatasetDegradedError):
        await PagerDutyOperationalSync(
            client=client,
            store=store,
            normalizer=PagerDutyNormalizer("org-1", "acme", SOURCE_TIME),
        ).run(PagerDutySyncOptions("services", None, None))

    store.load_active_operational_entities.assert_not_awaited()
    store.insert_operational_services.assert_not_awaited()


@pytest.mark.asyncio
async def test_malformed_service_snapshot_emits_no_reference_tombstones() -> None:
    client = Mock()
    client.list_services = AsyncMock(
        side_effect=PaginationException("PagerDuty pagination envelope is malformed")
    )
    client.drain_usage_observations.return_value = []
    store = Mock()
    store.load_active_operational_entities = AsyncMock()
    store.insert_operational_services = AsyncMock()

    with pytest.raises(PagerDutyDatasetDegradedError):
        await PagerDutyOperationalSync(
            client=client,
            store=store,
            normalizer=PagerDutyNormalizer("org-1", "acme", SOURCE_TIME),
        ).run(PagerDutySyncOptions("services", None, None))

    store.load_active_operational_entities.assert_not_awaited()
    store.insert_operational_services.assert_not_awaited()
