from __future__ import annotations

from datetime import datetime, timezone
from typing import TypeVar
from unittest.mock import AsyncMock, Mock

import pytest

from dev_health_ops.api.services.integrations import SyncRunService
from dev_health_ops.exceptions import (
    APIException,
    AuthenticationException,
    PaginationException,
)
from dev_health_ops.models.integrations import SyncRunUnit
from dev_health_ops.models.operational import (
    CanonicalOperationalEntity,
    EscalationPolicy,
    IncidentNote,
    IncidentTimelineEvent,
    OnCallAssignment,
    OnCallSchedule,
    OperationalAlert,
    OperationalIncident,
    OperationalService,
    OperationalTeam,
    OperationalUser,
)
from dev_health_ops.providers.pagerduty.degradation import (
    PagerDutyInsufficientScopeError,
)
from dev_health_ops.providers.pagerduty.models import Incident, Service
from dev_health_ops.providers.pagerduty.normalize import PagerDutyNormalizer
from dev_health_ops.providers.pagerduty.sync import (
    PagerDutyDatasetDegradedError,
    PagerDutyOperationalStore,
    PagerDutyOperationalSync,
    PagerDutySyncOptions,
)

SOURCE_TIME = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
T = TypeVar("T", bound=CanonicalOperationalEntity)


class _Store(PagerDutyOperationalStore):
    def __init__(self) -> None:
        self.services: list[OperationalService] = []
        self.incidents: list[OperationalIncident] = []

    async def load_active_operational_entities(
        self,
        entity_type: type[T],
        *,
        org_id: str,
        provider: str,
        provider_instance_id: str,
        source_entity_type: str,
    ) -> list[T]:
        del (
            entity_type,
            org_id,
            provider,
            provider_instance_id,
            source_entity_type,
        )
        return []

    async def insert_operational_services(
        self, values: list[OperationalService]
    ) -> None:
        self.services.extend(values)

    async def insert_operational_incidents(
        self, values: list[OperationalIncident]
    ) -> None:
        self.incidents.extend(values)

    async def insert_operational_alerts(self, values: list[OperationalAlert]) -> None:
        return None

    async def insert_operational_incident_timeline_events(
        self, values: list[IncidentTimelineEvent]
    ) -> None:
        return None

    async def insert_operational_incident_notes(
        self, values: list[IncidentNote]
    ) -> None:
        return None

    async def insert_operational_escalation_policies(
        self, values: list[EscalationPolicy]
    ) -> None:
        return None

    async def insert_operational_on_call_schedules(
        self, values: list[OnCallSchedule]
    ) -> None:
        return None

    async def insert_operational_on_call_assignments(
        self, values: list[OnCallAssignment]
    ) -> None:
        return None

    async def insert_operational_teams(self, values: list[OperationalTeam]) -> None:
        return None

    async def insert_operational_users(self, values: list[OperationalUser]) -> None:
        return None


def _normalizer() -> PagerDutyNormalizer:
    return PagerDutyNormalizer("org-1", "acme", SOURCE_TIME)


def _service_client() -> Mock:
    client = Mock()
    client.list_services = AsyncMock(
        return_value=[Service(id="service-1", name="Payments", updated_at=SOURCE_TIME)]
    )
    client.drain_usage_observations.return_value = []
    return client


def _partial_failure_summary(dataset_key: str, category: str) -> dict[str, object]:
    success = Mock(spec=SyncRunUnit)
    success.id = "unit-services"
    success.status = "success"
    success.source_id = "source-1"
    success.dataset_key = "services"
    success.cost_class = "medium"
    success.duration_seconds = None
    success.result = None
    failed = Mock(spec=SyncRunUnit)
    failed.id = "unit-failed"
    failed.status = "failed"
    failed.source_id = "source-1"
    failed.dataset_key = dataset_key
    failed.cost_class = "medium"
    failed.duration_seconds = None
    failed.result = {"error_category": category}
    return SyncRunService.build_unit_rollups([success, failed])[
        "partial_failure_summary"
    ]


@pytest.mark.asyncio
async def test_insufficient_scope_degrades_only_incident_dataset_and_reports_partial_failure() -> (
    None
):
    store = _Store()
    service_client = _service_client()
    await PagerDutyOperationalSync(
        client=service_client,
        store=store,
        normalizer=_normalizer(),
    ).run(PagerDutySyncOptions("services", None, None))

    denied_client = Mock()

    async def denied_pages(*, params: dict[str, str] | None = None):
        del params
        raise PagerDutyInsufficientScopeError("pagerduty forbidden: insufficient scope")
        yield []

    denied_client.iter_incident_pages = denied_pages
    denied_client.drain_usage_observations.return_value = []

    with pytest.raises(PagerDutyDatasetDegradedError, match="incidents"):
        await PagerDutyOperationalSync(
            client=denied_client,
            store=store,
            normalizer=_normalizer(),
        ).run(PagerDutySyncOptions("incidents", None, None))

    summary = _partial_failure_summary("incidents", "provider_error")
    assert [service.external_id for service in store.services] == ["service-1"]
    assert store.incidents == []
    assert summary == {
        "failed_sources": ["source-1"],
        "failed_datasets": ["incidents"],
        "error_categories": {"provider_error": 1},
    }


@pytest.mark.asyncio
async def test_partial_incident_page_flushes_prior_rows_and_reports_partial_failure() -> (
    None
):
    store = _Store()
    service_client = _service_client()
    await PagerDutyOperationalSync(
        client=service_client,
        store=store,
        normalizer=_normalizer(),
    ).run(PagerDutySyncOptions("services", None, None))

    partial_client = Mock()

    async def partial_pages(*, params: dict[str, str] | None = None):
        del params
        yield [
            Incident(id="incident-1", created_at=SOURCE_TIME, updated_at=SOURCE_TIME)
        ]
        raise APIException("pagerduty server error 503 while fetching the next page")

    partial_client.iter_incident_pages = partial_pages
    partial_client.drain_usage_observations.return_value = []

    with pytest.raises(PagerDutyDatasetDegradedError, match="incidents"):
        await PagerDutyOperationalSync(
            client=partial_client,
            store=store,
            normalizer=_normalizer(),
        ).run(PagerDutySyncOptions("incidents", None, None, batch_size=100))

    summary = _partial_failure_summary("incidents", "provider_error")
    assert [service.external_id for service in store.services] == ["service-1"]
    assert [incident.external_id for incident in store.incidents] == ["incident-1"]
    assert summary == {
        "failed_sources": ["source-1"],
        "failed_datasets": ["incidents"],
        "error_categories": {"provider_error": 1},
    }


@pytest.mark.asyncio
async def test_authentication_failure_is_not_silently_degraded() -> None:
    client = Mock()

    async def unauthorized_pages(*, params: dict[str, str] | None = None):
        del params
        raise AuthenticationException("pagerduty authentication failed")
        yield []

    client.iter_incident_pages = unauthorized_pages
    client.drain_usage_observations.return_value = []

    with pytest.raises(AuthenticationException):
        await PagerDutyOperationalSync(
            client=client,
            store=_Store(),
            normalizer=_normalizer(),
        ).run(PagerDutySyncOptions("incidents", None, None))


@pytest.mark.asyncio
async def test_no_progress_pagination_degrades_after_partial_incident_page() -> None:
    store = _Store()
    client = Mock()

    async def partial_pages(*, params: dict[str, str] | None = None):
        del params
        yield [
            Incident(id="incident-1", created_at=SOURCE_TIME, updated_at=SOURCE_TIME)
        ]
        raise PaginationException(
            "PagerDuty pagination made no progress for /incidents"
        )

    client.iter_incident_pages = partial_pages
    client.drain_usage_observations.return_value = []

    with pytest.raises(PagerDutyDatasetDegradedError, match="incidents"):
        await PagerDutyOperationalSync(
            client=client,
            store=store,
            normalizer=_normalizer(),
        ).run(PagerDutySyncOptions("incidents", None, None, batch_size=100))

    assert [incident.external_id for incident in store.incidents] == ["incident-1"]
