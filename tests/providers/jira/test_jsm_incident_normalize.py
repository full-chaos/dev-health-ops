from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import anyio
import pytest

from dev_health_ops.providers.jira.jsm_incidents import JsmIncidentProducer
from dev_health_ops.providers.jira.jsm_models import JsmPayloadError


def _issue(
    *,
    issue_id: str = "10001",
    key: str = "OPS-7",
    status: str = "In progress",
    status_category: str = "indeterminate",
) -> dict[str, object]:
    return {
        "id": issue_id,
        "key": key,
        "fields": {
            "summary": "Database outage",
            "created": "2026-07-20T09:00:00.000+0000",
            "updated": "2026-07-20T10:00:00.000+0000",
            "resolutiondate": "2026-07-20T11:00:00.000+0000",
            "status": {"name": status, "statusCategory": {"key": status_category}},
            "priority": {"name": "Highest"},
        },
    }


class _Client:
    async def iter_service_desks(self) -> AsyncIterator[str]:
        if False:
            yield ""

    async def iter_jsm_incident_issues(
        self,
        *,
        project_keys: tuple[str, ...],
        window_start: datetime,
        window_end: datetime,
    ) -> AsyncIterator[dict[str, object]]:
        if False:
            yield {}

    async def admit_jsm_incident(self, *, issue_id: str) -> bool:
        return True


def test_collect_builds_canonical_incident_without_alerts() -> None:
    class Client(_Client):
        async def iter_service_desks(self) -> AsyncIterator[str]:
            yield "OPS"

        async def iter_jsm_incident_issues(
            self,
            *,
            project_keys: tuple[str, ...],
            window_start: datetime,
            window_end: datetime,
        ) -> AsyncIterator[dict[str, object]]:
            assert project_keys == ("OPS",)
            yield _issue()

        async def admit_jsm_incident(self, *, issue_id: str) -> bool:
            assert issue_id == "10001"
            return True

    producer = JsmIncidentProducer(
        client=Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
        window_start=datetime(2026, 7, 20, tzinfo=UTC),
        window_end=datetime(2026, 7, 21, tzinfo=UTC),
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
        allowed_project_keys=("OPS",),
    )

    batch = anyio.run(producer.collect)

    incident = batch.incidents[0]
    assert batch.alerts == ()
    assert incident.provider == "jira"
    assert incident.provider_instance_id == "cloud-a"
    assert incident.external_id == "10001"
    assert incident.source_entity_type == "jsm_incident"
    assert incident.source_event_id == "OPS-7"
    assert incident.source_url == "https://example.atlassian.net/browse/OPS-7"
    assert incident.normalized_status == "active"
    assert incident.normalized_priority is None
    assert incident.normalized_severity is None
    assert incident.service_id is None
    assert incident.source_event_at == incident.started_at
    assert incident.resolved_at is None


def test_collect_excludes_category_candidate_when_native_incident_returns_404() -> None:
    class Client(_Client):
        async def iter_service_desks(self) -> AsyncIterator[str]:
            yield "OPS"

        async def iter_jsm_incident_issues(
            self,
            *,
            project_keys: tuple[str, ...],
            window_start: datetime,
            window_end: datetime,
        ) -> AsyncIterator[dict[str, object]]:
            yield _issue()

        async def admit_jsm_incident(self, *, issue_id: str) -> bool:
            assert issue_id == "10001"
            return False

    producer = JsmIncidentProducer(
        client=Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
        window_start=datetime(2026, 7, 20, tzinfo=UTC),
        window_end=datetime(2026, 7, 21, tzinfo=UTC),
        allowed_project_keys=("OPS",),
    )

    batch = anyio.run(producer.collect)

    assert batch.incidents == ()
    assert batch.alerts == ()


def test_collect_admits_category_candidate_when_native_incident_returns_200() -> None:
    class Client(_Client):
        async def iter_service_desks(self) -> AsyncIterator[str]:
            yield "OPS"

        async def iter_jsm_incident_issues(
            self,
            *,
            project_keys: tuple[str, ...],
            window_start: datetime,
            window_end: datetime,
        ) -> AsyncIterator[dict[str, object]]:
            yield _issue(issue_id="10001", key="OPS-7")

        async def admit_jsm_incident(self, *, issue_id: str) -> bool:
            assert issue_id == "10001"
            return True

    producer = JsmIncidentProducer(
        client=Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
        window_start=datetime(2026, 7, 20, tzinfo=UTC),
        window_end=datetime(2026, 7, 21, tzinfo=UTC),
        allowed_project_keys=("OPS",),
    )

    batch = anyio.run(producer.collect)

    assert len(batch.incidents) == 1
    assert batch.incidents[0].source_event_id == "OPS-7"


def test_collect_queries_only_configured_service_project_keys() -> None:
    class Client(_Client):
        async def iter_service_desks(self) -> AsyncIterator[str]:
            yield "OPS"
            yield "UNSCOPED"

        async def iter_jsm_incident_issues(
            self,
            *,
            project_keys: tuple[str, ...],
            window_start: datetime,
            window_end: datetime,
        ) -> AsyncIterator[dict[str, object]]:
            assert project_keys == ("OPS",)
            yield _issue()

    producer = JsmIncidentProducer(
        client=Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
        window_start=datetime(2026, 7, 20, tzinfo=UTC),
        window_end=datetime(2026, 7, 21, tzinfo=UTC),
        allowed_project_keys=("OPS",),
    )

    batch = anyio.run(producer.collect)

    assert len(batch.incidents) == 1


def test_collect_fails_closed_before_issue_search_when_source_project_is_not_a_service_desk() -> (
    None
):
    class Client(_Client):
        async def iter_service_desks(self) -> AsyncIterator[str]:
            yield "UNSCOPED"

        async def iter_jsm_incident_issues(
            self,
            *,
            project_keys: tuple[str, ...],
            window_start: datetime,
            window_end: datetime,
        ) -> AsyncIterator[dict[str, object]]:
            pytest.fail(f"unexpected issue query for {project_keys}")
            yield _issue()

    producer = JsmIncidentProducer(
        client=Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
        window_start=datetime(2026, 7, 20, tzinfo=UTC),
        window_end=datetime(2026, 7, 21, tzinfo=UTC),
        allowed_project_keys=("OPS",),
    )

    with pytest.raises(ValueError, match="service project"):
        anyio.run(producer.collect)


def test_normalize_done_incident_uses_resolution_date_and_replay_identity() -> None:
    producer = JsmIncidentProducer(
        client=_Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
    )
    done = _issue(status="Resolved")
    fields = done["fields"]
    assert isinstance(fields, dict)
    fields["status"] = {"name": "Resolved", "statusCategory": {"key": "done"}}

    first = producer.normalize([done]).incidents[0]
    replay = producer.normalize([done]).incidents[0]

    assert first.id == replay.id
    assert first.resolved_at == datetime(2026, 7, 20, 11, tzinfo=UTC)


def test_normalize_maps_new_status_category_to_canonical_open() -> None:
    producer = JsmIncidentProducer(
        client=_Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
    )

    incident = producer.normalize([_issue(status_category="new")]).incidents[0]

    assert incident.normalized_status == "open"


def test_normalize_keeps_organizations_and_cloud_instances_isolated() -> None:
    first = (
        JsmIncidentProducer(
            client=_Client(),
            org_id="org-a",
            provider_instance_id="cloud-a",
            base_url="https://a.atlassian.net",
        )
        .normalize([_issue()])
        .incidents[0]
    )
    second = (
        JsmIncidentProducer(
            client=_Client(),
            org_id="org-b",
            provider_instance_id="cloud-b",
            base_url="https://b.atlassian.net",
        )
        .normalize([_issue()])
        .incidents[0]
    )

    assert (first.org_id, first.provider_instance_id, first.id) != (
        second.org_id,
        second.provider_instance_id,
        second.id,
    )


def test_normalize_rejects_malformed_issue() -> None:
    producer = JsmIncidentProducer(
        client=_Client(),
        org_id="org-a",
        provider_instance_id="cloud-a",
        base_url="https://example.atlassian.net",
    )

    with pytest.raises(JsmPayloadError, match="id"):
        producer.normalize([{"key": "OPS-7", "fields": {}}])
