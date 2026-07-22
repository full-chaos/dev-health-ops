from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import UTC, datetime

import anyio
import pytest
import requests

from dev_health_ops.providers.jira.client import (
    JiraAuth,
    JiraClient,
    discover_jsm_cloud_id,
    validate_jsm_cloud_origin,
)


def _client() -> JiraClient:
    return JiraClient(
        auth=JiraAuth(
            base_url="https://example.atlassian.net",
            email="a@example.com",
            api_token="token",
        )
    )


def test_service_desks_and_enhanced_jql_use_pagination_and_exact_body(
    monkeypatch,
) -> None:
    client = _client()
    calls: list[tuple[str, str, dict[str, object]]] = []
    responses: Iterator[Mapping[str, object]] = iter(
        [
            {"cloudId": "test-cloud"},
            {
                "values": [{"projectKey": "OPS"}, {"projectKey": "ALERTS"}],
                "isLastPage": False,
                "start": 0,
                "limit": 2,
            },
            {
                "values": [{"projectKey": "HELP"}, {"name": "Incident desk"}],
                "isLastPage": True,
                "start": 2,
                "limit": 2,
            },
            {
                "issues": [{"id": "1", "key": "OPS-1"}],
                "nextPageToken": "page-2",
                "isLast": False,
            },
            {"issues": [{"id": "2", "key": "HELP-2"}], "isLast": True},
        ]
    )

    def request(
        *,
        method: str = "GET",
        path: str,
        params: dict[str, object] | None = None,
        json: dict[str, object] | None = None,
        allow_redirects: bool = True,
    ) -> dict[str, object]:
        payload = params if params is not None else json or {}
        calls.append((method, path, payload))
        return dict(next(responses))

    monkeypatch.setattr(client, "_request_json", request)

    async def collect() -> tuple[list[str], list[dict[str, object]]]:
        desks = [desk async for desk in client.iter_service_desks()]
        issues = [
            issue
            async for issue in client.iter_jsm_incident_issues(
                project_keys=("OPS", "HELP"),
                window_start=datetime(2026, 7, 20, tzinfo=UTC),
                window_end=datetime(2026, 7, 21, tzinfo=UTC),
            )
        ]
        return desks, issues

    desks, issues = anyio.run(collect)

    assert desks == ["OPS", "ALERTS", "HELP"]

    assert [issue["id"] for issue in issues] == ["1", "2"]
    assert calls[3] == (
        "POST",
        "/rest/api/3/search/jql",
        {
            "jql": (
                'project in (OPS, HELP) AND "Ticket category" = Incidents '
                'AND updated >= "2026-07-20T00:00:00+00:00" '
                'AND updated < "2026-07-21T00:00:00+00:00" '
                "ORDER BY updated ASC, key ASC"
            ),
            "maxResults": 100,
            "fields": [
                "id",
                "key",
                "summary",
                "created",
                "updated",
                "resolutiondate",
                "status",
                "priority",
            ],
        },
    )
    assert calls[4][2]["nextPageToken"] == "page-2"


def test_enhanced_jql_rejects_non_advancing_token(monkeypatch) -> None:
    client = _client()
    client._jsm_cloud_id = "test-cloud"

    monkeypatch.setattr(
        client,
        "_request_json",
        lambda **_: {
            "issues": [{"id": "1"}],
            "nextPageToken": "again",
            "isLast": False,
        },
    )

    try:
        anyio.run(lambda: _collect_issues(client, project_keys=("OPS",)))
    except RuntimeError as error:
        assert "nextPageToken" in str(error)
    else:
        raise AssertionError("expected non-advancing token rejection")


async def _collect_issues(
    client: JiraClient, *, project_keys: tuple[str, ...]
) -> list[dict[str, object]]:
    return [
        issue
        async for issue in client.iter_jsm_incident_issues(
            project_keys=project_keys,
            window_start=datetime(2026, 7, 20, tzinfo=UTC),
            window_end=datetime(2026, 7, 21, tzinfo=UTC),
        )
    ]


@pytest.mark.parametrize("payload", [{}, {"values": "not-a-list"}])
def test_service_desks_rejects_missing_or_malformed_values(
    monkeypatch, payload: dict[str, object]
) -> None:
    client = _client()
    client._jsm_cloud_id = "test-cloud"
    monkeypatch.setattr(client, "_request_json", lambda **_: payload)

    with pytest.raises(RuntimeError, match="values"):
        anyio.run(lambda: _collect_desks(client))


@pytest.mark.parametrize("payload", [{}, {"issues": "not-a-list"}])
def test_enhanced_jql_rejects_missing_or_malformed_issues(
    monkeypatch, payload: dict[str, object]
) -> None:
    client = _client()
    client._jsm_cloud_id = "test-cloud"
    monkeypatch.setattr(client, "_request_json", lambda **_: payload)

    with pytest.raises(RuntimeError, match="issues"):
        anyio.run(lambda: _collect_issues(client, project_keys=("OPS",)))


async def _collect_desks(client: JiraClient) -> list[str]:
    return [desk async for desk in client.iter_service_desks()]


@pytest.mark.parametrize(
    "origin",
    [
        "http://example.atlassian.net",
        "https://user@example.atlassian.net",
        "https://example.atlassian.net/tenant",
        "https://example.atlassian.net?token=secret",
        "https://example.atlassian.net#fragment",
        "https://example.atlassian.net.attacker.example",
        "https://169.254.169.254",
    ],
)
def test_jsm_cloud_origin_rejects_untrusted_origin(origin: str) -> None:
    with pytest.raises(RuntimeError, match="JSM incident reads"):
        validate_jsm_cloud_origin(origin)


def test_jsm_cloud_id_discovery_rejects_mismatch(monkeypatch) -> None:
    client = _client()
    monkeypatch.setattr(
        client, "_request_json", lambda **_: {"cloudId": "actual-cloud"}
    )

    with pytest.raises(RuntimeError, match="cloud ID does not match"):
        discover_jsm_cloud_id(client, expected_cloud_id="configured-cloud")


def test_jsm_incident_path_rejects_configured_cloud_id_mismatch(monkeypatch) -> None:
    client = JiraClient(
        auth=JiraAuth(
            base_url="https://example.atlassian.net",
            email="a@example.com",
            api_token="token",
            cloud_id="configured-cloud",
        )
    )
    monkeypatch.setattr(
        client, "_request_json", lambda **_: {"cloudId": "actual-cloud"}
    )

    with pytest.raises(RuntimeError, match="cloud ID does not match"):
        anyio.run(lambda: _collect_desks(client))


def test_native_incident_admission_uses_fixed_host_and_numeric_issue_id(
    monkeypatch,
) -> None:
    client = _client()
    client._jsm_cloud_id = "cloud-123"
    calls: list[tuple[str, object, object]] = []

    class Response:
        status_code = 200
        headers: dict[str, str] = {}

        def json(self) -> dict[str, object]:
            return {"id": "native-incident"}

        def raise_for_status(self) -> None:
            return None

    def get(url: str, **kwargs: object) -> Response:
        calls.append((url, kwargs["auth"], kwargs["allow_redirects"]))
        return Response()

    monkeypatch.setattr("dev_health_ops.providers.jira.client.requests.get", get)

    assert anyio.run(lambda: client.admit_jsm_incident(issue_id="10001")) is True
    assert calls == [
        (
            "https://api.atlassian.com/jsm/incidents/cloudId/cloud-123/v1/incident/10001",
            ("a@example.com", "token"),
            False,
        )
    ]
    observations = client.drain_usage_observations()
    assert observations[0]["route_family"] == "jira_jsm_incident_admission"


def test_native_incident_admission_treats_404_as_negative_candidate(
    monkeypatch,
) -> None:
    client = _client()
    client._jsm_cloud_id = "cloud-123"

    class Response:
        status_code = 404
        headers: dict[str, str] = {}

    monkeypatch.setattr(
        "dev_health_ops.providers.jira.client.requests.get",
        lambda *args, **kwargs: Response(),
    )

    assert anyio.run(lambda: client.admit_jsm_incident(issue_id="10001")) is False


@pytest.mark.parametrize("status_code", [201, 204])
def test_native_incident_admission_requires_exact_200_and_does_not_parse(
    monkeypatch, status_code: int
) -> None:
    client = _client()
    client._jsm_cloud_id = "cloud-123"
    json_calls: list[None] = []

    class Response:
        headers: dict[str, str] = {}

        def __init__(self) -> None:
            self.status_code = status_code

        def json(self) -> dict[str, object]:
            json_calls.append(None)
            return {"id": "native-incident"}

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise requests.HTTPError(f"HTTP {self.status_code}")

    monkeypatch.setattr(
        "dev_health_ops.providers.jira.client.requests.get",
        lambda *args, **kwargs: Response(),
    )

    with pytest.raises(RuntimeError, match="HTTP 200"):
        anyio.run(lambda: client.admit_jsm_incident(issue_id="10001"))

    assert json_calls == []


def test_native_incident_admission_raises_for_other_error_status(
    monkeypatch,
) -> None:
    client = _client()
    client._jsm_cloud_id = "cloud-123"

    class Response:
        status_code = 500
        headers: dict[str, str] = {}

        def raise_for_status(self) -> None:
            raise requests.HTTPError("HTTP 500")

    monkeypatch.setattr(
        "dev_health_ops.providers.jira.client.requests.get",
        lambda *args, **kwargs: Response(),
    )

    with pytest.raises(requests.HTTPError, match="HTTP 500"):
        anyio.run(lambda: client.admit_jsm_incident(issue_id="10001"))


@pytest.mark.parametrize(
    "payload",
    [
        {"issues": [], "isLast": "yes"},
        {"issues": [], "isLast": False},
        {"issues": [], "isLast": False, "nextPageToken": ""},
    ],
)
def test_enhanced_jql_rejects_inconsistent_terminal_markers(
    monkeypatch, payload: dict[str, object]
) -> None:
    client = _client()
    client._jsm_cloud_id = "test-cloud"
    monkeypatch.setattr(client, "_request_json", lambda **_: payload)

    with pytest.raises(RuntimeError, match="JSM enhanced JQL"):
        anyio.run(lambda: _collect_issues(client, project_keys=("OPS",)))
