from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from dev_health_ops.exceptions import AuthenticationException
from dev_health_ops.providers.pagerduty.auth import OAuthBearerAuth
from dev_health_ops.providers.pagerduty.client import (
    PagerDutyClient,
    pagerduty_base_url,
)


@pytest.mark.asyncio
async def test_incident_pagination_honors_more_and_uses_bearer_auth() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        offset = request.url.params["offset"]
        return httpx.Response(
            200,
            json={"incidents": [{"id": offset, "raw": {}}], "more": offset == "0"},
        )

    client = PagerDutyClient(
        OAuthBearerAuth("oauth"), transport=httpx.MockTransport(handler)
    )
    incidents = await client.list_incidents(params={"statuses[]": "triggered"})

    assert [incident.id for incident in incidents] == ["0", "1"]
    assert [request.url.params["offset"] for request in requests] == ["0", "1"]
    assert requests[0].headers["Authorization"] == "Bearer oauth"
    assert requests[0].headers["Accept"] == "application/vnd.pagerduty+json;version=2"


@pytest.mark.asyncio
async def test_incident_page_iterator_preserves_window_at_each_resume_offset() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        offset = request.url.params["offset"]
        return httpx.Response(
            200,
            json={"incidents": [{"id": offset, "raw": {}}], "more": offset == "0"},
        )

    client = PagerDutyClient(
        OAuthBearerAuth("oauth"), transport=httpx.MockTransport(handler)
    )
    pages = [
        page
        async for page in client.iter_incident_pages(
            params={"since": "a", "until": "b"}
        )
    ]

    assert [[incident.id for incident in page] for page in pages] == [["0"], ["1"]]
    assert [
        (request.url.params["since"], request.url.params["until"])
        for request in requests
    ] == [("a", "b"), ("a", "b")]


@pytest.mark.asyncio
async def test_incident_alert_page_iterator_fetches_only_requested_pages() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        offset = request.url.params["offset"]
        return httpx.Response(
            200,
            json={"alerts": [{"id": offset, "raw": {}}], "more": offset == "0"},
        )

    client = PagerDutyClient(
        OAuthBearerAuth("oauth"), transport=httpx.MockTransport(handler)
    )
    pages = client.iter_incident_alert_pages("incident-1")
    first_page = await anext(pages)

    assert [alert.id for alert in first_page] == ["0"]
    assert [request.url.params["offset"] for request in requests] == ["0"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_retries_transient_response_with_bounded_attempts(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    responses = iter(
        [
            httpx.Response(status, headers={"ratelimit-reset": "1"}),
            httpx.Response(200, json={"users": [], "more": False}),
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr("dev_health_ops.providers._http.asyncio.sleep", sleep)
    client = PagerDutyClient(
        OAuthBearerAuth("oauth"),
        transport=httpx.MockTransport(lambda _: next(responses)),
    )

    assert await client.list_users() == []
    assert sleep.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403])
async def test_does_not_retry_auth_failures(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(status))
    sleep = AsyncMock()
    monkeypatch.setattr("dev_health_ops.providers._http.asyncio.sleep", sleep)
    client = PagerDutyClient(OAuthBearerAuth("oauth"), transport=transport)

    with pytest.raises(AuthenticationException):
        await client.list_users()
    assert sleep.await_count == 0


def test_client_has_no_public_mutation_methods_and_region_is_explicit() -> None:
    names = {name for name in dir(PagerDutyClient) if not name.startswith("_")}
    assert not {"create", "update", "delete", "post", "put", "patch"}.intersection(
        names
    )
    assert pagerduty_base_url(region="eu") == "https://api.eu.pagerduty.com"
    assert pagerduty_base_url(region="us") == "https://api.pagerduty.com"
