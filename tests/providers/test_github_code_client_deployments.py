from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient


def _client(transport: httpx.AsyncBaseTransport) -> GitHubCodeClient:
    return GitHubCodeClient(auth=GitHubAuth(token="unit-test-pat"), transport=transport)


def _sequence(
    responses: list[httpx.Response],
) -> tuple[httpx.MockTransport, dict[str, int]]:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        response = responses[min(calls["n"], len(responses) - 1)]
        calls["n"] += 1
        return response

    return httpx.MockTransport(handler), calls


def test_deployments_send_token_and_accept_headers() -> None:
    asyncio.run(_test_deployments_send_token_and_accept_headers())


async def _test_deployments_send_token_and_accept_headers() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[])

    client = _client(httpx.MockTransport(handler))
    await client.get_deployments("acme", "widgets")
    await client.close()

    assert seen[0].headers["Authorization"] == "token unit-test-pat"
    assert seen[0].headers["Accept"] == "application/vnd.github+json"


def test_deployments_follow_link_header_pages() -> None:
    asyncio.run(_test_deployments_follow_link_header_pages())


async def _test_deployments_follow_link_header_pages() -> None:
    page1 = httpx.Response(
        200,
        json=[{"id": 1, "created_at": "2026-01-01T00:00:00Z"}],
        headers={
            "Link": (
                "<https://api.github.com/repos/acme/widgets/deployments?page=2>; "
                'rel="next"'
            )
        },
    )
    page2 = httpx.Response(200, json=[{"id": 2, "created_at": "2026-01-02T00:00:00Z"}])
    transport, calls = _sequence([page1, page2])
    client = _client(transport)

    deployments = await client.get_deployments("acme", "widgets")
    await client.close()

    assert [deployment.deployment_id for deployment in deployments] == ["1", "2"]
    assert calls["n"] == 2


def test_deployment_first_request_params_followups_use_link_url() -> None:
    asyncio.run(_test_deployment_first_request_params_followups_use_link_url())


async def _test_deployment_first_request_params_followups_use_link_url() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                200,
                json=[{"id": 1, "created_at": "2026-01-01T00:00:00Z"}],
                headers={
                    "Link": (
                        "<https://api.github.com/repos/acme/widgets/deployments?page=2>; "
                        'rel="next"'
                    )
                },
            )
        return httpx.Response(
            200, json=[{"id": 2, "created_at": "2026-01-02T00:00:00Z"}]
        )

    client = _client(httpx.MockTransport(handler))
    await client.get_deployments("acme", "widgets")
    await client.close()

    assert seen[0].url.params["per_page"] == "100"
    assert "per_page" not in seen[1].url.params


def test_deployments_max_limit_bounds_pagination() -> None:
    asyncio.run(_test_deployments_max_limit_bounds_pagination())


async def _test_deployments_max_limit_bounds_pagination() -> None:
    page1 = httpx.Response(
        200,
        json=[{"id": 1, "created_at": "2026-01-01T00:00:00Z"}],
        headers={
            "Link": (
                "<https://api.github.com/repos/acme/widgets/deployments?page=2>; "
                'rel="next"'
            )
        },
    )
    page2 = httpx.Response(200, json=[{"id": 2, "created_at": "2026-01-02T00:00:00Z"}])
    transport, calls = _sequence([page1, page2])
    client = _client(transport)

    deployments = await client.get_deployments("acme", "widgets", max_deployments=1)
    await client.close()

    assert [deployment.deployment_id for deployment in deployments] == ["1"]
    assert calls["n"] == 1


def test_deployment_releases_max_limit_bounds_pagination() -> None:
    asyncio.run(_test_deployment_releases_max_limit_bounds_pagination())


async def _test_deployment_releases_max_limit_bounds_pagination() -> None:
    page1 = httpx.Response(
        200,
        json=[{"tag_name": "v1"}],
        headers={
            "Link": (
                "<https://api.github.com/repos/acme/widgets/releases?page=2>; "
                'rel="next"'
            )
        },
    )
    page2 = httpx.Response(200, json=[{"tag_name": "v2"}])
    transport, calls = _sequence([page1, page2])
    client = _client(transport)

    releases = await client.get_deployment_releases("acme", "widgets", max_releases=1)
    await client.close()

    assert [release.tag_name for release in releases] == ["v1"]
    assert calls["n"] == 1


def test_deployment_field_mapping() -> None:
    asyncio.run(_test_deployment_field_mapping())


async def _test_deployment_field_mapping() -> None:
    payload = [
        {
            "id": 99,
            "state": "success",
            "environment": "prod",
            "created_at": "2026-01-01T00:00:00Z",
            "sha": "abc123",
            "ref": "v1.2.3",
            "payload": {"release_tag": "v1.2.3"},
        }
    ]
    client = _client(httpx.MockTransport(lambda r: httpx.Response(200, json=payload)))

    deployments = await client.get_deployments("acme", "widgets")
    await client.close()

    deployment = deployments[0]
    assert deployment.deployment_id == "99"
    assert deployment.state == "success"
    assert deployment.environment == "prod"
    assert deployment.created_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert deployment.sha == "abc123"
    assert deployment.ref == "v1.2.3"
    assert deployment.payload == {"release_tag": "v1.2.3"}


def test_deployment_pr_lookup_prefers_direct_merge() -> None:
    asyncio.run(_test_deployment_pr_lookup_prefers_direct_merge())


async def _test_deployment_pr_lookup_prefers_direct_merge() -> None:
    pulls: list[dict[str, Any]] = [
        {
            "number": 100,
            "merged_at": "2026-01-02T00:00:00Z",
            "merge_commit_sha": "other",
        },
        {
            "number": 42,
            "merged_at": "2026-01-03T00:00:00Z",
            "merge_commit_sha": "abc123",
        },
    ]
    client = _client(httpx.MockTransport(lambda r: httpx.Response(200, json=pulls)))

    number, merged_at = await client.get_deployment_pull_request(
        "acme", "widgets", "abc123"
    )
    await client.close()

    assert number == 42
    assert merged_at == datetime(2026, 1, 3, tzinfo=timezone.utc)


def test_deployment_pr_lookup_failure_is_soft() -> None:
    asyncio.run(_test_deployment_pr_lookup_failure_is_soft())


async def _test_deployment_pr_lookup_failure_is_soft() -> None:
    client = _client(
        httpx.MockTransport(lambda r: httpx.Response(404, json={"message": "missing"}))
    )

    assert await client.get_deployment_pull_request("acme", "widgets", "abc") == (
        None,
        None,
    )
    await client.close()


def test_deployments_rate_limited_403_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(_test_deployments_rate_limited_403_retries(monkeypatch))


async def _test_deployments_rate_limited_403_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
    )
    limited = httpx.Response(
        403,
        headers={"Retry-After": "0"},
        json={"message": "You have exceeded a secondary rate limit."},
    )
    ok = httpx.Response(200, json=[{"id": 5, "created_at": "2026-01-01T00:00:00Z"}])
    transport, calls = _sequence([limited, ok])
    client = _client(transport)

    deployments = await client.get_deployments("acme", "widgets")
    await client.close()

    assert [deployment.deployment_id for deployment in deployments] == ["5"]
    assert calls["n"] == 2


def test_deployments_usage_resolves_to_deployments_family() -> None:
    asyncio.run(_test_deployments_usage_resolves_to_deployments_family())


async def _test_deployments_usage_resolves_to_deployments_family() -> None:
    client = _client(httpx.MockTransport(lambda r: httpx.Response(200, json=[])))

    await client.get_deployments("acme", "widgets")
    observations = client.drain_usage_observations()
    await client.close()

    assert len(observations) == 1
    assert observations[0]["route_family"] == "deployments"
    assert observations[0]["request_count"] == 1
