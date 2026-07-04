"""``GitHubCodeClient.get_file_contents`` / ``get_file_blame`` tests
(CHAOS-2773 CS7).

Proves the GraphQL content/blame methods POST through the client's owned
``InstrumentedRESTCore``, label operations with the explicit ``files:``/
``blame:`` route-family prefix (the CS1 resolver short-circuit), and drain
usage observations under the ``contents_blob`` dimension -- mirroring
``test_github_code_client_commits.py``'s pattern for the git/commit_stats
families.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping

import httpx
import pytest

from dev_health_ops.exceptions import APIException, RateLimitException
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient
from dev_health_ops.sync.budget_types import BudgetDimension


def _client(transport: httpx.AsyncBaseTransport) -> GitHubCodeClient:
    return GitHubCodeClient(auth=GitHubAuth(token="unit-test-pat"), transport=transport)


def _mock_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """No-op ``asyncio.sleep`` so retry backoff (a rate-limited 403 gets
    retried before the terminal ``RateLimitException``) does not actually
    block the test -- mirrors ``test_github_code_client_security.py``."""
    from unittest.mock import AsyncMock

    monkeypatch.setattr(
        "dev_health_ops.providers._http.asyncio.sleep",
        AsyncMock(return_value=None),
    )


def _blob_response(fields: Mapping[str, Mapping[str, object] | None]) -> httpx.Response:
    return httpx.Response(200, json={"data": {"repository": fields}})


def test_get_file_contents_posts_graphql_and_omits_binary_and_truncated() -> None:
    asyncio.run(_test_get_file_contents_posts_graphql_and_omits_binary_and_truncated())


async def _test_get_file_contents_posts_graphql_and_omits_binary_and_truncated() -> (
    None
):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return _blob_response(
            {
                "f0": {"text": "print(1)\n", "isBinary": False, "isTruncated": False},
                "f1": {"text": None, "isBinary": True, "isTruncated": False},
            }
        )

    client = _client(httpx.MockTransport(handler))
    result = await client.get_file_contents(
        "acme", "widgets", ["src/app.py", "src/logo.png"], ref="main"
    )
    await client.close()

    assert result == {"src/app.py": "print(1)\n"}
    assert len(seen) == 1
    assert str(seen[0].url).endswith("/graphql")
    assert seen[0].headers["Authorization"] == "Bearer unit-test-pat"
    body = json.loads(seen[0].content)
    assert body["variables"] == {"owner": "acme", "repo": "widgets"}
    assert "f0:" in body["query"]
    assert "f1:" in body["query"]


def test_get_file_contents_batches_across_batch_size() -> None:
    asyncio.run(_test_get_file_contents_batches_across_batch_size())


async def _test_get_file_contents_batches_across_batch_size() -> None:
    calls: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body)
        # Reply with contents for however many aliased fields were requested.
        n = body["query"].count("object(expression:")
        fields = {
            f"f{i}": {"text": f"text-{i}", "isBinary": False, "isTruncated": False}
            for i in range(n)
        }
        return _blob_response(fields)

    client = _client(httpx.MockTransport(handler))
    paths = [f"src/f{i}.py" for i in range(5)]
    result = await client.get_file_contents(
        "acme", "widgets", paths, ref="main", batch_size=2
    )
    await client.close()

    # 5 paths at batch_size=2 -> 3 physical GraphQL requests.
    assert len(calls) == 3
    assert set(result.keys()) <= set(paths)


def test_get_file_contents_usage_resolves_to_files_contents_blob_family() -> None:
    asyncio.run(_test_get_file_contents_usage_resolves_to_files_contents_blob_family())


async def _test_get_file_contents_usage_resolves_to_files_contents_blob_family() -> (
    None
):
    client = _client(
        httpx.MockTransport(
            lambda r: _blob_response(
                {"f0": {"text": "x", "isBinary": False, "isTruncated": False}}
            )
        )
    )

    await client.get_file_contents("acme", "widgets", ["src/app.py"], ref="main")
    observations = client.drain_usage_observations()
    await client.close()

    assert len(observations) == 1
    assert observations[0]["route_family"] == "files"
    assert observations[0]["dimension"] == BudgetDimension.CONTENTS_BLOB.value
    assert observations[0]["request_count"] == 1
    assert observations[0]["example_operation"].startswith("files:")


def test_get_file_contents_raises_api_exception_on_graphql_errors() -> None:
    asyncio.run(_test_get_file_contents_raises_api_exception_on_graphql_errors())


async def _test_get_file_contents_raises_api_exception_on_graphql_errors() -> None:
    client = _client(
        httpx.MockTransport(
            lambda r: httpx.Response(
                200, json={"errors": [{"message": "field does not exist"}]}
            )
        )
    )

    with pytest.raises(APIException, match="field does not exist"):
        await client.get_file_contents("acme", "widgets", ["src/app.py"], ref="main")
    await client.close()


def test_get_file_blame_posts_graphql_and_parses_ranges() -> None:
    asyncio.run(_test_get_file_blame_posts_graphql_and_parses_ranges())


async def _test_get_file_blame_posts_graphql_and_parses_ranges() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "data": {
                    "repository": {
                        "object": {
                            "blame": {
                                "ranges": [
                                    {
                                        "startingLine": 1,
                                        "endingLine": 2,
                                        "commit": {
                                            "oid": "sha1",
                                            "authoredDate": "2020-01-01T00:00:00Z",
                                            "author": {
                                                "name": "Ada",
                                                "email": "ada@example.com",
                                            },
                                        },
                                    }
                                ]
                            }
                        }
                    }
                }
            },
        )

    client = _client(httpx.MockTransport(handler))
    blame = await client.get_file_blame("acme", "widgets", "src/app.py", ref="main")
    await client.close()

    assert len(seen) == 1
    body = json.loads(seen[0].content)
    assert body["variables"] == {
        "owner": "acme",
        "repo": "widgets",
        "path": "src/app.py",
        "ref": "main",
    }
    assert blame.file_path == "src/app.py"
    assert [
        (rng.starting_line, rng.ending_line, rng.commit_sha, rng.author)
        for rng in blame.ranges
    ] == [(1, 2, "sha1", "Ada")]


def test_get_file_blame_usage_resolves_to_blame_contents_blob_family() -> None:
    asyncio.run(_test_get_file_blame_usage_resolves_to_blame_contents_blob_family())


async def _test_get_file_blame_usage_resolves_to_blame_contents_blob_family() -> None:
    client = _client(
        httpx.MockTransport(lambda r: httpx.Response(200, json={"data": {}}))
    )

    await client.get_file_blame("acme", "widgets", "src/app.py", ref="main")
    observations = client.drain_usage_observations()
    await client.close()

    assert len(observations) == 1
    assert observations[0]["route_family"] == "blame"
    assert observations[0]["dimension"] == BudgetDimension.CONTENTS_BLOB.value
    assert observations[0]["example_operation"].startswith("blame:")


def test_get_file_blame_raises_rate_limit_exception_on_primary_403(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_sleep(monkeypatch)
    asyncio.run(_test_get_file_blame_raises_rate_limit_exception_on_primary_403())


async def _test_get_file_blame_raises_rate_limit_exception_on_primary_403() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            headers={"x-ratelimit-remaining": "0", "x-ratelimit-reset": "0"},
            json={"message": "API rate limit exceeded"},
        )

    client = _client(httpx.MockTransport(handler))

    with pytest.raises(RateLimitException) as exc_info:
        await client.get_file_blame("acme", "widgets", "src/app.py", ref="main")
    await client.close()

    assert exc_info.value.signal is not None
    assert exc_info.value.signal.dimension == BudgetDimension.CONTENTS_BLOB
