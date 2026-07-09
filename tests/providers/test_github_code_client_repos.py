from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient
from dev_health_ops.sync.budget_types import BudgetDimension


def _client(transport: httpx.AsyncBaseTransport) -> GitHubCodeClient:
    return GitHubCodeClient(auth=GitHubAuth(token="unit-test-pat"), transport=transport)


def test_get_repo_normalizes_metadata_and_records_repo_usage() -> None:
    asyncio.run(_test_get_repo_normalizes_metadata_and_records_repo_usage())


async def _test_get_repo_normalizes_metadata_and_records_repo_usage() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "id": "123",
                "name": "widgets",
                "full_name": "acme/widgets",
                "default_branch": "trunk",
                "description": "Widget service",
                "html_url": "https://github.com/acme/widgets",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-02T00:00:00Z",
                "language": "Python",
                "stargazers_count": "5",
                "forks_count": 2,
            },
        )

    client = _client(httpx.MockTransport(handler))

    repo = await client.get_repo("acme", "widgets")
    observations = client.drain_usage_observations()
    await client.close()

    assert seen[0].url.path == "/repos/acme/widgets"
    assert repo.id == 123
    assert repo.name == "widgets"
    assert repo.full_name == "acme/widgets"
    assert repo.default_branch == "trunk"
    assert repo.url == "https://github.com/acme/widgets"
    assert repo.created_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert repo.updated_at == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert repo.language == "Python"
    assert repo.stars == 5
    assert repo.forks == 2
    assert len(observations) == 1
    assert observations[0]["route_family"] == "repo"
    assert observations[0]["dimension"] == BudgetDimension.REST_CORE.value
    assert observations[0]["request_count"] == 1
    assert observations[0]["example_operation"].startswith("repo:")


def test_get_repo_percent_encodes_owner_and_repo_path_segments() -> None:
    asyncio.run(_test_get_repo_percent_encodes_owner_and_repo_path_segments())


async def _test_get_repo_percent_encodes_owner_and_repo_path_segments() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "id": 1,
                "name": "widgets",
                "full_name": "acme/widgets",
                "default_branch": "main",
            },
        )

    client = _client(httpx.MockTransport(handler))

    await client.get_repo("acme/evil", "widgets?x=1")
    await client.close()

    assert seen[0].url.raw_path.decode() == "/repos/acme%2Fevil/widgets%3Fx%3D1"
    assert seen[0].url.params.get("x") is None


def test_list_repositories_follows_link_header_applies_pattern_and_records_pages() -> (
    None
):
    asyncio.run(
        _test_list_repositories_follows_link_header_applies_pattern_and_records_pages()
    )


async def _test_list_repositories_follows_link_header_applies_pattern_and_records_pages() -> (
    None
):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                200,
                headers={
                    "Link": (
                        '<https://api.github.com/orgs/acme/repos?page=2>; rel="next"'
                    )
                },
                json=[
                    {
                        "id": 1,
                        "name": "api",
                        "full_name": "acme/api",
                        "default_branch": "main",
                    },
                    {
                        "id": 2,
                        "name": "web",
                        "full_name": "acme/web",
                        "default_branch": "main",
                    },
                ],
            )
        return httpx.Response(
            200,
            json=[
                {
                    "id": 3,
                    "name": "api-admin",
                    "full_name": "acme/api-admin",
                    "default_branch": "main",
                }
            ],
        )

    client = _client(httpx.MockTransport(handler))

    repos = await client.list_repositories(
        org_name="acme", pattern="*/api*", max_repos=2
    )
    observations = client.drain_usage_observations()
    await client.close()

    assert [repo.full_name for repo in repos] == ["acme/api", "acme/api-admin"]
    assert len(seen) == 2
    assert seen[0].url.path == "/orgs/acme/repos"
    assert seen[0].url.params["per_page"] == "100"
    assert observations[0]["route_family"] == "repo"
    assert observations[0]["request_count"] == 2


def test_list_repositories_stops_pagination_when_max_matching_repos_reached() -> None:
    asyncio.run(
        _test_list_repositories_stops_pagination_when_max_matching_repos_reached()
    )


async def _test_list_repositories_stops_pagination_when_max_matching_repos_reached() -> (
    None
):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) > 1:
            raise AssertionError("max_repos should stop before fetching page 2")
        return httpx.Response(
            200,
            headers={
                "Link": '<https://api.github.com/orgs/acme/repos?page=2>; rel="next"'
            },
            json=[
                {
                    "id": 1,
                    "name": "api",
                    "full_name": "acme/api",
                    "default_branch": "main",
                },
                {
                    "id": 2,
                    "name": "web",
                    "full_name": "acme/web",
                    "default_branch": "main",
                },
                {
                    "id": 3,
                    "name": "api-admin",
                    "full_name": "acme/api-admin",
                    "default_branch": "main",
                },
            ],
        )

    client = _client(httpx.MockTransport(handler))

    repos = await client.list_repositories(
        org_name="acme", pattern="*/api*", max_repos=2
    )
    observations = client.drain_usage_observations()
    await client.close()

    assert [repo.full_name for repo in repos] == ["acme/api", "acme/api-admin"]
    assert len(seen) == 1
    assert observations[0]["request_count"] == 1


def test_list_repositories_search_uses_scoped_search_and_data_key() -> None:
    asyncio.run(_test_list_repositories_search_uses_scoped_search_and_data_key())


async def _test_list_repositories_search_uses_scoped_search_and_data_key() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "id": "4",
                        "name": "api",
                        "full_name": "acme/api",
                        "default_branch": "main",
                    }
                ]
            },
        )

    client = _client(httpx.MockTransport(handler))

    repos = await client.list_repositories(org_name="acme", search="api")
    await client.close()

    assert [repo.id for repo in repos] == [4]
    assert seen[0].url.path == "/search/repositories"
    assert seen[0].url.params["q"] == "api org:acme"


def test_list_installation_repositories_uses_installation_endpoint() -> None:
    asyncio.run(_test_list_installation_repositories_uses_installation_endpoint())


async def _test_list_installation_repositories_uses_installation_endpoint() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "repositories": [
                    {
                        "id": 5,
                        "name": "install-repo",
                        "full_name": "acme/install-repo",
                        "default_branch": "main",
                    }
                ]
            },
        )

    client = _client(httpx.MockTransport(handler))

    repos = await client.list_installation_repositories(search="install")
    await client.close()

    assert [repo.full_name for repo in repos] == ["acme/install-repo"]
    assert seen[0].url.path == "/installation/repositories"
