from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from dev_health_ops.exceptions import APIException, NotFoundException
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient
from dev_health_ops.sync.budget_types import BudgetDimension


def _client(transport: httpx.AsyncBaseTransport) -> GitHubCodeClient:
    return GitHubCodeClient(auth=GitHubAuth(token="unit-test-pat"), transport=transport)


def test_iter_pulls_normalizes_fields_and_utc_datetimes() -> None:
    asyncio.run(_test_iter_pulls_normalizes_fields_and_utc_datetimes())


async def _test_iter_pulls_normalizes_fields_and_utc_datetimes() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json=[
                {
                    "id": 123,
                    "number": "42",
                    "title": "Fix widgets",
                    "body": "Details",
                    "state": "closed",
                    "user": {"login": "octocat"},
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-02T00:00:00Z",
                    "merged_at": "2026-01-03T00:00:00Z",
                    "closed_at": "2026-01-04T00:00:00Z",
                    "head": {"ref": "feature"},
                    "base": {"ref": "main"},
                    "additions": "5",
                    "deletions": 2,
                    "changed_files": "3",
                    "comments": "7",
                }
            ],
        )

    client = _client(httpx.MockTransport(handler))
    pulls = await client.iter_pulls("acme", "widgets", state="all")
    await client.close()

    assert len(seen) == 1
    assert seen[0].url.params["state"] == "all"
    assert seen[0].url.params["sort"] == "updated"
    assert seen[0].url.params["direction"] == "desc"
    pull = pulls[0]
    assert pull.pull_id == "123"
    assert pull.number == 42
    assert pull.author_login == "octocat"
    assert pull.created_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert pull.updated_at == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert pull.merged_at == datetime(2026, 1, 3, tzinfo=timezone.utc)
    assert pull.closed_at == datetime(2026, 1, 4, tzinfo=timezone.utc)
    assert pull.additions == 5
    assert pull.deletions == 2
    assert pull.changed_files == 3
    assert pull.comments_count == 7


def test_iter_pulls_follows_link_header_until_absent_last_page() -> None:
    asyncio.run(_test_iter_pulls_follows_link_header_until_absent_last_page())


async def _test_iter_pulls_follows_link_header_until_absent_last_page() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                200,
                json=[{"id": 1, "number": 1}],
                headers={
                    "Link": (
                        "<https://api.github.com/repos/acme/widgets/pulls?page=2>; "
                        'rel="next"'
                    )
                },
            )
        return httpx.Response(200, json=[{"id": 2, "number": 2}])

    client = _client(httpx.MockTransport(handler))
    pulls = await client.iter_pulls("acme", "widgets")
    observations = client.drain_usage_observations()
    await client.close()

    assert [pull.number for pull in pulls] == [1, 2]
    assert len(seen) == 2
    assert observations[0]["route_family"] == "prs"
    assert observations[0]["dimension"] == BudgetDimension.REST_CORE.value
    assert observations[0]["request_count"] == 2
    assert observations[0]["example_operation"].startswith("prs:")


def test_iter_pulls_stops_at_since_boundary_without_fetching_later_pages() -> None:
    asyncio.run(_test_iter_pulls_stops_at_since_boundary_without_fetching_later_pages())


async def _test_iter_pulls_stops_at_since_boundary_without_fetching_later_pages() -> (
    None
):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                200,
                json=[
                    {"id": 1, "number": 1, "updated_at": "2026-01-03T00:00:00Z"},
                    {"id": 2, "number": 2, "updated_at": "2026-01-01T00:00:00Z"},
                ],
                headers={
                    "Link": (
                        "<https://api.github.com/repos/acme/widgets/pulls?page=2>; "
                        'rel="next"'
                    )
                },
            )
        return httpx.Response(
            200,
            json=[{"id": 3, "number": 3, "updated_at": "2025-12-31T00:00:00Z"}],
        )

    client = _client(httpx.MockTransport(handler))
    pulls = await client.iter_pulls(
        "acme",
        "widgets",
        since=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    observations = client.drain_usage_observations()
    await client.close()

    assert [pull.number for pull in pulls] == [1]
    assert len(seen) == 1
    assert observations[0]["route_family"] == "prs"
    assert observations[0]["request_count"] == 1


def test_get_pull_detail_records_prs_usage_and_normalizes_stats() -> None:
    asyncio.run(_test_get_pull_detail_records_prs_usage_and_normalizes_stats())


async def _test_get_pull_detail_records_prs_usage_and_normalizes_stats() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "id": 42,
                "number": 7,
                "updated_at": "2026-01-03T00:00:00Z",
                "additions": 123,
                "deletions": 45,
                "changed_files": 6,
                "comments": 8,
            },
        )

    client = _client(httpx.MockTransport(handler))
    pull = await client.get_pull_detail("acme", "widgets", 7)
    observations = client.drain_usage_observations()
    await client.close()

    assert seen[0].url.path == "/repos/acme/widgets/pulls/7"
    assert pull.additions == 123
    assert pull.deletions == 45
    assert pull.changed_files == 6
    assert pull.comments_count == 8
    assert observations[0]["route_family"] == "prs"
    assert observations[0]["request_count"] == 1
    assert observations[0]["example_operation"].startswith("prs:")


def test_iter_pulls_empty_first_page_stops_without_followup() -> None:
    asyncio.run(_test_iter_pulls_empty_first_page_stops_without_followup())


async def _test_iter_pulls_empty_first_page_stops_without_followup() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[])

    client = _client(httpx.MockTransport(handler))
    pulls = await client.iter_pulls("acme", "widgets")
    observations = client.drain_usage_observations()
    await client.close()

    assert pulls == []
    assert len(seen) == 1
    assert observations[0]["request_count"] == 1


def test_iter_pulls_404_is_not_treated_as_empty() -> None:
    asyncio.run(_test_iter_pulls_404_is_not_treated_as_empty())


async def _test_iter_pulls_404_is_not_treated_as_empty() -> None:
    client = _client(httpx.MockTransport(lambda r: httpx.Response(404, json={})))

    with pytest.raises(NotFoundException):
        await client.iter_pulls("acme", "missing")
    observations = client.drain_usage_observations()
    await client.close()

    assert observations[0]["route_family"] == "prs"
    assert observations[0]["request_count"] == 1


def test_iter_pull_commits_normalizes_and_encodes_path_segments() -> None:
    asyncio.run(_test_iter_pull_commits_normalizes_and_encodes_path_segments())


async def _test_iter_pull_commits_normalizes_and_encodes_path_segments() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json=[
                {
                    "sha": 123,
                    "commit": {
                        "message": "msg",
                        "author": {
                            "name": "Ada",
                            "email": "a@example.com",
                            "date": "2026-02-01T00:00:00Z",
                        },
                        "committer": {
                            "name": "Grace",
                            "email": "g@example.com",
                            "date": "2026-02-02T00:00:00Z",
                        },
                    },
                    "parents": [{"sha": "p"}],
                }
            ],
        )

    client = _client(httpx.MockTransport(handler))
    commits = await client.iter_pull_commits("acme/evil", "widgets?x=1", 42)
    observations = client.drain_usage_observations()
    await client.close()

    assert commits[0].sha == "123"
    assert commits[0].author_when == datetime(2026, 2, 1, tzinfo=timezone.utc)
    assert commits[0].committer_when == datetime(2026, 2, 2, tzinfo=timezone.utc)
    assert commits[0].parent_count == 1
    assert (
        seen[0]
        .url.raw_path.decode()
        .startswith("/repos/acme%2Fevil/widgets%3Fx%3D1/pulls/42/commits?")
    )
    assert seen[0].url.params.get("x") is None
    assert observations[0]["route_family"] == "prs"
    assert observations[0]["request_count"] == 1


def test_iter_issues_sends_label_filter_and_skips_pull_requests() -> None:
    asyncio.run(_test_iter_issues_sends_label_filter_and_skips_pull_requests())


async def _test_iter_issues_sends_label_filter_and_skips_pull_requests() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json=[
                {
                    "id": "100",
                    "number": "5",
                    "state": "closed",
                    "created_at": "2026-03-01T00:00:00Z",
                    "closed_at": "2026-03-02T00:00:00Z",
                },
                {
                    "id": 101,
                    "number": 6,
                    "state": "open",
                    "created_at": "2026-03-03T00:00:00Z",
                    "pull_request": {"url": "https://api.github.com/pulls/6"},
                },
            ],
        )

    client = _client(httpx.MockTransport(handler))
    issues = await client.iter_issues(
        "acme", "widgets", state="all", labels=["incident"]
    )
    observations = client.drain_usage_observations()
    await client.close()

    assert seen[0].url.params["labels"] == "incident"
    assert [issue.issue_id for issue in issues] == ["100"]
    assert issues[0].created_at == datetime(2026, 3, 1, tzinfo=timezone.utc)
    assert issues[0].closed_at == datetime(2026, 3, 2, tzinfo=timezone.utc)
    assert observations[0]["route_family"] == "incidents"
    assert observations[0]["dimension"] == BudgetDimension.REST_CORE.value
    assert observations[0]["example_operation"].startswith("incidents:")


def test_iter_issues_failure_mid_pagination_preserves_partial_observations() -> None:
    asyncio.run(
        _test_iter_issues_failure_mid_pagination_preserves_partial_observations()
    )


async def _test_iter_issues_failure_mid_pagination_preserves_partial_observations() -> (
    None
):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                200,
                json=[{"id": 1, "number": 1, "created_at": "2026-01-01T00:00:00Z"}],
                headers={
                    "Link": (
                        "<https://api.github.com/repos/acme/widgets/issues?page=2>; "
                        'rel="next"'
                    )
                },
            )
        return httpx.Response(500, json={"message": "boom"})

    client = _client(httpx.MockTransport(handler))

    with pytest.raises(APIException):
        await client.iter_issues("acme", "widgets", state="all", labels=["incident"])
    observations = client.drain_usage_observations()
    await client.close()

    assert len(seen) == 6
    assert observations[0]["route_family"] == "incidents"
    assert observations[0]["request_count"] == len(seen)
    assert observations[0]["latest_status"] == 500
