from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient


def _client(transport: httpx.AsyncBaseTransport) -> GitHubCodeClient:
    return GitHubCodeClient(auth=GitHubAuth(token="unit-test-pat"), transport=transport)


def test_commits_send_window_params_and_report_truncation() -> None:
    asyncio.run(_test_commits_send_window_params_and_report_truncation())


async def _test_commits_send_window_params_and_report_truncation() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json=[
                {
                    "sha": "a",
                    "commit": {"message": "one", "author": {}, "committer": {}},
                    "parents": [],
                },
                {
                    "sha": "b",
                    "commit": {"message": "two", "author": {}, "committer": {}},
                    "parents": [],
                },
            ],
        )

    client = _client(httpx.MockTransport(handler))
    since = datetime(2026, 1, 1, tzinfo=timezone.utc)
    until = datetime(2026, 1, 2, tzinfo=timezone.utc)

    commits, truncated = await client.get_commits(
        "acme", "widgets", max_commits=1, since=since, until=until
    )
    await client.close()

    assert [commit.sha for commit in commits] == ["a"]
    assert truncated is True
    assert seen[0].url.params["since"] == since.isoformat()
    assert seen[0].url.params["until"] == until.isoformat()


def test_commits_exact_size_window_is_not_truncated() -> None:
    asyncio.run(_test_commits_exact_size_window_is_not_truncated())


async def _test_commits_exact_size_window_is_not_truncated() -> None:
    # A window of exactly ``max_commits`` items (the iterator/pager ended
    # right at the cap, with no extra commit peeked) is a COMPLETE window,
    # not a truncated one -- indistinguishable from truncation by count alone,
    # so this locks the ``fetch_limit`` look-ahead boundary.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "sha": sha,
                    "commit": {"message": "m", "author": {}, "committer": {}},
                    "parents": [],
                }
                for sha in ("a", "b", "c")
            ],
        )

    client = _client(httpx.MockTransport(handler))
    commits, truncated = await client.get_commits("acme", "widgets", max_commits=3)
    await client.close()

    assert [commit.sha for commit in commits] == ["a", "b", "c"]
    assert truncated is False


def test_commits_under_cap_window_is_not_truncated() -> None:
    asyncio.run(_test_commits_under_cap_window_is_not_truncated())


async def _test_commits_under_cap_window_is_not_truncated() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {
                    "sha": sha,
                    "commit": {"message": "m", "author": {}, "committer": {}},
                    "parents": [],
                }
                for sha in ("a", "b")
            ],
        )

    client = _client(httpx.MockTransport(handler))
    commits, truncated = await client.get_commits("acme", "widgets", max_commits=3)
    await client.close()

    assert [commit.sha for commit in commits] == ["a", "b"]
    assert truncated is False


def test_commits_usage_resolves_to_git_family() -> None:
    asyncio.run(_test_commits_usage_resolves_to_git_family())


async def _test_commits_usage_resolves_to_git_family() -> None:
    client = _client(httpx.MockTransport(lambda r: httpx.Response(200, json=[])))

    await client.get_commits("acme", "widgets", max_commits=None)
    observations = client.drain_usage_observations()
    await client.close()

    assert len(observations) == 1
    assert observations[0]["route_family"] == "git"
    assert observations[0]["request_count"] == 1


def test_uncapped_commits_follow_all_link_pages() -> None:
    asyncio.run(_test_uncapped_commits_follow_all_link_pages())


async def _test_uncapped_commits_follow_all_link_pages() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        page = int(request.url.params.get("page", "1"))
        headers = {}
        if page < 101:
            headers["Link"] = (
                "<https://api.github.com/repos/acme/widgets/commits?page="
                f'{page + 1}>; rel="next"'
            )
        return httpx.Response(
            200,
            headers=headers,
            json=[
                {
                    "sha": f"sha-{page}",
                    "commit": {"message": "msg", "author": {}, "committer": {}},
                    "parents": [],
                }
            ],
        )

    client = _client(httpx.MockTransport(handler))

    commits, truncated = await client.get_commits("acme", "widgets", max_commits=None)
    await client.close()

    assert len(seen) == 101
    assert commits[-1].sha == "sha-101"
    assert truncated is False


def test_commit_file_stats_usage_resolves_to_commit_stats_family() -> None:
    asyncio.run(_test_commit_file_stats_usage_resolves_to_commit_stats_family())


async def _test_commit_file_stats_usage_resolves_to_commit_stats_family() -> None:
    client = _client(
        httpx.MockTransport(
            lambda r: httpx.Response(
                200,
                json={
                    "files": [
                        {"filename": "src/app.py", "additions": 3, "deletions": 1}
                    ]
                },
            )
        )
    )

    stats = await client.get_commit_file_stats("acme", "widgets", "abc123")
    observations = client.drain_usage_observations()
    await client.close()

    assert [(stat.file_path, stat.additions, stat.deletions) for stat in stats] == [
        ("src/app.py", 3, 1)
    ]
    assert len(observations) == 1
    assert observations[0]["route_family"] == "commit_stats"
    assert observations[0]["request_count"] == 1


def test_get_commits_percent_encodes_owner_and_repo_path_segments() -> None:
    asyncio.run(_test_get_commits_percent_encodes_owner_and_repo_path_segments())


async def _test_get_commits_percent_encodes_owner_and_repo_path_segments() -> None:
    # Security review (CHAOS-2807 CS6): owner/repo are untrusted path segments
    # -- a slash, a query-string character, or a dot-segment must be
    # percent-encoded so they cannot escape the ``/repos/{owner}/{repo}/commits``
    # template (path traversal / route confusion / query injection).
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[])

    client = _client(httpx.MockTransport(handler))

    await client.get_commits("acme/evil", "widgets?x=1", max_commits=None)
    await client.close()

    assert len(seen) == 1
    raw_path = seen[0].url.raw_path.decode()
    assert raw_path.startswith("/repos/acme%2Fevil/widgets%3Fx%3D1/commits?")
    # The injected ``?x=1`` must NOT surface as a real query parameter --
    # only the client's own params (per_page) are present.
    assert seen[0].url.params.get("x") is None
    assert seen[0].url.params["per_page"] == "100"


def test_get_commit_file_stats_percent_encodes_path_segments() -> None:
    asyncio.run(_test_get_commit_file_stats_percent_encodes_path_segments())


async def _test_get_commit_file_stats_percent_encodes_path_segments() -> None:
    # Security review (CHAOS-2807 CS6): sha (and owner/repo) are untrusted
    # path segments -- a dot-segment/slash combination must be percent-encoded
    # so it cannot traverse out of ``/repos/{owner}/{repo}/commits/{sha}``.
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"files": []})

    client = _client(httpx.MockTransport(handler))

    await client.get_commit_file_stats("acme", "widgets", "abc/../secret")
    await client.close()

    assert len(seen) == 1
    raw_path = seen[0].url.raw_path.decode()
    assert raw_path == "/repos/acme/widgets/commits/abc%2F..%2Fsecret"
    # Decoding the wire path collapses back to the literal, untraversed value --
    # proof the encoded form never let ``..`` act as a real path segment.
    assert seen[0].url.path == "/repos/acme/widgets/commits/abc/../secret"
