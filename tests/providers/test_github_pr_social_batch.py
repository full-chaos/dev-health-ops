from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient


def _client() -> tuple[GitHubWorkClient, MagicMock, MagicMock]:
    gate = MagicMock()
    with (
        patch("github.Github"),
        patch(
            "dev_health_ops.providers.github.client.GitHubGraphQLClient"
        ) as graphql_cls,
    ):
        client = GitHubWorkClient(auth=GitHubAuth(token="token"), gate=gate)
    return client, graphql_cls.return_value, gate


def test_work_client_uses_pygithub_token_auth_without_internal_retry() -> None:
    gate = MagicMock()
    with (
        patch("github.Github") as github_cls,
        patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
    ):
        GitHubWorkClient(auth=GitHubAuth(token="token"), gate=gate)

    _, kwargs = github_cls.call_args
    assert kwargs["auth"].__class__.__name__ == "Token"
    assert kwargs["retry"] is None
    assert "login_or_token" not in kwargs


def test_work_client_uses_pygithub_app_installation_auth_without_internal_retry() -> (
    None
):
    gate = MagicMock()
    with (
        patch("github.Github") as github_cls,
        patch("dev_health_ops.providers.github.client.GitHubGraphQLClient"),
        patch(
            "dev_health_ops.providers.github.client.GitHubAppTokenProvider"
        ) as token_provider_cls,
    ):
        token_provider_cls.return_value.get_token.return_value = "installation-token"
        GitHubWorkClient(
            auth=GitHubAuth(
                app_id="123",
                private_key="not-a-real-private-key",
                installation_id="456",
            ),
            gate=gate,
        )

    _, kwargs = github_cls.call_args
    assert kwargs["auth"].__class__.__name__ == "AppInstallationAuth"
    assert kwargs["auth"].installation_id == 456
    assert kwargs["retry"] is None
    assert "login_or_token" not in kwargs


def test_work_client_get_repo_classifies_primary_rate_limit_403(monkeypatch) -> None:
    from github.GithubException import GithubException

    client, _graphql, gate = _client()
    cast(Any, client.github).get_repo = MagicMock(
        side_effect=GithubException(
            403,
            {"message": "API rate limit exceeded for installation ID 141773132."},
            {
                "x-ratelimit-remaining": "0",
                "x-ratelimit-reset": "105",
                "x-github-request-id": "REQ:5",
            },
        )
    )
    monkeypatch.setattr("dev_health_ops.providers.github.client.time.time", lambda: 100)

    with pytest.raises(RateLimitException) as exc_info:
        client.get_repo(owner="full-chaos", repo="dev-health-web")

    message = str(exc_info.value)
    assert "GitHub rate limit" in message
    assert "GET /repos/full-chaos/dev-health-web" in message
    assert "x-ratelimit-remaining" in message
    assert "REQ:5" in message
    assert exc_info.value.retry_after_seconds == pytest.approx(5.0)
    gate.penalize.assert_called_once_with(5.0)


def test_work_client_get_repo_classifies_permission_403() -> None:
    from github.GithubException import GithubException

    client, _graphql, gate = _client()
    cast(Any, client.github).get_repo = MagicMock(
        side_effect=GithubException(
            403,
            {"message": "Resource not accessible by integration"},
            {"x-github-request-id": "REQ:9"},
        )
    )

    with pytest.raises(AuthenticationException) as exc_info:
        client.get_repo(owner="full-chaos", repo="private-repo")

    message = str(exc_info.value)
    assert "GitHub 403" in message
    assert "GET /repos/full-chaos/private-repo" in message
    assert "REQ:9" in message
    gate.penalize.assert_called_once_with(None)


def _lazy_rate_limit_iterable() -> Iterable[object]:
    from github.GithubException import GithubException

    raise GithubException(
        403,
        {"message": "API rate limit exceeded for installation ID 141773132."},
        {
            "x-ratelimit-remaining": "0",
            "x-ratelimit-reset": "105",
            "x-github-request-id": "REQ:lazy",
        },
    )
    yield object()


def test_work_client_iter_issues_classifies_lazy_primary_rate_limit_403(
    monkeypatch,
) -> None:
    client, _graphql, gate = _client()
    repo = MagicMock()
    repo.get_issues.return_value = _lazy_rate_limit_iterable()
    cast(Any, client.github).get_repo.return_value = repo
    monkeypatch.setattr("dev_health_ops.providers.github.client.time.time", lambda: 100)

    with pytest.raises(RateLimitException) as exc_info:
        list(client.iter_issues(owner="full-chaos", repo="dev-health-web"))

    message = str(exc_info.value)
    assert "GitHub rate limit" in message
    assert "GET /repos/full-chaos/dev-health-web/issues" in message
    assert "REQ:lazy" in message
    assert exc_info.value.retry_after_seconds == pytest.approx(5.0)
    gate.penalize.assert_called_once_with(5.0)


def test_work_client_iter_milestones_classifies_lazy_primary_rate_limit_403(
    monkeypatch,
) -> None:
    client, _graphql, gate = _client()
    repo = MagicMock()
    repo.get_milestones.return_value = _lazy_rate_limit_iterable()
    cast(Any, client.github).get_repo.return_value = repo
    monkeypatch.setattr("dev_health_ops.providers.github.client.time.time", lambda: 100)

    with pytest.raises(RateLimitException) as exc_info:
        list(client.iter_repo_milestones(owner="full-chaos", repo="dev-health-ops"))

    message = str(exc_info.value)
    assert "GitHub rate limit" in message
    assert "GET /repos/full-chaos/dev-health-ops/milestones" in message
    assert "REQ:lazy" in message
    assert exc_info.value.retry_after_seconds == pytest.approx(5.0)
    gate.penalize.assert_called_once_with(5.0)


def test_work_client_iter_pull_requests_classifies_lazy_primary_rate_limit_403(
    monkeypatch,
) -> None:
    client, _graphql, gate = _client()
    repo = MagicMock()
    repo.get_pulls.return_value = _lazy_rate_limit_iterable()
    cast(Any, client.github).get_repo.return_value = repo
    monkeypatch.setattr("dev_health_ops.providers.github.client.time.time", lambda: 100)

    with pytest.raises(RateLimitException) as exc_info:
        list(client.iter_pull_requests(owner="full-chaos", repo="dev-health-web"))

    message = str(exc_info.value)
    assert "GET /repos/full-chaos/dev-health-web/pulls" in message
    assert "REQ:lazy" in message
    gate.penalize.assert_called_once_with(5.0)


def test_work_client_iter_issue_events_classifies_lazy_primary_rate_limit_403(
    monkeypatch,
) -> None:
    client, _graphql, gate = _client()
    issue = MagicMock()
    issue.number = 123
    issue.get_events.return_value = _lazy_rate_limit_iterable()
    monkeypatch.setattr("dev_health_ops.providers.github.client.time.time", lambda: 100)

    with pytest.raises(RateLimitException) as exc_info:
        list(client.iter_issue_events(issue))

    message = str(exc_info.value)
    assert "GET issue events for #123" in message
    assert "REQ:lazy" in message
    gate.penalize.assert_called_once_with(5.0)


def test_work_client_iter_issue_comments_classifies_lazy_primary_rate_limit_403(
    monkeypatch,
) -> None:
    client, _graphql, gate = _client()
    issue = MagicMock()
    issue.number = 456
    issue.get_comments.return_value = _lazy_rate_limit_iterable()
    monkeypatch.setattr("dev_health_ops.providers.github.client.time.time", lambda: 100)

    with pytest.raises(RateLimitException) as exc_info:
        list(client.iter_issue_comments(issue))

    message = str(exc_info.value)
    assert "GET issue comments for #456" in message
    assert "REQ:lazy" in message
    gate.penalize.assert_called_once_with(5.0)


def test_work_client_iter_pr_review_comments_classifies_lazy_primary_rate_limit_403(
    monkeypatch,
) -> None:
    client, _graphql, gate = _client()
    pr = MagicMock()
    pr.number = 789
    pr.get_review_comments.return_value = _lazy_rate_limit_iterable()
    monkeypatch.setattr("dev_health_ops.providers.github.client.time.time", lambda: 100)

    with pytest.raises(RateLimitException) as exc_info:
        list(client.iter_pr_review_comments(pr))

    message = str(exc_info.value)
    assert "GET pull request review comments for #789" in message
    assert "REQ:lazy" in message
    gate.penalize.assert_called_once_with(5.0)


def _pr(number: int) -> MagicMock:
    pr = MagicMock()
    pr.number = number
    pr.get_comments.side_effect = AssertionError("REST comments should not be used")
    pr.get_review_comments.side_effect = AssertionError(
        "REST review comments should not be used"
    )
    return pr


def _comment_node(comment_id: int, body: str = "comment") -> dict[str, object]:
    return {
        "id": f"IC_{comment_id}",
        "databaseId": comment_id,
        "fullDatabaseId": str(comment_id),
        "body": body,
        "createdAt": "2026-01-02T03:04:05Z",
        "author": {"login": "octocat"},
    }


def _review_node(review_id: int) -> dict[str, object]:
    return {
        "id": f"PRR_{review_id}",
        "databaseId": review_id,
        "fullDatabaseId": str(review_id),
        "body": "LGTM",
        "state": "APPROVED",
        "submittedAt": "2026-01-03T03:04:05Z",
        "url": f"https://github.test/review/{review_id}",
        "author": {"login": "reviewer"},
        "comments": {
            "nodes": [_comment_node(review_id + 1000, "review comment")],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        },
    }


def test_batched_pr_comments_fetches_n_prs_with_one_graphql_call() -> None:
    client, graphql, _gate = _client()
    prs = [_pr(number) for number in range(1, 6)]
    graphql.query.return_value = {
        "repository": {
            f"pr{idx}": {
                "number": number,
                "comments": {
                    "nodes": [_comment_node(number)],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }
            for idx, number in enumerate(range(1, 6))
        }
    }

    result = dict(
        client.iter_pr_comments_batch(owner="owner", repo="repo", prs=prs, limit=100)
    )

    assert sorted(result) == [1, 2, 3, 4, 5]
    assert result[1][0].id == 1
    assert result[1][0].body == "comment"
    assert getattr(result[1][0].user, "login") == "octocat"
    assert result[1][0].created_at == datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert graphql.query.call_count == 1
    for pr in prs:
        pr.get_comments.assert_not_called()


def test_batched_pr_social_data_paginates_nested_connections() -> None:
    client, graphql, _gate = _client()
    graphql.query.side_effect = [
        {
            "repository": {
                "pr0": {
                    "number": 1,
                    "comments": {
                        "nodes": [_comment_node(1)],
                        "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                    },
                    "reviews": {
                        "nodes": [_review_node(10)],
                        "pageInfo": {"hasNextPage": True, "endCursor": "r1"},
                    },
                }
            }
        },
        {
            "repository": {
                "pr0": {
                    "number": 1,
                    "comments": {
                        "nodes": [_comment_node(2)],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        },
        {
            "repository": {
                "pr0": {
                    "number": 1,
                    "reviews": {
                        "nodes": [_review_node(11)],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        },
    ]

    payload = next(
        iter(
            client.iter_pr_social_data_batch(
                owner="owner",
                repo="repo",
                prs=[_pr(1)],
                comments_limit=10,
                review_comments_limit=10,
                reviews_limit=10,
            )
        )
    )

    assert [comment.id for comment in payload.issue_comments] == [1, 2]
    assert [review.id for review in payload.reviews] == [10, 11]
    assert [comment.body for comment in payload.review_comments] == [
        "review comment",
        "review comment",
    ]
    assert graphql.query.call_count == 3


def test_batched_pr_social_data_handles_empty_connections() -> None:
    client, graphql, _gate = _client()
    graphql.query.return_value = {
        "repository": {
            "pr0": {
                "number": 1,
                "comments": {"nodes": [], "pageInfo": {"hasNextPage": False}},
                "reviews": {"nodes": [], "pageInfo": {"hasNextPage": False}},
            }
        }
    }

    payload = next(
        iter(client.iter_pr_social_data_batch(owner="owner", repo="repo", prs=[_pr(1)]))
    )

    assert payload.issue_comments == ()
    assert payload.review_comments == ()
    assert payload.reviews == ()


def test_batched_pr_social_data_surfaces_graphql_errors() -> None:
    client, graphql, gate = _client()
    graphql.query.side_effect = APIException("GraphQL errors: bad field")

    with pytest.raises(APIException):
        list(client.iter_pr_comments_batch(owner="owner", repo="repo", prs=[_pr(1)]))

    gate.penalize.assert_called_once()


def test_batched_pr_social_data_penalizes_retry_after_on_rate_limit() -> None:
    client, graphql, gate = _client()
    graphql.query.side_effect = RateLimitException("limited", retry_after_seconds=42.0)

    with pytest.raises(RateLimitException):
        list(client.iter_pr_comments_batch(owner="owner", repo="repo", prs=[_pr(1)]))

    gate.penalize.assert_called_once_with(42.0)


def test_batched_pr_reviews_preserve_rest_consumed_fields() -> None:
    client, graphql, _gate = _client()
    graphql.query.return_value = {
        "repository": {
            "pr0": {
                "number": 1,
                "reviews": {
                    "nodes": [_review_node(10)],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }
        }
    }

    reviews = dict(
        client.iter_pr_reviews_batch(owner="owner", repo="repo", prs=[_pr(1)], limit=10)
    )[1]

    assert reviews[0].id == 10
    assert reviews[0].reviewer == "reviewer"
    assert reviews[0].state == "APPROVED"
    assert reviews[0].submitted_at == datetime(2026, 1, 3, 3, 4, 5, tzinfo=timezone.utc)
    assert reviews[0].body == "LGTM"
    assert reviews[0].url == "https://github.test/review/10"


def _timeline_node(typename: str, created_at: str, login: str) -> dict[str, object]:
    return {
        "__typename": typename,
        "createdAt": created_at,
        "actor": {"login": login},
    }


def test_batched_pr_events_fetched_and_mapped() -> None:
    """PR timeline events (merged/closed/reopened) are batched into the same
    GraphQL call as comments and mapped to the event shape the normalizers
    consume (event string, created_at, actor.login). This replaces the per-PR
    REST iter_issue_events call that exhausted the installation rate limit."""
    client, graphql, _gate = _client()
    prs = [_pr(number) for number in range(1, 4)]
    graphql.query.return_value = {
        "repository": {
            f"pr{idx}": {
                "number": number,
                "timelineItems": {
                    "nodes": [
                        _timeline_node("MergedEvent", "2026-02-01T00:00:00Z", "merger"),
                        _timeline_node("ClosedEvent", "2026-02-02T00:00:00Z", "closer"),
                        _timeline_node(
                            "ReopenedEvent", "2026-02-03T00:00:00Z", "reopener"
                        ),
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }
            for idx, number in enumerate(range(1, 4))
        }
    }

    payloads = {
        payload.number: payload
        for payload in client.iter_pr_social_data_batch(
            owner="owner", repo="repo", prs=prs, events_limit=1000
        )
    }

    # One GraphQL call covers all three PRs' events (no per-PR REST fan-out).
    assert graphql.query.call_count == 1
    query_str = graphql.query.call_args.args[0]
    assert "timelineItems" in query_str
    for pr in prs:
        pr.get_issue_events.assert_not_called()

    events = payloads[1].events
    assert [e.event for e in events] == ["merged", "closed", "reopened"]
    assert events[0].created_at == datetime(2026, 2, 1, tzinfo=timezone.utc)
    assert getattr(events[0].actor, "login") == "merger"
    assert getattr(events[2].actor, "login") == "reopener"


def test_batched_pr_events_omitted_when_events_limit_zero() -> None:
    """Existing comment/review batches default events_limit=0, so the
    timelineItems connection must not be added to their query (no extra cost)."""
    client, graphql, _gate = _client()
    graphql.query.return_value = {
        "repository": {
            "pr0": {
                "number": 1,
                "comments": {
                    "nodes": [_comment_node(1)],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }
        }
    }

    payload = next(
        iter(
            client.iter_pr_comments_batch(
                owner="owner", repo="repo", prs=[_pr(1)], limit=100
            )
        )
    )

    assert payload  # consumed
    query_str = graphql.query.call_args.args[0]
    assert "timelineItems" not in query_str
