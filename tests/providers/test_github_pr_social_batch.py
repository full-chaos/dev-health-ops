from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.connectors.exceptions import APIException, RateLimitException
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
