from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.graphql.context import GraphQLContext
from dev_health_ops.api.graphql.resolvers.pr import parse_pr_id, resolve_pr

ORG_ID = "org-pr-detail-test"
REPO_ID = "11111111-1111-1111-1111-111111111111"
PR_ID = f"{REPO_ID}#pr42"
NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _ctx() -> GraphQLContext:
    ctx = GraphQLContext(org_id=ORG_ID, db_url="clickhouse://localhost:8123/d")
    ctx.client = MagicMock(spec=["query"])
    return ctx


def _qresult(columns: list[str], rows: list[list[Any]]) -> Any:
    result = MagicMock()
    result.column_names = columns
    result.result_rows = rows
    return result


@pytest.mark.asyncio
async def test_pr_resolver_returns_pr_reviews_commits_and_provenance() -> None:
    ctx = _ctx()
    ctx.client.query.side_effect = [
        _qresult(
            [
                "repo_id",
                "repo_name",
                "number",
                "title",
                "body",
                "state",
                "author_name",
                "author_email",
                "created_at",
                "merged_at",
                "closed_at",
                "head_branch",
                "base_branch",
                "additions",
                "deletions",
                "changed_files",
                "first_review_at",
                "first_comment_at",
                "changes_requested_count",
                "reviews_count",
                "comments_count",
            ],
            [
                [
                    REPO_ID,
                    "full-chaos/dev-health",
                    42,
                    "Wire PR detail",
                    "Body",
                    "merged",
                    "Ada",
                    "ada@example.com",
                    NOW,
                    NOW,
                    None,
                    "feature/pr-detail",
                    "main",
                    12,
                    3,
                    4,
                    NOW,
                    NOW,
                    1,
                    2,
                    5,
                ]
            ],
        ),
        _qresult(
            ["review_id", "reviewer", "state", "submitted_at"],
            [["r1", "reviewer@example.com", "APPROVED", NOW]],
        ),
        _qresult(
            [
                "hash",
                "message",
                "author_name",
                "author_email",
                "author_when",
                "confidence",
                "provenance",
                "evidence",
            ],
            [
                [
                    "abc123",
                    "commit msg",
                    "Ada",
                    "ada@example.com",
                    NOW,
                    0.99,
                    "native",
                    "api_pr_commits",
                ]
            ],
        ),
        _qresult(
            ["work_item_id", "confidence", "provenance", "evidence"],
            [["jira:CHAOS-2387", 0.95, "native", "linked issue"]],
        ),
    ]

    result = await resolve_pr(ctx, PR_ID)

    assert result is not None
    assert result.id == PR_ID
    assert result.repo_id == REPO_ID
    assert result.title == "Wire PR detail"
    assert result.reviews[0].reviewer == "reviewer@example.com"
    assert result.commits[0].hash == "abc123"
    assert result.commits[0].provenance == "native"
    assert result.linked_issues[0].work_item_id == "jira:CHAOS-2387"
    assert result.linked_issues[0].provenance == "native"


@pytest.mark.asyncio
async def test_pr_resolver_returns_none_for_missing_pr() -> None:
    ctx = _ctx()
    ctx.client.query.return_value = _qresult([], [])

    result = await resolve_pr(ctx, PR_ID)

    assert result is None
    assert ctx.client.query.call_count == 1


@pytest.mark.asyncio
async def test_pr_resolver_returns_none_for_invalid_id_without_querying() -> None:
    ctx = _ctx()

    result = await resolve_pr(ctx, "not-a-pr-id")

    assert result is None
    ctx.client.query.assert_not_called()


def test_parse_pr_id_accepts_stable_work_graph_format() -> None:
    parsed = parse_pr_id(PR_ID)

    assert parsed is not None
    assert parsed.repo_id == REPO_ID
    assert parsed.number == 42
