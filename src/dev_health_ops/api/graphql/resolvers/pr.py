from __future__ import annotations

import logging
import re
from typing import Any, NamedTuple

import strawberry

from dev_health_ops.api.queries.client import query_dicts

from ..authz import require_org_id
from ..context import GraphQLContext
from ..models.pr import (
    PullRequestCommit,
    PullRequestDetail,
    PullRequestIssueLink,
    PullRequestReview,
)

logger = logging.getLogger(__name__)

_PR_ID_RE = re.compile(r"^(?P<repo>[0-9a-fA-F-]{36})(?:#pr|#|:|/pr/)(?P<number>\d+)$")


class _ParsedPrId(NamedTuple):
    repo_id: str
    number: int


def parse_pr_id(value: str) -> _ParsedPrId | None:
    match = _PR_ID_RE.match(value.strip())
    if match is None:
        return None
    return _ParsedPrId(
        repo_id=match.group("repo").lower(), number=int(match.group("number"))
    )


def _require_client(context: GraphQLContext) -> Any:
    if context.client is None:
        raise RuntimeError("Database client not available for PullRequest resolver")
    return context.client


async def _fetch_pr_row(
    client: Any, *, org_id: str, repo_id: str, number: int
) -> dict[str, Any] | None:
    rows = await query_dicts(
        client,
        """
        SELECT
            toString(pr.repo_id) AS repo_id,
            anyLast(repos.repo) AS repo_name,
            pr.number AS number,
            pr.title AS title,
            pr.body AS body,
            pr.state AS state,
            pr.author_name AS author_name,
            pr.author_email AS author_email,
            pr.created_at AS created_at,
            pr.merged_at AS merged_at,
            pr.closed_at AS closed_at,
            pr.head_branch AS head_branch,
            pr.base_branch AS base_branch,
            pr.additions AS additions,
            pr.deletions AS deletions,
            pr.changed_files AS changed_files,
            pr.first_review_at AS first_review_at,
            pr.first_comment_at AS first_comment_at,
            pr.changes_requested_count AS changes_requested_count,
            pr.reviews_count AS reviews_count,
            pr.comments_count AS comments_count
        FROM git_pull_requests FINAL AS pr
        LEFT JOIN repos FINAL AS repos
            ON repos.org_id = pr.org_id AND repos.id = pr.repo_id
        WHERE pr.org_id = {org_id:String}
          AND toString(pr.repo_id) = {repo_id:String}
          AND pr.number = {number:UInt32}
        GROUP BY
            pr.repo_id,
            pr.number,
            pr.title,
            pr.body,
            pr.state,
            pr.author_name,
            pr.author_email,
            pr.created_at,
            pr.merged_at,
            pr.closed_at,
            pr.head_branch,
            pr.base_branch,
            pr.additions,
            pr.deletions,
            pr.changed_files,
            pr.first_review_at,
            pr.first_comment_at,
            pr.changes_requested_count,
            pr.reviews_count,
            pr.comments_count
        LIMIT 1
        """,
        {"org_id": org_id, "repo_id": repo_id, "number": number},
    )
    return rows[0] if rows else None


async def _fetch_reviews(
    client: Any, *, org_id: str, repo_id: str, number: int
) -> list[PullRequestReview]:
    rows = await query_dicts(
        client,
        """
        SELECT review_id, reviewer, state, submitted_at
        FROM git_pull_request_reviews FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND number = {number:UInt32}
        ORDER BY submitted_at ASC, review_id ASC
        LIMIT 500
        """,
        {"org_id": org_id, "repo_id": repo_id, "number": number},
    )
    return [
        PullRequestReview(
            review_id=str(row.get("review_id") or ""),
            reviewer=str(row.get("reviewer") or ""),
            state=str(row.get("state") or ""),
            submitted_at=row["submitted_at"],
        )
        for row in rows
    ]


async def _fetch_commits(
    client: Any, *, org_id: str, repo_id: str, number: int
) -> list[PullRequestCommit]:
    rows = await query_dicts(
        client,
        """
        SELECT
            link.commit_hash AS hash,
            anyLast(commit.message) AS message,
            anyLast(commit.author_name) AS author_name,
            anyLast(commit.author_email) AS author_email,
            anyLast(commit.author_when) AS author_when,
            argMax(link.confidence, link.last_synced) AS confidence,
            argMax(link.provenance, link.last_synced) AS provenance,
            argMax(link.evidence, link.last_synced) AS evidence
        FROM work_graph_pr_commit FINAL AS link
        LEFT JOIN git_commits FINAL AS commit
            ON commit.org_id = link.org_id
           AND commit.repo_id = link.repo_id
           AND commit.hash = link.commit_hash
        WHERE link.org_id = {org_id:String}
          AND toString(link.repo_id) = {repo_id:String}
          AND link.pr_number = {number:UInt32}
        GROUP BY link.commit_hash
        ORDER BY anyLast(commit.author_when) ASC, link.commit_hash ASC
        LIMIT 500
        """,
        {"org_id": org_id, "repo_id": repo_id, "number": number},
    )
    return [
        PullRequestCommit(
            hash=str(row.get("hash") or ""),
            message=row.get("message"),
            author_name=row.get("author_name"),
            author_email=row.get("author_email"),
            author_when=row.get("author_when"),
            confidence=float(row["confidence"])
            if row.get("confidence") is not None
            else None,
            provenance=row.get("provenance"),
            evidence=row.get("evidence"),
        )
        for row in rows
    ]


async def _fetch_linked_issues(
    client: Any, *, org_id: str, repo_id: str, number: int
) -> list[PullRequestIssueLink]:
    rows = await query_dicts(
        client,
        """
        SELECT
            work_item_id,
            argMax(confidence, last_synced) AS confidence,
            argMax(provenance, last_synced) AS provenance,
            argMax(evidence, last_synced) AS evidence
        FROM work_graph_issue_pr FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND pr_number = {number:UInt32}
        GROUP BY work_item_id
        ORDER BY confidence DESC, work_item_id ASC
        LIMIT 500
        """,
        {"org_id": org_id, "repo_id": repo_id, "number": number},
    )
    return [
        PullRequestIssueLink(
            work_item_id=str(row.get("work_item_id") or ""),
            confidence=float(row.get("confidence") or 0.0),
            provenance=str(row.get("provenance") or ""),
            evidence=str(row.get("evidence") or ""),
        )
        for row in rows
    ]


async def resolve_pr(context: GraphQLContext, id: str) -> PullRequestDetail | None:
    org_id = require_org_id(context)
    parsed = parse_pr_id(id)
    if parsed is None:
        logger.debug("Invalid PR detail id %r", id)
        return None

    client = _require_client(context)
    pr_row = await _fetch_pr_row(
        client, org_id=org_id, repo_id=parsed.repo_id, number=parsed.number
    )
    if pr_row is None:
        return None

    reviews = await _fetch_reviews(
        client, org_id=org_id, repo_id=parsed.repo_id, number=parsed.number
    )
    commits = await _fetch_commits(
        client, org_id=org_id, repo_id=parsed.repo_id, number=parsed.number
    )
    linked_issues = await _fetch_linked_issues(
        client, org_id=org_id, repo_id=parsed.repo_id, number=parsed.number
    )

    return PullRequestDetail(
        id=strawberry.ID(f"{parsed.repo_id}#pr{parsed.number}"),
        org_id=org_id,
        repo_id=strawberry.ID(parsed.repo_id),
        repo_name=pr_row.get("repo_name"),
        number=int(pr_row.get("number") or parsed.number),
        title=pr_row.get("title"),
        body=pr_row.get("body"),
        state=pr_row.get("state"),
        author_name=pr_row.get("author_name"),
        author_email=pr_row.get("author_email"),
        created_at=pr_row["created_at"],
        merged_at=pr_row.get("merged_at"),
        closed_at=pr_row.get("closed_at"),
        head_branch=pr_row.get("head_branch"),
        base_branch=pr_row.get("base_branch"),
        additions=pr_row.get("additions"),
        deletions=pr_row.get("deletions"),
        changed_files=pr_row.get("changed_files"),
        first_review_at=pr_row.get("first_review_at"),
        first_comment_at=pr_row.get("first_comment_at"),
        changes_requested_count=int(pr_row.get("changes_requested_count") or 0),
        reviews_count=int(pr_row.get("reviews_count") or 0),
        comments_count=int(pr_row.get("comments_count") or 0),
        reviews=reviews,
        commits=commits,
        linked_issues=linked_issues,
    )
