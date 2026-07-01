from __future__ import annotations

from datetime import datetime

import strawberry


@strawberry.type
class PullRequestReview:
    review_id: str
    reviewer: str
    state: str
    submitted_at: datetime


@strawberry.type
class PullRequestCommit:
    hash: str
    message: str | None = None
    author_name: str | None = None
    author_email: str | None = None
    author_when: datetime | None = None
    confidence: float | None = None
    provenance: str | None = None
    evidence: str | None = None


@strawberry.type
class PullRequestIssueLink:
    work_item_id: str
    confidence: float
    provenance: str
    evidence: str


@strawberry.type
class PullRequestDetail:
    id: strawberry.ID
    org_id: str
    repo_id: strawberry.ID
    repo_name: str | None = None
    number: int
    title: str | None = None
    body: str | None = None
    state: str | None = None
    author_name: str | None = None
    author_email: str | None = None
    created_at: datetime
    merged_at: datetime | None = None
    closed_at: datetime | None = None
    head_branch: str | None = None
    base_branch: str | None = None
    additions: int | None = None
    deletions: int | None = None
    changed_files: int | None = None
    first_review_at: datetime | None = None
    first_comment_at: datetime | None = None
    changes_requested_count: int = 0
    reviews_count: int = 0
    comments_count: int = 0
    reviews: list[PullRequestReview] = strawberry.field(default_factory=list)
    commits: list[PullRequestCommit] = strawberry.field(default_factory=list)
    linked_issues: list[PullRequestIssueLink] = strawberry.field(default_factory=list)
