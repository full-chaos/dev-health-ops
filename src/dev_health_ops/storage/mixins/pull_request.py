from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from dev_health_ops.models.git import GitPullRequest, GitPullRequestReview


class PullRequestMixin:
    async def insert_git_pull_requests(self, pr_data: List[GitPullRequest]) -> None:
        if not pr_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in pr_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "number": item.get("number"),
                    "title": item.get("title"),
                    "body": item.get("body"),
                    "state": item.get("state"),
                    "author_name": item.get("author_name"),
                    "author_email": item.get("author_email"),
                    "created_at": item.get("created_at"),
                    "merged_at": item.get("merged_at"),
                    "closed_at": item.get("closed_at"),
                    "head_branch": item.get("head_branch"),
                    "base_branch": item.get("base_branch"),
                    "additions": item.get("additions"),
                    "deletions": item.get("deletions"),
                    "changed_files": item.get("changed_files"),
                    "first_review_at": item.get("first_review_at"),
                    "first_comment_at": item.get("first_comment_at"),
                    "changes_requested_count": item.get("changes_requested_count", 0),
                    "reviews_count": item.get("reviews_count", 0),
                    "comments_count": item.get("comments_count", 0),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "number": getattr(item, "number"),
                    "title": getattr(item, "title"),
                    "body": getattr(item, "body", None),
                    "state": getattr(item, "state"),
                    "author_name": getattr(item, "author_name"),
                    "author_email": getattr(item, "author_email"),
                    "created_at": getattr(item, "created_at"),
                    "merged_at": getattr(item, "merged_at"),
                    "closed_at": getattr(item, "closed_at"),
                    "head_branch": getattr(item, "head_branch"),
                    "base_branch": getattr(item, "base_branch"),
                    "additions": getattr(item, "additions", None),
                    "deletions": getattr(item, "deletions", None),
                    "changed_files": getattr(item, "changed_files", None),
                    "first_review_at": getattr(item, "first_review_at", None),
                    "first_comment_at": getattr(item, "first_comment_at", None),
                    "changes_requested_count": getattr(
                        item, "changes_requested_count", 0
                    ),
                    "reviews_count": getattr(item, "reviews_count", 0),
                    "comments_count": getattr(item, "comments_count", 0),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitPullRequest,
            rows,
            conflict_columns=["repo_id", "number"],
            update_columns=[
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
                "last_synced",
            ],
        )

    async def insert_git_pull_request_reviews(
        self, review_data: List[GitPullRequestReview]
    ) -> None:
        if not review_data:
            return
        synced_at_default = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []
        for item in review_data:
            if isinstance(item, dict):
                row = {
                    "repo_id": item.get("repo_id"),
                    "number": item.get("number"),
                    "review_id": item.get("review_id"),
                    "reviewer": item.get("reviewer"),
                    "state": item.get("state"),
                    "submitted_at": item.get("submitted_at"),
                    "last_synced": item.get("last_synced") or synced_at_default,
                }
            else:
                row = {
                    "repo_id": getattr(item, "repo_id"),
                    "number": getattr(item, "number"),
                    "review_id": getattr(item, "review_id"),
                    "reviewer": getattr(item, "reviewer"),
                    "state": getattr(item, "state"),
                    "submitted_at": getattr(item, "submitted_at"),
                    "last_synced": getattr(item, "last_synced", None)
                    or synced_at_default,
                }
            rows.append(row)

        await self._upsert_many(
            GitPullRequestReview,
            rows,
            conflict_columns=["repo_id", "number", "review_id"],
            update_columns=[
                "reviewer",
                "state",
                "submitted_at",
                "last_synced",
            ],
        )
