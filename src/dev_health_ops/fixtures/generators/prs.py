"""Pull request fixture generators (PRs, PR reviews, issue↔PR links)."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from dev_health_ops.fixtures.generators.base import BaseGeneratorMixin
from dev_health_ops.models.ai_attribution import (
    AIAttributionKind,
    AIAttributionRecord,
    AIAttributionSource,
)
from dev_health_ops.models.git import GitPullRequest, GitPullRequestReview
from dev_health_ops.models.work_items import WorkItem


class PrsGeneratorMixin(BaseGeneratorMixin):
    """Generates synthetic pull requests, reviews, and issue↔PR linkage rows."""

    def generate_prs(
        self,
        count: int = 20,
        issue_numbers: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        prs = []
        end_date = datetime.now(timezone.utc)
        issue_numbers = issue_numbers or []
        pr_keywords = [
            "feature",
            "refactor",
            "incident",
            "bug",
            "test",
            "deploy",
            "rollback",
            "cleanup",
            "hotfix",
        ]
        pr_titles = [
            "Implement User Auth",
            "Fix NPE in Service",
            "Refactor DB Layer",
            "Update API Docs",
            "Add Integration Tests",
            "Bump version",
            "Optimize Startup",
            "Remove Legacy Code",
            "Feature X",
            "Fix Bug Y",
            "Cleanup Z",
        ]

        for i in range(1, count + 1):
            author_name, author_email = random.choice(self.repo_authors)
            # PRs created over the last 60 days
            created_at = end_date - timedelta(
                days=random.randint(0, 60), hours=random.randint(0, 23)
            )
            issue_ref = None
            if issue_numbers and random.random() > 0.3:
                issue_ref = random.choice(issue_numbers)

            # Simulated lifecycle
            state = random.choice(["merged", "merged", "merged", "open", "closed"])
            merged_at = None
            closed_at = None

            first_review_at = None
            first_comment_at = None
            reviews_count = 0
            comments_count = random.randint(0, 10)

            if comments_count > 0:
                first_comment_at = created_at + timedelta(
                    minutes=random.randint(5, 120)
                )

            # Review stats
            has_review = random.random() > 0.2
            if has_review:
                first_review_at = created_at + timedelta(hours=random.randint(1, 48))
                reviews_count = random.randint(1, 5)

            if state == "merged":
                merged_at = created_at + timedelta(days=random.randint(1, 7))
                closed_at = merged_at
            elif state == "closed":
                closed_at = created_at + timedelta(days=random.randint(1, 14))

            is_revert = i % 7 == 0
            summary = random.choice(pr_titles)
            keywords = random.sample(pr_keywords, 2)
            if is_revert:
                title = f'Revert "[{keywords[0]}] {summary}"'
                additions = random.randint(2, 20)
                deletions = random.randint(80, 300)
                changed_files = random.randint(2, 8)
            else:
                title = f"[{keywords[0]}] {summary}"
                additions = random.randint(10, 500)
                deletions = random.randint(5, 200)
                changed_files = random.randint(1, 10)
            if issue_ref is not None:
                title = f"{title} (Fixes #{issue_ref})"
            body = (
                f"{summary}.\n\n"
                f"This change includes {keywords[0]} updates and {keywords[1]} coverage.\n"
            )
            if issue_ref is not None:
                body += f"\nFixes #{issue_ref}\n"

            prs.append(
                {
                    "pr": GitPullRequest(
                        repo_id=self.repo_id,
                        number=i,
                        title=title,
                        body=body,
                        state=state,
                        author_name=author_name,
                        author_email=author_email,
                        created_at=created_at,
                        merged_at=merged_at,
                        closed_at=closed_at,
                        head_branch=f"feature/{i}",
                        base_branch="main",
                        additions=additions,
                        deletions=deletions,
                        changed_files=changed_files,
                        first_review_at=first_review_at,
                        first_comment_at=first_comment_at,
                        reviews_count=reviews_count,
                        comments_count=comments_count,
                        changes_requested_count=random.randint(0, 2),
                    ),
                    "reviews": self._generate_pr_reviews(
                        i, first_review_at, reviews_count
                    )
                    if first_review_at
                    else [],
                }
            )
        return prs

    def _generate_pr_reviews(
        self, pr_number: int, first_review_at: datetime, count: int
    ) -> list[GitPullRequestReview]:
        reviews = []
        for i in range(count):
            reviewer_name, reviewer_email = random.choice(self.repo_authors)
            review_time = first_review_at + timedelta(hours=random.randint(0, 24) * i)
            state = (
                "APPROVED"
                if i == count - 1
                else random.choice(["COMMENTED", "CHANGES_REQUESTED", "APPROVED"])
            )
            reviews.append(
                GitPullRequestReview(
                    repo_id=self.repo_id,
                    number=pr_number,
                    review_id=f"rev_{pr_number}_{i}",
                    reviewer=reviewer_email,
                    state=state,
                    submitted_at=review_time,
                )
            )
        return reviews

    def generate_ai_attributions(
        self,
        prs: list[GitPullRequest],
        *,
        org_id: str,
    ) -> list[AIAttributionRecord]:
        """Generate synthetic AI attribution signals for a subset of PRs."""
        if not prs:
            return []

        attribution_variants = (
            (
                AIAttributionKind.AI_ASSISTED,
                AIAttributionSource.PR_LABEL,
                "ai-assisted",
                "Claude Code",
            ),
            (
                AIAttributionKind.AGENT_CREATED,
                AIAttributionSource.BOT_AUTHOR,
                "agent-created",
                "claude-code[bot]",
            ),
            (
                AIAttributionKind.AI_REVIEW,
                AIAttributionSource.PR_BODY,
                "ai-review",
                "Code Review Agent",
            ),
        )
        org_uuid = uuid.UUID(str(org_id))
        records: list[AIAttributionRecord] = []

        for index, pr in enumerate(prs):
            if index % 3 == 2:
                records.append(
                    AIAttributionRecord(
                        org_id=org_uuid,
                        provider=self.provider,
                        subject_type="pull_request",
                        subject_id=str(pr.number),
                        repo_id=pr.repo_id,
                        kind=AIAttributionKind.HUMAN,
                        source=AIAttributionSource.MANUAL,
                        confidence=1.0,
                        actor=pr.author_email,
                        evidence={
                            "source": "synthetic_fixture",
                            "label": "human-authored",
                            "reason": "baseline_for_ai_comparison",
                        },
                        observed_at=pr.merged_at or pr.created_at,
                    )
                )
                continue

            kind, source, label, actor = attribution_variants[
                index % len(attribution_variants)
            ]
            records.append(
                AIAttributionRecord(
                    org_id=org_uuid,
                    provider=self.provider,
                    subject_type="pull_request",
                    subject_id=str(pr.number),
                    repo_id=pr.repo_id,
                    kind=kind,
                    source=source,
                    confidence=0.9,
                    actor=actor,
                    evidence={
                        "source": "synthetic_fixture",
                        "label": label,
                        "tool_name": "claude-code",
                        "model_name": "claude",
                    },
                    observed_at=pr.merged_at or pr.created_at,
                )
            )

        return records

    def generate_issue_pr_links(
        self,
        work_items: list[WorkItem],
        prs: list[GitPullRequest],
        *,
        min_coverage: float = 0.7,
        cluster_size: int = 5,
        org_id: str = "",
    ) -> list[dict[str, Any]]:
        """Generate work_graph_issue_pr rows with isolated clusters for multiple components."""
        if not work_items or not prs:
            return []

        candidates = [wi for wi in work_items if getattr(wi, "work_item_id", None)]
        pr_numbers = [
            int(pr.number) for pr in prs if getattr(pr, "number", None) is not None
        ]
        if not candidates or not pr_numbers:
            return []

        target_count = max(1, int(len(candidates) * float(min_coverage)))
        random.shuffle(candidates)
        linked_items = candidates[:target_count]

        synced_at = datetime.now(timezone.utc)
        links: list[dict[str, Any]] = []

        num_clusters = max(1, len(linked_items) // cluster_size)
        pr_idx = 0

        for cluster_idx in range(num_clusters):
            start = cluster_idx * cluster_size
            end = min(start + cluster_size, len(linked_items))
            cluster_items = linked_items[start:end]

            if not cluster_items:
                continue

            cluster_prs = [pr_numbers[pr_idx % len(pr_numbers)]]
            pr_idx += 1

            if len(pr_numbers) > 1 and random.random() < 0.3:
                second_pr = pr_numbers[pr_idx % len(pr_numbers)]
                if second_pr != cluster_prs[0]:
                    cluster_prs.append(second_pr)
                pr_idx += 1

            for wi in cluster_items:
                links.append(
                    {
                        "repo_id": str(self.repo_id),
                        "work_item_id": str(wi.work_item_id),
                        "pr_number": cluster_prs[0],
                        "confidence": 1.0,
                        "provenance": "native",
                        "evidence": "generated_fixture",
                        "last_synced": synced_at,
                        "org_id": org_id,
                    }
                )
                if len(cluster_prs) > 1 and random.random() < 0.2:
                    links.append(
                        {
                            "repo_id": str(self.repo_id),
                            "work_item_id": str(wi.work_item_id),
                            "pr_number": cluster_prs[1],
                            "confidence": 1.0,
                            "provenance": "native",
                            "evidence": "generated_fixture",
                            "last_synced": synced_at,
                            "org_id": org_id,
                        }
                    )

        return links
