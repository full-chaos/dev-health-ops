"""
GitHub provider implementation conforming to the Provider contract.

This wraps the GitHub client and normalization logic without changing
the underlying ingestion behavior.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.base import (
    IngestionContext,
    Provider,
    ProviderBatch,
    ProviderCapabilities,
)
from dev_health_ops.providers.identity import IdentityResolver, load_identity_resolver
from dev_health_ops.providers.status_mapping import StatusMapping, load_status_mapping

logger = logging.getLogger(__name__)


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


class GitHubProvider(Provider):
    """
    Provider implementation for GitHub.

    Capabilities:
    - work_items: yes (issues + pull requests)
    - status_transitions: yes (via issue events)
    - dependencies: yes (via body text parsing)
    - interactions: yes (via comments)
    - sprints: yes (via milestones)
    - reopen_events: yes (via issue events)
    - priority: yes (via labels)
    """

    name = "github"
    capabilities = ProviderCapabilities(
        work_items=True,
        status_transitions=True,
        dependencies=True,
        interactions=True,
        sprints=True,
        reopen_events=True,
        priority=True,
    )

    def __init__(
        self,
        *,
        status_mapping: Optional[StatusMapping] = None,
        identity: Optional[IdentityResolver] = None,
    ) -> None:
        """
        Initialize the GitHub provider.

        Args:
            status_mapping: optional StatusMapping instance (loads default if None)
            identity: optional IdentityResolver instance (loads default if None)
        """
        self._status_mapping = status_mapping
        self._identity = identity

    @property
    def status_mapping(self) -> StatusMapping:
        if self._status_mapping is None:
            self._status_mapping = load_status_mapping()
        return self._status_mapping

    @property
    def identity(self) -> IdentityResolver:
        if self._identity is None:
            self._identity = load_identity_resolver()
        return self._identity

    def ingest(self, ctx: IngestionContext) -> ProviderBatch:
        """
        Ingest work items from GitHub within the given context.

        Uses environment variables for authentication:
        - GITHUB_TOKEN: GitHub personal access token or app token
        - GITHUB_BASE_URL: GitHub Enterprise base URL (optional)

        Optional env vars:
        - GITHUB_INCLUDE_PRS: whether to include pull requests (default: true)
        - GITHUB_FETCH_COMMENTS: whether to fetch comments (default: true)
        - GITHUB_COMMENTS_LIMIT: max comments per item (default: 500)
        - GITHUB_FETCH_MILESTONES: whether to fetch milestones (default: true)
        """
        from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient
        from dev_health_ops.providers.github.normalize import (
            detect_github_reopen_events,
            enrich_work_item_with_priority,
            extract_github_dependencies,
            github_comment_to_interaction_event,
            github_issue_to_work_item,
            github_milestone_to_sprint,
            github_pr_to_work_item,
        )

        if not ctx.repo:
            raise ValueError("GitHub provider requires ctx.repo (owner/repo)")

        # Parse owner/repo from ctx.repo
        parts = ctx.repo.split("/")
        if len(parts) != 2:
            raise ValueError(f"Invalid repo format: {ctx.repo} (expected owner/repo)")
        owner, repo = parts

        # Get auth from environment
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            raise ValueError("GITHUB_TOKEN environment variable is required")

        base_url = os.getenv("GITHUB_BASE_URL")
        auth = GitHubAuth(token=token, base_url=base_url)
        client = GitHubWorkClient(auth=auth)

        work_items: List[WorkItem] = []
        transitions: List[WorkItemStatusTransition] = []
        dependencies: List[WorkItemDependency] = []
        reopen_events: List[WorkItemReopenEvent] = []
        interactions: List[WorkItemInteractionEvent] = []
        sprints: List[Sprint] = []

        include_prs = _env_flag("GITHUB_INCLUDE_PRS", True)
        fetch_comments = _env_flag("GITHUB_FETCH_COMMENTS", True)
        fetch_milestones = _env_flag("GITHUB_FETCH_MILESTONES", True)

        raw_comments_limit = os.getenv("GITHUB_COMMENTS_LIMIT")
        comments_limit = 500
        if raw_comments_limit is not None:
            try:
                comments_limit = int(raw_comments_limit)
            except ValueError:
                logger.warning(
                    "Invalid GITHUB_COMMENTS_LIMIT value %r; falling back to 500",
                    raw_comments_limit,
                )

        sprint_cache: Dict[str, Sprint] = {}
        repo_full_name = f"{owner}/{repo}"

        # Determine time window
        since: Optional[datetime] = None
        if ctx.window.updated_since:
            since = _to_utc(ctx.window.updated_since)

        logger.info(
            "GitHub: fetching work items from %s (since=%s)",
            repo_full_name,
            since,
        )

        fetched_count = 0

        # Fetch milestones first to populate sprint cache
        if fetch_milestones:
            try:
                for ms in client.iter_repo_milestones(
                    owner=owner, repo=repo, state="all"
                ):
                    sprint = github_milestone_to_sprint(
                        milestone=ms, repo_full_name=repo_full_name
                    )
                    sprint_cache[sprint.sprint_id] = sprint
                    sprints.append(sprint)
            except Exception as exc:
                logger.warning(
                    "GitHub: failed to fetch milestones for %s: %s", repo_full_name, exc
                )

        # Fetch issues
        try:
            for issue in client.iter_issues(
                owner=owner,
                repo=repo,
                state="all",
                since=since,
                limit=ctx.limit,
            ):
                if ctx.limit is not None and fetched_count >= ctx.limit:
                    break

                # Get events for transitions and reopen detection
                events = list(client.iter_issue_events(issue, limit=1000))

                wi, wi_transitions = github_issue_to_work_item(
                    issue=issue,
                    repo_full_name=repo_full_name,
                    repo_id=None,
                    status_mapping=self.status_mapping,
                    identity=self.identity,
                    events=events,
                )

                # Enrich with priority from labels
                wi = enrich_work_item_with_priority(wi, wi.labels)

                work_items.append(wi)
                transitions.extend(wi_transitions)

                # Detect reopen events
                reopen_events.extend(
                    detect_github_reopen_events(
                        work_item_id=wi.work_item_id,
                        events=events,
                        identity=self.identity,
                    )
                )

                # Extract dependencies from body
                dependencies.extend(
                    extract_github_dependencies(
                        work_item_id=wi.work_item_id,
                        issue_or_pr=issue,
                        repo_full_name=repo_full_name,
                    )
                )

                # Fetch comments for interactions
                if fetch_comments:
                    try:
                        for comment in client.iter_issue_comments(
                            issue, limit=comments_limit
                        ):
                            event = github_comment_to_interaction_event(
                                comment=comment,
                                work_item_id=wi.work_item_id,
                                identity=self.identity,
                            )
                            if event:
                                interactions.append(event)
                    except Exception as exc:
                        logger.debug(
                            "GitHub: failed to fetch comments for issue %s: %s",
                            wi.work_item_id,
                            exc,
                        )

                fetched_count += 1

        except Exception as exc:
            logger.error(
                "GitHub: failed to fetch issues from %s: %s", repo_full_name, exc
            )
            raise

        # Fetch pull requests
        if include_prs:
            try:
                remaining_limit = None
                if ctx.limit is not None:
                    remaining_limit = ctx.limit - fetched_count
                    if remaining_limit <= 0:
                        remaining_limit = None

                for pr in client.iter_pull_requests(
                    owner=owner,
                    repo=repo,
                    state="all",
                    limit=remaining_limit,
                ):
                    if ctx.limit is not None and fetched_count >= ctx.limit:
                        break

                    # Get events for transitions and reopen detection
                    events = list(client.iter_issue_events(pr, limit=1000))

                    wi, wi_transitions = github_pr_to_work_item(
                        pr=pr,
                        repo_full_name=repo_full_name,
                        repo_id=None,
                        status_mapping=self.status_mapping,
                        identity=self.identity,
                        events=events,
                    )

                    # Enrich with priority from labels
                    wi = enrich_work_item_with_priority(wi, wi.labels)

                    work_items.append(wi)
                    transitions.extend(wi_transitions)

                    # Detect reopen events
                    reopen_events.extend(
                        detect_github_reopen_events(
                            work_item_id=wi.work_item_id,
                            events=events,
                            identity=self.identity,
                        )
                    )

                    # Extract dependencies from body
                    dependencies.extend(
                        extract_github_dependencies(
                            work_item_id=wi.work_item_id,
                            issue_or_pr=pr,
                            repo_full_name=repo_full_name,
                        )
                    )

                    # Fetch comments for interactions
                    if fetch_comments:
                        try:
                            for comment in client.iter_pr_comments(
                                pr, limit=comments_limit
                            ):
                                event = github_comment_to_interaction_event(
                                    comment=comment,
                                    work_item_id=wi.work_item_id,
                                    identity=self.identity,
                                )
                                if event:
                                    interactions.append(event)
                        except Exception as exc:
                            logger.debug(
                                "GitHub: failed to fetch comments for PR %s: %s",
                                wi.work_item_id,
                                exc,
                            )

                    fetched_count += 1

            except Exception as exc:
                logger.warning(
                    "GitHub: failed to fetch PRs from %s: %s", repo_full_name, exc
                )

        logger.info(
            "GitHub: fetched %d work items (%d issues, %d PRs) from %s",
            len(work_items),
            sum(1 for w in work_items if w.work_item_id.startswith("gh:")),
            sum(1 for w in work_items if w.work_item_id.startswith("ghpr:")),
            repo_full_name,
        )

        return ProviderBatch(
            work_items=work_items,
            status_transitions=transitions,
            dependencies=dependencies,
            interactions=interactions,
            sprints=sprints,
            reopen_events=reopen_events,
        )
