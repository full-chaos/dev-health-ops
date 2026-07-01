"""
GitHub provider implementation conforming to the Provider contract.

This wraps the GitHub client and normalization logic without changing
the underlying ingestion behavior.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.connectors.exceptions import RateLimitException
from dev_health_ops.models.ai_attribution import AIAttributionRecord
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
    ProviderBatch,
    ProviderCapabilities,
    ProviderWithClient,
)
from dev_health_ops.providers.github.client import (
    GitHubGraphQLComment,
    GitHubGraphQLEvent,
    GitHubWorkClient,
)
from dev_health_ops.providers.normalize_common import to_utc as _to_utc
from dev_health_ops.providers.utils import env_flag as _env_flag
from dev_health_ops.providers.utils import env_int

logger = logging.getLogger(__name__)


# Max PR timeline events fetched per PR via GraphQL. Only MERGED/CLOSED/
# REOPENED items are requested, so this is effectively unbounded in practice;
# mirrors the prior REST iter_issue_events(limit=1000) intent.
_PR_EVENTS_LIMIT = 1000


class GitHubProvider(ProviderWithClient[GitHubWorkClient]):
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
    client_cls = GitHubWorkClient

    def __init__(
        self,
        *,
        status_mapping: Any | None = None,
        identity: Any | None = None,
        client: GitHubWorkClient | None = None,
    ) -> None:
        super().__init__(
            status_mapping=status_mapping,
            identity=identity,
            client=client,
        )

    def _validate_ctx(self, ctx: IngestionContext) -> None:
        if not ctx.repo:
            raise ValueError("GitHub provider requires ctx.repo (owner/repo)")
        parts = ctx.repo.split("/")
        if len(parts) != 2:
            raise ValueError(f"Invalid repo format: {ctx.repo} (expected owner/repo)")

    def _ingest_with_client(
        self, *, client: GitHubWorkClient, ctx: IngestionContext
    ) -> ProviderBatch:
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
        from dev_health_ops.providers.github.normalize import (
            detect_github_reopen_events,
            detect_pr_attributions,
            enrich_work_item_with_priority,
            extract_github_comment_dependencies,
            extract_github_dependencies,
            github_comment_to_interaction_event,
            github_issue_to_work_item,
            github_milestone_to_sprint,
            github_pr_to_work_item,
        )

        # ctx.repo is already validated by _validate_ctx() in the base's ingest()
        assert ctx.repo is not None  # for type checker
        owner, repo = ctx.repo.split("/")

        work_items: list[WorkItem] = []
        transitions: list[WorkItemStatusTransition] = []
        dependencies: list[WorkItemDependency] = []
        reopen_events: list[WorkItemReopenEvent] = []
        interactions: list[WorkItemInteractionEvent] = []
        sprints: list[Sprint] = []
        ai_attributions: list[AIAttributionRecord] = []
        _wi_opts = ctx.work_item_options
        include_issues = (
            True if _wi_opts.include_issues is None else _wi_opts.include_issues
        )
        include_prs = (
            _env_flag("GITHUB_INCLUDE_PRS", True)
            if _wi_opts.include_pull_requests is None
            else _wi_opts.include_pull_requests
        )
        fetch_comments = (
            _env_flag("GITHUB_FETCH_COMMENTS", True)
            if _wi_opts.fetch_comments is None
            else _wi_opts.fetch_comments
        )
        fetch_milestones = (
            _env_flag("GITHUB_FETCH_MILESTONES", True)
            if _wi_opts.fetch_milestones is None
            else _wi_opts.fetch_milestones
        )

        comments_limit = env_int("GITHUB_COMMENTS_LIMIT", 500)

        sprint_cache: dict[str, Sprint] = {}
        repo_full_name = f"{owner}/{repo}"

        # Determine time window
        since: datetime | None = None
        if ctx.window.updated_since:
            since = _to_utc(ctx.window.updated_since)
        until: datetime | None = None
        if ctx.window.active_until:
            until = _to_utc(ctx.window.active_until)

        def within_active_window(item: Any) -> bool:
            if until is None:
                return True
            updated_at = _to_utc(getattr(item, "updated_at", None))
            return updated_at is None or updated_at <= until

        # Cap how many items we page through to find the active window. With no
        # caller limit we leave it unbounded (None) UNLESS the env var is set.
        # env_int falls back to its default only for a missing/empty/invalid
        # value, so a present-but-empty/invalid var here resolves to 0, which
        # _iter_with_limit treats as "scan nothing". Set a positive integer to
        # cap, or leave the var unset to keep historical scans unbounded.
        active_window_scan_limit: int | None = None
        if until is not None:
            if ctx.limit is not None:
                active_window_scan_limit = env_int(
                    "GITHUB_ACTIVE_WINDOW_SCAN_LIMIT", max(ctx.limit * 10, 100)
                )
            elif "GITHUB_ACTIVE_WINDOW_SCAN_LIMIT" in os.environ:
                active_window_scan_limit = env_int("GITHUB_ACTIVE_WINDOW_SCAN_LIMIT", 0)

        logger.info(
            "GitHub: fetching work items from %s (since=%s, until=%s)",
            repo_full_name,
            since,
            until,
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
            except RateLimitException:
                raise
            except Exception as exc:
                logger.warning(
                    "GitHub: failed to fetch milestones for %s: %s", repo_full_name, exc
                )

        # Fetch issues
        try:
            issues_iter = (
                client.iter_issues(
                    owner=owner,
                    repo=repo,
                    state="all",
                    since=since,
                    until=until,
                    limit=ctx.limit,
                    scan_limit=active_window_scan_limit,
                )
                if include_issues
                else ()
            )
            for issue in issues_iter:
                # The client already bounds the window (server `since` + `skip`/
                # scan_limit pagination); re-checking here keeps correctness
                # independent of client internals and is the enforcement point
                # for clients/mocks that don't filter. Intentional, not redundant.
                if not within_active_window(issue):
                    continue
                if ctx.limit is not None and fetched_count >= ctx.limit:
                    break

                # Get events for transitions and reopen detection
                events = list(client.iter_issue_events(issue, limit=1000))

                wi, wi_transitions = github_issue_to_work_item(
                    issue=issue,
                    repo_full_name=repo_full_name,
                    repo_id=ctx.repo_id,
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
                    except RateLimitException:
                        raise
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

        if ctx.limit is not None and fetched_count >= ctx.limit:
            include_prs = False

        # Fetch pull requests
        if include_prs:
            remaining_limit = None
            if ctx.limit is not None:
                remaining_limit = ctx.limit - fetched_count

            try:
                prs = list(
                    client.iter_pull_requests(
                        owner=owner,
                        repo=repo,
                        state="all",
                        since=since,
                        until=until,
                        limit=remaining_limit,
                        scan_limit=active_window_scan_limit,
                    )
                )
                # Belt-and-suspenders: iter_pull_requests already applies the
                # `until` skip + `limit`, so in production these are no-ops. We
                # re-apply so the window holds regardless of the client impl and
                # so tests with non-paginating mock clients stay correct.
                if until is not None:
                    prs = [pr for pr in prs if within_active_window(pr)]
                    if ctx.limit is not None:
                        prs = prs[: ctx.limit - fetched_count]
            except RateLimitException:
                raise
            except Exception as exc:
                logger.warning(
                    "GitHub: failed to fetch PRs from %s: %s", repo_full_name, exc
                )
            else:
                # Batch PR timeline events and comments via GraphQL (one query
                # per <=50 PRs) instead of a per-PR REST issue-events call.
                # The per-PR REST events fetch exhausted the GitHub App
                # installation's REST primary rate limit (403). Events drive
                # status transitions + reopen detection; comments drive
                # interaction events + the Linear linkback dependency capture.
                pr_comments_by_number: dict[int, tuple[GitHubGraphQLComment, ...]] = {}
                pr_events_by_number: dict[int, tuple[GitHubGraphQLEvent, ...]] = {}
                try:
                    for payload in client.iter_pr_social_data_batch(
                        owner=owner,
                        repo=repo,
                        prs=prs,
                        comments_limit=comments_limit if fetch_comments else 0,
                        review_comments_limit=0,
                        reviews_limit=0,
                        events_limit=_PR_EVENTS_LIMIT,
                    ):
                        pr_events_by_number[payload.number] = payload.events
                        if fetch_comments:
                            pr_comments_by_number[payload.number] = (
                                payload.issue_comments
                            )
                except RateLimitException:
                    raise
                except Exception as exc:
                    logger.warning(
                        "GitHub: failed to fetch PR social data from %s: %s",
                        repo_full_name,
                        exc,
                    )

                try:
                    for pr in prs:
                        if ctx.limit is not None and fetched_count >= ctx.limit:
                            break

                        # Events for transitions/reopen detection, from the batch.
                        events = list(
                            pr_events_by_number.get(
                                int(getattr(pr, "number", 0) or 0), ()
                            )
                        )

                        wi, wi_transitions = github_pr_to_work_item(
                            pr=pr,
                            repo_full_name=repo_full_name,
                            repo_id=ctx.repo_id,
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

                        # Detect AI attribution signals for this PR.
                        # Each signal is converted to a full AIAttributionRecord
                        # using subject context from this ingestion pass.
                        pr_signals = detect_pr_attributions(pr=pr)
                        if pr_signals:
                            if ctx.org_id is None:
                                raise ValueError(
                                    "GitHub AI attribution requires ctx.org_id"
                                )
                            _observed = _to_utc(getattr(pr, "created_at", None))
                            _observed = _observed or datetime.now(timezone.utc)
                            # CHAOS-2396: subject_id must be the BARE PR number, not
                            # the prefixed work_item_id ('ghpr:{repo}#{n}'). The AI
                            # governance loader and the ai_impact/ai_detector readers
                            # join ai_attribution.subject_id = toString(pr.number)
                            # (scoped by repo_id), and GitLab already writes the bare
                            # iid. Writing the prefixed id made every GitHub PR
                            # attribution miss the join, so GitHub orgs got zero AI
                            # governance/coverage. repo_id (below) disambiguates the
                            # same PR number across repos.
                            _pr_number = str(int(getattr(pr, "number", 0) or 0))
                            for _sig in pr_signals:
                                ai_attributions.append(
                                    AIAttributionRecord.from_signal(
                                        _sig,
                                        org_id=ctx.org_id,
                                        provider="github",
                                        subject_type="pull_request",
                                        subject_id=_pr_number,
                                        repo_id=ctx.repo_id,
                                        observed_at=_observed,
                                    )
                                )
                            logger.debug(
                                "GitHub: detected %d AI attribution signal(s) for %s",
                                len(pr_signals),
                                wi.work_item_id,
                            )

                        # Fetch comments for interactions
                        if fetch_comments:
                            try:
                                pr_number = int(getattr(pr, "number", 0) or 0)
                                comments = pr_comments_by_number.get(pr_number, ())
                                for comment in comments:
                                    event = github_comment_to_interaction_event(
                                        comment=comment,
                                        work_item_id=wi.work_item_id,
                                        identity=self.identity,
                                    )
                                    if event:
                                        interactions.append(event)
                                # Secondary link capture: the Linear integration
                                # bot's linkback comment when the PR body/branch
                                # carries no reference. Only that bot actor is
                                # trusted (see extract_github_comment_dependencies).
                                dependencies.extend(
                                    extract_github_comment_dependencies(
                                        work_item_id=wi.work_item_id,
                                        comments=[
                                            (
                                                getattr(c, "body", None),
                                                getattr(
                                                    getattr(c, "user", None),
                                                    "login",
                                                    None,
                                                ),
                                            )
                                            for c in comments
                                        ],
                                    )
                                )
                            except Exception as exc:
                                logger.debug(
                                    "GitHub: failed to fetch comments for PR %s: %s",
                                    wi.work_item_id,
                                    exc,
                                )

                        fetched_count += 1
                except Exception as exc:
                    logger.warning(
                        "GitHub: failed to process PRs from %s: %s",
                        repo_full_name,
                        exc,
                    )
        logger.info(
            "GitHub: fetched %d work items (%d issues, %d PRs) from %s",
            len(work_items),
            sum(1 for w in work_items if w.work_item_id.startswith("gh:")),
            sum(1 for w in work_items if w.work_item_id.startswith("ghpr:")),
            repo_full_name,
        )
        if ai_attributions:
            logger.info(
                "GitHub: emitting %d AI attribution record(s) from %s",
                len(ai_attributions),
                repo_full_name,
            )

        from dev_health_ops.providers.usage import (
            PROVIDER_USAGE_OBSERVATION_KEY,
            drain_provider_usage,
        )

        usage_observations = drain_provider_usage(client)
        observations: dict[str, Any] = {}
        if usage_observations:
            # Emit the provider-neutral key (CHAOS-2754) alongside the legacy
            # github_usage key, which is pinned by tests and the admin schema and
            # must stay intact.
            observations["github_usage"] = usage_observations
            observations[PROVIDER_USAGE_OBSERVATION_KEY] = usage_observations

        return ProviderBatch(
            work_items=work_items,
            status_transitions=transitions,
            dependencies=dependencies,
            interactions=interactions,
            sprints=sprints,
            reopen_events=reopen_events,
            ai_attributions=ai_attributions,
            observations=observations,
        )
