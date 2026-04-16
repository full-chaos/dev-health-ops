"""
Linear provider implementation conforming to the Provider contract.

This wraps the Linear client and normalization logic for work item ingestion.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime

from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
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
from dev_health_ops.providers.linear.client import LinearClient
from dev_health_ops.providers.normalize_common import to_utc as _to_utc
from dev_health_ops.providers.utils import env_flag as _env_flag

logger = logging.getLogger(__name__)


class LinearProvider(ProviderWithClient[LinearClient]):
    """
    Provider implementation for Linear.

    Capabilities:
    - work_items: yes (issues)
    - status_transitions: yes (via issue history)
    - dependencies: no (Linear doesn't expose blocking relationships in API)
    - interactions: yes (via comments)
    - sprints: yes (via cycles)
    - reopen_events: yes (via issue history)
    - priority: yes (native priority field)

    Environment variables:
    - LINEAR_API_KEY: Linear API key (required)
    - LINEAR_FETCH_COMMENTS: whether to fetch comments (default: true)
    - LINEAR_FETCH_HISTORY: whether to fetch issue history for transitions (default: true)
    - LINEAR_FETCH_CYCLES: whether to fetch cycles as sprints (default: true)
    - LINEAR_COMMENTS_LIMIT: max comments per issue (default: 100)
    """

    name = "linear"
    capabilities = ProviderCapabilities(
        work_items=True,
        status_transitions=True,
        dependencies=False,  # Linear API doesn't expose blocking relations well
        interactions=True,
        sprints=True,
        reopen_events=True,
        priority=True,
    )
    client_cls = LinearClient

    def ingest(self, ctx: IngestionContext) -> ProviderBatch:
        """Backward-compatible single-batch ingest that merges iter_ingest()."""
        merged = ProviderBatch()
        for batch in self.iter_ingest(ctx):
            merged.work_items.extend(batch.work_items)
            merged.status_transitions.extend(batch.status_transitions)
            merged.reopen_events.extend(batch.reopen_events)
            merged.interactions.extend(batch.interactions)
            merged.sprints.extend(batch.sprints)
        return merged

    def iter_ingest(self, ctx: IngestionContext) -> Iterable[ProviderBatch]:
        """Yield one ProviderBatch per GraphQL page for memory-bounded ingestion.

        Linear overrides ``iter_ingest`` (not ``_ingest_with_client``) because
        its ingestion is naturally streaming: each GraphQL page yields one
        ProviderBatch for memory-bounded processing.
        """
        from dev_health_ops.providers.linear.normalize import (
            detect_linear_reopen_events,
            linear_comment_to_interaction_event,
            linear_cycle_to_sprint,
            linear_issue_to_work_item,
        )

        client = self._make_client()

        fetch_comments = _env_flag("LINEAR_FETCH_COMMENTS", True)
        fetch_history = _env_flag("LINEAR_FETCH_HISTORY", True)
        fetch_cycles = _env_flag("LINEAR_FETCH_CYCLES", True)

        cycle_cache: dict[str, Sprint] = {}

        updated_after: datetime | None = None
        if ctx.window.updated_since:
            updated_after = _to_utc(ctx.window.updated_since)

        team_key = ctx.repo
        teams_to_sync: list[dict] = []

        if team_key:
            all_teams = list(client.iter_teams())
            for team in all_teams:
                if team.get("key") == team_key or team.get("name") == team_key:
                    teams_to_sync.append(team)
                    break
            if not teams_to_sync:
                raise ValueError(
                    f"Linear team '{team_key}' not found. "
                    f"Available teams: {[t.get('key') for t in all_teams]}"
                )
        else:
            teams_to_sync = list(client.iter_teams())

        logger.info(
            "Linear: syncing %d team(s) (updated_after=%s)",
            len(teams_to_sync),
            updated_after,
        )

        fetched_count = 0
        fetched_transitions = 0
        fetched_interactions = 0
        yielded_batch = False

        for team in teams_to_sync:
            team_id = team.get("id")
            team_key_str = team.get("key", "")

            if not team_id:
                continue

            logger.info("Linear: fetching issues for team %s", team_key_str)

            batch_sprints: list[Sprint] = []
            if fetch_cycles:
                try:
                    for cycle in client.iter_cycles(team_id=team_id):
                        sprint = linear_cycle_to_sprint(cycle)
                        if sprint.sprint_id not in cycle_cache:
                            cycle_cache[sprint.sprint_id] = sprint
                            batch_sprints.append(sprint)
                except Exception as exc:
                    logger.warning(
                        "Linear: failed to fetch cycles for team %s: %s",
                        team_key_str,
                        exc,
                    )

            if batch_sprints:
                yield ProviderBatch(sprints=batch_sprints)
                yielded_batch = True

            try:
                pages_seen = False
                for issues_page in client.iter_issues_pages(
                    team_keys=[team_key_str] if team_key_str else None,
                    updated_after=updated_after,
                ):
                    pages_seen = True
                    page_items: list[WorkItem] = []
                    page_transitions: list[WorkItemStatusTransition] = []
                    page_reopen_events: list[WorkItemReopenEvent] = []
                    page_interactions: list[WorkItemInteractionEvent] = []

                    for issue in issues_page:
                        if ctx.limit is not None and fetched_count >= ctx.limit:
                            break
                        if not issue.get("id"):
                            continue

                        history: list[dict] = []
                        if fetch_history:
                            history = issue.get("history", {}).get("nodes", [])

                        wi, wi_transitions = linear_issue_to_work_item(
                            issue=issue,
                            status_mapping=self.status_mapping,
                            identity=self.identity,
                            history=history,
                        )

                        page_items.append(wi)
                        page_transitions.extend(wi_transitions)

                        if history:
                            page_reopen_events.extend(
                                detect_linear_reopen_events(
                                    work_item_id=wi.work_item_id,
                                    history=history,
                                    identity=self.identity,
                                )
                            )

                        if fetch_comments:
                            comments = issue.get("comments", {}).get("nodes", [])
                            for comment in comments:
                                event = linear_comment_to_interaction_event(
                                    comment=comment,
                                    work_item_id=wi.work_item_id,
                                    identity=self.identity,
                                )
                                if event:
                                    page_interactions.append(event)

                        fetched_count += 1

                    if (
                        page_items
                        or page_transitions
                        or page_reopen_events
                        or page_interactions
                    ):
                        yield ProviderBatch(
                            work_items=page_items,
                            status_transitions=page_transitions,
                            interactions=page_interactions,
                            reopen_events=page_reopen_events,
                        )
                        yielded_batch = True
                        fetched_transitions += len(page_transitions)
                        fetched_interactions += len(page_interactions)

                    if ctx.limit is not None and fetched_count >= ctx.limit:
                        break

                if not pages_seen:
                    fallback_issues = list(
                        client.iter_issues(
                            team_keys=[team_key_str] if team_key_str else None,
                            updated_after=updated_after,
                            limit=ctx.limit,
                        )
                    )
                    if fallback_issues:
                        page_items = []
                        page_transitions = []
                        page_reopen_events = []
                        page_interactions = []
                        for issue in fallback_issues:
                            if ctx.limit is not None and fetched_count >= ctx.limit:
                                break
                            if not issue.get("id"):
                                continue

                            history = (
                                issue.get("history", {}).get("nodes", [])
                                if fetch_history
                                else []
                            )
                            wi, wi_transitions = linear_issue_to_work_item(
                                issue=issue,
                                status_mapping=self.status_mapping,
                                identity=self.identity,
                                history=history,
                            )
                            page_items.append(wi)
                            page_transitions.extend(wi_transitions)
                            if history:
                                page_reopen_events.extend(
                                    detect_linear_reopen_events(
                                        work_item_id=wi.work_item_id,
                                        history=history,
                                        identity=self.identity,
                                    )
                                )
                            if fetch_comments:
                                for comment in issue.get("comments", {}).get(
                                    "nodes", []
                                ):
                                    event = linear_comment_to_interaction_event(
                                        comment=comment,
                                        work_item_id=wi.work_item_id,
                                        identity=self.identity,
                                    )
                                    if event:
                                        page_interactions.append(event)
                            fetched_count += 1

                        if (
                            page_items
                            or page_transitions
                            or page_reopen_events
                            or page_interactions
                        ):
                            yield ProviderBatch(
                                work_items=page_items,
                                status_transitions=page_transitions,
                                interactions=page_interactions,
                                reopen_events=page_reopen_events,
                            )
                            yielded_batch = True
                            fetched_transitions += len(page_transitions)
                            fetched_interactions += len(page_interactions)

            except Exception as exc:
                logger.error(
                    "Linear: failed to fetch issues for team %s: %s",
                    team_key_str,
                    exc,
                )
                raise

            if ctx.limit is not None and fetched_count >= ctx.limit:
                break

        logger.info(
            "Linear: fetched %d work items, %d transitions, %d interactions, %d cycles from %d team(s)",
            fetched_count,
            fetched_transitions,
            fetched_interactions,
            len(cycle_cache),
            len(teams_to_sync),
        )

        if not yielded_batch:
            yield ProviderBatch()
