"""
Linear provider implementation conforming to the Provider contract.

This wraps the Linear client and normalization logic for work item ingestion.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
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


class LinearProvider(Provider):
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

    def __init__(
        self,
        *,
        status_mapping: Optional[StatusMapping] = None,
        identity: Optional[IdentityResolver] = None,
    ) -> None:
        """
        Initialize the Linear provider.

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
        Ingest work items from Linear within the given context.

        The ctx.repo field should contain the Linear team key (e.g., "ENG").
        If not provided, all accessible teams will be synced.

        Uses environment variables for authentication:
        - LINEAR_API_KEY: Linear API key

        Optional env vars:
        - LINEAR_FETCH_COMMENTS: whether to fetch comments (default: true)
        - LINEAR_FETCH_HISTORY: whether to fetch issue history (default: true)
        - LINEAR_FETCH_CYCLES: whether to fetch cycles as sprints (default: true)
        - LINEAR_COMMENTS_LIMIT: max comments per issue (default: 100)
        """
        from dev_health_ops.providers.linear.client import LinearClient
        from dev_health_ops.providers.linear.normalize import (
            detect_linear_reopen_events,
            linear_comment_to_interaction_event,
            linear_cycle_to_sprint,
            linear_issue_to_work_item,
        )

        client = LinearClient.from_env()

        work_items: List[WorkItem] = []
        transitions: List[WorkItemStatusTransition] = []
        reopen_events: List[WorkItemReopenEvent] = []
        interactions: List[WorkItemInteractionEvent] = []
        sprints: List[Sprint] = []

        fetch_comments = _env_flag("LINEAR_FETCH_COMMENTS", True)
        fetch_history = _env_flag("LINEAR_FETCH_HISTORY", True)
        fetch_cycles = _env_flag("LINEAR_FETCH_CYCLES", True)

        raw_comments_limit = os.getenv("LINEAR_COMMENTS_LIMIT")
        comments_limit = 100
        if raw_comments_limit is not None:
            try:
                comments_limit = int(raw_comments_limit)
            except ValueError:
                logger.warning(
                    "Invalid LINEAR_COMMENTS_LIMIT value %r; falling back to 100",
                    raw_comments_limit,
                )

        cycle_cache: Dict[str, Sprint] = {}

        updated_after: Optional[datetime] = None
        if ctx.window.updated_since:
            updated_after = _to_utc(ctx.window.updated_since)

        team_key = ctx.repo
        teams_to_sync: List[Dict] = []

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

        for team in teams_to_sync:
            team_id = team.get("id")
            team_key_str = team.get("key", "")

            if not team_id:
                continue

            logger.info("Linear: fetching issues for team %s", team_key_str)

            if fetch_cycles:
                try:
                    for cycle in client.iter_cycles(team_id=team_id):
                        sprint = linear_cycle_to_sprint(cycle)
                        cycle_cache[sprint.sprint_id] = sprint
                        sprints.append(sprint)
                except Exception as exc:
                    logger.warning(
                        "Linear: failed to fetch cycles for team %s: %s",
                        team_key_str,
                        exc,
                    )

            try:
                for issue in client.iter_issues(
                    team_keys=[team_key_str] if team_key_str else None,
                    updated_after=updated_after,
                ):
                    if ctx.limit is not None and fetched_count >= ctx.limit:
                        break

                    issue_id = issue.get("id")
                    if not issue_id:
                        continue

                    history: List[Dict] = []
                    if fetch_history:
                        try:
                            history = client.get_issue_history(issue_id)
                        except Exception as exc:
                            logger.debug(
                                "Linear: failed to fetch history for issue %s: %s",
                                issue.get("identifier"),
                                exc,
                            )

                    wi, wi_transitions = linear_issue_to_work_item(
                        issue=issue,
                        status_mapping=self.status_mapping,
                        identity=self.identity,
                        history=history,
                    )

                    work_items.append(wi)
                    transitions.extend(wi_transitions)

                    if history:
                        reopen_events.extend(
                            detect_linear_reopen_events(
                                work_item_id=wi.work_item_id,
                                history=history,
                                identity=self.identity,
                            )
                        )

                    # Fetch comments for interactions (if enabled)
                    if fetch_comments:
                        try:
                            comments = client.get_issue_comments(
                                issue_id,
                                limit=comments_limit,
                            )
                            for comment in comments:
                                event = linear_comment_to_interaction_event(
                                    comment=comment,
                                    work_item_id=wi.work_item_id,
                                    identity=self.identity,
                                )
                                if event:
                                    interactions.append(event)
                        except Exception as exc:
                            logger.debug(
                                "Linear: failed to fetch comments for issue %s: %s",
                                issue.get("identifier"),
                                exc,
                            )

                    fetched_count += 1

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
            "Linear: fetched %d work items, %d cycles from %d team(s)",
            len(work_items),
            len(sprints),
            len(teams_to_sync),
        )

        return ProviderBatch(
            work_items=work_items,
            status_transitions=transitions,
            dependencies=[],
            interactions=interactions,
            sprints=sprints,
            reopen_events=reopen_events,
        )
