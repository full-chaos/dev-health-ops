"""
Linear provider implementation conforming to the Provider contract.

This wraps the Linear client and normalization logic for work item ingestion.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

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
from dev_health_ops.providers.linear.client import LinearClient
from dev_health_ops.providers.normalize_common import to_utc as _to_utc
from dev_health_ops.providers.utils import env_flag as _env_flag

logger = logging.getLogger(__name__)


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    return (
        row.get(key, default) if isinstance(row, dict) else getattr(row, key, default)
    )


def _linear_reference_team(row: Any, team_key: str) -> dict[str, Any] | None:
    row_provider = str(_row_value(row, "provider", "") or "")
    if row_provider and row_provider != "linear":
        return None
    row_id = str(_row_value(row, "id", "") or "").strip()
    row_name = str(_row_value(row, "name", "") or "").strip()
    native_key = str(_row_value(row, "native_team_key", "") or "").strip()
    project_keys = [
        str(key).strip() for key in (_row_value(row, "project_keys", []) or [])
    ]
    candidates = {
        value for value in [row_id, row_name, native_key, *project_keys] if value
    }
    if team_key not in candidates:
        return None
    return {
        "id": row_id or team_key,
        "key": native_key or team_key,
        "name": row_name or team_key,
    }


def _linear_sprints_from_reference(
    rows: Sequence[Sprint] | None, *, team_key: str | None
) -> list[Sprint]:
    if not team_key:
        return []
    return [
        sprint
        for sprint in rows or []
        if sprint.provider == "linear" and sprint.native_team_key == team_key
    ]


class LinearProvider(ProviderWithClient[LinearClient]):
    """
    Provider implementation for Linear.

    Capabilities:
    - work_items: yes (issues)
    - status_transitions: yes (via issue history)
    - dependencies: yes (PR/MR links via issue attachments; not blocking relations)
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
        dependencies=True,  # PR/MR links via issue attachments (not blocking relations)
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
            merged.dependencies.extend(batch.dependencies)
        return merged

    def iter_ingest(self, ctx: IngestionContext) -> Iterable[ProviderBatch]:
        """Yield one ProviderBatch per GraphQL page for memory-bounded ingestion.

        Linear overrides ``iter_ingest`` (not ``_ingest_with_client``) because
        its ingestion is naturally streaming: each GraphQL page yields one
        ProviderBatch for memory-bounded processing.
        """
        from dev_health_ops.providers.linear.normalize import (
            detect_linear_reopen_events,
            extract_linear_dependencies,
            linear_comment_to_interaction_event,
            linear_cycle_to_sprint,
            linear_issue_to_work_item,
        )

        client = self._make_client()

        def _issue_dependencies(
            issue: dict, work_item_id: str
        ) -> list[WorkItemDependency]:
            # PR/MR -> issue edges from linked attachments. If the issue's
            # attachment page was truncated, fetch the full set so a link past
            # the first page is not silently dropped — best-effort: a transient
            # fetch error must not abort the whole sync, so fall back to the
            # first page already in hand.
            source: dict = issue
            att_page = issue.get("attachments") or {}
            if (att_page.get("pageInfo") or {}).get("hasNextPage") and issue.get("id"):
                try:
                    full = client.get_issue_attachments(str(issue["id"]))
                    source = {"attachments": {"nodes": full}}
                except Exception as exc:
                    logger.warning(
                        "Linear: failed to fetch full attachments for %s; "
                        "using first page only (%s)",
                        work_item_id,
                        exc,
                    )
            return extract_linear_dependencies(issue=source, work_item_id=work_item_id)

        fetch_comments = _env_flag("LINEAR_FETCH_COMMENTS", True)
        fetch_history = _env_flag("LINEAR_FETCH_HISTORY", True)
        fetch_cycles = _env_flag("LINEAR_FETCH_CYCLES", True)

        updated_after: datetime | None = None
        if ctx.window.updated_since:
            updated_after = _to_utc(ctx.window.updated_since)
        updated_before: datetime | None = None
        if ctx.window.active_until:
            updated_before = _to_utc(ctx.window.active_until)

        yielded_batch = False
        team_key = ctx.repo
        cycle_cache: dict[str, Sprint] = {
            sprint.sprint_id: sprint
            for sprint in _linear_sprints_from_reference(
                ctx.reference_sprints,
                team_key=team_key,
            )
        }
        teams_to_sync: list[dict] = []

        if team_key:
            api_team: dict[str, Any] | None = None
            for row in ctx.reference_teams or []:
                team = _linear_reference_team(row, team_key)
                if team:
                    teams_to_sync.append(team)
                    break
            if not teams_to_sync:
                api_team = client.get_team_by_key(team_key)
                team = api_team
                if not isinstance(api_team, dict) or not api_team:
                    if ctx.reference_teams is None:
                        for legacy_team in client.iter_teams():
                            if (
                                legacy_team.get("key") == team_key
                                or legacy_team.get("name") == team_key
                            ):
                                team = legacy_team
                                break
                    if not isinstance(team, dict) or not team:
                        raise ValueError(f"Linear team '{team_key}' not found")
                assert isinstance(team, dict)
                resolved_team: dict[str, Any] = team
                teams_to_sync.append(resolved_team)
            if not cycle_cache and fetch_cycles:
                fetched_sprints = []
                if api_team is None:
                    api_team = client.get_team_by_key(team_key)
                if api_team and api_team.get("id"):
                    for cycle in client.iter_cycles(team_id=str(api_team["id"])):
                        sprint = replace(
                            linear_cycle_to_sprint(cycle), native_team_key=team_key
                        )
                        cycle_cache[sprint.sprint_id] = sprint
                        fetched_sprints.append(sprint)
                if fetched_sprints and ctx.reference_sink is not None:
                    ctx.reference_sink.write_sprints(
                        [
                            Sprint(
                                provider=sprint.provider,
                                sprint_id=sprint.sprint_id,
                                name=sprint.name,
                                state=sprint.state,
                                started_at=sprint.started_at,
                                ended_at=sprint.ended_at,
                                completed_at=sprint.completed_at,
                                native_team_key=team_key,
                                last_synced=sprint.last_synced,
                                org_id=str(ctx.org_id or ""),
                            )
                            for sprint in fetched_sprints
                        ]
                    )
        else:
            teams_to_sync = list(client.iter_teams())
            if fetch_cycles:
                for team in teams_to_sync:
                    team_id = team.get("id")
                    if not team_id:
                        continue
                    native_team_key = str(team.get("key") or "") or None
                    for cycle in client.iter_cycles(team_id=team_id):
                        sprint = replace(
                            linear_cycle_to_sprint(cycle),
                            native_team_key=native_team_key,
                        )
                        cycle_cache.setdefault(sprint.sprint_id, sprint)
                if cycle_cache:
                    yield ProviderBatch(sprints=list(cycle_cache.values()))
                    yielded_batch = True

        if team_key and cycle_cache:
            yield ProviderBatch(sprints=list(cycle_cache.values()))
            yielded_batch = True

        logger.info(
            "Linear: syncing %d team(s) (updated_after=%s)",
            len(teams_to_sync),
            updated_after,
        )

        fetched_count = 0
        fetched_transitions = 0
        fetched_interactions = 0

        for team in teams_to_sync:
            team_id = team.get("id")
            team_key_str = team.get("key", "")

            if not team_id:
                continue

            logger.info("Linear: fetching issues for team %s", team_key_str)

            try:
                pages_seen = False
                for issues_page in client.iter_issues_pages(
                    team_keys=[team_key_str] if team_key_str else None,
                    updated_after=updated_after,
                    updated_before=updated_before,
                ):
                    pages_seen = True
                    page_items: list[WorkItem] = []
                    page_transitions: list[WorkItemStatusTransition] = []
                    page_reopen_events: list[WorkItemReopenEvent] = []
                    page_interactions: list[WorkItemInteractionEvent] = []
                    page_dependencies: list[WorkItemDependency] = []

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
                        page_dependencies.extend(
                            _issue_dependencies(issue, wi.work_item_id)
                        )

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
                        or page_dependencies
                    ):
                        yield ProviderBatch(
                            work_items=page_items,
                            status_transitions=page_transitions,
                            interactions=page_interactions,
                            reopen_events=page_reopen_events,
                            dependencies=page_dependencies,
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
                            updated_before=updated_before,
                            limit=ctx.limit,
                        )
                    )
                    if fallback_issues:
                        page_items = []
                        page_transitions = []
                        page_reopen_events = []
                        page_interactions = []
                        page_dependencies = []
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
                            page_dependencies.extend(
                                _issue_dependencies(issue, wi.work_item_id)
                            )
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
                            or page_dependencies
                        ):
                            yield ProviderBatch(
                                work_items=page_items,
                                status_transitions=page_transitions,
                                interactions=page_interactions,
                                reopen_events=page_reopen_events,
                                dependencies=page_dependencies,
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
