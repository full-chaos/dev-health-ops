"""
GitLab provider implementation conforming to the Provider contract.

This wraps the GitLab client and normalization logic without changing
the underlying ingestion behavior.
"""

from __future__ import annotations

import logging
import os
from dataclasses import replace
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


class GitLabProvider(Provider):
    """
    Provider implementation for GitLab.

    Capabilities:
    - work_items: yes (issues + merge requests)
    - status_transitions: yes (via resource_label_events and resource_state_events)
    - dependencies: yes (via issue links + description parsing)
    - interactions: yes (via notes/comments)
    - sprints: yes (via milestones)
    - reopen_events: yes (via resource_state_events)
    - priority: yes (via labels)
    """

    name = "gitlab"
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
        Initialize the GitLab provider.

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
        Ingest work items from GitLab within the given context.

        Uses environment variables for authentication:
        - GITLAB_TOKEN: GitLab personal access token
        - GITLAB_URL: GitLab instance URL (default: https://gitlab.com)

        Optional env vars:
        - GITLAB_INCLUDE_MRS: whether to include merge requests (default: true)
        - GITLAB_FETCH_NOTES: whether to fetch notes/comments (default: true)
        - GITLAB_NOTES_LIMIT: max notes per item (default: 500)
        - GITLAB_FETCH_LINKS: whether to fetch issue links (default: true)
        - GITLAB_FETCH_MILESTONES: whether to fetch milestones (default: true)
        - GITLAB_FETCH_EPICS: whether to fetch epics (requires Premium/Ultimate, default: true)
        """
        from dev_health_ops.providers.gitlab.client import GitLabWorkClient
        from dev_health_ops.providers.gitlab.normalize import (
            build_epic_id_for_issue,
            detect_gitlab_reopen_events,
            enrich_work_item_with_priority,
            extract_gitlab_dependencies,
            gitlab_epic_to_work_item,
            gitlab_issue_to_work_item,
            gitlab_milestone_to_sprint,
            gitlab_mr_to_work_item,
            gitlab_note_to_interaction_event,
        )

        if not ctx.repo:
            raise ValueError("GitLab provider requires ctx.repo (project path)")

        project_path = ctx.repo
        client = GitLabWorkClient.from_env()

        work_items: List[WorkItem] = []
        transitions: List[WorkItemStatusTransition] = []
        dependencies: List[WorkItemDependency] = []
        reopen_events: List[WorkItemReopenEvent] = []
        interactions: List[WorkItemInteractionEvent] = []
        sprints: List[Sprint] = []

        include_mrs = _env_flag("GITLAB_INCLUDE_MRS", True)
        fetch_notes = _env_flag("GITLAB_FETCH_NOTES", True)
        fetch_links = _env_flag("GITLAB_FETCH_LINKS", True)
        fetch_milestones = _env_flag("GITLAB_FETCH_MILESTONES", True)
        fetch_epics = _env_flag("GITLAB_FETCH_EPICS", True)

        raw_notes_limit = os.getenv("GITLAB_NOTES_LIMIT")
        notes_limit = 500
        if raw_notes_limit is not None:
            try:
                notes_limit = int(raw_notes_limit)
            except ValueError:
                logger.warning(
                    "Invalid GITLAB_NOTES_LIMIT value %r; falling back to 500",
                    raw_notes_limit,
                )

        sprint_cache: Dict[str, Sprint] = {}
        epic_count = 0

        # Determine time window
        updated_after: Optional[datetime] = None
        if ctx.window.updated_since:
            updated_after = _to_utc(ctx.window.updated_since)

        logger.info(
            "GitLab: fetching work items from %s (updated_after=%s)",
            project_path,
            updated_after,
        )

        fetched_count = 0

        # Fetch milestones first to populate sprint cache
        if fetch_milestones:
            try:
                for ms in client.iter_project_milestones(
                    project_id_or_path=project_path, state="all"
                ):
                    sprint = gitlab_milestone_to_sprint(
                        milestone=ms, project_full_path=project_path
                    )
                    sprint_cache[sprint.sprint_id] = sprint
                    sprints.append(sprint)
            except Exception as exc:
                logger.warning(
                    "GitLab: failed to fetch milestones for %s: %s", project_path, exc
                )

        # Fetch epics (group-level, requires Premium/Ultimate)
        if fetch_epics:
            # Extract group path from project path (e.g., "group/subgroup/project" -> "group/subgroup")
            path_parts = project_path.split("/")
            if len(path_parts) >= 2:
                group_full_path = "/".join(path_parts[:-1])
                try:
                    group = client.get_group(group_full_path)
                    if group:
                        for epic in client.iter_group_epics(
                            group_id_or_path=group_full_path,
                            state="all",
                            updated_after=updated_after,
                        ):
                            # Get state events for transitions and reopen detection
                            state_events = client.get_epic_resource_state_events(epic)

                            wi, wi_transitions = gitlab_epic_to_work_item(
                                epic=epic,
                                group_full_path=group_full_path,
                                status_mapping=self.status_mapping,
                                identity=self.identity,
                                state_events=state_events,
                            )

                            # Enrich with priority from labels
                            wi = enrich_work_item_with_priority(wi, wi.labels)

                            work_items.append(wi)
                            transitions.extend(wi_transitions)

                            # Reopen detection for epics
                            reopen_events.extend(
                                detect_gitlab_reopen_events(
                                    work_item_id=wi.work_item_id,
                                    state_events=state_events,
                                    identity=self.identity,
                                )
                            )

                            # Get notes for interactions
                            if fetch_notes:
                                try:
                                    notes = client.get_epic_notes(
                                        epic, limit=notes_limit
                                    )
                                    for note in notes:
                                        event = gitlab_note_to_interaction_event(
                                            note=note,
                                            work_item_id=wi.work_item_id,
                                            identity=self.identity,
                                        )
                                        if event:
                                            interactions.append(event)
                                except Exception as exc:
                                    logger.debug(
                                        "GitLab: failed to fetch notes for epic %s: %s",
                                        wi.work_item_id,
                                        exc,
                                    )

                            epic_count += 1

                except Exception as exc:
                    # Epics require Premium/Ultimate - 403/404 is expected on free tier
                    if "403" in str(exc) or "404" in str(exc):
                        logger.debug(
                            "GitLab: epics not available for group %s (requires Premium/Ultimate): %s",
                            group_full_path,
                            exc,
                        )
                    else:
                        logger.warning(
                            "GitLab: failed to fetch epics from group %s: %s",
                            group_full_path,
                            exc,
                        )

        # Fetch issues
        try:
            for issue in client.iter_project_issues(
                project_id_or_path=project_path,
                state="all",
                updated_after=updated_after,
                limit=ctx.limit,
            ):
                if ctx.limit is not None and fetched_count >= ctx.limit:
                    break

                # Get label events for transitions
                label_events = client.get_issue_resource_label_events(issue)

                wi, wi_transitions = gitlab_issue_to_work_item(
                    issue=issue,
                    project_full_path=project_path,
                    repo_id=None,
                    status_mapping=self.status_mapping,
                    identity=self.identity,
                    label_events=label_events,
                )

                # Enrich with priority from labels
                wi = enrich_work_item_with_priority(wi, wi.labels)

                # Link to parent epic if present
                if fetch_epics:
                    path_parts = project_path.split("/")
                    if len(path_parts) >= 2:
                        group_path = "/".join(path_parts[:-1])
                        epic_id = build_epic_id_for_issue(
                            issue=issue,
                            group_full_path=group_path,
                        )
                        if epic_id:
                            wi = replace(wi, epic_id=epic_id)

                work_items.append(wi)
                transitions.extend(wi_transitions)

                # Get state events for reopen detection
                state_events = client.get_issue_resource_state_events(issue)
                reopen_events.extend(
                    detect_gitlab_reopen_events(
                        work_item_id=wi.work_item_id,
                        state_events=state_events,
                        identity=self.identity,
                    )
                )

                # Get issue links for dependencies
                if fetch_links:
                    try:
                        linked_issues = client.get_issue_links(issue)
                        dependencies.extend(
                            extract_gitlab_dependencies(
                                work_item_id=wi.work_item_id,
                                issue=issue,
                                project_full_path=project_path,
                                linked_issues=linked_issues,
                            )
                        )
                    except Exception as exc:
                        logger.debug(
                            "GitLab: failed to fetch links for issue %s: %s",
                            wi.work_item_id,
                            exc,
                        )

                # Get notes for interactions
                if fetch_notes:
                    try:
                        notes = client.get_issue_notes(issue, limit=notes_limit)
                        for note in notes:
                            event = gitlab_note_to_interaction_event(
                                note=note,
                                work_item_id=wi.work_item_id,
                                identity=self.identity,
                            )
                            if event:
                                interactions.append(event)
                    except Exception as exc:
                        logger.debug(
                            "GitLab: failed to fetch notes for issue %s: %s",
                            wi.work_item_id,
                            exc,
                        )

                fetched_count += 1

        except Exception as exc:
            logger.error(
                "GitLab: failed to fetch issues from %s: %s", project_path, exc
            )
            raise

        # Fetch merge requests
        if include_mrs:
            try:
                for mr in client.iter_project_merge_requests(
                    project_id_or_path=project_path,
                    state="all",
                    updated_after=updated_after,
                    limit=ctx.limit - fetched_count if ctx.limit else None,
                ):
                    if ctx.limit is not None and fetched_count >= ctx.limit:
                        break

                    # Get state events for transitions and reopen detection
                    state_events = client.get_mr_resource_state_events(mr)

                    wi, wi_transitions = gitlab_mr_to_work_item(
                        mr=mr,
                        project_full_path=project_path,
                        repo_id=None,
                        status_mapping=self.status_mapping,
                        identity=self.identity,
                        state_events=state_events,
                    )

                    # Enrich with priority from labels
                    wi = enrich_work_item_with_priority(wi, wi.labels)

                    work_items.append(wi)
                    transitions.extend(wi_transitions)

                    # Reopen detection for MRs
                    reopen_events.extend(
                        detect_gitlab_reopen_events(
                            work_item_id=wi.work_item_id,
                            state_events=state_events,
                            identity=self.identity,
                        )
                    )

                    # Get notes for interactions
                    if fetch_notes:
                        try:
                            notes = client.get_mr_notes(mr, limit=notes_limit)
                            for note in notes:
                                event = gitlab_note_to_interaction_event(
                                    note=note,
                                    work_item_id=wi.work_item_id,
                                    identity=self.identity,
                                )
                                if event:
                                    interactions.append(event)
                        except Exception as exc:
                            logger.debug(
                                "GitLab: failed to fetch notes for MR %s: %s",
                                wi.work_item_id,
                                exc,
                            )

                    fetched_count += 1

            except Exception as exc:
                logger.warning(
                    "GitLab: failed to fetch MRs from %s: %s", project_path, exc
                )

        logger.info(
            "GitLab: fetched %d work items (%d issues, %d MRs) from %s",
            len(work_items),
            sum(1 for w in work_items if "#" in w.work_item_id),
            sum(1 for w in work_items if "!" in w.work_item_id),
            project_path,
        )

        return ProviderBatch(
            work_items=work_items,
            status_transitions=transitions,
            dependencies=dependencies,
            interactions=interactions,
            sprints=sprints,
            reopen_events=reopen_events,
        )
