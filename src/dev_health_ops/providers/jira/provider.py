"""
Jira provider implementation conforming to the Provider contract.

This wraps the existing Jira client and normalization logic without changing
the underlying ingestion behavior.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

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
from dev_health_ops.providers.jira.atlassian_compat import atlassian_client_enabled
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


class JiraProvider(Provider):
    """
    Provider implementation for Jira Cloud.

    Capabilities:
    - work_items: yes
    - status_transitions: yes (via changelog)
    - dependencies: yes (via issue links)
    - interactions: yes (via comments)
    - sprints: yes (via agile API)
    - reopen_events: yes (derived from transitions)
    - priority: yes (via priority field)
    """

    name = "jira"
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
        Initialize the Jira provider.

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
        Ingest work items from Jira within the given context.

        Uses environment variables for authentication:
        - JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN

        Optional env vars:
        - JIRA_PROJECT_KEYS: comma-separated list of project keys
        - JIRA_JQL: custom JQL override
        - JIRA_FETCH_ALL: fetch all issues (ignores time window)
        - JIRA_FETCH_COMMENTS: whether to fetch comments (default: true)
        - JIRA_COMMENTS_LIMIT: max comments per issue (0 = no limit)
        - ATLASSIAN_CLIENT_ENABLED: use new atlassian-client library
        - ATLASSIAN_GQL_ENABLED: enable GraphQL worklog enrichment (REST fallback)
        """
        if atlassian_client_enabled():
            logger.info(
                "Jira: using atlassian-client library (ATLASSIAN_CLIENT_ENABLED=true)"
            )
            return self._ingest_via_atlassian_client(ctx)
        return self._ingest_via_legacy_client(ctx)

    def _ingest_via_legacy_client(self, ctx: IngestionContext) -> ProviderBatch:
        from dev_health_ops.providers.jira.client import JiraClient, build_jira_jql
        from dev_health_ops.providers.jira.normalize import (
            detect_reopen_events,
            extract_jira_issue_dependencies,
            jira_comment_to_interaction_event,
            jira_issue_to_work_item,
            jira_sprint_payload_to_model,
        )

        # Determine project keys
        project_keys: Optional[Sequence[str]] = None
        if ctx.project_key:
            project_keys = [ctx.project_key]
        else:
            raw_keys = os.getenv("JIRA_PROJECT_KEYS") or ""
            project_keys = [k.strip() for k in raw_keys.split(",") if k.strip()] or None

        jql_override = (os.getenv("JIRA_JQL") or "").strip()
        fetch_all = (os.getenv("JIRA_FETCH_ALL") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

        client = JiraClient.from_env()

        work_items: List[WorkItem] = []
        transitions: List[WorkItemStatusTransition] = []
        dependencies: List[WorkItemDependency] = []
        reopen_events: List[WorkItemReopenEvent] = []
        interactions: List[WorkItemInteractionEvent] = []
        sprints: List[Sprint] = []

        fetch_comments = _env_flag("JIRA_FETCH_COMMENTS", True)
        raw_comments_limit = os.getenv("JIRA_COMMENTS_LIMIT")
        comments_limit = 0
        if raw_comments_limit is not None:
            try:
                comments_limit = int(raw_comments_limit)
            except ValueError:
                logger.warning(
                    "Invalid JIRA_COMMENTS_LIMIT value %r; falling back to 0",
                    raw_comments_limit,
                )
        sprint_cache: Dict[str, Sprint] = {}
        sprint_ids: set[str] = set()

        # Build time window parameters
        updated_since: Optional[str] = None
        active_until: Optional[str] = None
        if ctx.window.updated_since:
            updated_since = _to_utc(ctx.window.updated_since).date().isoformat()
        if ctx.window.active_until:
            active_until = _to_utc(ctx.window.active_until).date().isoformat()

        logger.info("Jira: fetching work items (updated_since=%s)", updated_since)

        # Build JQL queries
        jqls: List[str] = []
        if jql_override:
            jqls = [jql_override]
            logger.info("Jira: using JIRA_JQL override")
        elif fetch_all:
            logger.info("Jira: using JIRA_FETCH_ALL=1 (may be slow on large instances)")
            if project_keys:
                for key in project_keys:
                    jqls.append(
                        build_jira_jql(
                            project_key=key, updated_since=None, active_until=None
                        )
                    )
            else:
                jqls.append(
                    build_jira_jql(
                        project_key=None, updated_since=None, active_until=None
                    )
                )
        else:
            if project_keys:
                for key in project_keys:
                    jqls.append(
                        build_jira_jql(
                            project_key=key,
                            updated_since=updated_since,
                            active_until=active_until,
                        )
                    )
            else:
                jqls.append(
                    build_jira_jql(
                        project_key=None,
                        updated_since=updated_since,
                        active_until=active_until,
                    )
                )

        try:
            fetched_count = 0
            for jql in jqls:
                logger.debug("Jira: JQL=%s", jql)
                for issue in client.iter_issues(jql=jql, expand_changelog=True):
                    if ctx.limit is not None and fetched_count >= ctx.limit:
                        break

                    issue_key = issue.get("key") if isinstance(issue, dict) else None
                    wi, wi_transitions = jira_issue_to_work_item(
                        issue=issue,
                        status_mapping=self.status_mapping,
                        identity=self.identity,
                        repo_id=None,
                    )
                    work_items.append(wi)
                    transitions.extend(wi_transitions)
                    dependencies.extend(
                        extract_jira_issue_dependencies(
                            issue=issue, work_item_id=wi.work_item_id
                        )
                    )
                    reopen_events.extend(
                        detect_reopen_events(
                            work_item_id=wi.work_item_id,
                            transitions=wi_transitions,
                        )
                    )

                    if fetch_comments and issue_key:
                        try:
                            comment_count = 0
                            for comment in client.iter_issue_comments(
                                issue_id_or_key=str(issue_key)
                            ):
                                if (
                                    comments_limit > 0
                                    and comment_count >= comments_limit
                                ):
                                    break
                                event = jira_comment_to_interaction_event(
                                    work_item_id=wi.work_item_id,
                                    comment=comment,
                                    identity=self.identity,
                                )
                                if event:
                                    interactions.append(event)
                                    comment_count += 1
                        except Exception as exc:
                            logger.warning(
                                "Jira: failed to fetch comments for issue %s: %s",
                                issue_key,
                                exc,
                            )

                    if wi.sprint_id:
                        if wi.sprint_id not in sprint_cache:
                            sprint_ids.add(wi.sprint_id)

                    fetched_count += 1

                if ctx.limit is not None and fetched_count >= ctx.limit:
                    break

            # Fetch sprint details
            for sprint_id in sorted(sprint_ids):
                if sprint_id in sprint_cache:
                    continue
                try:
                    payload = client.get_sprint(sprint_id=str(sprint_id))
                except Exception as exc:
                    logger.warning(
                        "Jira: failed to fetch sprint %s: %s", sprint_id, exc
                    )
                    continue
                sprint = jira_sprint_payload_to_model(payload)
                if sprint:
                    sprint_cache[sprint_id] = sprint
                    sprints.append(sprint)

            logger.info(
                "Jira: fetched %d work items (updated_since=%s)",
                len(work_items),
                updated_since,
            )
        finally:
            try:
                client.close()
            except Exception as exc:
                logger.warning("Failed to close Jira client: %s", exc)

        return ProviderBatch(
            work_items=work_items,
            status_transitions=transitions,
            dependencies=dependencies,
            interactions=interactions,
            sprints=sprints,
            reopen_events=reopen_events,
        )

    def _ingest_via_atlassian_client(self, ctx: IngestionContext) -> ProviderBatch:
        from atlassian import (
            iter_board_sprints_via_rest,
            iter_issue_changelog_via_rest,
            iter_issue_worklogs_via_rest,
            iter_issues_via_rest,
        )
        from atlassian.graph.api.jira_worklogs import iter_issue_worklogs_via_graphql
        from atlassian.rest.api.jira_boards import iter_boards_via_rest

        from dev_health_ops.models.work_items import Worklog
        from dev_health_ops.providers.jira.atlassian_compat import (
            build_atlassian_rest_client,
            build_atlassian_graphql_client,
            get_atlassian_cloud_id,
        )
        from dev_health_ops.providers.jira.client import build_jira_jql
        from dev_health_ops.providers.jira.normalize import (
            canonical_changelog_to_transitions,
            canonical_jira_issue_to_work_item,
            canonical_sprint_to_model,
            canonical_worklog_to_model,
            derive_started_completed_from_transitions,
            detect_reopen_events,
        )

        project_keys: Optional[Sequence[str]] = None
        if ctx.project_key:
            project_keys = [ctx.project_key]
        else:
            raw_keys = os.getenv("JIRA_PROJECT_KEYS") or ""
            project_keys = [k.strip() for k in raw_keys.split(",") if k.strip()] or None

        jql_override = (os.getenv("JIRA_JQL") or "").strip()
        fetch_all = _env_flag("JIRA_FETCH_ALL", False)

        updated_since: Optional[str] = None
        active_until: Optional[str] = None
        if ctx.window.updated_since:
            updated_since = _to_utc(ctx.window.updated_since).date().isoformat()
        if ctx.window.active_until:
            active_until = _to_utc(ctx.window.active_until).date().isoformat()

        jqls: List[str] = []
        if jql_override:
            jqls = [jql_override]
        elif fetch_all:
            if project_keys:
                for key in project_keys:
                    jqls.append(build_jira_jql(project_key=key))
            else:
                jqls.append(build_jira_jql())
        else:
            if project_keys:
                for key in project_keys:
                    jqls.append(
                        build_jira_jql(
                            project_key=key,
                            updated_since=updated_since,
                            active_until=active_until,
                        )
                    )
            else:
                jqls.append(
                    build_jira_jql(
                        updated_since=updated_since,
                        active_until=active_until,
                    )
                )

        cloud_id = get_atlassian_cloud_id()
        if not cloud_id:
            raise ValueError(
                "ATLASSIAN_CLOUD_ID required when using atlassian-client. "
                "Set ATLASSIAN_CLOUD_ID or derive from ATLASSIAN_JIRA_BASE_URL."
            )

        work_items: List[WorkItem] = []
        transitions: List[WorkItemStatusTransition] = []
        reopen_events: List[WorkItemReopenEvent] = []
        sprints: List[Sprint] = []
        worklogs: List[Worklog] = []
        sprint_ids: set[str] = set()

        fetch_worklogs = _env_flag("JIRA_FETCH_WORKLOGS", False)
        use_graphql = _env_flag("ATLASSIAN_GQL_ENABLED", False)

        client = build_atlassian_rest_client()
        graphql_client = None
        if use_graphql and fetch_worklogs:
            graphql_client = build_atlassian_graphql_client()
        try:
            fetched_count = 0
            for jql in jqls:
                logger.debug("Jira (atlassian-client): JQL=%s", jql)
                for issue in iter_issues_via_rest(client, cloud_id, jql):
                    if ctx.limit is not None and fetched_count >= ctx.limit:
                        break

                    wi = canonical_jira_issue_to_work_item(
                        issue=issue,
                        status_mapping=self.status_mapping,
                        identity=self.identity,
                    )

                    changelog_events = list(
                        iter_issue_changelog_via_rest(client, issue_key=issue.key)
                    )
                    wi_transitions = canonical_changelog_to_transitions(
                        issue_key=issue.key,
                        changelog_events=changelog_events,
                        status_mapping=self.status_mapping,
                        identity=self.identity,
                        labels=list(issue.labels),
                    )

                    started_at, completed_at = (
                        derive_started_completed_from_transitions(
                            transitions=wi_transitions,
                            normalized_status=wi.status,
                            resolved_at=wi.completed_at,
                            updated_at=wi.updated_at,
                        )
                    )

                    wi = WorkItem(
                        work_item_id=wi.work_item_id,
                        provider=wi.provider,
                        repo_id=wi.repo_id,
                        project_key=wi.project_key,
                        project_id=wi.project_id,
                        title=wi.title,
                        description=wi.description,
                        type=wi.type,
                        status=wi.status,
                        status_raw=wi.status_raw,
                        assignees=wi.assignees,
                        reporter=wi.reporter,
                        created_at=wi.created_at,
                        updated_at=wi.updated_at,
                        started_at=started_at,
                        completed_at=completed_at,
                        closed_at=wi.closed_at,
                        labels=wi.labels,
                        story_points=wi.story_points,
                        sprint_id=wi.sprint_id,
                        sprint_name=wi.sprint_name,
                        parent_id=wi.parent_id,
                        epic_id=wi.epic_id,
                        url=wi.url,
                        priority_raw=wi.priority_raw,
                        service_class=wi.service_class,
                        due_at=wi.due_at,
                    )

                    work_items.append(wi)
                    transitions.extend(wi_transitions)
                    reopen_events.extend(
                        detect_reopen_events(
                            work_item_id=wi.work_item_id,
                            transitions=wi_transitions,
                        )
                    )

                    if wi.sprint_id:
                        sprint_ids.add(wi.sprint_id)

                    if fetch_worklogs:
                        if use_graphql and graphql_client is not None and cloud_id:
                            try:
                                for wl in iter_issue_worklogs_via_graphql(
                                    graphql_client,
                                    cloud_id=cloud_id,
                                    issue_key=issue.key,
                                ):
                                    worklogs.append(
                                        canonical_worklog_to_model(
                                            issue_key=issue.key,
                                            worklog=wl,
                                            identity=self.identity,
                                        )
                                    )
                            except Exception as exc:
                                logger.warning(
                                    "Jira: GraphQL worklog fetch failed for %s; falling back to REST: %s",
                                    issue.key,
                                    exc,
                                )
                                try:
                                    for wl in iter_issue_worklogs_via_rest(
                                        client, issue_key=issue.key
                                    ):
                                        worklogs.append(
                                            canonical_worklog_to_model(
                                                issue_key=issue.key,
                                                worklog=wl,
                                                identity=self.identity,
                                            )
                                        )
                                except Exception as rest_exc:
                                    logger.warning(
                                        "Jira: failed to fetch worklogs for %s: %s",
                                        issue.key,
                                        rest_exc,
                                    )
                        else:
                            try:
                                for wl in iter_issue_worklogs_via_rest(
                                    client, issue_key=issue.key
                                ):
                                    worklogs.append(
                                        canonical_worklog_to_model(
                                            issue_key=issue.key,
                                            worklog=wl,
                                            identity=self.identity,
                                        )
                                    )
                            except Exception as exc:
                                logger.warning(
                                    "Jira: failed to fetch worklogs for %s: %s",
                                    issue.key,
                                    exc,
                                )

                    fetched_count += 1

                if ctx.limit is not None and fetched_count >= ctx.limit:
                    break

            if _env_flag("JIRA_FETCH_BOARD_SPRINTS", False):
                try:
                    for board in iter_boards_via_rest(client):
                        logger.debug(
                            "Jira: fetching sprints for board %s (%s)",
                            board.id,
                            board.name,
                        )
                        for sprint in iter_board_sprints_via_rest(
                            client, board_id=int(board.id)
                        ):
                            sprints.append(canonical_sprint_to_model(sprint=sprint))
                except Exception as exc:
                    logger.warning("Jira: failed to fetch board sprints: %s", exc)

            logger.info(
                "Jira (atlassian-client): fetched %d work items, %d worklogs, %d sprints (updated_since=%s)",
                len(work_items),
                len(worklogs),
                len(sprints),
                updated_since,
            )
        finally:
            client.close()
            if graphql_client is not None:
                graphql_client.close()

        return ProviderBatch(
            work_items=work_items,
            status_transitions=transitions,
            dependencies=[],
            interactions=[],
            sprints=list(sprints),
            reopen_events=reopen_events,
            worklogs=worklogs,
        )
