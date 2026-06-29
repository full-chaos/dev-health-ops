from __future__ import annotations

import logging
import os
import random
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dev_health_ops.metrics.dependencies import get_metrics_dependencies
from dev_health_ops.models.ai_attribution import AIAttributionRecord
from dev_health_ops.models.work_items import (
    Sprint,
    WorkItem,
    WorkItemDependency,
    WorkItemInteractionEvent,
    WorkItemReopenEvent,
    WorkItemStatusTransition,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping
from dev_health_ops.utils.datetime import to_utc

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveredRepo:
    repo_id: uuid.UUID
    full_name: str
    source: str  # github|gitlab|local|...
    settings: dict[str, object]


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


def fetch_synthetic_work_items(
    *,
    repos: Sequence[DiscoveredRepo],
    days: int = 30,
) -> tuple[list[WorkItem], list[WorkItemStatusTransition]]:
    """
    Generate synthetic work items for testing/demo purposes.
    """
    deps = get_metrics_dependencies()

    all_items: list[WorkItem] = []
    all_transitions: list[WorkItemStatusTransition] = []

    for repo in repos:
        if repo.source != "synthetic":
            continue
        logger.info("Generating synthetic work items for repo: %s", repo.full_name)
        seed = int(repo.repo_id.hex, 16) % (2**32)
        random.seed(seed)

        generator = deps.synthetic_generator_factory(
            repo_id=repo.repo_id,
            repo_name=repo.full_name,
        )
        items = generator.generate_work_items(days=days)
        transitions = generator.generate_work_item_transitions(items)

        all_items.extend(items)
        all_transitions.extend(transitions)

    return all_items, all_transitions


def fetch_jira_work_items_with_extras(
    *,
    since: datetime,
    until: datetime | None = None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    project_keys: Sequence[str] | None = None,
    client: Any | None = None,
    jql_override: str | None = None,
    fetch_all: bool | None = None,
    use_env_query_options: bool = True,
    reference_sprints: Sequence[Sprint] | None = None,
    reference_sink: Any | None = None,
) -> tuple[
    list[WorkItem],
    list[WorkItemStatusTransition],
    list[WorkItemDependency],
    list[WorkItemReopenEvent],
    list[WorkItemInteractionEvent],
    list[Sprint],
]:
    """
    Fetch Jira issues updated since `since` and normalize into WorkItems.

    Jira configuration is provided via env vars:
    - JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN
    - optional: JIRA_PROJECT_KEYS (comma-separated)
    """
    deps = get_metrics_dependencies()

    if project_keys is None and use_env_query_options:
        raw_keys = os.getenv("JIRA_PROJECT_KEYS") or ""
        project_keys = [k.strip() for k in raw_keys.split(",") if k.strip()] or None

    jql_override = (
        str(jql_override or "").strip()
        if jql_override is not None or not use_env_query_options
        else (os.getenv("JIRA_JQL") or "").strip()
    )
    if fetch_all is None:
        fetch_all = use_env_query_options and (
            os.getenv("JIRA_FETCH_ALL") or ""
        ).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    if client is None and _env_flag("JIRA_USE_PROVIDER", False):
        if project_keys:
            if len(project_keys) != 1:
                logger.warning(
                    "JiraProvider supports a single project_key override; using env JIRA_PROJECT_KEYS instead"
                )
        (
            batch_work_items,
            batch_status_transitions,
            batch_dependencies,
            batch_reopen_events,
            batch_interactions,
            batch_sprints,
        ) = deps.jira_provider_ingest(
            since=since,
            until=until,
            status_mapping=status_mapping,
            identity=identity,
            project_keys=project_keys,
        )
        if batch_interactions == [] and _env_flag("JIRA_FETCH_COMMENTS", True):
            logger.info(
                "JiraProvider does not fetch comments; set JIRA_USE_PROVIDER=0 to use legacy comment ingestion"
            )
        if batch_dependencies == []:
            logger.info(
                "JiraProvider does not fetch dependency edges; set JIRA_USE_PROVIDER=0 to use legacy dependency ingestion"
            )
        return (
            batch_work_items,
            batch_status_transitions,
            batch_dependencies,
            batch_reopen_events,
            batch_interactions,
            batch_sprints,
        )

    jira_client: Any = client or deps.jira_client_factory()
    work_items: list[WorkItem] = []
    transitions: list[WorkItemStatusTransition] = []
    dependencies: list[WorkItemDependency] = []
    reopen_events: list[WorkItemReopenEvent] = []
    interactions: list[WorkItemInteractionEvent] = []
    sprints: list[Sprint] = []

    fetch_comments = _env_flag("JIRA_FETCH_COMMENTS", True)
    comments_limit = int(os.getenv("JIRA_COMMENTS_LIMIT", "0"))  # 0 means no limit
    sprint_cache: dict[str, Sprint] = {
        sprint.sprint_id: sprint
        for sprint in reference_sprints or []
        if sprint.provider == "jira"
    }
    sprint_ids: set[str] = set()

    updated_since = to_utc(since).date().isoformat()
    active_until = to_utc(until).date().isoformat() if until is not None else None
    logger.info("Jira: fetching work items updated since %s", updated_since)
    jqls: list[str] = []
    if jql_override:
        jqls = [jql_override]
        logger.info("Jira: using JIRA_JQL override")
    elif fetch_all:
        logger.info("Jira: using JIRA_FETCH_ALL=1 (may be slow on large instances)")
        if project_keys:
            for key in project_keys:
                jqls.append(
                    deps.jira_build_jql(
                        project_key=key, updated_since=None, active_until=None
                    )
                )
        else:
            jqls.append(
                deps.jira_build_jql(
                    project_key=None,
                    updated_since=None,
                    active_until=None,
                )
            )
    else:
        if project_keys:
            for key in project_keys:
                jqls.append(
                    deps.jira_build_jql(
                        project_key=key,
                        updated_since=updated_since,
                        active_until=active_until,
                    )
                )
        else:
            jqls.append(
                deps.jira_build_jql(
                    project_key=None,
                    updated_since=updated_since,
                    active_until=active_until,
                )
            )

    for jql in jqls:
        logger.debug("Jira: JQL=%s", jql)
        for issue in jira_client.iter_issues(jql=jql, expand_changelog=True):
            issue_key = issue.get("key") if isinstance(issue, dict) else None
            wi, wi_transitions = deps.jira_issue_to_work_item(
                issue=issue,
                status_mapping=status_mapping,
                identity=identity,
                repo_id=None,
            )
            work_items.append(wi)
            transitions.extend(wi_transitions)
            dependencies.extend(
                deps.jira_extract_dependencies(
                    issue=issue, work_item_id=wi.work_item_id
                )
            )
            reopen_events.extend(
                deps.jira_detect_reopen_events(
                    work_item_id=wi.work_item_id,
                    transitions=wi_transitions,
                )
            )

            if fetch_comments and issue_key:
                try:
                    comment_count = 0
                    for comment in jira_client.iter_issue_comments(
                        issue_id_or_key=str(issue_key)
                    ):
                        if comments_limit > 0 and comment_count >= comments_limit:
                            break
                        event = deps.jira_comment_to_interaction(
                            work_item_id=wi.work_item_id,
                            comment=comment,
                            identity=identity,
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
                sprint_ids.add(wi.sprint_id)

    fetched_sprints: list[Sprint] = []
    for sprint_id in sorted(sprint_ids):
        if sprint_id in sprint_cache:
            sprints.append(sprint_cache[sprint_id])
            continue
        try:
            payload = jira_client.get_sprint(sprint_id=str(sprint_id))
        except Exception as exc:
            logger.warning("Jira: failed to fetch sprint %s: %s", sprint_id, exc)
            continue
        sprint = deps.jira_sprint_to_model(payload)
        if sprint:
            sprint_cache[sprint_id] = sprint
            sprints.append(sprint)
            fetched_sprints.append(sprint)
    if reference_sink is not None and fetched_sprints:
        reference_sink.write_sprints(fetched_sprints)

    logger.info("Fetched %d Jira work items (since %s)", len(work_items), updated_since)
    try:
        jira_client.close()
    except Exception as exc:
        logger.warning("Failed to close Jira client: %s", exc)
    return work_items, transitions, dependencies, reopen_events, interactions, sprints


def fetch_jira_work_items(
    *,
    since: datetime,
    until: datetime | None = None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    project_keys: Sequence[str] | None = None,
) -> tuple[list[WorkItem], list[WorkItemStatusTransition]]:
    work_items, transitions, _, _, _, _ = fetch_jira_work_items_with_extras(
        since=since,
        until=until,
        status_mapping=status_mapping,
        identity=identity,
        project_keys=project_keys,
    )
    return work_items, transitions


def fetch_github_work_items(
    *,
    repos: Sequence[DiscoveredRepo],
    since: datetime,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    include_issue_events: bool = True,
    max_events_per_issue: int = 300,
) -> tuple[list[WorkItem], list[WorkItemStatusTransition]]:
    """
    Fetch GitHub issues updated since `since` for the given repos and normalize into WorkItems.

    Requires `GITHUB_TOKEN`.
    """
    token = os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise ValueError("GitHub token required (set GITHUB_TOKEN)")

    deps = get_metrics_dependencies()

    client = deps.github_client_factory(token=token)
    work_items: dict[str, WorkItem] = {}
    transitions: list[WorkItemStatusTransition] = []

    since_utc = to_utc(since)
    github_repos = [r for r in repos if r.source == "github"]
    logger.info(
        "GitHub: fetching work items from %d repos (since %s)",
        len(github_repos),
        since_utc.isoformat(),
    )
    for repo in repos:
        if repo.source != "github":
            continue
        logger.debug("GitHub: repo=%s", repo.full_name)
        try:
            owner, name = repo.full_name.split("/", 1)
        except ValueError:
            continue
        for issue in client.iter_issues(
            owner=owner, repo=name, state="all", since=since_utc
        ):
            events = None
            if include_issue_events:
                try:
                    events = list(
                        client.iter_issue_events(issue, limit=max_events_per_issue)
                    )
                except Exception:
                    events = None
            wi, _transitions = deps.github_issue_to_work_item(
                issue=issue,
                repo_full_name=repo.full_name,
                repo_id=repo.repo_id,
                status_mapping=status_mapping,
                identity=identity,
                events=events,
            )
            work_items[wi.work_item_id] = wi
            transitions.extend(list(_transitions or []))

    logger.info(
        "Fetched %d GitHub work items (since %s)",
        len(work_items),
        since_utc.isoformat(),
    )
    return list(work_items.values()), transitions


def fetch_github_project_v2_items(
    *,
    projects: Sequence[tuple[str, int]],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
) -> tuple[list[WorkItem], list[WorkItemStatusTransition]]:
    """
    Fetch GitHub Projects v2 items for (org_login, project_number).

    Configure via env `GITHUB_PROJECTS_V2` as comma-separated `org:project_number` entries:
      GITHUB_PROJECTS_V2="myorg:3,anotherorg:12"
    """
    token = os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise ValueError("GitHub token required (set GITHUB_TOKEN)")

    deps = get_metrics_dependencies()

    client = deps.github_client_factory(token=token)
    items: dict[str, WorkItem] = {}
    transitions: list[WorkItemStatusTransition] = []
    for org_login, project_number in projects:
        project_scope_id = f"ghprojv2:{org_login}#{int(project_number)}"
        logger.info("GitHub: fetching Projects v2 items for %s", project_scope_id)
        for node in client.iter_project_v2_items(
            org_login=org_login, project_number=int(project_number), first=50
        ):
            wi, wi_transitions = deps.github_project_item_to_work_item(
                item_node=node,
                project_scope_id=project_scope_id,
                status_mapping=status_mapping,
                identity=identity,
            )
            if wi is None:
                continue
            items[wi.work_item_id] = wi
            transitions.extend(wi_transitions)

    logger.info("Fetched %d GitHub Projects v2 items", len(items))
    return list(items.values()), transitions


def parse_github_projects_v2_env() -> list[tuple[str, int]]:
    raw = os.getenv("GITHUB_PROJECTS_V2") or ""
    projects: list[tuple[str, int]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            org, number_str = part.split(":", 1)
            projects.append((org.strip(), int(number_str.strip())))
        except Exception:
            continue
    return projects


def fetch_gitlab_work_items(
    *,
    repos: Sequence[DiscoveredRepo],
    since: datetime,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    token: str,
    gitlab_url: str | None = None,
    include_label_events: bool = True,
    max_label_events: int = 300,
    org_id: str = "",
) -> tuple[
    list[WorkItem],
    list[WorkItemStatusTransition],
    list[AIAttributionRecord],
]:
    """
    Fetch GitLab issues updated since `since` for the given projects and normalize into WorkItems.

    Also scans merge requests updated since `since` for AI attribution signals
    (labels, bot authors, commit trailers in the description, branch names) and
    returns them as :class:`AIAttributionRecord` so the work-items sync job can
    persist GitLab governance coverage through the SAME ``write_ai_attribution``
    sink path used by GitHub (CHAOS-2379). Attribution detection is the only
    reason MRs are fetched here; their work items continue to be produced by the
    provider path, so this scan does not duplicate MR work-item rows.

    Attribution rows are written with the caller's real ``org_id`` and are
    skipped entirely when ``org_id`` is blank (a CLI-only run with no tenant
    scope) so a blank-tenant row is never persisted.

    Credentials (``token`` and optional ``gitlab_url``) are threaded explicitly
    by the caller; this function never reads from ``os.environ``.
    """
    from uuid import UUID

    from dev_health_ops.providers.gitlab.normalize import gitlab_mr_ai_attributions
    from dev_health_ops.providers.utils import env_flag

    deps = get_metrics_dependencies()

    client = deps.gitlab_client_factory(token=token, gitlab_url=gitlab_url)
    work_items: dict[str, WorkItem] = {}
    transitions: list[WorkItemStatusTransition] = []
    ai_attributions: list[AIAttributionRecord] = []

    since_utc = to_utc(since)
    gitlab_repos = [r for r in repos if r.source == "gitlab"]
    logger.info(
        "GitLab: fetching work items from %d projects (since %s)",
        len(gitlab_repos),
        since_utc.isoformat(),
    )
    # AI attribution requires a tenant scope and the MR-attribution feature.
    org_uuid = UUID(org_id) if org_id else None
    scan_mrs = org_uuid is not None and env_flag("GITLAB_INCLUDE_MRS", True)
    for repo in repos:
        if repo.source != "gitlab":
            continue
        logger.debug("GitLab: project=%s", repo.full_name)
        for issue in client.iter_project_issues(
            project_id_or_path=repo.full_name,
            state="all",
            updated_after=since_utc,
        ):
            label_events = None
            if include_label_events:
                try:
                    # python-gitlab provides resource_label_events on issue objects.
                    label_events = list(
                        issue.resource_label_events.list(per_page=100, iterator=True)
                    )[:max_label_events]
                except Exception:
                    label_events = None

            wi, _transitions = deps.gitlab_issue_to_work_item(
                issue=issue,
                project_full_path=repo.full_name,
                repo_id=repo.repo_id,
                status_mapping=status_mapping,
                identity=identity,
                label_events=label_events,
            )
            work_items[wi.work_item_id] = wi
            transitions.extend(list(_transitions or []))

        if scan_mrs:
            assert org_uuid is not None  # for type checker; guarded by scan_mrs
            try:
                for mr in client.iter_project_merge_requests(
                    project_id_or_path=repo.full_name,
                    state="all",
                    updated_after=since_utc,
                ):
                    ai_attributions.extend(
                        gitlab_mr_ai_attributions(
                            mr=mr,
                            project_full_path=repo.full_name,
                            org_id=org_uuid,
                            repo_id=repo.repo_id,
                        )
                    )
            except Exception as exc:
                logger.warning(
                    "GitLab: failed to scan MRs for AI attribution in %s: %s",
                    repo.full_name,
                    exc,
                )

    logger.info(
        "Fetched %d GitLab work items (since %s); %d AI attribution record(s)",
        len(work_items),
        since_utc.isoformat(),
        len(ai_attributions),
    )
    return list(work_items.values()), transitions, ai_attributions
