from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from dev_health_ops.models.git import get_repo_uuid_from_repo
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

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveredRepo:
    repo_id: uuid.UUID
    full_name: str
    source: str  # github|gitlab|local|...
    settings: Dict[str, object]


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


def fetch_synthetic_work_items(
    *,
    repos: Sequence[DiscoveredRepo],
    days: int = 30,
) -> Tuple[List[WorkItem], List[WorkItemStatusTransition]]:
    """
    Generate synthetic work items for testing/demo purposes.
    """
    from dev_health_ops.fixtures.generator import SyntheticDataGenerator

    all_items: List[WorkItem] = []
    all_transitions: List[WorkItemStatusTransition] = []

    for repo in repos:
        if repo.source != "synthetic":
            continue
        logger.info("Generating synthetic work items for repo: %s", repo.full_name)
        # Use repo_id as random seed for stability
        import random

        seed = int(repo.repo_id.hex, 16) % (2**32)
        random.seed(seed)

        generator = SyntheticDataGenerator(
            repo_id=repo.repo_id, repo_name=repo.full_name
        )
        items = generator.generate_work_items(days=days)
        transitions = generator.generate_work_item_transitions(items)

        all_items.extend(items)
        all_transitions.extend(transitions)

    return all_items, all_transitions


def fetch_jira_work_items_with_extras(
    *,
    since: datetime,
    until: Optional[datetime] = None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    project_keys: Optional[Sequence[str]] = None,
) -> Tuple[
    List[WorkItem],
    List[WorkItemStatusTransition],
    List[WorkItemDependency],
    List[WorkItemReopenEvent],
    List[WorkItemInteractionEvent],
    List[Sprint],
]:
    """
    Fetch Jira issues updated since `since` and normalize into WorkItems.

    Jira configuration is provided via env vars:
    - JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN
    - optional: JIRA_PROJECT_KEYS (comma-separated)
    """
    try:
        from dev_health_ops.providers.jira.client import JiraClient, build_jira_jql
        from dev_health_ops.providers.jira.normalize import (
            detect_reopen_events,
            extract_jira_issue_dependencies,
            jira_comment_to_interaction_event,
            jira_issue_to_work_item,
            jira_sprint_payload_to_model,
        )
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"Jira provider not available: {exc}") from exc

    if project_keys is None:
        raw_keys = os.getenv("JIRA_PROJECT_KEYS") or ""
        project_keys = [k.strip() for k in raw_keys.split(",") if k.strip()] or None

    jql_override = (os.getenv("JIRA_JQL") or "").strip()
    fetch_all = (os.getenv("JIRA_FETCH_ALL") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if _env_flag("JIRA_USE_PROVIDER", False):
        from dev_health_ops.providers.base import IngestionContext, IngestionWindow
        from dev_health_ops.providers.jira.provider import JiraProvider

        ctx = IngestionContext(
            window=IngestionWindow(updated_since=since, active_until=until)
        )
        if project_keys:
            if len(project_keys) == 1:
                ctx = IngestionContext(
                    window=ctx.window,
                    project_key=project_keys[0],
                )
            else:
                logger.warning(
                    "JiraProvider supports a single project_key override; using env JIRA_PROJECT_KEYS instead"
                )

        batch = JiraProvider(
            status_mapping=status_mapping,
            identity=identity,
        ).ingest(ctx)
        if batch.interactions == [] and _env_flag("JIRA_FETCH_COMMENTS", True):
            logger.info(
                "JiraProvider does not fetch comments; set JIRA_USE_PROVIDER=0 to use legacy comment ingestion"
            )
        if batch.dependencies == []:
            logger.info(
                "JiraProvider does not fetch dependency edges; set JIRA_USE_PROVIDER=0 to use legacy dependency ingestion"
            )
        return (
            batch.work_items,
            batch.status_transitions,
            batch.dependencies,
            batch.reopen_events,
            batch.interactions,
            batch.sprints,
        )

    client = JiraClient.from_env()
    work_items: List[WorkItem] = []
    transitions: List[WorkItemStatusTransition] = []
    dependencies: List[WorkItemDependency] = []
    reopen_events: List[WorkItemReopenEvent] = []
    interactions: List[WorkItemInteractionEvent] = []
    sprints: List[Sprint] = []

    fetch_comments = _env_flag("JIRA_FETCH_COMMENTS", True)
    comments_limit = int(os.getenv("JIRA_COMMENTS_LIMIT", "0"))  # 0 means no limit
    sprint_cache: Dict[str, Sprint] = {}
    sprint_ids: set[str] = set()

    updated_since = _to_utc(since).date().isoformat()
    active_until = _to_utc(until).date().isoformat() if until is not None else None
    logger.info("Jira: fetching work items updated since %s", updated_since)
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
                build_jira_jql(project_key=None, updated_since=None, active_until=None)
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

    for jql in jqls:
        logger.debug("Jira: JQL=%s", jql)
        for issue in client.iter_issues(jql=jql, expand_changelog=True):
            issue_key = issue.get("key") if isinstance(issue, dict) else None
            wi, wi_transitions = jira_issue_to_work_item(
                issue=issue,
                status_mapping=status_mapping,
                identity=identity,
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
                        if comments_limit > 0 and comment_count >= comments_limit:
                            break
                        event = jira_comment_to_interaction_event(
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
                if wi.sprint_id not in sprint_cache:
                    sprint_ids.add(wi.sprint_id)

    for sprint_id in sorted(sprint_ids):
        if sprint_id in sprint_cache:
            continue
        try:
            payload = client.get_sprint(sprint_id=str(sprint_id))
        except Exception as exc:
            logger.warning("Jira: failed to fetch sprint %s: %s", sprint_id, exc)
            continue
        sprint = jira_sprint_payload_to_model(payload)
        if sprint:
            sprint_cache[sprint_id] = sprint
            sprints.append(sprint)

    logger.info("Fetched %d Jira work items (since %s)", len(work_items), updated_since)
    try:
        client.close()
    except Exception as exc:
        logger.warning("Failed to close Jira client: %s", exc)
    return work_items, transitions, dependencies, reopen_events, interactions, sprints


def fetch_jira_work_items(
    *,
    since: datetime,
    until: Optional[datetime] = None,
    status_mapping: StatusMapping,
    identity: IdentityResolver,
    project_keys: Optional[Sequence[str]] = None,
) -> Tuple[List[WorkItem], List[WorkItemStatusTransition]]:
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
) -> Tuple[List[WorkItem], List[WorkItemStatusTransition]]:
    """
    Fetch GitHub issues updated since `since` for the given repos and normalize into WorkItems.

    Requires `GITHUB_TOKEN`.
    """
    token = os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise ValueError("GitHub token required (set GITHUB_TOKEN)")

    from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient
    from dev_health_ops.providers.github.normalize import github_issue_to_work_item

    client = GitHubWorkClient(auth=GitHubAuth(token=token))
    work_items: Dict[str, WorkItem] = {}
    transitions: List[WorkItemStatusTransition] = []

    since_utc = _to_utc(since)
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
            wi, _transitions = github_issue_to_work_item(
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
    projects: Sequence[Tuple[str, int]],
    status_mapping: StatusMapping,
    identity: IdentityResolver,
) -> Tuple[List[WorkItem], List[WorkItemStatusTransition]]:
    """
    Fetch GitHub Projects v2 items for (org_login, project_number).

    Configure via env `GITHUB_PROJECTS_V2` as comma-separated `org:project_number` entries:
      GITHUB_PROJECTS_V2="myorg:3,anotherorg:12"
    """
    token = os.getenv("GITHUB_TOKEN") or ""
    if not token:
        raise ValueError("GitHub token required (set GITHUB_TOKEN)")

    from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient
    from dev_health_ops.providers.github.normalize import (
        github_project_v2_item_to_work_item,
    )

    client = GitHubWorkClient(auth=GitHubAuth(token=token))
    items: Dict[str, WorkItem] = {}
    transitions: List[WorkItemStatusTransition] = []
    for org_login, project_number in projects:
        project_scope_id = f"ghprojv2:{org_login}#{int(project_number)}"
        logger.info("GitHub: fetching Projects v2 items for %s", project_scope_id)
        for node in client.iter_project_v2_items(
            org_login=org_login, project_number=int(project_number), first=50
        ):
            wi, wi_transitions = github_project_v2_item_to_work_item(
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


def parse_github_projects_v2_env() -> List[Tuple[str, int]]:
    raw = os.getenv("GITHUB_PROJECTS_V2") or ""
    projects: List[Tuple[str, int]] = []
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
    include_label_events: bool = True,
    max_label_events: int = 300,
) -> Tuple[List[WorkItem], List[WorkItemStatusTransition]]:
    """
    Fetch GitLab issues updated since `since` for the given projects and normalize into WorkItems.

    Requires `GITLAB_TOKEN` and optional `GITLAB_URL`.
    """
    from dev_health_ops.providers.gitlab.client import GitLabWorkClient
    from dev_health_ops.providers.gitlab.normalize import gitlab_issue_to_work_item

    client = GitLabWorkClient.from_env()
    work_items: Dict[str, WorkItem] = {}
    transitions: List[WorkItemStatusTransition] = []

    since_utc = _to_utc(since)
    gitlab_repos = [r for r in repos if r.source == "gitlab"]
    logger.info(
        "GitLab: fetching work items from %d projects (since %s)",
        len(gitlab_repos),
        since_utc.isoformat(),
    )
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

            wi, _transitions = gitlab_issue_to_work_item(
                issue=issue,
                project_full_path=repo.full_name,
                repo_id=repo.repo_id,
                status_mapping=status_mapping,
                identity=identity,
                label_events=label_events,
            )
            work_items[wi.work_item_id] = wi
            transitions.extend(list(_transitions or []))

    logger.info(
        "Fetched %d GitLab work items (since %s)",
        len(work_items),
        since_utc.isoformat(),
    )
    return list(work_items.values()), transitions


def discover_repos_from_records(
    records: Iterable[Tuple[str, Dict[str, object]]],
) -> List[DiscoveredRepo]:
    """
    Convert (repo_full_name, settings) tuples into DiscoveredRepo rows.
    """
    repos: List[DiscoveredRepo] = []
    for full_name, settings in records:
        source = str((settings or {}).get("source") or "").strip().lower()
        if not source:
            continue
        try:
            repo_id = get_repo_uuid_from_repo(full_name)
        except Exception:
            continue
        repos.append(
            DiscoveredRepo(
                repo_id=repo_id,
                full_name=full_name,
                source=source,
                settings=settings or {},
            )
        )
    return repos
