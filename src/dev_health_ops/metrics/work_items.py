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
    WorkItem,
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
    usage_observations: list[dict[str, Any]] | None = None,
    id_scoped_project_ids: dict[uuid.UUID, str] | None = None,
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

    ``id_scoped_project_ids`` [CHAOS-2763 codex HIGH]: maps ``repo_id`` ->
    immutable GitLab project id, for repos whose work-item unit was matched
    by numeric ``settings.project_id`` rather than by path (job_work_items.py
    scoping). Those repos are fetched from the GitLab API using the numeric
    id, NOT ``repo.full_name``. Matching a row by its immutable project_id
    does not make its ``full_name`` safe to fetch by: if the project was
    renamed/moved after discovery and the stale path was reused by a
    *different* project, fetching by path would silently pull the wrong
    project's issues/MRs and attribute them to this row's ``repo_id``. Repos
    absent from the mapping (path-matched units, org-wide/no-source runs)
    are unaffected and keep fetching by ``full_name`` as before.
    ``repo.full_name`` itself is always passed to normalization
    (``project_full_path=``) unchanged — only the API call identifier
    changes for id-scoped repos.
    """
    from uuid import UUID

    from dev_health_ops.providers.gitlab.normalize import gitlab_mr_ai_attributions
    from dev_health_ops.providers.utils import env_flag

    deps = get_metrics_dependencies()
    id_scoped_project_ids = id_scoped_project_ids or {}

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
        # Fetch by the immutable project id when this repo's unit was
        # id-scoped; otherwise (path-matched or org-wide) fetch by the
        # discovered path, unchanged from before this fix.
        api_project_ref = id_scoped_project_ids.get(repo.repo_id, repo.full_name)
        logger.debug("GitLab: project=%s (api_ref=%s)", repo.full_name, api_project_ref)
        for issue in client.iter_project_issues(
            project_id_or_path=api_project_ref,
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
                    project_id_or_path=api_project_ref,
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
    if usage_observations is not None:
        # Drain the live-sync GitLab client's request actuals into the caller's
        # accumulator (CHAOS-2754). This is the sync path the worker actually
        # runs (GitLabProvider.ingest is the provider-pattern path); both drain.
        from dev_health_ops.providers.usage import drain_provider_usage

        usage_observations.extend(drain_provider_usage(client))
    return list(work_items.values()), transitions, ai_attributions
