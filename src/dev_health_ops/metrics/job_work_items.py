from __future__ import annotations

import argparse
import asyncio
import logging
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from dev_health_ops.analytics.investment import InvestmentClassifier
from dev_health_ops.db import resolve_sink_uri
from dev_health_ops.metrics.compute_work_item_state_durations import (
    compute_work_item_state_durations_daily,
)
from dev_health_ops.metrics.compute_work_items import (
    build_linked_issue_team_resolver,
    compute_work_item_metrics_daily,
    compute_work_item_team_attributions,
    resolve_team_attribution,
)
from dev_health_ops.metrics.job_daily import (
    REPO_ROOT,
    _discover_repos,
    _to_utc,
)
from dev_health_ops.metrics.loaders.base import to_dataclass
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.schemas import (
    InvestmentClassificationRecord,
    InvestmentMetricsRecord,
    IssueTypeMetricsRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.work_items import (
    fetch_github_project_v2_items,
    fetch_gitlab_work_items,
    fetch_jira_work_items_with_extras,
    parse_github_projects_v2_env,
)
from dev_health_ops.models.work_items import (
    WorkItem,
    WorkItemType,
)
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.status_mapping import load_status_mapping
from dev_health_ops.providers.teams import (
    build_project_key_resolver,
    load_team_resolver,
    normalize_team_id,
)
from dev_health_ops.storage import detect_db_type
from dev_health_ops.utils.cli import (
    add_date_range_args,
    add_sink_arg,
    resolve_date_range,
    validate_sink,
)

logger = logging.getLogger(__name__)

_LeaseCheck = Callable[[str], bool]
_WORK_ITEMS_SYNC_LEASE_CHECK: ContextVar[_LeaseCheck | None] = ContextVar(
    "work_items_sync_lease_check",
    default=None,
)


class WorkItemsSyncLeaseLost(RuntimeError):
    def __init__(self, surface: str) -> None:
        self.surface = surface
        super().__init__(f"sync unit lease lost before {surface} write")


@contextmanager
def work_items_sync_lease_check(check: _LeaseCheck) -> Iterator[None]:
    token = _WORK_ITEMS_SYNC_LEASE_CHECK.set(check)
    try:
        yield
    finally:
        _WORK_ITEMS_SYNC_LEASE_CHECK.reset(token)


def _ensure_unit_lease_for_write(surface: str) -> None:
    check = _WORK_ITEMS_SYNC_LEASE_CHECK.get()
    if check is not None and not check(surface):
        raise WorkItemsSyncLeaseLost(surface)


def _date_range(end_day: date, backfill_days: int) -> list[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def _build_github_work_client(
    *, org_id: str, credentials: dict[str, Any] | None = None
) -> Any:
    """Construct a GitHub work-items client from config-resolved credentials.

    Precedence: explicit ``credentials`` mapping → database credential scoped
    to ``org_id`` → environment variables as a last resort. An org-scoped
    caller must never silently pick up ambient ``GITHUB_TOKEN``/App env vars
    over its org's database credential (tenant boundary, CHAOS-2292); env
    resolution applies only when no organization scope is available (a
    pure-CLI run) or when org-scoped resolution finds no database row
    (DB-less dev setups, via ``CredentialResolver``'s env fallback).
    """
    from dev_health_ops.credentials.resolver import (
        github_credentials_from_mapping,
        resolve_credentials_sync,
    )
    from dev_health_ops.credentials.types import GitHubCredentials
    from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient

    if credentials:
        github_credentials = github_credentials_from_mapping(credentials)
        if github_credentials is None:
            raise ValueError(
                "Missing GitHub token or App credentials for work-items sync configuration"
            )
        return GitHubWorkClient(
            auth=GitHubAuth.from_credentials(github_credentials), org_id=org_id
        )

    if not org_id:
        return GitHubWorkClient.from_env()

    resolved_credentials = resolve_credentials_sync(
        "github", org_id=org_id, allow_env_fallback=True
    )
    if not isinstance(resolved_credentials, GitHubCredentials):
        raise ValueError("Resolved credentials are not GitHub credentials")
    return GitHubWorkClient(
        auth=GitHubAuth.from_credentials(resolved_credentials), org_id=org_id
    )


def _build_gitlab_work_client(
    *, org_id: str, credentials: dict[str, Any] | None = None
) -> tuple[str, str | None]:
    """Resolve GitLab credentials and return (token, gitlab_url) for explicit threading.

    Precedence: explicit ``credentials`` mapping → database credential scoped
    to ``org_id`` → environment variables as a last resort. An org-scoped
    caller must never silently pick up ambient ``GITLAB_TOKEN``/URL env vars
    over its org's database credential (tenant boundary, CHAOS-2461); env
    resolution applies only when no organization scope is available (a
    pure-CLI run) or when org-scoped resolution finds no database row
    (DB-less dev setups, via ``CredentialResolver``'s env fallback).
    """
    from dev_health_ops.credentials.resolver import (
        gitlab_credentials_from_mapping,
        resolve_credentials_sync,
    )
    from dev_health_ops.credentials.types import GitLabCredentials

    if credentials:
        gitlab_credentials = gitlab_credentials_from_mapping(credentials)
        if gitlab_credentials is None:
            raise ValueError("Missing GitLab token for work-items sync configuration")
        return gitlab_credentials.token, gitlab_credentials.base_url or None

    if not org_id:
        import os

        token = os.environ.get("GITLAB_TOKEN", "")
        gitlab_url = os.environ.get("GITLAB_URL") or None
        return token, gitlab_url

    resolved_credentials = resolve_credentials_sync(
        "gitlab", org_id=org_id, allow_env_fallback=True
    )
    if not isinstance(resolved_credentials, GitLabCredentials):
        raise ValueError("Resolved credentials are not GitLab credentials")
    return resolved_credentials.token, resolved_credentials.base_url or None


def _build_jira_work_client(
    *, org_id: str, credentials: dict[str, Any] | None = None
) -> Any:
    from dev_health_ops.credentials.resolver import (
        jira_credentials_from_mapping,
        resolve_credentials_sync,
    )
    from dev_health_ops.credentials.types import JiraCredentials
    from dev_health_ops.providers.jira.client import (
        JiraAuth,
        JiraClient,
        _normalize_jira_base_url,
    )

    if credentials:
        jira_credentials = jira_credentials_from_mapping(credentials)
        if jira_credentials is None:
            raise ValueError(
                "Missing Jira credentials for work-items sync configuration"
            )
    elif org_id:
        resolved_credentials = resolve_credentials_sync(
            "jira", org_id=org_id, allow_env_fallback=True
        )
        if not isinstance(resolved_credentials, JiraCredentials):
            raise ValueError("Resolved credentials are not Jira credentials")
        jira_credentials = resolved_credentials
    else:
        return JiraClient.from_env()

    return JiraClient(
        auth=JiraAuth(
            base_url=_normalize_jira_base_url(jira_credentials.base_url),
            email=jira_credentials.email,
            api_token=jira_credentials.api_token,
        ),
        org_id=org_id or None,
    )


def _build_linear_work_client(
    *, org_id: str, credentials: dict[str, Any] | None = None
) -> Any:
    from dev_health_ops.credentials.resolver import (
        linear_credentials_from_mapping,
        resolve_credentials_sync,
    )
    from dev_health_ops.credentials.types import LinearCredentials
    from dev_health_ops.providers.linear.client import LinearAuth, LinearClient

    if credentials:
        linear_credentials = linear_credentials_from_mapping(credentials)
        if linear_credentials is None:
            raise ValueError("Missing Linear API key for work-items sync configuration")
    elif org_id:
        resolved_credentials = resolve_credentials_sync(
            "linear", org_id=org_id, allow_env_fallback=True
        )
        if not isinstance(resolved_credentials, LinearCredentials):
            raise ValueError("Resolved credentials are not Linear credentials")
        linear_credentials = resolved_credentials
    else:
        return LinearClient.from_env()

    return LinearClient(
        auth=LinearAuth(api_key=linear_credentials.api_key),
        org_id=org_id or None,
    )


def run_work_items_sync_job(
    *,
    db_url: str,
    day: date,
    backfill_days: int,
    provider: str,
    sink: str = "auto",
    repo_id: uuid.UUID | None = None,
    repo_name: str | None = None,
    search_pattern: str | None = None,
    org_id: str = "",
    credentials: dict[str, Any] | None = None,
    jira_project_keys: list[str] | None = None,
    jira_jql: str | None = None,
    jira_fetch_all: bool | None = None,
    include_issues: bool | None = None,
    include_pull_requests: bool | None = None,
    fetch_comments: bool | None = None,
    fetch_milestones: bool | None = None,
) -> dict[str, Any] | None:
    """
    Sync work tracking facts from provider APIs and write derived work item tables.

    This job exists so `metrics daily` does not need to call external APIs.
    """
    if not db_url:
        raise ValueError("Database URI is required (pass --db or set DATABASE_URI).")

    backend = detect_db_type(db_url)
    if backend != "clickhouse":
        raise ValueError(
            f"Unsupported backend '{backend}'. Only ClickHouse is supported (CHAOS-641). "
            "Set CLICKHOUSE_URI and use a clickhouse:// connection string."
        )

    provider = (provider or "none").strip().lower()
    provider_set: set[str]
    if provider in {"none", "off", "skip"}:
        raise ValueError(
            "work item sync requires --provider (jira|github|gitlab|linear|synthetic|all)"
        )
    if provider in {"all", "*"}:
        provider_set = {"jira", "github", "gitlab", "linear", "synthetic"}
    else:
        provider_set = {provider}
    unknown = provider_set - {"jira", "github", "gitlab", "linear", "synthetic"}
    if unknown:
        raise ValueError(f"Unknown provider(s): {sorted(unknown)}")

    status_mapping = load_status_mapping()
    identity = load_identity_resolver()
    team_resolver = load_team_resolver()

    investment_classifier = InvestmentClassifier(
        REPO_ROOT / "src/dev_health_ops/config/investment_areas.yaml"
    )

    computed_at = datetime.now(timezone.utc)
    days = _date_range(day, backfill_days)
    since_dt = datetime.combine(min(days), time.min, tzinfo=timezone.utc)
    until_dt = datetime.combine(max(days), time.max, tzinfo=timezone.utc)

    primary_sink = ClickHouseMetricsSink(db_url)
    sinks: list[Any] = [primary_sink]
    for s in sinks:
        setattr(s, "org_id", org_id)

    try:
        for s in sinks:
            s.ensure_tables()

        _teams_data = (
            primary_sink.query_dicts(
                "SELECT id, name, project_keys, provider, native_team_key FROM teams FINAL"
                + (" WHERE org_id = {org_id:String}" if org_id else ""),
                {"org_id": org_id} if org_id else {},
            )
            if hasattr(primary_sink, "query_dicts")
            else []
        )
        pk_resolver = build_project_key_resolver(_teams_data)
        from dev_health_ops.models.work_items import Sprint

        _sprints_data = (
            primary_sink.query_dicts(
                "SELECT provider, sprint_id, name, state, started_at, ended_at, completed_at, last_synced, org_id FROM sprints FINAL"
                + (" WHERE org_id = {org_id:String}" if org_id else ""),
                {"org_id": org_id} if org_id else {},
            )
            if hasattr(primary_sink, "query_dicts")
            else []
        )
        reference_sprints = [
            Sprint(
                provider=row["provider"],
                sprint_id=row["sprint_id"],
                name=row.get("name"),
                state=row.get("state"),
                started_at=row.get("started_at"),
                ended_at=row.get("ended_at"),
                completed_at=row.get("completed_at"),
                last_synced=row.get("last_synced") or computed_at,
                org_id=str(row.get("org_id") or ""),
            )
            for row in _sprints_data
        ]

        discovered_repos = _discover_repos(
            backend=backend,
            primary_sink=primary_sink,
            repo_id=repo_id,
            repo_name=repo_name,
            org_id=org_id,
            provider=provider if provider not in {"all", "*"} else "auto",
        )
        from dev_health_ops.utils import match_pattern

        before = len(discovered_repos)
        discovered_repos = [
            r for r in discovered_repos if match_pattern(r.full_name, search_pattern)
        ]
        logger.info(
            "Filtered repos by '%s': %d/%d",
            search_pattern,
            len(discovered_repos),
            before,
        )

        if "synthetic" in provider_set and not any(
            r.source == "synthetic" for r in discovered_repos
        ):
            from dev_health_ops.metrics.work_items import DiscoveredRepo

            discovered_repos.append(
                DiscoveredRepo(
                    repo_id=uuid.uuid4(),
                    full_name="synthetic/demo-repo",
                    source="synthetic",
                    settings={},
                )
            )

        work_items: list[Any] = []
        transitions: list[Any] = []
        dependencies: list[Any] = []
        reopen_events: list[Any] = []
        interactions: list[Any] = []
        sprints: list[Any] = []
        # AI attribution records collected across all provider batches.
        # Populated when providers emit attribution signals (GitHub PRs).
        # Written to sink via write_ai_attribution() at end of sync loop.
        ai_attributions: list[Any] = []
        github_usage_observations: list[dict[str, Any]] = []
        linear_page_count = 0
        linear_batch_count = 0

        if "jira" in provider_set:
            (
                items,
                tr,
                dep,
                reopen,
                interaction,
                sprint_rows,
            ) = fetch_jira_work_items_with_extras(
                since=since_dt,
                until=until_dt,
                status_mapping=status_mapping,
                identity=identity,
                client=_build_jira_work_client(org_id=org_id, credentials=credentials),
                project_keys=jira_project_keys,
                jql_override=jira_jql,
                fetch_all=jira_fetch_all,
                use_env_query_options=not bool(org_id or credentials),
                reference_sprints=reference_sprints,
                reference_sink=primary_sink,
            )
            work_items.extend(items)
            transitions.extend(tr)
            dependencies.extend(dep)
            reopen_events.extend(reopen)
            interactions.extend(interaction)
            sprints.extend(sprint_rows)

        if "github" in provider_set:
            from uuid import UUID

            from dev_health_ops.providers.base import (
                IngestionContext,
                IngestionWindow,
                WorkItemIngestionOptions,
            )
            from dev_health_ops.providers.github.provider import GitHubProvider

            github_provider = GitHubProvider(
                status_mapping=status_mapping,
                identity=identity,
                client=_build_github_work_client(
                    org_id=org_id, credentials=credentials
                ),
            )
            github_org_id = UUID(org_id) if org_id else None
            for discovered_repo in discovered_repos:
                if discovered_repo.source != "github":
                    continue
                ctx = IngestionContext(
                    window=IngestionWindow(
                        updated_since=since_dt,
                        active_until=until_dt,
                    ),
                    repo=discovered_repo.full_name,
                    repo_id=discovered_repo.repo_id,
                    org_id=github_org_id,
                    work_item_options=WorkItemIngestionOptions(
                        include_issues=include_issues,
                        include_pull_requests=include_pull_requests,
                        fetch_comments=fetch_comments,
                        fetch_milestones=fetch_milestones,
                    ),
                )
                for batch in github_provider.iter_ingest(ctx):
                    work_items.extend(batch.work_items)
                    transitions.extend(batch.status_transitions)
                    dependencies.extend(batch.dependencies)
                    reopen_events.extend(batch.reopen_events)
                    interactions.extend(batch.interactions)
                    sprints.extend(batch.sprints)
                    ai_attributions.extend(batch.ai_attributions)
                    raw_github_usage = batch.observations.get("github_usage")
                    if isinstance(raw_github_usage, list):
                        github_usage_observations.extend(
                            item for item in raw_github_usage if isinstance(item, dict)
                        )

            projects = parse_github_projects_v2_env()
            if projects:
                proj_items, proj_tr = fetch_github_project_v2_items(
                    projects=projects,
                    status_mapping=status_mapping,
                    identity=identity,
                )
                by_id = {w.work_item_id: w for w in work_items}
                for w in proj_items:
                    by_id[w.work_item_id] = w
                work_items = list(by_id.values())
                transitions.extend(list(proj_tr or []))

        if "gitlab" in provider_set:
            gl_token, gl_url = _build_gitlab_work_client(
                org_id=org_id, credentials=credentials
            )
            items, tr, gl_ai_attributions = fetch_gitlab_work_items(
                repos=discovered_repos,
                since=since_dt,
                status_mapping=status_mapping,
                identity=identity,
                token=gl_token,
                gitlab_url=gl_url,
                include_label_events=True,
                org_id=org_id,
            )
            work_items.extend(items)
            transitions.extend(tr)
            ai_attributions.extend(gl_ai_attributions)
            # Extract dependency edges (same-provider refs + cross-provider
            # external keys) from each GitLab work item's description so GitLab
            # items participate in linked-issue team inheritance like GitHub.
            # get_attr-based extractor reads WorkItem.description directly.
            from dev_health_ops.providers.gitlab.normalize import (
                extract_gitlab_dependencies,
            )

            for wi in items:
                dependencies.extend(
                    extract_gitlab_dependencies(
                        work_item_id=wi.work_item_id,
                        issue=wi,
                        project_full_path=(wi.project_id or wi.project_key or ""),
                    )
                )

        if "synthetic" in provider_set:
            from dev_health_ops.metrics.work_items import fetch_synthetic_work_items

            items, tr = fetch_synthetic_work_items(
                repos=discovered_repos, days=backfill_days + 1
            )
            work_items.extend(items)
            transitions.extend(tr)

        if "linear" in provider_set:
            from dev_health_ops.providers.base import IngestionContext, IngestionWindow
            from dev_health_ops.providers.linear.provider import LinearProvider

            linear_provider = LinearProvider(
                status_mapping=status_mapping,
                identity=identity,
                client=_build_linear_work_client(
                    org_id=org_id, credentials=credentials
                ),
            )
            ctx = IngestionContext(
                window=IngestionWindow(updated_since=since_dt, active_until=until_dt),
                repo=None,
                org_id=uuid.UUID(org_id) if org_id else None,
                reference_teams=_teams_data,
                reference_sprints=reference_sprints,
                reference_sink=primary_sink,
            )
            fetched_items = 0
            fetched_transitions = 0
            fetched_sprints = 0
            for batch in linear_provider.iter_ingest(ctx):
                linear_batch_count += 1
                if (
                    batch.work_items
                    or batch.status_transitions
                    or batch.reopen_events
                    or batch.interactions
                    or batch.dependencies
                ):
                    linear_page_count += 1
                work_items.extend(batch.work_items)
                transitions.extend(batch.status_transitions)
                reopen_events.extend(batch.reopen_events)
                interactions.extend(batch.interactions)
                sprints.extend(batch.sprints)
                # PR/MR -> issue edges from Linear attachments (links to source
                # control) drive linked-issue team inheritance for the PR/MR.
                dependencies.extend(batch.dependencies)
                # Collect any AI attribution records in the batch.
                if hasattr(batch, "ai_attributions"):
                    ai_attributions.extend(batch.ai_attributions)
                fetched_items += len(batch.work_items)
                fetched_transitions += len(batch.status_transitions)
                fetched_sprints += len(batch.sprints)
            logger.info(
                "Linear: fetched %d work items, %d transitions, %d sprints",
                fetched_items,
                fetched_transitions,
                fetched_sprints,
            )

        logger.info(
            "Work item sync: fetched %d items and %d transitions (providers=%s)",
            len(work_items),
            len(transitions),
            sorted(provider_set),
        )
        providers_label = ",".join(sorted(provider_set))
        if dependencies:
            logger.info(
                "%s: extracted %d dependency edges", providers_label, len(dependencies)
            )
        if reopen_events:
            logger.info(
                "%s: extracted %d reopen events", providers_label, len(reopen_events)
            )
        if interactions:
            logger.info(
                "%s: extracted %d interaction events",
                providers_label,
                len(interactions),
            )
        if sprints:
            logger.info(
                "%s: extracted %d sprint records", providers_label, len(sprints)
            )

        # Stamp org_id on work items, transitions AND dependencies before
        # writing to sinks. Dependencies must be tagged too: the work_item_dependencies
        # table is tenant-partitioned and the donor-read path filters by org_id,
        # so unstamped edges would be invisible to tenant-scoped inheritance.
        if org_id:
            work_items = [
                replace(wi, org_id=org_id) if hasattr(wi, "org_id") else wi
                for wi in work_items
            ]
            transitions = [
                replace(t, org_id=org_id) if hasattr(t, "org_id") else t
                for t in transitions
            ]
            dependencies = [
                replace(dep, org_id=org_id) if hasattr(dep, "org_id") else dep
                for dep in dependencies
            ]

        # Write raw work items and transitions to sinks
        for s in sinks:
            if hasattr(s, "write_work_items") and work_items:
                _ensure_unit_lease_for_write("work_items")
                logger.info(
                    "Writing %d work items to %s", len(work_items), type(s).__name__
                )
                s.write_work_items(work_items)
            if hasattr(s, "write_work_item_transitions") and transitions:
                _ensure_unit_lease_for_write("work_item_transitions")
                logger.info(
                    "Writing %d transitions to %s", len(transitions), type(s).__name__
                )
                s.write_work_item_transitions(transitions)

        for s in sinks:
            if dependencies and hasattr(s, "write_work_item_dependencies"):
                _ensure_unit_lease_for_write("work_item_dependencies")
                s.write_work_item_dependencies(dependencies)
            if reopen_events and hasattr(s, "write_work_item_reopen_events"):
                _ensure_unit_lease_for_write("work_item_reopen_events")
                s.write_work_item_reopen_events(reopen_events)
            if interactions and hasattr(s, "write_work_item_interactions"):
                _ensure_unit_lease_for_write("work_item_interactions")
                s.write_work_item_interactions(interactions)
            if sprints and hasattr(s, "write_sprints"):
                _ensure_unit_lease_for_write("sprints")
                s.write_sprints(sprints)
            # AI attribution records — gated with hasattr so this is a no-op
            # until CHAOS-1579 (storage-worker) lands write_ai_attribution.
            if ai_attributions and hasattr(s, "write_ai_attribution"):
                _ensure_unit_lease_for_write("ai_attribution")
                logger.info(
                    "Writing %d AI attribution records to %s",
                    len(ai_attributions),
                    type(s).__name__,
                )
                s.write_ai_attribution(ai_attributions)

        # Build the linked-issue team-inheritance fallback once for the whole
        # run: PRs/MRs that map to no team of their own inherit the team of an
        # issue they link to (provider-agnostic — e.g. a GitHub PR closing a
        # Linear issue).
        #
        # Freshly-extracted edges are AUTHORITATIVE for the items synced this
        # run — they are the current source-of-truth, so a link removed from a
        # PR is simply absent and stops granting inheritance (no append-only
        # stale-edge problem on the sync path). We therefore use the fresh edges
        # only. The donor *items* they point at may have been synced earlier, so
        # those are loaded from ClickHouse — bounded to the referenced targets,
        # never a full-history scan — and unioned with the fresh items.
        donor_by_id: dict[str, Any] = {}
        merged_deps: dict[tuple[str, str, str], Any] = {}
        for dep in dependencies:
            merged_deps[
                (
                    dep.source_work_item_id,
                    dep.target_work_item_id,
                    dep.relationship_type,
                )
            ] = dep

        # Load only the donor items referenced by a fresh edge target — bounded
        # to the linked surface, under tenant scope, degrading gracefully.
        if org_id and merged_deps and hasattr(primary_sink, "query_dicts"):
            _ids: set[str] = set()
            _keys: set[str] = set()
            for dep in merged_deps.values():
                target = dep.target_work_item_id
                if target.startswith("extkey:"):
                    _keys.add(target.split(":", 1)[1].strip().upper())
                elif target:
                    _ids.add(target)
            if _ids or _keys:
                _clauses: list[str] = []
                _params: dict[str, Any] = {"org_id": org_id}
                if _ids:
                    _params["donor_ids"] = sorted(_ids)
                    _clauses.append("work_item_id IN {donor_ids:Array(String)}")
                if _keys:
                    _params["donor_keys"] = sorted(_keys)
                    _clauses.append(
                        "upper(splitByChar(':', work_item_id)[-1]) "
                        "IN {donor_keys:Array(String)}"
                    )
                try:
                    for r in primary_sink.query_dicts(
                        "SELECT * FROM work_items FINAL "
                        "WHERE org_id = {org_id:String} AND ("
                        + " OR ".join(_clauses)
                        + ")",
                        _params,
                    ):
                        wi = to_dataclass(WorkItem, r)
                        donor_by_id[wi.work_item_id] = wi
                except Exception:
                    logger.warning(
                        "Donor item load failed; inheritance limited to the "
                        "sync window",
                        exc_info=True,
                    )
        # Freshly-synced items win (newest attribution fields).
        for wi in work_items:
            donor_by_id[wi.work_item_id] = wi

        team_attribution_context = None
        if org_id:
            try:
                team_attribution_context = asyncio.run(
                    ClickHouseDataLoader(
                        primary_sink.client, org_id=org_id
                    ).load_team_attribution_context(as_of=computed_at)
                )
            except Exception:
                logger.warning(
                    "Team attribution context load failed; using legacy resolvers only",
                    exc_info=True,
                )

        linked_issue_resolver = build_linked_issue_team_resolver(
            work_items=list(donor_by_id.values()),
            dependencies=list(merged_deps.values()),
            team_resolver=team_resolver,
            project_key_resolver=pk_resolver,
            attribution_context=team_attribution_context,
        )

        for d in days:
            wi_metrics, wi_user_metrics, wi_cycle_times = (
                compute_work_item_metrics_daily(
                    day=d,
                    work_items=work_items,
                    transitions=transitions,
                    computed_at=computed_at,
                    team_resolver=team_resolver,
                    project_key_resolver=pk_resolver,
                    linked_issue_resolver=linked_issue_resolver,
                    attribution_context=team_attribution_context,
                )
            )
            wi_team_attributions = compute_work_item_team_attributions(
                work_items=work_items,
                computed_at=computed_at,
                team_resolver=team_resolver,
                project_key_resolver=pk_resolver,
                linked_issue_resolver=linked_issue_resolver,
                attribution_context=team_attribution_context,
            )
            wi_state_durations = compute_work_item_state_durations_daily(
                day=d,
                work_items=work_items,
                transitions=transitions,
                computed_at=computed_at,
                team_resolver=team_resolver,
                project_key_resolver=pk_resolver,
                linked_issue_resolver=linked_issue_resolver,
                attribution_context=team_attribution_context,
            )

            # --- Issue Type Metrics ---
            issue_type_stats: dict[
                tuple[uuid.UUID, Any, str, WorkItemType], dict[str, Any]
            ] = {}

            def _get_team(wi: Any) -> str:
                team_id, _, _ = resolve_team_attribution(
                    wi,
                    team_resolver,
                    pk_resolver,
                    linked_issue_resolver=linked_issue_resolver,
                    attribution_context=team_attribution_context,
                )
                return normalize_team_id(team_id)

            def _normalize_investment_team_id(team_id: str | None) -> str | None:
                if not team_id or team_id == "unassigned":
                    return None
                return team_id

            start_dt = _to_utc(datetime.combine(d, time.min, tzinfo=timezone.utc))
            end_dt = start_dt + timedelta(days=1)
            for item in work_items:
                r_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
                prov = item.provider
                team_id = _get_team(item)
                norm_type = status_mapping.normalize_type(
                    provider=prov,
                    type_raw=item.type,
                    labels=getattr(item, "labels", []),
                )

                key = (r_id, prov, team_id, norm_type)
                if key not in issue_type_stats:
                    issue_type_stats[key] = {
                        "created": 0,
                        "completed": 0,
                        "active": 0,
                        "cycle_hours": [],
                    }

                stats = issue_type_stats[key]
                created = _to_utc(item.created_at)
                if start_dt <= created < end_dt:
                    stats["created"] += 1

                if item.completed_at:
                    completed = _to_utc(item.completed_at)
                    if start_dt <= completed < end_dt:
                        stats["completed"] += 1
                        if item.started_at:
                            started = _to_utc(item.started_at)
                            h = (completed - started).total_seconds() / 3600.0
                            if h >= 0:
                                stats["cycle_hours"].append(h)

                if created < end_dt and (
                    not item.completed_at or _to_utc(item.completed_at) >= start_dt
                ):
                    stats["active"] += 1

            issue_type_metrics_rows: list[IssueTypeMetricsRecord] = []
            for (r_id, prov, team_id, norm_type), stat in issue_type_stats.items():
                cycles = sorted(stat["cycle_hours"])
                p50 = cycles[len(cycles) // 2] if cycles else 0.0
                p90 = cycles[int(len(cycles) * 0.9)] if cycles else 0.0
                issue_type_metrics_rows.append(
                    IssueTypeMetricsRecord(
                        repo_id=r_id if r_id.int != 0 else None,
                        day=d,
                        provider=prov,
                        team_id=team_id,
                        issue_type_norm=norm_type,
                        created_count=stat["created"],
                        completed_count=stat["completed"],
                        active_count=stat["active"],
                        cycle_p50_hours=p50,
                        cycle_p90_hours=p90,
                        lead_p50_hours=0.0,
                        computed_at=computed_at,
                    )
                )

            # --- Investment areas ---
            investment_classifications: list[InvestmentClassificationRecord] = []
            inv_metrics_map: dict[tuple[Any, str, str, str], dict[str, Any]] = {}

            for item in work_items:
                r_id = getattr(item, "repo_id", None) or uuid.UUID(int=0)
                created = _to_utc(item.created_at)
                if not (
                    created < end_dt
                    and (
                        not item.completed_at or _to_utc(item.completed_at) >= start_dt
                    )
                ):
                    continue

                cls = investment_classifier.classify(
                    {
                        "labels": getattr(item, "labels", []),
                        "component": getattr(item, "component", ""),
                        "title": item.title,
                        "provider": item.provider,
                    }
                )

                investment_classifications.append(
                    InvestmentClassificationRecord(
                        repo_id=r_id if r_id.int != 0 else None,
                        day=d,
                        artifact_type="work_item",
                        artifact_id=item.work_item_id,
                        provider=item.provider,
                        investment_area=cls.investment_area,
                        project_stream=cls.project_stream or "",
                        confidence=cls.confidence,
                        rule_id=cls.rule_id,
                        computed_at=computed_at,
                    )
                )

                if item.completed_at:
                    completed = _to_utc(item.completed_at)
                    if not (start_dt <= completed < end_dt):
                        continue
                    team_id_value = _normalize_investment_team_id(_get_team(item)) or ""
                    inv_key = (
                        r_id,
                        team_id_value,
                        cls.investment_area,
                        cls.project_stream or "",
                    )
                    if inv_key not in inv_metrics_map:
                        inv_metrics_map[inv_key] = {
                            "units": 0,
                            "completed": 0,
                            "churn": 0,
                            "cycles": [],
                        }
                    inv_metrics_map[inv_key]["completed"] += 1
                    points = getattr(item, "story_points", 1) or 1
                    inv_metrics_map[inv_key]["units"] += int(points)
                    if item.started_at:
                        started = _to_utc(item.started_at)
                        h = (completed - started).total_seconds() / 3600.0
                        if h >= 0:
                            inv_metrics_map[inv_key]["cycles"].append(h)

            investment_metrics_rows: list[InvestmentMetricsRecord] = []
            for (r_id, team_id, area, stream), data in inv_metrics_map.items():
                cycles = sorted(data["cycles"])
                p50 = cycles[len(cycles) // 2] if cycles else 0.0
                investment_metrics_rows.append(
                    InvestmentMetricsRecord(
                        repo_id=r_id if r_id.int != 0 else None,
                        day=d,
                        team_id=team_id,
                        investment_area=area,
                        project_stream=stream,
                        delivery_units=data["units"],
                        work_items_completed=data["completed"],
                        prs_merged=0,
                        churn_loc=data["churn"],
                        cycle_p50_hours=p50,
                        computed_at=computed_at,
                    )
                )

            for s in sinks:
                if wi_metrics:
                    _ensure_unit_lease_for_write("work_item_metrics_daily")
                    s.write_work_item_metrics(wi_metrics)
                if wi_user_metrics:
                    _ensure_unit_lease_for_write("work_item_user_metrics_daily")
                    s.write_work_item_user_metrics(wi_user_metrics)
                if wi_cycle_times:
                    _ensure_unit_lease_for_write("work_item_cycle_times")
                    s.write_work_item_cycle_times(wi_cycle_times)
                if wi_team_attributions and hasattr(
                    s, "write_work_item_team_attributions"
                ):
                    _ensure_unit_lease_for_write("work_item_team_attributions")
                    s.write_work_item_team_attributions(wi_team_attributions)
                if wi_state_durations:
                    _ensure_unit_lease_for_write("work_item_state_durations_daily")
                    s.write_work_item_state_durations(wi_state_durations)

                if hasattr(s, "write_issue_type_metrics") and issue_type_metrics_rows:
                    _ensure_unit_lease_for_write("issue_type_metrics_daily")
                    s.write_issue_type_metrics(issue_type_metrics_rows)
                if (
                    hasattr(s, "write_investment_classifications")
                    and investment_classifications
                ):
                    _ensure_unit_lease_for_write("investment_classifications_daily")
                    s.write_investment_classifications(investment_classifications)
                if hasattr(s, "write_investment_metrics") and investment_metrics_rows:
                    _ensure_unit_lease_for_write("investment_metrics_daily")
                    s.write_investment_metrics(investment_metrics_rows)
        observations: dict[str, Any] = {}
        if github_usage_observations:
            observations["github_usage"] = github_usage_observations
        if "linear" in provider_set:
            observations["linear_page_count"] = linear_page_count
            observations["linear_batch_count"] = linear_batch_count
        if observations:
            return {"observations": observations}
        return None
    finally:
        for s in sinks:
            try:
                s.close()
            except Exception:
                logger.exception("Error closing sink %s", type(s).__name__)


def register_commands(sync_subparsers: argparse._SubParsersAction) -> None:
    wi = sync_subparsers.add_parser(
        "work-items",
        help="Sync work tracking facts and compute derived work item tables.",
    )
    add_date_range_args(wi)
    wi.add_argument(
        "--provider",
        choices=["all", "jira", "github", "gitlab", "linear", "synthetic", "none"],
        default="all",
        help="Provider to sync from (default: all).",
    )
    add_sink_arg(wi)
    wi.add_argument("--repo-id", type=uuid.UUID, help="Filter to specific repo ID.")
    wi.add_argument("--repo-name", help="Filter to specific repo name.")
    wi.add_argument("-s", "--search", help="Repo name search pattern (glob).")
    wi.set_defaults(func=_cmd_sync_work_items)


def _cmd_sync_work_items(ns: argparse.Namespace) -> int:
    try:
        validate_sink(ns)
        end_day, backfill_days = resolve_date_range(ns)
        run_work_items_sync_job(
            db_url=resolve_sink_uri(ns),
            day=end_day,
            backfill_days=backfill_days,
            provider=ns.provider,
            sink=ns.sink,
            repo_id=ns.repo_id,
            repo_name=ns.repo_name,
            search_pattern=ns.search,
        )
        return 0
    except Exception as e:
        logger.error(f"Work item sync job failed: {e}")
        return 1
