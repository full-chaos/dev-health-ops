from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import uuid
from collections.abc import Callable, Iterator, Sequence
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
    _INHERITABLE_RELATIONSHIP_TYPES,
    build_linked_issue_team_resolver,
    compute_estimate_coverage_metrics_daily,
    compute_work_item_metrics_daily,
    compute_work_item_team_attributions,
)
from dev_health_ops.metrics.job_daily import (
    REPO_ROOT,
    _discover_repos,
)
from dev_health_ops.metrics.loaders.base import to_dataclass
from dev_health_ops.metrics.loaders.clickhouse import ClickHouseDataLoader
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.team_attribution_telemetry import (
    ATTRIBUTION_DOWNGRADES_TOTAL,
    STORED_EDGE_LOAD_FAILURES_TOTAL,
)
from dev_health_ops.metrics.work_item_engine_destinations import (
    compute_work_item_engine_destinations_daily,
)
from dev_health_ops.metrics.work_items import (
    fetch_github_project_v2_items,
    fetch_gitlab_work_items,
    fetch_jira_work_items_with_extras,
    parse_github_projects_v2_env,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemDependency
from dev_health_ops.providers.gitlab.instance import normalize_gitlab_instance
from dev_health_ops.providers.identity import load_identity_resolver
from dev_health_ops.providers.status_mapping import load_status_mapping
from dev_health_ops.providers.teams import (
    build_project_key_resolver,
    load_team_resolver,
)
from dev_health_ops.providers.usage import (
    attach_partial_observations,
    drain_provider_usage,
    read_partial_observations,
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


class WorkItemUnitMissingSource(ValueError):
    def __init__(self, provider: str, source_kind: str) -> None:
        self.provider = provider
        self.source_kind = source_kind
        super().__init__(
            f"{provider} work-item unit had no source ({source_kind}); "
            "refusing org-wide fan-out"
        )


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


# CHAOS-4112: sources that mean "this item has no owning team". Anything else
# that carries a team_id is a real attribution, so falling from it to one of
# these across a recompute is a data loss, not a precedence change.
_TEAMLESS_ATTRIBUTION_SOURCES = frozenset({"unassigned", ""})


def _merge_stored_inheritable_edges(
    primary_sink: Any,
    org_id: str,
    work_items: Sequence[Any],
    merged_deps: dict[tuple[str, str, str], Any],
) -> int:
    """Union the STORED inheritable edges for the items being recomputed,
    minus the ones this run's provider snapshot proves were removed.

    CHAOS-4112: the donor preload used to consider this run's fresh edges
    only. Attribution rows are re-stamped every run, so once a PR's
    ``relates_to`` edge aged out of the sync window every later recompute
    rebuilt that PR as ``unassigned`` and superseded its own earlier, correct
    ``linked_issue`` attribution -- the recency of the EDGE decided, not the
    recency of the items.

    The naive union would fix that at the cost of never forgetting a removed
    link, because ``work_item_dependencies`` is insert-only and carries no
    tombstone. It does not have to: the producers RE-EXTRACT an item's links
    on every sync and stamp them ``last_synced=now`` (all four providers
    construct ``WorkItemDependency``, whose ``last_synced`` defaults to now),
    so a link still present upstream reappears in THIS run's fresh edges. An
    edge that is only in the store, for an item whose current sync DID produce
    edges, is therefore a link the provider has stopped emitting -- a removal.

    That is what the "extractor ran" proof below tests, keyed on
    ``(source_work_item_id, relationship_type_raw)``. A stored edge is dropped
    only when THAT extractor produced a fresh edge for THAT item this run --
    positive evidence it ran and simply did not re-emit this link. An
    item-level proof would be unsound: GitHub edges come from the PR body
    (always parsed) and from Linear linkback comments (gated by
    GITHUB_FETCH_COMMENTS, capped by GITHUB_COMMENTS_LIMIT), so a fresh body
    edge says nothing about whether comment extraction ran, and treating it as
    proof would delete stored `github_comment_linear_url` edges -- decaying
    exactly the population this ticket protects. Where no such evidence
    exists, "removed" and "that extractor did not run" are
    indistinguishable, so the stored edge is kept: never re-introducing the
    decay this function exists to remove.

    Verified against the dev store: 1,263 edges are stamped in the same pass
    as their source item, and every one of the 25 that lag their item belongs
    to an item that ALSO has fresh edges -- i.e. the extractor ran and those
    links were genuinely dropped upstream.

    Residual: removals are only detected per provenance, so an item that loses
    its LAST edge of a given kind produces no fresh edge of that kind and its
    stored one keeps donating until another appears. Closing that needs a
    sync-layer "this extractor ran and found nothing" marker (an empty-snapshot
    tombstone), tracked in **CHAOS-4129** -- follow-up machinery, not something
    this function can synthesise. The residual errs toward PRESERVING a team,
    the opposite failure direction from the decay this fix removes.

    Bounded to this run's items as the edge SOURCE and to
    ``_INHERITABLE_RELATIONSHIP_TYPES``, so this is a keyed read, never a
    history scan. A fresh edge stays authoritative for its own
    ``(source, target, relationship_type)``; a stored edge differing only by
    type is kept so ``build_linked_issue_team_resolver``'s ``latest_edge``
    collapse can settle the pair by ``last_synced`` -- which is what protects
    the retype case (``relates_to`` -> ``blocked_by``) that motivated the
    fresh-only rule.

    Returns the number of stored edges added, for logging and tests.
    """
    if not org_id or not work_items:
        return 0
    if not hasattr(primary_sink, "query_dicts"):
        return 0
    source_ids = sorted(
        {
            str(getattr(wi, "work_item_id", "") or "")
            for wi in work_items
            if getattr(wi, "work_item_id", "")
        }
    )
    if not source_ids:
        return 0

    # (source_work_item_id, relationship_type_raw) pairs this run produced.
    #
    # The pruning proof is PER PROVENANCE, not per item, because "the item was
    # re-synced" does not imply "every extractor that can produce its edges
    # ran". GitHub edges come from at least two places: the PR body, always
    # parsed, and Linear linkback COMMENTS, which are gated by
    # GITHUB_FETCH_COMMENTS and capped by GITHUB_COMMENTS_LIMIT. A PR whose
    # body edge is fresh while comments were disabled, capped or failed would,
    # under an item-level proof, have its stored `github_comment_linear_url`
    # edge deleted -- decaying exactly the Linear-linkback population this
    # ticket exists to protect.
    #
    # Keying the proof on relationship_type_raw means a stored edge is only
    # discarded when THAT extractor demonstrably ran and did not re-emit it.
    # A retype (`linear_relation:related` -> `linear_relation:blocks`) changes
    # the raw value, so the old row is not pruned here; `latest_edge`'s
    # recency collapse remains the backstop for that case.
    resynced_provenances = {
        (dep.source_work_item_id, dep.relationship_type_raw)
        for dep in merged_deps.values()
    }

    # One bounded retry. This read is now load-bearing for attribution
    # correctness, and a transient ClickHouse blip would otherwise drop the
    # run back to sync-window-only inheritance -- silently recreating the very
    # decay this function exists to prevent. Retrying costs one keyed lookup
    # and removes the most common failure mode.
    last_error: Exception | None = None
    for _attempt in range(2):
        added = 0
        try:
            rows = list(
                primary_sink.query_dicts(
                    "SELECT * FROM work_item_dependencies FINAL "
                    "WHERE org_id = {org_id:String} "
                    "  AND source_work_item_id IN {source_ids:Array(String)} "
                    "  AND relationship_type IN {rel_types:Array(String)}",
                    {
                        "org_id": org_id,
                        "source_ids": source_ids,
                        "rel_types": sorted(_INHERITABLE_RELATIONSHIP_TYPES),
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001 - retried, then reported below
            last_error = exc
            continue

        for row in rows:
            stored_dep = to_dataclass(WorkItemDependency, row)
            key = (
                stored_dep.source_work_item_id,
                stored_dep.target_work_item_id,
                stored_dep.relationship_type,
            )
            if key in merged_deps:
                continue
            provenance = (
                stored_dep.source_work_item_id,
                stored_dep.relationship_type_raw,
            )
            if provenance in resynced_provenances:
                # This extractor ran for this item and did not re-emit this
                # edge: the provider has removed the link.
                continue
            merged_deps[key] = stored_dep
            added += 1
        break
    else:
        # Both attempts failed. Inheritance falls back to the sync window for
        # THIS run, which is the pre-CHAOS-4112 behaviour: an item whose donor
        # edge is older than the window can be rebuilt as `unassigned`. The
        # next successful run restores it -- the stored edge is found again --
        # so the regression is transient and self-healing, unlike the original
        # bug. It is still a degraded window, so it is counted rather than
        # left to a log line nobody aggregates -- and it is doubly observable,
        # since the downgrade counter also fires on the degraded write.
        #
        # Shipping this rather than failing closed is a deliberate ruling: a
        # partial skip would trade a transient inconsistency for a cross-table
        # one, because compute stamps the same resolver's team onto
        # work_item_cycle_times and the state-duration rows too. Making all
        # three writers fail closed together is a design question tracked in
        # **CHAOS-4123**.
        logger.warning(
            "Stored inheritable edge load failed after retry; team "
            "inheritance is limited to the sync window for this run, so "
            "items whose donor edge is older than the window may be rebuilt "
            "as unassigned until the next successful run (CHAOS-4112)",
            exc_info=last_error,
        )
        STORED_EDGE_LOAD_FAILURES_TOTAL.labels(org_id=org_id).inc()
        return 0

    if added:
        logger.info(
            "Team inheritance: unioned %d stored inheritable edge(s) with the "
            "sync window's fresh edges",
            added,
        )
    return added


def _load_prior_primary_attributions(
    primary_sink: Any, org_id: str, work_item_ids: list[str]
) -> dict[str, tuple[str, str]]:
    """Primary (source, provider) per work item as stored BEFORE this run.

    Read before the recompute writes, so it is the state a downgrade would
    supersede. Bounded to the items being recomputed. Same latest-primary
    fence as every other reader of this table (FINAL + is_primary + the
    (work_item_id, max(computed_at)) tuple filter): without it an older
    candidate row can masquerade as the current attribution and manufacture a
    phantom downgrade.
    """
    if not org_id or not work_item_ids:
        return {}
    if not hasattr(primary_sink, "query_dicts"):
        return {}
    prior: dict[str, tuple[str, str]] = {}
    try:
        for row in primary_sink.query_dicts(
            "SELECT work_item_id, source, provider, team_id "
            "FROM work_item_team_attributions FINAL "
            "WHERE org_id = {org_id:String} "
            "  AND work_item_id IN {ids:Array(String)} "
            "  AND is_primary = 1 "
            "  AND (work_item_id, computed_at) IN ("
            "      SELECT work_item_id, max(computed_at) "
            "      FROM work_item_team_attributions "
            "      WHERE org_id = {org_id:String} "
            "        AND work_item_id IN {ids:Array(String)} "
            "      GROUP BY work_item_id)",
            {"org_id": org_id, "ids": sorted(set(work_item_ids))},
        ):
            work_item_id = str(row.get("work_item_id") or "")
            if not work_item_id:
                continue
            source = str(row.get("source") or "")
            team_id = str(row.get("team_id") or "").strip()
            # Only a row that actually carried a team can be downgraded FROM.
            if source in _TEAMLESS_ATTRIBUTION_SOURCES or not team_id:
                continue
            prior[work_item_id] = (source, str(row.get("provider") or ""))
    except Exception:
        # Telemetry must never fail the run it observes.
        logger.warning(
            "Prior attribution load failed; team-attribution downgrade "
            "telemetry is unavailable for this run",
            exc_info=True,
        )
        return {}
    return prior


def _report_attribution_downgrades(
    prior_primary: dict[str, tuple[str, str]],
    new_attributions: Sequence[Any],
) -> int:
    """Count and log items that lost a real team on this recompute.

    The decay CHAOS-4112 describes is silent by construction -- the rebuilt
    row simply supersedes the good one -- so this is the only place the
    transition is observable. Recoveries (unassigned -> teamed) and moves
    between two teamed sources are deliberately NOT counted; neither loses
    data.
    """
    downgraded = 0
    for record in new_attributions:
        if int(getattr(record, "is_primary", 0) or 0) != 1:
            continue
        work_item_id = str(getattr(record, "work_item_id", "") or "")
        previous = prior_primary.get(work_item_id)
        if previous is None:
            continue
        source = str(getattr(record, "source", "") or "")
        team_id = str(getattr(record, "team_id", "") or "").strip()
        if source not in _TEAMLESS_ATTRIBUTION_SOURCES and team_id:
            continue
        previous_source, previous_provider = previous
        downgraded += 1
        logger.warning(
            "Team attribution DOWNGRADED to unassigned on recompute: "
            "work_item_id=%s previous_source=%s new_source=%s. A stored "
            "inheritable edge or donor attribution that previously resolved "
            "this item no longer does (CHAOS-4112).",
            work_item_id,
            previous_source,
            source or "unassigned",
        )
        ATTRIBUTION_DOWNGRADES_TOTAL.labels(
            provider=str(getattr(record, "provider", "") or previous_provider or ""),
            previous_source=previous_source,
        ).inc()
    if downgraded:
        logger.warning(
            "%d work item(s) lost a previously-attributed team on this "
            "recompute (CHAOS-4112 decay signature)",
            downgraded,
        )
    return downgraded


def _date_range(end_day: date, backfill_days: int) -> list[date]:
    if backfill_days <= 1:
        return [end_day]
    start_day = end_day - timedelta(days=backfill_days - 1)
    return [start_day + timedelta(days=i) for i in range(backfill_days)]


def _has_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _repo_settings_dict(repo: Any) -> dict[str, Any]:
    """Return ``repo.settings`` as a dict regardless of on-disk representation.

    ``discover_repos`` (job_daily.py) parses the ClickHouse ``settings`` JSON
    string into a dict, but this stays defensive: other callers (backfill,
    directly-constructed ``DiscoveredRepo`` instances, tests) may still hand
    back a raw JSON string, ``None``, or an already-parsed dict. Malformed
    JSON or an unexpected type yields ``{}`` — the CHAOS-2763 gitlab numeric-id
    match then fails closed rather than raising.
    """
    settings = getattr(repo, "settings", None)
    if isinstance(settings, dict):
        return settings
    if isinstance(settings, str):
        try:
            parsed = json.loads(settings)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _require_work_item_unit_source(
    *,
    provider_set: set[str],
    repo_name: str | None,
    jira_project_keys: list[str] | None,
) -> None:
    required_sources = {
        "github": (repo_name, "repo"),
        "gitlab": (repo_name, "project"),
        "linear": (repo_name, "team"),
    }
    for provider, (source, source_kind) in required_sources.items():
        if provider in provider_set and not _has_text(source):
            error = WorkItemUnitMissingSource(provider, source_kind)
            logger.error(str(error))
            raise error

    if "jira" in provider_set and not any(
        _has_text(project_key) for project_key in (jira_project_keys or [])
    ):
        error = WorkItemUnitMissingSource("jira", "project_keys")
        logger.error(str(error))
        raise error


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


def _build_work_item_observations(
    *,
    github_usage: list[dict[str, Any]],
    provider_usage: list[dict[str, Any]],
    linear_page_count: int,
    linear_batch_count: int,
    include_linear_counts: bool,
) -> dict[str, Any]:
    """Assemble the unit-result ``observations`` fragment.

    Emits the provider-neutral ``provider_usage`` key alongside the legacy
    ``github_usage`` key (kept intact for pinned tests + admin consumers).
    """

    observations: dict[str, Any] = {}
    if github_usage:
        observations["github_usage"] = github_usage
    if provider_usage:
        observations["provider_usage"] = provider_usage
    if include_linear_counts:
        observations["linear_page_count"] = linear_page_count
        observations["linear_batch_count"] = linear_batch_count
    return observations


def attach_work_item_partial_observations(
    exc: BaseException, observations: dict[str, Any]
) -> None:
    """Stash partial usage observations on an in-flight exception (no-op when
    empty) so a rate-limit deferral / failure can still record actuals.

    Delegates to the provider-neutral ``providers.usage.attach_partial_observations``
    (CHAOS-2803/CS2), which now owns the exception-attribute mechanism; kept
    here (same name/behavior) since this is the work-items sync job's public
    call site and is pinned by existing tests.
    """

    attach_partial_observations(exc, observations)


def read_work_item_partial_observations(
    exc: BaseException,
) -> dict[str, Any] | None:
    """Read observations attached by :func:`attach_work_item_partial_observations`.

    Delegates to ``providers.usage.read_partial_observations`` (CHAOS-2803/CS2)
    -- same underlying exception attribute, so this also reads observations
    attached via the provider-neutral ``attach_partial_observations`` alias
    (e.g. from the code-dataset adapters).
    """

    return read_partial_observations(exc)


def _merge_github_project_v2_rows(
    repository_items: list[WorkItem],
    repository_transitions: list[Any],
    project_items: list[WorkItem],
    project_transitions: list[Any] | None,
) -> tuple[list[WorkItem], list[Any]]:
    """Preserve the active producer's ordered last-wins/append composition."""
    by_id = {item.work_item_id: item for item in repository_items}
    for item in project_items:
        by_id[item.work_item_id] = item
    return list(by_id.values()), [
        *repository_transitions,
        *(project_transitions or []),
    ]


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
    comments_limit: int | None = None,
    require_source: bool = False,
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

    # Usage accumulators are declared before the try so the failure/deferral
    # path (the ``except`` below) can attach whatever actuals were gathered
    # before a mid-sync raise (CHAOS-2754).
    github_usage_observations: list[dict[str, Any]] = []
    provider_usage_observations: list[dict[str, Any]] = []
    linear_page_count = 0
    linear_batch_count = 0

    try:
        for s in sinks:
            s.ensure_tables()

        _teams_data = (
            primary_sink.query_dicts(
                "SELECT id, argMax(name, updated_at) AS name, "
                "argMax(project_keys, updated_at) AS project_keys, provider, native_team_key "
                "FROM teams"
                + (" WHERE org_id = {org_id:String}" if org_id else "")
                + " GROUP BY id, provider, native_team_key, org_id",
                {"org_id": org_id} if org_id else {},
            )
            if hasattr(primary_sink, "query_dicts")
            else []
        )
        pk_resolver = build_project_key_resolver(_teams_data)
        from dev_health_ops.models.work_items import Sprint

        _sprints_data = (
            primary_sink.query_dicts(
                "SELECT provider, sprint_id, argMax(name, last_synced) AS name, argMax(state, last_synced) AS state, "
                "argMax(started_at, last_synced) AS started_at, argMax(ended_at, last_synced) AS ended_at, "
                "argMax(completed_at, last_synced) AS completed_at, max(last_synced) AS last_synced_max, "
                "argMax(native_team_key, last_synced) AS native_team_key, org_id FROM sprints"
                + (" WHERE org_id = {org_id:String}" if org_id else "")
                + " GROUP BY provider, sprint_id, org_id",
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
                native_team_key=row.get("native_team_key") or None,
                last_synced=row.get("last_synced_max") or computed_at,
                org_id=str(row.get("org_id") or ""),
            )
            for row in _sprints_data
        ]

        if require_source:
            _require_work_item_unit_source(
                provider_set=provider_set,
                repo_name=repo_name,
                jira_project_keys=jira_project_keys,
            )

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

        # CHAOS-2720 / CHAOS-2763: A config-aware work-item unit carries exactly
        # one source repo/project in ``repo_name``. ``_discover_repos`` only
        # short-circuits on ``repo_id`` and otherwise returns every repo for the
        # org, so without scoping the GitHub/GitLab ingest loops below run a
        # full ingest per org repo — one unit fanning out into N-repo API
        # amplification. Scope the discovered repos to the unit's source so a
        # unit ingests only its own repo/project.
        #
        # GitHub: matching is case-insensitive on ``full_name`` because both
        # the source ``external_id`` and ``repos.repo`` are the GitHub
        # ``owner/repo`` slug (admin sync planner, api/admin/routers/sync.py).
        #
        # GitLab (below, after this block): the unit's ``repo_name`` is the
        # numeric GitLab project id (``IntegrationSource.external_id``), which
        # never matches ``repos.repo`` (``path_with_namespace``) — so
        # ASCII-decimal inputs match instead against the immutable
        # ``settings.project_id`` captured at code-dataset sync time. Only
        # path-shaped (non-numeric) inputs — e.g. CLI ``--repo-name
        # grp/proj`` — fall back to the case-insensitive ``full_name`` match; a
        # numeric id never falls back to ``full_name`` (a stale row's mutable
        # path could otherwise coincidentally equal the id string).
        #
        # Only GitHub/GitLab-source repos are touched by their respective
        # blocks; every other source passes through unchanged. When
        # ``repo_name`` is absent (CLI/org-wide), discovery stays org-wide as
        # before.
        if repo_name and "github" in provider_set:
            wanted = repo_name.strip().lower()
            scoped: list[Any] = []
            dropped = 0
            for repo in discovered_repos:
                if (
                    repo.source == "github"
                    and (repo.full_name or "").strip().lower() != wanted
                ):
                    dropped += 1
                    continue
                scoped.append(repo)
            if dropped:
                logger.info(
                    "Scoped GitHub work-item unit to source repo '%s': "
                    "dropped %d off-source repo(s)",
                    repo_name,
                    dropped,
                )
            discovered_repos = scoped

        # CHAOS-2763: gitlab twin of the GitHub scoping block above. See the
        # comment there for the full rationale; summary: ASCII-decimal
        # ``repo_name`` (the unit's canonical source-id shape) matches ONLY
        # ``settings.project_id`` (never falls back to ``full_name`` —
        # str.isdigit()-style non-ASCII digits are deliberately excluded via
        # re.fullmatch so they fall into the path-shaped branch instead).
        # Path-shaped ``repo_name`` (e.g. CLI ``--repo-name grp/proj``) matches
        # ``full_name`` case-insensitively, same as GitHub.
        #
        # gitlab_id_scoped_project_ids [codex HIGH]: matching a row by its
        # immutable project_id does NOT make ``repo.full_name`` safe to fetch
        # by. If project 123 is renamed/moved after discovery and its old
        # path is later reused by a *different* project, a unit for id "123"
        # still matches this row by project_id, but fetching by the stale
        # ``full_name`` would silently pull the wrong project's issues/MRs
        # and write them under this row's repo_id — defeating the isolation
        # the id match exists to provide. So every numeric-id-matched repo's
        # immutable project id is recorded here and threaded into
        # ``fetch_gitlab_work_items`` to use for the actual GitLab API calls;
        # ``full_name`` is kept on the (unmodified) ``DiscoveredRepo`` for
        # display/normalization only. Path-matched and org-wide (no-source)
        # repos are NOT added here, so they keep fetching by ``full_name`` —
        # unchanged, existing behavior; only an id-matched unit's identifier
        # resolution changes.
        #
        # CHAOS-2801 [codex HIGH, PR #1143 round-3]: numeric ``project_id``
        # is only unique WITHIN one GitLab instance — two GitLab integrations
        # in the same org (different self-hosted instances, or one self-hosted
        # + gitlab.com) can both expose ``project_id=123``. Without an
        # instance check, a unit authenticated to instance A could match
        # instance B's row purely on the numeric id, then fetch project 123
        # from A and persist/write the result under B's ``repo_id``.
        #
        # Resolve this unit's authenticated GitLab client (token + base URL)
        # up front — *before* the id-matching loop, not after it as before —
        # so ``gitlab_unit_instance`` is available at match time. This is a
        # reordering only: ``_build_gitlab_work_client`` was already called
        # unconditionally whenever "gitlab" is in ``provider_set`` (previously
        # just after this block, at the ``fetch_gitlab_work_items`` call
        # site); moving it earlier does not change whether/when it can raise.
        #
        # Design semantic — the three-case instance rule (documented in
        # docs/architecture/sync-unit-model.md and the CHAOS-2801 PR body).
        # ``normalize_gitlab_instance`` (providers/gitlab/instance.py) is the
        # SINGLE normalizer, shared with the write sites that persist
        # ``settings.gitlab_instance_url`` — never fork a second copy
        # (default-port/case/path spelling differences would otherwise
        # false-mismatch, codex MED PR #1148 round-1).
        #
        # When the unit's instance is KNOWN, per project_id:
        #   (a) a same-project_id row with a MATCHING discriminator exists
        #       -> scope to the discriminated match(es) ONLY; mismatching
        #       and undiscriminated (legacy) rows are dropped. Without the
        #       legacy drop, absent-accept would act as a CO-MATCH (codex
        #       HIGH, PR #1148 round-1): both repo_ids would enter
        #       ``gitlab_id_scoped_project_ids`` and the single fetch
        #       against this unit's client would also be written under the
        #       legacy (possibly other-instance) row's ``repo_id``.
        #   (b) NO discriminated row exists at all for this project_id ->
        #       ACCEPT legacy rows. This is the compatibility pin: a
        #       pure-legacy org (every row written before this change) sees
        #       zero behavior change.
        #   (c) only MISMATCHING discriminated row(s) exist -> FAIL CLOSED
        #       for this project_id: the mismatch rows are rejected AND the
        #       legacy rows are dropped too (codex HIGH, PR #1148 round-2).
        #       A known mismatching discriminator PROVES cross-instance
        #       ambiguity exists for this numeric id — the legacy row is
        #       plausibly that other instance's pre-discriminator row, so
        #       accepting it risks the exact wrong-repo_id write this fix
        #       closes. Nothing matches, so the existing CHAOS-2737
        #       ``require_source`` path below raises with its audit log;
        #       remediation is re-discovery, which now stamps
        #       discriminators on every row.
        # Cases (a)+(c) collapse to one predicate: a legacy row is accepted
        # ONLY when no same-project_id row carries ANY known discriminator.
        # When the unit's instance is UNKNOWN (e.g. a bare CLI run with no
        # resolvable base URL), the check never engages; behavior matches
        # pre-CHAOS-2801.
        #
        # This never changes GitHub-path behavior and never weakens the
        # existing CHAOS-2737 fail-closed ``require_source`` path below — a
        # rejected row simply does not count as "discovered", so a unit with
        # no other matching row still raises exactly as before.
        gl_token: str = ""
        gl_url: str | None = None
        gitlab_unit_instance: str | None = None
        if "gitlab" in provider_set:
            gl_token, gl_url = _build_gitlab_work_client(
                org_id=org_id, credentials=credentials
            )
            gitlab_unit_instance = normalize_gitlab_instance(gl_url)

        gitlab_id_scoped_project_ids: dict[uuid.UUID, str] = {}
        if repo_name and "gitlab" in provider_set:
            wanted_gl = repo_name.strip()
            is_numeric_id = re.fullmatch(r"[0-9]+", wanted_gl) is not None

            def _gl_id_match(settings: dict[str, Any]) -> bool:
                project_id = settings.get("project_id")
                return project_id is not None and str(project_id).strip() == wanted_gl

            # Pre-pass (codex HIGH, PR #1148 rounds 1+2): does ANY
            # same-project_id row carry a KNOWN discriminator, and does any
            # of those MATCH this unit's instance? A legacy
            # (no-discriminator) row is accepted below ONLY when no known
            # discriminator exists at all for this project_id — a matching
            # one shadows it (case a), and a mismatching-only one proves
            # cross-instance ambiguity and fails closed (case c).
            has_discriminated_row = False
            has_discriminated_match = False
            if is_numeric_id and gitlab_unit_instance is not None:
                for repo in discovered_repos:
                    if repo.source != "gitlab":
                        continue
                    settings = _repo_settings_dict(repo)
                    if not _gl_id_match(settings):
                        continue
                    row_instance = normalize_gitlab_instance(
                        settings.get("gitlab_instance_url")
                    )
                    if row_instance is not None:
                        has_discriminated_row = True
                        if row_instance == gitlab_unit_instance:
                            has_discriminated_match = True
                            break

            scoped_gl: list[Any] = []
            dropped_gl = 0
            dropped_gl_instance_mismatch = 0
            dropped_gl_shadowed_legacy = 0
            dropped_gl_ambiguous_legacy = 0
            for repo in discovered_repos:
                if repo.source != "gitlab":
                    scoped_gl.append(repo)
                    continue
                settings = _repo_settings_dict(repo)
                if is_numeric_id:
                    matched = _gl_id_match(settings)
                    if matched and gitlab_unit_instance is not None:
                        row_instance = normalize_gitlab_instance(
                            settings.get("gitlab_instance_url")
                        )
                        if row_instance is not None and row_instance != (
                            gitlab_unit_instance
                        ):
                            matched = False
                            dropped_gl_instance_mismatch += 1
                        elif row_instance is None and has_discriminated_row:
                            # Some same-project_id row has a KNOWN
                            # discriminator, so this legacy row must not
                            # match: shadowed by a matching row (case a) or
                            # fail-closed on cross-instance ambiguity when
                            # only mismatching rows exist (case c).
                            matched = False
                            if has_discriminated_match:
                                dropped_gl_shadowed_legacy += 1
                            else:
                                dropped_gl_ambiguous_legacy += 1
                else:
                    matched = (
                        repo.full_name or ""
                    ).strip().lower() == wanted_gl.lower()
                if not matched:
                    dropped_gl += 1
                    continue
                if is_numeric_id:
                    gitlab_id_scoped_project_ids[repo.repo_id] = wanted_gl
                scoped_gl.append(repo)
            if dropped_gl:
                logger.info(
                    "Scoped GitLab work-item unit to source project '%s': "
                    "dropped %d off-source repo(s)",
                    repo_name,
                    dropped_gl,
                )
            if dropped_gl_instance_mismatch:
                logger.warning(
                    "CHAOS-2801: dropped %d GitLab repo(s) matching project_id "
                    "'%s' but a different instance than this unit's "
                    "authenticated GitLab host — cross-instance numeric-id "
                    "collision, not this unit's project",
                    dropped_gl_instance_mismatch,
                    repo_name,
                )
            if dropped_gl_shadowed_legacy:
                logger.warning(
                    "CHAOS-2801: dropped %d legacy GitLab repo row(s) with no "
                    "instance discriminator for project_id '%s' — a row "
                    "discriminated to this unit's instance already matched, "
                    "so the undiscriminated row(s) must not co-match (they "
                    "may belong to a different instance); re-discovery will "
                    "stamp them",
                    dropped_gl_shadowed_legacy,
                    repo_name,
                )
            if dropped_gl_ambiguous_legacy:
                logger.warning(
                    "CHAOS-2801: dropped %d legacy GitLab repo row(s) with no "
                    "instance discriminator for project_id '%s' — another "
                    "row carries a KNOWN, MISMATCHING instance discriminator "
                    "for the same project_id, proving cross-instance "
                    "ambiguity; failing closed rather than risking a "
                    "wrong-instance write. Remediation: re-run discovery "
                    "(it now stamps gitlab_instance_url on every row)",
                    dropped_gl_ambiguous_legacy,
                    repo_name,
                )
            discovered_repos = scoped_gl

        if require_source:
            if repo_name and {"github", "gitlab"}.intersection(provider_set):
                provider_sources = {repo.source for repo in discovered_repos}
                expected = {
                    provider
                    for provider in provider_set
                    if provider in {"github", "gitlab"}
                }
                if not expected.issubset(provider_sources):
                    logger.error(
                        "Work-item unit source was not discovered",
                        extra={"repo_name": repo_name, "providers": sorted(expected)},
                    )
                    raise ValueError(
                        f"Work-item unit source was not discovered: {repo_name}"
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

        if "jira" in provider_set:
            jira_client = _build_jira_work_client(
                org_id=org_id, credentials=credentials
            )
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
                client=jira_client,
                project_keys=jira_project_keys,
                jql_override=jira_jql,
                fetch_all=jira_fetch_all,
                use_env_query_options=not bool(org_id or credentials),
                reference_sprints=reference_sprints,
                reference_sink=primary_sink,
            )
            provider_usage_observations.extend(drain_provider_usage(jira_client))
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
                        comments_limit=comments_limit,
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
                    raw_provider_usage = batch.observations.get("provider_usage")
                    if isinstance(raw_provider_usage, list):
                        provider_usage_observations.extend(
                            item
                            for item in raw_provider_usage
                            if isinstance(item, dict)
                        )

            projects = parse_github_projects_v2_env()
            if projects:
                proj_items, proj_tr = fetch_github_project_v2_items(
                    projects=projects,
                    status_mapping=status_mapping,
                    identity=identity,
                )
                work_items, transitions = _merge_github_project_v2_rows(
                    work_items, transitions, proj_items, proj_tr
                )

        if "gitlab" in provider_set:
            # gl_token/gl_url were already resolved above (before the
            # CHAOS-2801 instance-scoping block) so the unit's authenticated
            # instance is known at match time; reused here rather than
            # re-resolved.
            items, tr, gl_ai_attributions = fetch_gitlab_work_items(
                repos=discovered_repos,
                since=since_dt,
                status_mapping=status_mapping,
                identity=identity,
                token=gl_token,
                gitlab_url=gl_url,
                include_label_events=True,
                org_id=org_id,
                usage_observations=provider_usage_observations,
                id_scoped_project_ids=gitlab_id_scoped_project_ids,
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

            linear_repo_name = repo_name.strip() if repo_name else None
            if (repo_name is not None and not linear_repo_name) or (
                require_source and not linear_repo_name
            ):
                logger.error("Linear work-item sync received an empty source context")
                raise ValueError(
                    "Linear work-item sync requires a non-empty source team key"
                )

            linear_client = _build_linear_work_client(
                org_id=org_id, credentials=credentials
            )
            linear_provider = LinearProvider(
                status_mapping=status_mapping,
                identity=identity,
                client=linear_client,
            )
            ctx = IngestionContext(
                window=IngestionWindow(updated_since=since_dt, active_until=until_dt),
                repo=linear_repo_name,
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
            provider_usage_observations.extend(drain_provider_usage(linear_client))

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

        # Stamp org_id on every tenant-partitioned raw work-item row before
        # writing to sinks. Leaving reopen, interaction, or sprint rows at the
        # dataclass default ("") collapses unrelated organizations onto the
        # same ClickHouse tenant key and makes org-scoped reads miss the data.
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
            reopen_events = [
                replace(event, org_id=org_id) if hasattr(event, "org_id") else event
                for event in reopen_events
            ]
            interactions = [
                replace(event, org_id=org_id) if hasattr(event, "org_id") else event
                for event in interactions
            ]
            sprints = [
                replace(sprint, org_id=org_id) if hasattr(sprint, "org_id") else sprint
                for sprint in sprints
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
        # run, but they are NOT the whole edge set. Attribution rows are
        # recomputed and re-stamped on every run, so using the fresh edges
        # alone meant that once a PR's `relates_to` edge aged out of the sync
        # window, every later recompute rebuilt that PR as `unassigned` and
        # SUPERSEDED its own earlier, correct `linked_issue` attribution
        # (CHAOS-4112: 69 items org-wide were wrongly unassigned this way; the
        # recency of the EDGE, not of the items, decided). We therefore union
        # the STORED inheritable edges for the items being recomputed with the
        # fresh ones, and let `build_linked_issue_team_resolver`'s existing
        # `latest_edge` collapse settle any conflict by `last_synced` — which
        # is what protects the stale-edge case that motivated the fresh-only
        # rule: a relationship retyped `relates_to` -> `blocked_by` arrives
        # fresh with a newer timestamp and supersedes the stored row.
        #
        # Residual, accepted deliberately on this ticket: work_item_dependencies
        # is insert-only (ReplacingMergeTree keyed on
        # (source, target, relationship_type)), so a link genuinely REMOVED
        # from a PR leaves no tombstone and its stored row keeps donating. The
        # ticket weighs that against 69 silently-decayed attributions and
        # chooses the union; a real fix needs edge deletion at the sync layer.
        #
        # The donor *items* the edges point at may have been synced earlier, so
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

        _merge_stored_inheritable_edges(primary_sink, org_id, work_items, merged_deps)

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

        # CHAOS-4112 telemetry: snapshot the stored primary attributions
        # BEFORE this run overwrites them, so a teamed -> unassigned
        # transition can be detected. Loaded once per run, not per day: the
        # attribution compute below does not vary with the day, so reporting
        # inside the loop would multiply every downgrade by the backfill
        # window.
        prior_primary_attributions = _load_prior_primary_attributions(
            primary_sink, org_id, [wi.work_item_id for wi in work_items]
        )
        attribution_downgrades_reported = False

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
            estimate_coverage_metrics = compute_estimate_coverage_metrics_daily(
                day=d,
                work_items=work_items,
                computed_at=computed_at,
                team_resolver=team_resolver,
                project_key_resolver=pk_resolver,
                linked_issue_resolver=linked_issue_resolver,
                attribution_context=team_attribution_context,
            )
            wi_team_attributions = compute_work_item_team_attributions(
                work_items=work_items,
                computed_at=computed_at,
                team_resolver=team_resolver,
                project_key_resolver=pk_resolver,
                linked_issue_resolver=linked_issue_resolver,
                attribution_context=team_attribution_context,
            )
            if not attribution_downgrades_reported:
                attribution_downgrades_reported = True
                _report_attribution_downgrades(
                    prior_primary_attributions, wi_team_attributions
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

            (
                issue_type_metrics_rows,
                investment_classifications,
                investment_metrics_rows,
            ) = compute_work_item_engine_destinations_daily(
                day=d,
                work_items=work_items,
                computed_at=computed_at,
                org_id=org_id,
                status_mapping=status_mapping,
                investment_classifier=investment_classifier,
                team_resolver=team_resolver,
                project_key_resolver=pk_resolver,
                linked_issue_resolver=linked_issue_resolver,
                attribution_context=team_attribution_context,
            )

            for s in sinks:
                if wi_metrics:
                    _ensure_unit_lease_for_write("work_item_metrics_daily")
                    s.write_work_item_metrics(wi_metrics)
                if estimate_coverage_metrics and hasattr(
                    s, "write_estimate_coverage_metrics"
                ):
                    _ensure_unit_lease_for_write("estimate_coverage_metrics_daily")
                    s.write_estimate_coverage_metrics(estimate_coverage_metrics)
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
        observations = _build_work_item_observations(
            github_usage=github_usage_observations,
            provider_usage=provider_usage_observations,
            linear_page_count=linear_page_count,
            linear_batch_count=linear_batch_count,
            include_linear_counts="linear" in provider_set,
        )
        if observations:
            return {"observations": observations}
        return None
    except Exception as exc:
        # Preserve actuals gathered before the raise (partial fetch) so the
        # worker's rate-limit deferral / failure stamp can persist them
        # (CHAOS-2754). Never suppresses the error.
        attach_work_item_partial_observations(
            exc,
            _build_work_item_observations(
                github_usage=github_usage_observations,
                provider_usage=provider_usage_observations,
                linear_page_count=linear_page_count,
                linear_batch_count=linear_batch_count,
                include_linear_counts="linear" in provider_set,
            ),
        )
        raise
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
