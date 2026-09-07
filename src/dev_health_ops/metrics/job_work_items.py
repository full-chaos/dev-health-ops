from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date, timedelta
from typing import Any

from dev_health_ops.metrics.compute_work_items import (
    _INHERITABLE_RELATIONSHIP_TYPES,
)
from dev_health_ops.metrics.loaders.base import to_dataclass
from dev_health_ops.metrics.team_attribution_telemetry import (
    STORED_EDGE_LOAD_FAILURES_TOTAL,
)
from dev_health_ops.models.work_items import WorkItem, WorkItemDependency
from dev_health_ops.providers.usage import (
    attach_partial_observations,
    read_partial_observations,
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


# CHAOS-5321/CHAOS-3092 (R6): _load_prior_primary_attributions and
# _report_attribution_downgrades (CHAOS-4112 team-attribution downgrade
# telemetry) are DELETED -- their only caller was this function's own
# compute_work_item_team_attributions call, deleted alongside them (native
# Go executor + providersync ingest derivation are the only writers of
# work_item_team_attributions now; see the deletion comment above the sync
# loop below). ATTRIBUTION_DOWNGRADES_TOTAL (team_attribution_telemetry.py) and
# _TEAMLESS_ATTRIBUTION_SOURCES are deleted with them -- both had no other
# caller. Their dedicated tests (test_team_inheritance_decay.py's downgrade-
# specific cases) are deleted too.


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

