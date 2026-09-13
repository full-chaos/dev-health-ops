from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Mapping
from typing import Any, cast

from dev_health_ops.metrics.prometheus import (
    record_team_autoimport_reference_category_outcome,
)
from dev_health_ops.providers.team_capabilities import team_provider_capabilities
from dev_health_ops.workers.team_autoimport_categories import (
    import_categories_from_sync_options,
)

logger = logging.getLogger(__name__)

_IMPORTER_MODULES = {
    "linear": "dev_health_ops.workers.team_autoimport_linear",
    "jira": "dev_health_ops.workers.team_autoimport_jira",
    "github": "dev_health_ops.workers.team_autoimport_github",
    "gitlab": "dev_health_ops.workers.team_autoimport_gitlab",
}

# CHAOS-4555: CHAOS-4431's Go collector is the sole live writer for Linear
# team/project/ownership catalog rows (prod deploy 5.6+; CHAOS-4530 proved
# and #2012 deleted the {org}:linear:{team} pseudo-project artifact this
# Python populator still emits at team_autoimport_linear.py:463-465).
#
# Enumerated by mechanism (not by route name -- chris 2026-08-29, "Not
# again": 4466/4495/4493 each missed a sibling caller outside the named
# producer), every path that can reach this module's populator dispatch for
# provider=linear:
#   - HTTP bridge (worker_sync.py), all reachable with WORKER_OPERATIONAL_
#     BRIDGE_TOKEN and no provider scoping of their own:
#       * POST /team-autoimport -> run_post_sync_team_autoimport() ->
#         run_team_autoimport (proved live-local: HTTP 200, recreated the
#         pseudo-project, before this guard existed). CHAOS-3093 (PR2b)
#         dropped the `@celery_app.task` decorator that used to wrap this
#         call (Celery has had zero consumers since CHAOS-4026); the bridge
#         now calls the plain function directly instead of its former
#         `.run()`.
#       * POST /reference-discovery-populate ->
#         run_reference_discovery_populate_for_sync_run ->
#         run_reference_discovery_populate_strict -> run_team_autoimport_strict.
#       * POST /reference-discovery -> run_sync_reference_discovery.run() ->
#         (same run_reference_discovery_populate_strict call, reference_discovery.py:135).
#   - Go side (internal/, cmd/dev-health-worker/): every one of the three
#     HTTP calls above is Fallback-only. teamCatalogAutoimportBridge.
#     TeamAutoImport (team_catalog_clients.go:426-536) resolves the sync
#     run's own provider and, for any provider in its `native` map (linear
#     since #1989/27bef7286), runs the Go collector and `return`s WITHOUT
#     ever calling the wrapped CoordinatorBridge.TeamAutoImport --
#     structurally unreachable for linear. TeamCatalogDiscoveryExecutor.
#     Discover (team_catalog_discovery_executor.go:137-143) is the same
#     shape for the other two: `Native[provider]` always hits for linear,
#     so `Fallback.Discover` (bridge_discovery_executor.go, which itself
#     only calls PopulateReferenceDiscovery -> /reference-discovery-populate,
#     never HTTPBridge.Discover -> /reference-discovery) never runs.
#     HTTPBridge.Discover exists only to satisfy the CoordinatorBridge
#     interface; nothing in cmd/dev-health-worker invokes it -- the native
#     reference-discovery River worker (worker.go:308) calls
#     NativeReferenceDiscoveryService.Discover, which is the executor chain
#     above, not the bridge directly.
#   - backfill/runner.py (CHAOS-4498): no longer calls run_team_autoimport_
#     strict in-process for any provider; it arms the same ledger/outbox
#     row sync-time dispatch uses and goes through the identical
#     TeamCatalogDiscoveryExecutor chain above.
#   - Celery: retired (CHAOS-4026) -- no consumer drains `apply_async`; the
#     bridge call sites above execute synchronously (team-autoimport's own
#     `@celery_app.task` decorator is gone too, CHAOS-3093 PR2b -- it is now
#     a plain function call, not `.run()`).
#
# So every live and dead-but-reachable path funnels through this module's
# _resolve_populator/run_team_autoimport(_strict). Refuse at the lowest
# shared layer -- _resolve_populator itself -- so no current or future
# caller, of this function or of run_team_autoimport(_strict), can ever
# resolve the Linear writer again.
#
# jira joined this set once its own Go collector (JiraTeamCatalogCollector,
# registered in nativeTeamCatalogCollectors) went live -- the same
# structural argument applies verbatim, with jira substituted for linear
# throughout. It was the last provider team_provider_capabilities() lists
# still resolving a Python populator module here, so every path this
# function's docstring enumerates is now refused for every provider: the
# HTTP bridge routes above are unreachable for team-autoimport regardless of
# which provider a caller names.
_GO_NATIVE_PROVIDERS = frozenset({"linear", "jira"})

TeamAutoimportPopulator = Callable[..., dict[str, Any]]


def _zero_summary(*, provider: str, org_id: str, reason: str) -> dict[str, Any]:
    return {
        "status": "skipped",
        "provider": provider,
        "org_id": org_id,
        "reason": reason,
        "projects_imported": 0,
        "members_imported": 0,
        "team_memberships_imported": 0,
        "team_project_ownership_imported": 0,
        "team_repo_ownership_imported": 0,
        "work_item_team_attributions_imported": 0,
    }


def _provider_capability(provider: str) -> bool:
    normalized = provider.strip().lower()
    return any(
        capability.provider == normalized and capability.supports_org_drift_discovery
        for capability in team_provider_capabilities()
    )


def _resolve_populator(provider: str) -> TeamAutoimportPopulator | None:
    normalized = provider.strip().lower()
    if normalized in _GO_NATIVE_PROVIDERS:
        # CHAOS-4555: the actual refusal. Every caller of this function --
        # today's run_team_autoimport/run_team_autoimport_strict and any
        # future one -- gets None here for linear, same as an unregistered
        # provider, regardless of which of the three bridge routes (or a
        # not-yet-written caller) reached it.
        return None
    module_name = _IMPORTER_MODULES.get(normalized)
    if module_name is None:
        return None
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        return None
    populate = getattr(module, "populate", None)
    if not callable(populate):
        return None
    return cast(TeamAutoimportPopulator, populate)


def run_team_autoimport(
    *,
    provider: str,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any] | None = None,
    analytics_db_url: str | None = None,
) -> dict[str, Any]:
    normalized_provider = provider.strip().lower()
    if normalized_provider in _GO_NATIVE_PROVIDERS:
        # Redundant defence: _resolve_populator already refuses this
        # provider (would fall through to the generic "populator_not_
        # available" skip below either way). Kept explicit so this
        # function's own log line/reason names CHAOS-4555 instead of
        # reading as an ordinary missing-module case.
        logger.info(
            "Skipping team auto-import for provider=%s org_id=%s: writer is Go-native (CHAOS-4555)",
            normalized_provider,
            org_id,
        )
        return _zero_summary(
            provider=normalized_provider,
            org_id=org_id,
            reason="provider_writer_is_go_native",
        )
    if not _provider_capability(normalized_provider):
        logger.info(
            "Skipping team auto-import for provider=%s org_id=%s: provider is not import-capable",
            normalized_provider,
            org_id,
        )
        return _zero_summary(
            provider=normalized_provider,
            org_id=org_id,
            reason="provider_not_import_capable",
        )

    populator = _resolve_populator(normalized_provider)
    if populator is None:
        logger.info(
            "Skipping team auto-import for provider=%s org_id=%s: no populator module is available",
            normalized_provider,
            org_id,
        )
        return _zero_summary(
            provider=normalized_provider,
            org_id=org_id,
            reason="populator_not_available",
        )

    # CHAOS-4323: the single auto_import_teams flag became three independent
    # booleans (auto_import_teams/auto_import_projects/auto_import_members).
    # This is the ONLY call site that threads the resulting selection into the
    # populator scope -- run_team_autoimport_strict deliberately does not, so
    # reference discovery and backfill keep importing everything they always
    # have (see team_autoimport_categories module docstring).
    scope_sync_options = (scope or {}).get("sync_options")
    import_categories = import_categories_from_sync_options(
        scope_sync_options if isinstance(scope_sync_options, Mapping) else None
    )
    if not any(import_categories.values()):
        logger.info(
            "Skipping team auto-import for provider=%s org_id=%s: no category selected",
            normalized_provider,
            org_id,
        )
        return _zero_summary(
            provider=normalized_provider,
            org_id=org_id,
            reason="no_categories_selected",
        )

    try:
        populator_scope = dict(scope or {})
        populator_scope["import_categories"] = import_categories
        if analytics_db_url:
            populator_scope["analytics_db"] = analytics_db_url

        summary = populator(
            org_id=org_id,
            credentials=credentials,
            scope=populator_scope,
        )
    except Exception as exc:
        logger.exception(
            "Team auto-import failed for provider=%s org_id=%s; sync result remains successful",
            normalized_provider,
            org_id,
        )
        return {
            **_zero_summary(
                provider=normalized_provider,
                org_id=org_id,
                reason="populator_error",
            ),
            "error": str(exc),
        }

    if not isinstance(summary, Mapping):
        logger.warning(
            "Team auto-import populator for provider=%s org_id=%s returned non-mapping summary",
            normalized_provider,
            org_id,
        )
        return {
            **_zero_summary(
                provider=normalized_provider,
                org_id=org_id,
                reason="invalid_populator_summary",
            ),
            "summary_type": type(summary).__name__,
        }

    return {
        "status": "success",
        "provider": normalized_provider,
        "org_id": org_id,
        **dict(summary),
    }


def run_team_autoimport_strict(
    *,
    provider: str,
    org_id: str,
    credentials: dict[str, Any],
    scope: dict[str, Any] | None = None,
    analytics_db_url: str | None = None,
) -> dict[str, Any]:
    normalized_provider = provider.strip().lower()
    if normalized_provider in _GO_NATIVE_PROVIDERS:
        # NOT redundant: without this, _resolve_populator returning None
        # below would hit this function's "populator is unavailable" branch
        # and raise ValueError (a 500 through the /reference-discovery* and
        # /reference-discovery-populate bridge routes) instead of a clean
        # no-op. This mirrors the _provider_capability no-op just below it.
        logger.info(
            "Reference discovery no-op for provider=%s org_id=%s: writer is "
            "Go-native (CHAOS-4555)",
            normalized_provider,
            org_id,
        )
        return _zero_summary(
            provider=normalized_provider,
            org_id=org_id,
            reason="provider_writer_is_go_native",
        )
    if not _provider_capability(normalized_provider):
        # Providers without a reference tier (e.g. launchdarkly) have nothing
        # to discover today, so strict reference discovery is a successful
        # no-op rather than a hard failure that would fail the whole sync run.
        # (LD work-item association — Commits<>PRs<>Issues — is planned but not
        # implemented yet; CHAOS-2740.) Genuine failures still surface: a capable
        # provider with a missing populator raises below, and a capable provider
        # with bad credentials raises inside the populator.
        logger.info(
            "Reference discovery no-op for provider=%s org_id=%s: provider is "
            "not import-capable (nothing to discover yet)",
            normalized_provider,
            org_id,
        )
        return _zero_summary(
            provider=normalized_provider,
            org_id=org_id,
            reason="provider_not_import_capable",
        )
    populator = _resolve_populator(normalized_provider)
    if populator is None:
        raise ValueError(
            f"team auto-import populator is unavailable: {normalized_provider}"
        )

    # CHAOS-4437: honour the org's CHAOS-4323 per-category selection the SAME
    # way run_team_autoimport already does, instead of always importing every
    # category. Reference discovery still runs (and still verifies via
    # readback -- see reference_discovery._verify_reference_readback / the Go
    # ReferenceReadbackVerifier) for dispatch-blocking key resolution, but it
    # must not write teams/team_memberships/team_project_ownership rows for a
    # category the org explicitly disabled just because this is the strict
    # path. Each populator already reads scope["import_categories"] via
    # team_autoimport_categories.resolve_import_categories -- absent it
    # defaults every category True (unrestricted), which is exactly the
    # pre-fix bug this closes.
    #
    # CHAOS-4437 (codex review): a caller's scope["sync_options"] is only a
    # trustworthy CHAOS-4323 selection when the caller marks it canonical.
    # reference_discovery._load_discovery_context sets
    # sync_options_is_canonical=False when it had to fall back to
    # Integration.config (no SyncConfiguration row exists) -- that dict is
    # NOT an authoritative selection (it predates the category split and
    # commonly has no auto_import_* keys at all), so treating it as one would
    # flip "unrestricted" into "everything off" for that org. Callers that
    # predate this flag (e.g. backfill's run_backfill_for_config, which
    # always passes the real canonical SyncConfiguration.sync_options)
    # default to canonical=True, preserving their existing behavior exactly.
    scope_sync_options = (scope or {}).get("sync_options")
    sync_options_is_canonical = bool(
        (scope or {}).get("sync_options_is_canonical", True)
    )
    import_categories = import_categories_from_sync_options(
        scope_sync_options
        if sync_options_is_canonical and isinstance(scope_sync_options, Mapping)
        else None
    )
    for category, selected in import_categories.items():
        record_team_autoimport_reference_category_outcome(
            provider=normalized_provider,
            category=category,
            outcome="written" if selected else "skipped_selection",
        )

    populator_scope = dict(scope or {})
    populator_scope["strict_reference_discovery"] = True
    populator_scope["import_categories"] = import_categories
    if analytics_db_url:
        populator_scope["analytics_db"] = analytics_db_url
    summary = populator(
        org_id=org_id,
        credentials=credentials,
        scope=populator_scope,
    )
    if not isinstance(summary, Mapping):
        raise TypeError(
            f"team auto-import populator returned {type(summary).__name__}, expected mapping"
        )
    return {
        "status": "success",
        "provider": normalized_provider,
        "org_id": org_id,
        **dict(summary),
    }
