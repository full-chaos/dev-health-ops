from __future__ import annotations

import importlib
import logging
import uuid
from collections.abc import Callable, Mapping
from typing import Any, cast

from dev_health_ops.providers.team_capabilities import team_provider_capabilities
from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

_IMPORTER_MODULES = {
    "linear": "dev_health_ops.workers.team_autoimport_linear",
    "jira": "dev_health_ops.workers.team_autoimport_jira",
    "github": "dev_health_ops.workers.team_autoimport_github",
    "gitlab": "dev_health_ops.workers.team_autoimport_gitlab",
}

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
    module_name = _IMPORTER_MODULES.get(provider.strip().lower())
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

    try:
        populator_scope = dict(scope or {})
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

    populator_scope = dict(scope or {})
    populator_scope["strict_reference_discovery"] = True
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


@celery_app.task(
    queue="sync",
    name="dev_health_ops.workers.tasks.run_post_sync_team_autoimport",
)
def run_post_sync_team_autoimport(sync_run_id: str) -> dict[str, Any]:
    """Refresh team/project/member attribution after a successful sync run.

    Restores the legacy post-sync team auto-import (CHAOS-2647) on the unitized
    fan-out path. The post-sync relay dispatches this once per terminal SyncRun
    when the run's canonical config has ``auto_import_teams`` enabled. Credentials
    are resolved via :func:`resolve_run_auth` — the SAME run-stamped auth context
    the unit workers used (CHAOS-2755) — so auto-import authenticates identically
    to the sync that just completed even if ``Integration.credential_id`` was
    repointed mid-run (and never the legacy ``SyncConfiguration.credential_id``
    row, which can drift). Best-effort and non-fatal: :func:`run_team_autoimport` capability-
    gates providers and swallows populator exceptions, so a failure here never
    fails the sync.
    """
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models import (
        Integration,
        SyncRun,
        SyncRunStatus,
    )
    from dev_health_ops.sync.trigger_routing import (
        canonical_sync_config_for_sync_run,
    )
    from dev_health_ops.workers.sync_bootstrap import resolve_run_auth
    from dev_health_ops.workers.task_utils import (
        _get_db_url,
    )

    run_uuid = uuid.UUID(str(sync_run_id))
    with get_postgres_session_sync() as session:
        run = session.query(SyncRun).filter(SyncRun.id == run_uuid).one_or_none()
        if run is None:
            return {
                "status": "skipped",
                "reason": "run_not_found",
                "sync_run_id": sync_run_id,
            }
        # Gate on SUCCESS: the relay only dispatches once per terminal run, but a
        # partial/failed run can still have some successful units; team auto-import
        # must only run for a fully successful sync (mirrors the legacy path, which
        # ran it only on the success branch).
        if run.status != SyncRunStatus.SUCCESS.value:
            return {
                "status": "skipped",
                "reason": "run_not_successful",
                "sync_run_id": sync_run_id,
                "run_status": str(run.status),
            }

        config = canonical_sync_config_for_sync_run(session, run)
        if config is None:
            return {
                "status": "skipped",
                "reason": "no_canonical_config",
                "sync_run_id": sync_run_id,
            }
        sync_options = dict(config.sync_options or {})
        if not sync_options.get("auto_import_teams"):
            return {
                "status": "skipped",
                "reason": "auto_import_disabled",
                "sync_run_id": sync_run_id,
            }

        provider = str(config.provider or "").strip().lower()
        org_id = str(run.org_id)
        sync_targets = [str(t) for t in (config.sync_targets or [])]
        config_id = str(config.id)
        triggered_by = str(run.triggered_by)

        integration = (
            session.query(Integration)
            .filter(
                Integration.id == run.integration_id,
                Integration.org_id == org_id,
            )
            .one_or_none()
        )
        if integration is None:
            # Mirror the unit workers (SyncTaskBootstrap.load treats a missing
            # integration as an error): skip rather than silently authenticating
            # with env credentials that may not match the synced integration.
            return {
                "status": "skipped",
                "reason": "integration_not_found",
                "sync_run_id": sync_run_id,
            }
        # CHAOS-2755: resolve via the run-stamped auth context (resolve_run_auth)
        # so a mid-run credential repoint cannot make post-sync attribution use a
        # different credential than the units that produced the synced data.
        # Best-effort contract preserved: resolution failures (stamped credential
        # deleted, strict fingerprint mismatch) skip rather than fail the task.
        try:
            _credential_id, credentials = resolve_run_auth(
                session,
                run=run,
                integration=integration,
                provider=provider,
                error_label=f"team_autoimport run: {sync_run_id}",
            )
        except Exception as exc:
            return {
                "status": "skipped",
                "reason": "credential_resolution_failed",
                "detail": str(exc),
                "sync_run_id": sync_run_id,
            }

    summary = run_team_autoimport(
        provider=provider,
        org_id=org_id,
        credentials=credentials,
        scope={
            "mode": "sync_config",
            "sync_config_id": config_id,
            "sync_targets": sync_targets,
            "sync_options": sync_options,
            "triggered_by": triggered_by,
        },
        analytics_db_url=_get_db_url(),
    )
    return {
        "status": "dispatched",
        "sync_run_id": sync_run_id,
        "provider": provider,
        "team_autoimport": summary,
    }
