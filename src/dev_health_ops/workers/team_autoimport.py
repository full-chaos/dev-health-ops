from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Mapping
from typing import Any, cast

from dev_health_ops.providers.team_capabilities import team_provider_capabilities

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
        summary = populator(
            org_id=org_id,
            credentials=credentials,
            scope=dict(scope or {}),
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
