from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from dev_health_ops.workers.celery_app import celery_app

logger = logging.getLogger(__name__)

_SUPPORTED_PROVIDERS = {"github", "gitlab", "jira", "linear"}


@dataclass(frozen=True)
class _DriftSyncConfig:
    provider: str
    credentials: dict[str, Any]
    sync_options: dict[str, Any]
    name: str


@dataclass(frozen=True)
class _SkippedConfig:
    """A ``SyncConfiguration`` dropped by the fail-closed auth check (CHAOS-2762).

    Distinct from the per-provider ``"skipped"`` results ``_discover_*``
    return (missing credentials fields, unsupported provider, a discovery
    exception): those configs at least got as far as attempting discovery.
    A ``_SkippedConfig`` never gets that far -- it never becomes a
    ``_DriftSyncConfig`` at all, so without this record it would be
    invisible in ``provider_results`` and the task result would look like a
    clean, complete success even if EVERY configured provider was skipped
    (e.g. a fleet-wide credential deactivation/deletion).
    """

    config_id: str
    provider: str
    reason: str


@dataclass(frozen=True)
class _ConfiguredProviderSyncs:
    configs: list[_DriftSyncConfig]
    skipped: list[_SkippedConfig]


# _SkippedConfig.reason values -- stable strings, safe to alert/dashboard on.
_SKIP_REASON_INTEGRATION_NOT_FOUND = "integration_not_found_or_cross_org"
_SKIP_REASON_CREDENTIAL_NOT_FOUND = "credential_not_found_or_cross_org"
_SKIP_REASON_CREDENTIAL_PROVIDER_MISMATCH = "credential_provider_mismatch"
_SKIP_REASON_CREDENTIAL_INACTIVE = "credential_inactive"


@celery_app.task(queue="sync", name="sync_team_drift")
def sync_team_drift(org_id: str) -> dict[str, Any]:
    return asyncio.run(_sync_team_drift_async(org_id=str(org_id)))


async def _sync_team_drift_async(org_id: str) -> dict[str, Any]:
    """Run team-drift discovery for every configured provider in ``org_id``.

    Auth semantics (CHAOS-2762): a config whose linked ``Integration``
    credential is missing, inactive, or provider-mismatched is dropped
    entirely (fail closed, planner parity -- see ``_configured_provider_syncs``)
    rather than falling back to environment auth. Dropped configs are
    reported in the result's ``configs_skipped`` (one entry per config, with
    its ``reason``) alongside ``configs_skipped_count``, and a single
    WARNING aggregate line is logged whenever the list is non-empty.

    ``status`` stays ``"success"`` even when every configured provider was
    skipped this way: a fail-closed skip is policy (a credential the planner
    itself would reject), not a task error, so it must never retry/alert as
    one. The result is still fully observable through ``configs_skipped`` --
    the whole point of this shape is that a fleet-wide credential outage is
    visible in the task result, not just grep-able from worker logs.
    """
    from dev_health_ops.api.services.configuration.clickhouse_team_drift_projector import (
        project_team_rows_with_store,
    )
    from dev_health_ops.storage.clickhouse import ClickHouseStore
    from dev_health_ops.workers.task_utils import (
        _get_db_url,
        _validate_worker_clickhouse_uri,
    )

    db_url = _validate_worker_clickhouse_uri(_get_db_url())
    resolved = _configured_provider_syncs(org_id)
    configs = resolved.configs
    configs_skipped = resolved.skipped
    provider_results: list[dict[str, Any]] = []
    team_rows_by_provider: dict[str, list[dict[str, Any]]] = {}
    discovered_at_by_provider: dict[str, datetime] = {}
    complete_by_provider: dict[str, bool] = {}
    async with ClickHouseStore(db_url) as store:
        store.org_id = org_id
        for config in configs:
            complete_by_provider.setdefault(config.provider, True)
            result = await _discover_provider_team_rows(org_id=org_id, config=config)
            provider_results.append(result)
            if result.get("status") != "success":
                complete_by_provider[config.provider] = False
                continue
            if not _provider_scan_complete(result):
                complete_by_provider[config.provider] = False
            team_rows_by_provider.setdefault(config.provider, []).extend(
                list(result["team_rows"])
            )
            discovered_at_by_provider[config.provider] = result["discovered_at"]

        for provider, team_rows in team_rows_by_provider.items():
            await project_team_rows_with_store(
                store=store,
                org_id=org_id,
                provider=provider,
                team_rows=team_rows,
                discovered_at=discovered_at_by_provider[provider],
                resolve_missing_provider_changes=complete_by_provider[provider],
            )

    if configs_skipped:
        logger.warning(
            "Team drift sync for org_id=%s skipped %d of %d configured "
            "provider config(s) due to fail-closed credential checks "
            "(CHAOS-2762 planner parity): %s",
            org_id,
            len(configs_skipped),
            len(configs) + len(configs_skipped),
            ", ".join(
                f"{s.provider}/{s.config_id}:{s.reason}" for s in configs_skipped
            ),
        )

    return {
        "status": "success",
        "org_id": org_id,
        "providers": [_public_provider_result(row) for row in provider_results],
        "providers_attempted": len(provider_results),
        "teams_discovered": sum(
            int(row.get("teams_discovered", 0)) for row in provider_results
        ),
        "configs_skipped": [
            {"config_id": s.config_id, "provider": s.provider, "reason": s.reason}
            for s in configs_skipped
        ],
        "configs_skipped_count": len(configs_skipped),
    }


def _configured_provider_syncs(org_id: str) -> _ConfiguredProviderSyncs:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models import (
        Integration,
        IntegrationCredential,
        SyncConfiguration,
    )
    from dev_health_ops.workers.task_utils import (
        _credential_mapping,
        _resolve_env_credentials,
    )

    configs: list[_DriftSyncConfig] = []
    skipped: list[_SkippedConfig] = []
    with get_postgres_session_sync() as session:
        rows = (
            session.query(SyncConfiguration)
            .filter(
                SyncConfiguration.org_id == org_id,
                SyncConfiguration.is_active.is_(True),
                SyncConfiguration.provider.in_(sorted(_SUPPORTED_PROVIDERS)),
            )
            .order_by(SyncConfiguration.provider, SyncConfiguration.name)
            .all()
        )
        for row in rows:
            provider = str(row.provider or "").strip().lower()
            credential = None
            skip_reason: str | None = None
            # CHAOS-2762: SyncConfiguration carries no credential of its own --
            # resolve through the linked Integration (the single sanctioned
            # surface reached via integration_id), never a per-row column.
            # Mirrors sync/planner.py's _resolve_credential_stamp exactly:
            # Integration.credential_id NULL -> env auth (below); non-NULL ->
            # the credential MUST be an active, same-org, same-provider row or
            # this config fails closed rather than silently falling back to
            # env auth. Falling back on a missing/inactive credential would
            # let this worker authenticate where the planner would reject
            # outright -- a capacity increase through this second surface,
            # exactly what the epic invariant forbids.
            if row.integration_id is not None:
                integration = (
                    session.query(Integration)
                    .filter(
                        Integration.id == row.integration_id,
                        Integration.org_id == org_id,
                    )
                    .one_or_none()
                )
                if integration is None:
                    # Dangling / out-of-org integration_id -- fail closed.
                    skip_reason = _SKIP_REASON_INTEGRATION_NOT_FOUND
                elif integration.credential_id is not None:
                    credential_row = (
                        session.query(IntegrationCredential)
                        .filter(
                            IntegrationCredential.id == integration.credential_id,
                            IntegrationCredential.org_id == org_id,
                        )
                        .one_or_none()
                    )
                    if credential_row is None:
                        skip_reason = _SKIP_REASON_CREDENTIAL_NOT_FOUND
                    elif str(credential_row.provider or "").strip().lower() != provider:
                        skip_reason = _SKIP_REASON_CREDENTIAL_PROVIDER_MISMATCH
                    elif not bool(credential_row.is_active):
                        skip_reason = _SKIP_REASON_CREDENTIAL_INACTIVE
                    else:
                        credential = credential_row
            if skip_reason is not None:
                logger.warning(
                    "Skipping team drift sync for provider=%s org_id=%s config=%s: "
                    "%s -- failing closed (CHAOS-2762 planner parity) rather than "
                    "falling back to environment auth",
                    provider,
                    org_id,
                    row.name,
                    skip_reason,
                )
                skipped.append(
                    _SkippedConfig(
                        config_id=str(row.id), provider=provider, reason=skip_reason
                    )
                )
                continue
            credentials = (
                _credential_mapping(credential)
                if credential is not None
                else dict(_resolve_env_credentials(provider))
            )
            configs.append(
                _DriftSyncConfig(
                    provider=provider,
                    credentials=credentials,
                    sync_options=dict(row.sync_options or {}),
                    name=str(row.name or provider),
                )
            )
    return _ConfiguredProviderSyncs(configs=configs, skipped=skipped)


async def _discover_provider_team_rows(
    *, org_id: str, config: _DriftSyncConfig
) -> dict[str, Any]:
    try:
        if config.provider == "github":
            return await _discover_github(org_id=org_id, config=config)
        if config.provider == "gitlab":
            return await _discover_gitlab(org_id=org_id, config=config)
        if config.provider == "jira":
            return await _discover_jira(org_id=org_id, config=config)
        if config.provider == "linear":
            return await _discover_linear(org_id=org_id, config=config)
    except Exception as exc:
        logger.info(
            "Skipping team drift sync for provider=%s org_id=%s config=%s: %s",
            config.provider,
            org_id,
            config.name,
            exc,
        )
        return _skipped(config, "provider_discovery_skipped", error=str(exc))
    return _skipped(config, "unsupported_provider")


async def _discover_github(*, org_id: str, config: _DriftSyncConfig) -> dict[str, Any]:
    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )
    from dev_health_ops.credentials.resolver import github_credentials_from_mapping
    from dev_health_ops.workers.team_autoimport_github import _github_org, _team_rows

    github_credentials = github_credentials_from_mapping(config.credentials)
    org_name = _github_org(
        credentials=config.credentials, scope={"sync_options": config.sync_options}
    )
    token = github_credentials.token if github_credentials is not None else None
    if github_credentials is not None and github_credentials.is_app_auth:
        from dev_health_ops.connectors.utils.github_app import GitHubAppTokenProvider

        if (
            github_credentials.app_id is None
            or github_credentials.private_key is None
            or github_credentials.installation_id is None
        ):
            return _skipped(config, "missing_github_app_credentials")
        token = GitHubAppTokenProvider(
            app_id=github_credentials.app_id,
            private_key=github_credentials.private_key,
            installation_id=github_credentials.installation_id,
            api_base_url=github_credentials.base_url or "https://api.github.com",
        ).get_token()
    if not token or not org_name:
        return _skipped(config, "missing_github_credentials_or_org")
    now = datetime.now(timezone.utc)
    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    teams = await discovery.discover_github(token=token, org_name=org_name)
    return _success(config, _team_rows(org_id=org_id, teams=teams, now=now), now)


async def _discover_gitlab(*, org_id: str, config: _DriftSyncConfig) -> dict[str, Any]:
    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )
    from dev_health_ops.credentials.resolver import (
        gitlab_credentials_from_mapping,
        resolve_gitlab_url,
    )
    from dev_health_ops.workers.team_autoimport_gitlab import _gitlab_group, _team_rows

    gitlab_credentials = gitlab_credentials_from_mapping(config.credentials)
    group_path = _gitlab_group(
        credentials=config.credentials, scope={"sync_options": config.sync_options}
    )
    if gitlab_credentials is None or not group_path:
        return _skipped(config, "missing_gitlab_credentials_or_group")
    now = datetime.now(timezone.utc)
    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    result = await discovery.discover_gitlab(
        token=gitlab_credentials.token,
        group_path=group_path,
        url=resolve_gitlab_url(config.sync_options, gitlab_credentials),
    )
    response = _success(
        config, _team_rows(org_id=org_id, teams=result.teams, now=now), now
    )
    response["complete"] = not result.truncated
    if result.truncated:
        response["warnings"] = list(result.warnings)
    return response


async def _discover_jira(*, org_id: str, config: _DriftSyncConfig) -> dict[str, Any]:
    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )
    from dev_health_ops.credentials.resolver import jira_credentials_from_mapping

    jira_credentials = jira_credentials_from_mapping(config.credentials)
    if jira_credentials is None:
        return _skipped(config, "missing_jira_credentials")
    now = datetime.now(timezone.utc)
    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    teams = await discovery.discover_jira(
        email=jira_credentials.email,
        api_token=jira_credentials.api_token,
        url=jira_credentials.base_url,
    )
    response = _success(config, _generic_team_rows(org_id, "jira", teams, now), now)
    response["complete"] = False
    response["warnings"] = ["jira_project_discovery_is_bounded"]
    return response


async def _discover_linear(*, org_id: str, config: _DriftSyncConfig) -> dict[str, Any]:
    from dev_health_ops.api.services.configuration.team_discovery import (
        TeamDiscoveryService,
    )
    from dev_health_ops.credentials.resolver import linear_credentials_from_mapping

    linear_credentials = linear_credentials_from_mapping(config.credentials)
    if linear_credentials is None:
        return _skipped(config, "missing_linear_credentials")
    now = datetime.now(timezone.utc)
    discovery = TeamDiscoveryService(session=None, org_id=org_id)
    teams = await discovery.discover_linear(api_key=linear_credentials.api_key)
    return _success(config, _generic_team_rows(org_id, "linear", teams, now), now)


def _generic_team_rows(
    org_id: str, provider: str, teams: list[Any], now: datetime
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        team_id = str(getattr(team, "provider_team_id"))
        associations = dict(getattr(team, "associations", None) or {})
        project_keys = [str(key) for key in associations.get("project_keys", []) if key]
        if not project_keys:
            project_keys = [team_id]
        rows.append(
            {
                "id": team_id,
                "name": str(getattr(team, "name", team_id)),
                "description": getattr(team, "description", None),
                "members": [],
                "project_keys": project_keys,
                "repo_patterns": [],
                "is_active": True,
                "updated_at": now,
                "org_id": org_id,
                "provider": provider,
                "native_team_key": team_id,
                "parent_team_id": None,
            }
        )
    return rows


def _success(
    config: _DriftSyncConfig, team_rows: list[dict[str, Any]], discovered_at: datetime
) -> dict[str, Any]:
    return {
        "status": "success",
        "provider": config.provider,
        "config": config.name,
        "team_rows": team_rows,
        "teams_discovered": len(team_rows),
        "discovered_at": discovered_at,
    }


def _provider_scan_complete(result: dict[str, Any]) -> bool:
    return bool(
        result.get("status") == "success"
        and result.get("complete", True)
        and not result.get("warnings")
    )


def _skipped(
    config: _DriftSyncConfig, reason: str, *, error: str | None = None
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": "skipped",
        "provider": config.provider,
        "config": config.name,
        "reason": reason,
        "teams_discovered": 0,
    }
    if error:
        result["error"] = error
    return result


def _public_provider_result(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in {"team_rows"}}
