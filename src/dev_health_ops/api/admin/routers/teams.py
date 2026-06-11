from __future__ import annotations

from typing import Any, Protocol, cast

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    TeamDiscoverResponse,
    TeamImportRequest,
    TeamImportResponse,
    TeamMappingCreate,
    TeamMappingResponse,
    TeamMappingUpdate,
)
from dev_health_ops.api.services.configuration import (
    AmbiguousCredentialError,
    IntegrationCredentialsService,
    SyncConfigurationService,
    TeamDiscoveryService,
    TeamMappingService,
)

from .common import get_session

router = APIRouter()


def _string_value(value: object) -> str | None:
    return value if isinstance(value, str) else None


async def _derive_owners_from_sync_configs(
    session: AsyncSession,
    org_id: str,
    provider: str,
    option_keys: tuple[str, ...],
) -> list[str]:
    """Derive distinct owner/group values from the org's sync configurations.

    Repo sync stores the GitHub org / GitLab group in
    ``SyncConfiguration.sync_options`` (key ``owner``, see the sync-config
    batch endpoint), so orgs with a working repo sync can discover teams
    without any extra configuration.
    """
    svc = SyncConfigurationService(session, org_id)
    configs = await svc.list_all(active_only=True)
    owners: list[str] = []
    for config in configs:
        if str(getattr(config, "provider", "")).lower() != provider:
            continue
        options: dict[str, Any] = dict(getattr(config, "sync_options") or {})
        for key in option_keys:
            value = _string_value(options.get(key))
            if value and value not in owners:
                owners.append(value)
    return owners


def _dedupe_teams(teams: list[Any]) -> list[Any]:
    """Dedupe discovered teams by provider team key/slug."""
    seen: set[str] = set()
    unique: list[Any] = []
    for team in teams:
        key = str(getattr(team, "provider_team_id"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(team)
    return unique


class _MutableTeamMapping(Protocol):
    name: str
    description: str | None
    repo_patterns: list[str]
    project_keys: list[str]
    extra_data: dict[str, Any]
    managed_fields: list[str]
    sync_policy: int


def _team_mapping_response(team: object) -> TeamMappingResponse:
    return TeamMappingResponse.model_validate(
        {
            "id": str(getattr(team, "id")),
            "team_id": getattr(team, "team_id"),
            "name": getattr(team, "name"),
            "description": getattr(team, "description"),
            "repo_patterns": list(getattr(team, "repo_patterns") or []),
            "project_keys": list(getattr(team, "project_keys") or []),
            "extra_data": dict(getattr(team, "extra_data") or {}),
            "managed_fields": list(getattr(team, "managed_fields") or []),
            "sync_policy": int(getattr(team, "sync_policy")),
            "flagged_changes": getattr(team, "flagged_changes"),
            "last_drift_sync_at": getattr(team, "last_drift_sync_at"),
            "is_active": getattr(team, "is_active"),
            "created_at": getattr(team, "created_at"),
            "updated_at": getattr(team, "updated_at"),
        }
    )


@router.get("/teams", response_model=list[TeamMappingResponse])
async def list_teams(
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[TeamMappingResponse]:
    svc = TeamMappingService(session, org_id)
    teams = await svc.list_all(active_only=active_only)
    return [_team_mapping_response(team) for team in teams]


@router.post("/teams", response_model=TeamMappingResponse)
async def create_or_update_team(
    payload: TeamMappingCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMappingResponse:
    from dev_health_ops.workers.product_tasks import sync_teams_to_analytics

    svc = TeamMappingService(session, org_id)
    team = await svc.create_or_update(
        team_id=payload.team_id,
        name=payload.name,
        description=payload.description,
        repo_patterns=payload.repo_patterns,
        project_keys=payload.project_keys,
        extra_data=payload.extra_data,
    )
    await session.commit()
    getattr(sync_teams_to_analytics, "apply_async")(
        kwargs={"org_id": org_id}, queue="metrics"
    )
    return _team_mapping_response(team)


@router.delete("/teams/{team_id}")
async def delete_team(
    team_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = TeamMappingService(session, org_id)
    deleted = await svc.delete(team_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Team not found")
    return {"deleted": True}


@router.get("/teams/discover", response_model=TeamDiscoverResponse)
async def discover_teams(
    provider: str = Query(..., pattern="^(github|gitlab|jira|linear)$"),
    credential_id: str | None = Query(None),
    credential_name: str | None = Query(None),
    org: str | None = Query(
        None, description="GitHub organization to discover teams from"
    ),
    group: str | None = Query(
        None, description="GitLab group path to discover teams from"
    ),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> TeamDiscoverResponse:
    creds_svc = IntegrationCredentialsService(session, org_id)
    try:
        credential, decrypted = await creds_svc.resolve_with_fallback(
            provider, name=credential_name, credential_id=credential_id
        )
    except AmbiguousCredentialError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if credential is None or decrypted is None:
        raise HTTPException(
            status_code=404,
            detail=f"No credentials found for provider '{provider}'",
        )

    config: dict[str, Any] = getattr(credential, "config") or {}
    discovery_svc = TeamDiscoveryService(session, org_id)
    truncated = False
    warnings: list[str] = []

    if provider == "github":
        token = decrypted.get("token")
        if not token:
            raise HTTPException(
                status_code=400,
                detail="GitHub credentials require a token",
            )
        # Resolution order: explicit query param -> credential config ->
        # owners derived from existing repo sync configurations.
        if org:
            org_names = [org]
        elif _string_value(config.get("org")):
            org_names = [cast(str, _string_value(config.get("org")))]
        else:
            org_names = await _derive_owners_from_sync_configs(
                session, org_id, "github", ("owner", "org")
            )
        if not org_names:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Could not determine a GitHub organization for team "
                    "discovery. Pass ?org=<org-name>, set config.org on the "
                    "credential, or configure a GitHub repository sync first."
                ),
            )
        discovered: list[Any] = []
        for org_name in org_names:
            discovered.extend(
                await discovery_svc.discover_github(token=token, org_name=org_name)
            )
        teams = _dedupe_teams(discovered)
    elif provider == "gitlab":
        token = decrypted.get("token")
        url_value = config.get("url", "https://gitlab.com")
        url = url_value if isinstance(url_value, str) else "https://gitlab.com"
        if not token:
            raise HTTPException(
                status_code=400,
                detail="GitLab credentials require a token",
            )
        if group:
            group_paths = [group]
        elif _string_value(config.get("group")):
            group_paths = [cast(str, _string_value(config.get("group")))]
        else:
            group_paths = await _derive_owners_from_sync_configs(
                session, org_id, "gitlab", ("group", "owner")
            )
        if not group_paths:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Could not determine a GitLab group for team discovery. "
                    "Pass ?group=<group-path>, set config.group on the "
                    "credential, or configure a GitLab repository sync first."
                ),
            )
        discovered_gitlab: list[Any] = []
        for group_path in group_paths:
            gitlab_result = await discovery_svc.discover_gitlab(
                token=token,
                group_path=group_path,
                url=url,
            )
            discovered_gitlab.extend(gitlab_result.teams)
            truncated = truncated or gitlab_result.truncated
            warnings.extend(gitlab_result.warnings)
        teams = _dedupe_teams(discovered_gitlab)
    elif provider == "linear":
        api_key = decrypted.get("apiKey") or decrypted.get("api_key")
        if not api_key:
            raise HTTPException(
                status_code=400,
                detail="Linear credentials require apiKey",
            )
        teams = await discovery_svc.discover_linear(api_key=api_key)
    else:
        email = decrypted.get("email")
        api_token = decrypted.get("api_token") or decrypted.get("token")
        jira_config_url = config.get("url")
        jira_url = jira_config_url if isinstance(jira_config_url, str) else None
        if jira_url is None:
            decrypted_url = decrypted.get("url")
            jira_url = decrypted_url if isinstance(decrypted_url, str) else None
        if not email or not api_token or not jira_url:
            raise HTTPException(
                status_code=400,
                detail="Jira credentials require email, api_token, and url",
            )
        teams = await discovery_svc.discover_jira(
            email=email,
            api_token=api_token,
            url=jira_url,
        )

    return TeamDiscoverResponse(
        provider=provider,
        teams=teams,
        total=len(teams),
        truncated=truncated,
        warnings=warnings,
    )


@router.post("/teams/import", response_model=TeamImportResponse)
async def import_teams(
    payload: TeamImportRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> TeamImportResponse:
    from dev_health_ops.workers.product_tasks import sync_teams_to_analytics

    svc = TeamDiscoveryService(session, org_id)
    result = await svc.import_teams(payload.teams, payload.on_conflict)
    await session.commit()
    getattr(sync_teams_to_analytics, "apply_async")(
        kwargs={"org_id": org_id}, queue="metrics"
    )
    return TeamImportResponse(**result)


@router.get("/teams/pending-changes")
async def get_pending_changes(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.services.configuration import TeamDriftSyncService

    svc = TeamDriftSyncService(session, org_id)
    changes = await svc.get_all_pending_changes()
    return {"changes": changes, "total": len(changes)}


@router.post("/teams/{team_id}/approve-changes")
async def approve_team_changes(
    team_id: str,
    change_indices: list[int] | None = None,
    approve_all: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.services.configuration import TeamDriftSyncService
    from dev_health_ops.workers.product_tasks import sync_teams_to_analytics

    svc = TeamDriftSyncService(session, org_id)
    indices = None if approve_all else change_indices
    result = await svc.approve_changes(team_id, indices)
    await session.commit()
    getattr(sync_teams_to_analytics, "apply_async")(
        kwargs={"org_id": org_id}, queue="metrics"
    )
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/teams/{team_id}/dismiss-changes")
async def dismiss_team_changes(
    team_id: str,
    change_indices: list[int] | None = None,
    dismiss_all: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.services.configuration import TeamDriftSyncService

    svc = TeamDriftSyncService(session, org_id)
    indices = None if dismiss_all else change_indices
    result = await svc.dismiss_changes(team_id, indices)
    await session.commit()
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/teams/trigger-drift-sync")
async def trigger_drift_sync(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.workers.sync_tasks import sync_team_drift

    getattr(sync_team_drift, "apply_async")(kwargs={"org_id": org_id}, queue="sync")
    return {"status": "dispatched"}


@router.get("/teams/{team_id}", response_model=TeamMappingResponse)
async def get_team(
    team_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMappingResponse:
    svc = TeamMappingService(session, org_id)
    team = await svc.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")
    return _team_mapping_response(team)


@router.patch("/teams/{team_id}", response_model=TeamMappingResponse)
async def update_team(
    team_id: str,
    payload: TeamMappingUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMappingResponse:
    svc = TeamMappingService(session, org_id)
    existing = await svc.get(team_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Team not found")

    mutable_existing = cast(_MutableTeamMapping, existing)
    if payload.name is not None:
        mutable_existing.name = payload.name
    if payload.description is not None:
        mutable_existing.description = payload.description
    if payload.repo_patterns is not None:
        mutable_existing.repo_patterns = payload.repo_patterns
    if payload.project_keys is not None:
        mutable_existing.project_keys = payload.project_keys
    if payload.extra_data is not None:
        mutable_existing.extra_data = payload.extra_data
    if payload.managed_fields is not None:
        mutable_existing.managed_fields = payload.managed_fields
    if payload.sync_policy is not None:
        mutable_existing.sync_policy = payload.sync_policy

    await session.flush()
    return _team_mapping_response(existing)
