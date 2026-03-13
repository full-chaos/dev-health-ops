from __future__ import annotations

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
from dev_health_ops.api.services.settings import (
    IntegrationCredentialsService,
    TeamDiscoveryService,
    TeamMappingService,
)

from .common import get_session

router = APIRouter()


@router.get("/teams", response_model=list[TeamMappingResponse])
async def list_teams(
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[TeamMappingResponse]:
    svc = TeamMappingService(session, org_id)
    teams = await svc.list_all(active_only=active_only)
    return [
        TeamMappingResponse(
            id=str(t.id),
            team_id=t.team_id,
            name=t.name,
            description=t.description,
            repo_patterns=t.repo_patterns,
            project_keys=t.project_keys,
            extra_data=t.extra_data,
            managed_fields=t.managed_fields,
            sync_policy=t.sync_policy,
            flagged_changes=t.flagged_changes,
            last_drift_sync_at=t.last_drift_sync_at,
            is_active=t.is_active,
            created_at=t.created_at,
            updated_at=t.updated_at,
        )
        for t in teams
    ]


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
    sync_teams_to_analytics.apply_async(kwargs={"org_id": org_id}, queue="metrics")
    return TeamMappingResponse(
        id=str(team.id),
        team_id=team.team_id,
        name=team.name,
        description=team.description,
        repo_patterns=team.repo_patterns,
        project_keys=team.project_keys,
        extra_data=team.extra_data,
        managed_fields=team.managed_fields,
        sync_policy=team.sync_policy,
        flagged_changes=team.flagged_changes,
        last_drift_sync_at=team.last_drift_sync_at,
        is_active=team.is_active,
        created_at=team.created_at,
        updated_at=team.updated_at,
    )


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
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> TeamDiscoverResponse:
    creds_svc = IntegrationCredentialsService(session, org_id)
    credential = await creds_svc.get(provider, "default")
    decrypted = await creds_svc.get_decrypted_credentials(provider, "default")
    if credential is None or decrypted is None:
        raise HTTPException(
            status_code=404,
            detail=f"No credentials found for provider '{provider}'",
        )

    config = credential.config or {}
    discovery_svc = TeamDiscoveryService(session, org_id)

    if provider == "github":
        token = decrypted.get("token")
        org_name = config.get("org")
        if not token or not org_name:
            raise HTTPException(
                status_code=400,
                detail="GitHub credentials require token and config.org",
            )
        teams = await discovery_svc.discover_github(token=token, org_name=org_name)
    elif provider == "gitlab":
        token = decrypted.get("token")
        group_path = config.get("group")
        url = config.get("url", "https://gitlab.com")
        if not token or not group_path:
            raise HTTPException(
                status_code=400,
                detail="GitLab credentials require token and config.group",
            )
        teams = await discovery_svc.discover_gitlab(
            token=token,
            group_path=group_path,
            url=url,
        )
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
        jira_url = config.get("url") or decrypted.get("url")
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

    return TeamDiscoverResponse(provider=provider, teams=teams, total=len(teams))


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
    sync_teams_to_analytics.apply_async(kwargs={"org_id": org_id}, queue="metrics")
    return TeamImportResponse(**result)


@router.get("/teams/pending-changes")
async def get_pending_changes(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
):
    from dev_health_ops.api.services.settings import TeamDriftSyncService

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
    from dev_health_ops.api.services.settings import TeamDriftSyncService
    from dev_health_ops.workers.product_tasks import sync_teams_to_analytics

    svc = TeamDriftSyncService(session, org_id)
    indices = None if approve_all else change_indices
    result = await svc.approve_changes(team_id, indices)
    await session.commit()
    sync_teams_to_analytics.apply_async(kwargs={"org_id": org_id}, queue="metrics")
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
    from dev_health_ops.api.services.settings import TeamDriftSyncService

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

    sync_team_drift.apply_async(kwargs={"org_id": org_id}, queue="sync")
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
    return TeamMappingResponse(
        id=str(team.id),
        team_id=team.team_id,
        name=team.name,
        description=team.description,
        repo_patterns=team.repo_patterns,
        project_keys=team.project_keys,
        extra_data=team.extra_data,
        managed_fields=team.managed_fields,
        sync_policy=team.sync_policy,
        flagged_changes=team.flagged_changes,
        last_drift_sync_at=team.last_drift_sync_at,
        is_active=team.is_active,
        created_at=team.created_at,
        updated_at=team.updated_at,
    )


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

    if payload.name is not None:
        existing.name = payload.name
    if payload.description is not None:
        existing.description = payload.description
    if payload.repo_patterns is not None:
        existing.repo_patterns = payload.repo_patterns
    if payload.project_keys is not None:
        existing.project_keys = payload.project_keys
    if payload.extra_data is not None:
        existing.extra_data = payload.extra_data
    if payload.managed_fields is not None:
        existing.managed_fields = payload.managed_fields
    if payload.sync_policy is not None:
        existing.sync_policy = payload.sync_policy

    await session.flush()
    return TeamMappingResponse(
        id=str(existing.id),
        team_id=existing.team_id,
        name=existing.name,
        description=existing.description,
        repo_patterns=existing.repo_patterns,
        project_keys=existing.project_keys,
        extra_data=existing.extra_data,
        managed_fields=existing.managed_fields,
        sync_policy=existing.sync_policy,
        flagged_changes=existing.flagged_changes,
        last_drift_sync_at=existing.last_drift_sync_at,
        is_active=existing.is_active,
        created_at=existing.created_at,
        updated_at=existing.updated_at,
    )
