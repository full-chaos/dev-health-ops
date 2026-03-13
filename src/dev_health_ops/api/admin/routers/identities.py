from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    ConfirmInferredMembersRequest,
    ConfirmInferredMembersResponse,
    ConfirmMembersRequest,
    ConfirmMembersResponse,
    IdentityMappingCreate,
    IdentityMappingResponse,
    JiraActivityInferenceResponse,
    TeamMembersDiscoverResponse,
)
from dev_health_ops.api.services.settings import (
    IdentityMappingService,
    IntegrationCredentialsService,
    JiraActivityInferenceService,
    TeamMappingService,
    TeamMembershipService,
)

from .common import get_session

router = APIRouter()

@router.get("/identities", response_model=list[IdentityMappingResponse])
async def list_identities(
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[IdentityMappingResponse]:
    svc = IdentityMappingService(session, org_id)
    mappings = await svc.list_all(active_only=active_only)
    return [
        IdentityMappingResponse(
            id=str(m.id),
            canonical_id=m.canonical_id,
            display_name=m.display_name,
            email=m.email,
            provider_identities=m.provider_identities,
            team_ids=m.team_ids,
            is_active=m.is_active,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )
        for m in mappings
    ]


@router.post("/identities", response_model=IdentityMappingResponse)
async def create_or_update_identity(
    payload: IdentityMappingCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IdentityMappingResponse:
    svc = IdentityMappingService(session, org_id)
    mapping = await svc.create_or_update(
        canonical_id=payload.canonical_id,
        display_name=payload.display_name,
        email=payload.email,
        provider_identities=payload.provider_identities,
        team_ids=payload.team_ids,
    )
    return IdentityMappingResponse(
        id=str(mapping.id),
        canonical_id=mapping.canonical_id,
        display_name=mapping.display_name,
        email=mapping.email,
        provider_identities=mapping.provider_identities,
        team_ids=mapping.team_ids,
        is_active=mapping.is_active,
        created_at=mapping.created_at,
        updated_at=mapping.updated_at,
    )

@router.get(
    "/teams/{team_id}/discover-members",
    response_model=TeamMembersDiscoverResponse,
)
async def discover_team_members(
    team_id: str,
    provider: str = Query(..., pattern="^(github|gitlab|jira)$"),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMembersDiscoverResponse:
    team_svc = TeamMappingService(session, org_id)
    team = await team_svc.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")

    creds_svc = IntegrationCredentialsService(session, org_id)
    credential = await creds_svc.get(provider, "default")
    decrypted = await creds_svc.get_decrypted_credentials(provider, "default")
    if credential is None or decrypted is None:
        raise HTTPException(
            status_code=404,
            detail=f"No credentials found for provider '{provider}'",
        )

    config = credential.config or {}
    membership_svc = TeamMembershipService(session, org_id)
    provider_team_id = str((team.extra_data or {}).get("provider_team_id") or team_id)

    if provider == "github":
        token = decrypted.get("token")
        org_name = config.get("org")
        team_slug = provider_team_id.removeprefix("gh:")
        if not token or not org_name:
            raise HTTPException(
                status_code=400,
                detail="GitHub credentials require token and config.org",
            )
        members = await membership_svc.discover_members_github(
            token=token,
            org_name=org_name,
            team_slug=team_slug,
        )
    elif provider == "gitlab":
        token = decrypted.get("token")
        group_path = provider_team_id.removeprefix("gl:")
        url = config.get("url", "https://gitlab.com")
        if not token or not group_path:
            raise HTTPException(
                status_code=400,
                detail="GitLab credentials require token and team provider path",
            )
        members = await membership_svc.discover_members_gitlab(
            token=token,
            group_path=group_path,
            url=url,
        )
    else:
        email = decrypted.get("email")
        api_token = decrypted.get("api_token") or decrypted.get("token")
        jira_url = config.get("url") or decrypted.get("url")
        project_key = provider_team_id
        if ":" in project_key:
            project_key = project_key.split(":", 1)[1]
        if not project_key and team.project_keys:
            project_key = team.project_keys[0]
        if not email or not api_token or not jira_url or not project_key:
            raise HTTPException(
                status_code=400,
                detail="Jira credentials require email, api_token, url, and project key",
            )
        members = await membership_svc.discover_members_jira(
            email=email,
            api_token=api_token,
            url=jira_url,
            project_key=project_key,
        )

    matched = await membership_svc.match_members(members)
    return TeamMembersDiscoverResponse(
        team_id=team_id,
        provider=provider,
        members=matched,
        total=len(matched),
    )


@router.post(
    "/teams/{team_id}/confirm-members",
    response_model=ConfirmMembersResponse,
)
async def confirm_team_members(
    team_id: str,
    payload: ConfirmMembersRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> ConfirmMembersResponse:
    from dev_health_ops.workers.tasks import sync_teams_to_analytics

    if payload.team_id != team_id:
        raise HTTPException(
            status_code=400, detail="team_id mismatch between path and body"
        )

    team_svc = TeamMappingService(session, org_id)
    team = await team_svc.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")

    membership_svc = TeamMembershipService(session, org_id)
    result = await membership_svc.confirm_links(team_id=team_id, links=payload.links)
    await session.commit()
    sync_teams_to_analytics.apply_async(kwargs={"org_id": org_id}, queue="metrics")
    return ConfirmMembersResponse(**result)


@router.get(
    "/teams/{team_id}/infer-members",
    response_model=JiraActivityInferenceResponse,
)
async def infer_team_members_from_jira_activity(
    team_id: str,
    window_days: int = Query(90, ge=1, le=365),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> JiraActivityInferenceResponse:
    team_svc = TeamMappingService(session, org_id)
    team = await team_svc.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")

    extra_data = team.extra_data or {}
    project_key = next(iter(team.project_keys or []), None)
    if not project_key and extra_data.get("provider_type") == "jira":
        project_key = extra_data.get("provider_team_id")
    if not project_key:
        raise HTTPException(
            status_code=400,
            detail="Team does not have a Jira project key configured",
        )

    creds_svc = IntegrationCredentialsService(session, org_id)
    credential = await creds_svc.get("jira", "default")
    decrypted = await creds_svc.get_decrypted_credentials("jira", "default")
    if credential is None or decrypted is None:
        raise HTTPException(
            status_code=404,
            detail="No credentials found for provider 'jira'",
        )

    config = credential.config or {}
    email = decrypted.get("email")
    api_token = decrypted.get("api_token") or decrypted.get("token")
    jira_url = config.get("url") or decrypted.get("url")
    if not email or not api_token or not jira_url:
        raise HTTPException(
            status_code=400,
            detail="Jira credentials require email, api_token, and url",
        )

    inference_svc = JiraActivityInferenceService(session, org_id)
    inferred_members = await inference_svc.infer_members(
        email=email,
        api_token=api_token,
        jira_url=jira_url,
        project_key=project_key,
        window_days=window_days,
    )

    identity_svc = IdentityMappingService(session, org_id)
    for member in inferred_members:
        matched = await identity_svc.find_by_provider_identity(
            "jira", member.account_id
        )
        if matched is not None:
            if not member.display_name and matched.display_name:
                member.display_name = matched.display_name
            if not member.email and matched.email:
                member.email = matched.email

    return JiraActivityInferenceResponse(
        team_id=team_id,
        project_key=project_key,
        window_days=window_days,
        inferred_members=inferred_members,
        total=len(inferred_members),
    )


@router.post(
    "/teams/{team_id}/confirm-inferred-members",
    response_model=ConfirmInferredMembersResponse,
)
async def confirm_inferred_team_members(
    team_id: str,
    payload: ConfirmInferredMembersRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> ConfirmInferredMembersResponse:
    from dev_health_ops.workers.tasks import sync_teams_to_analytics

    if payload.team_id != team_id:
        raise HTTPException(status_code=400, detail="team_id in path/body must match")

    inference_svc = JiraActivityInferenceService(session, org_id)
    try:
        result = await inference_svc.match_and_confirm(team_id, payload.members)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    await session.commit()
    sync_teams_to_analytics.apply_async(kwargs={"org_id": org_id}, queue="metrics")
    return ConfirmInferredMembersResponse(**result)
