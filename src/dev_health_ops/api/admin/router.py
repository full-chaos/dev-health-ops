from __future__ import annotations

import logging
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.settings import (
    IdentityMappingService,
    IntegrationCredentialsService,
    SettingsService,
    SyncConfigurationService,
    TeamMappingService,
)
from dev_health_ops.api.services.users import (
    MembershipService,
    OrganizationService,
    UserService,
)
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.settings import SettingCategory

from .schemas import (
    IdentityMappingCreate,
    IdentityMappingResponse,
    IntegrationCredentialCreate,
    IntegrationCredentialResponse,
    IntegrationCredentialUpdate,
    MembershipCreate,
    MembershipResponse,
    MembershipUpdateRole,
    OrganizationCreate,
    OrganizationResponse,
    OrganizationUpdate,
    OwnershipTransfer,
    SettingCreate,
    SettingResponse,
    SettingsListResponse,
    SettingUpdate,
    SyncConfigCreate,
    SyncConfigResponse,
    TeamMappingCreate,
    TeamMappingResponse,
    TestConnectionRequest,
    TestConnectionResponse,
    UserCreate,
    UserResponse,
    UserSetPassword,
    UserUpdate,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with get_postgres_session() as session:
        yield session


def get_org_id(x_org_id: Annotated[str, Header(alias="X-Org-Id")] = "default") -> str:
    return x_org_id


@router.get("/settings/categories")
async def list_setting_categories() -> list[str]:
    return [c.value for c in SettingCategory]


@router.get("/settings/{category}", response_model=SettingsListResponse)
async def list_settings_by_category(
    category: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> SettingsListResponse:
    svc = SettingsService(session, org_id)
    settings = await svc.list_by_category(category)
    return SettingsListResponse(
        category=category,
        settings=[SettingResponse(**s) for s in settings],
    )


@router.get("/settings/{category}/{key}", response_model=SettingResponse)
async def get_setting(
    category: str,
    key: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> SettingResponse:
    svc = SettingsService(session, org_id)
    value = await svc.get(key, category)
    if value is None:
        raise HTTPException(status_code=404, detail="Setting not found")
    return SettingResponse(
        key=key, value=value, category=category, is_encrypted=False, description=None
    )


@router.put("/settings/{category}/{key}", response_model=SettingResponse)
async def set_setting(
    category: str,
    key: str,
    payload: SettingUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> SettingResponse:
    svc = SettingsService(session, org_id)
    setting = await svc.set(
        key=key,
        value=payload.value,
        category=category,
        encrypt=payload.encrypt or False,
        description=payload.description,
    )
    return SettingResponse(
        key=setting.key,
        value=setting.value if not setting.is_encrypted else "[ENCRYPTED]",
        category=setting.category,
        is_encrypted=setting.is_encrypted,
        description=setting.description,
    )


@router.post("/settings", response_model=SettingResponse)
async def create_setting(
    payload: SettingCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> SettingResponse:
    svc = SettingsService(session, org_id)
    setting = await svc.set(
        key=payload.key,
        value=payload.value,
        category=payload.category,
        encrypt=payload.encrypt,
        description=payload.description,
    )
    return SettingResponse(
        key=setting.key,
        value=setting.value if not setting.is_encrypted else "[ENCRYPTED]",
        category=setting.category,
        is_encrypted=setting.is_encrypted,
        description=setting.description,
    )


@router.delete("/settings/{category}/{key}")
async def delete_setting(
    category: str,
    key: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> dict:
    svc = SettingsService(session, org_id)
    deleted = await svc.delete(key, category)
    if not deleted:
        raise HTTPException(status_code=404, detail="Setting not found")
    return {"deleted": True}


@router.get("/credentials", response_model=list[IntegrationCredentialResponse])
async def list_credentials(
    provider: str | None = None,
    active_only: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> list[IntegrationCredentialResponse]:
    svc = IntegrationCredentialsService(session, org_id)
    if provider:
        creds = await svc.list_by_provider(provider)
    else:
        creds = await svc.list_all(active_only=active_only)
    return [
        IntegrationCredentialResponse(
            id=str(c.id),
            provider=c.provider,
            name=c.name,
            is_active=c.is_active,
            config=c.config or {},
            last_test_at=c.last_test_at,
            last_test_success=c.last_test_success,
            last_test_error=c.last_test_error,
            created_at=c.created_at,
            updated_at=c.updated_at,
        )
        for c in creds
    ]


@router.get(
    "/credentials/{provider}/{name}", response_model=IntegrationCredentialResponse
)
async def get_credential(
    provider: str,
    name: str = "default",
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> IntegrationCredentialResponse:
    svc = IntegrationCredentialsService(session, org_id)
    cred = await svc.get(provider, name)
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")
    return IntegrationCredentialResponse(
        id=str(cred.id),
        provider=cred.provider,
        name=cred.name,
        is_active=cred.is_active,
        config=cred.config or {},
        last_test_at=cred.last_test_at,
        last_test_success=cred.last_test_success,
        last_test_error=cred.last_test_error,
        created_at=cred.created_at,
        updated_at=cred.updated_at,
    )


@router.post("/credentials", response_model=IntegrationCredentialResponse)
async def create_credential(
    payload: IntegrationCredentialCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> IntegrationCredentialResponse:
    svc = IntegrationCredentialsService(session, org_id)
    cred = await svc.set(
        provider=payload.provider,
        credentials=payload.credentials,
        name=payload.name,
        config=payload.config,
    )
    return IntegrationCredentialResponse(
        id=str(cred.id),
        provider=cred.provider,
        name=cred.name,
        is_active=cred.is_active,
        config=cred.config or {},
        last_test_at=cred.last_test_at,
        last_test_success=cred.last_test_success,
        last_test_error=cred.last_test_error,
        created_at=cred.created_at,
        updated_at=cred.updated_at,
    )


@router.patch(
    "/credentials/{provider}/{name}", response_model=IntegrationCredentialResponse
)
async def update_credential(
    provider: str,
    name: str,
    payload: IntegrationCredentialUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> IntegrationCredentialResponse:
    svc = IntegrationCredentialsService(session, org_id)
    existing = await svc.get(provider, name)
    if not existing:
        raise HTTPException(status_code=404, detail="Credential not found")

    if payload.credentials is not None:
        existing = await svc.set(
            provider=provider,
            credentials=payload.credentials,
            name=name,
            config=payload.config if payload.config is not None else existing.config,
            is_active=payload.is_active
            if payload.is_active is not None
            else existing.is_active,
        )
    else:
        if payload.config is not None:
            existing.config = payload.config
        if payload.is_active is not None:
            existing.is_active = payload.is_active
        await session.flush()

    return IntegrationCredentialResponse(
        id=str(existing.id),
        provider=existing.provider,
        name=existing.name,
        is_active=existing.is_active,
        config=existing.config or {},
        last_test_at=existing.last_test_at,
        last_test_success=existing.last_test_success,
        last_test_error=existing.last_test_error,
        created_at=existing.created_at,
        updated_at=existing.updated_at,
    )


@router.delete("/credentials/{provider}/{name}")
async def delete_credential(
    provider: str,
    name: str = "default",
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> dict:
    svc = IntegrationCredentialsService(session, org_id)
    deleted = await svc.delete(provider, name)
    if not deleted:
        raise HTTPException(status_code=404, detail="Credential not found")
    return {"deleted": True}


@router.post("/credentials/test", response_model=TestConnectionResponse)
async def test_connection(
    payload: TestConnectionRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> TestConnectionResponse:
    svc = IntegrationCredentialsService(session, org_id)
    creds = await svc.get_decrypted_credentials(payload.provider, payload.name)
    if not creds:
        raise HTTPException(status_code=404, detail="Credential not found")

    success = False
    error = None
    details = {}

    try:
        if payload.provider == "github":
            success, details = await _test_github_connection(creds)
        elif payload.provider == "gitlab":
            success, details = await _test_gitlab_connection(creds)
        elif payload.provider == "jira":
            success, details = await _test_jira_connection(creds)
        elif payload.provider == "linear":
            success, details = await _test_linear_connection(creds)
        else:
            error = f"Unknown provider: {payload.provider}"
    except Exception as e:
        error = str(e)
        safe_provider = str(payload.provider).replace("\r", "").replace("\n", "")
        logger.exception("Test connection failed for %s", safe_provider)

    await svc.update_test_result(payload.provider, success, error, payload.name)
    return TestConnectionResponse(success=success, error=error, details=details or None)


async def _test_github_connection(creds: dict) -> tuple[bool, dict]:
    import httpx

    token = creds.get("token")
    if not token:
        return False, {"error": "No token provided"}

    base_url = creds.get("base_url", "https://api.github.com")
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{base_url}/user",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return True, {"user": data.get("login"), "name": data.get("name")}
        return False, {"status": resp.status_code, "error": resp.text[:200]}


async def _test_gitlab_connection(creds: dict) -> tuple[bool, dict]:
    import httpx

    token = creds.get("token")
    if not token:
        return False, {"error": "No token provided"}

    base_url = creds.get("base_url", "https://gitlab.com/api/v4")
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{base_url}/user",
            headers={"PRIVATE-TOKEN": token},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return True, {"user": data.get("username"), "name": data.get("name")}
        return False, {"status": resp.status_code, "error": resp.text[:200]}


async def _test_jira_connection(creds: dict) -> tuple[bool, dict]:
    import httpx

    email = creds.get("email")
    api_token = creds.get("api_token")
    base_url = creds.get("base_url")

    if not all([email, api_token, base_url]):
        return False, {
            "error": "Missing required credentials (email, api_token, base_url)"
        }

    import base64

    auth = base64.b64encode(f"{email}:{api_token}".encode()).decode()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{base_url}/rest/api/3/myself",
            headers={"Authorization": f"Basic {auth}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return True, {
                "user": data.get("emailAddress"),
                "name": data.get("displayName"),
            }
        return False, {"status": resp.status_code, "error": resp.text[:200]}


async def _test_linear_connection(creds: dict) -> tuple[bool, dict]:
    import httpx

    api_key = creds.get("api_key")
    if not api_key:
        return False, {"error": "No API key provided"}

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.linear.app/graphql",
            headers={"Authorization": api_key, "Content-Type": "application/json"},
            json={"query": "{ viewer { id email name } }"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            viewer = data.get("data", {}).get("viewer", {})
            if viewer:
                return True, {"user": viewer.get("email"), "name": viewer.get("name")}
        return False, {"status": resp.status_code, "error": resp.text[:200]}


@router.get("/sync-configs", response_model=list[SyncConfigResponse])
async def list_sync_configs(
    active_only: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> list[SyncConfigResponse]:
    svc = SyncConfigurationService(session, org_id)
    configs = await svc.list_all(active_only=active_only)
    return [
        SyncConfigResponse(
            id=str(c.id),
            name=c.name,
            provider=c.provider,
            credential_id=str(c.credential_id) if c.credential_id else None,
            sync_targets=c.sync_targets,
            sync_options=c.sync_options,
            is_active=c.is_active,
            last_sync_at=c.last_sync_at,
            last_sync_success=c.last_sync_success,
            last_sync_error=c.last_sync_error,
            created_at=c.created_at,
            updated_at=c.updated_at,
        )
        for c in configs
    ]


@router.post("/sync-configs", response_model=SyncConfigResponse)
async def create_sync_config(
    payload: SyncConfigCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.create(
        name=payload.name,
        provider=payload.provider,
        sync_targets=payload.sync_targets,
        sync_options=payload.sync_options,
        credential_id=payload.credential_id,
    )
    return SyncConfigResponse(
        id=str(config.id),
        name=config.name,
        provider=config.provider,
        credential_id=str(config.credential_id) if config.credential_id else None,
        sync_targets=config.sync_targets,
        sync_options=config.sync_options,
        is_active=config.is_active,
        last_sync_at=config.last_sync_at,
        last_sync_success=config.last_sync_success,
        last_sync_error=config.last_sync_error,
        created_at=config.created_at,
        updated_at=config.updated_at,
    )


@router.get("/identities", response_model=list[IdentityMappingResponse])
async def list_identities(
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
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


@router.get("/teams", response_model=list[TeamMappingResponse])
async def list_teams(
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
) -> TeamMappingResponse:
    svc = TeamMappingService(session, org_id)
    team = await svc.create_or_update(
        team_id=payload.team_id,
        name=payload.name,
        description=payload.description,
        repo_patterns=payload.repo_patterns,
        project_keys=payload.project_keys,
        extra_data=payload.extra_data,
    )
    return TeamMappingResponse(
        id=str(team.id),
        team_id=team.team_id,
        name=team.name,
        description=team.description,
        repo_patterns=team.repo_patterns,
        project_keys=team.project_keys,
        extra_data=team.extra_data,
        is_active=team.is_active,
        created_at=team.created_at,
        updated_at=team.updated_at,
    )


@router.delete("/teams/{team_id}")
async def delete_team(
    team_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> dict:
    svc = TeamMappingService(session, org_id)
    deleted = await svc.delete(team_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Team not found")
    return {"deleted": True}


@router.get("/users", response_model=list[UserResponse])
async def list_users(
    limit: int = 100,
    offset: int = 0,
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
) -> list[UserResponse]:
    svc = UserService(session)
    users = await svc.list_all(limit=limit, offset=offset, active_only=active_only)
    return [
        UserResponse(
            id=str(u.id),
            email=u.email,
            username=u.username,
            full_name=u.full_name,
            avatar_url=u.avatar_url,
            auth_provider=u.auth_provider,
            is_active=u.is_active,
            is_verified=u.is_verified,
            is_superuser=u.is_superuser,
            last_login_at=u.last_login_at,
            created_at=u.created_at,
            updated_at=u.updated_at,
        )
        for u in users
    ]


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: str,
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    svc = UserService(session)
    user = await svc.get_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.username,
        full_name=user.full_name,
        avatar_url=user.avatar_url,
        auth_provider=user.auth_provider,
        is_active=user.is_active,
        is_verified=user.is_verified,
        is_superuser=user.is_superuser,
        last_login_at=user.last_login_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


@router.post("/users", response_model=UserResponse, status_code=201)
async def create_user(
    payload: UserCreate,
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    svc = UserService(session)
    try:
        user = await svc.create(
            email=payload.email,
            password=payload.password,
            username=payload.username,
            full_name=payload.full_name,
            auth_provider=payload.auth_provider,
            auth_provider_id=payload.auth_provider_id,
            is_verified=payload.is_verified,
            is_superuser=payload.is_superuser,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.username,
        full_name=user.full_name,
        avatar_url=user.avatar_url,
        auth_provider=user.auth_provider,
        is_active=user.is_active,
        is_verified=user.is_verified,
        is_superuser=user.is_superuser,
        last_login_at=user.last_login_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


@router.patch("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str,
    payload: UserUpdate,
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    svc = UserService(session)
    try:
        user = await svc.update(
            user_id=user_id,
            email=payload.email,
            username=payload.username,
            full_name=payload.full_name,
            avatar_url=payload.avatar_url,
            is_active=payload.is_active,
            is_verified=payload.is_verified,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(
        id=str(user.id),
        email=user.email,
        username=user.username,
        full_name=user.full_name,
        avatar_url=user.avatar_url,
        auth_provider=user.auth_provider,
        is_active=user.is_active,
        is_verified=user.is_verified,
        is_superuser=user.is_superuser,
        last_login_at=user.last_login_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


@router.post("/users/{user_id}/password")
async def set_user_password(
    user_id: str,
    payload: UserSetPassword,
    session: AsyncSession = Depends(get_session),
) -> dict:
    svc = UserService(session)
    try:
        success = await svc.set_password(user_id, payload.password)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    return {"success": True}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: str,
    session: AsyncSession = Depends(get_session),
) -> dict:
    svc = UserService(session)
    deleted = await svc.delete(user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="User not found")
    return {"deleted": True}


@router.get("/orgs", response_model=list[OrganizationResponse])
async def list_organizations(
    limit: int = 100,
    offset: int = 0,
    active_only: bool = True,
    session: AsyncSession = Depends(get_session),
) -> list[OrganizationResponse]:
    svc = OrganizationService(session)
    orgs = await svc.list_all(limit=limit, offset=offset, active_only=active_only)
    return [
        OrganizationResponse(
            id=str(o.id),
            slug=o.slug,
            name=o.name,
            description=o.description,
            tier=o.tier,
            settings=o.settings or {},
            is_active=o.is_active,
            created_at=o.created_at,
            updated_at=o.updated_at,
        )
        for o in orgs
    ]


@router.get("/orgs/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: str,
    session: AsyncSession = Depends(get_session),
) -> OrganizationResponse:
    svc = OrganizationService(session)
    org = await svc.get_by_id(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return OrganizationResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        settings=org.settings or {},
        is_active=org.is_active,
        created_at=org.created_at,
        updated_at=org.updated_at,
    )


@router.post("/orgs", response_model=OrganizationResponse, status_code=201)
async def create_organization(
    payload: OrganizationCreate,
    session: AsyncSession = Depends(get_session),
) -> OrganizationResponse:
    svc = OrganizationService(session)
    org = await svc.create(
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        settings=payload.settings,
        tier=payload.tier,
        owner_user_id=payload.owner_user_id,
    )
    return OrganizationResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        settings=org.settings or {},
        is_active=org.is_active,
        created_at=org.created_at,
        updated_at=org.updated_at,
    )


@router.patch("/orgs/{org_id}", response_model=OrganizationResponse)
async def update_organization(
    org_id: str,
    payload: OrganizationUpdate,
    session: AsyncSession = Depends(get_session),
) -> OrganizationResponse:
    svc = OrganizationService(session)
    org = await svc.update(
        org_id=org_id,
        name=payload.name,
        description=payload.description,
        settings=payload.settings,
        tier=payload.tier,
        is_active=payload.is_active,
    )
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    return OrganizationResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        settings=org.settings or {},
        is_active=org.is_active,
        created_at=org.created_at,
        updated_at=org.updated_at,
    )


@router.delete("/orgs/{org_id}")
async def delete_organization(
    org_id: str,
    session: AsyncSession = Depends(get_session),
) -> dict:
    svc = OrganizationService(session)
    deleted = await svc.delete(org_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Organization not found")
    return {"deleted": True}


@router.get("/orgs/{org_id}/members", response_model=list[MembershipResponse])
async def list_members(
    org_id: str,
    role: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> list[MembershipResponse]:
    svc = MembershipService(session)
    members = await svc.list_members(org_id, role=role)
    return [
        MembershipResponse(
            id=str(m.id),
            org_id=str(m.org_id),
            user_id=str(m.user_id),
            role=m.role,
            invited_by_id=str(m.invited_by_id) if m.invited_by_id else None,
            joined_at=m.joined_at,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )
        for m in members
    ]


@router.post(
    "/orgs/{org_id}/members", response_model=MembershipResponse, status_code=201
)
async def add_member(
    org_id: str,
    payload: MembershipCreate,
    session: AsyncSession = Depends(get_session),
) -> MembershipResponse:
    svc = MembershipService(session)
    try:
        membership = await svc.add_member(
            org_id=org_id,
            user_id=payload.user_id,
            role=payload.role,
            invited_by_id=payload.invited_by_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return MembershipResponse(
        id=str(membership.id),
        org_id=str(membership.org_id),
        user_id=str(membership.user_id),
        role=membership.role,
        invited_by_id=str(membership.invited_by_id)
        if membership.invited_by_id
        else None,
        joined_at=membership.joined_at,
        created_at=membership.created_at,
        updated_at=membership.updated_at,
    )


@router.patch("/orgs/{org_id}/members/{user_id}", response_model=MembershipResponse)
async def update_member_role(
    org_id: str,
    user_id: str,
    payload: MembershipUpdateRole,
    session: AsyncSession = Depends(get_session),
) -> MembershipResponse:
    svc = MembershipService(session)
    try:
        membership = await svc.update_role(org_id, user_id, payload.role)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not membership:
        raise HTTPException(status_code=404, detail="Membership not found")
    return MembershipResponse(
        id=str(membership.id),
        org_id=str(membership.org_id),
        user_id=str(membership.user_id),
        role=membership.role,
        invited_by_id=str(membership.invited_by_id)
        if membership.invited_by_id
        else None,
        joined_at=membership.joined_at,
        created_at=membership.created_at,
        updated_at=membership.updated_at,
    )


@router.delete("/orgs/{org_id}/members/{user_id}")
async def remove_member(
    org_id: str,
    user_id: str,
    session: AsyncSession = Depends(get_session),
) -> dict:
    svc = MembershipService(session)
    try:
        deleted = await svc.remove_member(org_id, user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=404, detail="Membership not found")
    return {"deleted": True}


@router.post("/orgs/{org_id}/transfer-ownership/{from_user_id}")
async def transfer_ownership(
    org_id: str,
    from_user_id: str,
    payload: OwnershipTransfer,
    session: AsyncSession = Depends(get_session),
) -> dict:
    svc = MembershipService(session)
    try:
        await svc.transfer_ownership(org_id, from_user_id, payload.new_owner_user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"success": True}
