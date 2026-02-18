from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Annotated, AsyncGenerator, Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.audit import (
    AuditService,
    AuditLogFilter as ServiceAuditLogFilter,
)
from dev_health_ops.api.services.ip_allowlist import IPAllowlistService
from dev_health_ops.api.services.retention import RetentionService
from dev_health_ops.licensing import require_feature
from dev_health_ops.api.services.settings import (
    IdentityMappingService,
    IntegrationCredentialsService,
    JiraActivityInferenceService,
    SettingsService,
    SyncConfigurationService,
    TeamDiscoveryService,
    TeamMembershipService,
    TeamMappingService,
)
from dev_health_ops.api.services.users import (
    MembershipService,
    OrganizationService,
    UserService,
)
from dev_health_ops.db import get_postgres_session
from dev_health_ops.models.settings import (
    JobRun,
    ScheduledJob,
    SettingCategory,
    SyncConfiguration,
)
from dev_health_ops.models.users import Membership, Organization, User

from .schemas import (
    AuditLogListResponse,
    AuditLogResponse,
    ConfirmMembersRequest,
    ConfirmMembersResponse,
    ConfirmInferredMembersRequest,
    ConfirmInferredMembersResponse,
    IdentityMappingCreate,
    IdentityMappingResponse,
    IntegrationCredentialCreate,
    IntegrationCredentialResponse,
    IntegrationCredentialUpdate,
    JOB_RUN_STATUS_LABELS,
    JobRunResponse,
    IPAllowlistCreate,
    IPAllowlistListResponse,
    IPAllowlistResponse,
    IPAllowlistUpdate,
    IPCheckRequest,
    IPCheckResponse,
    MembershipCreate,
    MembershipResponse,
    MembershipUpdateRole,
    OrganizationCreate,
    OrganizationResponse,
    OrganizationUpdate,
    OwnershipTransfer,
    PlatformStatsResponse,
    JiraActivityInferenceResponse,
    RetentionExecuteResponse,
    RetentionPolicyCreate,
    RetentionPolicyListResponse,
    RetentionPolicyResponse,
    RetentionPolicyUpdate,
    SettingCreate,
    SettingResponse,
    SettingsListResponse,
    SettingUpdate,
    SyncConfigCreate,
    SyncConfigResponse,
    SyncConfigUpdate,
    TeamDiscoverResponse,
    TeamImportRequest,
    TeamImportResponse,
    TeamMembersDiscoverResponse,
    TeamMappingCreate,
    TeamMappingResponse,
    TeamMappingUpdate,
    TestConnectionRequest,
    TestConnectionResponse,
    UserCreate,
    UserResponse,
    UserSetPassword,
    UserUpdate,
)

logger = logging.getLogger(__name__)

# Canonical mapping of provider → supported sync targets.
# Jira/Linear only support work-items; Git/CI/CD come from code hosts.
PROVIDER_SYNC_TARGETS: dict[str, list[str]] = {
    "github": ["git", "prs", "cicd", "deployments", "incidents", "work-items"],
    "gitlab": ["git", "prs", "cicd", "deployments", "incidents", "work-items"],
    "jira": ["work-items"],
    "linear": ["work-items"],
}

router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with get_postgres_session() as session:
        yield session


def get_org_id(x_org_id: Annotated[str, Header(alias="X-Org-Id")] = "default") -> str:
    return x_org_id


def get_user_id(
    x_user_id: Annotated[Optional[str], Header(alias="X-User-Id")] = None,
) -> Optional[str]:
    return x_user_id


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

    creds = payload.credentials  # inline (pre-save) or fall back to stored
    if not creds:
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

    # Always persist the test result when a stored credential exists
    # (covers both inline pre-save tests and DB-sourced tests)
    stored = await svc.get(payload.provider, payload.name)
    if stored:
        await svc.update_test_result(payload.provider, success, error, payload.name)
    return TestConnectionResponse(success=success, error=error, details=details or None)


async def _test_github_connection(creds: dict) -> tuple[bool, dict]:
    import httpx
    from urllib.parse import urlparse

    token = creds.get("token")
    if not token:
        return False, {"error": "No token provided"}

    base_url = creds.get("base_url", "https://api.github.com")
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        return False, {"error": "Invalid URL scheme"}
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
    from urllib.parse import urlparse

    token = creds.get("token")
    if not token:
        return False, {"error": "No token provided"}

    base_url = creds.get("url") or creds.get("base_url", "https://gitlab.com/api/v4")
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        return False, {"error": "Invalid URL scheme"}
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
    from urllib.parse import urlparse

    email = creds.get("email")
    api_token = creds.get("token") or creds.get("api_token")
    base_url = creds.get("url") or creds.get("base_url")

    if not all([email, api_token, base_url]):
        return False, {
            "error": "Missing required credentials (email, api_token, base_url)"
        }

    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https"):
        return False, {"error": "Invalid URL scheme"}

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

    api_key = creds.get("apiKey") or creds.get("api_key")
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


@router.get("/sync-targets")
async def get_provider_sync_targets() -> dict[str, list[str]]:
    return PROVIDER_SYNC_TARGETS


@router.get("/sync-configs", response_model=list[SyncConfigResponse])
async def list_sync_configs(
    active_only: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> list[SyncConfigResponse]:
    svc = SyncConfigurationService(session, org_id)
    configs = await svc.list_all(active_only=active_only)
    return [_sync_config_to_response(c) for c in configs]


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
    return _sync_config_to_response(config)


def _sync_config_to_response(c) -> SyncConfigResponse:
    return SyncConfigResponse(
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


@router.get("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def get_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    return _sync_config_to_response(config)


@router.patch("/sync-configs/{config_id}", response_model=SyncConfigResponse)
async def update_sync_config(
    config_id: str,
    payload: SyncConfigUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> SyncConfigResponse:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    updated = await svc.update(
        name=config.name,
        sync_targets=payload.sync_targets,
        sync_options=payload.sync_options,
        is_active=payload.is_active,
    )
    return _sync_config_to_response(updated)


@router.delete("/sync-configs/{config_id}", status_code=204)
async def delete_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> None:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")
    await svc.delete(config.name)


@router.post("/sync-configs/{config_id}/trigger", status_code=202)
async def trigger_sync_config(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> dict:
    svc = SyncConfigurationService(session, org_id)
    config = await svc.get_by_id(config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    try:
        from dev_health_ops.workers.tasks import run_sync_config

        result = run_sync_config.delay(
            config_id=str(config.id),
            org_id=org_id,
            triggered_by="manual",
        )
        return {
            "status": "triggered",
            "config_id": str(config.id),
            "task_id": result.id,
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Task queue unavailable: {e}")


@router.get("/sync-configs/{config_id}/jobs", response_model=list[JobRunResponse])
async def list_sync_config_jobs(
    config_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> list[JobRunResponse]:
    svc = SyncConfigurationService(session, org_id)
    existing = await svc.get_by_id(config_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Sync configuration not found")

    job_stmt = select(ScheduledJob.id).where(
        ScheduledJob.org_id == org_id,
        ScheduledJob.sync_config_id == uuid.UUID(config_id),
    )
    job_result = await session.execute(job_stmt)
    job_ids = list(job_result.scalars().all())

    if not job_ids:
        return []

    runs_stmt = (
        select(JobRun)
        .where(JobRun.job_id.in_(job_ids))
        .order_by(JobRun.created_at.desc())
        .limit(50)
    )
    runs_result = await session.execute(runs_stmt)
    runs = list(runs_result.scalars().all())

    return [
        JobRunResponse(
            id=str(run.id),
            job_id=str(run.job_id),
            status=JOB_RUN_STATUS_LABELS.get(run.status, "unknown"),
            started_at=run.started_at,
            completed_at=run.completed_at,
            duration_seconds=run.duration_seconds,
            result=run.result,
            error=run.error,
            triggered_by=run.triggered_by,
            created_at=run.created_at,
        )
        for run in runs
    ]


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
    org_id: str = Depends(get_org_id),
) -> TeamMappingResponse:
    from dev_health_ops.workers.tasks import sync_teams_to_analytics

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
    org_id: str = Depends(get_org_id),
) -> dict:
    svc = TeamMappingService(session, org_id)
    deleted = await svc.delete(team_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Team not found")
    return {"deleted": True}


@router.get("/teams/discover", response_model=TeamDiscoverResponse)
async def discover_teams(
    provider: str = Query(..., pattern="^(github|gitlab|jira)$"),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
) -> TeamImportResponse:
    from dev_health_ops.workers.tasks import sync_teams_to_analytics

    svc = TeamDiscoveryService(session, org_id)
    result = await svc.import_teams(payload.teams, payload.on_conflict)
    await session.commit()
    sync_teams_to_analytics.apply_async(kwargs={"org_id": org_id}, queue="metrics")
    return TeamImportResponse(**result)


@router.get("/teams/pending-changes")
async def get_pending_changes(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
):
    from dev_health_ops.api.services.settings import TeamDriftSyncService
    from dev_health_ops.workers.tasks import sync_teams_to_analytics

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
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
):
    from dev_health_ops.workers.tasks import sync_team_drift

    sync_team_drift.apply_async(kwargs={"org_id": org_id}, queue="sync")
    return {"status": "dispatched"}


@router.get("/teams/{team_id}", response_model=TeamMappingResponse)
async def get_team(
    team_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
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


@router.get(
    "/teams/{team_id}/discover-members",
    response_model=TeamMembersDiscoverResponse,
)
async def discover_team_members(
    team_id: str,
    provider: str = Query(..., pattern="^(github|gitlab|jira)$"),
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
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
    org_id: str = Depends(get_org_id),
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
            is_superuser=payload.is_superuser,
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


@router.get("/platform/stats", response_model=PlatformStatsResponse)
async def platform_stats(
    session: AsyncSession = Depends(get_session),
) -> PlatformStatsResponse:
    total_organizations = (
        await session.execute(select(func.count()).select_from(Organization))
    ).scalar_one()
    active_organizations = (
        await session.execute(
            select(func.count())
            .select_from(Organization)
            .where(Organization.is_active.is_(True))
        )
    ).scalar_one()
    total_users = (
        await session.execute(select(func.count()).select_from(User))
    ).scalar_one()
    active_users = (
        await session.execute(
            select(func.count()).select_from(User).where(User.is_active.is_(True))
        )
    ).scalar_one()
    superuser_count = (
        await session.execute(
            select(func.count()).select_from(User).where(User.is_superuser.is_(True))
        )
    ).scalar_one()
    total_memberships = (
        await session.execute(select(func.count()).select_from(Membership))
    ).scalar_one()

    tier_rows = (
        await session.execute(
            select(Organization.tier, func.count()).group_by(Organization.tier)
        )
    ).all()
    tier_distribution = {str(tier): int(count) for tier, count in tier_rows}

    total_sync_configs = (
        await session.execute(select(func.count()).select_from(SyncConfiguration))
    ).scalar_one()
    active_sync_configs = (
        await session.execute(
            select(func.count())
            .select_from(SyncConfiguration)
            .where(SyncConfiguration.is_active.is_(True))
        )
    ).scalar_one()

    since = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_sync_filter = (
        SyncConfiguration.last_sync_at.is_not(None),
        SyncConfiguration.last_sync_at >= since,
    )

    recent_syncs_success = (
        await session.execute(
            select(func.count())
            .select_from(SyncConfiguration)
            .where(
                *recent_sync_filter,
                SyncConfiguration.last_sync_success.is_(True),
            )
        )
    ).scalar_one()
    recent_syncs_failed = (
        await session.execute(
            select(func.count())
            .select_from(SyncConfiguration)
            .where(
                *recent_sync_filter,
                SyncConfiguration.last_sync_success.is_(False),
            )
        )
    ).scalar_one()

    return PlatformStatsResponse(
        total_organizations=int(total_organizations),
        active_organizations=int(active_organizations),
        total_users=int(total_users),
        active_users=int(active_users),
        superuser_count=int(superuser_count),
        total_memberships=int(total_memberships),
        tier_distribution=tier_distribution,
        total_sync_configs=int(total_sync_configs),
        active_sync_configs=int(active_sync_configs),
        recent_syncs_success=int(recent_syncs_success),
        recent_syncs_failed=int(recent_syncs_failed),
    )


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


# ---- Audit Log endpoints (Enterprise feature: audit_log) ----


@router.get("/audit-logs", response_model=AuditLogListResponse)
@require_feature("audit_log", required_tier="enterprise")
async def list_audit_logs(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    action: Optional[str] = Query(None, description="Filter by action type"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    resource_id: Optional[str] = Query(None, description="Filter by resource ID"),
    status: Optional[str] = Query(
        None, description="Filter by status (success/failure)"
    ),
    start_date: Optional[datetime] = Query(
        None, description="Filter logs after this date"
    ),
    end_date: Optional[datetime] = Query(
        None, description="Filter logs before this date"
    ),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> AuditLogListResponse:
    """List audit logs for the organization with optional filters.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    filters = ServiceAuditLogFilter(
        user_id=user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        status=status,
        start_date=start_date,
        end_date=end_date,
    )

    logs, total = await svc.get_logs(
        org_id=uuid.UUID(org_id),
        filters=filters,
        limit=limit,
        offset=offset,
    )

    return AuditLogListResponse(
        items=[
            AuditLogResponse(
                id=str(log.id),
                org_id=str(log.org_id),
                user_id=str(log.user_id) if log.user_id else None,
                action=str(log.action),
                resource_type=str(log.resource_type),
                resource_id=str(log.resource_id),
                description=log.description,
                changes=log.changes,
                request_metadata=log.request_metadata,
                status=str(log.status),
                error_message=log.error_message,
                created_at=log.created_at,
            )
            for log in logs
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/audit-logs/{log_id}", response_model=AuditLogResponse)
@require_feature("audit_log", required_tier="enterprise")
async def get_audit_log(
    log_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> AuditLogResponse:
    """Get a specific audit log entry by ID.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    log = await svc.get_log_by_id(
        org_id=uuid.UUID(org_id),
        log_id=uuid.UUID(log_id),
    )

    if not log:
        raise HTTPException(status_code=404, detail="Audit log not found")

    return AuditLogResponse(
        id=str(log.id),
        org_id=str(log.org_id),
        user_id=str(log.user_id) if log.user_id else None,
        action=str(log.action),
        resource_type=str(log.resource_type),
        resource_id=str(log.resource_id),
        description=log.description,
        changes=log.changes,
        request_metadata=log.request_metadata,
        status=str(log.status),
        error_message=log.error_message,
        created_at=log.created_at,
    )


@router.get(
    "/audit-logs/resource/{resource_type}/{resource_id}",
    response_model=list[AuditLogResponse],
)
@require_feature("audit_log", required_tier="enterprise")
async def get_resource_audit_history(
    resource_type: str,
    resource_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
) -> list[AuditLogResponse]:
    """Get audit history for a specific resource.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    logs = await svc.get_resource_history(
        org_id=uuid.UUID(org_id),
        resource_type=resource_type,
        resource_id=resource_id,
        limit=limit,
    )

    return [
        AuditLogResponse(
            id=str(log.id),
            org_id=str(log.org_id),
            user_id=str(log.user_id) if log.user_id else None,
            action=str(log.action),
            resource_type=str(log.resource_type),
            resource_id=str(log.resource_id),
            description=log.description,
            changes=log.changes,
            request_metadata=log.request_metadata,
            status=str(log.status),
            error_message=log.error_message,
            created_at=log.created_at,
        )
        for log in logs
    ]


@router.get("/audit-logs/user/{user_id}", response_model=list[AuditLogResponse])
@require_feature("audit_log", required_tier="enterprise")
async def get_user_audit_activity(
    user_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
    limit: int = Query(50, ge=1, le=500, description="Maximum number of results"),
) -> list[AuditLogResponse]:
    """Get audit log activity for a specific user.

    Requires Enterprise tier (audit_log feature).
    """
    svc = AuditService(session)

    logs = await svc.get_user_activity(
        org_id=uuid.UUID(org_id),
        user_id=uuid.UUID(user_id),
        limit=limit,
    )

    return [
        AuditLogResponse(
            id=str(log.id),
            org_id=str(log.org_id),
            user_id=str(log.user_id) if log.user_id else None,
            action=str(log.action),
            resource_type=str(log.resource_type),
            resource_id=str(log.resource_id),
            description=log.description,
            changes=log.changes,
            request_metadata=log.request_metadata,
            status=str(log.status),
            error_message=log.error_message,
            created_at=log.created_at,
        )
        for log in logs
    ]


@router.get("/ip-allowlist", response_model=IPAllowlistListResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def list_ip_allowlist_entries(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
    active_only: bool = Query(False, description="Filter to active entries only"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> IPAllowlistListResponse:
    svc = IPAllowlistService(session)
    entries, total = await svc.list_entries(
        org_id=uuid.UUID(org_id),
        active_only=active_only,
        limit=limit,
        offset=offset,
    )
    return IPAllowlistListResponse(
        items=[
            IPAllowlistResponse(
                id=str(e.id),
                org_id=str(e.org_id),
                ip_range=str(e.ip_range),
                description=e.description,
                is_active=bool(e.is_active),
                created_by_id=str(e.created_by_id) if e.created_by_id else None,
                created_at=e.created_at,
                updated_at=e.updated_at,
                expires_at=e.expires_at,
            )
            for e in entries
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post("/ip-allowlist", response_model=IPAllowlistResponse, status_code=201)
@require_feature("ip_allowlist", required_tier="enterprise")
async def create_ip_allowlist_entry(
    payload: IPAllowlistCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
    user_id: Optional[str] = Depends(get_user_id),
) -> IPAllowlistResponse:
    svc = IPAllowlistService(session)
    try:
        entry = await svc.create_entry(
            org_id=uuid.UUID(org_id),
            ip_range=payload.ip_range,
            description=payload.description,
            created_by_id=uuid.UUID(user_id) if user_id else None,
            expires_at=payload.expires_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return IPAllowlistResponse(
        id=str(entry.id),
        org_id=str(entry.org_id),
        ip_range=str(entry.ip_range),
        description=entry.description,
        is_active=bool(entry.is_active),
        created_by_id=str(entry.created_by_id) if entry.created_by_id else None,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        expires_at=entry.expires_at,
    )


@router.get("/ip-allowlist/{entry_id}", response_model=IPAllowlistResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def get_ip_allowlist_entry(
    entry_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> IPAllowlistResponse:
    svc = IPAllowlistService(session)
    entry = await svc.get_entry(
        org_id=uuid.UUID(org_id),
        entry_id=uuid.UUID(entry_id),
    )
    if not entry:
        raise HTTPException(status_code=404, detail="IP allowlist entry not found")
    return IPAllowlistResponse(
        id=str(entry.id),
        org_id=str(entry.org_id),
        ip_range=str(entry.ip_range),
        description=entry.description,
        is_active=bool(entry.is_active),
        created_by_id=str(entry.created_by_id) if entry.created_by_id else None,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        expires_at=entry.expires_at,
    )


@router.patch("/ip-allowlist/{entry_id}", response_model=IPAllowlistResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def update_ip_allowlist_entry(
    entry_id: str,
    payload: IPAllowlistUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> IPAllowlistResponse:
    svc = IPAllowlistService(session)
    try:
        entry = await svc.update_entry(
            org_id=uuid.UUID(org_id),
            entry_id=uuid.UUID(entry_id),
            ip_range=payload.ip_range,
            description=payload.description,
            is_active=payload.is_active,
            expires_at=payload.expires_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not entry:
        raise HTTPException(status_code=404, detail="IP allowlist entry not found")
    return IPAllowlistResponse(
        id=str(entry.id),
        org_id=str(entry.org_id),
        ip_range=str(entry.ip_range),
        description=entry.description,
        is_active=bool(entry.is_active),
        created_by_id=str(entry.created_by_id) if entry.created_by_id else None,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        expires_at=entry.expires_at,
    )


@router.delete("/ip-allowlist/{entry_id}")
@require_feature("ip_allowlist", required_tier="enterprise")
async def delete_ip_allowlist_entry(
    entry_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> dict:
    svc = IPAllowlistService(session)
    deleted = await svc.delete_entry(
        org_id=uuid.UUID(org_id),
        entry_id=uuid.UUID(entry_id),
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="IP allowlist entry not found")
    return {"deleted": True}


@router.post("/ip-allowlist/check", response_model=IPCheckResponse)
@require_feature("ip_allowlist", required_tier="enterprise")
async def check_ip_allowed(
    payload: IPCheckRequest,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> IPCheckResponse:
    svc = IPAllowlistService(session)
    allowed = await svc.check_ip_allowed(
        org_id=uuid.UUID(org_id),
        ip_address=payload.ip_address,
    )
    return IPCheckResponse(
        allowed=allowed,
        ip_address=payload.ip_address,
    )


@router.get("/retention-policies", response_model=RetentionPolicyListResponse)
@require_feature("retention_policies", required_tier="enterprise")
async def list_retention_policies(
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
    active_only: bool = Query(False, description="Filter to active policies only"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
) -> RetentionPolicyListResponse:
    svc = RetentionService(session)
    policies, total = await svc.list_policies(
        org_id=uuid.UUID(org_id),
        active_only=active_only,
        limit=limit,
        offset=offset,
    )
    return RetentionPolicyListResponse(
        items=[
            RetentionPolicyResponse(
                id=str(p.id),
                org_id=str(p.org_id),
                resource_type=str(p.resource_type),
                retention_days=int(p.retention_days),
                description=p.description,
                is_active=bool(p.is_active),
                last_run_at=p.last_run_at,
                last_run_deleted_count=p.last_run_deleted_count,
                next_run_at=p.next_run_at,
                created_by_id=str(p.created_by_id) if p.created_by_id else None,
                created_at=p.created_at,
                updated_at=p.updated_at,
            )
            for p in policies
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/retention-policies/resource-types")
@require_feature("retention_policies", required_tier="enterprise")
async def list_retention_resource_types() -> list[str]:
    return RetentionService.get_available_resource_types()


@router.post(
    "/retention-policies", response_model=RetentionPolicyResponse, status_code=201
)
@require_feature("retention_policies", required_tier="enterprise")
async def create_retention_policy(
    payload: RetentionPolicyCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
    user_id: Optional[str] = Depends(get_user_id),
) -> RetentionPolicyResponse:
    svc = RetentionService(session)
    try:
        policy = await svc.create_policy(
            org_id=uuid.UUID(org_id),
            resource_type=payload.resource_type,
            retention_days=payload.retention_days,
            description=payload.description,
            created_by_id=uuid.UUID(user_id) if user_id else None,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return RetentionPolicyResponse(
        id=str(policy.id),
        org_id=str(policy.org_id),
        resource_type=str(policy.resource_type),
        retention_days=int(policy.retention_days),
        description=policy.description,
        is_active=bool(policy.is_active),
        last_run_at=policy.last_run_at,
        last_run_deleted_count=policy.last_run_deleted_count,
        next_run_at=policy.next_run_at,
        created_by_id=str(policy.created_by_id) if policy.created_by_id else None,
        created_at=policy.created_at,
        updated_at=policy.updated_at,
    )


@router.get("/retention-policies/{policy_id}", response_model=RetentionPolicyResponse)
@require_feature("retention_policies", required_tier="enterprise")
async def get_retention_policy(
    policy_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> RetentionPolicyResponse:
    svc = RetentionService(session)
    policy = await svc.get_policy(
        org_id=uuid.UUID(org_id),
        policy_id=uuid.UUID(policy_id),
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    return RetentionPolicyResponse(
        id=str(policy.id),
        org_id=str(policy.org_id),
        resource_type=str(policy.resource_type),
        retention_days=int(policy.retention_days),
        description=policy.description,
        is_active=bool(policy.is_active),
        last_run_at=policy.last_run_at,
        last_run_deleted_count=policy.last_run_deleted_count,
        next_run_at=policy.next_run_at,
        created_by_id=str(policy.created_by_id) if policy.created_by_id else None,
        created_at=policy.created_at,
        updated_at=policy.updated_at,
    )


@router.patch("/retention-policies/{policy_id}", response_model=RetentionPolicyResponse)
@require_feature("retention_policies", required_tier="enterprise")
async def update_retention_policy(
    policy_id: str,
    payload: RetentionPolicyUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> RetentionPolicyResponse:
    svc = RetentionService(session)
    try:
        policy = await svc.update_policy(
            org_id=uuid.UUID(org_id),
            policy_id=uuid.UUID(policy_id),
            retention_days=payload.retention_days,
            description=payload.description,
            is_active=payload.is_active,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    return RetentionPolicyResponse(
        id=str(policy.id),
        org_id=str(policy.org_id),
        resource_type=str(policy.resource_type),
        retention_days=int(policy.retention_days),
        description=policy.description,
        is_active=bool(policy.is_active),
        last_run_at=policy.last_run_at,
        last_run_deleted_count=policy.last_run_deleted_count,
        next_run_at=policy.next_run_at,
        created_by_id=str(policy.created_by_id) if policy.created_by_id else None,
        created_at=policy.created_at,
        updated_at=policy.updated_at,
    )


@router.delete("/retention-policies/{policy_id}")
@require_feature("retention_policies", required_tier="enterprise")
async def delete_retention_policy(
    policy_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> dict:
    svc = RetentionService(session)
    deleted = await svc.delete_policy(
        org_id=uuid.UUID(org_id),
        policy_id=uuid.UUID(policy_id),
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    return {"deleted": True}


@router.post(
    "/retention-policies/{policy_id}/execute", response_model=RetentionExecuteResponse
)
@require_feature("retention_policies", required_tier="enterprise")
async def execute_retention_policy(
    policy_id: str,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_org_id),
) -> RetentionExecuteResponse:
    svc = RetentionService(session)
    deleted_count, error = await svc.execute_policy(
        org_id=uuid.UUID(org_id),
        policy_id=uuid.UUID(policy_id),
    )
    return RetentionExecuteResponse(
        deleted_count=deleted_count,
        error=error,
    )
