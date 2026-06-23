from __future__ import annotations

from typing import Any, cast

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
)
from dev_health_ops.api.services.configuration.clickhouse_team_admin import (
    ClickHouseTeamAdminService,
)
from dev_health_ops.credentials.resolver import github_credentials_from_mapping
from dev_health_ops.storage.clickhouse import ClickHouseStore

from .common import get_clickhouse_store, get_session

router = APIRouter()


def _string_value(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _validate_github_base_url(base_url: str) -> None:
    from dev_health_ops.api.admin.routers.credentials import _validate_external_url

    is_valid, error = _validate_external_url(base_url)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)


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
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> list[TeamMappingResponse]:
    svc = ClickHouseTeamAdminService(store, org_id)
    teams = await svc.list_all(active_only=active_only)
    return [_team_mapping_response(team) for team in teams]


@router.post("/teams", response_model=TeamMappingResponse)
async def create_or_update_team(
    payload: TeamMappingCreate,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMappingResponse:
    svc = ClickHouseTeamAdminService(store, org_id)
    team = await svc.create_or_update(
        team_id=payload.team_id,
        name=payload.name,
        description=payload.description,
        repo_patterns=payload.repo_patterns,
        project_keys=payload.project_keys,
    )
    return _team_mapping_response(team)


@router.delete("/teams/{team_id}")
async def delete_team(
    team_id: str,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> dict:
    svc = ClickHouseTeamAdminService(store, org_id)
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
        github_credentials = github_credentials_from_mapping(decrypted)
        if github_credentials is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "GitHub credentials require either token or "
                    "app_id + private_key + installation_id"
                ),
            )
        if github_credentials.is_app_auth:
            from dev_health_ops.connectors.utils.github_app import (
                GitHubAppTokenProvider,
            )

            assert github_credentials.app_id is not None
            assert github_credentials.private_key is not None
            assert github_credentials.installation_id is not None
            base_url = github_credentials.base_url or "https://api.github.com"
            _validate_github_base_url(base_url)
            try:
                token = GitHubAppTokenProvider(
                    app_id=github_credentials.app_id,
                    private_key=github_credentials.private_key,
                    installation_id=github_credentials.installation_id,
                    api_base_url=base_url,
                ).get_token()
            except Exception as exc:
                raise HTTPException(
                    status_code=401, detail="GitHub App authentication failed"
                ) from exc
        else:
            token_value = github_credentials.token
            if not token_value:
                raise HTTPException(
                    status_code=400, detail="GitHub credential missing token"
                )
            token = token_value
        if not token:
            raise HTTPException(
                status_code=400, detail="GitHub credential missing token"
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
        token_value = decrypted.get("token")
        url_value = config.get("url", "https://gitlab.com")
        url = url_value if isinstance(url_value, str) else "https://gitlab.com"
        if not isinstance(token_value, str) or not token_value:
            raise HTTPException(
                status_code=400,
                detail="GitLab credentials require a token",
            )
        token = token_value
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
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> TeamImportResponse:
    svc = ClickHouseTeamAdminService(store, org_id)
    result = await svc.import_teams(payload.teams, payload.on_conflict)
    return TeamImportResponse(**result)


# --- Team drift review -----------------------------------------------------
# The drift-review surface was built on the Postgres ``TeamMapping``
# flagged-changes machinery, which was removed when ClickHouse became the team
# system of record (CHAOS-2600). The backing service + Postgres tables are gone
# (CS6), so these endpoints are disabled (HTTP 501) rather than returning fake
# success; a ClickHouse-backed rebuild is tracked by CHAOS-2622. They remain as
# pure 501 stubs (no deleted PG code) so the dev-health-web admin keeps getting a
# clean 501 instead of a bogus 404 from the ``/teams/{team_id}`` fallthrough;
# the endpoints + their web caller are removed together in CS7.
#
# ROUTING: the static paths (``/teams/pending-changes``,
# ``/teams/trigger-drift-sync``) are declared BEFORE the ``/teams/{team_id}``
# path-param route so FastAPI matches them rather than treating the literal as a
# team id.

_DRIFT_DISABLED_DETAIL = (
    "Team drift review is disabled; the Postgres flagged-changes machinery was "
    "removed in CHAOS-2600 CS6. A ClickHouse-backed rebuild is tracked by "
    "CHAOS-2622."
)


@router.get("/teams/pending-changes")
async def get_pending_changes(
    org_id: str = Depends(get_admin_org_id),
) -> dict[str, Any]:
    raise HTTPException(status_code=501, detail=_DRIFT_DISABLED_DETAIL)


@router.post("/teams/{team_id}/approve-changes")
async def approve_team_changes(
    team_id: str,
    change_indices: list[int] | None = None,
    approve_all: bool = False,
    org_id: str = Depends(get_admin_org_id),
) -> dict[str, Any]:
    raise HTTPException(status_code=501, detail=_DRIFT_DISABLED_DETAIL)


@router.post("/teams/{team_id}/dismiss-changes")
async def dismiss_team_changes(
    team_id: str,
    change_indices: list[int] | None = None,
    dismiss_all: bool = False,
    org_id: str = Depends(get_admin_org_id),
) -> dict[str, Any]:
    raise HTTPException(status_code=501, detail=_DRIFT_DISABLED_DETAIL)


@router.post("/teams/trigger-drift-sync")
async def trigger_drift_sync(
    org_id: str = Depends(get_admin_org_id),
) -> dict[str, Any]:
    raise HTTPException(status_code=501, detail=_DRIFT_DISABLED_DETAIL)


@router.get("/teams/{team_id}", response_model=TeamMappingResponse)
async def get_team(
    team_id: str,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMappingResponse:
    svc = ClickHouseTeamAdminService(store, org_id)
    team = await svc.get(team_id)
    if team is None:
        raise HTTPException(status_code=404, detail="Team not found")
    return _team_mapping_response(team)


@router.patch("/teams/{team_id}", response_model=TeamMappingResponse)
async def update_team(
    team_id: str,
    payload: TeamMappingUpdate,
    store: ClickHouseStore = Depends(get_clickhouse_store),
    org_id: str = Depends(get_admin_org_id),
) -> TeamMappingResponse:
    # ClickHouse is the team system of record. Only the fields the CH ``teams``
    # table carries (name/description/repo_patterns/project_keys) are mutable
    # here; the Postgres-only drift fields (extra_data/managed_fields/
    # sync_policy) have no ClickHouse counterpart and are surfaced as stable
    # defaults in the response.
    svc = ClickHouseTeamAdminService(store, org_id)
    existing = await svc.get(team_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Team not found")

    updated = await svc.create_or_update(
        team_id=team_id,
        name=payload.name if payload.name is not None else existing.name,
        description=(
            payload.description
            if payload.description is not None
            else existing.description
        ),
        repo_patterns=(
            payload.repo_patterns
            if payload.repo_patterns is not None
            else existing.repo_patterns
        ),
        project_keys=(
            payload.project_keys
            if payload.project_keys is not None
            else existing.project_keys
        ),
    )
    return _team_mapping_response(updated)
