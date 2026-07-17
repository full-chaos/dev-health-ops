from __future__ import annotations

import ipaddress
import logging
import socket
from typing import Any, Protocol, cast
from urllib.parse import urlparse, urlunparse

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.admin.schemas import (
    DiscoveredRepo,
    DiscoveredReposResponse,
    IntegrationCredentialCreate,
    IntegrationCredentialResponse,
    IntegrationCredentialUpdate,
    TestConnectionRequest,
    TestConnectionResponse,
)
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.credentials.resolver import github_credentials_from_mapping
from dev_health_ops.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient
from dev_health_ops.sync.error_sanitize import sanitize_error_text

from .common import get_session

logger = logging.getLogger(__name__)

router = APIRouter()


class _MutableIntegrationCredential(Protocol):
    config: dict[str, Any] | None
    is_active: bool


def _string_value(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _github_credentials_or_400(creds: dict[str, Any]):
    github_credentials = github_credentials_from_mapping(creds)
    if github_credentials is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "GitHub credentials require either token or "
                "app_id + private_key + installation_id"
            ),
        )
    return github_credentials


def _validated_github_base_url(base_url: str | None) -> str:
    effective_base_url = base_url or "https://api.github.com"
    is_valid, error = _validate_external_url(effective_base_url)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)
    return effective_base_url


def _integration_credential_response(
    credential: object,
) -> IntegrationCredentialResponse:
    return IntegrationCredentialResponse.model_validate(
        {
            "id": str(getattr(credential, "id")),
            "provider": getattr(credential, "provider"),
            "name": getattr(credential, "name"),
            "is_active": getattr(credential, "is_active"),
            "config": getattr(credential, "config") or {},
            "last_test_at": getattr(credential, "last_test_at"),
            "last_test_success": getattr(credential, "last_test_success"),
            "last_test_error": getattr(credential, "last_test_error"),
            "created_at": getattr(credential, "created_at"),
            "updated_at": getattr(credential, "updated_at"),
        }
    )


@router.get("/credentials", response_model=list[IntegrationCredentialResponse])
async def list_credentials(
    provider: str | None = None,
    active_only: bool = False,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> list[IntegrationCredentialResponse]:
    svc = IntegrationCredentialsService(session, org_id)
    if provider:
        creds = await svc.list_by_provider(provider)
    else:
        creds = await svc.list_all(active_only=active_only)
    return [_integration_credential_response(credential) for credential in creds]


@router.get(
    "/credentials/{credential_id}/repos", response_model=DiscoveredReposResponse
)
async def list_credential_repos(
    credential_id: str,
    owner: str | None = None,
    search: str | None = None,
    max_repos: int = 100,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> DiscoveredReposResponse:
    """List repositories accessible via a stored credential.

    Must be registered BEFORE ``/credentials/{provider}/{name}`` so FastAPI
    matches the more specific ``/repos`` suffix first.
    """
    svc = IntegrationCredentialsService(session, org_id)
    decrypted, credential = await svc.get_decrypted_credentials_by_id(credential_id)
    if credential is None or decrypted is None:
        raise HTTPException(status_code=404, detail="Credential not found")

    provider = str(getattr(credential, "provider"))
    config: dict[str, Any] = getattr(credential, "config") or {}

    if provider == "github":
        github_credentials = _github_credentials_or_400(
            {
                **decrypted,
                "base_url": decrypted.get("base_url") or config.get("base_url"),
            }
        )
        _validated_github_base_url(github_credentials.base_url)
        effective_owner = (
            owner
            or _string_value(config.get("org"))
            or _string_value(decrypted.get("org"))
        )
        try:
            if not effective_owner and github_credentials.is_app_auth:
                repos = await _list_github_app_installation_repos(
                    github_credentials=github_credentials,
                    base_url=github_credentials.base_url or "https://api.github.com",
                    search=search,
                    max_repos=max_repos,
                )
            else:
                client = await _github_repo_list_client(github_credentials)
                try:
                    repos = await client.list_repositories(
                        org_name=effective_owner or None,
                        search=search if effective_owner else None,
                        pattern=f"*{search}*"
                        if search and not effective_owner
                        else None,
                        max_repos=max_repos,
                    )
                finally:
                    client.drain_usage_observations()
                    await client.close()
        except NotFoundException:
            return DiscoveredReposResponse(provider=provider, repos=[], total=0)
        except AuthenticationException as exc:
            raise HTTPException(status_code=401, detail=str(exc))
        except RateLimitException as exc:
            raise HTTPException(status_code=429, detail=str(exc))
        except APIException as exc:
            raise HTTPException(status_code=502, detail=str(exc))
    elif provider == "gitlab":
        token = decrypted.get("token")
        url = (
            _string_value(decrypted.get("url"))
            or _string_value(decrypted.get("base_url"))
            or _string_value(config.get("url"))
            or _string_value(config.get("base_url"))
            or "https://gitlab.com"
        )
        if not token:
            raise HTTPException(
                status_code=400, detail="GitLab credential missing token"
            )
        is_valid, url_error = _validate_external_url(url)
        if not is_valid:
            raise HTTPException(status_code=400, detail=url_error)
        effective_owner = owner or _string_value(config.get("group"))
        try:
            if effective_owner:
                repos = await _list_gitlab_code_client_repos(
                    url=url,
                    token=str(token),
                    owner=effective_owner,
                    search=search,
                    max_repos=max_repos,
                )
            else:
                repos = await _list_gitlab_membership_repos(
                    url=url,
                    token=str(token),
                    search=search,
                    max_repos=max_repos,
                )
        except NotFoundException:
            return DiscoveredReposResponse(provider=provider, repos=[], total=0)
        except AuthenticationException as exc:
            raise HTTPException(status_code=401, detail=str(exc))
        except RateLimitException as exc:
            raise HTTPException(status_code=429, detail=str(exc))
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Repo listing not supported for provider: {provider}",
        )

    discovered = [
        DiscoveredRepo(
            name=r.name,
            full_name=r.full_name,
            description=r.description,
            url=r.url,
        )
        for r in repos
    ]
    return DiscoveredReposResponse(
        provider=provider, repos=discovered, total=len(discovered)
    )


@router.get(
    "/credentials/{provider}/{name}", response_model=IntegrationCredentialResponse
)
async def get_credential(
    provider: str,
    name: str = "default",
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IntegrationCredentialResponse:
    svc = IntegrationCredentialsService(session, org_id)
    cred = await svc.get(provider, name)
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")
    return _integration_credential_response(cred)


@router.post("/credentials", response_model=IntegrationCredentialResponse)
async def create_credential(
    payload: IntegrationCredentialCreate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
) -> IntegrationCredentialResponse:
    svc = IntegrationCredentialsService(session, org_id)
    cred = await svc.set(
        provider=payload.provider,
        credentials=payload.credentials,
        name=payload.name,
        config=payload.config,
    )
    return _integration_credential_response(cred)


@router.patch(
    "/credentials/{provider}/{name}", response_model=IntegrationCredentialResponse
)
async def update_credential(
    provider: str,
    name: str,
    payload: IntegrationCredentialUpdate,
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
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
            config=payload.config
            if payload.config is not None
            else getattr(existing, "config"),
            is_active=payload.is_active
            if payload.is_active is not None
            else bool(getattr(existing, "is_active")),
        )
    else:
        mutable_existing = cast(_MutableIntegrationCredential, existing)
        if payload.config is not None:
            mutable_existing.config = payload.config
        if payload.is_active is not None:
            mutable_existing.is_active = payload.is_active
        await session.flush()

    return _integration_credential_response(existing)


@router.delete("/credentials/{provider}/{name}")
async def delete_credential(
    provider: str,
    name: str = "default",
    session: AsyncSession = Depends(get_session),
    org_id: str = Depends(get_admin_org_id),
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
    org_id: str = Depends(get_admin_org_id),
) -> TestConnectionResponse:
    svc = IntegrationCredentialsService(session, org_id)

    creds = payload.credentials  # inline (pre-save) or fall back to stored
    stored = None
    if not creds:
        # Prefer credential_id (UUID) lookup; fall back to provider+name
        if payload.credential_id:
            creds, stored = await svc.get_decrypted_credentials_by_id(
                payload.credential_id
            )
        else:
            creds = await svc.get_decrypted_credentials(payload.provider, payload.name)
        if not creds:
            raise HTTPException(status_code=404, detail="Credential not found")

    success = False
    error = None
    details: dict[str, Any] = {}

    try:
        if payload.provider == "github":
            success, details = await _test_github_connection(creds)
        elif payload.provider == "gitlab":
            success, details = await _test_gitlab_connection(creds)
        elif payload.provider == "jira":
            success, details = await _test_jira_connection(creds)
        elif payload.provider == "linear":
            success, details = await _test_linear_connection(creds)
        elif payload.provider == "launchdarkly":
            success, details = await _test_launchdarkly_connection(creds)
        elif payload.provider == "pagerduty":
            success, details = await _test_pagerduty_connection(creds)
        else:
            error = f"Unknown provider: {payload.provider}"
    except Exception as e:
        error = str(e)
        safe_provider = str(payload.provider).replace("\r", "").replace("\n", "")
        logger.exception("Test connection failed for %s", safe_provider)

    # CHAOS-2780: this is the credential-test flow -- the most likely place
    # for a secret-bearing exception message (or, via the provider helpers
    # below, a raw external HTTP response body) to appear. Sanitize before
    # it reaches EITHER sink: the persisted last_test_error (below) and the
    # HTTP response returned to the caller (below that). sanitize_error_text
    # is a no-op on None/already-clean text, so this is safe regardless of
    # which branch above set `error`.
    error = sanitize_error_text(error)

    # Always persist the test result when a stored credential exists
    # (covers both inline pre-save tests and DB-sourced tests)
    if stored is None:
        stored = await svc.get(payload.provider, payload.name)
    if stored:
        await svc.update_test_result(
            str(getattr(stored, "provider")),
            success,
            error,
            str(getattr(stored, "name")),
        )
    return TestConnectionResponse(success=success, error=error, details=details or None)


def _validate_external_url(url: str) -> tuple[bool, str | None]:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False, "Invalid URL scheme - only http and https are allowed"

    hostname = parsed.hostname
    if not hostname:
        return False, "No hostname in URL"

    blocked_hostnames = {"localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]"}
    if hostname.lower() in blocked_hostnames:
        return False, "Connection to localhost is not allowed"

    try:
        addr_info = socket.getaddrinfo(
            hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        for family, _, _, _, sockaddr in addr_info:
            if family not in (socket.AF_INET, socket.AF_INET6):
                continue
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False, "Connection to private/internal networks is not allowed"
    except socket.gaierror:
        return False, f"Cannot resolve hostname: {hostname}"

    return True, None


def _build_safe_url(validated_base: str, path: str) -> str:
    """Build a URL from a validated base by reconstructing from parsed components.

    This breaks the CodeQL taint chain by constructing a fresh URL from
    individually validated scheme, netloc, and a hardcoded path — rather than
    string-concatenating or joining the original user-supplied URL.
    """
    parsed = urlparse(validated_base)
    base_path = parsed.path.rstrip("/")
    safe_path = (
        f"{base_path}/{path.lstrip('/')}" if base_path else f"/{path.lstrip('/')}"
    )
    return urlunparse((parsed.scheme, parsed.netloc, safe_path, "", "", ""))


async def _github_repo_list_client(github_credentials: Any) -> GitHubCodeClient:
    if github_credentials.is_app_auth:
        from dev_health_ops.connectors.utils.github_app import GitHubAppTokenProvider

        assert github_credentials.app_id is not None
        assert github_credentials.private_key is not None
        assert github_credentials.installation_id is not None
        try:
            token = GitHubAppTokenProvider(
                app_id=github_credentials.app_id,
                private_key=github_credentials.private_key,
                installation_id=github_credentials.installation_id,
                api_base_url=github_credentials.base_url or "https://api.github.com",
            ).get_token()
        except Exception as exc:
            raise AuthenticationException("GitHub App authentication failed") from exc
    else:
        token = github_credentials.token
    return GitHubCodeClient(
        auth=GitHubAuth(token=token, base_url=github_credentials.base_url)
    )


async def _list_github_app_installation_repos(
    github_credentials: Any,
    base_url: str,
    search: str | None,
    max_repos: int,
) -> list[Any]:
    """Enumerate repos accessible to a GitHub App installation.

    Uses the installation/repositories REST endpoint (paginated) so that
    blank-owner discovery works for App auth, which has no user surface.
    Applies search as a client-side name filter (fnmatch) to avoid a global
    GitHub search.
    """
    _validated_github_base_url(base_url)
    client = await _github_repo_list_client(github_credentials)
    try:
        return await client.list_installation_repositories(
            search=search,
            max_repos=max_repos,
        )
    except AuthenticationException as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except RateLimitException as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    except APIException as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    finally:
        client.drain_usage_observations()
        await client.close()


async def _list_gitlab_code_client_repos(
    url: str,
    token: str,
    owner: str | None,
    search: str | None,
    max_repos: int,
) -> list[Any]:
    from dev_health_ops.providers.gitlab.code_client import GitLabCodeClient

    async with GitLabCodeClient(private_token=token, base_url=url) as client:
        return await client.list_projects(
            group_name=owner,
            search=search if owner else None,
            pattern=f"*{search}*" if search and not owner else None,
            membership=owner is None,
            max_projects=max_repos,
        )


async def _list_gitlab_membership_repos(
    url: str,
    token: str,
    search: str | None,
    max_repos: int,
) -> list[Any]:
    return await _list_gitlab_code_client_repos(
        url=url,
        token=token,
        owner=None,
        search=search,
        max_repos=max_repos,
    )


async def _test_github_connection(creds: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    import httpx

    github_credentials = github_credentials_from_mapping(creds)
    if github_credentials is None:
        return False, {
            "error": "Missing GitHub token or App credentials (app_id, private_key, installation_id)"
        }

    try:
        base_url = _validated_github_base_url(github_credentials.base_url)
    except HTTPException as exc:
        return False, {"error": exc.detail}

    token = github_credentials.token
    path = "user"
    if github_credentials.is_app_auth:
        from dev_health_ops.connectors.utils.github_app import GitHubAppTokenProvider

        assert github_credentials.app_id is not None
        assert github_credentials.private_key is not None
        assert github_credentials.installation_id is not None
        try:
            token = GitHubAppTokenProvider(
                app_id=github_credentials.app_id,
                private_key=github_credentials.private_key,
                installation_id=github_credentials.installation_id,
                api_base_url=base_url,
            ).get_token()
        except Exception:
            return False, {"error": "GitHub App authentication failed"}
        path = "installation/repositories"
    if not token:
        return False, {"error": "No token provided"}

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            _build_safe_url(base_url, path),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            if github_credentials.is_app_auth:
                return True, {
                    "auth_mode": "github_app",
                    "installation_id": github_credentials.installation_id,
                    "repository_count": data.get("total_count"),
                }
            return True, {"user": data.get("login"), "name": data.get("name")}
        return False, {
            "status": resp.status_code,
            # CHAOS-2780: this is a raw response body from the external
            # provider, not something this codebase formats -- some
            # providers echo request details (including the submitted
            # credential) back in error/diagnostic bodies, so it must go
            # through the same redaction as any other error-bearing field
            # in this response. Sanitize the FULL body (not a blind [:200]
            # slice first) so the 200-char cap can never split a credential
            # in half and leave a partial, still-identifiable fragment --
            # same redact-before-truncate ordering sanitize_error_text
            # itself guarantees internally.
            "error": sanitize_error_text(resp.text, max_length=200),
        }


async def _test_gitlab_connection(creds: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    import httpx

    token = creds.get("token")
    if not token:
        return False, {"error": "No token provided"}

    base_url = (
        _string_value(creds.get("url"))
        or _string_value(creds.get("base_url"))
        or "https://gitlab.com/api/v4"
    )
    is_valid, error = _validate_external_url(base_url)
    if not is_valid:
        return False, {"error": error}

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            _build_safe_url(base_url, "user"),
            headers={"PRIVATE-TOKEN": token},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return True, {"user": data.get("username"), "name": data.get("name")}
        return False, {
            "status": resp.status_code,
            # CHAOS-2780: this is a raw response body from the external
            # provider, not something this codebase formats -- some
            # providers echo request details (including the submitted
            # credential) back in error/diagnostic bodies, so it must go
            # through the same redaction as any other error-bearing field
            # in this response. Sanitize the FULL body (not a blind [:200]
            # slice first) so the 200-char cap can never split a credential
            # in half and leave a partial, still-identifiable fragment --
            # same redact-before-truncate ordering sanitize_error_text
            # itself guarantees internally.
            "error": sanitize_error_text(resp.text, max_length=200),
        }


async def _test_jira_connection(creds: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    import httpx

    email = _string_value(creds.get("email"))
    api_token = _string_value(creds.get("token")) or _string_value(
        creds.get("api_token")
    )
    base_url = _string_value(creds.get("url")) or _string_value(creds.get("base_url"))

    if email is None or api_token is None or base_url is None:
        return False, {
            "error": "Missing required credentials (email, api_token, base_url)"
        }

    is_valid, error = _validate_external_url(base_url)
    if not is_valid:
        return False, {"error": error}

    import base64

    auth = base64.b64encode(f"{email}:{api_token}".encode()).decode()
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            _build_safe_url(base_url, "rest/api/3/myself"),
            headers={"Authorization": f"Basic {auth}", "Accept": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return True, {
                "user": data.get("emailAddress"),
                "name": data.get("displayName"),
            }
        return False, {
            "status": resp.status_code,
            # CHAOS-2780: this is a raw response body from the external
            # provider, not something this codebase formats -- some
            # providers echo request details (including the submitted
            # credential) back in error/diagnostic bodies, so it must go
            # through the same redaction as any other error-bearing field
            # in this response. Sanitize the FULL body (not a blind [:200]
            # slice first) so the 200-char cap can never split a credential
            # in half and leave a partial, still-identifiable fragment --
            # same redact-before-truncate ordering sanitize_error_text
            # itself guarantees internally.
            "error": sanitize_error_text(resp.text, max_length=200),
        }


async def _test_linear_connection(creds: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    import httpx

    api_key = _string_value(creds.get("apiKey")) or _string_value(creds.get("api_key"))
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
        return False, {
            "status": resp.status_code,
            # CHAOS-2780: this is a raw response body from the external
            # provider, not something this codebase formats -- some
            # providers echo request details (including the submitted
            # credential) back in error/diagnostic bodies, so it must go
            # through the same redaction as any other error-bearing field
            # in this response. Sanitize the FULL body (not a blind [:200]
            # slice first) so the 200-char cap can never split a credential
            # in half and leave a partial, still-identifiable fragment --
            # same redact-before-truncate ordering sanitize_error_text
            # itself guarantees internally.
            "error": sanitize_error_text(resp.text, max_length=200),
        }


async def _test_pagerduty_connection(
    creds: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    """Preflight a minimal PagerDuty read without exposing credential material."""
    from dev_health_ops.providers.pagerduty.auth import ApiTokenAuth, OAuthBearerAuth
    from dev_health_ops.providers.pagerduty.client import PagerDutyClient
    from dev_health_ops.providers.pagerduty.oauth import missing_read_scopes

    access_token = creds.get("access_token")
    api_token = creds.get("api_token")
    if not access_token and not api_token:
        return False, {"error": "PagerDuty access_token or api_token is required"}
    auth = (
        OAuthBearerAuth(str(access_token))
        if access_token
        else ApiTokenAuth(str(api_token))
    )
    client = PagerDutyClient(auth, region=str(creds.get("region", "us")))
    users = await client.list_users()
    datasets = {str(value) for value in creds.get("enabled_datasets", [])}
    granted = {str(value) for value in creds.get("granted_scopes", [])}
    missing = sorted(missing_read_scopes(datasets, granted)) if access_token else []
    return not missing, {"users_checked": len(users), "missing_scopes": missing}


async def _test_launchdarkly_connection(
    creds: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    from dev_health_ops.connectors.launchdarkly import LaunchDarklyConnector

    api_key = _string_value(creds.get("api_key"))
    project_key = _string_value(creds.get("project_key"))
    if not api_key or not project_key:
        return False, {"error": "Missing required credentials (api_key, project_key)"}

    async with LaunchDarklyConnector(
        api_key=api_key, project_key=project_key
    ) as connector:
        flags = await connector.get_flags(project_key)
        return True, {"project_key": project_key, "flag_count": len(flags)}
