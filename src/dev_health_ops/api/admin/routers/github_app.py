from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.admin.middleware import get_admin_org_id
from dev_health_ops.api.integrations.github_app_config import (
    github_app_callback_url,
    github_app_client_id,
    github_app_client_secret,
    github_app_id,
    github_app_private_key,
    github_app_slug,
)
from dev_health_ops.api.integrations.github_app_state import (
    GITHUB_APP_STATE_TTL_MINUTES,
    GitHubAppStateError,
    mint_github_app_install_state,
    verify_github_app_install_state,
)
from dev_health_ops.api.services.configuration import IntegrationCredentialsService
from dev_health_ops.core.cache import create_cache
from dev_health_ops.models.settings import GithubAppInstallation

from .common import get_session

logger = logging.getLogger(__name__)

router = APIRouter()

# CHAOS-2676 (C4): the frontend may ask the backend to remember where the
# browser should land after the GitHub App install round-trip. The value is
# signed into the install-state JWT and echoed back by the callback; the Next
# route performs the actual redirect. Only an exact, in-app path from this
# allowlist is ever trusted: anything else (absolute URLs, protocol-relative
# //host, backslashes, encoded traversal, query strings, unknown paths) falls
# back to the admin default so a tampered value can never become an open
# redirect.
RETURN_TO_ADMIN_DEFAULT = "/org/admin/integrations/github"
RETURN_TO_ONBOARDING = "/auth/onboard/integration"
_ALLOWED_RETURN_TO = frozenset({RETURN_TO_ONBOARDING, RETURN_TO_ADMIN_DEFAULT})


def canonicalize_return_to(raw: str | None) -> str:
    """Return a safe, exact in-app redirect path for the install round-trip.

    Accepts only the two allowlisted paths verbatim; every other input —
    including ``None``, absolute URLs, protocol-relative ``//host``, paths with
    backslashes, percent-encoded traversal, or any query/fragment — collapses
    to :data:`RETURN_TO_ADMIN_DEFAULT`.
    """
    if not isinstance(raw, str):
        return RETURN_TO_ADMIN_DEFAULT
    if raw in _ALLOWED_RETURN_TO:
        return raw
    return RETURN_TO_ADMIN_DEFAULT


class GitHubInstallUrlRequest(BaseModel):
    return_to: str | None = None


class GitHubInstallUrlResponse(BaseModel):
    install_url: str


class GitHubInstallCallbackRequest(BaseModel):
    installation_id: int = Field(..., ge=1)
    setup_action: str | None = None
    state: str
    code: str | None = None


class GitHubInstallCallbackResponse(BaseModel):
    connected: bool
    installation_id: int
    credential_name: str
    return_to: str


@router.post(
    "/integrations/github/install-url",
    response_model=GitHubInstallUrlResponse,
)
async def create_github_install_url(
    body: GitHubInstallUrlRequest | None = None,
    org_id: str = Depends(get_admin_org_id),
) -> GitHubInstallUrlResponse:
    slug = github_app_slug()
    if not slug:
        raise HTTPException(status_code=400, detail="GITHUB_APP_SLUG is not configured")
    return_to = canonicalize_return_to(body.return_to if body else None)
    state = mint_github_app_install_state(org_id, return_to=return_to)
    params: dict[str, str] = {"state": state}
    callback_url = github_app_callback_url()
    if callback_url:
        params["redirect_uri"] = callback_url
    query = urlencode(params)
    install_url = (
        f"https://github.com/apps/{quote(slug, safe='')}/installations/new?{query}"
    )
    return GitHubInstallUrlResponse(install_url=install_url)


@router.post(
    "/integrations/github/install-callback",
    response_model=GitHubInstallCallbackResponse,
)
async def complete_github_install(
    body: GitHubInstallCallbackRequest,
    session: AsyncSession = Depends(get_session),
    admin_org_id: str = Depends(get_admin_org_id),
) -> GitHubInstallCallbackResponse:
    try:
        verified_state = verify_github_app_install_state(body.state)
    except GitHubAppStateError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if verified_state.org_id != admin_org_id:
        raise HTTPException(status_code=403, detail="GitHub App state org mismatch")
    if not body.code:
        raise HTTPException(
            status_code=400,
            detail="GitHub user authorization is required to complete the install; enable 'Request user authorization (OAuth) during installation' on the App",
        )
    _consume_install_state_jti(verified_state.jti)

    app_id = github_app_id()
    try:
        private_key = github_app_private_key()
    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail="GITHUB_APP_PRIVATE_KEY_PATH could not be read",
        ) from exc
    if not app_id or not private_key:
        raise HTTPException(
            status_code=500,
            detail="GITHUB_APP_ID and GITHUB_APP_PRIVATE_KEY or GITHUB_APP_PRIVATE_KEY_PATH are required",
        )
    verified_installation = await _verify_installer_installation_access(
        body.installation_id, body.code
    )

    await _upsert_installation(
        session=session,
        installation_id=body.installation_id,
        org_id=admin_org_id,
        account_login=_account_field(verified_installation, "login"),
        account_type=_account_field(verified_installation, "type"),
    )

    svc = IntegrationCredentialsService(session, admin_org_id)
    await svc.set(
        provider="github",
        name="github-app",
        credentials={
            "app_id": app_id,
            "private_key": private_key,
            "installation_id": str(body.installation_id),
        },
        config={
            "auth_mode": "github_app",
            "installation_id": body.installation_id,
            "setup_action": body.setup_action,
        },
        is_active=True,
    )
    return GitHubInstallCallbackResponse(
        connected=True,
        installation_id=body.installation_id,
        credential_name="github-app",
        return_to=canonicalize_return_to(verified_state.return_to),
    )


async def _upsert_installation(
    session: AsyncSession,
    installation_id: int,
    org_id: str,
    account_login: str | None,
    account_type: str | None,
) -> GithubAppInstallation:
    now = datetime.now(timezone.utc)
    existing_id = (
        await session.execute(
            select(GithubAppInstallation.id).where(
                GithubAppInstallation.installation_id == installation_id
            )
        )
    ).scalar_one_or_none()
    if existing_id is None:
        try:
            async with session.begin_nested():
                installation = GithubAppInstallation()
                installation.installation_id = installation_id
                installation.created_at = now
                installation.org_id = org_id
                installation.account_login = account_login
                installation.account_type = account_type
                installation.suspended_at = None
                installation.updated_at = now
                session.add(installation)
                await session.flush()
                return installation
        except IntegrityError as exc:
            raced = (
                await session.execute(
                    select(GithubAppInstallation).where(
                        GithubAppInstallation.installation_id == installation_id
                    )
                )
            ).scalar_one_or_none()
            if raced is None:
                raise exc

    claim_result = await session.execute(
        update(GithubAppInstallation)
        .where(
            GithubAppInstallation.installation_id == installation_id,
            or_(
                GithubAppInstallation.org_id.is_(None),
                GithubAppInstallation.org_id == org_id,
            ),
        )
        .values(
            org_id=org_id,
            account_login=account_login,
            account_type=account_type,
            suspended_at=None,
            updated_at=now,
        )
    )
    if getattr(claim_result, "rowcount", 0) == 0:
        raise HTTPException(
            status_code=409,
            detail="installation already linked to another organization",
        )
    await session.flush()
    claimed = (
        await session.execute(
            select(GithubAppInstallation).where(
                GithubAppInstallation.installation_id == installation_id
            )
        )
    ).scalar_one()
    return claimed


def _consume_install_state_jti(jti: str) -> None:
    # Primary replay protection is GitHub's single-use OAuth code; this cache is defense-in-depth.
    cache_key = f"github_app_install_state:{jti}"
    try:
        cache = create_cache(ttl_seconds=GITHUB_APP_STATE_TTL_MINUTES * 60)
        if cache.get(cache_key) is not None:
            raise HTTPException(
                status_code=400,
                detail="GitHub App installation state already used",
            )
        cache.set(cache_key, "used")
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            "GitHub App installation state replay cache unavailable: %s", exc
        )


async def _verify_installer_installation_access(
    installation_id: int,
    code: str,
) -> dict[str, Any]:
    client_id = github_app_client_id()
    client_secret = github_app_client_secret()
    if not client_id or not client_secret:
        raise HTTPException(
            status_code=500,
            detail="GitHub App OAuth client credentials are required",
        )
    async with httpx.AsyncClient(timeout=10) as client:
        token_response = await client.post(
            "https://github.com/login/oauth/access_token",
            headers={"Accept": "application/json"},
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
            },
        )
        if token_response.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail="GitHub user authorization could not be verified",
            )
        token_payload = token_response.json()
        access_token = token_payload.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise HTTPException(
                status_code=400,
                detail="GitHub user authorization could not be verified",
            )
        return await _find_accessible_installation(
            client, access_token, installation_id
        )


async def _find_accessible_installation(
    client: httpx.AsyncClient,
    access_token: str,
    installation_id: int,
) -> dict[str, Any]:
    page = 1
    while True:
        response = await client.get(
            "https://api.github.com/user/installations",
            params={"per_page": 100, "page": page},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        if response.status_code >= 400:
            raise HTTPException(
                status_code=400,
                detail="GitHub user authorization could not be verified",
            )
        payload = response.json()
        installations = payload.get("installations")
        if not isinstance(installations, list):
            raise HTTPException(
                status_code=400,
                detail="GitHub user authorization could not be verified",
            )
        for installation in installations:
            if (
                isinstance(installation, dict)
                and installation.get("id") == installation_id
            ):
                return installation
        if len(installations) < 100:
            break
        page += 1
    raise HTTPException(
        status_code=403,
        detail="installer does not have access to this GitHub App installation",
    )


def _account_field(installation: dict[str, Any], field: str) -> str | None:
    account = installation.get("account")
    if not isinstance(account, dict):
        return None
    value = account.get(field)
    return value if isinstance(value, str) else None
