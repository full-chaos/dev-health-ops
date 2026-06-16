from __future__ import annotations

import logging
import os
import uuid as uuid_mod
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from dev_health_ops.api.services.oauth import (
    OAuthConfig,
    OAuthProviderError,
    OAuthUserInfoError,
    create_oauth_provider,
    get_default_scopes,
)
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.models.users import AuthProvider, Membership, User

from .common import LoginResponse, UserInfo, _expiry_to_utc

logger = logging.getLogger(__name__)

router = APIRouter()


class SocialLoginRequest(BaseModel):
    provider: str = Field(..., pattern="^(github|gitlab|google)$")
    provider_access_token: str


class SocialLoginResponse(LoginResponse):
    is_new_user: bool = False


@router.post("/social-login", response_model=SocialLoginResponse)
async def social_login(
    payload: SocialLoginRequest,
    request: Request,
) -> SocialLoginResponse:
    from dev_health_ops.api.auth.router import (
        create_refresh_token_record,
        get_auth_service,
        get_postgres_session,
    )

    _SOCIAL_PROVIDERS = {"github": "GITHUB", "gitlab": "GITLAB", "google": "GOOGLE"}
    provider_key = _SOCIAL_PROVIDERS.get(payload.provider.lower())
    if not provider_key:
        raise HTTPException(
            status_code=400, detail=error_detail("Unsupported provider")
        )
    provider_name = provider_key.lower()
    env_prefix = f"SOCIAL_{provider_key}"
    client_id = os.environ.get(f"{env_prefix}_CLIENT_ID", "")
    client_secret = os.environ.get(f"{env_prefix}_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        raise HTTPException(
            status_code=503,
            detail=error_detail(f"Social login not configured for {provider_name}"),
        )

    config = OAuthConfig(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri="https://localhost",
        scopes=get_default_scopes(provider_name),
    )

    try:
        provider = create_oauth_provider(provider_name, config)
        user_info = await provider.fetch_user_info(payload.provider_access_token)
    except OAuthUserInfoError as exc:
        logger.warning(
            "Social login user info fetch failed for %s: %s",
            provider_name,
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=401,
            detail=error_detail("Invalid or expired provider token"),
        ) from exc
    except OAuthProviderError as exc:
        logger.error(
            "Social login provider error for %s: %s", provider_name, type(exc).__name__
        )
        raise HTTPException(
            status_code=502,
            detail=error_detail("Failed to verify with provider"),
        ) from exc

    provider_enum = AuthProvider(provider_name)

    is_new_user = False

    async with get_postgres_session() as db:
        result = await db.execute(
            select(User).where(
                User.auth_provider_id == user_info.provider_user_id,
                User.auth_provider == provider_enum.value,
            )
        )
        user = result.scalar_one_or_none()

        if user is None:
            result = await db.execute(
                select(User).where(func.lower(User.email) == user_info.email.lower())
            )
            user = result.scalar_one_or_none()

            if user is not None:
                existing_provider = str(user.auth_provider)
                if existing_provider == AuthProvider.LOCAL.value:
                    pass
                elif existing_provider == provider_enum.value:
                    current_provider_user_id = getattr(user, "auth_provider_id", None)
                    if current_provider_user_id != user_info.provider_user_id:
                        setattr(
                            user,
                            "auth_provider_id",
                            user_info.provider_user_id,
                        )
                else:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": (
                                f"An account with this email already exists. "
                                f"Please sign in with {existing_provider}."
                            ),
                            "existing_provider": existing_provider,
                        },
                    )

        if user is None:
            user = User(
                email=user_info.email,
                username=user_info.username,
                full_name=user_info.full_name,
                auth_provider=provider_enum.value,
                auth_provider_id=user_info.provider_user_id,
                is_verified=True,
                password_hash=None,
            )
            db.add(user)
            await db.flush()
            is_new_user = True

        membership_result = await db.execute(
            select(Membership).where(Membership.user_id == user.id).limit(1)
        )
        membership = membership_result.scalar_one_or_none()
        needs_onboarding = membership is None and not bool(user.is_superuser)

        setattr(user, "last_login_at", datetime.now(timezone.utc))

        await db.commit()

        auth_service = get_auth_service()
        token_pair = auth_service.create_token_pair(
            user_id=str(user.id),
            email=str(user.email),
            org_id=str(membership.org_id) if membership else "",
            role=str(membership.role) if membership else "member",
            is_superuser=bool(user.is_superuser),
            username=str(user.username) if user.username is not None else None,
            full_name=str(user.full_name) if user.full_name is not None else None,
            token_version=int(getattr(user, "token_version", 0) or 0),
        )

        refresh_payload = auth_service.validate_token(
            token_pair.refresh_token, token_type="refresh"
        )
        if refresh_payload and membership and refresh_payload.get("jti"):
            expires_at = _expiry_to_utc(refresh_payload.get("exp"))
            if expires_at is not None:
                await create_refresh_token_record(
                    db=db,
                    user_id=str(user.id),
                    org_id=str(membership.org_id),
                    token_hash=str(refresh_payload["jti"]),
                    family_id=str(refresh_payload.get("family_id") or uuid_mod.uuid4()),
                    expires_at=expires_at,
                    ip_address=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent"),
                )

        return SocialLoginResponse(
            access_token=token_pair.access_token,
            refresh_token=token_pair.refresh_token,
            token_type=token_pair.token_type,
            expires_in=token_pair.expires_in,
            needs_onboarding=needs_onboarding,
            is_new_user=is_new_user,
            user=UserInfo(
                id=str(user.id),
                email=str(user.email),
                username=str(user.username) if user.username is not None else None,
                full_name=str(user.full_name) if user.full_name is not None else None,
                org_id=str(membership.org_id) if membership else None,
                role=str(membership.role) if membership else "member",
                is_superuser=bool(user.is_superuser),
            ),
        )
