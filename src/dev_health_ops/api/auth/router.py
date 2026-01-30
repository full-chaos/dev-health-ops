from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from dev_health_ops.api.services.auth import get_auth_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


class TokenRefreshRequest(BaseModel):
    refresh_token: str


class TokenRefreshResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class TokenValidateRequest(BaseModel):
    token: str


class TokenValidateResponse(BaseModel):
    valid: bool
    user_id: str | None = None
    email: str | None = None
    org_id: str | None = None
    role: str | None = None
    expires_at: str | None = None


@router.post("/refresh", response_model=TokenRefreshResponse)
async def refresh_token(payload: TokenRefreshRequest) -> TokenRefreshResponse:
    auth_service = get_auth_service()

    refresh_payload = auth_service.validate_token(
        payload.refresh_token, token_type="refresh"
    )
    if not refresh_payload:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")

    new_access_token = auth_service.create_access_token(
        user_id=refresh_payload["sub"],
        email=refresh_payload.get("email", ""),
        org_id=refresh_payload.get("org_id", ""),
        role=refresh_payload.get("role", "member"),
        is_superuser=refresh_payload.get("is_superuser", False),
    )

    return TokenRefreshResponse(
        access_token=new_access_token,
        token_type="bearer",
        expires_in=3600,
    )


@router.post("/validate", response_model=TokenValidateResponse)
async def validate_token(payload: TokenValidateRequest) -> TokenValidateResponse:
    auth_service = get_auth_service()
    user = auth_service.get_authenticated_user(payload.token)

    if not user:
        return TokenValidateResponse(valid=False)

    return TokenValidateResponse(
        valid=True,
        user_id=user.user_id,
        email=user.email,
        org_id=user.org_id,
        role=user.role,
    )


@router.post("/logout")
async def logout() -> dict:
    return {"message": "Logout successful"}
