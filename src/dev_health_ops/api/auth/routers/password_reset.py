from __future__ import annotations

import importlib
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, EmailStr
from sqlalchemy import func, select

from dev_health_ops.api.middleware.rate_limit import get_auth_key, limiter
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.models.users import User

from .common import VerifyEmailResponse

logger = logging.getLogger(__name__)

router = APIRouter()


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str


@router.post("/forgot-password", response_model=VerifyEmailResponse)
@limiter.limit("3/hour", key_func=get_auth_key)
async def forgot_password(
    payload: ForgotPasswordRequest,
    request: Request,
) -> VerifyEmailResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    password_reset_service = importlib.import_module(
        "dev_health_ops.api.services.password_reset"
    )
    create_reset_token = password_reset_service.create_password_reset_token
    send_reset_email = password_reset_service.send_password_reset_email

    generic_response = VerifyEmailResponse(
        message="If the account exists, a password reset email has been sent"
    )
    async with get_postgres_session() as db:
        email_normalized = payload.email.lower().strip()
        result = await db.execute(
            select(User).where(func.lower(User.email) == email_normalized)
        )
        user = result.scalar_one_or_none()
        if user is None:
            return generic_response

        reset_token = await create_reset_token(db, user.id)
        await db.commit()

        try:
            await send_reset_email(
                to_email=str(user.email),
                full_name=str(user.full_name) if user.full_name is not None else None,
                token=reset_token,
            )
        except Exception as exc:
            logger.error(  # nosemgrep: python-logger-credential-disclosure
                "Failed to send pw-reset email for %s: %s: %s",
                sanitize_for_log(payload.email),
                type(exc).__name__,
                sanitize_for_log(str(exc)),
                exc_info=True,
            )
        return generic_response


@router.post("/reset-password", response_model=VerifyEmailResponse)
async def reset_password(payload: ResetPasswordRequest) -> VerifyEmailResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    password_reset_service = importlib.import_module(
        "dev_health_ops.api.services.password_reset"
    )
    reset_with_token = password_reset_service.reset_password_with_token

    async with get_postgres_session() as db:
        user = await reset_with_token(db, payload.token, payload.new_password)
        if user is None:
            raise HTTPException(
                status_code=400,
                detail=error_detail("Invalid or expired token"),
            )
        await db.commit()

    return VerifyEmailResponse(message="Password reset successful")
