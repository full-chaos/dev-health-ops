from __future__ import annotations

import importlib
import logging
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, EmailStr
from sqlalchemy import func, select

from dev_health_ops.api.middleware.rate_limit import get_auth_key, limiter
from dev_health_ops.api.utils.errors import error_detail
from dev_health_ops.api.utils.logging import sanitize_for_log
from dev_health_ops.models.users import User

from .common import VerifyEmailResponse

logger = logging.getLogger(__name__)

router = APIRouter()


class ResendVerificationRequest(BaseModel):
    email: EmailStr


@router.get("/verify", response_model=VerifyEmailResponse)
@limiter.limit("10/hour", key_func=get_auth_key)
async def verify_email(
    token: Annotated[str, Query(min_length=1)],
    request: Request,
) -> VerifyEmailResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    email_verification_service = importlib.import_module(
        "dev_health_ops.api.services.email_verification"
    )
    verify_token = email_verification_service.verify_email_token

    async with get_postgres_session() as db:
        user = await verify_token(db, token)
        if user is None:
            raise HTTPException(
                status_code=400,
                detail=error_detail("Invalid or expired verification token"),
            )
        await db.commit()

    return VerifyEmailResponse(
        message="Email verified successfully",
        verified=True,
    )


@router.post("/resend-verification", response_model=VerifyEmailResponse)
@limiter.limit("3/hour", key_func=get_auth_key)
async def resend_verification_email(
    payload: ResendVerificationRequest,
    request: Request,
) -> VerifyEmailResponse:
    from dev_health_ops.api.auth.router import get_postgres_session

    email_verification_service = importlib.import_module(
        "dev_health_ops.api.services.email_verification"
    )
    create_verification_token = (
        email_verification_service.create_email_verification_token
    )
    send_verification = email_verification_service.send_verification_email

    generic_response = VerifyEmailResponse(
        message="If an account exists with that email, a verification link has been sent"
    )
    async with get_postgres_session() as db:
        email_normalized = payload.email.lower().strip()
        result = await db.execute(
            select(User).where(func.lower(User.email) == email_normalized)
        )
        user = result.scalar_one_or_none()
        if user is None or bool(getattr(user, "is_verified", False)):
            return generic_response

        verification_token = await create_verification_token(db, user.id)
        await db.commit()

        try:
            await send_verification(
                to_email=str(user.email),
                full_name=str(user.full_name) if user.full_name is not None else None,
                token=verification_token,
            )
        except Exception as exc:
            logger.error(
                "Failed to resend verification email for %s: %s: %s",
                sanitize_for_log(payload.email),
                type(exc).__name__,
                sanitize_for_log(str(exc)),
                exc_info=True,
            )
        return generic_response
