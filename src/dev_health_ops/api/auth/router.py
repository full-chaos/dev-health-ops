from __future__ import annotations

import importlib
import logging

from fastapi import APIRouter

from dev_health_ops.api.services.auth import get_auth_service
from dev_health_ops.api.services.refresh_tokens import (
    create_refresh_token as create_refresh_token_record,
)
from dev_health_ops.db import get_postgres_session

from .routers import (
    _extract_unverified_org_and_subject,
    get_current_user,
    get_current_user_optional,
    invites_router,
    login_router,
    oauth_router,
    onboarding_router,
    password_reset_router,
    refresh_router,
    register_router,
    session_router,
    verify_router,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

router.include_router(register_router)
router.include_router(verify_router)
router.include_router(password_reset_router)
router.include_router(login_router)
router.include_router(invites_router)
router.include_router(onboarding_router)
router.include_router(session_router)
router.include_router(refresh_router)
router.include_router(oauth_router)

try:
    sso_module = importlib.import_module("dev_health_ops.api.auth.sso")
except ImportError as exc:
    logger.info(
        "SSO router not loaded because optional 'sso' module is missing: %s", exc
    )
else:
    maybe_sso_router = getattr(sso_module, "sso_router", None)
    if isinstance(maybe_sso_router, APIRouter):
        router.include_router(maybe_sso_router)

__all__ = [
    "_extract_unverified_org_and_subject",
    "create_refresh_token_record",
    "get_auth_service",
    "get_current_user",
    "get_current_user_optional",
    "get_postgres_session",
    "router",
]
