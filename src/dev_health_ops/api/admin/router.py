from __future__ import annotations

from fastapi import APIRouter, Depends

from dev_health_ops.api.admin.middleware import require_admin

from .routers import (
    credentials_router,
    features_router,
    get_session,
    get_user_id,
    governance_router,
    identities_router,
    orgs_router,
    platform_router,
    settings_router,
    sync_router,
    teams_router,
    users_router,
)
from .routers.credentials import (
    _test_github_connection,
    _test_gitlab_connection,
    _test_jira_connection,
    _test_linear_connection,
)

router = APIRouter(
    prefix="/api/v1/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin)],
)

router.include_router(settings_router)
router.include_router(credentials_router)
router.include_router(sync_router)
router.include_router(identities_router)
router.include_router(teams_router)
router.include_router(users_router)
router.include_router(orgs_router)
router.include_router(platform_router)
router.include_router(features_router)
router.include_router(governance_router)

__all__ = [
    "router",
    "get_session",
    "get_user_id",
    "_test_github_connection",
    "_test_gitlab_connection",
    "_test_jira_connection",
    "_test_linear_connection",
]
