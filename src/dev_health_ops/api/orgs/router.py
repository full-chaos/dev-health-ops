"""Self-service organization profile endpoints.

These endpoints let authenticated org members manage their own organization
profile (name, description) without requiring superuser access.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.auth.router import get_current_user
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.api.services.users import MembershipService, OrganizationService
from dev_health_ops.db import postgres_session_dependency

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/orgs", tags=["orgs"])


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class OrgProfileUpdate(BaseModel):
    """Fields an org admin/owner can self-service update."""

    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = None


class OrgProfileResponse(BaseModel):
    id: str
    slug: str
    name: str
    description: str | None
    tier: str
    is_active: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/me", response_model=OrgProfileResponse)
async def get_own_org(
    session: AsyncSession = Depends(postgres_session_dependency),
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> OrgProfileResponse:
    """Return the current user's organization profile."""
    org_id = current_user.org_id
    if not org_id:
        raise HTTPException(status_code=400, detail="No organization context")

    svc = OrganizationService(session)
    org = await svc.get_by_id(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    return OrgProfileResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        is_active=org.is_active,
    )


@router.patch("/me", response_model=OrgProfileResponse)
async def update_own_org(
    payload: OrgProfileUpdate,
    session: AsyncSession = Depends(postgres_session_dependency),
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> OrgProfileResponse:
    """Update name/description for the current user's organization.

    Requires the user to be an admin or owner of the organization.
    Does NOT require superuser access — this is the self-service path.
    """
    org_id = current_user.org_id
    if not org_id:
        raise HTTPException(status_code=400, detail="No organization context")

    # Verify the user has admin/owner role in this org
    membership_svc = MembershipService(session)
    membership = await membership_svc.get_membership(org_id, current_user.user_id)
    if not membership or membership.role not in ("admin", "owner"):
        raise HTTPException(
            status_code=403,
            detail="You must be an org admin or owner to update organization settings",
        )

    svc = OrganizationService(session)
    org = await svc.update(
        org_id=org_id,
        name=payload.name,
        description=payload.description,
        # Intentionally NOT passing tier, settings, is_active —
        # those require superuser via the admin endpoint.
    )
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    return OrgProfileResponse(
        id=str(org.id),
        slug=org.slug,
        name=org.name,
        description=org.description,
        tier=org.tier,
        is_active=org.is_active,
    )
