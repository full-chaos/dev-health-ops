from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class LicenseTier(str, Enum):
    COMMUNITY = "community"
    TEAM = "team"
    ENTERPRISE = "enterprise"


class FeatureCategory(str, Enum):
    """Categories for grouping features."""

    CORE = "core"
    ANALYTICS = "analytics"
    INTEGRATIONS = "integrations"
    SECURITY = "security"
    COMPLIANCE = "compliance"
    ADMIN = "admin"


class LicenseLimits(BaseModel):
    users: int = Field(description="Max users, -1 for unlimited")
    repos: int = Field(description="Max repos, -1 for unlimited")
    api_rate: int = Field(description="Requests per minute, -1 for unlimited")
    backfill_days: int | None = Field(
        default=None, description="Max backfill days, None for unlimited"
    )

    def is_unlimited(self, field: str) -> bool:
        return getattr(self, field, 0) == -1


class LicensePayload(BaseModel):
    iss: Literal["fullchaos.studio"]
    sub: str = Field(description="Organization ID")
    iat: int = Field(description="Issued at (unix timestamp)")
    exp: int = Field(description="Expires (unix timestamp)")
    tier: LicenseTier
    features: dict[str, bool]
    limits: LicenseLimits
    grace_days: int = Field(ge=0, description="Days after expiry before hard cutoff")
    org_name: str | None = None
    contact_email: str | None = None
    license_id: str | None = None


DEFAULT_LIMITS: dict[LicenseTier, LicenseLimits] = {
    LicenseTier.COMMUNITY: LicenseLimits(
        users=5, repos=3, api_rate=100, backfill_days=30
    ),
    LicenseTier.TEAM: LicenseLimits(users=20, repos=10, api_rate=500, backfill_days=90),
    LicenseTier.ENTERPRISE: LicenseLimits(
        users=-1, repos=-1, api_rate=-1, backfill_days=None
    ),
}

GRACE_DAYS: dict[LicenseTier, int] = {
    LicenseTier.COMMUNITY: 0,
    LicenseTier.TEAM: 14,
    LicenseTier.ENTERPRISE: 30,
}

# Tier ordering for comparison (higher index = higher tier)
TIER_ORDER: list[LicenseTier] = [
    LicenseTier.COMMUNITY,
    LicenseTier.TEAM,
    LicenseTier.ENTERPRISE,
]
