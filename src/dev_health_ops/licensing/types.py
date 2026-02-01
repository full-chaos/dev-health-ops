from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class LicenseTier(str, Enum):
    COMMUNITY = "community"
    TEAM = "team"
    ENTERPRISE = "enterprise"


class LicenseLimits(BaseModel):
    users: int = Field(description="Max users, -1 for unlimited")
    repos: int = Field(description="Max repos, -1 for unlimited")
    api_rate: int = Field(description="Requests per minute, -1 for unlimited")

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


DEFAULT_FEATURES: dict[LicenseTier, dict[str, bool]] = {
    LicenseTier.COMMUNITY: {
        "basic_analytics": True,
        "investment_view": True,
        "team_dashboard": False,
        "sso": False,
        "audit_log": False,
        "ip_allowlist": False,
        "retention_policies": False,
        "custom_integrations": False,
        "priority_support": False,
    },
    LicenseTier.TEAM: {
        "basic_analytics": True,
        "investment_view": True,
        "team_dashboard": True,
        "sso": False,
        "audit_log": False,
        "ip_allowlist": False,
        "retention_policies": False,
        "custom_integrations": True,
        "priority_support": False,
    },
    LicenseTier.ENTERPRISE: {
        "basic_analytics": True,
        "investment_view": True,
        "team_dashboard": True,
        "sso": True,
        "audit_log": True,
        "ip_allowlist": True,
        "retention_policies": True,
        "custom_integrations": True,
        "priority_support": True,
    },
}

DEFAULT_LIMITS: dict[LicenseTier, LicenseLimits] = {
    LicenseTier.COMMUNITY: LicenseLimits(users=5, repos=3, api_rate=60),
    LicenseTier.TEAM: LicenseLimits(users=25, repos=20, api_rate=300),
    LicenseTier.ENTERPRISE: LicenseLimits(users=-1, repos=-1, api_rate=-1),
}

GRACE_DAYS: dict[LicenseTier, int] = {
    LicenseTier.COMMUNITY: 0,
    LicenseTier.TEAM: 14,
    LicenseTier.ENTERPRISE: 30,
}
