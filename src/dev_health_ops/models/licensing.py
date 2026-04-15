"""Licensing and Feature Flag models for Enterprise Edition.

This module defines the models for:
- Feature Flags: Available features that can be gated by tier or toggle
- Org Feature Overrides: Per-org feature enable/disable toggles
- Org Licenses: License key storage and validation metadata
- Tier Limits: Configurable limits per organization tier

These models support both SaaS (tier-based gating) and self-hosted (license-based) deployments.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.models.git import GUID, Base


class FeatureCategory(str, Enum):
    """Categories for grouping features."""

    CORE = "core"
    ANALYTICS = "analytics"
    INTEGRATIONS = "integrations"
    SECURITY = "security"
    COMPLIANCE = "compliance"
    ADMIN = "admin"


class FeatureFlag(Base):
    """Feature flag definitions.

    Defines available features that can be gated by tier or toggled per-org.
    Features are global definitions; per-org state is in OrgFeatureOverride.

    Example features:
    - capacity_forecast: Capacity planning forecasts
    - sso_saml: SAML SSO authentication
    - audit_log: Audit logging
    - custom_retention: Custom data retention policies
    """

    __tablename__ = "feature_flags"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    key = Column(
        Text,
        nullable=False,
        unique=True,
        index=True,
        comment="Unique feature identifier (e.g., 'capacity_forecast', 'sso_saml')",
    )
    name = Column(Text, nullable=False, comment="Human-readable feature name")
    description = Column(Text, nullable=True)
    category = Column(
        Text,
        nullable=False,
        default=FeatureCategory.CORE.value,
        index=True,
    )

    min_tier = Column(
        Text,
        nullable=False,
        default=LicenseTier.COMMUNITY.value,
        comment="Minimum tier required to access this feature",
    )
    is_enabled = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="Global kill switch for this feature",
    )
    is_beta = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="Whether this feature is in beta",
    )
    is_deprecated = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="Whether this feature is deprecated",
    )
    config_schema = Column(
        JSON,
        nullable=True,
        comment="JSON Schema for feature-specific configuration",
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __init__(
        self,
        key: str,
        name: str,
        description: str | None = None,
        category: str = FeatureCategory.CORE.value,
        min_tier: str = LicenseTier.COMMUNITY.value,
        is_enabled: bool = True,
        is_beta: bool = False,
        is_deprecated: bool = False,
        config_schema: dict | None = None,
    ):
        self.id = uuid.uuid4()
        self.key = key
        self.name = name
        self.description = description
        self.category = category
        self.min_tier = min_tier
        self.is_enabled = is_enabled
        self.is_beta = is_beta
        self.is_deprecated = is_deprecated
        self.config_schema = config_schema
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return f"<FeatureFlag {self.key}>"


class OrgFeatureOverride(Base):
    """Per-organization feature overrides.

    Allows enabling/disabling features for specific organizations,
    overriding the default tier-based gating.

    Use cases:
    - Grant a free org access to a pro feature (trial/promotion)
    - Disable a feature for a specific org (compliance/support)
    - Store feature-specific configuration per org
    """

    __tablename__ = "org_feature_overrides"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    feature_id = Column(
        GUID(),
        ForeignKey("feature_flags.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    is_enabled = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="Override: True=force enable, False=force disable",
    )
    expires_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="When this override expires (null = never)",
    )
    config = Column(
        JSON,
        nullable=True,
        default=dict,
        comment="Feature-specific configuration for this org",
    )
    reason = Column(
        Text,
        nullable=True,
        comment="Why this override was created (support ticket, promotion, etc.)",
    )
    created_by = Column(
        GUID(),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    organization = relationship("Organization")
    feature = relationship("FeatureFlag")
    creator = relationship("User")

    __table_args__ = (
        UniqueConstraint("org_id", "feature_id", name="uq_org_feature_override"),
        Index("ix_org_feature_overrides_org", "org_id"),
        Index("ix_org_feature_overrides_feature", "feature_id"),
    )

    def __init__(
        self,
        org_id: uuid.UUID,
        feature_id: uuid.UUID,
        is_enabled: bool = True,
        expires_at: datetime | None = None,
        config: dict | None = None,
        reason: str | None = None,
        created_by: uuid.UUID | None = None,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.feature_id = feature_id
        self.is_enabled = is_enabled
        self.expires_at = expires_at
        self.config = config or {}
        self.reason = reason
        self.created_by = created_by
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return f"<OrgFeatureOverride org={self.org_id} feature={self.feature_id}>"


class OrgLicense(Base):
    """Organization license storage.

    Stores license keys and their decoded/validated information.
    License keys are JWT tokens encoding tier, features, limits, and expiry.

    For SaaS: license is auto-generated based on subscription tier.
    For self-hosted: license is provided by customer (validated against signing key).
    """

    __tablename__ = "org_licenses"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(
        GUID(),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    license_key = Column(
        Text,
        nullable=True,
        comment="Encrypted license key (JWT)",
    )
    tier = Column(
        Text,
        nullable=False,
        default=LicenseTier.COMMUNITY.value,
        comment="Decoded tier from license",
    )
    licensed_users = Column(
        Integer,
        nullable=True,
        comment="Max users allowed (null = unlimited)",
    )
    licensed_repos = Column(
        Integer,
        nullable=True,
        comment="Max repos allowed (null = unlimited)",
    )
    issued_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the license was issued",
    )
    expires_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the license expires (null = never)",
    )
    is_valid = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="Whether the license passed validation",
    )
    validation_error = Column(
        Text,
        nullable=True,
        comment="Error message if validation failed",
    )
    last_validated_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="When the license was last validated",
    )
    license_type = Column(
        Text,
        nullable=False,
        default="saas",
        comment="License type: saas, self-hosted, trial, evaluation",
    )
    customer_id = Column(
        Text,
        nullable=True,
        comment="External customer ID (Stripe, etc.)",
    )
    features_override = Column(
        JSON,
        nullable=True,
        default=dict,
        comment="Feature overrides encoded in license",
    )
    limits_override = Column(
        JSON,
        nullable=True,
        default=dict,
        comment="Limit overrides encoded in license",
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    organization = relationship("Organization")

    def __init__(
        self,
        org_id: uuid.UUID,
        tier: str = LicenseTier.COMMUNITY.value,
        license_key: str | None = None,
        licensed_users: int | None = None,
        licensed_repos: int | None = None,
        issued_at: datetime | None = None,
        expires_at: datetime | None = None,
        license_type: str = "saas",
        customer_id: str | None = None,
        features_override: dict | None = None,
        limits_override: dict | None = None,
    ):
        self.id = uuid.uuid4()
        self.org_id = org_id
        self.tier = tier
        self.license_key = license_key
        self.licensed_users = licensed_users
        self.licensed_repos = licensed_repos
        self.issued_at = issued_at
        self.expires_at = expires_at
        self.is_valid = True
        self.license_type = license_type
        self.customer_id = customer_id
        self.features_override = features_override or {}
        self.limits_override = limits_override or {}
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    def __repr__(self) -> str:
        return f"<OrgLicense org={self.org_id} tier={self.tier}>"


class TierLimit(Base):
    """Database-driven tier limit defaults.

    Stores the default limits for each tier so they can be changed at runtime
    without a code deploy.  ``TierLimitService`` reads from this table first
    and falls back to ``TIER_LIMITS_DEFAULTS`` only when no row exists.

    Each row represents one (tier, limit_key) pair — for example
    ("community", "max_repos", 3).
    """

    __tablename__ = "tier_limits"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    tier = Column(
        Text,
        nullable=False,
        index=True,
        comment="Tier this limit applies to (community, team, enterprise)",
    )
    limit_key = Column(
        Text,
        nullable=False,
        comment="Limit identifier (e.g. max_repos, backfill_days)",
    )
    limit_value = Column(
        Text,
        nullable=True,
        comment="Limit value as text (null = unlimited). Cast to int/float at read time.",
    )
    description = Column(
        Text,
        nullable=True,
        comment="Human-readable explanation of this limit",
    )

    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (UniqueConstraint("tier", "limit_key", name="uq_tier_limit_key"),)

    def __init__(
        self,
        tier: str,
        limit_key: str,
        limit_value: str | None = None,
        description: str | None = None,
    ):
        self.id = uuid.uuid4()
        self.tier = tier
        self.limit_key = limit_key
        self.limit_value = limit_value
        self.description = description
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)

    @property
    def typed_value(self) -> int | float | None:
        """Cast the text value to a numeric type."""
        if self.limit_value is None:
            return None
        try:
            # Try int first, then float
            f = float(self.limit_value)
            return int(f) if f == int(f) else f
        except (ValueError, TypeError):
            return None

    def __repr__(self) -> str:
        return f"<TierLimit {self.tier}/{self.limit_key}={self.limit_value}>"


# ---------------------------------------------------------------------------
# Hardcoded fallback — used only when the tier_limits table is empty or
# missing a key.  Once the migration seeds the DB rows, the service reads
# from the database instead.
# ---------------------------------------------------------------------------
TIER_LIMITS_DEFAULTS = {
    LicenseTier.COMMUNITY: {
        "max_users": 5,
        "max_repos": 3,
        "max_work_items": 1000,
        "retention_days": 30,
        "backfill_days": 30,
        "api_rate_limit_per_min": 100,
        "min_sync_interval_hours": 24,
    },
    LicenseTier.TEAM: {
        "max_users": 20,
        "max_repos": 10,
        "max_work_items": 10000,
        "retention_days": 90,
        "backfill_days": 90,
        "api_rate_limit_per_min": 500,
        "min_sync_interval_hours": 6,
    },
    LicenseTier.ENTERPRISE: {
        "max_users": None,
        "max_repos": None,
        "max_work_items": None,
        "retention_days": None,
        "backfill_days": None,
        "api_rate_limit_per_min": None,
        "min_sync_interval_hours": 0.25,
    },
}

# Backward-compat alias — existing code imports TIER_LIMITS
TIER_LIMITS = TIER_LIMITS_DEFAULTS

STANDARD_FEATURES = [
    (
        "git_sync",
        "Git Sync",
        FeatureCategory.CORE,
        LicenseTier.COMMUNITY,
        "Sync git commits and PRs",
    ),
    (
        "work_items_sync",
        "Work Items Sync",
        FeatureCategory.CORE,
        LicenseTier.COMMUNITY,
        "Sync work items from providers",
    ),
    (
        "basic_analytics",
        "Basic Analytics",
        FeatureCategory.ANALYTICS,
        LicenseTier.COMMUNITY,
        "Basic metrics and dashboards",
    ),
    (
        "team_management",
        "Team Management",
        FeatureCategory.CORE,
        LicenseTier.COMMUNITY,
        "Basic team configuration",
    ),
    (
        "github_integration",
        "GitHub Integration",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.TEAM,
        "GitHub provider integration",
    ),
    (
        "gitlab_integration",
        "GitLab Integration",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.TEAM,
        "GitLab provider integration",
    ),
    (
        "jira_integration",
        "Jira Integration",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.TEAM,
        "Jira provider integration",
    ),
    (
        "investment_view",
        "Investment View",
        FeatureCategory.ANALYTICS,
        LicenseTier.TEAM,
        "Investment categorization view",
    ),
    (
        "api_access",
        "API Access",
        FeatureCategory.CORE,
        LicenseTier.TEAM,
        "REST and GraphQL API access",
    ),
    (
        "capacity_forecast",
        "Capacity Forecast",
        FeatureCategory.ANALYTICS,
        LicenseTier.TEAM,
        "Capacity planning forecasts",
    ),
    (
        "work_graph",
        "Work Graph",
        FeatureCategory.ANALYTICS,
        LicenseTier.TEAM,
        "Work graph analysis",
    ),
    (
        "quadrant_analysis",
        "Quadrant Analysis",
        FeatureCategory.ANALYTICS,
        LicenseTier.TEAM,
        "Quadrant metrics analysis",
    ),
    (
        "linear_integration",
        "Linear Integration",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.TEAM,
        "Linear provider integration",
    ),
    (
        "llm_categorization",
        "LLM Categorization",
        FeatureCategory.ANALYTICS,
        LicenseTier.TEAM,
        "AI-powered work categorization",
    ),
    (
        "webhooks",
        "Webhooks",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.TEAM,
        "Webhook ingestion",
    ),
    (
        "scheduled_jobs",
        "Scheduled Jobs",
        FeatureCategory.CORE,
        LicenseTier.TEAM,
        "Automated scheduled sync jobs",
    ),
    (
        "sso_saml",
        "SAML SSO",
        FeatureCategory.SECURITY,
        LicenseTier.ENTERPRISE,
        "SAML single sign-on",
    ),
    (
        "sso_oidc",
        "OIDC SSO",
        FeatureCategory.SECURITY,
        LicenseTier.ENTERPRISE,
        "OIDC single sign-on",
    ),
    (
        "audit_log",
        "Audit Log",
        FeatureCategory.COMPLIANCE,
        LicenseTier.ENTERPRISE,
        "Audit logging",
    ),
    (
        "custom_retention",
        "Custom Retention",
        FeatureCategory.COMPLIANCE,
        LicenseTier.ENTERPRISE,
        "Custom data retention policies",
    ),
    (
        "ip_allowlist",
        "IP Allowlist",
        FeatureCategory.SECURITY,
        LicenseTier.ENTERPRISE,
        "IP address allowlisting",
    ),
    (
        "data_export",
        "Data Export",
        FeatureCategory.COMPLIANCE,
        LicenseTier.ENTERPRISE,
        "Bulk data export",
    ),
    (
        "multi_org",
        "Multi-Organization",
        FeatureCategory.ADMIN,
        LicenseTier.ENTERPRISE,
        "Multiple organization support",
    ),
    (
        "custom_branding",
        "Custom Branding",
        FeatureCategory.ADMIN,
        LicenseTier.ENTERPRISE,
        "Custom branding and white-label",
    ),
    (
        "priority_support",
        "Priority Support",
        FeatureCategory.ADMIN,
        LicenseTier.ENTERPRISE,
        "Priority support SLA",
    ),
]

# Re-export get_features_for_tier for callers that import from models.licensing.
# The canonical definition lives in licensing.types to avoid circular imports.
from dev_health_ops.licensing.types import (
    get_features_for_tier as get_features_for_tier,
)  # noqa: E402,F401
