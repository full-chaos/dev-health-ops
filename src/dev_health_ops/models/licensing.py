"""Licensing, Feature Flags, and Tier models for Enterprise Edition.

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
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from dev_health_ops.models.git import Base, GUID


class Tier(str, Enum):
    """Organization tier levels. Higher tiers unlock more features and limits."""

    FREE = "free"
    STARTER = "starter"
    PRO = "pro"
    ENTERPRISE = "enterprise"


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
        default=Tier.FREE.value,
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
        description: Optional[str] = None,
        category: str = FeatureCategory.CORE.value,
        min_tier: str = Tier.FREE.value,
        is_enabled: bool = True,
        is_beta: bool = False,
        is_deprecated: bool = False,
        config_schema: Optional[dict] = None,
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
        expires_at: Optional[datetime] = None,
        config: Optional[dict] = None,
        reason: Optional[str] = None,
        created_by: Optional[uuid.UUID] = None,
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
        default=Tier.FREE.value,
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
        tier: str = Tier.FREE.value,
        license_key: Optional[str] = None,
        licensed_users: Optional[int] = None,
        licensed_repos: Optional[int] = None,
        issued_at: Optional[datetime] = None,
        expires_at: Optional[datetime] = None,
        license_type: str = "saas",
        customer_id: Optional[str] = None,
        features_override: Optional[dict] = None,
        limits_override: Optional[dict] = None,
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


TIER_LIMITS = {
    Tier.FREE: {
        "max_users": 5,
        "max_repos": 3,
        "max_work_items": 1000,
        "retention_days": 30,
        "api_rate_limit_per_min": 100,
        "min_sync_interval_hours": 24,
    },
    Tier.STARTER: {
        "max_users": 20,
        "max_repos": 10,
        "max_work_items": 10000,
        "retention_days": 90,
        "api_rate_limit_per_min": 500,
        "min_sync_interval_hours": 6,
    },
    Tier.PRO: {
        "max_users": 100,
        "max_repos": 50,
        "max_work_items": 100000,
        "retention_days": 365,
        "api_rate_limit_per_min": 2000,
        "min_sync_interval_hours": 1,
    },
    Tier.ENTERPRISE: {
        "max_users": None,
        "max_repos": None,
        "max_work_items": None,
        "retention_days": None,
        "api_rate_limit_per_min": None,
        "min_sync_interval_hours": 0.25,
    },
}

STANDARD_FEATURES = [
    (
        "git_sync",
        "Git Sync",
        FeatureCategory.CORE,
        Tier.FREE,
        "Sync git commits and PRs",
    ),
    (
        "work_items_sync",
        "Work Items Sync",
        FeatureCategory.CORE,
        Tier.FREE,
        "Sync work items from providers",
    ),
    (
        "basic_analytics",
        "Basic Analytics",
        FeatureCategory.ANALYTICS,
        Tier.FREE,
        "Basic metrics and dashboards",
    ),
    (
        "team_management",
        "Team Management",
        FeatureCategory.CORE,
        Tier.FREE,
        "Basic team configuration",
    ),
    (
        "github_integration",
        "GitHub Integration",
        FeatureCategory.INTEGRATIONS,
        Tier.STARTER,
        "GitHub provider integration",
    ),
    (
        "gitlab_integration",
        "GitLab Integration",
        FeatureCategory.INTEGRATIONS,
        Tier.STARTER,
        "GitLab provider integration",
    ),
    (
        "jira_integration",
        "Jira Integration",
        FeatureCategory.INTEGRATIONS,
        Tier.STARTER,
        "Jira provider integration",
    ),
    (
        "investment_view",
        "Investment View",
        FeatureCategory.ANALYTICS,
        Tier.STARTER,
        "Investment categorization view",
    ),
    (
        "api_access",
        "API Access",
        FeatureCategory.CORE,
        Tier.STARTER,
        "REST and GraphQL API access",
    ),
    (
        "capacity_forecast",
        "Capacity Forecast",
        FeatureCategory.ANALYTICS,
        Tier.PRO,
        "Capacity planning forecasts",
    ),
    (
        "work_graph",
        "Work Graph",
        FeatureCategory.ANALYTICS,
        Tier.PRO,
        "Work graph analysis",
    ),
    (
        "quadrant_analysis",
        "Quadrant Analysis",
        FeatureCategory.ANALYTICS,
        Tier.PRO,
        "Quadrant metrics analysis",
    ),
    (
        "linear_integration",
        "Linear Integration",
        FeatureCategory.INTEGRATIONS,
        Tier.PRO,
        "Linear provider integration",
    ),
    (
        "llm_categorization",
        "LLM Categorization",
        FeatureCategory.ANALYTICS,
        Tier.PRO,
        "AI-powered work categorization",
    ),
    (
        "webhooks",
        "Webhooks",
        FeatureCategory.INTEGRATIONS,
        Tier.PRO,
        "Webhook ingestion",
    ),
    (
        "scheduled_jobs",
        "Scheduled Jobs",
        FeatureCategory.CORE,
        Tier.PRO,
        "Automated scheduled sync jobs",
    ),
    (
        "sso_saml",
        "SAML SSO",
        FeatureCategory.SECURITY,
        Tier.ENTERPRISE,
        "SAML single sign-on",
    ),
    (
        "sso_oidc",
        "OIDC SSO",
        FeatureCategory.SECURITY,
        Tier.ENTERPRISE,
        "OIDC single sign-on",
    ),
    (
        "audit_log",
        "Audit Log",
        FeatureCategory.COMPLIANCE,
        Tier.ENTERPRISE,
        "Audit logging",
    ),
    (
        "custom_retention",
        "Custom Retention",
        FeatureCategory.COMPLIANCE,
        Tier.ENTERPRISE,
        "Custom data retention policies",
    ),
    (
        "ip_allowlist",
        "IP Allowlist",
        FeatureCategory.SECURITY,
        Tier.ENTERPRISE,
        "IP address allowlisting",
    ),
    (
        "data_export",
        "Data Export",
        FeatureCategory.COMPLIANCE,
        Tier.ENTERPRISE,
        "Bulk data export",
    ),
    (
        "multi_org",
        "Multi-Organization",
        FeatureCategory.ADMIN,
        Tier.ENTERPRISE,
        "Multiple organization support",
    ),
    (
        "custom_branding",
        "Custom Branding",
        FeatureCategory.ADMIN,
        Tier.ENTERPRISE,
        "Custom branding and white-label",
    ),
    (
        "priority_support",
        "Priority Support",
        FeatureCategory.ADMIN,
        Tier.ENTERPRISE,
        "Priority support SLA",
    ),
]
