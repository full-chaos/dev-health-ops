"""Canonical feature registry.

Single source of truth for feature keys. Lives in `licensing/` (not `models/`)
so it can be imported without pulling in SQLAlchemy, and without creating
a cycle between `models.licensing` and `licensing.types`.
"""

from __future__ import annotations

from dev_health_ops.licensing.types import TIER_ORDER, FeatureCategory, LicenseTier

STANDARD_FEATURE_ROW = tuple[str, str, FeatureCategory, LicenseTier, str]


def get_features_for_tier(tier: LicenseTier) -> dict[str, bool]:
    """Return a feature-key → enabled dict for the given tier.

    A feature is enabled when its ``min_tier`` is <= the requested tier.
    Canonical single source of truth (replaces the deleted ``DEFAULT_FEATURES``).
    """
    tier_index = TIER_ORDER.index(tier) if tier in TIER_ORDER else 0
    result: dict[str, bool] = {}
    for key, _name, _category, min_tier, _desc in STANDARD_FEATURES:
        min_index = TIER_ORDER.index(min_tier) if min_tier in TIER_ORDER else 0
        result[key] = tier_index >= min_index
    return result


STANDARD_FEATURES: list[STANDARD_FEATURE_ROW] = [
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
