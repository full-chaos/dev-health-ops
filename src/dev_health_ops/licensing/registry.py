"""Canonical feature registry.

Single source of truth for feature keys. Lives in `licensing/` (not `models/`)
so it can be imported without pulling in SQLAlchemy, and without creating
a cycle between `models.licensing` and `licensing.types`.
"""

from __future__ import annotations

from typing import Final

from dev_health_ops.licensing.types import TIER_ORDER, FeatureCategory, LicenseTier

STANDARD_FEATURE_ROW = tuple[str, str, FeatureCategory, LicenseTier, str]

CANONICAL_INCIDENT_INGESTION_FEATURE: Final = "canonical_incident_ingestion"
ASK_DEV_FEATURE: Final = "ask_dev"
ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE: Final = "ask_dev_contextual_entrypoints"
#: Wave 3.1 rollout gate (Amendment TRD v2 §15 Phase A). Off means the run
#: behaves exactly as it does today: no server-owned interpretation, no subject
#: preflight, every tool advertised, and the CHAOS-3289 backstop terminating.
ASK_DEV_WAVE_3_1_FEATURE: Final = "ask_dev_wave_3_1"
#: Wave 3.2 design-partner rollout gate (CHAOS-3502). Off means the run
#: behaves exactly as it does today: no graph-assisted routing branch is
#: attempted, whether or not the orchestrator was constructed with a
#: ``graph_investigation_query`` -- this is the SECOND, organization-level
#: gate; ``orchestrator.graph_routing_runtime_enabled()`` is the independent,
#: same-process runtime kill switch. Both must be true for a run to attempt
#: the graph route.
ASK_DEV_GRAPH_ROUTING_FEATURE: Final = "ask_dev_graph_routing"
EXPLICIT_PURCHASE_FEATURES: frozenset[str] = frozenset(
    {
        "agent_context_runtime",
        ASK_DEV_FEATURE,
        ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE,
        ASK_DEV_WAVE_3_1_FEATURE,
        ASK_DEV_GRAPH_ROUTING_FEATURE,
    }
)
ORG_OVERRIDE_ONLY_FEATURES: frozenset[str] = frozenset()


def is_explicit_purchase_feature(feature_key: str) -> bool:
    return feature_key in EXPLICIT_PURCHASE_FEATURES


def is_org_override_only_feature(feature_key: str) -> bool:
    return feature_key in ORG_OVERRIDE_ONLY_FEATURES


def get_features_for_tier(tier: LicenseTier) -> dict[str, bool]:
    """Return a feature-key → enabled dict for the given tier.

    A feature is enabled when its ``min_tier`` is <= the requested tier.
    Canonical single source of truth (replaces the deleted ``DEFAULT_FEATURES``).
    """
    tier_index = TIER_ORDER.index(tier) if tier in TIER_ORDER else 0
    result: dict[str, bool] = {}
    for key, _name, _category, min_tier, _desc in STANDARD_FEATURES:
        min_index = TIER_ORDER.index(min_tier) if min_tier in TIER_ORDER else 0
        result[key] = tier_index >= min_index and not is_explicit_purchase_feature(key)
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
        "customer_push_ingest",
        "Customer Push Ingest",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.TEAM,
        "Customer-owned external ingestion runners",
    ),
    (
        CANONICAL_INCIDENT_INGESTION_FEATURE,
        "Canonical Incident Ingestion",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.COMMUNITY,
        "Canonical operational incident ingestion and consumption",
    ),
    (
        "agent_context_runtime",
        "Agent Context Runtime",
        FeatureCategory.INTEGRATIONS,
        LicenseTier.COMMUNITY,
        "Hosted evidence-backed context for authorized coding agents",
    ),
    (
        ASK_DEV_FEATURE,
        "Ask Dev",
        FeatureCategory.ANALYTICS,
        LicenseTier.COMMUNITY,
        "Evidence-backed conversational interaction with Context Fabric",
    ),
    (
        ASK_DEV_CONTEXTUAL_ENTRYPOINTS_FEATURE,
        "Ask Dev Contextual Entrypoints",
        FeatureCategory.ANALYTICS,
        LicenseTier.COMMUNITY,
        "Typed Ask Dev handoffs from approved product surfaces",
    ),
    (
        ASK_DEV_WAVE_3_1_FEATURE,
        "Ask Dev Wave 3.1",
        FeatureCategory.ANALYTICS,
        LicenseTier.COMMUNITY,
        "Server-owned question intent and named-subject preflight",
    ),
    (
        ASK_DEV_GRAPH_ROUTING_FEATURE,
        "Ask Dev Graph-Assisted Routing",
        FeatureCategory.ANALYTICS,
        LicenseTier.COMMUNITY,
        "Graph-assisted investigation for ambiguous, bounded, relational, and cohort questions (design-partner beta)",
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
    (
        "byo_llm",
        "BYO LLM",
        FeatureCategory.ANALYTICS,
        LicenseTier.TEAM,
        "Bring-your-own LLM provider credentials",
    ),
]
