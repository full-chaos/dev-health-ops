package admincli

// Feature is one row of the standard feature registry.
type Feature struct {
	Key         string
	Name        string
	Category    string
	MinTier     string
	Description string
}

// StandardFeatures is the feature registry `admin features seed` writes, in
// its order. It is a port of STANDARD_FEATURES in
// src/dev_health_ops/licensing/registry.py; while that file exists,
// TestStandardFeaturesMatchPython runs it and requires this list to equal it.
var StandardFeatures = []Feature{
	{Key: "git_sync", Name: "Git Sync", Category: "core", MinTier: "community", Description: "Sync git commits and PRs"},
	{Key: "work_items_sync", Name: "Work Items Sync", Category: "core", MinTier: "community", Description: "Sync work items from providers"},
	{Key: "basic_analytics", Name: "Basic Analytics", Category: "analytics", MinTier: "community", Description: "Basic metrics and dashboards"},
	{Key: "team_management", Name: "Team Management", Category: "core", MinTier: "community", Description: "Basic team configuration"},
	{Key: "github_integration", Name: "GitHub Integration", Category: "integrations", MinTier: "team", Description: "GitHub provider integration"},
	{Key: "gitlab_integration", Name: "GitLab Integration", Category: "integrations", MinTier: "team", Description: "GitLab provider integration"},
	{Key: "jira_integration", Name: "Jira Integration", Category: "integrations", MinTier: "team", Description: "Jira provider integration"},
	{Key: "investment_view", Name: "Investment View", Category: "analytics", MinTier: "team", Description: "Investment categorization view"},
	{Key: "api_access", Name: "API Access", Category: "core", MinTier: "team", Description: "REST and GraphQL API access"},
	{Key: "capacity_forecast", Name: "Capacity Forecast", Category: "analytics", MinTier: "team", Description: "Capacity planning forecasts"},
	{Key: "work_graph", Name: "Work Graph", Category: "analytics", MinTier: "team", Description: "Work graph analysis"},
	{Key: "quadrant_analysis", Name: "Quadrant Analysis", Category: "analytics", MinTier: "team", Description: "Quadrant metrics analysis"},
	{Key: "linear_integration", Name: "Linear Integration", Category: "integrations", MinTier: "team", Description: "Linear provider integration"},
	{Key: "llm_categorization", Name: "LLM Categorization", Category: "analytics", MinTier: "team", Description: "AI-powered work categorization"},
	{Key: "webhooks", Name: "Webhooks", Category: "integrations", MinTier: "team", Description: "Webhook ingestion"},
	{Key: "customer_push_ingest", Name: "Customer Push Ingest", Category: "integrations", MinTier: "team", Description: "Customer-owned external ingestion runners"},
	{Key: "canonical_incident_ingestion", Name: "Canonical Incident Ingestion", Category: "integrations", MinTier: "community", Description: "Canonical operational incident ingestion and consumption"},
	{Key: "agent_context_runtime", Name: "Agent Context Runtime", Category: "integrations", MinTier: "community", Description: "Hosted evidence-backed context for authorized coding agents"},
	{Key: "ask_dev", Name: "Ask Dev", Category: "analytics", MinTier: "community", Description: "Evidence-backed conversational interaction with Context Fabric"},
	{Key: "ask_dev_contextual_entrypoints", Name: "Ask Dev Contextual Entrypoints", Category: "analytics", MinTier: "community", Description: "Typed Ask Dev handoffs from approved product surfaces"},
	{Key: "ask_dev_wave_3_1", Name: "Ask Dev Wave 3.1", Category: "analytics", MinTier: "community", Description: "Server-owned question intent and named-subject preflight"},
	{Key: "scheduled_jobs", Name: "Scheduled Jobs", Category: "core", MinTier: "team", Description: "Automated scheduled sync jobs"},
	{Key: "sso_saml", Name: "SAML SSO", Category: "security", MinTier: "enterprise", Description: "SAML single sign-on"},
	{Key: "sso_oidc", Name: "OIDC SSO", Category: "security", MinTier: "enterprise", Description: "OIDC single sign-on"},
	{Key: "audit_log", Name: "Audit Log", Category: "compliance", MinTier: "enterprise", Description: "Audit logging"},
	{Key: "custom_retention", Name: "Custom Retention", Category: "compliance", MinTier: "enterprise", Description: "Custom data retention policies"},
	{Key: "ip_allowlist", Name: "IP Allowlist", Category: "security", MinTier: "enterprise", Description: "IP address allowlisting"},
	{Key: "data_export", Name: "Data Export", Category: "compliance", MinTier: "enterprise", Description: "Bulk data export"},
	{Key: "multi_org", Name: "Multi-Organization", Category: "admin", MinTier: "enterprise", Description: "Multiple organization support"},
	{Key: "custom_branding", Name: "Custom Branding", Category: "admin", MinTier: "enterprise", Description: "Custom branding and white-label"},
	{Key: "priority_support", Name: "Priority Support", Category: "admin", MinTier: "enterprise", Description: "Priority support SLA"},
	{Key: "byo_llm", Name: "BYO LLM", Category: "analytics", MinTier: "team", Description: "Bring-your-own LLM provider credentials"},
}
