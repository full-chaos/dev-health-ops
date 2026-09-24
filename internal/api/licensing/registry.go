package licensing

// standardFeature is one row of dev_health_ops.licensing.registry's
// STANDARD_FEATURES: the feature key and the lowest tier that includes it.
type standardFeature struct{ key, minTier string }

// standardFeatures is STANDARD_FEATURES in declaration order (the order
// get_features_for_tier's dict, and so the entitlement response, lists
// them). TestTierFeaturesMatchLivePython holds it to the Python registry.
var standardFeatures = []standardFeature{
	{"git_sync", "community"}, {"work_items_sync", "community"}, {"basic_analytics", "community"},
	{"team_management", "community"}, {"github_integration", "team"}, {"gitlab_integration", "team"},
	{"jira_integration", "team"}, {"investment_view", "team"}, {"api_access", "team"},
	{"capacity_forecast", "team"}, {"work_graph", "team"}, {"quadrant_analysis", "team"},
	{"linear_integration", "team"}, {"llm_categorization", "team"}, {"webhooks", "team"},
	{"customer_push_ingest", "team"}, {"canonical_incident_ingestion", "community"},
	{"agent_context_runtime", "community"}, {"ask_dev", "community"},
	{"ask_dev_contextual_entrypoints", "community"}, {"ask_dev_wave_3_1", "community"},
	{"scheduled_jobs", "team"}, {"sso_saml", "enterprise"}, {"sso_oidc", "enterprise"},
	{"audit_log", "enterprise"}, {"custom_retention", "enterprise"}, {"ip_allowlist", "enterprise"},
	{"data_export", "enterprise"}, {"multi_org", "enterprise"}, {"custom_branding", "enterprise"},
	{"priority_support", "enterprise"}, {"byo_llm", "team"},
}

// IsStandardFeature reports whether key is a STANDARD_FEATURES key (the
// registry _sync_org_license validates bundle features against).
func IsStandardFeature(key string) bool {
	for _, feature := range standardFeatures {
		if feature.key == key {
			return true
		}
	}
	return false
}

// TierFeature is one feature of a tier's default set.
type TierFeature struct {
	Key     string
	Enabled bool
}

// FeaturesForTier is get_features_for_tier: every standard feature, in
// registry order, enabled when the tier reaches its minimum tier and it is
// not an explicit-purchase feature. An unknown tier ranks as community.
func FeaturesForTier(tier string) []TierFeature {
	tierRank, ok := TierRank(tier)
	if !ok {
		tierRank = 0
	}
	out := make([]TierFeature, len(standardFeatures))
	for index, feature := range standardFeatures {
		minRank, _ := TierRank(feature.minTier)
		out[index] = TierFeature{Key: feature.key, Enabled: tierRank >= minRank && !explicitPurchaseFeatures[feature.key]}
	}
	return out
}

// Limits is the users/repos/api_rate part of DEFAULT_LIMITS[tier] (-1 =
// unlimited).
type Limits struct{ Users, Repos, APIRate int64 }

// DefaultLimits is licensing.types.DEFAULT_LIMITS; ok is false for a tier
// outside LicenseTier.
func DefaultLimits(tier string) (Limits, bool) {
	switch tier {
	case "community":
		return Limits{Users: 5, Repos: 3, APIRate: 100}, true
	case "team":
		return Limits{Users: 20, Repos: 10, APIRate: 500}, true
	case "enterprise":
		return Limits{Users: -1, Repos: -1, APIRate: -1}, true
	}
	return Limits{}, false
}

// EntitlementDecisionKeys is get_org_entitlements_from_db's decision_keys:
// sorted(EXPLICIT_PURCHASE_FEATURES | {canonical_incident_ingestion,
// customer_push_ingest}).
func EntitlementDecisionKeys() []string {
	return []string{"agent_context_runtime", "ask_dev", "ask_dev_contextual_entrypoints", "ask_dev_wave_3_1",
		"canonical_incident_ingestion", "customer_push_ingest"}
}
