package orgs

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The registry constants the entitlements read needs besides the decision
// engine: tiers, registry.STANDARD_FEATURES' keys and TIER_LIMITS. The live
// Python oracle in this package executes the Python registry and must agree.

// Tiers, lowest first (types.TIER_ORDER).
const (
	TierCommunity  = "community"
	TierTeam       = "team"
	TierEnterprise = "enterprise"
)

// TierOrder is types.TIER_ORDER.
var TierOrder = []string{TierCommunity, TierTeam, TierEnterprise}

// TierIndex returns the tier's rank; ok is false for a value LicenseTier()
// would refuse.
func TierIndex(tier string) (int, bool) {
	for index, known := range TierOrder {
		if known == tier {
			return index, true
		}
	}
	return 0, false
}

// CoerceTier is `LicenseTier(str(value))` with ValueError read as
// COMMUNITY.
func CoerceTier(tier string) string {
	if _, ok := TierIndex(tier); ok {
		return tier
	}
	return TierCommunity
}

// StandardFeatureKeys is registry.STANDARD_FEATURES' keys, in order.
var StandardFeatureKeys = []string{
	"git_sync", "work_items_sync", "basic_analytics", "team_management", "github_integration",
	"gitlab_integration", "jira_integration", "investment_view", "api_access", "capacity_forecast",
	"work_graph", "quadrant_analysis", "linear_integration", "llm_categorization", "webhooks",
	"customer_push_ingest", "canonical_incident_ingestion", "agent_context_runtime", "ask_dev",
	"ask_dev_contextual_entrypoints", "ask_dev_wave_3_1", "scheduled_jobs", "sso_saml", "sso_oidc",
	"audit_log", "custom_retention", "ip_allowlist", "data_export", "multi_org", "custom_branding",
	"priority_support", "byo_llm",
}

// LimitValue is one TIER_LIMITS value: nil (unlimited), an int, or a float.
type LimitValue any

// TierLimit is one (key, value) of TIER_LIMITS, in the dict's order.
type TierLimit struct {
	Key   string
	Value LimitValue // nil, int64 or float64
}

// TierLimits is models.licensing.TIER_LIMITS (TIER_LIMITS_DEFAULTS).
var TierLimits = map[string][]TierLimit{
	TierCommunity: {
		{"max_users", int64(5)}, {"max_repos", int64(3)}, {"max_work_items", int64(1000)},
		{"retention_days", int64(30)}, {"backfill_days", int64(30)},
		{"api_rate_limit_per_min", int64(100)}, {"min_sync_interval_hours", int64(24)},
	},
	TierTeam: {
		{"max_users", int64(20)}, {"max_repos", int64(10)}, {"max_work_items", int64(10000)},
		{"retention_days", int64(90)}, {"backfill_days", int64(90)},
		{"api_rate_limit_per_min", int64(500)}, {"min_sync_interval_hours", int64(6)},
	},
	TierEnterprise: {
		{"max_users", nil}, {"max_repos", nil}, {"max_work_items", nil},
		{"retention_days", nil}, {"backfill_days", nil},
		{"api_rate_limit_per_min", nil}, {"min_sync_interval_hours", 0.25},
	},
}

// Querier is the pgx surface the loaders use (a pool or a transaction).
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// FeatureRow is a feature_flags row.
type FeatureRow struct {
	ID        uuid.UUID
	Key       string
	MinTier   string
	IsEnabled bool
}

// OverrideRow is an org_feature_overrides row.
type OverrideRow struct {
	FeatureID uuid.UUID
	IsEnabled bool
	ExpiresAt *time.Time
}

// LicenseRow is the org_licenses row of one org. The two JSON columns are
// decoded as json.loads would (nil for SQL NULL or JSON null).
type LicenseRow struct {
	Tier             string
	LicensedUsers    *int64
	LicensedRepos    *int64
	ExpiresAt        *time.Time
	IsValid          bool
	FeaturesOverride pyjson.Value
	LimitsOverride   pyjson.Value
}

// Rows is feature_decision_store.FeatureRows.
type Rows struct {
	Features  []FeatureRow
	Overrides []OverrideRow
	License   *LicenseRow
	// OrgTier is organizations.tier; nil when the org has no row.
	OrgTier *string
}

// LoadLicense reads the org's org_licenses row, or nil.
func LoadLicense(ctx context.Context, q Querier, orgID uuid.UUID) (*LicenseRow, error) {
	var row LicenseRow
	var features, limits *string
	var users, repos *int32
	err := q.QueryRow(ctx, `
SELECT tier, licensed_users, licensed_repos, expires_at, is_valid,
       features_override::text, limits_override::text
FROM org_licenses WHERE org_id = $1 LIMIT 1`, orgID,
	).Scan(&row.Tier, &users, &repos, &row.ExpiresAt, &row.IsValid, &features, &limits)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if users != nil {
		value := int64(*users)
		row.LicensedUsers = &value
	}
	if repos != nil {
		value := int64(*repos)
		row.LicensedRepos = &value
	}
	if row.FeaturesOverride, err = decodeJSON(features); err != nil {
		return nil, err
	}
	if row.LimitsOverride, err = decodeJSON(limits); err != nil {
		return nil, err
	}
	return &row, nil
}

func decodeJSON(text *string) (pyjson.Value, error) {
	if text == nil {
		return nil, nil
	}
	return pyjson.Decode([]byte(*text))
}

// LoadRows is load_feature_rows_async.
func LoadRows(ctx context.Context, q Querier, orgID uuid.UUID, keys []string) (Rows, error) {
	var rows Rows
	featureRows, err := q.Query(ctx,
		`SELECT id, key, min_tier, is_enabled FROM feature_flags WHERE key = ANY($1)`, keys)
	if err != nil {
		return Rows{}, err
	}
	rows.Features, err = pgx.CollectRows(featureRows, func(row pgx.CollectableRow) (FeatureRow, error) {
		var feature FeatureRow
		err := row.Scan(&feature.ID, &feature.Key, &feature.MinTier, &feature.IsEnabled)
		return feature, err
	})
	if err != nil {
		return Rows{}, err
	}
	if len(rows.Features) > 0 {
		ids := make([]uuid.UUID, len(rows.Features))
		for index, feature := range rows.Features {
			ids[index] = feature.ID
		}
		overrideRows, err := q.Query(ctx, `
SELECT feature_id, is_enabled, expires_at FROM org_feature_overrides
WHERE org_id = $1 AND feature_id = ANY($2)`, orgID, ids)
		if err != nil {
			return Rows{}, err
		}
		rows.Overrides, err = pgx.CollectRows(overrideRows, func(row pgx.CollectableRow) (OverrideRow, error) {
			var override OverrideRow
			err := row.Scan(&override.FeatureID, &override.IsEnabled, &override.ExpiresAt)
			return override, err
		})
		if err != nil {
			return Rows{}, err
		}
	}
	if rows.License, err = LoadLicense(ctx, q, orgID); err != nil {
		return Rows{}, err
	}
	var tier string
	switch err := q.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, orgID).Scan(&tier); {
	case err == pgx.ErrNoRows:
	case err != nil:
		return Rows{}, err
	default:
		rows.OrgTier = &tier
	}
	return rows, nil
}

// LicenseOverrides is the bool map _decisions_from_rows reads from
// features_override: {str(k): bool(v)} for a JSON object, else empty.
func LicenseOverrides(value pyjson.Value) *pyjson.Object {
	out := pyjson.NewObject()
	object, ok := value.(*pyjson.Object)
	if !ok {
		return out
	}
	for _, key := range object.Keys() {
		item, _ := object.Get(key)
		out.Set(key, Truthy(item))
	}
	return out
}

// Truthy is bool() of a decoded JSON value.
func Truthy(value pyjson.Value) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case pyjson.Int:
		return typed.Int != nil && typed.Sign() != 0
	case pyjson.Float:
		return typed != 0
	case []pyjson.Value:
		return len(typed) > 0
	case *pyjson.Object:
		return typed.Len() > 0
	default:
		return true
	}
}

// Decisions is _decisions_from_rows: allowed per key, each decided by the
// shared engine (licensing.Decide, feature_policy.decide_feature).
func Decisions(keys []string, rows Rows, evaluatedAt time.Time) map[string]bool {
	featuresByKey := make(map[string]FeatureRow, len(rows.Features))
	for _, feature := range rows.Features {
		featuresByKey[feature.Key] = feature
	}
	overridesByFeature := make(map[uuid.UUID]OverrideRow, len(rows.Overrides))
	for _, override := range rows.Overrides {
		overridesByFeature[override.FeatureID] = override
	}
	licenseOverrides := pyjson.NewObject()
	orgTier := "None" // str(None): LicenseTier() refuses it, so COMMUNITY
	if rows.OrgTier != nil {
		orgTier = *rows.OrgTier
	}
	if rows.License != nil {
		licenseOverrides = LicenseOverrides(rows.License.FeaturesOverride)
		orgTier = rows.License.Tier
	}
	allowed := make(map[string]bool, len(keys))
	for _, key := range keys {
		state := licensing.State{MinTier: TierCommunity, OrgTier: orgTier, EvaluatedAt: evaluatedAt}
		if value, ok := licenseOverrides.Get(key); ok {
			override := value.(bool)
			state.LicenseOverride = &override
		}
		if feature, registered := featuresByKey[key]; registered {
			state.Registered = true
			state.MinTier = feature.MinTier
			state.GloballyEnabled = feature.IsEnabled
			if row, ok := overridesByFeature[feature.ID]; ok {
				state.OrgOverride = &licensing.Override{Enabled: row.IsEnabled, ExpiresAt: row.ExpiresAt}
			}
		}
		allowed[key] = licensing.Decide(key, state).Allowed
	}
	return allowed
}

// UniqueSorted is sorted(set(keys)).
func UniqueSorted(keys []string) []string {
	seen := make(map[string]bool, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if !seen[key] {
			seen[key] = true
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}
