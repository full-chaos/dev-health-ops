package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const jiraIncidentFeatureKey = "canonical_incident_ingestion"

var ErrJiraIncidentEntitlementDisabled = errors.New(
	"canonical Jira incident ingestion entitlement is disabled",
)

// JiraIncidentEntitlement is checked twice for one Jira incident occurrence:
// before provider fetch and immediately after collection before the batch is
// handed to persistence. This deliberately mirrors Python's
// _require_jira_incident_entitlement calls on both sides of
// JsmIncidentProducer.collect so a revoked entitlement cannot be bypassed by a
// long-running fetch.
type JiraIncidentEntitlement interface {
	Require(context.Context, string) error
}

type PostgresJiraIncidentEntitlement struct {
	Pool *pgxpool.Pool
	Now  func() time.Time
}

type jiraIncidentFeatureState struct {
	Registered      bool
	GloballyEnabled bool
	MinTier         string
	OrgTier         string
	OrgOverride     *jiraIncidentFeatureOverride
	LicenseOverride *bool
	EvaluatedAt     time.Time
}

type jiraIncidentFeatureOverride struct {
	Enabled   bool
	ExpiresAt *time.Time
}

type jiraIncidentFeatureDecision struct {
	FeatureKey string          `json:"feature_key"`
	Allowed    bool            `json:"allowed"`
	Reason     string          `json:"reason"`
	ExpiresAt  *time.Time      `json:"expires_at"`
	Config     *map[string]any `json:"config"`
}

func (entitlement PostgresJiraIncidentEntitlement) Require(
	ctx context.Context, orgID string,
) error {
	parsedOrgID, err := uuid.Parse(strings.TrimSpace(orgID))
	if ctx == nil || err != nil || entitlement.Pool == nil {
		return ErrJiraIncidentEntitlementDisabled
	}
	evaluatedAt := time.Now().UTC()
	if entitlement.Now != nil {
		evaluatedAt = entitlement.Now().UTC()
	}
	state, err := loadJiraIncidentFeatureState(
		ctx, entitlement.Pool, parsedOrgID.String(), evaluatedAt,
	)
	if err != nil || !jiraIncidentFeatureAllowed(state) {
		return ErrJiraIncidentEntitlementDisabled
	}
	return nil
}

type jiraIncidentFeatureQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadJiraIncidentFeatureState(
	ctx context.Context, queryer jiraIncidentFeatureQueryer,
	orgID string, evaluatedAt time.Time,
) (jiraIncidentFeatureState, error) {
	state := jiraIncidentFeatureState{
		MinTier: "community", OrgTier: "community", EvaluatedAt: evaluatedAt.UTC(),
	}
	var overrideEnabled *bool
	var overrideExpiresAt *time.Time
	var licenseTier, orgTier *string
	var encodedOverrides []byte
	// Read the complete policy input in one PostgreSQL statement. This gives
	// one MVCC snapshot without requiring UPDATE privilege solely to take row
	// locks, preserving the domain worker's read-only licensing posture.
	err := queryer.QueryRow(ctx, `
SELECT feature.min_tier, feature.is_enabled,
       org_override.is_enabled, org_override.expires_at,
       license.tier, license.features_override, organization.tier
FROM feature_flags AS feature
LEFT JOIN org_feature_overrides AS org_override
  ON org_override.org_id = $2::uuid AND org_override.feature_id = feature.id
LEFT JOIN org_licenses AS license ON license.org_id = $2::uuid
LEFT JOIN organizations AS organization ON organization.id = $2::uuid
WHERE feature.key = $1`, jiraIncidentFeatureKey, orgID).Scan(
		&state.MinTier, &state.GloballyEnabled,
		&overrideEnabled, &overrideExpiresAt,
		&licenseTier, &encodedOverrides, &orgTier,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	state.Registered = true
	if overrideEnabled != nil {
		state.OrgOverride = &jiraIncidentFeatureOverride{
			Enabled: *overrideEnabled, ExpiresAt: overrideExpiresAt,
		}
	}
	if licenseTier != nil {
		state.OrgTier = *licenseTier
		var overrides map[string]any
		if len(encodedOverrides) != 0 && json.Unmarshal(encodedOverrides, &overrides) != nil {
			return state, ErrJiraIncidentEntitlementDisabled
		}
		if raw, ok := overrides[jiraIncidentFeatureKey]; ok {
			value := pythonJSONTruth(raw)
			state.LicenseOverride = &value
		}
	} else if orgTier != nil {
		state.OrgTier = *orgTier
	}
	return state, nil
}

func jiraIncidentFeatureAllowed(state jiraIncidentFeatureState) bool {
	return decideJiraIncidentFeature(state).Allowed
}

func decideJiraIncidentFeature(state jiraIncidentFeatureState) jiraIncidentFeatureDecision {
	closed := func(reason string) jiraIncidentFeatureDecision {
		return jiraIncidentFeatureDecision{FeatureKey: jiraIncidentFeatureKey, Reason: reason}
	}
	minTier, minOK := jiraLicenseTierIndex(state.MinTier)
	orgTier, orgOK := jiraLicenseTierIndex(state.OrgTier)
	if !minOK {
		return closed("invalid_feature_state")
	}
	if !state.Registered {
		return closed("feature_not_registered")
	}
	if !state.GloballyEnabled {
		return closed("global_disabled")
	}
	if !orgOK {
		orgTier = 0
	}
	if state.OrgOverride != nil {
		expired := state.OrgOverride.ExpiresAt != nil &&
			!state.OrgOverride.ExpiresAt.After(state.EvaluatedAt)
		if !expired {
			if !state.OrgOverride.Enabled {
				return closed("org_override_disabled")
			}
			return jiraIncidentFeatureDecision{
				FeatureKey: jiraIncidentFeatureKey, Allowed: true,
				Reason: "enabled_by_org_override", ExpiresAt: state.OrgOverride.ExpiresAt,
			}
		}
	}
	if state.LicenseOverride != nil {
		if !*state.LicenseOverride {
			return closed("license_override_disabled")
		}
		return jiraIncidentFeatureDecision{
			FeatureKey: jiraIncidentFeatureKey, Allowed: true,
			Reason: "enabled_by_license_override",
		}
	}
	if orgTier >= minTier {
		return jiraIncidentFeatureDecision{
			FeatureKey: jiraIncidentFeatureKey, Allowed: true,
			Reason: "enabled_by_tier",
		}
	}
	return closed("tier_required")
}

func jiraLicenseTierIndex(value string) (int, bool) {
	switch value {
	case "community":
		return 0, true
	case "team":
		return 1, true
	case "enterprise":
		return 2, true
	default:
		return 0, false
	}
}

func pythonJSONTruth(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case float64:
		return typed != 0
	case []any:
		return len(typed) != 0
	case map[string]any:
		return len(typed) != 0
	default:
		return false
	}
}

var _ JiraIncidentEntitlement = PostgresJiraIncidentEntitlement{}
