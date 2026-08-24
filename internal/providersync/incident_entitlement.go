package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const canonicalIncidentFeatureKey = "canonical_incident_ingestion"

var ErrIncidentEntitlementDisabled = errors.New(
	"canonical incident ingestion entitlement is disabled",
)

// IncidentEntitlement is the execution-time re-check of the
// canonical_incident_ingestion feature. It gates every dataset whose legacy
// sync target is "incidents" or "operational" (src/dev_health_ops/sync/
// datasets.py, _GATED_SYNC_TARGETS): Jira incidents and EVERY PagerDuty
// dataset. One implementation serves both providers; only the feature key is
// provider-specific, and it is shared.
//
// A gated route calls it twice -- before provider fetch and again at the
// ClickHouse write boundary. This deliberately mirrors Python's
// require_canonical_incident_feature_for_update_sync calls on both sides of
// the producer (workers/sync_units.py:1289 and :1311) while also covering Go
// effect-ledger work between collection and persistence. The dispatch-side
// gate (CanonicalIncidentDecision) is NON-locking by ruling (CHAOS-4209), so a
// disable committed after its read is only harmless because this re-check
// refuses the unit at execution.
type IncidentEntitlement interface {
	Require(context.Context, string) error
}

// Seam labels for the refusal counter. Bounded because they become a
// Prometheus label; anything else collapses to "other" in providerfoundation.
const (
	IncidentEntitlementSeamCollect = "collect"
	IncidentEntitlementSeamWrite   = "write"
)

// requireIncidentEntitlement is the ONE call every gated route makes at each
// of its two seams. A nil entitlement is a construction defect and fails
// closed as ErrInvalidConfiguration rather than passing the unit through. A
// refusal is counted by provider, dataset and seam BEFORE it is returned, so a
// disable that landed after the dispatch gate's read is visible on
// dev_health_provider_incident_entitlement_refused_total, not merely harmless.
func requireIncidentEntitlement(
	ctx context.Context, entitlement IncidentEntitlement,
	metrics *providerfoundation.Metrics, claim Claim, seam string,
) error {
	if entitlement == nil {
		return ErrInvalidConfiguration
	}
	err := entitlement.Require(ctx, claim.OrgID)
	if err != nil {
		metrics.RecordIncidentEntitlementRefused(claim.Provider, claim.Dataset, seam)
	}
	return err
}

type PostgresIncidentEntitlement struct {
	Pool *pgxpool.Pool
	Now  func() time.Time
}

type canonicalIncidentFeatureState struct {
	Registered      bool
	GloballyEnabled bool
	MinTier         string
	OrgTier         string
	OrgOverride     *canonicalIncidentFeatureOverride
	LicenseOverride *bool
	EvaluatedAt     time.Time
}

type canonicalIncidentFeatureOverride struct {
	Enabled   bool
	ExpiresAt *time.Time
}

type canonicalIncidentFeatureDecision struct {
	FeatureKey string          `json:"feature_key"`
	Allowed    bool            `json:"allowed"`
	Reason     string          `json:"reason"`
	ExpiresAt  *time.Time      `json:"expires_at"`
	Config     *map[string]any `json:"config"`
}

func (entitlement PostgresIncidentEntitlement) Require(
	ctx context.Context, orgID string,
) error {
	parsedOrgID, err := uuid.Parse(strings.TrimSpace(orgID))
	if ctx == nil || err != nil || entitlement.Pool == nil {
		return ErrIncidentEntitlementDisabled
	}
	evaluatedAt := time.Now().UTC()
	if entitlement.Now != nil {
		evaluatedAt = entitlement.Now().UTC()
	}
	state, err := loadCanonicalIncidentFeatureState(
		ctx, entitlement.Pool, parsedOrgID.String(), evaluatedAt,
	)
	if err != nil || !canonicalIncidentFeatureAllowed(state) {
		return ErrIncidentEntitlementDisabled
	}
	return nil
}

type canonicalIncidentFeatureQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadCanonicalIncidentFeatureState(
	ctx context.Context, queryer canonicalIncidentFeatureQueryer,
	orgID string, evaluatedAt time.Time,
) (canonicalIncidentFeatureState, error) {
	state := canonicalIncidentFeatureState{
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
WHERE feature.key = $1`, canonicalIncidentFeatureKey, orgID).Scan(
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
		state.OrgOverride = &canonicalIncidentFeatureOverride{
			Enabled: *overrideEnabled, ExpiresAt: overrideExpiresAt,
		}
	}
	if licenseTier != nil {
		state.OrgTier = *licenseTier
		var overrides map[string]any
		if len(encodedOverrides) != 0 && json.Unmarshal(encodedOverrides, &overrides) != nil {
			return state, ErrIncidentEntitlementDisabled
		}
		if raw, ok := overrides[canonicalIncidentFeatureKey]; ok {
			value := pythonJSONTruth(raw)
			state.LicenseOverride = &value
		}
	} else if orgTier != nil {
		state.OrgTier = *orgTier
	}
	return state, nil
}

func canonicalIncidentFeatureAllowed(state canonicalIncidentFeatureState) bool {
	return decideCanonicalIncidentFeature(state).Allowed
}

func decideCanonicalIncidentFeature(state canonicalIncidentFeatureState) canonicalIncidentFeatureDecision {
	closed := func(reason string) canonicalIncidentFeatureDecision {
		return canonicalIncidentFeatureDecision{FeatureKey: canonicalIncidentFeatureKey, Reason: reason}
	}
	minTier, minOK := licenseTierIndex(state.MinTier)
	orgTier, orgOK := licenseTierIndex(state.OrgTier)
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
			return canonicalIncidentFeatureDecision{
				FeatureKey: canonicalIncidentFeatureKey, Allowed: true,
				Reason: "enabled_by_org_override", ExpiresAt: state.OrgOverride.ExpiresAt,
			}
		}
	}
	if state.LicenseOverride != nil {
		if !*state.LicenseOverride {
			return closed("license_override_disabled")
		}
		return canonicalIncidentFeatureDecision{
			FeatureKey: canonicalIncidentFeatureKey, Allowed: true,
			Reason: "enabled_by_license_override",
		}
	}
	if orgTier >= minTier {
		return canonicalIncidentFeatureDecision{
			FeatureKey: canonicalIncidentFeatureKey, Allowed: true,
			Reason: "enabled_by_tier",
		}
	}
	return closed("tier_required")
}

func licenseTierIndex(value string) (int, bool) {
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

var _ IncidentEntitlement = PostgresIncidentEntitlement{}
