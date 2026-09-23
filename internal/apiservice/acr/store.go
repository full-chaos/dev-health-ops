package acr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrOrgNotFound means the organization row does not exist. Python:
// api/internal/acr.py:181-192's 404 ("Not found") on both an unparseable
// org_id and a missing Organization row.
var ErrOrgNotFound = errors.New("organization not found")

// ErrUnavailable means the decision could not be READ (query error, pool
// exhaustion, scan failure, malformed features_override JSON). Python:
// api/internal/acr.py:196-204's 503 ("Service unavailable") on the
// get_org_entitlements_from_db call failing. Deliberately distinct from
// ErrOrgNotFound and from a closed (Allowed: false) decision -- a decision
// that was correctly evaluated and came back closed is not an error.
var ErrUnavailable = errors.New("agent_context_runtime entitlement could not be evaluated")

// Entitlement is the one fact this package's routes serve.
type Entitlement struct {
	// OrgID echoes the caller's own org_id path value verbatim, exactly as
	// Python's route does (api/internal/acr.py:223 returns the org_id
	// function parameter, not str(org_uuid)) -- acr's client asserts this
	// equals the org_id IT sent (internal/entitlements/response.go:137),
	// which trivially holds by construction on both planes.
	OrgID               string
	AgentContextRuntime bool
}

// EntitlementStore looks up one org's agent_context_runtime entitlement.
type EntitlementStore interface {
	Lookup(ctx context.Context, orgID string) (Entitlement, error)
}

// PostgresEntitlementStore is the production EntitlementStore.
type PostgresEntitlementStore struct {
	Pool *pgxpool.Pool
	// Now is injectable so a test can drive the override-expiry clock. Nil
	// means time.Now.
	Now func() time.Time
}

var _ EntitlementStore = PostgresEntitlementStore{}

func (store PostgresEntitlementStore) Lookup(ctx context.Context, orgID string) (Entitlement, error) {
	// Format validation runs before touching the pool, matching Python's own
	// order: uuid.UUID(org_id) is checked with no query issued for the check
	// itself (api/internal/acr.py:169-180), so a malformed org_id is 404 on
	// both planes even when the store is not configured at all.
	parsedOrgID, err := uuid.Parse(orgID)
	if err != nil {
		return Entitlement{}, ErrOrgNotFound
	}
	if store.Pool == nil {
		return Entitlement{}, ErrUnavailable
	}
	evaluatedAt := time.Now().UTC()
	if store.Now != nil {
		evaluatedAt = store.Now().UTC()
	}
	state, found, err := loadFeatureState(ctx, store.Pool, parsedOrgID.String(), evaluatedAt)
	if err != nil {
		return Entitlement{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if !found {
		return Entitlement{}, ErrOrgNotFound
	}
	decision := decideAgentContextRuntimeFeature(state)
	return Entitlement{OrgID: orgID, AgentContextRuntime: decision.Allowed}, nil
}

type featureStateQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// loadFeatureState reads the complete policy input for orgID's
// agent_context_runtime decision in one statement, and separately proves the
// organization exists (found == false if the WHERE organizations.id = $1
// join root matches no row) -- mirroring get_org_entitlements_from_db's own
// two facts (Organization existence via session.get, then feature decision
// via evaluate_org_features_async), read together here for one round trip
// rather than two, since both come from the same MVCC snapshot either way.
//
// organizations is the FROM root (not feature_flags, as
// providersync.loadCanonicalIncidentFeatureState uses) specifically so a
// registered-feature row is not a precondition for detecting org existence:
// providersync's equivalent query never needs to answer "does this org
// exist" on its own, only "canonical_incident_ingestion is decided". This
// package's route does need org-not-found as an independent, always-checked
// fact (Python's session.get happens unconditionally, before entitlement
// evaluation).
func loadFeatureState(
	ctx context.Context, queryer featureStateQueryer, orgID string, evaluatedAt time.Time,
) (featureState, bool, error) {
	state := featureState{MinTier: "community", OrgTier: "community", EvaluatedAt: evaluatedAt}
	var featureMinTier, licenseTier *string
	var featureEnabled, overrideEnabled *bool
	var overrideExpiresAt *time.Time
	var orgTier string
	var encodedOverrides []byte
	err := queryer.QueryRow(ctx, `
SELECT organization.tier,
       feature.min_tier, feature.is_enabled,
       org_override.is_enabled, org_override.expires_at,
       license.tier, license.features_override
FROM organizations AS organization
LEFT JOIN feature_flags AS feature ON feature.key = $2
LEFT JOIN org_feature_overrides AS org_override
  ON org_override.org_id = organization.id AND org_override.feature_id = feature.id
LEFT JOIN org_licenses AS license ON license.org_id = organization.id
WHERE organization.id = $1`, orgID, AgentContextRuntimeFeatureKey).Scan(
		&orgTier,
		&featureMinTier, &featureEnabled,
		&overrideEnabled, &overrideExpiresAt,
		&licenseTier, &encodedOverrides,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	state.OrgTier = orgTier
	if licenseTier != nil {
		// An org_licenses row exists: its tier takes priority over
		// organizations.tier (gating.py _resolved_org_tier), and its
		// features_override JSON is the license-override source. Malformed
		// JSON here is a genuine read failure (matching Python's own
		// isinstance(...) guard silently treating a non-dict as "no
		// overrides" -- json.Unmarshal failing outright is stricter, so it
		// is surfaced as ErrUnavailable rather than silently ignored).
		state.OrgTier = *licenseTier
		if len(encodedOverrides) != 0 {
			var overrides map[string]any
			if err := json.Unmarshal(encodedOverrides, &overrides); err != nil {
				return state, false, fmt.Errorf("decode org_licenses.features_override: %w", err)
			}
			if raw, ok := overrides[AgentContextRuntimeFeatureKey]; ok {
				value := pythonJSONTruth(raw)
				state.LicenseOverride = &value
			}
		}
	}
	if featureMinTier != nil {
		state.Registered = true
		state.MinTier = *featureMinTier
		state.GloballyEnabled = featureEnabled != nil && *featureEnabled
	}
	if overrideEnabled != nil {
		state.OrgOverride = &featureOverride{Enabled: *overrideEnabled, ExpiresAt: overrideExpiresAt}
	}
	return state, true, nil
}
