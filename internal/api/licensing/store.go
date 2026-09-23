package licensing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrUnavailable means the policy state could not be READ (query error,
// pool exhaustion, scan failure, malformed features_override JSON) --
// distinct from a correctly-evaluated CLOSED decision, which is not an
// error.
var ErrUnavailable = errors.New("licensing: feature decision could not be evaluated")

// Store decides one org's entitlement for one feature key. It does not
// resolve whether the org itself exists -- Python's own decision engine
// (get_org_entitlements_from_db) does not check that either, it silently
// falls back to the COMMUNITY default tier when no organizations/org_licenses
// row matches. A caller that must distinguish "org does not exist" (e.g. a
// 404) checks that separately, against organizations, before or after
// calling Decide.
type Store interface {
	Decide(ctx context.Context, orgID, featureKey string) (Decision, error)
}

// PostgresStore is the production Store.
type PostgresStore struct {
	Pool *pgxpool.Pool
	// Now is injectable so a test can drive the override-expiry clock. Nil
	// means time.Now.
	Now func() time.Time
}

var _ Store = PostgresStore{}

func (store PostgresStore) Decide(ctx context.Context, orgID, featureKey string) (Decision, error) {
	if store.Pool == nil {
		return Decision{}, ErrUnavailable
	}
	evaluatedAt := time.Now().UTC()
	if store.Now != nil {
		evaluatedAt = store.Now().UTC()
	}
	state, err := LoadState(ctx, store.Pool, orgID, featureKey, evaluatedAt)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return Decide(featureKey, state), nil
}

// Queryer is the minimal *pgxpool.Pool surface LoadState needs -- exported
// so a caller with its own hand-rolled query interface (e.g. a white-box
// unit test injecting a fake row, or a caller open-coding a transaction)
// can call LoadState directly without a real pool. *pgxpool.Pool satisfies
// this interface already; no adapter is needed for the production path.
type Queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// LoadState reads the complete policy input for (orgID, featureKey) in one
// statement. FROM feature_flags (not organizations) is deliberate: this
// function never needs to answer "does the org exist" -- see Store's doc
// comment -- only "what does the engine decide", which is well-defined
// (COMMUNITY tier, no overrides) even for an org with no organizations or
// org_licenses row at all. Exported so a caller already holding its own
// Queryer-shaped connection (or a fake one, in a test) can load a State
// without going through PostgresStore/a real pool.
func LoadState(
	ctx context.Context, queryer Queryer, orgID, featureKey string, evaluatedAt time.Time,
) (State, error) {
	state := State{MinTier: "community", OrgTier: "community", EvaluatedAt: evaluatedAt}
	var featureMinTier, licenseTier *string
	var featureEnabled, overrideEnabled *bool
	var overrideExpiresAt *time.Time
	var orgTier *string
	var encodedOverrides, encodedOrgOverrideConfig []byte
	err := queryer.QueryRow(ctx, `
SELECT feature.min_tier, feature.is_enabled,
       org_override.is_enabled, org_override.expires_at, org_override.config,
       license.tier, license.features_override, organization.tier
FROM feature_flags AS feature
LEFT JOIN org_feature_overrides AS org_override
  ON org_override.org_id = $2::uuid AND org_override.feature_id = feature.id
LEFT JOIN org_licenses AS license ON license.org_id = $2::uuid
LEFT JOIN organizations AS organization ON organization.id = $2::uuid
WHERE feature.key = $1`, featureKey, orgID).Scan(
		&featureMinTier, &featureEnabled,
		&overrideEnabled, &overrideExpiresAt, &encodedOrgOverrideConfig,
		&licenseTier, &encodedOverrides, &orgTier,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// The feature key itself is not registered at all: every other
		// column stays at State's zero/default values, and Decide's own
		// !Registered check closes the decision -- matching Python's
		// features_by_key.get(feature_key) returning None.
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if featureMinTier != nil {
		state.Registered = true
		state.MinTier = *featureMinTier
		state.GloballyEnabled = featureEnabled != nil && *featureEnabled
	}
	if overrideEnabled != nil {
		state.OrgOverride = &Override{Enabled: *overrideEnabled, ExpiresAt: overrideExpiresAt}
		if len(encodedOrgOverrideConfig) != 0 {
			// gating.py's _override_snapshot applies the identical
			// isinstance(dict) guard to override.config that it applies to
			// features_override below -- non-object JSON here is a silent
			// nil Config, never a read failure, on both planes.
			decoded, err := decodeJSONTolerantly(encodedOrgOverrideConfig)
			if err != nil {
				return state, fmt.Errorf("decode org_feature_overrides.config: %w", err)
			}
			if config, ok := decoded.(map[string]any); ok {
				state.OrgOverride.Config = &config
			}
		}
	}
	if licenseTier != nil {
		// An org_licenses row exists: its tier takes priority over
		// organizations.tier (gating.py _resolved_org_tier), and its
		// features_override JSON is the license-override source.
		state.OrgTier = *licenseTier
		if len(encodedOverrides) != 0 {
			decoded, err := decodeJSONTolerantly(encodedOverrides)
			if err != nil {
				return state, fmt.Errorf("decode org_licenses.features_override: %w", err)
			}
			// gating.py's own guard reads raw_license_overrides as the
			// override source only `if isinstance(raw_license_overrides,
			// dict)`, else treats it as {} -- valid JSON that decodes to
			// something other than an object (an array, string, number,
			// bool, or null) is a silent NO-OVERRIDE state on both planes,
			// never a read failure. Scanning straight into map[string]any
			// instead of checking the decoded shape first made a
			// features_override value of exactly `[]` return an error here
			// and deny an active org grant with a false 503 -- confirmed
			// live against real Postgres. Only genuinely malformed JSON
			// bytes (the decode error below) are ErrUnavailable.
			if overrides, ok := decoded.(map[string]any); ok {
				if raw, ok := overrides[featureKey]; ok {
					value := jsonTruth(raw)
					state.LicenseOverride = &value
				}
			}
		}
	} else if orgTier != nil {
		state.OrgTier = *orgTier
	}
	return state, nil
}

// decodeJSONTolerantly decodes one JSON column value the same way Python's
// json.loads does: every syntactically valid JSON number decodes
// successfully, including a magnitude that overflows float64 (Python's C
// double parser returns +/-Inf on overflow; it never raises). encoding/json's
// default number handling refuses to unmarshal such a value into
// interface{} at all -- decodeState.literalStore's strconv.ParseFloat call
// treats ErrRange as a hard decode error -- which turned a features_override
// value containing a single out-of-range number (confirmed live:
// `{"agent_context_runtime":1e10000}`) into ErrUnavailable for the WHOLE
// column, denying an otherwise-valid entitlement. Decoding with UseNumber
// defers number parsing to jsonTruth, which tolerates the same overflow.
func decodeJSONTolerantly(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}
