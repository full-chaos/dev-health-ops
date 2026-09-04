package llmorgsettings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// byoLLMFeatureKey is licensing/registry.py's STANDARD_FEATURES row seeded
// by alembic 0017 (min_tier "team", is_enabled true).
const byoLLMFeatureKey = "byo_llm"

// byoLLMMinTier is the hard TEAM-tier floor api/services/licensing.py's
// byo_llm_flag_state passes as feature_flag_state's min_tier: a positive
// per-org OR license override must NOT bypass it (a COMMUNITY-tier org's
// admin-granted override is still refused) -- this is checked BEFORE
// evaluate_org_feature_sync's own override precedence even runs, not
// folded into it.
const byoLLMMinTier = "team"

// Feature-flag state strings, matching byo_llm_flag_state's own return
// values verbatim (they are compared as strings by this package's only
// caller, credentials.py's _apply_byo_llm_flag_gate).
const (
	flagStateEnabled      = "enabled"
	flagStateDisabled     = "disabled"
	flagStateUnregistered = "unregistered"
)

// tierIndex mirrors licensing/types.py's TIER_ORDER (community < team <
// enterprise); ok is false for any other/invalid string, matching
// LicenseTier(str(x)) raising ValueError -- callers treat an invalid tier
// as COMMUNITY, same as resolve_org_tier's except-ValueError branch.
func tierIndex(tier string) (int, bool) {
	switch tier {
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

// byoLLMFeatureState is the Postgres row set byoLLMFlagState needs, read in
// one statement (one MVCC snapshot, no row locks -- this is a read-only
// resolver, never a mutation path) mirroring
// licensing/feature_decision_store.py's load_feature_rows_sync narrowed to
// a single feature key. The shape and the one-query-with-LEFT-JOINs
// approach mirror internal/providersync/incident_entitlement.go's
// canonical_incident_ingestion port (a sibling feature-flag gate already
// reviewed and running in production) -- not shared code, because that
// gate has no external min_tier floor and does not need to distinguish
// "feature row absent" from every other closed reason, both of which
// byo_llm_flag_state does.
type byoLLMFeatureState struct {
	registered      bool
	minTier         string
	globallyEnabled bool
	orgTier         string
	overrideEnabled *bool
	overrideExpires *time.Time
	licenseOverride *bool
}

type featureRowQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadByoLLMFeatureState(
	ctx context.Context, queryer featureRowQueryer, orgID uuid.UUID,
) (byoLLMFeatureState, error) {
	state := byoLLMFeatureState{minTier: "community", orgTier: "community"}
	var minTier *string
	var globallyEnabled *bool
	var licenseTier, orgTier *string
	var featuresOverride []byte

	err := queryer.QueryRow(ctx, `
SELECT feature.min_tier, feature.is_enabled,
       org_override.is_enabled, org_override.expires_at,
       license.tier, license.features_override, organization.tier
FROM feature_flags AS feature
LEFT JOIN org_feature_overrides AS org_override
  ON org_override.org_id = $2 AND org_override.feature_id = feature.id
LEFT JOIN org_licenses AS license ON license.org_id = $2
LEFT JOIN organizations AS organization ON organization.id = $2
WHERE feature.key = $1`, byoLLMFeatureKey, orgID).Scan(
		&minTier, &globallyEnabled,
		&state.overrideEnabled, &state.overrideExpires,
		&licenseTier, &featuresOverride, &orgTier,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// No feature_flags row at all -- feature is not registered
		// (pre-migration / minimal DB), matching FEATURE_NOT_REGISTERED.
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("query byo_llm feature state: %w", err)
	}

	state.registered = true
	if minTier != nil {
		state.minTier = *minTier
	}
	if globallyEnabled != nil {
		state.globallyEnabled = *globallyEnabled
	}
	if licenseTier != nil {
		state.orgTier = *licenseTier
		var overrides map[string]json.RawMessage
		if len(featuresOverride) != 0 {
			if err := json.Unmarshal(featuresOverride, &overrides); err != nil {
				// Malformed features_override JSON: _resolved_org_tier's
				// own dict-parse never fails this way in Python (the ORM
				// already validated JSON on write), so there is no
				// documented Python behavior to mirror here -- fail
				// closed rather than guess.
				return state, fmt.Errorf("parse org_licenses.features_override: %w", err)
			}
		}
		if raw, ok := overrides[byoLLMFeatureKey]; ok {
			truthy := jsonTruthy(raw)
			state.licenseOverride = &truthy
		}
	} else if orgTier != nil {
		state.orgTier = *orgTier
	}
	return state, nil
}

// jsonTruthy mirrors bool(value) over an already-decoded JSON scalar/
// container, matching decisions.py's `bool(value)` cast on
// features_override[key] (a raw JSON value, not necessarily a JSON
// boolean -- org_license.features_override is a free-form JSON column).
func jsonTruthy(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	switch trimmed {
	case "", "null", "false", "0", `""`, "[]", "{}":
		return false
	default:
		return true
	}
}

// decideByoLLM ports decide_feature narrowed to byo_llm's own
// classification (byo_llm is in neither EXPLICIT_PURCHASE_FEATURES nor
// ORG_OVERRIDE_ONLY_FEATURES -- licensing/registry.py -- so those two
// branches, and the org-override-expired-forces-closed special case that
// applies only to ORG_OVERRIDE_ONLY_FEATURES, are never reached for this
// key and are not ported).
func decideByoLLM(state byoLLMFeatureState, evaluatedAt time.Time) string {
	minTier, minOK := tierIndex(state.minTier)
	if !minOK {
		return flagStateDisabled // INVALID_FEATURE_STATE
	}
	if !state.registered {
		return flagStateUnregistered // FEATURE_NOT_REGISTERED
	}
	if !state.globallyEnabled {
		return flagStateDisabled // GLOBAL_DISABLED
	}
	orgTier, orgOK := tierIndex(state.orgTier)
	if !orgOK {
		orgTier = 0
	}
	tierAllowed := orgTier >= minTier

	if state.overrideEnabled != nil {
		expired := state.overrideExpires != nil && !state.overrideExpires.After(evaluatedAt)
		if !expired {
			if !*state.overrideEnabled {
				return flagStateDisabled // ORG_OVERRIDE_DISABLED
			}
			return flagStateEnabled // ENABLED_BY_ORG_OVERRIDE
		}
	}

	if state.licenseOverride != nil {
		if !*state.licenseOverride {
			return flagStateDisabled // LICENSE_OVERRIDE_DISABLED
		}
		return flagStateEnabled // ENABLED_BY_LICENSE_OVERRIDE
	}

	if tierAllowed {
		return flagStateEnabled // ENABLED_BY_TIER
	}
	return flagStateDisabled // TIER_REQUIRED
}

// byoLLMFlagState ports api/services/licensing.py's byo_llm_flag_state:
// feature_flag_state(session, org_id, "byo_llm", min_tier=TEAM). The
// TEAM-tier floor is a HARD gate checked first -- a COMMUNITY-tier org is
// "disabled" regardless of any org/license override recorded for it (a
// positive override must not bypass the floor); only once that passes
// does the ordinary evaluate_org_feature_sync precedence
// (override > license override > tier) run.
func byoLLMFlagState(
	ctx context.Context, queryer featureRowQueryer, orgID uuid.UUID, now time.Time,
) (string, error) {
	state, err := loadByoLLMFeatureState(ctx, queryer, orgID)
	if err != nil {
		return "", err
	}
	orgTierFloor, orgFloorOK := tierIndex(state.orgTier)
	if !orgFloorOK {
		orgTierFloor = 0
	}
	minTierFloor, _ := tierIndex(byoLLMMinTier)
	if orgTierFloor < minTierFloor {
		return flagStateDisabled, nil
	}
	return decideByoLLM(state, now), nil
}
