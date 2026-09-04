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

// byoLLMFeatureState is the state byoLLMFlagState needs, mirroring
// licensing/feature_decision_store.py's load_feature_rows_sync narrowed to
// a single feature key.
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

// loadByoLLMFeatureState reads org tier and the byo_llm feature row as TWO
// INDEPENDENT queries, deliberately NOT one LEFT-JOIN chain anchored on
// feature_flags -- mirroring load_feature_rows_sync's own shape, which
// issues separate session.scalar(s) calls for FeatureFlag/
// OrgFeatureOverride/OrgLicense/Organization.tier, none of them joined to
// or gated by another's existence. A single-query version was tried first
// and had a real bug this package's own integration test caught: when
// byo_llm's feature_flags row is absent (pre-migration / not yet seeded),
// anchoring the FROM clause on feature_flags means the WHOLE row -- org
// tier included -- comes back as pgx.ErrNoRows, silently defaulting an
// ENTERPRISE org's tier to "community" and failing byoLLMFlagState's
// TEAM-tier floor for a reason that has nothing to do with the org's real
// tier. Python never has this coupling (its floor check's org-tier read
// and its feature-registration check are two separately dispatched
// queries), so neither should this port.
func loadByoLLMFeatureState(
	ctx context.Context, queryer featureRowQueryer, orgID uuid.UUID,
) (byoLLMFeatureState, error) {
	orgTier, licenseOverrides, err := resolveOrgTierAndLicenseOverrides(ctx, queryer, orgID)
	if err != nil {
		return byoLLMFeatureState{}, err
	}
	state := byoLLMFeatureState{minTier: "community", orgTier: orgTier}

	var minTier *string
	var globallyEnabled *bool
	err = queryer.QueryRow(ctx, `
SELECT feature.min_tier, feature.is_enabled,
       org_override.is_enabled, org_override.expires_at
FROM feature_flags AS feature
LEFT JOIN org_feature_overrides AS org_override
  ON org_override.org_id = $2 AND org_override.feature_id = feature.id
WHERE feature.key = $1`, byoLLMFeatureKey, orgID).Scan(
		&minTier, &globallyEnabled, &state.overrideEnabled, &state.overrideExpires,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// No feature_flags row at all -- feature is not registered
		// (pre-migration / minimal DB), matching FEATURE_NOT_REGISTERED.
		// state.orgTier is still the REAL org tier resolved above, not a
		// coupled default -- the bug this split fixes.
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
	if raw, ok := licenseOverrides[byoLLMFeatureKey]; ok {
		truthy := jsonTruthy(raw)
		state.licenseOverride = &truthy
	}
	return state, nil
}

// resolveOrgTierAndLicenseOverrides ports resolve_org_tier's own
// precedence (OrgLicense.tier wins when a row exists, else
// Organization.tier, else "community") as two independent, unconditional
// reads -- neither gated on whether byo_llm's feature_flags row exists.
// The org_licenses row (when present) also carries features_override,
// read here rather than a third query, matching how Python's
// load_feature_rows_sync fetches the whole OrgLicense row once and reads
// both fields off it.
func resolveOrgTierAndLicenseOverrides(
	ctx context.Context, queryer featureRowQueryer, orgID uuid.UUID,
) (tier string, licenseOverrides map[string]json.RawMessage, err error) {
	var licenseTier *string
	var featuresOverride []byte
	err = queryer.QueryRow(ctx,
		`SELECT tier, features_override FROM org_licenses WHERE org_id = $1`, orgID,
	).Scan(&licenseTier, &featuresOverride)
	switch {
	case err == nil:
		if len(featuresOverride) != 0 {
			if jerr := json.Unmarshal(featuresOverride, &licenseOverrides); jerr != nil {
				// Malformed features_override JSON: Python's own dict-parse
				// never fails this way (the ORM already validated JSON on
				// write), so there is no documented Python behavior to
				// mirror here -- fail closed rather than guess.
				return "", nil, fmt.Errorf("parse org_licenses.features_override: %w", jerr)
			}
		}
		if licenseTier != nil {
			return *licenseTier, licenseOverrides, nil
		}
		return "community", licenseOverrides, nil
	case errors.Is(err, pgx.ErrNoRows):
		// No org_licenses row -- fall through to organizations.tier.
	default:
		return "", nil, fmt.Errorf("query org_licenses: %w", err)
	}

	var orgTier string
	err = queryer.QueryRow(ctx,
		`SELECT tier FROM organizations WHERE id = $1`, orgID,
	).Scan(&orgTier)
	switch {
	case err == nil:
		return orgTier, nil, nil
	case errors.Is(err, pgx.ErrNoRows):
		return "community", nil, nil
	default:
		return "", nil, fmt.Errorf("query organizations: %w", err)
	}
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
