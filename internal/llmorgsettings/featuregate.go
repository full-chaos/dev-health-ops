package llmorgsettings

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

// loadByoLLMFeatureState reads the byo_llm feature row and org
// tier/license-override as TWO INDEPENDENT queries, deliberately NOT one
// LEFT-JOIN chain anchored on feature_flags -- mirroring
// load_feature_rows_sync's own shape, which issues separate
// session.scalar(s) calls for FeatureFlag/OrgFeatureOverride/OrgLicense/
// Organization.tier, none of them joined to or gated by another's
// existence. A single-query version was tried first and had a real bug
// this package's own integration test caught: when byo_llm's feature_flags
// row is absent (pre-migration / not yet seeded), anchoring the FROM
// clause on feature_flags means the WHOLE row -- org tier included --
// comes back as pgx.ErrNoRows, silently defaulting an ENTERPRISE org's
// tier to "community" and failing byoLLMFlagState's TEAM-tier floor for a
// reason that has nothing to do with the org's real tier. Python never has
// this coupling (its floor check's org-tier read and its
// feature-registration check are two separately dispatched queries), so
// neither should this port.
//
// The feature/override query runs FIRST, tier/license SECOND -- codex
// round 3, P1: load_feature_rows_sync (feature_decision_store.py:39-65)
// itself queries FeatureFlag/OrgFeatureOverride BEFORE OrgLicense/
// Organization.tier, in that fixed order, within one function call. An
// earlier version of this port read tier/license first and features
// second -- the REVERSE order -- which admits a torn-read interleaving
// Python's own order cannot produce: a concurrent transaction that
// downgrades the org's tier AND deletes a disabling override, committing
// between these two reads, lands as OLD tier + NEW (absent) override in
// the old Go order (wrongly enabling BYO), a combination Python can never
// observe because it always reads the override before the tier. Matching
// Python's own read order constrains this port's torn-read window to the
// same reachable-state space as the oracle's, rather than a wider one.
func loadByoLLMFeatureState(
	ctx context.Context, queryer featureRowQueryer, orgID uuid.UUID,
) (byoLLMFeatureState, error) {
	state := byoLLMFeatureState{minTier: "community"}

	var minTier *string
	var globallyEnabled *bool
	rowErr := queryer.QueryRow(ctx, `
SELECT feature.min_tier, feature.is_enabled,
       org_override.is_enabled, org_override.expires_at
FROM feature_flags AS feature
LEFT JOIN org_feature_overrides AS org_override
  ON org_override.org_id = $2 AND org_override.feature_id = feature.id
WHERE feature.key = $1`, byoLLMFeatureKey, orgID).Scan(
		&minTier, &globallyEnabled, &state.overrideEnabled, &state.overrideExpires,
	)
	switch {
	case rowErr == nil:
		state.registered = true
	case errors.Is(rowErr, pgx.ErrNoRows):
		// No feature_flags row at all -- feature is not registered
		// (pre-migration / minimal DB), matching FEATURE_NOT_REGISTERED.
		// Still fall through to the tier/license read below: Python's
		// load_feature_rows_sync always reads OrgLicense/Organization.tier
		// too, regardless of whether the FeatureFlag row was found.
	default:
		return byoLLMFeatureState{}, fmt.Errorf("query byo_llm feature state: %w", rowErr)
	}

	orgTier, licenseOverrides, err := resolveOrgTierAndLicenseOverrides(ctx, queryer, orgID)
	if err != nil {
		return byoLLMFeatureState{}, err
	}
	state.orgTier = orgTier

	if !state.registered {
		return state, nil
	}
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
				var typeErr *json.UnmarshalTypeError
				if errors.As(jerr, &typeErr) {
					// features_override is valid JSON but its TOP-LEVEL
					// value is not a JSON object (array/string/number/
					// bool/null) -- codex round 3, P2: features_override is
					// a free-form JSON column (models/licensing.py:347),
					// and _decisions_from_rows only treats it as a
					// license-override map when isinstance(raw, dict)
					// (feature_decisions.py:79); any other JSON type falls
					// through to {} there, it is NOT a lookup failure.
					// json.Unmarshal into map[string]json.RawMessage can
					// only raise UnmarshalTypeError for a non-object
					// TOP-LEVEL value here -- a json.RawMessage map VALUE
					// accepts any valid JSON verbatim, so no nested value
					// can ever trigger this error -- so this branch
					// unambiguously means "not an object", matching
					// isinstance(..., dict) is False. licenseOverrides
					// stays nil (no license override present), not an
					// error.
					licenseOverrides = nil
				} else {
					// Genuinely malformed JSON syntax: Python's own
					// dict-parse never fails this way (the ORM already
					// validated JSON on write), so there is no documented
					// Python behavior to mirror here -- fail closed rather
					// than guess.
					return "", nil, fmt.Errorf("parse org_licenses.features_override: %w", jerr)
				}
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
// Decodes the value and applies Python's real truthiness rules
// (null/false/0/""/[]/{} falsy, everything else truthy) rather than
// string-matching the raw JSON text -- a text-match on a fixed literal
// set (codex round 1, P2) missed every OTHER falsy spelling of the same
// value (0.0, -0, 0e0, whitespace padding), silently flipping the
// license-override decision for any of them.
func jsonTruthy(raw json.RawMessage) bool {
	// UseNumber(): decode JSON numbers as json.Number (the raw literal
	// text), not float64 -- codex round 2, P2: a plain
	// json.Unmarshal(raw, &any) decodes numbers via strconv.ParseFloat,
	// which returns AN ERROR for a syntactically valid but
	// float64-overflowing literal like 1e400 (features_override is a
	// free-form JSON column with no magnitude constraint -- CHAOS-4989's
	// own schema comment). The old code's error branch then failed
	// closed (false), while Python's json.loads("1e400") succeeds
	// (float("1e400") == inf, no exception) and bool(inf) is True --
	// silently flipping a TRUTHY license-override value to falsy. This
	// decoder never errors on magnitude; only a genuine parse failure
	// reaches the error branch below.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		// Malformed JSON for this one override value: fail closed
		// (falsy) -- matches the "no license override present" outcome
		// rather than guessing that unparseable content should enable
		// BYO LLM for the org.
		return false
	}
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case json.Number:
		return numberTruthy(v)
	case string:
		return v != ""
	case []any:
		return len(v) != 0
	case map[string]any:
		return len(v) != 0
	default:
		return true
	}
}

// numberTruthy mirrors Python's bool(float(literal)) for a JSON number
// literal -- codex round 2's own executed repro
// (python3 -c 'json.loads("1e400")' -> inf, bool(inf) is True). A
// magnitude too large for float64 is NOT a parse failure the way a
// malformed literal is: strconv.ParseFloat returns the correctly-signed
// +-Inf VALUE alongside a range error for exactly this case (Go's
// encoding/json's own number grammar already guarantees n is
// syntactically valid JSON -- UseNumber() only defers the numeric
// conversion, it does not relax the grammar), so only a genuine,
// non-range parse error (unreachable in practice, given that grammar
// guarantee, but not assumed away) fails closed; an overflow uses the
// returned +-Inf, which is nonzero and therefore truthy, exactly
// matching Python.
func numberTruthy(n json.Number) bool {
	f, err := strconv.ParseFloat(n.String(), 64)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) && errors.Is(numErr.Err, strconv.ErrRange) {
			return f != 0
		}
		return false
	}
	return f != 0
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
	// Table-existence check FIRST, matching feature_flag_state's own
	// `if not sa.inspect(...).has_table("feature_flags"): return
	// "unregistered"` -- a pre-migration/minimal DB short-circuits to
	// "unregistered" WITHOUT ever running the tier floor check, not just
	// without running the main decision (codex round 1, P2: an earlier
	// version only special-cased a missing ROW, pgx.ErrNoRows, inside the
	// feature-row query -- a missing TABLE surfaces as a different
	// Postgres error there and was wrongly treated as a fatal lookup
	// failure instead of Python's documented backward-compat path).
	registered, err := byoLLMFeatureFlagsTableExists(ctx, queryer)
	if err != nil {
		return "", err
	}
	if !registered {
		return flagStateUnregistered, nil
	}

	// Floor-check tier read and the main-decision's own tier read
	// (inside loadByoLLMFeatureState) are DELIBERATELY two independent,
	// separately-dispatched queries, not one shared read reused for both
	// -- matching Python's actual double-read shape exactly:
	// feature_flag_state's own resolve_org_tier call for the floor gate,
	// then evaluate_org_feature_sync's SEPARATE load_feature_rows_sync
	// read of Organization.tier/OrgLicense for the real decision. codex
	// round 1's P1: the FIRST version of this function reused ONE shared
	// tier read for both purposes, which is a real -- if narrow --
	// torn-read regression neither plane has when each read is genuinely
	// independent: an org whose tier is downgraded AND whose blocking
	// override is deleted in the same committed transaction, landing
	// between these two reads, must still see the FRESH (post-downgrade)
	// tier at the main-decision point, not the stale floor-check value.
	// Reading twice reproduces Python's own window instead of collapsing
	// it into a narrower one that then behaves differently.
	floorTier, _, err := resolveOrgTierAndLicenseOverrides(ctx, queryer, orgID)
	if err != nil {
		return "", err
	}
	orgTierFloor, orgFloorOK := tierIndex(floorTier)
	if !orgFloorOK {
		orgTierFloor = 0
	}
	minTierFloor, _ := tierIndex(byoLLMMinTier)
	if orgTierFloor < minTierFloor {
		return flagStateDisabled, nil
	}

	state, err := loadByoLLMFeatureState(ctx, queryer, orgID)
	if err != nil {
		return "", err
	}
	return decideByoLLM(state, now), nil
}

// byoLLMFeatureFlagsTableExists ports feature_flag_state's own
// `sa.inspect(session.get_bind()).has_table("feature_flags")` check --
// `to_regclass` is Postgres's own "does this relation exist, in the
// current search_path" primitive (returns NULL, not an error, when it
// does not), so this never itself throws the undefined-table error the
// row-lookup query below would.
func byoLLMFeatureFlagsTableExists(ctx context.Context, queryer featureRowQueryer) (bool, error) {
	var exists bool
	err := queryer.QueryRow(ctx, `SELECT to_regclass('feature_flags') IS NOT NULL`).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check feature_flags table existence: %w", err)
	}
	return exists, nil
}
