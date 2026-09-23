package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/orgs"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// requireFeature is licensing/gating.py's `@require_feature(feature,
// required_tier="enterprise")` decorator on an async route whose kwargs
// carry `session` and `org_id` (every gated route here does): the org's own
// license decides, in _check_org_feature_async's order.
//
//  1. An org_licenses row wins: its tier's features (tier rank >= the
//     feature's minimum tier) allow, else a truthy features_override[feature]
//     allows, else deny. A tier value outside LicenseTier denies.
//  2. With no org_licenses row, organizations.tier decides the same way; a
//     missing or empty tier, or one outside LicenseTier, denies.
//
// A failure of the check itself denies too (Python swallows every exception
// into False); it is logged loudly here, never silently.
//
// Named limit: the Python process-wide LicenseManager (a signed JWT in
// LICENSE_KEY, self-hosted installs) is not consulted. Without one it is
// the community tier, which holds none of the three features gated here, so
// on an install with no process license the two planes agree; an install
// with one keeps the Python api until it is ported.
func (h *handlers) requireFeature(ctx context.Context, w http.ResponseWriter, feature, orgID string) bool {
	allowed, err := h.orgHasFeature(ctx, feature, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: feature check failed; denying", "feature", feature, "org_id", orgID, "error", err)
	}
	if allowed {
		return true
	}
	detail := pyjson.NewObject()
	detail.Set("error", "feature_not_licensed")
	detail.Set("feature", feature)
	detail.Set("required_tier", "enterprise")
	detail.Set("current_tier", "community")
	policy.WriteDetail(w, http.StatusPaymentRequired, detail, nil)
	return false
}

// featureMinTier is the STANDARD_FEATURES minimum tier of each feature this
// package gates; none is an explicit-purchase feature.
var featureMinTier = map[string]string{
	"audit_log":        "enterprise",
	"ip_allowlist":     "enterprise",
	"custom_retention": "enterprise",
}

func (h *handlers) orgHasFeature(ctx context.Context, feature, orgID string) (bool, error) {
	minTier, known := featureMinTier[feature]
	if !known {
		return false, errors.New("feature has no registered minimum tier")
	}
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return false, nil
	}
	want, _ := licensing.TierRank(minTier)
	tierAllows := func(tier string) bool {
		have, ok := licensing.TierRank(tier)
		return ok && have >= want
	}
	license, err := h.store.orgLicenseForGate(ctx, org)
	if err != nil {
		return false, err
	}
	if license != nil {
		if _, ok := licensing.TierRank(license.tier); !ok {
			return false, nil
		}
		if tierAllows(license.tier) {
			return true, nil
		}
		return featureOverrideTruthy(license.featuresOverride, feature), nil
	}
	tier, err := h.store.orgTierForGate(ctx, org)
	if err != nil {
		return false, err
	}
	if tier == "" {
		return false, nil
	}
	return tierAllows(tier), nil
}

// featureOverrideTruthy is `org_license.features_override.get(feature)`
// truthiness; anything that is not a JSON object has no .get and denies.
func featureOverrideTruthy(raw []byte, feature string) bool {
	if len(raw) == 0 {
		return false
	}
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return false
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return false
	}
	item, present := object.Get(feature)
	return present && orgs.Truthy(item)
}

type gateLicense struct {
	tier             string
	featuresOverride []byte
}

// orgLicenseForGate reads the org's org_licenses row, nil when there is none.
func (s pgStore) orgLicenseForGate(ctx context.Context, orgID uuid.UUID) (*gateLicense, error) {
	var license gateLicense
	err := s.Pool.QueryRow(ctx, `SELECT tier, features_override FROM org_licenses WHERE org_id = $1`, orgID).
		Scan(&license.tier, &license.featuresOverride)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &license, nil
}

// orgTierForGate reads organizations.tier, "" when the org has none.
func (s pgStore) orgTierForGate(ctx context.Context, orgID uuid.UUID) (string, error) {
	var tier *string
	err := s.Pool.QueryRow(ctx, `SELECT tier FROM organizations WHERE id = $1`, orgID).Scan(&tier)
	if errors.Is(err, pgx.ErrNoRows) || tier == nil {
		return "", nil
	}
	return *tier, err
}
