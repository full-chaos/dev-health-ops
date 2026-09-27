package admin

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// requireFeature is licensing/gating.py's `@require_feature(feature,
// required_tier="enterprise")` decorator on an async route whose kwargs
// carry `session` and `org_id` (every gated route here does), in its order:
// the process license first (has_feature: the self-hosted install license,
// licensing.ProcessHasFeature, CHAOS-6663), and only when it does not grant
// the feature, the org's own license (_check_org_feature_async):
//
//  1. An org_licenses row wins: its tier's features (tier rank >= the
//     feature's minimum tier) allow, else a truthy features_override[feature]
//     allows, else deny. A tier value outside LicenseTier denies.
//  2. With no org_licenses row, organizations.tier decides the same way; a
//     missing or empty tier, or one outside LicenseTier, denies.
//
// The two are separate gates either of which allows (R459: the install
// license is not the org entitlement); neither is derived from the other.
// A failure of the org check itself denies too (Python swallows every
// exception into False); it is logged loudly here, never silently.
func (h *handlers) requireFeature(ctx context.Context, w http.ResponseWriter, feature, orgID string) bool {
	if licensing.ProcessHasFeature(feature) {
		return true
	}
	allowed, err := h.orgHasFeature(ctx, feature, orgID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin: feature check failed; denying", "feature", feature, "org_id", orgID, "error", err)
	}
	if allowed {
		return true
	}
	writeFeatureNotLicensed(w, feature)
	return false
}

// writeFeatureNotLicensed is the decorator's 402 body
// (@require_feature(feature, required_tier="enterprise")).
func writeFeatureNotLicensed(w http.ResponseWriter, feature string) {
	tier := "enterprise"
	policy.WriteDetail(w, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail(feature, &tier), nil)
}

// orgHasFeature is _check_org_feature_async for this package's gated
// routes: the shared licensing.OrgHasFeature over the api role's pool.
func (h *handlers) orgHasFeature(ctx context.Context, feature, orgID string) (bool, error) {
	return licensing.OrgHasFeature(ctx, h.store.Pool, orgID, feature)
}
