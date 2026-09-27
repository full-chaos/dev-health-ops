package sso

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// requireEntitlement is D2725's real gate for the OIDC authorize/callback
// routes (CHAOS-6658): the org's own entitlement for "sso_saml" at
// "enterprise" OR the process tier -- either satisfies it -- unlike
// gated() (sso.go), which only ever consults the process tier because
// Python's decorator never reaches _check_org_feature_async for this router
// (the 17 handlers take no session/org_id kwarg, so the per-org branch of
// require_feature is dead code there; D2725 records that as a deliberate
// delta, not a parity target). A route ported for real can run the org
// check correctly -- it has an org id, whether from an authenticated
// caller or, on these two public per-provider routes, the provider row
// itself -- so it does.
//
// A failure of the check itself denies and is logged loudly, never
// silently, matching admin/featuregate.go's own rule.
//
// Only initiateOIDCAuth and oidcCallback call this; the other 15 SSO
// routes are unchanged in this PR and keep gated()'s process-tier-only
// shim pending CHAOS-6659/6986.
func (h handlers) requireEntitlement(w http.ResponseWriter, r *http.Request, orgID string) bool {
	if licensing.ProcessHasFeature(ssoFeature) {
		return true
	}
	allowed, err := licensing.OrgHasFeature(r.Context(), h.Pool, orgID, ssoFeature)
	if err != nil {
		h.Logger.ErrorContext(r.Context(), "api sso: org entitlement check failed; denying",
			"feature", ssoFeature, "org_id", orgID, "path", r.URL.Path, "error", err)
	}
	if allowed {
		return true
	}
	policy.WriteDetail(w, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail(ssoFeature, &ssoRequiredTier), nil)
	return false
}
