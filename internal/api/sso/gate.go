package sso

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
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
// The sign-in routes call this with the provider row's org; the provider
// CRUD routes, while auth-service owns the providers, use providerGate.
func (h handlers) requireEntitlement(w http.ResponseWriter, r *http.Request, orgID string) bool {
	if h.entitled(r, orgID) {
		return true
	}
	policy.WriteDetail(w, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail(ssoFeature, &ssoRequiredTier), nil)
	return false
}

// entitled is the process tier or the org's own entitlement (OrgEntitled); a failed org read denies and is logged.
func (h handlers) entitled(r *http.Request, orgID string) bool {
	if licensing.ProcessHasFeature(ssoFeature) {
		return true
	}
	allowed, err := h.OrgEntitled(r.Context(), orgID)
	if err != nil {
		h.Logger.ErrorContext(r.Context(), "api sso: org entitlement check failed; denying",
			"feature", ssoFeature, "org_id", orgID, "path", r.Pattern, "error", err)
	}
	return allowed
}

// providerGate is the 422 and the 402 of a provider CRUD route while auth-service owns the providers (product rule C1,
// R459): the query and body validation first, then the process tier or the caller organization's enterprise entitlement.
// The organization is the token's, the one providerScope serves, never the body's or the X-Org-Id's; a caller without one is
// refused. The 402 is gate's, byte for byte (current_tier is the process tier, as every sibling gate answers it).
func (h handlers) providerGate(validate validator) func(http.ResponseWriter, *http.Request) bool {
	return func(w http.ResponseWriter, r *http.Request) bool {
		var errs pybody.Errors
		validate(r, &errs)
		if len(errs) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
			return false
		}
		orgID := ""
		if caller := policy.UserFrom(r.Context()); caller != nil {
			orgID = caller.OrgID
		}
		if h.entitled(r, orgID) {
			return true
		}
		h.Logger.WarnContext(r.Context(), "License audit: feature_access_denied",
			"feature", ssoFeature, "current_tier", licensing.ProcessTier(), "path", r.Pattern, "org_id", orgID)
		policy.WriteDetail(w, http.StatusPaymentRequired, licensing.FeatureNotLicensedDetail(ssoFeature, &ssoRequiredTier), nil)
		return false
	}
}
