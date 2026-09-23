package customerpush

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	customerPushFeature = "customer_push_ingest"
	requiredTier        = "team"
)

// requireAccess is _require_customer_push_access: the org's
// customer_push_ingest state must be "enabled". Below the TEAM tier is 402
// feature_not_licensed with the org's tier; an unregistered feature is 402
// with tier "unknown"; any other state is 403 feature_not_enabled. It
// answers the response itself and returns false when access is refused.
func (h *handlers) requireAccess(w http.ResponseWriter, r *http.Request, orgID string) bool {
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		policy.WriteDetail(w, http.StatusNotFound, "Organization not found", nil)
		return false
	}
	state, tier, err := licensing.FeatureFlagState(r.Context(), h.pool, org, customerPushFeature, requiredTier, h.now())
	if err != nil {
		h.internal(w, r, "resolve customer_push_ingest access", err)
		return false
	}
	if state == licensing.StateEnabled {
		return true
	}
	have, _ := licensing.TierRank(tier)
	want, _ := licensing.TierRank(requiredTier)
	detail := pyjson.NewObject()
	switch {
	case have < want:
		detail.Set("error", "feature_not_licensed")
		detail.Set("feature", customerPushFeature)
		detail.Set("required_tier", requiredTier)
		detail.Set("current_tier", tier)
		policy.WriteDetail(w, http.StatusPaymentRequired, detail, nil)
	case state == licensing.StateUnregistered:
		detail.Set("error", "feature_not_licensed")
		detail.Set("feature", customerPushFeature)
		detail.Set("required_tier", requiredTier)
		detail.Set("current_tier", "unknown")
		policy.WriteDetail(w, http.StatusPaymentRequired, detail, nil)
	default:
		detail.Set("error", "feature_not_enabled")
		detail.Set("feature", customerPushFeature)
		detail.Set("message", "Customer push ingest is not enabled for this organization")
		policy.WriteDetail(w, http.StatusForbidden, detail, nil)
	}
	return false
}
