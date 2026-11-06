package server

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// observeRESTShadow tells the process's 5.12 shadow what the legacy check decided for this request. It runs after the
// decision and before the answer, returns at once, and never changes the answer. No gate, or a gate with no shadow
// (off, enforce, a bad knob), is a no-op, and so is a route whose 5.17 cell is on: the PDP answer is the answer.
func observeRESTShadow(r *http.Request, bearer, orgID, caller string, delegated, legacyAllowed bool) {
	gate := pdp.FromContext(r.Context())
	shadow := gate.Shadow()
	if shadow == nil {
		return
	}
	class := principal.AccessClassOf(bearer)
	reason := shadowSkipReason(class)
	if reason != policy.ShadowReasonNone {
		slog.DebugContext(r.Context(), "query-api pdp shadow: the credential class never asks the PDP", slog.String("reason", reason))
	}
	pattern := r.Pattern
	if !strings.Contains(pattern, " ") {
		pattern = routeProfile(r)
	}
	if gate.Enforced(pattern) {
		return
	}
	shadow.Observe(r.Context(), policy.ShadowObservation{Pattern: pattern, Bearer: bearer, OrgID: orgID, Caller: caller,
		UserAccessToken: class == principal.AccessClassUser, Impersonating: delegated, SkipReason: reason, LegacyAllowed: legacyAllowed})
}

func shadowSkipReason(class principal.AccessClass) string {
	switch class {
	case principal.AccessClassEdge:
		return policy.ShadowReasonHS256
	case principal.AccessClassOther:
		return policy.ShadowReasonEnvelope
	}
	return policy.ShadowReasonNone
}
