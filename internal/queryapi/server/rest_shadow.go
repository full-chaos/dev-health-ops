package server

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The closed reasons a credential class never asks the PDP: the HS256 edge token and the effective-principal
// envelope stay on today's path until they retire. The shadow counts them skipped; the reason is a debug field, not a
// label, so the 5.12 counters keep their label sets.
const (
	shadowSkipHS256    = "hs256"
	shadowSkipEnvelope = "envelope"
)

// observeRESTShadow tells the process's 5.12 shadow what the legacy check decided for this request. It runs after the
// decision and before the answer, returns at once, and never changes the answer. No gate, or a gate with no shadow
// (off, enforce, a bad knob), is a no-op.
func observeRESTShadow(r *http.Request, bearer, orgID, caller string, delegated, legacyAllowed bool) {
	shadow := pdp.FromContext(r.Context()).Shadow()
	if shadow == nil {
		return
	}
	class := principal.AccessClassOf(bearer)
	if reason := shadowSkipReason(class); reason != "" {
		slog.DebugContext(r.Context(), "query-api pdp shadow: the credential class never asks the PDP", slog.String("reason", reason))
	}
	pattern := r.Pattern
	if !strings.Contains(pattern, " ") {
		pattern = routeProfile(r)
	}
	shadow.Observe(r.Context(), policy.ShadowObservation{Pattern: pattern, Bearer: bearer, OrgID: orgID, Caller: caller,
		UserAccessToken: class == principal.AccessClassUser, Impersonating: delegated, LegacyAllowed: legacyAllowed})
}

func shadowSkipReason(class principal.AccessClass) string {
	switch class {
	case principal.AccessClassEdge:
		return shadowSkipHS256
	case principal.AccessClassOther:
		return shadowSkipEnvelope
	}
	return ""
}
