package server

import (
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// restScopeOrg is the organization a REST handler reads: the one of the scope the request was allowed with. A
// request whose scope names none is refused with the 503 of the PDP gate before any statement runs.
func restScopeOrg(w http.ResponseWriter, r *http.Request, component string, claims authctx.Claims) (string, bool) {
	org, ok := claims.Scope.OrgID()
	if ok {
		return org, true
	}
	requestID := envelopeRequestID(r)
	if !acceptableCorrelationID(requestID) {
		requestID = ""
	}
	slog.WarnContext(r.Context(), "query-api: the request has no organization in its scope", slog.String("component", component),
		slog.String("request_id", requestID))
	policy.WriteDetail(w, http.StatusServiceUnavailable, "Authorization unavailable", nil)
	return "", false
}
