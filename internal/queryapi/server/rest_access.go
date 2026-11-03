package server

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// authenticateRESTRouted is authenticateRESTRequest for an edge or user access
// token once the edge verifier routes by row: the request's row picks the
// validator, then the organization rule applies to either class. Answers: a
// refusal is the same 401 "Invalid or expired token"; a valid token with no
// organization is 403 unless the caller is a superuser; a check that could not
// be made is policy.WriteAuthFailure's 503 (unavailable) or 500, never a pass.
func authenticateRESTRouted(w http.ResponseWriter, r *http.Request, edgeVerifier *principal.EdgeVerifier, token, component string) (authctx.Claims, bool) {
	ctx := principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r))
	claims, err := edgeVerifier.VerifyRouted(ctx, routeProfile(r), token)
	if err == nil {
		return authctx.Claims{OrgID: claims.OrgID}, true
	}
	var orgRequired *principal.OrgRequiredError
	switch {
	case errors.As(err, &orgRequired):
		policy.WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
	case policy.IsRefusal(err):
		writeRESTUnauthorized(w, r, component, "Invalid or expired token")
	default:
		requestID := envelopeRequestID(r)
		if !acceptableCorrelationID(requestID) {
			requestID = ""
		}
		slog.WarnContext(r.Context(), "query-api: access token check failed",
			slog.String("component", component), slog.String("request_id", requestID),
			slog.Bool("unavailable", errors.Is(err, policy.ErrUnavailable)), logging.ErrorAttr(err))
		policy.WriteAuthFailure(w, "", err)
	}
	return authctx.Claims{}, false
}
