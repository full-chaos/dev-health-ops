package server

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// legacyClaims are the claims of an accepted caller the PDP does not decide (an exempt credential class): the
// organization of the credential and the Legacy scope of today's path.
func legacyClaims(orgID string) authctx.Claims {
	return authctx.Claims{OrgID: orgID, Scope: pepgate.Legacy(orgID)}
}

// authenticateRESTRouted is authenticateRESTRequest for an edge or user access
// token once the edge verifier routes by row: the request's row picks the
// validator, then the organization rule applies to either class. Answers: a
// refusal is the same 401 "Invalid or expired token"; a valid token with no
// organization is 403 unless the caller is a superuser; a check that could not
// be made is policy.WriteAuthFailure's 503 (unavailable) or 500, never a pass.
// An accepted user access token then asks the request's PDP gate; an accepted
// edge token is on the exempt list and is counted.
func authenticateRESTRouted(w http.ResponseWriter, r *http.Request, edgeVerifier *principal.EdgeVerifier, token, component string) (authctx.Claims, bool) {
	ctx := principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r))
	claims, err := edgeVerifier.VerifyRouted(ctx, routeProfile(r), token)
	if err == nil {
		gate := pdp.FromContext(r.Context())
		if principal.AccessClassOf(token) != principal.AccessClassUser {
			gate.CountExempt(r.Context(), pdp.ClassHS256, routeProfile(r))
			observeRESTShadow(r, token, claims.OrgID, claims.Caller, claims.Delegated, true)
			return legacyClaims(claims.OrgID), true
		}
		scope, denied := gate.Authorize(r.Context(), routeProfile(r), token, claims.OrgID)
		if denied != nil {
			writePDPDenied(w, r, component, denied)
			return authctx.Claims{}, false
		}
		observeRESTShadow(r, token, claims.OrgID, claims.Caller, claims.Delegated, true)
		return authctx.Claims{OrgID: claims.OrgID, Scope: scope}, true
	}
	var orgRequired *principal.OrgRequiredError
	switch {
	case errors.As(err, &orgRequired):
		observeRESTShadow(r, token, "", orgRequired.Caller, orgRequired.Delegated, false)
		policy.WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
	case policy.IsRefusal(err):
		writeRESTUnauthorized(w, r, component, "Invalid or expired token")
	default:
		requestID := envelopeRequestID(r)
		if !acceptableCorrelationID(requestID) {
			requestID = ""
		}
		slog.WarnContext(r.Context(), "query-api: access token check failed",
			slog.String("component", component), slog.String("request_id", logSafeRequestID(requestID)),
			slog.Bool("unavailable", errors.Is(err, policy.ErrUnavailable)), logging.ErrorAttr(err))
		policy.WriteAuthFailure(w, "", err)
	}
	return authctx.Claims{}, false
}
