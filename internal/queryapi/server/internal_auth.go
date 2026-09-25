package server

import (
	"log"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// authenticateInternalRequest authenticates a request on an INTERNAL-ONLY
// path: /query and /buildinfo, which no Ingress routes to (CHAOS-6144,
// _records/6144/invariant.md). It accepts exactly one carrier:
//
//   - the internal identity headers (the in-cluster caller states the
//     identity; no token), or
//   - the effective-principal envelope bearer (the migration carrier, until
//     the Python edge stops minting it).
//
// A request carrying both is refused, never resolved by preferring one: two
// carriers can name two identities. A request carrying neither, or a
// malformed one, is refused with the same bare 401 the envelope path always
// answered. Never call this from a route an Ingress reaches: a browser could
// set the identity headers itself. Those routes keep authenticateRESTRequest.
//
// verifier may be nil only where a test exercises the no-carrier refusal.
func authenticateInternalRequest(w http.ResponseWriter, r *http.Request, verifier *principal.Verifier) (authctx.Claims, bool) {
	hasHeaders := internalidentity.Present(r.Header)
	hasBearer := len(r.Header.Values("Authorization")) > 0

	switch {
	case hasHeaders && hasBearer:
		refuseInternal(w, r, "ambiguous_carrier", "both")
		return authctx.Claims{}, false
	case hasHeaders:
		claims, err := internalidentity.FromHeader(r.Header)
		if err != nil {
			refuseInternal(w, r, internalidentity.ReasonOf(err), "headers")
			return authctx.Claims{}, false
		}
		internalidentity.RecordOutcome("headers", "accepted")
		return claims, true
	}

	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		refuseInternal(w, r, "no_carrier", "none")
		return authctx.Claims{}, false
	}
	verifyCtx := principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r))
	claims, err := verifier.Verify(verifyCtx, token)
	if err != nil {
		// principal.Verify logs and counts its own rejection reason.
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return authctx.Claims{}, false
	}
	internalidentity.RecordOutcome("envelope", "accepted")
	return authctx.Claims{OrgID: claims.OrgID, Role: claims.Role, IsSuperuser: claims.IsSuperuser, ImpersonationActive: claims.ImpersonationActive}, true
}

// refuseInternal answers the bare 401 and leaves a line naming why. The line
// carries the reason and the path, never a header value.
func refuseInternal(w http.ResponseWriter, r *http.Request, reason, carrier string) {
	internalidentity.RecordOutcome(carrier, reason)
	log.Printf("query-api: internal request refused: reason=%s carrier=%s path=%s request_id=%s",
		reason, carrier, r.URL.Path, envelopeRequestID(r))
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
