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
	if refuseAmbiguousCarrier(w, r) {
		return authctx.Claims{}, false
	}
	if internalidentity.Present(r.Header) {
		// The public listener deletes these headers before any handler runs
		// (CHAOS-6780), so a request reaching here with them off the internal
		// listener means the handler was wired without that middleware: refuse
		// rather than trust a header no listener vouched for.
		if !internalidentity.OnInternalListener(r.Context()) {
			refuseInternal(w, r, "headers_off_internal_listener", "headers")
			return authctx.Claims{}, false
		}
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
		// principal.Verify logs and counts its own rejection reason; the
		// internal-path outcome is counted here so the carrier series is whole.
		internalidentity.RecordOutcome("envelope", "invalid")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return authctx.Claims{}, false
	}
	internalidentity.RecordOutcome("envelope", "accepted")
	return authctx.Claims{OrgID: claims.OrgID, Role: claims.Role, IsSuperuser: claims.IsSuperuser, ImpersonationActive: claims.ImpersonationActive}, true
}

// refuseAmbiguousCarrier answers 401 and reports true when the request carries
// both the internal identity headers and an Authorization header. /query calls
// it before it looks the document up, so an ambiguous request is refused even
// for a document this router would otherwise 404 (r1 P1: the 404 came first).
func refuseAmbiguousCarrier(w http.ResponseWriter, r *http.Request) bool {
	if !internalidentity.Present(r.Header) || len(r.Header.Values("Authorization")) == 0 {
		return false
	}
	refuseInternal(w, r, "ambiguous_carrier", "both")
	return true
}

// refuseInternal answers the bare 401 and leaves a line naming why. The line
// carries the reason and the path, never a header value.
func refuseInternal(w http.ResponseWriter, r *http.Request, reason, carrier string) {
	internalidentity.RecordOutcome(carrier, reason)
	log.Printf("query-api: internal request refused: reason=%s carrier=%s path=%s request_id=%s",
		reason, carrier, r.URL.Path, envelopeRequestID(r))
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
