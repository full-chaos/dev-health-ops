package server

import (
	"errors"
	"log"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
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
//     the Python edge stops minting it), or
//   - (CHAOS-6263 PR (a), /query only: edgeAuth non-nil) the user's own
//     edge access token -- the SAME HS256 bearer authenticateRESTRequest
//     already accepts on every REST route, disambiguated from the
//     envelope by the token's own JWT `alg` header, exactly as that
//     function does.
//
// A request carrying both the header carrier and ANY Authorization value
// is refused, never resolved by preferring one: two carriers can name two
// identities. A request carrying more than one Authorization header value
// is refused the same way -- two bearer tokens can also name two
// identities, and taking "the first" (net/http's Header.Get) would silently
// ignore the second rather than notice the ambiguity. A request carrying
// neither, or a malformed one, is refused with the same bare 401 the
// envelope path always answered. Never call this from a route an Ingress
// reaches: a browser could set the identity headers itself. Those routes
// keep authenticateRESTRequest.
//
// verifier may be nil only where a test exercises the no-carrier refusal.
// edgeAuth/edgeStore are nil for /buildinfo (this credential is /query-only,
// per D2898/D2905) and for a pod with no GO_API_EDGE_JWT_SECRET configured.
func authenticateInternalRequest(w http.ResponseWriter, r *http.Request, verifier *principal.Verifier, edgeAuth *policy.Authenticator, edgeStore policy.Store) (authctx.Claims, bool) {
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

	if edgeAuth != nil {
		if alg, ok := jwtHeaderAlg(token); ok && alg == principal.EdgeAlgorithm {
			return authenticateEdgeCarrier(w, r, edgeAuth, edgeStore, token)
		}
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

// authenticateEdgeCarrier is /query's edge-access-token path (CHAOS-6263
// PR (a), D2905). Unlike the envelope branch above, it does not trust the
// token's own is_superuser/impersonation claims (there ARE none --
// EdgeClaims/the raw JWT carry is_superuser too, but Authenticate replaces
// it with the LIVE users row value, same as go-api's REST plane and
// Python's authenticate_access_token, see principal.go:19,144 there): is
// active, is_superuser and token_version are read live via
// policy.Authenticator.Authenticate; membership and impersonation-session
// state are read live via the SAME store Authenticate used (edgeStore),
// mirroring policy.Scope's own mayUseOrg/Impersonation call shape
// (scope.go) rather than reimplementing the DB checks. Role is trusted
// from the token, unchanged inherited behaviour -- Python's own
// get_authenticated_user does the same (services/auth.py:350), never
// re-verified against a live row on either plane today.
func authenticateEdgeCarrier(w http.ResponseWriter, r *http.Request, edgeAuth *policy.Authenticator, edgeStore policy.Store, token string) (authctx.Claims, bool) {
	ctx := r.Context()
	user, err := edgeAuth.Authenticate(ctx, token)
	if err != nil {
		internalidentity.RecordOutcome("edge", "invalid")
		if errors.Is(err, policy.ErrUnavailable) {
			log.Printf("query-api: internal request refused: reason=edge_store_unavailable carrier=edge path=%s request_id=%s",
				r.URL.Path, envelopeRequestID(r))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return authctx.Claims{}, false
		}
		refuseInternal(w, r, "edge_rejected", "edge")
		return authctx.Claims{}, false
	}

	orgID := user.OrgID
	impersonationActive := false
	if user.IsSuperuser {
		session, sessionErr := edgeStore.ActiveImpersonation(ctx, user.ID)
		if sessionErr != nil {
			// FAIL CLOSED (D2919 condition 2, reversing this file's own
			// earlier draft): a superuser who IS impersonating must never
			// be served as the plain, non-impersonated principal during a
			// database fault -- that would let them pass a
			// "not-impersonating" gate (e.g. RequirePlatformAdmin) exactly
			// while genuinely impersonating. Every live check on this path
			// fails closed; this is not the one exception. (go-api's own
			// Scope.Impersonation, scope.go, DOES fail open on this same
			// lookup today -- reported to team-lead as a separate finding,
			// D2919 condition 2's own instruction: not copied here.)
			log.Printf("query-api: internal request refused: reason=edge_impersonation_lookup_failed carrier=edge path=%s request_id=%s",
				r.URL.Path, envelopeRequestID(r))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return authctx.Claims{}, false
		}
		if session != nil {
			orgID = session.TargetOrgID.String()
			impersonationActive = true
		}
	}

	// Membership existence for the token's own claimed org (D2905
	// condition 1) -- skipped only when impersonating (the live session
	// above IS the authorization for the target org; an impersonating
	// admin is deliberately not a member of it) or when orgID is empty
	// (a platform-wide operation, e.g. productTelemetryPlatformDashboard,
	// which Python itself calls with org_id="" -- schema.py:222).
	if orgID != "" && !impersonationActive {
		member, memberErr := edgeAuth.IsMember(ctx, user.UserID, orgID)
		if memberErr != nil {
			log.Printf("query-api: internal request refused: reason=edge_membership_lookup_failed carrier=edge path=%s request_id=%s",
				r.URL.Path, envelopeRequestID(r))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return authctx.Claims{}, false
		}
		if !member {
			refuseInternal(w, r, "edge_not_a_member", "edge")
			return authctx.Claims{}, false
		}
	}

	internalidentity.RecordOutcome("edge", "accepted")
	return authctx.Claims{OrgID: orgID, Role: user.Role, IsSuperuser: user.IsSuperuser, ImpersonationActive: impersonationActive}, true
}

// refuseAmbiguousCarrier answers 401 and reports true when the request
// carries both the internal identity headers and an Authorization header,
// OR more than one Authorization header value (two bearer tokens can also
// name two identities; net/http's Header.Get silently returns only the
// first, which would hide the second rather than notice the ambiguity).
// /query calls it before it looks the document up, so an ambiguous
// request is refused even for a document this router would otherwise 404
// (r1 P1: the 404 came first).
func refuseAmbiguousCarrier(w http.ResponseWriter, r *http.Request) bool {
	authValues := len(r.Header.Values("Authorization"))
	if authValues > 1 {
		refuseInternal(w, r, "ambiguous_carrier", "authorization")
		return true
	}
	if !internalidentity.Present(r.Header) || authValues == 0 {
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
