package server

import (
	"context"
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
	claims, outcome := checkEdgeCarrier(r, edgeAuth, token, liveImpersonation(edgeStore))
	if outcome != edgeAccepted {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return authctx.Claims{}, false
	}
	return claims, true
}

// edgeOutcome is how checkEdgeCarrier decided a token. /query answers every
// failure with the same bare 401; /graphql answers a store it could not read
// as the Python app did (its unhandled 500), never as a bad credential.
type edgeOutcome int

const (
	edgeAccepted    edgeOutcome = iota
	edgeRefused                 // the credential is not good
	edgeUnavailable             // a live check could not be read: nothing was decided
)

// impersonationSource answers the active impersonation session that makes
// user's effective identity the target's, or nil. An error decided nothing.
type impersonationSource func(ctx context.Context, user *policy.User) (*policy.Impersonation, error)

// liveImpersonation is /query's source: nothing in front of /query read the
// session, so the store is read here, once, for a caller the live users row
// confirms is a superuser.
func liveImpersonation(store policy.Store) impersonationSource {
	return func(ctx context.Context, user *policy.User) (*policy.Impersonation, error) {
		if !user.IsSuperuser {
			return nil, nil
		}
		return store.ActiveImpersonation(ctx, user.ID)
	}
}

// errImpersonationUndecided: the request reached the /graphql pipeline
// without passing the Impersonation middleware, so nobody read the session.
var errImpersonationUndecided = errors.New("impersonation not decided for this request")

// decidedImpersonation is /graphql's source: the session policy.Scope's
// Impersonation middleware read for this request in front of the pipeline
// (graphQLEdgeChain), the one that set X-Impersonating on the response. A
// second store read here could see a session start or end between the two
// and serve one identity under the other's headers. The Python edge read it
// once as well: its ImpersonationMiddleware set the contextvar that
// get_context and effective_principal_identity read, whatever the later
// users-row read said. So this source applies the middleware's decision as
// it stands, and a request no middleware decided is refused, never served
// as a principal that is not impersonating.
func decidedImpersonation(ctx context.Context, _ *policy.User) (*policy.Impersonation, error) {
	if !policy.ImpersonationDecided(ctx) {
		return nil, errImpersonationUndecided
	}
	return policy.ImpersonationFrom(ctx), nil
}

// checkEdgeCarrier is the decision of authenticateEdgeCarrier, logged and
// counted, without an answer written. sessionOf says where the impersonation
// session comes from (liveImpersonation, decidedImpersonation).
func checkEdgeCarrier(r *http.Request, edgeAuth *policy.Authenticator, token string, sessionOf impersonationSource) (authctx.Claims, edgeOutcome) {
	ctx := r.Context()
	user, err := edgeAuth.Authenticate(ctx, token)
	if err != nil {
		internalidentity.RecordOutcome("edge", "invalid")
		if errors.Is(err, policy.ErrUnavailable) {
			log.Printf("query-api: internal request refused: reason=edge_store_unavailable carrier=edge path=%s request_id=%s",
				r.URL.Path, envelopeRequestID(r))
			return authctx.Claims{}, edgeUnavailable
		}
		noteRefusal(r, "edge_rejected", "edge")
		return authctx.Claims{}, edgeRefused
	}

	orgID := user.OrgID
	role := user.Role
	impersonationActive := false
	session, sessionErr := sessionOf(ctx, user)
	if sessionErr != nil {
		// FAIL CLOSED: a superuser who IS impersonating must never be
		// served as the plain, non-impersonated principal because the
		// session could not be read (a database fault) or was never read
		// (no middleware decided it) -- that would let them pass a
		// "not-impersonating" gate (e.g. RequirePlatformAdmin) exactly
		// while genuinely impersonating. Every live check on this path
		// fails closed; this is not the one exception. go-api's own
		// Scope.Impersonation (internal/api/policy/scope.go) refuses on
		// this same lookup failure too.
		reason := "edge_impersonation_lookup_failed"
		if errors.Is(sessionErr, errImpersonationUndecided) {
			reason = "edge_impersonation_undecided"
		}
		log.Printf("query-api: internal request refused: reason=%s carrier=edge path=%s request_id=%s",
			reason, r.URL.Path, envelopeRequestID(r))
		return authctx.Claims{}, edgeUnavailable
	}
	if session != nil {
		// The effective principal while impersonating is the TARGET's:
		// org and role both, as the Python edge stated it to /query
		// (principal_envelope.py effective_principal_identity).
		orgID = session.TargetOrgID.String()
		role = session.TargetRole
		impersonationActive = true
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
			return authctx.Claims{}, edgeUnavailable
		}
		if !member {
			noteRefusal(r, "edge_not_a_member", "edge")
			return authctx.Claims{}, edgeRefused
		}
	}

	internalidentity.RecordOutcome("edge", "accepted")
	return authctx.Claims{OrgID: orgID, Role: role, IsSuperuser: user.IsSuperuser, ImpersonationActive: impersonationActive}, edgeAccepted
}

// authenticateEdgeTokenOnly authenticates /graphql, the product path an
// Ingress reaches. Its ONE carrier is the user's own edge access token, the
// only credential the Python edge accepted there (graphql/app.py get_context,
// services/auth.py authenticate_access_token): the internal identity headers
// and the principal envelope are server-to-server carriers for /query, and
// neither may become usable from outside the cluster by riding the product
// path. So a request carrying the internal headers, a second Authorization
// value, no bearer, or a bearer that is not an edge token (any other JWT
// alg, the envelope's EdDSA included) is refused, and so is every request
// when this pod has no edge secret configured. The token itself is checked
// exactly as /query's edge carrier checks it (authenticateEdgeCarrier), except
// that the impersonation session is the one the middleware in front already
// read (decidedImpersonation), never a second read.
func authenticateEdgeTokenOnly(r *http.Request, edgeAuth *policy.Authenticator) (authctx.Claims, edgeOutcome) {
	if len(r.Header.Values("Authorization")) > 1 {
		noteRefusal(r, "ambiguous_carrier", "authorization")
		return authctx.Claims{}, edgeRefused
	}
	// The public listener deletes these headers before any handler runs, so
	// this refuses only on the internal listener, whose callers have no
	// business on the product path.
	if internalidentity.Present(r.Header) {
		noteRefusal(r, "internal_headers_on_edge_path", "headers")
		return authctx.Claims{}, edgeRefused
	}
	token, ok := bearerToken(r.Header.Get("Authorization"))
	if !ok {
		noteRefusal(r, "no_carrier", "none")
		return authctx.Claims{}, edgeRefused
	}
	if edgeAuth == nil {
		noteRefusal(r, "edge_not_configured", "edge")
		return authctx.Claims{}, edgeRefused
	}
	if alg, ok := jwtHeaderAlg(token); !ok || alg != principal.EdgeAlgorithm {
		noteRefusal(r, "not_an_edge_token", "edge")
		return authctx.Claims{}, edgeRefused
	}
	return checkEdgeCarrier(r, edgeAuth, token, decidedImpersonation)
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
	noteRefusal(r, reason, carrier)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

// noteRefusal counts a refused carrier and leaves the line naming why.
func noteRefusal(r *http.Request, reason, carrier string) {
	internalidentity.RecordOutcome(carrier, reason)
	log.Printf("query-api: internal request refused: reason=%s carrier=%s path=%s request_id=%s",
		reason, carrier, r.URL.Path, envelopeRequestID(r))
}
