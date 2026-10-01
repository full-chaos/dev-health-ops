package server

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"

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
	claims, outcome := checkEdgeCarrier(r, edgeAuth, token, liveScope(edgeStore))
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

// edgeScope answers, for a caller Authenticate has just accepted, the org the
// request acts in and the active impersonation session that makes the
// caller's effective identity the target's (nil when none). An error decided
// nothing.
type edgeScope func(ctx context.Context, user *policy.User) (org string, session *policy.Impersonation, err error)

// liveScope is /query's scope: nothing in front of /query decided anything,
// so the org is the token's own and the session is read here, once, for a
// caller the live users row confirms is a superuser.
func liveScope(store policy.Store) edgeScope {
	return func(ctx context.Context, user *policy.User) (string, *policy.Impersonation, error) {
		if !user.IsSuperuser {
			return user.OrgID, nil, nil
		}
		session, err := store.ActiveImpersonation(ctx, user.ID)
		return user.OrgID, session, err
	}
}

// errOrgUndecided and errImpersonationUndecided: the request reached the
// /graphql pipeline without passing the OrgScope or the Impersonation
// middleware, so nobody decided the org or read the session.
var (
	errOrgUndecided           = errors.New("org scope not decided for this request")
	errImpersonationUndecided = errors.New("impersonation not decided for this request")
)

// decidedScope is /graphql's scope: the org policy.Scope's OrgScope verified
// for this request (the X-Org-Id after its membership check, else the
// caller's own org; the impersonation target's org while impersonating) and
// the session its Impersonation middleware read, the one that set
// X-Impersonating on the response. Reading either again here could give the
// pipeline another answer than the middleware acted on: a session started or
// ended between two reads, or the token's org where the scope accepted the
// org the client selected. So this scope applies the middleware's decisions
// as they stand, and a request no middleware decided is refused, never served
// in an org nobody verified or as a principal that is not impersonating.
func decidedScope(ctx context.Context, _ *policy.User) (string, *policy.Impersonation, error) {
	if !policy.OrgScopeDecided(ctx) {
		return "", nil, errOrgUndecided
	}
	if !policy.ImpersonationDecided(ctx) {
		return "", nil, errImpersonationUndecided
	}
	return policy.OrgIDFrom(ctx), policy.ImpersonationFrom(ctx), nil
}

// storeFailureReason is the logged reason for an identity read that failed:
// reason itself, or, when the database role was denied the read, a reason that
// names the grant that is missing, so an operator sees what the migration has
// not applied yet.
func storeFailureReason(reason string, err error) string {
	if grant := policy.MissingGrant(err); grant != "" {
		return "edge_grant_missing missing_grant=" + strconv.Quote(grant)
	}
	return reason
}

// roleInOrg is the caller's role in the org the request acts in, and whether
// the caller may act there at all. A member holds the membership's role. A
// caller who is not a member may act only as a live superuser in an org other
// than the token's own -- the one case the org scope lets a non-member name
// an org -- and holds no role there (IsSuperuser is what they act by). The
// token's own org always needs the membership, superuser or not.
func roleInOrg(user *policy.User, orgID, memberRole string, member bool) (string, bool) {
	switch {
	case member:
		return memberRole, true
	case user.IsSuperuser && orgID != user.OrgID:
		return "", true
	}
	return "", false
}

// canonicalOrg is org in the one spelling the resolvers compare against
// (lower-case, hyphenated); an org id that does not parse is kept as given.
func canonicalOrg(org string) string {
	if parsed, ok := policy.ParsePyUUID(org); ok {
		return parsed.String()
	}
	return org
}

// checkEdgeCarrier is the decision of authenticateEdgeCarrier, logged and
// counted, without an answer written: the caller's ONE identity for the
// request. The org is the scope's (decidedScope, liveScope); the role is the
// role the caller holds IN THAT ORG, read from the same membership row that
// answers the membership check, never the role the token states for its own
// org; while impersonating, both are the session target's.
func checkEdgeCarrier(r *http.Request, edgeAuth *policy.Authenticator, token string, scope edgeScope) (authctx.Claims, edgeOutcome) {
	ctx := r.Context()
	user, err := edgeAuth.Authenticate(ctx, token)
	if err != nil {
		internalidentity.RecordOutcome("edge", "invalid")
		if errors.Is(err, policy.ErrUnavailable) || policy.MissingGrant(err) != "" {
			log.Printf("query-api: internal request refused: reason=%s carrier=edge path=%s request_id=%s",
				storeFailureReason("edge_store_unavailable", err), r.URL.Path, envelopeRequestID(r))
			return authctx.Claims{}, edgeUnavailable
		}
		noteRefusal(r, "edge_rejected", "edge")
		return authctx.Claims{}, edgeRefused
	}

	orgID, session, scopeErr := scope(ctx, user)
	if scopeErr != nil {
		// FAIL CLOSED: a superuser who IS impersonating must never be
		// served as the plain, non-impersonated principal because the
		// session could not be read (a database fault) or was never read
		// (no middleware decided it) -- that would let them pass a
		// "not-impersonating" gate (e.g. RequirePlatformAdmin) exactly
		// while genuinely impersonating -- and no request is served in an
		// org nobody verified. Every live check on this path fails closed.
		// go-api's own Scope.Impersonation (internal/api/policy/scope.go)
		// refuses on the same lookup failure too.
		reason := "edge_impersonation_lookup_failed"
		switch {
		case errors.Is(scopeErr, errOrgUndecided):
			reason = "edge_org_undecided"
		case errors.Is(scopeErr, errImpersonationUndecided):
			reason = "edge_impersonation_undecided"
		}
		log.Printf("query-api: internal request refused: reason=%s carrier=edge path=%s request_id=%s",
			storeFailureReason(reason, scopeErr), r.URL.Path, envelopeRequestID(r))
		return authctx.Claims{}, edgeUnavailable
	}

	role := ""
	impersonationActive := session != nil
	switch {
	case impersonationActive:
		// The effective principal while impersonating is the TARGET's: org
		// and role both, from the session (principal_envelope.py
		// effective_principal_identity). The session IS the authorization
		// for the target org; an impersonating admin is deliberately not a
		// member of it.
		orgID = session.TargetOrgID.String()
		role = session.TargetRole
	case orgID != "":
		// One read of the membership row for the org the request acts in:
		// its existence is the live membership check, its role is the
		// caller's role there. The org scope read the same row when it
		// verified an X-Org-Id, so within the request this is its answer.
		memberRole, member, memberErr := edgeAuth.Membership(ctx, user.UserID, orgID)
		if memberErr != nil {
			// The role could not be read, so there is no role: the request
			// is refused, never served with a default one.
			log.Printf("query-api: internal request refused: reason=%s carrier=edge path=%s request_id=%s",
				storeFailureReason("edge_membership_lookup_failed", memberErr), r.URL.Path, envelopeRequestID(r))
			return authctx.Claims{}, edgeUnavailable
		}
		orgRole, allowed := roleInOrg(user, orgID, memberRole, member)
		if !allowed {
			noteRefusal(r, "edge_not_a_member", "edge")
			return authctx.Claims{}, edgeRefused
		}
		role = orgRole
		orgID = canonicalOrg(orgID)
	}
	// No org at all (a platform-wide operation, e.g.
	// productTelemetryPlatformDashboard, which Python itself calls with
	// org_id="" -- schema.py:222): no membership, no role in an org.

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
// alg, the envelope's EdDSA included) is refused. A request with no
// authenticator bound to it (edgeAuth nil) is refused as undecidable. The token itself is checked
// exactly as /query's edge carrier checks it (authenticateEdgeCarrier), except
// that the org and the impersonation session are the ones the middleware in
// front already decided (decidedScope), never a second read.
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
		// No chain bound an authenticator to the request: nothing can be
		// decided with the answers the middleware saw, so it is refused as a
		// check that could not be made.
		log.Printf("query-api: internal request refused: reason=edge_not_bound carrier=edge path=%s request_id=%s",
			r.URL.Path, envelopeRequestID(r))
		return authctx.Claims{}, edgeUnavailable
	}
	if alg, ok := jwtHeaderAlg(token); !ok || alg != principal.EdgeAlgorithm {
		noteRefusal(r, "not_an_edge_token", "edge")
		return authctx.Claims{}, edgeRefused
	}
	return checkEdgeCarrier(r, edgeAuth, token, decidedScope)
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
