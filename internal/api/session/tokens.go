package session

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// accessExpiresIn is TokenPair.expires_in and the refresh route's
// expires_in: JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60 (60 minutes).
const accessExpiresIn = int64(60 * time.Minute / time.Second)

// tokenPair is a sign-in's tokens; session is auth-service's session-route
// token under its eddsa class, empty otherwise.
type tokenPair struct {
	access, refresh, session string
	// expiresIn is the access token's lifetime when auth-service issued the pair; 0 for go-api's own mint
	// (accessExpiresIn).
	expiresIn int64
}

// expiresInOf is the expires_in a sign-in answer states for pair.
func expiresInOf(pair tokenPair) int64 {
	if pair.expiresIn > 0 {
		return pair.expiresIn
	}
	return accessExpiresIn
}

// roleText is str(membership.role): a NULL role is the text "None".
func roleText(role *string) string {
	if role == nil {
		return "None"
	}
	return *role
}

// clientHost is request.client.host: the peer's host, nil without one.
func clientHost(r *http.Request) *string {
	if r.RemoteAddr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return &host
}

// userAgent is request.headers.get("user-agent"): the first value, decoded
// as Starlette decodes headers (latin-1), nil when absent.
func userAgent(r *http.Request) *string {
	values := r.Header.Values("User-Agent")
	if len(values) == 0 {
		return nil
	}
	value := policy.Latin1(values[0])
	return &value
}

// sessionsOffRoutes are the routes that hand out a token pair. go-api mints none itself: without a session
// issuer (AUTH_PROXY_SESSIONS off) each answers sessionsOff.
var sessionsOffRoutes = map[string]bool{
	"/api/v1/auth/login":           true,
	"/api/v1/auth/social-login":    true,
	"/api/v1/auth/refresh":         true,
	"/api/v1/auth/switch-org":      true,
	"/api/v1/auth/change-password": true,
	"/api/v1/auth/accept-invite":   true,
}

// sessionsOffOr is local, or sessionsOff for a route that hands out a pair while no session issuer is set.
func (h handlers) sessionsOffOr(pattern string, local http.HandlerFunc) http.Handler {
	if h.Issuer == nil && sessionsOffRoutes[pattern] {
		return http.HandlerFunc(h.sessionsOff)
	}
	return local
}

// refreshNotLocal is the refresh route's local handler: go-api keeps no refresh credential. With sessions on the
// proxy forwards the route and this never runs; without a session issuer sessionsOffOr answers first.
func (h handlers) refreshNotLocal(w http.ResponseWriter, _ *http.Request) {
	refuse(w, http.StatusServiceUnavailable, sessionissue.UnavailableText)
}

// sessionsOff answers 503 config_missing naming AUTH_PROXY_SESSIONS.
func (h handlers) sessionsOff(w http.ResponseWriter, r *http.Request) {
	h.Logger.ErrorContext(r.Context(), "session route refused: AUTH_PROXY_SESSIONS is not on", "path", r.Pattern)
	policy.WriteConfigMissing(w, "AUTH_PROXY_SESSIONS")
}

// amr of the sessions go-api's routes ask auth-service for: a password
// sign-in (or a re-issue to a user who signed in with one), and a federated
// one.
var (
	amrPassword  = []string{"pwd"}
	amrFederated = []string{"fed"}
)

// issueSession takes the pair from auth-service's issue route. predecessor,
// when set, is the caller's own session the new one supersedes (switch-org):
// auth-service revokes it in the transaction that creates the new session.
func (h handlers) issueSession(ctx context.Context, r *http.Request, userID uuid.UUID, orgID *uuid.UUID, amr []string, predecessor *uuid.UUID) (tokenPair, error) {
	pair, err := h.Issuer.Issue(ctx, r, sessionissue.Request{PrincipalID: userID, OrganizationID: orgID, AMR: amr, PredecessorSessionID: predecessor})
	if err != nil {
		return tokenPair{}, err
	}
	return tokenPair{access: pair.Access, refresh: pair.Refresh, session: pair.SessionAccess, expiresIn: pair.ExpiresIn}, nil
}

// issuerRefusedTheOrg reports whether the issue route refused the session as
// not entitled: the one refusal an organization deleted after the membership
// commit gives (its delete removed the membership).
func issuerRefusedTheOrg(err error) bool {
	var refused *sessionissue.RefusedError
	return errors.As(err, &refused) && refused.Status == http.StatusForbidden && refused.Reason == "not_entitled"
}

// orgGone reports whether orgID has no organizations row. A failed read
// reports false: the caller keeps its 503.
func orgGone(ctx context.Context, tx pgx.Tx, orgID uuid.UUID) bool {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM organizations WHERE id = $1)`, orgID).Scan(&exists); err != nil {
		return false
	}
	return !exists
}

// issueMembershipTokens is _issue_membership_tokens.
func (h handlers) issueMembershipTokens(ctx context.Context, tx pgx.Tx, r *http.Request, user *userRow, membership *membershipRow) (tokenPair, error) {
	return h.issueMembershipTokensSuperseding(ctx, tx, r, user, membership, nil)
}

// issueMembershipTokensSuperseding is issueMembershipTokens for a caller whose
// own auth-service session (predecessor) the new pair replaces: switch-org
// (CHAOS-12085). Without auth-service sessions there is no predecessor to
// revoke and it is issueMembershipTokens.
func (h handlers) issueMembershipTokensSuperseding(ctx context.Context, tx pgx.Tx, r *http.Request, user *userRow, membership *membershipRow,
	predecessor *uuid.UUID) (tokenPair, error) {
	var orgUUID *uuid.UUID
	if membership != nil {
		id := membership.OrgID
		orgUUID = &id
	}
	return h.issueSession(ctx, r, user.ID, orgUUID, amrPassword, predecessor)
}

func optionalText(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// userInfo is _to_user_info(user, membership).
func userInfo(user *userRow, membership *membershipRow) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", user.ID.String())
	out.Set("email", user.Email)
	out.Set("username", optionalText(user.Username))
	out.Set("full_name", optionalText(user.FullName))
	if membership != nil {
		out.Set("org_id", membership.OrgID.String())
		out.Set("role", roleText(membership.Role))
	} else {
		out.Set("org_id", nil)
		out.Set("role", "member")
	}
	out.Set("is_superuser", user.IsSuperuser)
	return out
}

// loginResponse is LoginResponse; extra appends SocialLoginResponse's
// is_new_user when non-nil.
func loginResponse(pair tokenPair, needsOnboarding bool, user *pyjson.Object, isNewUser *bool) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("access_token", pair.access)
	out.Set("refresh_token", pair.refresh)
	out.Set("token_type", "bearer")
	out.Set("expires_in", pyjson.IntOf(expiresInOf(pair)))
	out.Set("needs_onboarding", needsOnboarding)
	out.Set("user", user)
	if isNewUser != nil {
		out.Set("is_new_user", *isNewUser)
	}
	setSessionAccessToken(out, pair)
	return out
}

// setSessionAccessToken adds auth-service's session-route token as the last
// member when the pair has one, so a pair without it keeps the exact wire.
func setSessionAccessToken(out *pyjson.Object, pair tokenPair) {
	if pair.session != "" {
		out.Set("session_access_token", pair.session)
	}
}
