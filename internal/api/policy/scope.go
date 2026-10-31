package policy

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type contextKey int

const (
	orgIDKey contextKey = iota
	impersonationKey
	impersonationDecidedKey
	readsKey
	orgDecidedKey
)

// OrgIDFrom returns the request's resolved org (the contextvar
// _current_org_id): the verified X-Org-Id, else the caller's own org, and
// the impersonated org while an impersonation session is active. "" means
// no org context.
func OrgIDFrom(ctx context.Context) string {
	value, _ := ctx.Value(orgIDKey).(string)
	return value
}

// EffectiveOrgID is the org an org-scoped admin route acts in for user
// (CHAOS-9510). A superuser with a live impersonation session acts in the
// target's org. Every other caller, and a superuser without a session, acts
// in the org claim of their token: the Python api's rule for these routes
// (get_admin_org_id), which the venue oracles pin, so a superuser's X-Org-Id
// does not move them. The session is read from the request context the
// Impersonation middleware set after a live check, so it is re-decided on
// every request and never carried.
func EffectiveOrgID(ctx context.Context, user *User) string {
	if user == nil {
		return ""
	}
	if user.IsSuperuser {
		if session := ImpersonationFrom(ctx); session != nil {
			return session.TargetOrgID.String()
		}
	}
	return user.OrgID
}

// ImpersonationFrom returns the active impersonation session of the request
// (get_impersonation_context()), or nil.
func ImpersonationFrom(ctx context.Context) *Impersonation {
	value, _ := ctx.Value(impersonationKey).(*Impersonation)
	return value
}

// OrgScopeDecided reports whether the OrgScope middleware ran for the request
// and let it through: only then is OrgIDFrom the scope's verified answer (the
// X-Org-Id after its membership check, else the caller's own org) rather
// than "nobody decided".
func OrgScopeDecided(ctx context.Context) bool {
	decided, _ := ctx.Value(orgDecidedKey).(bool)
	return decided
}

// ImpersonationDecided reports whether the Impersonation middleware ran for
// the request and let it through. Only then does a nil ImpersonationFrom mean
// "not impersonating" rather than "nobody looked": a handler that serves the
// effective identity from the middleware's decision refuses without one.
func ImpersonationDecided(ctx context.Context) bool {
	decided, _ := ctx.Value(impersonationDecidedKey).(bool)
	return decided
}

// Scope holds the request-wide steps: Principal, then OrgScope and
// Impersonation. They run for every request on the api listener, public
// routes and unknown paths included, as the Python middlewares do.
type Scope struct {
	auth         *Authenticator
	logger       *slog.Logger
	rows         func(profile string) (BearerRow, bool)
	authenticate func(ctx context.Context, token string) (*User, error)
}

// NewScope builds the middlewares over auth.
func NewScope(auth *Authenticator, logger *slog.Logger) *Scope {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scope{auth: auth, logger: logger}
}

// acrInternalPrefix is the path prefix OrgIdMiddleware skips.
const acrInternalPrefix = "/api/v1/internal/acr/"

// principalFrom is the caller the Principal step decided for the request:
// the user, nil for an anonymous or refused caller, or the store failure.
// The scope never reads the Authorization header itself. With no decision
// (the Principal step did not run first) it answers errNoPrincipal, so both
// middlewares refuse the request with the generic 500: they fail closed.
func principalFrom(r *http.Request) (*User, error) {
	decision, ok := decisionFrom(r.Context())
	if !ok {
		return nil, errNoPrincipal
	}
	if decision.err != nil {
		return nil, decision.err
	}
	if decision.user == nil {
		return nil, nil
	}
	// Each consumer gets its own copy, so a change one of them makes to the
	// User never reaches the other one (the rule ReadOnce keeps for its
	// reads).
	return copyUser(decision.user), nil
}

// OrgScope is OrgIdMiddleware. With an X-Org-Id header and an authenticated
// caller, the header is accepted only for a superuser, the caller's own
// org, or an org the caller has a membership in; anything else is a 403
// before the route runs. With no header, the caller's own org is the
// scope. An anonymous or refused caller passes through with no org: the
// route's own guard answers 401.
func (s *Scope) OrgScope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, acrInternalPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		headerOrgID := ""
		if values := r.Header.Values("X-Org-Id"); len(values) > 0 {
			headerOrgID = strings.TrimFunc(Latin1(values[0]), isPySpace)
		}
		user, err := principalFrom(r)
		if err != nil {
			s.logger.ErrorContext(r.Context(), "api org scope: caller lookup failed",
				slog.String("path", r.Pattern), slog.Bool("unavailable", isUnavailable(err)),
				slog.String("missing_grant", MissingGrant(err)), slog.String("reason", scopeReason(err)))
			writeCallerLookupFailure(w, err)
			return
		}
		resolved := ""
		switch {
		case headerOrgID != "" && user != nil && !(headerOrgID == user.OrgID && s.readsTokenOrg(r)):
			allowed, err := s.mayUseOrg(r.Context(), user, headerOrgID)
			if err != nil {
				s.logger.ErrorContext(r.Context(), "api org scope: membership lookup failed", slog.String("user_id", user.UserID), slog.Bool("unavailable", isUnavailable(err)), slog.String("missing_grant", MissingGrant(err)))
				s.writeLookupFailure(w, r, err)
				return
			}
			if !allowed {
				s.logger.WarnContext(r.Context(), "X-Org-Id rejected", slog.String("user_id", user.UserID), slog.String("org_id", headerOrgID))
				writeOrgDenied(w)
				return
			}
			resolved = headerOrgID
		case user != nil && user.OrgID != "":
			if s.readsTokenOrg(r) && !s.useTokenOrg(w, r, user) {
				return
			}
			resolved = user.OrgID
		}
		ctx := context.WithValue(r.Context(), orgDecidedKey, true)
		if resolved != "" {
			ctx = context.WithValue(ctx, orgIDKey, resolved)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// mayUseOrg is OrgIdMiddleware's acceptance rule for a requested org.
func (s *Scope) mayUseOrg(ctx context.Context, user *User, orgID string) (bool, error) {
	if user.IsSuperuser || orgID == user.OrgID {
		return true, nil
	}
	return s.auth.IsMember(ctx, user.UserID, orgID)
}

// IsMember is user_is_member_of_org: an id that does not parse is not a
// member.
func (a *Authenticator) IsMember(ctx context.Context, userID, orgID string) (bool, error) {
	_, member, err := a.Membership(ctx, userID, orgID)
	return member, err
}

// Membership is the user's membership in the org: the role they hold there,
// and whether they are a member at all. An id that does not parse is not a
// member and reads nothing. Within a request bound to a (ReadOnce) the row is
// read once: the membership check and the role in that org are one answer.
func (a *Authenticator) Membership(ctx context.Context, userID, orgID string) (string, bool, error) {
	user, okUser := ParsePyUUID(userID)
	org, okOrg := ParsePyUUID(orgID)
	if !okUser || !okOrg {
		return "", false, nil
	}
	reads := a.readsFor(ctx)
	if reads == nil {
		return a.store.Membership(ctx, user, org)
	}
	reads.mu.Lock()
	defer reads.mu.Unlock()
	key := [2]uuid.UUID{user, org}
	if got, ok := reads.members[key]; ok {
		return got.role, got.member, got.err
	}
	role, member, err := a.store.Membership(ctx, user, org)
	reads.members[key] = memberRead{role: role, member: member, err: err}
	return role, member, err
}

// Impersonation is ImpersonationMiddleware. For a caller mayImpersonate lets
// in whose users row says superuser, an active session makes the target's
// org the request's org, sets the impersonation context, and stamps
// X-Impersonating / X-Impersonated-User-Id on the response. A session
// lookup failure refuses the request with the generic 500, like the other
// authorization lookups (fail closed, logged).
func (s *Scope) Impersonation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Every path below that calls next has decided; the refusals return
		// without calling it.
		r = r.WithContext(context.WithValue(r.Context(), impersonationDecidedKey, true))
		if !s.mayImpersonate(r) {
			next.ServeHTTP(w, r)
			return
		}
		user, err := principalFrom(r)
		if err != nil {
			s.logger.ErrorContext(r.Context(), "api impersonation: caller lookup failed",
				slog.Bool("unavailable", isUnavailable(err)), slog.String("missing_grant", MissingGrant(err)), slog.String("reason", scopeReason(err)))
			writeCallerLookupFailure(w, err)
			return
		}
		if user == nil || !user.IsSuperuser {
			next.ServeHTTP(w, r)
			return
		}
		session, err := s.auth.activeImpersonation(r.Context(), user.ID)
		if err != nil {
			// Fail closed: an admin whose active session could not be read
			// must not be served as though no session existed (that would
			// lift the impersonation restriction). Same refusal as the
			// caller and membership lookups above.
			s.logger.ErrorContext(r.Context(), "api impersonation: session lookup failed; refusing the request",
				slog.String("path", r.Pattern), slog.String("admin_user_id", user.UserID),
				slog.Bool("unavailable", isUnavailable(err)), slog.String("missing_grant", MissingGrant(err)), slog.Any("error", err))
			WriteInternal(w)
			return
		}
		if session == nil {
			next.ServeHTTP(w, r)
			return
		}
		ctx := context.WithValue(r.Context(), orgIDKey, session.TargetOrgID.String())
		ctx = context.WithValue(ctx, impersonationKey, session)
		header := w.Header()
		header.Add("X-Impersonating", "true")
		header.Add("X-Impersonated-User-Id", session.TargetUserID.String())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// pyTruthy is bool() of a decoded JSON value.
func pyTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case float64:
		return typed != 0
	case string:
		return typed != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

// orgDeniedBody is OrgIdMiddleware._deny's body: json.dumps with its default
// separators, so ": " with a space, unlike every JSONResponse body.
var orgDeniedBody = []byte(`{"detail": "X-Org-Id not permitted for this user"}`)

// writeOrgDenied is OrgIdMiddleware._deny: 403 with exactly content-type and
// content-length.
func writeOrgDenied(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(orgDeniedBody)))
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.Copy(w, bytes.NewReader(orgDeniedBody))
}

// ReadsPlatformRoles is the platform roles switch the impersonation gate
// reads: the one its users store reads is_superuser under.
func (s *Scope) ReadsPlatformRoles() bool { return s.auth != nil && s.auth.store.ReadsPlatformRoles() }

// useTokenOrg decides whether user may act in the token's own org, with or
// without an X-Org-Id naming it, on a route that acts in an org
// (readsTokenOrg): a token keeps its org_id claim after the member is
// removed, so the membership row is read now (tokenOrgMembership). It writes
// the refusal itself: an existing org without the row is the X-Org-Id
// refusal, byte for byte; an org that does not exist or a claim that is not
// a UUID goes on as on main; a failed read answers 503 (temporarily
// unavailable) or 500, never a pass and never the refusal of a member. Behind
// ReadOnce the route's guard gets the same read's answer.
func (s *Scope) useTokenOrg(w http.ResponseWriter, r *http.Request, user *User) bool {
	if user.IsSuperuser {
		return true
	}
	_, standing, err := s.auth.tokenOrgMembership(r.Context(), user.UserID, user.OrgID)
	if err != nil {
		s.logger.ErrorContext(r.Context(), "api org scope: membership lookup failed",
			slog.String("user_id", user.UserID), slog.Bool("unavailable", isUnavailable(err)),
			slog.String("missing_grant", MissingGrant(err)))
		WriteAuthFailure(w, "", err)
		return false
	}
	if standing == tokenOrgNoRow {
		s.logger.WarnContext(r.Context(), "token org without a membership rejected",
			slog.String("user_id", user.UserID), slog.String("org_id", user.OrgID))
		writeOrgDenied(w)
		return false
	}
	return true
}

// tokenOrgStanding is a caller's standing in its token's own org.
type tokenOrgStanding int

const (
	// tokenOrgMember: the membership row exists.
	tokenOrgMember tokenOrgStanding = iota
	// tokenOrgNoRow: the org exists and the caller has no row in it.
	tokenOrgNoRow
	// tokenOrgAsMain: the org does not exist, or the claim is not a UUID: no
	// refusal comes from the membership; the request goes on as on main.
	tokenOrgAsMain
)

// tokenOrgMembership is the caller's standing in the token's own org, and the
// row's role for a member, from one read (OrgMembership). Within a request
// bound to a (ReadOnce) it shares Membership's record of the pair: a row or
// a failure read first is the answer, and an earlier "no row" stays the
// answer while this read adds whether the org exists.
func (a *Authenticator) tokenOrgMembership(ctx context.Context, userID, orgID string) (string, tokenOrgStanding, error) {
	org, okOrg := ParsePyUUID(orgID)
	if !okOrg {
		return "", tokenOrgAsMain, nil
	}
	user, okUser := ParsePyUUID(userID)
	if !okUser {
		return "", tokenOrgNoRow, nil
	}
	read, err := a.orgMembershipRead(ctx, user, org)
	switch {
	case err != nil:
		return "", tokenOrgNoRow, err
	case read.member:
		return read.role, tokenOrgMember, nil
	case read.orgFound:
		return "", tokenOrgNoRow, nil
	}
	return "", tokenOrgAsMain, nil
}

func (a *Authenticator) orgMembershipRead(ctx context.Context, user, org uuid.UUID) (memberRead, error) {
	reads := a.readsFor(ctx)
	if reads == nil {
		role, member, orgFound, err := a.store.OrgMembership(ctx, user, org)
		return memberRead{role: role, member: member, orgRead: true, orgFound: orgFound}, err
	}
	reads.mu.Lock()
	defer reads.mu.Unlock()
	key := [2]uuid.UUID{user, org}
	got, seen := reads.members[key]
	if seen && (got.err != nil || got.member || got.orgRead) {
		return got, got.err
	}
	role, member, orgFound, err := a.store.OrgMembership(ctx, user, org)
	if err != nil {
		if !seen {
			reads.members[key] = memberRead{err: err}
		}
		return memberRead{}, err
	}
	if !seen {
		got = memberRead{role: role, member: member}
	}
	got.orgRead, got.orgFound = true, orgFound
	reads.members[key] = got
	return got, nil
}

// readsTokenOrg reports whether the request's route acts in an org, so the
// token's own org needs a membership read: a route of a scope with declared
// rows (go-api; TestGoAPIScopeDeclaresTheEndpointProfileRows holds that it
// has them) unless its row says it does not act in an org (tenant requirement
// none or cross_org_superuser_only); a route without a row reads it. A scope
// without rows (query-api's /graphql, whose edge reads the membership itself)
// reads nothing here. An org-less route (OrglessMayReach) serves the caller, not an org, and reads nothing.
func (s *Scope) readsTokenOrg(r *http.Request) bool {
	if s.rows == nil {
		return false
	}
	if OrglessMayReach(r.Method, r.URL.Path) {
		return false
	}
	row, found := s.rows(RouteProfileFrom(r.Context()))
	return !found || !row.NotOrgScoped
}

// writeLookupFailure answers a failed membership read of an X-Org-Id: on a
// route that acts in an org (readsTokenOrg, go-api) 503 when the database is
// temporarily unavailable, else 500, as the token org's read answers; on
// every other route main's 500.
func (s *Scope) writeLookupFailure(w http.ResponseWriter, r *http.Request, err error) {
	if s.readsTokenOrg(r) {
		WriteAuthFailure(w, "", err)
		return
	}
	WriteInternal(w)
}
