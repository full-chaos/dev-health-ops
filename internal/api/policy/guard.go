package policy

import (
	"context"
	"log/slog"
	"net/http"
)

// Authz is a route's authorization level. Every level above Public first
// authenticates the caller as get_current_user does.
type Authz int

const (
	// Public runs no credential check.
	Public Authz = iota
	// Authenticated is Depends(get_current_user).
	Authenticated
	// Admin is Depends(require_admin): role owner/admin or superuser.
	Admin
	// Superuser is Depends(require_superuser).
	Superuser
	// AdminOrg is Depends(get_admin_org_id): Admin, plus the caller's own
	// org (the token's org_id claim) must be non-empty.
	AdminOrg
	// Optional is Depends(get_current_user_optional): the caller is
	// authenticated when a usable bearer token is sent and anonymous
	// otherwise. A missing, non-Bearer or refused token never refuses the
	// request; only a store failure does (503 when the database is
	// temporarily unavailable, else 500), as in Python. UserFrom is nil for
	// an anonymous caller.
	Optional
)

func (a Authz) String() string {
	switch a {
	case Public:
		return "public"
	case Authenticated:
		return "authenticated"
	case Admin:
		return "admin"
	case Superuser:
		return "superuser"
	case AdminOrg:
		return "admin_org"
	case Optional:
		return "optional"
	default:
		return "unknown"
	}
}

const userKey contextKey = 100

// UserFrom returns the authenticated caller of a guarded route, or nil.
func UserFrom(ctx context.Context) *User {
	value, _ := ctx.Value(userKey).(*User)
	return value
}

// WithUser returns ctx carrying user. For tests of area handlers.
func WithUser(ctx context.Context, user *User) context.Context {
	return context.WithValue(ctx, userKey, user)
}

// WithOrgID returns ctx carrying the resolved org. For tests of area
// handlers.
func WithOrgID(ctx context.Context, orgID string) context.Context {
	return context.WithValue(ctx, orgIDKey, orgID)
}

// WithImpersonation returns ctx carrying session. For tests of area
// handlers.
func WithImpersonation(ctx context.Context, session *Impersonation) context.Context {
	return context.WithValue(ctx, impersonationKey, session)
}

// Guard applies a route's authorization level.
type Guard struct {
	auth   *Authenticator
	logger *slog.Logger
}

// NewGuard builds a Guard over auth.
func NewGuard(auth *Authenticator, logger *slog.Logger) *Guard {
	if logger == nil {
		logger = slog.Default()
	}
	return &Guard{auth: auth, logger: logger}
}

// gate names the action a platform wrap decides by; the zero gate is the
// generic wrap, whose org-less rule is admin.orgs.
type gate struct {
	action string
	kind   gateKind
}

type gateKind int

const (
	gateNone gateKind = iota
	// gatePlatform admits only a holder of the action.
	gatePlatform
	// gateAdminOr admits Admin, or a holder of the action.
	gateAdminOr
)

// orglessAction is the action that lets a caller with no organization reach a
// route: the route's own, else admin.orgs.
func (gt gate) orglessAction() string {
	if gt.action != "" {
		return gt.action
	}
	return ActionAdminOrgs
}

// Wrap returns next behind level. A refused caller never reaches next. The
// bare Superuser level admits nobody: a platform route is wrapped with
// WrapPlatform and its action.
func (g *Guard) Wrap(level Authz, next http.Handler) http.Handler {
	return g.wrap(level, gate{}, next)
}

func (g *Guard) wrap(level Authz, gt gate, next http.Handler) http.Handler {
	if level == Public {
		return countGuardLevel(level, next)
	}
	if level == Optional {
		return countGuardLevel(level, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, ok := g.optionalUser(w, r)
			if !ok {
				return
			}
			if user != nil {
				r = r.WithContext(WithUser(r.Context(), user))
			}
			next.ServeHTTP(w, r)
		}))
	}
	return countGuardLevel(level, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := g.currentUser(w, r, level, gt)
		if !ok {
			return
		}
		if user.OrgID == "" && !user.Holds(gt.orglessAction()) && !OrglessMayReach(r.Method, r.URL.Path) {
			WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
			return
		}
		switch level {
		case Admin, AdminOrg:
			// Org-scoped admin routes act in the effective org (CHAOS-9510):
			// handlers read UserFrom(ctx).OrgID, so an impersonating
			// superuser must not be served their own org's rows.
			if org := EffectiveOrgID(r.Context(), user); org != user.OrgID {
				acting := *user
				acting.OrgID = org
				user = &acting
			}
			if !user.IsAdmin() && !(gt.kind == gateAdminOr && user.Holds(gt.action)) {
				WriteDetail(w, http.StatusForbidden, "Admin access required", nil)
				return
			}
			if level == AdminOrg && user.OrgID == "" {
				WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
				return
			}
		case Superuser:
			if gt.kind != gatePlatform || !user.Holds(gt.action) {
				WriteDetail(w, http.StatusForbidden, "Superuser access required", nil)
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), user)))
	}))
}

// WrapPlatform returns next behind the platform gate of action: only a caller
// that holds action passes.
func (g *Guard) WrapPlatform(action string, next http.Handler) http.Handler {
	return g.wrap(Superuser, gate{action, gatePlatform}, next)
}

// WrapAdminOr returns next behind Admin, which a caller that holds action also
// passes.
func (g *Guard) WrapAdminOr(action string, next http.Handler) http.Handler {
	return g.wrap(Admin, gate{action, gateAdminOr}, next)
}

// currentUser is get_current_user. It writes the refusal itself. At the admin
// levels the role is the token org's membership row's (adminRole).
func (g *Guard) currentUser(w http.ResponseWriter, r *http.Request, level Authz, gt gate) (*User, bool) {
	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		authFailure(w, "Not authenticated", nil)
		return nil, false
	}
	token, ok := BearerToken(authorization)
	if !ok {
		authFailure(w, "Invalid authorization header", nil)
		return nil, false
	}
	user, err := g.auth.Authenticate(g.auth.routeCtx(r), token)
	if err != nil {
		if !isRefusal(err) {
			g.logger.ErrorContext(r.Context(), "api authentication: user lookup failed",
				slog.String("path", r.Pattern), slog.Bool("unavailable", isUnavailable(err)))
		}
		authFailure(w, "Invalid or expired token", err)
		return nil, false
	}
	switch {
	case level == Superuser:
		countRoleSource(r.Context(), kindRolePlatformGate)
	case level == Admin || level == AdminOrg:
		switch {
		case user.Holds(ActionAdminOrgs):
			countRoleSource(r.Context(), kindRolePlatformBypass)
		case user.OrgID == "":
			countRoleSource(r.Context(), kindRoleClaimFallback)
		}
	}
	if (level != Admin && level != AdminOrg) || user.Holds(ActionAdminOrgs) || (gt.kind == gateAdminOr && user.Holds(gt.action)) || user.OrgID == "" {
		return user, true
	}
	// At an admin level the caller's role in the token's own org is the
	// membership row's now, not the role claim; an existing org without the
	// row is the org scope's X-Org-Id refusal, an org that does not exist or
	// a claim that is not a UUID keeps main's answer (the claim), and a
	// failed read is 503 or 500. Behind ReadOnce this is the org scope's read.
	role, standing, err := g.auth.tokenOrgMembership(r.Context(), user.UserID, user.OrgID)
	if err != nil {
		g.logger.ErrorContext(r.Context(), "api guard: membership lookup failed",
			slog.String("path", r.Pattern), slog.Bool("unavailable", isUnavailable(err)),
			slog.String("missing_grant", MissingGrant(err)))
		WriteAuthFailure(w, "", err)
		return nil, false
	}
	switch standing {
	case tokenOrgNoRow:
		writeOrgDenied(w)
		return nil, false
	case tokenOrgAsMain:
		countRoleSource(r.Context(), kindRoleClaimFallback)
		return user, true
	}
	countRoleSource(r.Context(), kindRoleMembershipRow)
	if role != user.Role {
		acting := *user
		acting.Role = role
		user = &acting
	}
	return user, true
}

// optionalUser is get_current_user_optional. It writes the refusal itself
// (store failures only); a nil user with ok=true is an anonymous caller.
func (g *Guard) optionalUser(w http.ResponseWriter, r *http.Request) (*User, bool) {
	token, ok := BearerToken(r.Header.Get("Authorization"))
	if !ok {
		return nil, true
	}
	user, err := g.auth.Authenticate(g.auth.routeCtx(r), token)
	switch {
	case err == nil:
		return user, true
	case isRefusal(err):
		return nil, true
	default:
		g.logger.ErrorContext(r.Context(), "api optional authentication: user lookup failed",
			slog.String("path", r.Pattern), slog.Bool("unavailable", isUnavailable(err)))
		authFailure(w, "", err)
		return nil, false
	}
}
