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
	pdp    pdpHooks
}

// NewGuard builds a Guard over auth.
func NewGuard(auth *Authenticator, logger *slog.Logger) *Guard {
	if logger == nil {
		logger = slog.Default()
	}
	return &Guard{auth: auth, logger: logger}
}

// gate names the action a platform wrap decides by (zero gate: the generic wrap, org-less rule admin.orgs).
type gate struct {
	action string
	kind   gateKind
	deny   DenyText
	// late is the route's own checks that the legacy route answers before its handler's role check (FastAPI's 422, the
	// licence's 402): it runs after the guard's legacy checks and, for a request the decision point refuses, before that
	// refusal, so an enforced row keeps the legacy order of answers. It reports whether the request may go on; it has
	// written the answer when it did not. nil is none.
	late func(http.ResponseWriter, *http.Request) bool
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
				r = g.userRequest(r, user)
			}
			next.ServeHTTP(w, r)
		}))
	}
	if gt.late != nil {
		inner := next
		next = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if gt.late(w, r) {
				inner.ServeHTTP(w, r)
			}
		})
	}
	return countGuardLevel(level, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := g.currentUser(w, r, level, gt)
		if !ok {
			return
		}
		// The shadow call sees the caller as the legacy decision did, before an org swap, and
		// never changes the answer.
		caller := user
		bearer := ""
		if g.pdp.shadow != nil || g.pdp.enforcer != nil {
			bearer, _ = BearerToken(r.Header.Get("Authorization"))
		}
		// With the admin cell on, the handler's platform questions go to the decision point too (HoldsPlatform).
		platform := g.pdp.enforcer.platformDecisionsFor(r, bearer, user)
		if platform != nil {
			r = r.WithContext(context.WithValue(r.Context(), platformDecisionsKey, platform))
		}
		// A route whose namespace is switched on is decided by the decision point, not by the legacy checks below.
		switch verdict := g.enforceVerdict(r, user, bearer, level, level == Superuser && gt.kind == gatePlatform, gt, platform); verdict {
		case enforceDeny, enforceUnavailable, enforceNoDecisionPoint, enforceImpersonationRefused, enforceSessionRowUnavailable, enforceDenyNoOrg:
			if gt.late != nil && decisionPointRefusal(verdict) && !gt.late(w, r) {
				return
			}
			writeEnforceRefusal(w, r, g.pdp.enforcer.refusalLevel(r.Pattern, level), verdict, gt.deny)
			return
		case enforceAllow:
			next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), user)))
			return
		}
		// With the admin cell on, the action of an admin-or or platform wrap is the decision point's answer for a caller
		// it can judge, and go-api's own set otherwise; every other check is main's.
		holds := func(action string) (bool, error) {
			if platform == nil {
				return user.Holds(action), nil
			}
			return platform.holds(r.Context(), action)
		}
		refuse := func(err error) {
			g.observeShadow(r, caller, bearer, false, gt, level)
			WritePlatformDecisionRefusal(w, err)
		}
		if user.OrgID == "" && !user.Holds(gt.orglessAction()) && !OrglessMayReach(r.Method, r.URL.Path) {
			g.observeShadow(r, caller, bearer, false, gt, level)
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
			admitted := user.IsAdmin()
			if !admitted && gt.kind == gateAdminOr {
				held, err := holds(gt.action)
				if err != nil {
					refuse(err)
					return
				}
				admitted = held
			}
			if !admitted {
				g.observeShadow(r, caller, bearer, false, gt, level)
				WriteDetail(w, http.StatusForbidden, "Admin access required", nil)
				return
			}
			if level == AdminOrg && user.OrgID == "" {
				g.observeShadow(r, caller, bearer, false, gt, level)
				WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
				return
			}
		case Superuser:
			held := false
			if gt.kind == gatePlatform {
				var err error
				if held, err = holds(gt.action); err != nil {
					refuse(err)
					return
				}
			}
			if !held {
				g.observeShadow(r, caller, bearer, false, gt, level)
				gt.deny.refused(r)
				WriteDetail(w, http.StatusForbidden, gt.deny.denied("Superuser access required"), nil)
				return
			}
			if !liveRowExempt(r.Pattern) {
				session, err := g.sessionRow(r, user.ID)
				if err != nil {
					// The session state is unknown: the shadow is not asked.
					g.writeSessionRowFailure(w, r, err)
					return
				}
				if session != nil {
					g.logger.WarnContext(r.Context(), "api guard: platform action refused in an impersonation session",
						slog.String("path", r.Pattern), slog.String("action", gt.action))
					g.observeShadowAs(r, caller, bearer, false, true, gt, level)
					gt.deny.refused(r)
					WriteDetail(w, http.StatusForbidden, gt.deny.impersonated(), nil)
					return
				}
			}
		}
		g.observeShadow(r, caller, bearer, true, gt, level)
		next.ServeHTTP(w, g.userRequest(r, user))
	}))
}

// WrapLate is Wrap with late: the route's checks that the legacy route answers before its role check run before an enforced
// refusal too (see gate.late).
func (g *Guard) WrapLate(level Authz, late func(http.ResponseWriter, *http.Request) bool, next http.Handler) http.Handler {
	return g.wrap(level, gate{late: late}, next)
}

// BodyFirstLate is BodyFirst over WrapLate.
func (g *Guard) BodyFirstLate(level Authz, late func(http.ResponseWriter, *http.Request) bool, next http.Handler) http.Handler {
	return g.bodyFirstOver(g.WrapLate(level, late, next))
}

// WrapPlatform returns next behind the platform gate of action: only a caller
// that holds action passes.
func (g *Guard) WrapPlatform(action string, next http.Handler) http.Handler {
	return g.wrap(Superuser, gate{action: action, kind: gatePlatform}, next)
}

// WrapAdminOr returns next behind Admin, which a caller that holds action also
// passes.
func (g *Guard) WrapAdminOr(action string, next http.Handler) http.Handler {
	return g.wrap(Admin, gate{action: action, kind: gateAdminOr}, next)
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
	if g.RefuseDelegated(w, r, user) {
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
		if g.RefuseDelegated(w, r, user) {
			return nil, false
		}
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

// msgImpersonationDenied is the detail of a platform wrap's refusal of a
// request that holds an impersonation session.
const msgImpersonationDenied = "Not allowed in an impersonation session"

// stopImpersonationPattern is the platform route that ends an impersonation
// session.
const stopImpersonationPattern = "POST /api/v1/admin/impersonate/stop"

// RefuseImpersonatingCaller returns next behind the impersonation write
// refusal (403) for a caller whose bearer, authenticated as the route's row
// does (the user access token), belongs to an admin with an active
// impersonation session. It is for a route the scope layers
// skip, so it reads the session itself. It follows the live row, not the
// caller's admin.impersonate. A bearer go-api cannot verify (auth-service's)
// passes through. A failed read (users row or session) answers 503 and does
// not forward (fail closed).
func (g *Guard) RefuseImpersonatingCaller(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := BearerToken(r.Header.Get("Authorization"))
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		user, err := g.auth.Authenticate(g.auth.routeCtx(r), token)
		if isRefusal(err) {
			next.ServeHTTP(w, r)
			return
		}
		var session *Impersonation
		if err == nil {
			session, err = g.auth.activeImpersonation(r.Context(), user.ID)
		}
		switch {
		case err != nil:
			g.logger.ErrorContext(r.Context(), "api guard: impersonation lookup failed; refusing the request",
				slog.Bool("unavailable", isUnavailable(err)), slog.Any("error", err))
			WriteAuthFailure(w, "", ErrUnavailable)
		case session != nil:
			g.logger.WarnContext(r.Context(), "api guard: route refused in an impersonation session",
				slog.String("path", r.Pattern), slog.String("admin_user_id", session.AdminUserID.String()))
			WriteDetail(w, http.StatusForbidden, msgImpersonationReadOnly, nil)
		default:
			next.ServeHTTP(w, r)
		}
	})
}

// ImpersonationHandoffConsumePattern is the platform route that admits the
// start handoff of the caller's own impersonation session, once. The session
// row is live when it is called.
const ImpersonationHandoffConsumePattern = "POST /api/v1/admin/impersonate/handoff/consume"

// statusImpersonationPattern is the route that reads the caller's impersonation
// session.
const statusImpersonationPattern = "GET /api/v1/admin/impersonate/status"

// liveRowExemptPatterns are the routes that stay reachable while the caller's
// impersonation row is live: the admin's own bearer reads the session, admits
// its start handoff and ends it. Every live-row refusal, the scope's legacy
// refusal included, reads this one set through liveRowExempt. Status takes no
// platform action and is never enforced, so only the scope's refusal meets it.
var liveRowExemptPatterns = map[string]bool{statusImpersonationPattern: true, stopImpersonationPattern: true, ImpersonationHandoffConsumePattern: true}

func liveRowExempt(pattern string) bool { return liveRowExemptPatterns[pattern] }
