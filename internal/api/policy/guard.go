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

// Wrap returns next behind level. A refused caller never reaches next.
func (g *Guard) Wrap(level Authz, next http.Handler) http.Handler {
	if level == Public {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := g.currentUser(w, r)
		if !ok {
			return
		}
		switch level {
		case Admin, AdminOrg:
			if !user.IsAdmin() {
				WriteDetail(w, http.StatusForbidden, "Admin access required", nil)
				return
			}
			if level == AdminOrg && user.OrgID == "" {
				WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
				return
			}
		case Superuser:
			if !user.IsSuperuser {
				WriteDetail(w, http.StatusForbidden, "Superuser access required", nil)
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), user)))
	})
}

// currentUser is get_current_user. It writes the refusal itself.
func (g *Guard) currentUser(w http.ResponseWriter, r *http.Request) (*User, bool) {
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
	user, err := g.auth.Authenticate(r.Context(), token)
	if err != nil {
		if !isRefusal(err) {
			g.logger.ErrorContext(r.Context(), "api authentication: user lookup failed",
				slog.String("path", r.URL.Path), slog.Bool("unavailable", isUnavailable(err)))
		}
		authFailure(w, "Invalid or expired token", err)
		return nil, false
	}
	return user, true
}
