package authapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/social"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// Compatibility route patterns: go-api's own paths.
const (
	PatternCompatLogin       = "/api/v1/auth/login"
	PatternCompatRefresh     = "/api/v1/auth/refresh"
	PatternCompatLogout      = "/api/v1/auth/logout"
	PatternCompatSocialLogin = "/api/v1/auth/social-login"
)

const opSocialLogin = "social_login"

// CompatSessions is the session domain the compatibility routes drive
// (*session.Store).
type CompatSessions interface {
	Sessions
	RevokeByCredential(ctx context.Context, credential contracts.SecretString, actor session.Actor) (bool, error)
	CreateAudited(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string, event audit.AuditEvent) (session.Issued, error)
}

// Projector makes the auth schema's principal and organization rows agree
// with go-api's before a session or an audit row names them
// (*identity.Store).
type Projector interface {
	Project(ctx context.Context, userID uuid.UUID) error
	ProjectOrganization(ctx context.Context, orgID uuid.UUID) error
}

// LoginConfig mounts the local login route.
type LoginConfig struct {
	Users    local.Users
	Attempts local.Attempts
	// LockoutKey is AUTH_LOGIN_LOCKOUT_KEY (local.Config.LockoutKey).
	LockoutKey contracts.SecretString
	// Compare is the password check; nil means bcrypt. Tests count its calls.
	Compare func(hash, password string) bool
}

// CompatDeps are the compatibility routes' collaborators.
type CompatDeps struct {
	Sessions CompatSessions
	Users    Users
	Issuer   *Issuer
	Logger   *slog.Logger
	// Meter registers the outcome counter; nil uses the process meter.
	Meter     metric.Meter
	Projector Projector
	// AppendAudit writes an append-only audit row (audit.Append).
	AppendAudit func(ctx context.Context, event audit.AuditEvent) error
	// Login mounts POST /api/v1/auth/login; nil leaves it unmounted.
	Login *LoginConfig
	// Social mounts POST /api/v1/auth/social-login; nil leaves it unmounted.
	Social *social.Service
}

type compatHandlers struct {
	CompatDeps
	rec   recorder
	login *local.Service
}

// CompatRoutes is not built yet: it mounts the routes with stub handlers.
func CompatRoutes(deps CompatDeps) ([]httpapi.Route, error) {
	h := compatHandlers{CompatDeps: deps}
	routes := []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternCompatRefresh, Handler: http.HandlerFunc(notBuilt)},
		{Method: http.MethodPost, Pattern: PatternCompatLogout, Handler: http.HandlerFunc(notBuilt)},
	}
	if deps.Login != nil {
		routes = append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternCompatLogin, Handler: http.HandlerFunc(notBuilt)})
	}
	if deps.Social != nil {
		routes = append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternCompatSocialLogin, Handler: http.HandlerFunc(notBuilt)})
	}
	_ = h
	return routes, nil
}

func notBuilt(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotImplemented) }
