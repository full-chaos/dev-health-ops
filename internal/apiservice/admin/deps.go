// Package admin serves dho api's org, user, invite and impersonation admin
// routes (CHAOS-6250): api/admin/routers/orgs.py, api/admin/routers/users.py
// and api/admin/impersonation.py, ported route for route. Every route here
// authenticates and authorizes through internal/api/policy (Guard, Scope);
// this package never re-checks a bearer token or re-derives the request's
// org itself.
package admin

import (
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Deps is this area's dependency set, built from the api Service's shared
// Postgres pool, Valkey client and protected-route runtime -- see
// apiservice.Deps's own doc comment for why an area package never opens its
// own pool or client.
type Deps struct {
	Pool   *pgxpool.Pool
	Valkey valkeygo.Client
	Guard  *policy.Guard
	Logger *slog.Logger
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

// Routes is the admin area's route set.
func Routes(deps Deps) []httpapi.Route {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	store := pgStore{Pool: deps.Pool, Now: deps.Now}
	auditWriter := audit.PGWriter{Pool: deps.Pool, Now: deps.Now}
	cache := newImpersonationCache(deps.Valkey, store, logger)
	if deps.Now != nil {
		cache.now = deps.Now
	}
	area := &handlers{
		store:  store,
		audit:  auditWriter,
		cache:  cache,
		guard:  deps.Guard,
		logger: logger,
	}
	return area.routes()
}

// routes is this area's full route set: orgs, users, impersonation.
func (h *handlers) routes() []httpapi.Route {
	var out []httpapi.Route
	out = append(out, h.orgRoutes()...)
	out = append(out, h.userRoutes()...)
	out = append(out, h.impersonationRoutes()...)
	return out
}

// handlers holds the built dependencies every admin route handler closes
// over.
type handlers struct {
	store  pgStore
	audit  audit.Writer
	cache  *impersonationCache
	guard  *policy.Guard
	logger *slog.Logger
}
