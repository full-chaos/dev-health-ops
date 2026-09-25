// Package customerpush serves the customer-push admin routes
// (api/admin/routers/customer_push.py) under
// /api/v1/admin/customer-push: source registration, ingest tokens, the
// batch status read proxies, the validate proxy and the schema
// passthrough. Every route sits behind the admin router's require_admin
// (policy.Admin) and then the customer_push_ingest access gate.
package customerpush

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const prefix = "/api/v1/admin/customer-push"

// Deps are the area's dependencies, built by apiservice.
type Deps struct {
	Pool   *pgxpool.Pool
	Guard  *policy.Guard
	Logger *slog.Logger
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
	// Getenv reads EXTERNAL_INGEST_MAX_RECORDS/_MAX_BODY_BYTES; nil means
	// os.LookupEnv. Python reads them per request.
	Getenv func(string) (string, bool)
	// Cipher reads a managed integration's credential payload for the
	// operational ownership check; nil is a process without a key.
	Cipher credentials.Cipher
}

type handlers struct {
	pool   *pgxpool.Pool
	guard  *policy.Guard
	logger *slog.Logger
	now    func() time.Time
	getenv func(string) (string, bool)
	cipher credentials.Cipher
}

// Routes returns the area's routes. A path's Allow value is the methods of
// the first route FastAPI declares for it, which Starlette reports on a
// 405.
func Routes(deps Deps) []httpapi.Route {
	h := &handlers{pool: deps.Pool, guard: deps.Guard, logger: deps.Logger, now: deps.Now, getenv: deps.Getenv, cipher: deps.Cipher}
	if h.logger == nil {
		h.logger = slog.Default()
	}
	if h.now == nil {
		h.now = time.Now
	}
	if h.getenv == nil {
		h.getenv = lookupEnv
	}
	admin := func(handler http.HandlerFunc) http.Handler { return h.guard.Wrap(policy.Admin, handler) }
	// A route with a pydantic body reads it before the admin dependency
	// (FastAPI's order); its validation errors come after it.
	adminBody := func(handler http.HandlerFunc) http.Handler { return h.guard.BodyFirst(policy.Admin, handler) }
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: prefix + "/sources", Handler: adminBody(h.createSource), Allow: "POST"},
		{Method: http.MethodGet, Pattern: prefix + "/sources", Handler: admin(h.listSources)},
		{Method: http.MethodGet, Pattern: prefix + "/sources/{source_id}", Handler: admin(h.getSource), Allow: "GET"},
		{Method: http.MethodPatch, Pattern: prefix + "/sources/{source_id}", Handler: adminBody(h.patchSource)},
		{Method: http.MethodGet, Pattern: prefix + "/sources/{source_id}/tokens", Handler: admin(h.listSourceTokens), Allow: "GET"},
		{Method: http.MethodPost, Pattern: prefix + "/sources/{source_id}/tokens", Handler: adminBody(h.createSourceToken)},
		{Method: http.MethodGet, Pattern: prefix + "/tokens", Handler: admin(h.listOrgTokens), Allow: "GET"},
		{Method: http.MethodPost, Pattern: prefix + "/tokens", Handler: adminBody(h.createOrgToken)},
		{Method: http.MethodPost, Pattern: prefix + "/tokens/{token_id}/rotate", Handler: admin(h.rotateToken), Allow: "POST"},
		{Method: http.MethodPost, Pattern: prefix + "/tokens/{token_id}/revoke", Handler: admin(h.revokeToken), Allow: "POST"},
		{Method: http.MethodPost, Pattern: prefix + "/sources/{source_id}/validate", Handler: admin(h.validateSource), Allow: "POST"},
		{Method: http.MethodGet, Pattern: prefix + "/sources/{source_id}/batches", Handler: admin(h.listSourceBatches), Allow: "GET"},
		{Method: http.MethodGet, Pattern: prefix + "/batches/{ingestion_id}", Handler: admin(h.getBatch), Allow: "GET"},
		{Method: http.MethodGet, Pattern: prefix + "/schemas", Handler: admin(h.listSchemas), Allow: "GET"},
		{Method: http.MethodGet, Pattern: prefix + "/schemas/{schema_version}", Handler: admin(h.getSchema), Allow: "GET"},
	}
}

// internal logs the failure with its operation and answers the generic 500.
func (h *handlers) internal(w http.ResponseWriter, r *http.Request, operation string, err error) {
	h.logger.ErrorContext(r.Context(), "customer-push admin: "+operation+" failed", slog.String("error", err.Error()), slog.String("path", r.URL.Path))
	policy.WriteInternal(w)
}
