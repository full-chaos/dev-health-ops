// Package acr serves the internal routes acr's entitlement client calls
// directly (bypassing ingress): GET /api/v1/internal/acr/health,
// GET /api/v1/internal/acr/entitlements/{org_id} and the org llm-config
// route. All are reachable only inside the cluster network (deploy/helm/dev-health's
// TestInternalACRRoutesStayOffThePublicIngress pins that no Ingress routes to
// the Go api) and every one also checks a svc_acr_ bearer. Health and
// entitlements port api/internal/acr.py (CHAOS-9993): 100/minute per client
// address (429), the credential looked up by token hash (401 unknown or
// inactive, 403 without entitlements:read), and on the entitlement route
// last_used_at plus one internal_service_credential_audits row per outcome.
// CHAOS-7181: apiservice mounts these routes on the internal listener only
// (apiservice.InternalRoutes); the public api listener has none. The
// agent_context_runtime entitlement decision itself is ported in full via
// internal/api/licensing, this package's only dependency for it.
//
// Entitlement route answers (acr's client depends on these):
//   - 200 grants body: the org exists.
//   - 404 {"detail":"Not found","code":"acr_organization_unknown"}: the id is
//     a well-formed UUID and no organizations row exists (deleted or never
//     created). The only answer a caller may read as "organization gone".
//   - 404 {"detail":"Not found"} (no code): malformed id. Not "gone".
//   - 503 {"detail":"Service unavailable"}: the store cannot be read.
//
// Contract: .github/docs-legacy/architecture/acr-entitlement-route.md
package acr

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Deps is this area's dependency set. A nil Store means the api Service was
// started without APIDatabaseURI configured (CHAOS-6269 not yet rolled out
// for this deployment): Routes still mounts both paths, but the entitlement
// route answers 503 rather than reaching a nil pool.
type Deps struct {
	Store  EntitlementStore
	Logger *slog.Logger
	// LLMConfig and Credentials serve the llm-config route. Either nil makes
	// that route answer 503; it never serves unauthenticated.
	LLMConfig   LLMConfigSource
	Credentials CredentialVerifier
	// Auth and Limiter guard the health and entitlement routes with the
	// svc_acr_ bearer check (api/internal/acr.py). A nil Auth makes them
	// answer 503; they never serve unauthenticated. A nil Limiter does not
	// rate limit.
	Auth    CredentialStore
	Limiter *httpapi.KeyedLimiter
}

// Routes is the acr area's route set. Every path is internal and checks its
// own svc_acr_ bearer (routeAuth), so none carries the public Credentials/
// Authz/Tenant/Feature/Audit middleware.
func Routes(deps Deps) []httpapi.Route {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	deps.Limiter.Declare()
	return []httpapi.Route{
		{
			Method: http.MethodGet, Pattern: "/api/v1/internal/acr/health",
			Handler: healthHandler(deps.Store, routeAuth{store: deps.Auth, limiter: deps.Limiter, logger: logger, route: routeHealth}, logger),
		},
		{
			Method: http.MethodGet, Pattern: "/api/v1/internal/acr/entitlements/{org_id}",
			Handler: entitlementHandler(deps.Store, routeAuth{
				store: deps.Auth, limiter: deps.Limiter, logger: logger, route: routeEntitlement, audited: true,
			}, logger),
		},
		{
			Method: http.MethodGet, Pattern: "/api/v1/internal/acr/orgs/{org_id}/llm-config",
			Handler: llmConfigHandler(deps.Credentials, deps.LLMConfig, logger),
		},
	}
}

// healthSchemaVersion/entitlementSchemaVersion pin the two response shapes
// acr's client decoder requires exactly (internal/entitlements/response.go
// in the acr repo rejects an unknown field and requires every declared one).
const (
	healthSchemaVersion      = "acr_service_health.v1"
	entitlementSchemaVersion = "acr_entitlement.v1"
)

type healthResponse struct {
	SchemaVersion string `json:"schema_version"`
	Service       string `json:"service"`
	Status        string `json:"status"`
}

// healthHandler answers whether the entitlement service is usable, as
// acr's client reads it. Python's health route reads Postgres on every call
// (its credential lookup, api/internal/acr.py:93-103) and answers 503
// "Service unavailable" when that read fails, so the Go route makes the
// entitlement store's decision read (EntitlementStore.Ready): a nil store
// or a failed read is that 503, anything else the static success body. The bearer is checked first (routeAuth).
func healthHandler(store EntitlementStore, auth routeAuth, logger *slog.Logger) http.Handler {
	response := healthResponse{
		SchemaVersion: healthSchemaVersion, Service: "dev-health-ops", Status: "ok",
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := auth.authenticate(w, r, ""); !ok {
			return
		}
		if store == nil {
			logger.ErrorContext(r.Context(), "acr health: no entitlement store configured")
			countInternal(r.Context(), routeHealth, "unconfigured")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		if err := store.Ready(r.Context()); err != nil {
			logger.ErrorContext(r.Context(), "acr health: entitlement store unreachable", slog.Any("error", err))
			countInternal(r.Context(), routeHealth, "store_unavailable")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		body := pyjson.NewObject()
		body.Set("schema_version", response.SchemaVersion)
		body.Set("service", response.Service)
		body.Set("status", response.Status)
		countInternal(r.Context(), routeHealth, outcomeSuccess)
		writeModel(w, http.StatusOK, body)
	})
}

// UnknownOrganizationCode is the stable machine code on the entitlement
// route's 404 for a well-formed org id that has no organization row. acr
// treats only a 404 carrying this code as "organization deleted"; any other
// 404 (malformed id, unrouted path, proxy) is not.
const UnknownOrganizationCode = "acr_organization_unknown"

func entitlementHandler(store EntitlementStore, auth routeAuth, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		orgID := r.PathValue("org_id")
		cred, ok := auth.authenticate(w, r, orgID)
		if !ok {
			return
		}
		// finish writes the audit row for outcome before the answer, as
		// Python commits it before returning; an audit that cannot be
		// written is 503, never an unaudited answer.
		finish := func(outcome string, write func()) {
			if !auth.audit(ctx, cred.ID, orgID, outcome) {
				auth.unavailable(w, r, "audit_failed")
				return
			}
			countInternal(ctx, routeEntitlement, outcome)
			logger.InfoContext(ctx, "acr entitlement lookup: answered",
				slog.String("org_id", orgID), slog.String("credential_id", cred.ID), slog.String("outcome", outcome))
			write()
		}
		if store == nil {
			logger.ErrorContext(ctx, "acr entitlement lookup: no entitlement store configured",
				slog.String("org_id", orgID))
			finish(outcomeLookupFailed, func() { writeDetail(w, http.StatusServiceUnavailable, "Service unavailable") })
			return
		}
		entitlement, err := store.Lookup(ctx, orgID)
		switch {
		case err == nil:
			finish(outcomeSuccess, func() {
				body := pyjson.NewObject()
				body.Set("schema_version", entitlementSchemaVersion)
				body.Set("org_id", entitlement.OrgID)
				body.Set("agent_context_runtime", entitlement.AgentContextRuntime)
				body.Set("acr_service_model", entitlement.ACRServiceModel)
				body.Set("platform_model_fallback", entitlement.PlatformModelFallback)
				writeModel(w, http.StatusOK, body)
			})
		case errors.Is(err, ErrOrgUnknown):
			finish(outcomeOrgNotFound, func() {
				body := pyjson.NewObject()
				body.Set("detail", "Not found")
				body.Set("code", UnknownOrganizationCode)
				writeModel(w, http.StatusNotFound, body)
			})
		case errors.Is(err, ErrOrgNotFound):
			finish(outcomeOrgNotFound, func() { writeDetail(w, http.StatusNotFound, "Not found") })
		case errors.Is(err, ErrUnavailable):
			logger.ErrorContext(ctx, "acr entitlement lookup: store unavailable",
				slog.String("org_id", orgID), slog.Any("error", err))
			finish(outcomeLookupFailed, func() { writeDetail(w, http.StatusServiceUnavailable, "Service unavailable") })
		default:
			logger.ErrorContext(ctx, "acr entitlement lookup: unclassified error",
				slog.String("org_id", orgID), slog.Any("error", err))
			finish(outcomeLookupFailed, func() { policy.WriteInternal(w) })
		}
	})
}

// writeModel writes a success body as FastAPI writes the route's
// response_model (policy.WriteModel), keeping this route family's nosniff
// header.
func writeModel(w http.ResponseWriter, status int, body *pyjson.Object) {
	policy.WriteModel(w, status, body, http.Header{"X-Content-Type-Options": {"nosniff"}})
}

// writeDetail renders {"detail": "<text>"}, the body FastAPI's HTTPException
// handler writes (through JSONResponse, so policy.WriteDetail) for the
// statuses api/internal/acr.py raises. acr's own client treats every
// non-200 as unavailable, so only the status is load-bearing for it; the
// body is kept byte-identical for any other caller and for the venue.
func writeDetail(w http.ResponseWriter, status int, detail string) {
	policy.WriteDetail(w, status, detail, http.Header{"X-Content-Type-Options": {"nosniff"}})
}
