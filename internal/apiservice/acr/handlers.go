// Package acr serves the two internal routes acr's entitlement client calls
// directly (bypassing ingress): GET /api/v1/internal/acr/health and
// GET /api/v1/internal/acr/entitlements/{org_id}. Both routes are reachable
// only inside the cluster network, so neither carries the bearer/mint check
// or audit trail the Python routes had (api/internal/acr.py's credential
// lookup and its InternalServiceCredentialAudit rows are not ported) -- the
// network boundary is the control, not a per-request token
// (deploy/helm/dev-health's TestInternalACRRoutesStayOffThePublicIngress
// pins that no Ingress routes to the Go api). The
// agent_context_runtime entitlement decision itself is ported in full via
// internal/api/licensing, this package's only dependency for it.
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
// for this deployment): Routes still mounts both paths -- the ingress path
// table switch (spec.md §4.6), not process configuration, decides whether
// any traffic reaches them -- but the entitlement route answers 503 rather
// than reaching a nil pool.
type Deps struct {
	Store  EntitlementStore
	Logger *slog.Logger
}

// Routes is the acr area's route set. Both paths are internal, reachable
// only inside the cluster network, so neither carries Credentials/Authz/
// Tenant/Feature/Audit: the internal_svc_acr_token bearer check and its
// audit trail (api/internal/acr.py) are not ported -- the network boundary
// is the control, not a per-request token.
func Routes(deps Deps) []httpapi.Route {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/api/v1/internal/acr/health", Handler: healthHandler()},
		{
			Method: http.MethodGet, Pattern: "/api/v1/internal/acr/entitlements/{org_id}",
			Handler: entitlementHandler(deps.Store, logger),
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

// healthHandler is unconditional and dependency-free: with no credential
// check on this route, the Python success body
// (ACRServiceHealthResponse's field defaults, api/internal/acr.py:40-46) was
// already static regardless of database state, so this route stays exactly
// as cheap.
func healthHandler() http.Handler {
	response := healthResponse{
		SchemaVersion: healthSchemaVersion, Service: "dev-health-ops", Status: "ok",
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := pyjson.NewObject()
		body.Set("schema_version", response.SchemaVersion)
		body.Set("service", response.Service)
		body.Set("status", response.Status)
		writeModel(w, http.StatusOK, body)
	})
}

func entitlementHandler(store EntitlementStore, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		orgID := r.PathValue("org_id")
		if store == nil {
			logger.ErrorContext(r.Context(), "acr entitlement lookup: no entitlement store configured",
				slog.String("org_id", orgID))
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		entitlement, err := store.Lookup(r.Context(), orgID)
		switch {
		case err == nil:
			body := pyjson.NewObject()
			body.Set("schema_version", entitlementSchemaVersion)
			body.Set("org_id", entitlement.OrgID)
			body.Set("agent_context_runtime", entitlement.AgentContextRuntime)
			writeModel(w, http.StatusOK, body)
		case errors.Is(err, ErrOrgNotFound):
			writeDetail(w, http.StatusNotFound, "Not found")
		case errors.Is(err, ErrUnavailable):
			logger.ErrorContext(r.Context(), "acr entitlement lookup: store unavailable",
				slog.String("org_id", orgID), slog.Any("error", err))
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
		default:
			logger.ErrorContext(r.Context(), "acr entitlement lookup: unclassified error",
				slog.String("org_id", orgID), slog.Any("error", err))
			policy.WriteInternal(w)
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
