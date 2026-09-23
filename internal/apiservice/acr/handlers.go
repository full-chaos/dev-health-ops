package acr

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

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

// Routes is the acr area's route set. Both paths carry no Credentials/Authz/
// Tenant/Feature/Audit (R340): the internal_svc_acr_token bearer check and
// its audit trail (api/internal/acr.py) are not ported.
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

type entitlementResponse struct {
	SchemaVersion       string `json:"schema_version"`
	OrgID               string `json:"org_id"`
	AgentContextRuntime bool   `json:"agent_context_runtime"`
}

// healthHandler is unconditional and dependency-free: with the credential
// check removed (R340), the Python success body
// (ACRServiceHealthResponse's field defaults, api/internal/acr.py:40-46) was
// already static regardless of database state, so this route stays exactly
// as cheap.
func healthHandler() http.Handler {
	body, err := json.Marshal(healthResponse{
		SchemaVersion: healthSchemaVersion, Service: "dev-health-ops", Status: "ok",
	})
	if err != nil {
		panic("acr: static health body must marshal: " + err.Error())
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, body)
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
			body, marshalErr := json.Marshal(entitlementResponse{
				SchemaVersion:       entitlementSchemaVersion,
				OrgID:               entitlement.OrgID,
				AgentContextRuntime: entitlement.AgentContextRuntime,
			})
			if marshalErr != nil {
				logger.ErrorContext(r.Context(), "acr entitlement lookup: encode response failed",
					slog.String("org_id", orgID), slog.Any("error", marshalErr))
				writeDetail(w, http.StatusInternalServerError, "Internal Server Error")
				return
			}
			writeJSON(w, http.StatusOK, body)
		case errors.Is(err, ErrOrgNotFound):
			writeDetail(w, http.StatusNotFound, "Not found")
		case errors.Is(err, ErrUnavailable):
			logger.ErrorContext(r.Context(), "acr entitlement lookup: store unavailable",
				slog.String("org_id", orgID), slog.Any("error", err))
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
		default:
			logger.ErrorContext(r.Context(), "acr entitlement lookup: unclassified error",
				slog.String("org_id", orgID), slog.Any("error", err))
			writeDetail(w, http.StatusInternalServerError, "Internal Server Error")
		}
	})
}

// writeDetail renders {"detail": "<text>"}, the shape FastAPI's own
// HTTPException handler emits (Starlette's default exception handler:
// {"detail": exc.detail}) -- the exact bodies
// api/internal/acr.py:91,109,121,155,203 raise. acr's own client ignores
// every non-200 body (internal/entitlements/client.go: "if StatusCode !=
// 200 { return errUnavailable }"), so only the status code is load-bearing
// for that caller; the body shape is kept 1:1 for any other caller and for
// parity testing.
func writeDetail(w http.ResponseWriter, status int, detail string) {
	body, err := json.Marshal(map[string]string{"detail": detail})
	if err != nil {
		body, status = []byte(`{"detail":"Internal Server Error"}`), http.StatusInternalServerError
	}
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
