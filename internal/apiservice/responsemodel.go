package apiservice

import "github.com/full-chaos/dev-health-ops/internal/auth/httpapi"

// responseModelRoutes are the routes whose Python counterpart writes its
// success body as a FastAPI response_model: the route declares a
// response_model (or a return annotation FastAPI takes as one) and keeps
// the default response class, so FastAPI writes the body with
// pydantic-core dump_json, not json.dumps. Their handlers write success
// bodies with policy.WriteModel; every other route writes with
// policy.WriteJSON. TestRouteResponseModelsMatchFastAPI pins this table
// against the live FastAPI app.
var responseModelRoutes = map[string]bool{
	"DELETE /api/v1/admin/orgs/{org_id}":                            true,
	"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}":          true,
	"DELETE /api/v1/admin/teams/{team_id}":                          true,
	"DELETE /api/v1/admin/users/{user_id}":                          true,
	"GET /api/v1/admin/customer-push/batches/{ingestion_id}":        true,
	"GET /api/v1/admin/customer-push/schemas":                       true,
	"GET /api/v1/admin/customer-push/schemas/{schema_version}":      true,
	"GET /api/v1/admin/customer-push/sources":                       true,
	"GET /api/v1/admin/customer-push/sources/{source_id}":           true,
	"GET /api/v1/admin/customer-push/sources/{source_id}/batches":   true,
	"GET /api/v1/admin/customer-push/sources/{source_id}/tokens":    true,
	"GET /api/v1/admin/customer-push/tokens":                        true,
	"GET /api/v1/admin/identities":                                  true,
	"GET /api/v1/admin/impersonate/status":                          true,
	"GET /api/v1/admin/orgs":                                        true,
	"GET /api/v1/admin/orgs/{org_id}":                               true,
	"GET /api/v1/admin/orgs/{org_id}/members":                       true,
	"GET /api/v1/admin/sync-configs":                                true,
	"GET /api/v1/admin/sync-configs/auto-import-capabilities":       true,
	"GET /api/v1/admin/sync-configs/{config_id}":                    true,
	"GET /api/v1/admin/sync-configs/{config_id}/jobs":               true,
	"GET /api/v1/admin/sync-configs/{config_id}/repositories":       true,
	"GET /api/v1/admin/sync-runs/{run_id}":                          true,
	"GET /api/v1/admin/sync-targets":                                true,
	"GET /api/v1/admin/teams":                                       true,
	"GET /api/v1/admin/teams/{team_id}":                             true,
	"GET /api/v1/admin/users":                                       true,
	"GET /api/v1/admin/users/{user_id}":                             true,
	"GET /api/v1/external-ingest/availability":                      true,
	"GET /api/v1/external-ingest/batches":                           true,
	"GET /api/v1/external-ingest/batches/{ingestion_id}":            true,
	"GET /api/v1/external-ingest/schemas":                           true,
	"GET /api/v1/external-ingest/schemas/{schema_version}":          true,
	"GET /api/v1/internal/acr/entitlements/{org_id}":                true,
	"GET /api/v1/internal/acr/health":                               true,
	"GET /api/v1/licensing/entitlements/{org_id}":                   true,
	"GET /api/v1/orgs/me":                                           true,
	"GET /api/v1/telemetry/status":                                  true,
	"GET /api/v1/webhooks/health":                                   true,
	"GET /health":                                                   true,
	"HEAD /health":                                                  true,
	"PATCH /api/v1/admin/customer-push/sources/{source_id}":         true,
	"PATCH /api/v1/admin/orgs/{org_id}":                             true,
	"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}":           true,
	"PATCH /api/v1/admin/teams/{team_id}":                           true,
	"PATCH /api/v1/admin/users/{user_id}":                           true,
	"PATCH /api/v1/orgs/me":                                         true,
	"POST /api/v1/admin/customer-push/sources":                      true,
	"POST /api/v1/admin/customer-push/sources/{source_id}/tokens":   true,
	"POST /api/v1/admin/customer-push/sources/{source_id}/validate": true,
	"POST /api/v1/admin/customer-push/tokens":                       true,
	"POST /api/v1/admin/customer-push/tokens/{token_id}/revoke":     true,
	"POST /api/v1/admin/customer-push/tokens/{token_id}/rotate":     true,
	"POST /api/v1/admin/identities":                                 true,
	"POST /api/v1/admin/impersonate":                                true,
	"POST /api/v1/admin/impersonate/stop":                           true,
	"POST /api/v1/admin/orgs":                                       true,
	"POST /api/v1/admin/orgs/{org_id}/members":                      true,
	"POST /api/v1/admin/teams":                                      true,
	"POST /api/v1/admin/users":                                      true,
	"POST /api/v1/admin/users/{user_id}/password":                   true,
	"POST /api/v1/external-ingest/validate":                         true,
	"POST /api/v1/product-telemetry/events":                         true,
	"POST /api/v1/telemetry/opt-in":                                 true,
	"POST /api/v1/telemetry/opt-out":                                true,
	"POST /api/v1/telemetry/report":                                 true,
	"POST /api/v1/webhooks/github":                                  true,
	"POST /api/v1/webhooks/gitlab":                                  true,
	"POST /api/v1/webhooks/jira":                                    true,
}

// jsonResponseRoutes are response_model routes on the FastAPI side whose
// endpoint returns a JSONResponse on every success path, so FastAPI writes
// those bodies with json.dumps after all. Each entry names why;
// TestRouteResponseModelsMatchFastAPI requires each still to be a
// response_model route there, so a stale entry fails.
var jsonResponseRoutes = map[string]string{
	"POST /api/v1/external-ingest/batches": "accept_batch returns JSONResponse: 202 on acceptance, 200 from _replay_status_response on an idempotent replay",
}

// markResponseModels sets each route's ResponseModel from
// responseModelRoutes.
func markResponseModels(routes []httpapi.Route) []httpapi.Route {
	for index := range routes {
		routes[index].ResponseModel = responseModelRoutes[routes[index].Method+" "+routes[index].Pattern]
	}
	return routes
}
