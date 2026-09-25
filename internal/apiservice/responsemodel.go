package apiservice

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// responseModelRoutes says, for each Python route the dho api serves,
// whether FastAPI writes its success body as a response_model: the route
// declares a response_model (or a return annotation FastAPI takes as one)
// and keeps the default response class, so FastAPI writes the body with
// pydantic-core dump_json, not json.dumps. Keys are the Go route's
// "METHOD pattern", plus the literal paths a wildcard route dispatches
// itself (GET /teams/discover, GET /teams/pending-changes, POST
// /teams/import, POST /ip-allowlist/check). Handlers write a true route's success bodies with
// policy.WriteModel and every other body with policy.WriteJSON.
// TestVenueOracleRouteResponseModels pins this table against the live
// FastAPI app.
//
// The table is filled by the per-family files (responsemodel_<family>.go),
// each of which registers its own routes through registerResponseModelRoutes
// from its init() (see responsemodel_register.go, CHAOS-6722), so a PR that
// adds a route touches only its family's file. Do not add entries to this
// declaration.
var responseModelRoutes = map[string]bool{}

// jsonResponseRoutes are response_model routes on the FastAPI side whose
// endpoint returns a JSONResponse on every success path, so FastAPI writes
// those bodies with json.dumps after all (false in responseModelRoutes).
// Each entry names why; the registry test requires each still to be a
// response_model route there, so a stale entry fails.
var jsonResponseRoutes = map[string]string{
	"POST /api/v1/external-ingest/batches": "accept_batch returns JSONResponse: 202 on acceptance, 200 from _replay_status_response on an idempotent replay",
}

// markResponseModels sets each route's ResponseModel from
// responseModelRoutes, resolved per request: a literal path the table
// names (a wildcard route's dispatched literal) takes its own entry, and
// any other path takes the route pattern's.
func markResponseModels(routes []httpapi.Route) []httpapi.Route {
	for index := range routes {
		key := routes[index].Method + " " + routes[index].Pattern
		if _, inTable := responseModelRoutes[key]; !inTable && routes[index].ResponseModelFor != nil {
			// A dispatcher pattern with no Python route of its own (see
			// goRoutesWithoutPython) that decides per request itself.
			continue
		}
		routes[index].ResponseModel = responseModelRoutes[key]
		routes[index].ResponseModelFor = func(r *http.Request) bool {
			if model, literal := responseModelRoutes[r.Method+" "+r.URL.Path]; literal {
				return model
			}
			return responseModelRoutes[key]
		}
	}
	return routes
}
