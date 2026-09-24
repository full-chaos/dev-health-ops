package server

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// responseModelRoutes names every query-api "METHOD pattern" whose FastAPI
// route writes its success body through the response_model fast path
// (fastapi/routing.py use_dump_json: a response field and the default
// response class), which pydantic-core's dump_json renders. Those routes'
// success bodies go through writeModelResponse. The live registry test
// (TestQueryAPIResponseModelsMatchLiveFastAPI) fails when this table and
// the FastAPI app disagree.
var responseModelRoutes = map[string]bool{
	"GET /api/v1/meta":                                true,
	"GET /api/v1/home":                                true,
	"POST /api/v1/home":                               true,
	"GET /api/v1/explain":                             true,
	"POST /api/v1/explain":                            true,
	"GET /api/v1/heatmap":                             true,
	"GET /api/v1/work-units":                          true,
	"POST /api/v1/work-units":                         true,
	"POST /api/v1/work-units/{work_unit_id}/explain":  true,
	"GET /api/v1/flame":                               true,
	"GET /api/v1/flame/aggregated":                    true,
	"GET /api/v1/quadrant":                            true,
	"GET /api/v1/drilldown/prs":                       true,
	"POST /api/v1/drilldown/prs":                      true,
	"GET /api/v1/drilldown/issues":                    true,
	"POST /api/v1/drilldown/issues":                   true,
	"GET /api/v1/people":                              true,
	"GET /api/v1/people/{person_id}/summary":          true,
	"GET /api/v1/people/{person_id}/metric":           true,
	"GET /api/v1/people/{person_id}/drilldown/prs":    true,
	"GET /api/v1/people/{person_id}/drilldown/issues": true,
	"GET /api/v1/opportunities":                       true,
	"POST /api/v1/opportunities":                      true,
	"GET /api/v1/investment":                          true,
	"POST /api/v1/investment":                         true,
	"GET /api/v1/investment/sunburst":                 true,
	"POST /api/v1/investment/explain":                 true,
	"POST /api/v1/investment/flow":                    true,
	"POST /api/v1/investment/flow/repo-team":          true,
	"GET /api/v1/sankey":                              true,
	"POST /api/v1/sankey":                             true,
	"GET /api/v1/filters/options":                     true,
}

// markResponseModelRoutes hands every request's writer its route's
// response_model flag: the pattern the mux matched, with the method.
func markResponseModelRoutes(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		key := r.Method + " " + pattern
		serveModelRoute(mux, key, responseModelRoutes[key], w, r)
	})
}

// serveModelRoute serves r with a writer carrying the route's flag, and
// counts a 2xx success body on a response_model route that did not go
// through writeModelResponse (a writer left on encoding/json): the Python
// api writes that body with pydantic-core, so the bytes differ.
func serveModelRoute(handler http.Handler, key string, responseModel bool, w http.ResponseWriter, r *http.Request) {
	routeWriter := httpapi.NewRouteWriter(w, key, responseModel)
	handler.ServeHTTP(routeWriter, r)
	if routeWriter.MissingModelBody() {
		policy.RecordMissingModelBody(key)
	}
}

// writeModelResponse writes a 200 success body as FastAPI writes a
// response_model: pydantic-core's dump_json (policy.WriteModel), from the
// typed Go response (pyjson.FromGoModel keeps a float field a float and
// writes a nil slice or map as empty).
func writeModelResponse(w http.ResponseWriter, value any) error {
	body, err := pyjson.FromGoModel(value)
	if err != nil {
		return err
	}
	policy.WriteModel(w, http.StatusOK, body, nil)
	return nil
}

// writeModelFailure answers a success body that could not be converted
// with the Python api's generic 500, as policy.WriteModel does for a body
// it cannot serialize.
func writeModelFailure(w http.ResponseWriter) { policy.WriteInternal(w) }
