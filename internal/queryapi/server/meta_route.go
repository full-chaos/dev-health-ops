// GET /api/v1/meta -- the Go REST handler mirroring filter_options_route.go
// and quadrant_route.go's shape for a REST route mounted on query-api's
// mux, gated by its own routeswitch DynamicSwitch entry (a path-keyed
// operation, same reasoning as quadrantOperation's doc comment: a REST
// route has no GraphQL document to digest, so it is not
// PostgresSwitch-backed).
//
// Auth: NONE. Every other REST route ported into this binary so far
// carries the bearer-envelope verifier this file's siblings use -- meta is
// the first one that does not, because the Python route itself has none.
// main.py's meta() is a bare `@app.get("/api/v1/meta", ...)` with no
// `Depends(get_current_user)`, confirmed public by a committed test
// (tests/api/test_analytics_auth.py::test_public_endpoints_do_not_require_auth)
// and by contracts/auth/v1/endpoint-profiles.ops.json's own row for this
// route (classification: "public", tenant_requirement: "none"). Adding an
// auth check here would be adding auth the Python route never had -- see
// internal/meta's package doc comment for the full citation trail.
//
// The route takes no query parameters and returns no typed errors --
// see internal/meta's package doc comment for what BuildResponse does
// with a ClickHouse failure (degrades one field, still answers 200) versus
// the two narrow, declared divergences from Python's own error paths.
//
// Business logic (the static response shape, the version() probe, and the
// declared divergences from Python's error paths) lives in internal/meta
// -- see that package's doc comment for the full parity contract.
package server

import (
	"log"
	"net/http"
	"strconv"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/meta"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// metaOperation is this route's routeswitch operation name -- a
// PATH-keyed entry, same convention as quadrantOperation.
const metaOperation = "REST:GET:/api/v1/meta"

// metaEnabledEnvVar is the operator-facing toggle. Unset or anything other
// than a true-ish value leaves the route disabled, matching DynamicSwitch's
// own default.
const metaEnabledEnvVar = "GO_API_META_ENABLED"

func metaSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(metaEnabledEnvVar)); enabled {
		sw.Set(metaOperation, true)
	}
	return sw
}

// loadMetaRouteConfig reads this route's own, single dependency --
// ClickHouse. No envelope verifier trio (unlike loadFilterOptionsRouteConfig):
// this route carries no auth, so there is nothing to verify a token
// against. Deliberately NOT loadQueryRouteConfig: like quadrant and
// filter-options, this route never touches registry Postgres.
func loadMetaRouteConfig(getenv getenvFunc) (clickHouseURI string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	if clickHouseURI == "" {
		return "", false
	}
	return clickHouseURI, true
}

// buildMetaRoute constructs the handler, its own routeswitch Mux, and a
// cleanup function. ok is false when CLICKHOUSE_URI is not configured --
// main() only calls mux.HandleFunc when ok is true, same "stay unmounted,
// don't fail to build/start" contract every other optionally-configured
// route in this binary follows (see internal/meta's package doc comment,
// "Declared divergence: missing CLICKHOUSE_URI", for why this differs from
// Python's per-request 500 in that case).
func buildMetaRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, cfgOK := loadMetaRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, false, nil
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(metaSwitchFromEnv(getenv))
	routeMux.Register(metaOperation, newMetaWorkHandler(readClient))

	// No auth step: this route is public (see package doc comment).
	// Dispatch runs directly off the method check.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "meta")
			return
		}
		routeMux.Dispatch(metaOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// newMetaWorkHandler is the routeswitch-registered handler.
func newMetaWorkHandler(client meta.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := meta.BuildResponse(r.Context(), client)

		// The success body is the route's response_model, written as
		// pydantic-core dump_json writes it (writeModelResponse).
		if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
			log.Printf("query-api: meta: encode response failed: request_id=%s err=%v",
				envelopeRequestID(r), encodeErr)
			writeModelFailure(w)
		}
	}
}
