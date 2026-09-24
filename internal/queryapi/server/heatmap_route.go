// GET /api/v1/heatmap -- the Go REST handler mirroring
// quadrant_route.go/investment_explain_route.go's shape for a REST route
// mounted on query-api's mux, gated by its own routeswitch DynamicSwitch
// entry.
//
// Auth: the same bearer-envelope verifier every REST route in this
// binary uses. Python's route additionally rate-limits ("20/minute",
// main.py:570) and rejects a handful of comparative query-param keys via
// _reject_comparative_params (main.py:202-208, peopleForbiddenQueryParams
// below) -- this port carries no rate limiter, the same documented gap
// every sibling REST route in this binary already carries, but DOES port
// the comparative-params reject (people_route.go's own copy of the
// identical check).
//
// Business logic (the four heatmap metric definitions, the ClickHouse
// readers, the repo-scope resolution and the person/developer-scope
// identity resolution) lives in internal/heatmap -- see that package's
// doc comment for the full parity contract.
package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/heatmap"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// heatmapOperation is this route's routeswitch operation name -- a
// PATH-keyed entry, same convention as quadrantOperation.
const heatmapOperation = "REST:GET:/api/v1/heatmap"

// heatmapEnabledEnvVar is the operator-facing toggle. Unset or anything
// other than a true-ish value leaves the route disabled, matching
// DynamicSwitch's own default.
const heatmapEnabledEnvVar = "GO_API_HEATMAP_ENABLED"

func heatmapSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(heatmapEnabledEnvVar)); enabled {
		sw.Set(heatmapOperation, true)
	}
	return sw
}

// loadHeatmapRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go's own
// loadQuadrantRouteConfig doc comment: this route never touches registry
// Postgres.
func loadHeatmapRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildHeatmapRoute constructs the handler, its own routeswitch Mux, and
// a cleanup function. ok is false when this route's dependencies are not
// configured -- main() only calls mux.HandleFunc when ok is true, same
// "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildHeatmapRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadHeatmapRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, fmt.Errorf("heatmap: build envelope verifier: %w", err)
	}

	// Optional -- nil whenever the pod has not been given the edge
	// credential's key material, in which case authenticateRESTRequest
	// falls back to its pre-existing envelope-only behaviour. See
	// buildEdgeVerifierFromEnv's own doc comment for the pod env
	// contract this reads.
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, fmt.Errorf("heatmap: build read client: %w", err)
	}

	routeMux := routeswitch.NewMux(heatmapSwitchFromEnv(getenv))
	routeMux.Register(heatmapOperation, newHeatmapWorkHandler(readClient))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "heatmap")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "heatmap")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(heatmapOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// newHeatmapWorkHandler is the routeswitch-registered handler -- reached
// only after buildHeatmapRoute's entryHandler has already authenticated
// the request and attached authctx.Claims to its context.
//
// Validation error aggregation matches FastAPI's own solve_dependencies:
// every one of type/metric/range_days/start_date/end_date/limit is
// checked, and every failing one is reported together in a single 422,
// in the SAME order Python's endpoint signature declares them (type,
// metric, scope_type, scope_id, range_days, start_date, end_date, x, y,
// limit -- scope_type/scope_id/x/y are plain strings with no type-level
// validation to fail). Only once every field validates does
// _reject_comparative_params run (main.py:585), matching Python's own
// order: FastAPI's dependency solving runs to completion (422 or not)
// BEFORE the endpoint body -- where _reject_comparative_params is the
// first line -- ever executes.
func newHeatmapWorkHandler(client heatmap.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "heatmap", "Not authenticated")
			return
		}

		query := r.URL.Query()
		var validationErrors []pydanticErrorDetail

		heatmapType := query.Get("type")
		if !query.Has("type") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "type"}, nil))
		}
		metric := query.Get("metric")
		if !query.Has("metric") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "metric"}, nil))
		}

		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}

		rangeDays := 14
		if query.Has("range_days") {
			raw := query.Get("range_days")
			if parsed, err := strconv.Atoi(raw); err == nil {
				rangeDays = parsed
			} else {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
			}
		}

		startDate, startPresent, startOK := parseISODateQueryParam(query.Get("start_date"))
		if startPresent && !startOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "start_date"}, query.Get("start_date")))
		}
		endDate, endPresent, endOK := parseISODateQueryParam(query.Get("end_date"))
		if endPresent && !endOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "end_date"}, query.Get("end_date")))
		}

		limit := 50
		if query.Has("limit") {
			raw := query.Get("limit")
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			} else {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "limit"}, raw))
			}
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// _reject_comparative_params (main.py:202-208): ANY of these
		// query-param KEYS present -- regardless of value -- is a 400.
		// Reuses people_route.go's own peopleForbiddenQueryParams: both
		// port the SAME Python module-level set (main.py:167-175), this
		// binary's one call site for it.
		for key := range query {
			if peopleForbiddenQueryParams[key] {
				writeRESTError(w, r, "heatmap", claims.OrgID, http.StatusBadRequest, "Comparative parameters are not supported.")
				return
			}
		}

		params := heatmap.Params{
			Type:      heatmapType,
			Metric:    metric,
			ScopeType: scopeType,
			ScopeID:   query.Get("scope_id"),
			RangeDays: rangeDays,
			X:         query.Get("x"),
			Y:         query.Get("y"),
			Limit:     boundedLimitParamHeatmap(limit, 200),
		}
		if startPresent {
			params.StartDate = &startDate
		}
		if endPresent {
			params.EndDate = &endDate
		}

		resp, err := heatmap.BuildResponse(r.Context(), client, claims.OrgID, params)
		if err != nil {
			if reqErr, ok := heatmap.AsRequestError(err); ok {
				writeRESTError(w, r, "heatmap", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:604-605): any error that is
			// not one of build_heatmap_response's own typed
			// HTTPExceptions degrades to a generic 503, never a raw
			// ClickHouse error on the wire.
			writeRESTDataUnavailable(w, r, "heatmap", claims.OrgID, err)
			return
		}

		if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
			log.Printf("query-api: heatmap: encode response failed: org_id=%s request_id=%s err=%v",
				claims.OrgID, envelopeRequestID(r), encodeErr)
			writeModelFailure(w)
		}
	}
}

// boundedLimitParamHeatmap ports _bounded_limit_param(limit, 200)
// (main.py:211-214,589 -- the route's own call site into
// build_heatmap_response), the same clamp quadrant_route.go/people_
// route.go call at their own route layer rather than inside their
// package.
func boundedLimitParamHeatmap(limit, maxLimit int) int {
	if limit <= 0 {
		if 50 < maxLimit {
			return 50
		}
		return maxLimit
	}
	if limit > maxLimit {
		return maxLimit
	}
	if limit < 1 {
		return 1
	}
	return limit
}
