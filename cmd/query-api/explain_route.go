// GET+POST /api/v1/explain -- the Go REST handler mirroring
// drilldown_prs_route.go/investment_explain_route.go's shape for a REST
// route mounted on query-api's mux, gated by its own routeswitch entries
// (one per method+path, same convention as this route's siblings).
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's GET route additionally rate-limits
// ("20/minute", main.py:538); POST carries NO rate limiter in Python
// either (no @limiter.limit above explain_post, main.py:521-522). This
// port carries no rate limiter for either method -- the same documented
// gap quadrant_route.go/investment_explain_route.go/drilldown_prs_route.go
// already carry (this service has no rate-limiting mechanism anywhere
// yet).
//
// ERROR BODIES: every non-2xx response this route sends uses Python's
// own FastAPI/Starlette contract -- {"detail": ...} JSON, never a plain-
// text body -- via this binary's one shared REST error-response path
// (rest_error_response.go: writeRESTError/writeRESTUnauthorized/
// writeRESTMethodNotAllowed/writeRESTDataUnavailable/
// authenticateRESTRequest), the same helper every sibling REST route in
// this binary uses. The bodies themselves were captured live via an
// uncommitted `TestClient` one-off against ad hoc FastAPI apps
// reproducing get_current_user's three 401 branches (api/auth/routers/
// dependencies.py:37-74), explain's own 503 (api/main.py:534,566) and a
// same-path GET+POST route's 405. The 405 case has one confirmed
// idiosyncrasy this route passes through to writeRESTMethodNotAllowed's
// own optional Allow-header argument: Starlette's router reports the
// Allow header/detail of the FIRST route it registered for the path, not
// the union of every method registered there -- main.py registers POST
// /api/v1/explain (line 521) BEFORE GET (line 537), so a third method
// against the real Python route answers `Allow: POST` alone, GET absent.
// Ported verbatim, not "fixed" to list both methods -- that would no
// longer match the live contract this port exists to preserve.
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// explainGetOperation/explainPostOperation are this route's routeswitch
// operation names -- PATH+METHOD-keyed entries, same convention as
// drilldownPRsGetOperation/drilldownPRsPostOperation.
const (
	explainGetOperation  = "REST:GET:/api/v1/explain"
	explainPostOperation = "REST:POST:/api/v1/explain"
)

// explainEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves BOTH operations disabled,
// matching DynamicSwitch's own default. One toggle for the GET/POST
// pair: they share one handler package and one deploy unit (this file),
// matching drilldownPRsEnabledEnvVar's own reasoning.
const explainEnabledEnvVar = "GO_API_EXPLAIN_ENABLED"

func explainSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(explainEnabledEnvVar)); enabled {
		sw.Set(explainGetOperation, true)
		sw.Set(explainPostOperation, true)
	}
	return sw
}

// loadExplainRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go/
// drilldown_prs_route.go's own doc comments: this route never touches
// registry Postgres.
func loadExplainRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildExplainRoute constructs the handler, its own routeswitch Mux, and
// a cleanup function. ok is false when this route's dependencies are not
// configured -- main() only calls mux.HandleFunc when ok is true, same
// "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildExplainRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadExplainRouteConfig()
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, err
	}

	// Optional -- nil whenever the pod has not been given the edge
	// credential's key material, in which case authenticateRESTRequest
	// falls back to its pre-existing envelope-only behaviour. See
	// buildEdgeVerifierFromEnv's own doc comment for the pod env
	// contract this reads.
	edgeVerifier, err := buildEdgeVerifierFromEnv()
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, err
	}

	reader, err := explain.NewReader(readClient)
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(explainSwitchFromEnv())
	routeMux.Register(explainGetOperation, newExplainGetHandler(reader))
	routeMux.Register(explainPostOperation, newExplainPostHandler(reader))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses. authenticateRESTRequest (rest_error_response.go) is the
	// ONE place get_current_user's three 401 branches are classified --
	// this route no longer keeps its own copy of that distinction.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		var operation string
		switch r.Method {
		case http.MethodGet:
			operation = explainGetOperation
		case http.MethodPost:
			operation = explainPostOperation
		default:
			// Starlette reports the Allow header/detail of the FIRST route
			// it registered for this path, not the union of every method
			// registered there -- main.py registers POST /api/v1/explain
			// (line 521) BEFORE GET (line 537), so a third method against
			// the real Python route answers "Allow: POST" alone, GET
			// absent (confirmed live). Passed through verbatim, not
			// "fixed" to list both methods.
			writeRESTMethodNotAllowed(w, r, "explain", "POST")
			return
		}

		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "explain")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(operation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// writeExplainResponse writes resp as the final 200 JSON body via
// json.NewEncoder(w).Encode -- never a raw w.Write of pre-marshalled
// bytes, matching writeDrilldownPRsResponse's own established path.
func writeExplainResponse(w http.ResponseWriter, orgID, requestID string, resp *explain.Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("query-api: explain: encode response failed: org_id=%s request_id=%s err=%v", orgID, requestID, err)
	}
}

// explainTimeWindow ports time_window (api/services/filtering.py:78-92)
// IN FULL -- all four return values. The existing package-level
// timeWindow (investment_explain_route.go) only returns (start_day,
// end_day): investment/explain and drilldown/prs never need the
// comparison window. /explain uses all four (current value vs previous,
// plus the driver delta), so this is a parallel implementation over the
// same `filters["time"]` map shape, additionally reading compare_days
// (default 14, same floor-at-1 rule as range_days).
func explainTimeWindow(filters map[string]any) (startDay, endDay, compareStart, compareEnd time.Time) {
	timeFilter, _ := filters["time"].(map[string]any)

	rangeDays := intFromAny(timeFilter["range_days"], 14)
	if rangeDays < 1 {
		rangeDays = 1
	}
	compareDays := intFromAny(timeFilter["compare_days"], 14)
	if compareDays < 1 {
		compareDays = 1
	}

	endDate, hasEndDate := dateFromAny(timeFilter["end_date"])
	if !hasEndDate {
		endDate = time.Now().UTC().Truncate(24 * time.Hour)
	}
	endDay = endDate.AddDate(0, 0, 1)

	startDate, hasStartDate := dateFromAny(timeFilter["start_date"])
	if hasStartDate {
		startDay = startDate
		if !startDay.Before(endDay) {
			startDay = endDay.AddDate(0, 0, -1)
		}
	} else {
		startDay = endDay.AddDate(0, 0, -rangeDays)
	}

	compareEnd = startDay
	compareStart = compareEnd.AddDate(0, 0, -compareDays)
	return startDay, endDay, compareStart, compareEnd
}

// explainTimeFilterMap adapts GET's own query-param inputs into the
// map[string]any shape explainTimeWindow expects -- the SAME technique
// drilldownTimeFilterMap (drilldown_prs_route.go) already establishes,
// extended with compare_days (drilldown/investment_explain never need
// it).
func explainTimeFilterMap(rangeDays, compareDays int, startDate, endDate *time.Time) map[string]any {
	timeFilter := map[string]any{
		"range_days":   float64(rangeDays),
		"compare_days": float64(compareDays),
	}
	if startDate != nil {
		timeFilter["start_date"] = startDate.Format("2006-01-02")
	}
	if endDate != nil {
		timeFilter["end_date"] = endDate.Format("2006-01-02")
	}
	return map[string]any{"time": timeFilter}
}

// newExplainGetHandler is the routeswitch-registered handler for
// GET /api/v1/explain -- the Go port of explain (api/main.py:537-566).
// Reached only after buildExplainRoute's entryHandler has already
// authenticated the request and attached authctx.Claims.
func newExplainGetHandler(reader *explain.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildExplainRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches
			// this handler -- see authenticateRESTRequest's own doc
			// comment for the three real 401 shapes this route answers.
			writeRESTUnauthorized(w, r, "explain", "Not authenticated")
			return
		}

		query := r.URL.Query()

		var validationErrors []pydanticErrorDetail

		// metric: str has no default (api/main.py:542) -- required.
		// Present-but-empty ("?metric=") is a VALID (empty) string, same
		// as every other plain str query param in this binary; only the
		// key's absence is "missing".
		if !query.Has("metric") {
			validationErrors = append(validationErrors, missingFieldError([]any{"query", "metric"}, nil))
		}
		metric := query.Get("metric")

		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		scopeID := query.Get("scope_id")

		rangeDays := 14
		if raw := query.Get("range_days"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
			} else {
				rangeDays = parsed
			}
		}

		compareDays := 14
		if raw := query.Get("compare_days"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "compare_days"}, raw))
			} else {
				compareDays = parsed
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

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		var startDatePtr, endDatePtr *time.Time
		if startPresent {
			startDatePtr = &startDate
		}
		if endPresent {
			endDatePtr = &endDate
		}

		startDay, endDay, compareStart, compareEnd := explainTimeWindow(explainTimeFilterMap(rangeDays, compareDays, startDatePtr, endDatePtr))

		var scopeIDs []string
		if scopeID != "" {
			scopeIDs = []string{scopeID}
		}

		params := explain.Params{
			Metric:       metric,
			StartDay:     startDay,
			EndDay:       endDay,
			CompareStart: compareStart,
			CompareEnd:   compareEnd,
			ScopeLevel:   scopeType,
			ScopeIDs:     scopeIDs,
			// what.repos is never populated by _filters_from_query
			// (api/main.py:178-199) -- GET has no query param for it,
			// same as drilldown_prs_route.go's own GET handler.
			WhatRepos: nil,
		}

		resp, err := explain.BuildExplainResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			writeRESTDataUnavailable(w, r, "explain", claims.OrgID, err)
			return
		}

		// Python sets this header only on the GET route (main.py:562-563).
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeExplainResponse(w, claims.OrgID, envelopeRequestID(r), resp)
	}
}

// newExplainPostHandler is the routeswitch-registered handler for
// POST /api/v1/explain -- the Go port of explain_post
// (api/main.py:521-534).
func newExplainPostHandler(reader *explain.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildExplainRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches
			// this handler -- see authenticateRESTRequest's own doc
			// comment for the three real 401 shapes this route answers.
			writeRESTUnauthorized(w, r, "explain", "Not authenticated")
			return
		}

		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			// A body-read I/O failure (e.g. a client disconnect
			// mid-upload) has no Python counterpart to match -- see this
			// file's sibling REST routes for the same reasoning. Status
			// unchanged, only the wire body's encoding.
			writeRESTError(w, r, "explain", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}

		// ExplainRequest(metric: str, filters: MetricFilter) -- filters
		// carries no Field(default_factory=...), so it is REQUIRED, same
		// as DrilldownRequest (drilldown_prs_route.go's own doc comment
		// on this exact point). A genuinely empty/null body is Pydantic's
		// whole-model-missing case (loc ["body"]).
		decoded, bodyIsEmptyOrNull, syntaxDetail := decodeRequestBody([]any{"body"}, bodyBytes)
		if syntaxDetail != nil {
			writePydanticValidationError(w, r, claims.OrgID, *syntaxDetail)
			return
		}
		if bodyIsEmptyOrNull {
			writePydanticValidationError(w, r, claims.OrgID, missingFieldError([]any{"body"}, nil))
			return
		}

		body, isObject := decoded.(*pyjson.Object)
		if !isObject {
			writePydanticValidationError(w, r, claims.OrgID, modelAttributesTypeError([]any{"body"}, decoded))
			return
		}

		var validationErrors []pydanticErrorDetail

		// Field declaration order (ExplainRequest: metric, then filters,
		// api/models/filters.py:57-59) -- Pydantic aggregates every
		// simultaneously-invalid field in that same order.
		var metric string
		metricValue, hasMetric := body.Get("metric")
		if !hasMetric {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "metric"}, body))
		} else if s, isString := metricValue.(string); !isString {
			validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "metric"}, metricValue))
		} else {
			metric = s
		}

		filtersValue, hasFilters := body.Get("filters")
		if !hasFilters {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "filters"}, body))
		} else {
			validationErrors = append(validationErrors, validateMetricFilter([]any{"body", "filters"}, filtersValue)...)
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		filters, _ := legacyJSON(filtersValue).(map[string]any)
		scope, _ := filters["scope"].(map[string]any)
		scopeLevel, _ := scope["level"].(string)
		if scopeLevel == "" {
			scopeLevel = "org"
		}
		scopeIDs := stringsFromAny(scope["ids"])
		what, _ := filters["what"].(map[string]any)
		whatRepos := stringsFromAny(what["repos"])

		startDay, endDay, compareStart, compareEnd := explainTimeWindow(filters)

		params := explain.Params{
			Metric:       metric,
			StartDay:     startDay,
			EndDay:       endDay,
			CompareStart: compareStart,
			CompareEnd:   compareEnd,
			ScopeLevel:   scopeLevel,
			ScopeIDs:     scopeIDs,
			WhatRepos:    whatRepos,
		}

		resp, err := explain.BuildExplainResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			writeRESTDataUnavailable(w, r, "explain", claims.OrgID, err)
			return
		}
		writeExplainResponse(w, claims.OrgID, envelopeRequestID(r), resp)
	}
}
