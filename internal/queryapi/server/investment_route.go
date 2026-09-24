// GET+POST /api/v1/investment and GET /api/v1/investment/sunburst -- the
// Go REST handlers mirroring drilldown_prs_route.go/explain_route.go's
// shape for REST routes mounted on query-api's mux, each gated by its
// own routeswitch entries.
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's three routes additionally rate-limit
// ("60/minute" on both /investment methods, main.py:1229,1259; the
// sunburst GET carries no rate limiter in Python either); this port
// carries no rate limiter for any of the three, the same documented gap
// quadrant_route.go/drilldown_prs_route.go/explain_route.go already
// carry (this service has no rate-limiting mechanism anywhere yet).
//
// ERROR BODIES: every non-2xx response uses this binary's one shared
// REST error-response path (rest_error_response.go). All three Python
// handlers map EVERY exception to a single 503 "Data unavailable" and
// carry no typed HTTPException branch at all (api/main.py:1229-1305), so
// a business-logic failure from internal/investment always answers
// writeRESTDataUnavailable here, never writeRESTError with a different
// status.
//
// Business logic (the ClickHouse reads, the ReplacingMergeTree dedup,
// the evidence-quality driver computation) lives in internal/investment
// -- see that package's own doc comment for the full parity contract,
// including its declared divergences from the exact Python source.
package server

import (
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// investmentGetOperation/investmentPostOperation/
// investmentSunburstGetOperation are these routes' routeswitch operation
// names -- PATH+METHOD-keyed entries, same convention as
// drilldownPRsGetOperation/drilldownPRsPostOperation.
const (
	investmentGetOperation         = "REST:GET:/api/v1/investment"
	investmentPostOperation        = "REST:POST:/api/v1/investment"
	investmentSunburstGetOperation = "REST:GET:/api/v1/investment/sunburst"
)

// investmentEnabledEnvVar/investmentSunburstEnabledEnvVar are the
// operator-facing toggles -- unset or anything other than a true-ish
// value leaves the guarded operation(s) disabled, matching
// DynamicSwitch's own default. /api/v1/investment's GET/POST pair shares
// one toggle (one handler package, one deploy unit, same reasoning as
// drilldownPRsEnabledEnvVar); sunburst is a separate path with its own
// toggle, matching quadrant_route.go's single-operation convention.
const (
	investmentEnabledEnvVar         = "GO_API_INVESTMENT_ENABLED"
	investmentSunburstEnabledEnvVar = "GO_API_INVESTMENT_SUNBURST_ENABLED"
)

func investmentSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(investmentEnabledEnvVar)); enabled {
		sw.Set(investmentGetOperation, true)
		sw.Set(investmentPostOperation, true)
	}
	return sw
}

func investmentSunburstSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(investmentSunburstEnabledEnvVar)); enabled {
		sw.Set(investmentSunburstGetOperation, true)
	}
	return sw
}

// loadInvestmentRouteConfig reads this route family's own dependency set
// -- ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go's own
// loadQuadrantRouteConfig doc comment: none of these three routes touch
// registry Postgres.
func loadInvestmentRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildInvestmentRoute constructs the GET+POST /api/v1/investment
// handler, its own routeswitch Mux, and a cleanup function. ok is false
// when this route's dependencies are not configured -- main() only calls
// mux.HandleFunc when ok is true, same "stay unmounted, don't fail to
// build/start" contract every other optionally-configured route in this
// binary follows.
func buildInvestmentRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadInvestmentRouteConfig(getenv)
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
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, err
	}

	reader, err := investment.NewReader(analytics.PinInvestmentMembershipScope(readClient))
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(investmentSwitchFromEnv(getenv))
	routeMux.Register(investmentGetOperation, newInvestmentGetHandler(reader))
	routeMux.Register(investmentPostOperation, newInvestmentPostHandler(reader))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		var operation string
		switch r.Method {
		case http.MethodGet:
			operation = investmentGetOperation
		case http.MethodPost:
			operation = investmentPostOperation
		default:
			writeRESTMethodNotAllowed(w, r, "investment")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "investment")
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

// buildInvestmentSunburstRoute constructs the GET
// /api/v1/investment/sunburst handler, its own routeswitch Mux, and a
// cleanup function. Same "stay unmounted, don't fail to build/start"
// contract as buildInvestmentRoute; a separate ClickHouse client and
// routeswitch Mux from /api/v1/investment's own, matching the
// one-client-per-route-file convention every other route in this binary
// follows (quadrant_route.go, explain_route.go, drilldown_prs_route.go).
func buildInvestmentSunburstRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadInvestmentRouteConfig(getenv)
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
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, err
	}

	reader, err := investment.NewReader(analytics.PinInvestmentMembershipScope(readClient))
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(investmentSunburstSwitchFromEnv(getenv))
	routeMux.Register(investmentSunburstGetOperation, newInvestmentSunburstGetHandler(reader))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeRESTMethodNotAllowed(w, r, "investment_sunburst")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "investment_sunburst")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(investmentSunburstGetOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// investmentTimeFilterMap adapts GET's own query-param inputs into the
// map[string]any shape timeWindow (investment_explain_route.go, this
// package) expects. Both compare_days AND range_days carry rangeDays --
// _filters_from_query (api/main.py:178-199) is called with range_days
// passed for BOTH the range_days and compare_days positional arguments
// on all three of this route family's GET handlers, not merely
// range_days as the parameter names alone would suggest. timeWindow
// itself never reads compare_days (drilldown/prs and investment/explain
// never need it either -- see timeWindow's own doc comment), so this has
// no effect on the computed window; included for a faithful mirror of
// what the query-parameter map actually contains, not because it changes
// the result.
func investmentTimeFilterMap(rangeDays int, startDate, endDate *time.Time) map[string]any {
	timeFilter := map[string]any{
		"range_days":   float64(rangeDays),
		"compare_days": float64(rangeDays),
	}
	if startDate != nil {
		timeFilter["start_date"] = startDate.Format("2006-01-02")
	}
	if endDate != nil {
		timeFilter["end_date"] = endDate.Format("2006-01-02")
	}
	return map[string]any{"time": timeFilter}
}

// writeInvestmentResponse writes resp as the final 200 JSON body via
// json.NewEncoder(w).Encode -- this repo's JSON-response path, never a
// raw w.Write of pre-marshalled bytes.
func writeInvestmentResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *investment.Response) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: investment: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}

// writeInvestmentSunburstResponse writes resp as the final 200 JSON body
// -- a BARE JSON ARRAY, matching GET /api/v1/investment/sunburst's own
// response_model=list[InvestmentSunburstSlice] (never an object wrapping
// one): encoding a Go slice directly is already the bare-array wire
// shape, with no extra wrapping struct in the way.
func writeInvestmentSunburstResponse(w http.ResponseWriter, r *http.Request, orgID string, resp []investment.SunburstSlice) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: investment_sunburst: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}

// newInvestmentGetHandler is the routeswitch-registered handler for
// GET /api/v1/investment -- the Go port of investment (api/main.py:
// 1228-1250). Reached only after buildInvestmentRoute's entryHandler has
// already authenticated the request and attached authctx.Claims.
func newInvestmentGetHandler(reader *investment.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildInvestmentRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches
			// this handler -- see authenticateRESTRequest's own doc
			// comment for the three real 401 shapes this route answers.
			writeRESTUnauthorized(w, r, "investment", "Not authenticated")
			return
		}

		query := r.URL.Query()
		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		scopeID := query.Get("scope_id")

		var validationErrors []pydanticErrorDetail

		rangeDays := 30
		if raw := query.Get("range_days"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
			} else {
				rangeDays = parsed
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

		startTS, endTS, windowErr := timeWindow(investmentTimeFilterMap(rangeDays, startDatePtr, endDatePtr))
		if windowErr != nil {
			writeTimeWindowOverflow(w, r, "investment", claims.OrgID)
			return
		}

		var scopeIDs []string
		if scopeID != "" {
			scopeIDs = []string{scopeID}
		}

		params := investment.Params{
			StartTS: startTS, EndTS: endTS,
			ScopeLevel: scopeType, ScopeIDs: scopeIDs,
			// what.repos and why.work_category are never populated by
			// _filters_from_query (api/main.py:178-199) -- GET has no
			// query param for either, same as drilldown_prs_route.go's
			// own GET handler.
			WhatRepos: nil, WorkCategory: nil,
		}

		resp, err := investment.BuildResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1240-1241) -- this route has
			// no typed HTTPException branch at all.
			writeRESTDataUnavailable(w, r, "investment", claims.OrgID, err)
			return
		}

		// Python sets this header only on the GET route (main.py:
		// 1237-1238), steering callers toward POST with an explicit
		// filters body.
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeInvestmentResponse(w, r, claims.OrgID, resp)
	}
}

// newInvestmentPostHandler is the routeswitch-registered handler for
// POST /api/v1/investment -- the Go port of investment_post
// (api/main.py:1254-1268).
func newInvestmentPostHandler(reader *investment.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only -- see the GET handler's own comment above.
			writeRESTUnauthorized(w, r, "investment", "Not authenticated")
			return
		}

		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			writeRESTError(w, r, "investment", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}

		// HomeRequest(filters: MetricFilter) -- filters carries no
		// Field(default_factory=...), so it is REQUIRED, same as
		// DrilldownRequest/ExplainRequest's own "filters" field.
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

		startTS, endTS, windowErr := timeWindow(filters)
		if windowErr != nil {
			writeTimeWindowOverflow(w, r, "investment", claims.OrgID)
			return
		}

		params := investment.Params{
			StartTS: startTS, EndTS: endTS,
			ScopeLevel: scopeLevel, ScopeIDs: scopeIDs,
			WhatRepos: whatRepos, WorkCategory: workCategoryFromFilters(filters),
		}

		resp, err := investment.BuildResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			writeRESTDataUnavailable(w, r, "investment", claims.OrgID, err)
			return
		}
		writeInvestmentResponse(w, r, claims.OrgID, resp)
	}
}

// newInvestmentSunburstGetHandler is the routeswitch-registered handler
// for GET /api/v1/investment/sunburst -- the Go port of
// investment_sunburst (api/main.py:1271-1298).
func newInvestmentSunburstGetHandler(reader *investment.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "investment_sunburst", "Not authenticated")
			return
		}

		query := r.URL.Query()
		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		scopeID := query.Get("scope_id")

		var validationErrors []pydanticErrorDetail

		rangeDays := 30
		if raw := query.Get("range_days"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "range_days"}, raw))
			} else {
				rangeDays = parsed
			}
		}

		// limit: int = 500 (api/main.py:1278) -- the 500 default applies
		// only when the query param is ABSENT; a present value (including
		// 0 or a negative number) is passed straight through to
		// investment.FetchInvestmentSunburst, which binds it as given --
		// see that function's own doc comment on why no clamping happens
		// downstream of this route.
		limit := 500
		if raw := query.Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "limit"}, raw))
			} else {
				limit = parsed
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

		startTS, endTS, windowErr := timeWindow(investmentTimeFilterMap(rangeDays, startDatePtr, endDatePtr))
		if windowErr != nil {
			writeTimeWindowOverflow(w, r, "investment", claims.OrgID)
			return
		}

		var scopeIDs []string
		if scopeID != "" {
			scopeIDs = []string{scopeID}
		}

		params := investment.SunburstParams{
			StartTS: startTS, EndTS: endTS,
			ScopeLevel: scopeType, ScopeIDs: scopeIDs,
			WhatRepos: nil, WorkCategory: nil, Limit: limit,
		}

		resp, err := investment.BuildSunburstResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			writeRESTDataUnavailable(w, r, "investment_sunburst", claims.OrgID, err)
			return
		}

		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeInvestmentSunburstResponse(w, r, claims.OrgID, resp)
	}
}
