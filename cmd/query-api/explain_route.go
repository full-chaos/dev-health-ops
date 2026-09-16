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
// text body -- captured live via an uncommitted `TestClient` one-off
// against ad hoc FastAPI apps reproducing get_current_user's three 401
// branches (api/auth/routers/dependencies.py:37-74), explain's own 503
// (api/main.py:534,566) and a same-path GET+POST route's 405 (see this
// PR's TEST-EVIDENCE for the exact script and captured bodies). The 405
// case has one confirmed idiosyncrasy: Starlette's router reports the
// Allow header/detail of the FIRST route it registered for the path, not
// the union of every method registered there -- main.py registers POST
// /api/v1/explain (line 521) BEFORE GET (line 537), so a third method
// against the real Python route answers `Allow: POST` alone, GET absent.
// Ported verbatim (explainMethodNotAllowed below), not "fixed" to list
// both methods -- that would no longer match the live contract this port
// exists to preserve.
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

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/explain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
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
	// binary uses.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		var operation string
		switch r.Method {
		case http.MethodGet:
			operation = explainGetOperation
		case http.MethodPost:
			operation = explainPostOperation
		default:
			explainMethodNotAllowed(w)
			return
		}

		authHeader := r.Header.Get("Authorization")
		token, ok := bearerToken(authHeader)
		if !ok {
			// get_current_user (api/auth/routers/dependencies.py:42-47,
			// 50-55): "Not authenticated" when the header is absent,
			// "Invalid authorization header" when present but not
			// Bearer-shaped -- the ONE distinction this port's shared
			// bearerToken (query_route.go) collapses into a single bool,
			// re-derived here from the raw header alone.
			msg := "Invalid authorization header"
			if authHeader == "" {
				msg = "Not authenticated"
			}
			writeExplainAuthError(w, msg)
			return
		}
		verifyCtx := principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r))
		claims, err := verifier.Verify(verifyCtx, token)
		if err != nil {
			// get_current_user's third 401 branch (dependencies.py:67-72):
			// "Invalid or expired token".
			writeExplainAuthError(w, "Invalid or expired token")
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), authctx.Claims{OrgID: claims.OrgID}))
		routeMux.Dispatch(operation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return entryHandler, cleanupFn, true, nil
}

// explainErrorBody is FastAPI's default {"detail": ...} HTTPException
// envelope -- Detail is `any` because Python's own detail value varies
// by branch: a flat string (503, 405) or a nested {"message": ...} object
// (401, error_detail()'s own shape, api/utils/errors.py:6-15).
type explainErrorBody struct {
	Detail any `json:"detail"`
}

func writeExplainJSONError(w http.ResponseWriter, status int, detail any, headers map[string]string) {
	for name, value := range headers {
		w.Header().Set(name, value)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(explainErrorBody{Detail: detail}); err != nil {
		log.Printf("query-api: explain: encode error response failed: status=%d err=%v", status, err)
	}
}

// writeExplainAuthError ports get_current_user's 401 envelope --
// {"detail": {"message": msg}}, WWW-Authenticate: Bearer (confirmed live,
// see this file's own package doc comment for the capture method).
func writeExplainAuthError(w http.ResponseWriter, msg string) {
	writeExplainJSONError(w, http.StatusUnauthorized, map[string]string{"message": msg},
		map[string]string{"WWW-Authenticate": "Bearer"})
}

// explainMethodNotAllowed ports Starlette's default 405 for this exact
// path (see this file's own package doc comment for the Allow-header
// idiosyncrasy this reproduces verbatim).
func explainMethodNotAllowed(w http.ResponseWriter) {
	writeExplainJSONError(w, http.StatusMethodNotAllowed, "Method Not Allowed", map[string]string{"Allow": "POST"})
}

// writeExplainDataUnavailable ports explain.py's outer
// `except Exception: raise HTTPException(503, "Data unavailable")`
// (main.py:533-534, 565-566) -- {"detail": "Data unavailable"}, a flat
// string (no error_detail() wrapper on this one, unlike the 401 branches).
func writeExplainDataUnavailable(w http.ResponseWriter) {
	writeExplainJSONError(w, http.StatusServiceUnavailable, "Data unavailable", nil)
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
			writeExplainAuthError(w, "Not authenticated")
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
			writeExplainDataUnavailable(w)
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
			writeExplainAuthError(w, "Not authenticated")
			return
		}

		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		// ExplainRequest(metric: str, filters: MetricFilter) -- filters
		// carries no Field(default_factory=...), so it is REQUIRED, same
		// as DrilldownRequest (drilldown_prs_route.go's own doc comment
		// on this exact point). A genuinely empty/null body is Pydantic's
		// whole-model-missing case (loc ["body"]).
		var decoded any
		bodyIsEmptyOrNull := len(bodyBytes) == 0
		if !bodyIsEmptyOrNull {
			if err := json.Unmarshal(bodyBytes, &decoded); err != nil {
				writePydanticValidationError(w, r, claims.OrgID, jsonSyntaxErrorDetail([]any{"body"}, bodyBytes))
				return
			}
			if decoded == nil {
				bodyIsEmptyOrNull = true
			}
		}
		if bodyIsEmptyOrNull {
			writePydanticValidationError(w, r, claims.OrgID, missingFieldError([]any{"body"}, nil))
			return
		}

		body, isObject := decoded.(map[string]any)
		if !isObject {
			writePydanticValidationError(w, r, claims.OrgID, modelAttributesTypeError([]any{"body"}, decoded))
			return
		}

		var validationErrors []pydanticErrorDetail

		// Field declaration order (ExplainRequest: metric, then filters,
		// api/models/filters.py:57-59) -- Pydantic aggregates every
		// simultaneously-invalid field in that same order.
		var metric string
		metricValue, hasMetric := body["metric"]
		if !hasMetric {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "metric"}, body))
		} else if s, isString := metricValue.(string); !isString {
			validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "metric"}, metricValue))
		} else {
			metric = s
		}

		filtersValue, hasFilters := body["filters"]
		if !hasFilters {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "filters"}, body))
		} else {
			validationErrors = append(validationErrors, validateMetricFilter([]any{"body", "filters"}, filtersValue)...)
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		filters, _ := filtersValue.(map[string]any)
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
			writeExplainDataUnavailable(w)
			return
		}
		writeExplainResponse(w, claims.OrgID, envelopeRequestID(r), resp)
	}
}
