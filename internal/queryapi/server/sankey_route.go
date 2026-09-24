// GET+POST /api/v1/sankey -- the Go REST handler mirroring
// drilldown_prs_route.go/heatmap_route.go's shape for a REST route pair
// mounted on query-api's mux, one routeswitch operation per method+path.
//
// Auth: the same bearer-envelope verifier every REST route in this
// binary uses. Python's routes additionally rate-limit ("60/minute",
// main.py:1397,1432); this port carries no rate limiter, the same
// documented gap every sibling REST route in this binary already carries.
//
// ERROR SHAPE: unlike quadrant/heatmap/drilldown, sankey_get/sankey_post
// (api/main.py:1396-1452) catch EVERY exception the same, generic way --
// `except Exception: raise HTTPException(503, "Data unavailable")`, with
// no narrower except-ValueError branch a sibling route
// (investment_flow, main.py:1372-1373) has. So an unknown `mode` string
// (GET) or an invalid `scope_type` reaching ScopeFilter's own Pydantic
// constructor inside _filters_from_query (GET only -- POST validates
// scope.level through the SAME shared "filters" body field every other
// body-carrying route already validates, a real 422, since MetricFilter is
// a top-level Pydantic field there) both become a 503, never a 400/404.
// internal/sankey's own BuildResponse mirrors that: it has no
// RequestError type at all, and this file's handlers treat ANY
// BuildResponse error the same way, a 503.
//
// Business logic (schema-presence guard, the four sankey mode builders,
// the ClickHouse reads and their ReplacingMergeTree dedup fixes) lives in
// internal/sankey -- see that package's own doc comment for the full
// parity contract.
package server

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/sankey"
)

const (
	sankeyGetOperation  = "REST:GET:/api/v1/sankey"
	sankeyPostOperation = "REST:POST:/api/v1/sankey"
)

const sankeyEnabledEnvVar = "GO_API_SANKEY_ENABLED"

func sankeySwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(sankeyEnabledEnvVar)); enabled {
		sw.Set(sankeyGetOperation, true)
		sw.Set(sankeyPostOperation, true)
	}
	return sw
}

// loadSankeyRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go's own
// loadQuadrantRouteConfig doc comment: this route never touches registry
// Postgres.
func loadSankeyRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

func buildSankeyRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadSankeyRouteConfig(getenv)
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

	routeMux := routeswitch.NewMux(sankeySwitchFromEnv(getenv))
	routeMux.Register(sankeyGetOperation, newSankeyGetHandler(analytics.PinInvestmentMembershipScope(readClient)))
	routeMux.Register(sankeyPostOperation, newSankeyPostHandler(analytics.PinInvestmentMembershipScope(readClient)))

	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		var operation string
		switch r.Method {
		case http.MethodGet:
			operation = sankeyGetOperation
		case http.MethodPost:
			operation = sankeyPostOperation
		default:
			writeRESTMethodNotAllowed(w, r, "sankey")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "sankey")
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

// sankeyValidScopeLevels mirrors ScopeFilter.level's Literal set
// (api/models/filters.py:16-18).
var sankeyValidScopeLevels = map[string]bool{
	"org": true, "team": true, "repo": true, "service": true, "developer": true,
}

// sankeyModeValues mirrors SankeyRequest.mode's Literal set
// (api/models/filters.py:112).
var sankeyModeValues = []string{"investment", "expense", "state", "hotspot"}

func writeSankeyResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *sankey.Response) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: sankey: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}

// applySankeyWindow ports _apply_window_to_filters (services/sankey.py:
// 62-83): when window_start/window_end are present they override
// filters.time.start_date/end_date (and, when both are present,
// filters.time.range_days too), leaving filters untouched otherwise. A
// shallow-copied map, never mutating the caller's own filters value.
func applySankeyWindow(filters map[string]any, windowStart, windowEnd *time.Time) map[string]any {
	if windowStart == nil && windowEnd == nil {
		return filters
	}
	out := make(map[string]any, len(filters)+1)
	for k, v := range filters {
		out[k] = v
	}
	timeFilter, _ := out["time"].(map[string]any)
	newTimeFilter := make(map[string]any, len(timeFilter)+3)
	for k, v := range timeFilter {
		newTimeFilter[k] = v
	}
	if windowStart != nil {
		newTimeFilter["start_date"] = windowStart.Format("2006-01-02")
	}
	if windowEnd != nil {
		newTimeFilter["end_date"] = windowEnd.Format("2006-01-02")
	}
	if windowStart != nil && windowEnd != nil {
		days := int(windowEnd.Sub(*windowStart).Hours() / 24)
		if days < 1 {
			days = 1
		}
		newTimeFilter["range_days"] = float64(days)
	}
	out["time"] = newTimeFilter
	return out
}

// sankeyParamsFromFilters reads scope/what/why out of the raw filters
// map and StartDay/EndDay out of timeWindow's own return, building
// sankey.Params -- the shared shape both the GET and POST handlers below
// hand to sankey.BuildResponse.
func sankeyParamsFromFilters(mode string, filters map[string]any) sankey.Params {
	scope, _ := filters["scope"].(map[string]any)
	scopeLevel, _ := scope["level"].(string)
	if scopeLevel == "" {
		scopeLevel = "org"
	}
	scopeIDs := stringsFromAny(scope["ids"])
	what, _ := filters["what"].(map[string]any)
	whatRepos := stringsFromAny(what["repos"])
	why, _ := filters["why"].(map[string]any)
	workCategory := stringsFromAny(why["work_category"])

	startTS, endTS := timeWindow(filters)

	return sankey.Params{
		Mode:         mode,
		ScopeLevel:   scopeLevel,
		ScopeIDs:     scopeIDs,
		WhatRepos:    whatRepos,
		WorkCategory: workCategory,
		StartDay:     startTS,
		EndDay:       endTS,
	}
}

// newSankeyGetHandler is the routeswitch-registered handler for
// GET /api/v1/sankey -- the Go port of sankey_get (api/main.py:
// 1396-1428).
func newSankeyGetHandler(client sankey.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "sankey", "Not authenticated")
			return
		}

		query := r.URL.Query()
		mode := query.Get("mode")
		if !query.Has("mode") {
			mode = "investment"
		}
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
		windowStart, windowStartPresent, windowStartOK := parseISODateQueryParam(query.Get("window_start"))
		if windowStartPresent && !windowStartOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "window_start"}, query.Get("window_start")))
		}
		windowEnd, windowEndPresent, windowEndOK := parseISODateQueryParam(query.Get("window_end"))
		if windowEndPresent && !windowEndOK {
			validationErrors = append(validationErrors, dateQueryParamError([]any{"query", "window_end"}, query.Get("window_end")))
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// _filters_from_query (main.py:178-199) constructs ScopeFilter(
		// level=scope_type, ...) -- a genuine Pydantic model construction
		// that raises for any scope_type outside the Literal set, INSIDE
		// sankey_get's own try block, so it is caught by the same generic
		// `except Exception: 503` every other failure here is -- never a
		// 400/404, and the X-DevHealth-Deprecated header (set only after
		// build_sankey_response succeeds) is never attached to that 503.
		if !sankeyValidScopeLevels[scopeType] {
			writeRESTDataUnavailable(w, r, "sankey", claims.OrgID,
				fmt.Errorf("sankey: ScopeFilter construction: scope_type %q is not one of the valid levels", scopeType))
			return
		}

		timeFilter := map[string]any{"range_days": float64(rangeDays)}
		if startPresent {
			timeFilter["start_date"] = startDate.Format("2006-01-02")
		}
		if endPresent {
			timeFilter["end_date"] = endDate.Format("2006-01-02")
		}
		var scopeIDs []any
		if scopeID != "" {
			scopeIDs = []any{scopeID}
		}
		filters := map[string]any{
			"time":  timeFilter,
			"scope": map[string]any{"level": scopeType, "ids": scopeIDs},
		}

		var windowStartPtr, windowEndPtr *time.Time
		if windowStartPresent {
			windowStartPtr = &windowStart
		}
		if windowEndPresent {
			windowEndPtr = &windowEnd
		}
		filters = applySankeyWindow(filters, windowStartPtr, windowEndPtr)

		params := sankeyParamsFromFilters(mode, filters)

		resp, err := sankey.BuildResponse(r.Context(), client, claims.OrgID, params)
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1426-1428).
			writeRESTDataUnavailable(w, r, "sankey", claims.OrgID, err)
			return
		}

		// Python sets this header only on the GET route, and only once
		// build_sankey_response has already succeeded (main.py:1423-1424).
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeSankeyResponse(w, r, claims.OrgID, resp)
	}
}

// newSankeyPostHandler is the routeswitch-registered handler for
// POST /api/v1/sankey -- the Go port of sankey_post (api/main.py:
// 1431-1452).
func newSankeyPostHandler(client sankey.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "sankey", "Not authenticated")
			return
		}

		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			writeRESTError(w, r, "sankey", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}

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

		// Validated in SankeyRequest's own field order (api/models/
		// filters.py:111-116): mode, filters, context, window_start,
		// window_end.
		var validationErrors []pydanticErrorDetail

		modeValue, hasMode := body.Get("mode")
		var mode string
		if !hasMode {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "mode"}, body))
		} else if modeStr, isString := modeValue.(string); !isString || !stringInSlice(modeStr, sankeyModeValues) {
			validationErrors = append(validationErrors, literalErrorDetail([]any{"body", "mode"}, modeValue, sankeyModeValues))
		} else {
			mode = modeStr
		}

		filtersValue, hasFilters := body.Get("filters")
		if !hasFilters {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "filters"}, body))
		} else {
			validationErrors = append(validationErrors, validateMetricFilter([]any{"body", "filters"}, filtersValue)...)
		}

		if ctxValue, hasCtx := body.Get("context"); hasCtx && ctxValue != nil {
			ctxObj, isObj := ctxValue.(*pyjson.Object)
			if !isObj {
				validationErrors = append(validationErrors, modelAttributesTypeError([]any{"body", "context"}, ctxValue))
			} else {
				if v, ok := ctxObj.Get("entity_id"); ok && v != nil {
					if _, isStr := v.(string); !isStr {
						validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "context", "entity_id"}, v))
					}
				}
				if v, ok := ctxObj.Get("entity_label"); ok && v != nil {
					if _, isStr := v.(string); !isStr {
						validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "context", "entity_label"}, v))
					}
				}
			}
		}

		var windowStartValue, windowEndValue any
		if v, hasWindowStart := body.Get("window_start"); hasWindowStart {
			windowStartValue = v
			if detail := validateBodyDateField([]any{"body", "window_start"}, v); detail != nil {
				validationErrors = append(validationErrors, *detail)
			}
		}
		if v, hasWindowEnd := body.Get("window_end"); hasWindowEnd {
			windowEndValue = v
			if detail := validateBodyDateField([]any{"body", "window_end"}, v); detail != nil {
				validationErrors = append(validationErrors, *detail)
			}
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		filters, _ := legacyJSON(filtersValue).(map[string]any)

		var windowStartPtr, windowEndPtr *time.Time
		if t, ok := dateFromAny(legacyJSON(windowStartValue)); ok {
			windowStartPtr = &t
		}
		if t, ok := dateFromAny(legacyJSON(windowEndValue)); ok {
			windowEndPtr = &t
		}
		filters = applySankeyWindow(filters, windowStartPtr, windowEndPtr)

		params := sankeyParamsFromFilters(mode, filters)

		resp, err := sankey.BuildResponse(r.Context(), client, claims.OrgID, params)
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1448-1452).
			writeRESTDataUnavailable(w, r, "sankey", claims.OrgID, err)
			return
		}
		writeSankeyResponse(w, r, claims.OrgID, resp)
	}
}
