// GET+POST /api/v1/drilldown/prs -- the Go REST handler mirroring
// quadrant_route.go/investment_explain_route.go's shape for a
// REST route mounted on query-api's mux, gated by its own routeswitch
// entries (one per method+path, same convention as this route's siblings
// -- see restendpoints.go's own doc comment on why matching is by PATH,
// not (METHOD, PATH): today this is the only /api/v1/* route this binary
// mounts with two Python methods at the same path, both wired here).
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's routes additionally rate-limit ("60/minute",
// main.py:909,939); this port carries no rate limiter, the same
// documented gap quadrant_route.go and investment_explain_route.go
// already carry (this service has no rate-limiting mechanism anywhere
// yet).
//
// Business logic (repo-scope resolution, the ClickHouse read, the
// ReplacingMergeTree dedup) lives in internal/drilldown -- see that
// package's own doc comment for the full parity contract, including its
// documented ReplacingMergeTree-dedup and org-scope divergence notes.
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
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/drilldown"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch"
)

// drilldownPRsGetOperation/drilldownPRsPostOperation are this route's
// routeswitch operation names -- PATH+METHOD-keyed entries, same
// convention as quadrantOperation/investmentExplainOperation (a REST
// route has no GraphQL document to digest).
const (
	drilldownPRsGetOperation  = "REST:GET:/api/v1/drilldown/prs"
	drilldownPRsPostOperation = "REST:POST:/api/v1/drilldown/prs"
)

// drilldownPRsEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves BOTH operations disabled,
// matching DynamicSwitch's own default. One toggle for the GET/POST
// pair: they share one handler package and one deploy unit (this file),
// so there is no operational reason to enable one method without the
// other.
const drilldownPRsEnabledEnvVar = "GO_API_DRILLDOWN_PRS_ENABLED"

func drilldownPRsSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(drilldownPRsEnabledEnvVar)); enabled {
		sw.Set(drilldownPRsGetOperation, true)
		sw.Set(drilldownPRsPostOperation, true)
	}
	return sw
}

// loadDrilldownPRsRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go's own
// loadQuadrantRouteConfig doc comment: this route never touches registry
// Postgres.
func loadDrilldownPRsRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildDrilldownPRsRoute constructs the handler, its own routeswitch Mux,
// and a cleanup function. ok is false when this route's dependencies are
// not configured -- main() only calls mux.HandleFunc when ok is true,
// same "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildDrilldownPRsRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadDrilldownPRsRouteConfig()
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

	reader, err := drilldown.NewReader(readClient)
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(drilldownPRsSwitchFromEnv())
	routeMux.Register(drilldownPRsGetOperation, newDrilldownPRsGetHandler(reader))
	routeMux.Register(drilldownPRsPostOperation, newDrilldownPRsPostHandler(reader))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		var operation string
		switch r.Method {
		case http.MethodGet:
			operation = drilldownPRsGetOperation
		case http.MethodPost:
			operation = drilldownPRsPostOperation
		default:
			writeRESTMethodNotAllowed(w, r, "drilldown_prs")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, "drilldown_prs")
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

// drilldownTimeFilterMap adapts GET's own query-param inputs (already
// parsed to a range-days int and optional time.Time dates) into the
// map[string]any shape timeWindow (investment_explain_route.go, this
// package) expects -- the SAME time_window (api/services/filtering.py:
// 78-92) computation POST's raw JSON filters body already flows through
// unchanged. Reusing timeWindow here, rather than a second Go port of
// the same 15-line function, is a deliberate adapter, not indirection
// for its own sake: _filters_from_query (api/main.py:178-199) builds the
// exact same MetricFilter shape GET's handler passes to time_window, so
// this is the one Go function that shape ever reaches either way.
func drilldownTimeFilterMap(rangeDays int, startDate, endDate *time.Time) map[string]any {
	timeFilter := map[string]any{"range_days": float64(rangeDays)}
	if startDate != nil {
		timeFilter["start_date"] = startDate.Format("2006-01-02")
	}
	if endDate != nil {
		timeFilter["end_date"] = endDate.Format("2006-01-02")
	}
	return map[string]any{"time": timeFilter}
}

// writeDrilldownPRsResponse writes resp as the final 200 JSON body via
// json.NewEncoder(w).Encode -- this repo's JSON-response path
// (internal/auth/httpapi/envelope.go's WriteError is the precedent, and
// filter_options_route.go's own writer is the sibling REST-route copy of
// this exact shape), never a raw w.Write of pre-marshalled bytes. The 200
// status is already on the wire by the time Encode runs, so a failure
// here cannot change what the client sees -- but it is NOT silently
// dropped: it is logged with the same request-scoped fields
// query_route.go's digest-miss log line uses (org_id, the caller-supplied
// X-Request-Id), so an operator can correlate a truncated or aborted
// response back to the request that produced it instead of it vanishing.
func writeDrilldownPRsResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *drilldown.PRsResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
		log.Printf("query-api: drilldown: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
	}
}

// newDrilldownPRsGetHandler is the routeswitch-registered handler for
// GET /api/v1/drilldown/prs -- the Go port of drilldown_prs (api/main.py:
// 938-974). Reached only after buildDrilldownPRsRoute's entryHandler has
// already authenticated the request and attached authctx.Claims.
func newDrilldownPRsGetHandler(reader *drilldown.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildDrilldownPRsRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches this
			// handler -- see authenticateRESTRequest's own doc comment for
			// the three real 401 shapes this route actually answers.
			writeRESTUnauthorized(w, r, "drilldown_prs", "Not authenticated")
			return
		}

		query := r.URL.Query()
		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		scopeID := query.Get("scope_id")

		var validationErrors []pydanticErrorDetail

		rangeDays := 14
		if raw := query.Get("range_days"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				// Python's `range_days: int = 14` is FastAPI/Pydantic
				// query-param validation, not a handler-level try/except:
				// a non-numeric value never reaches the handler at all,
				// it is rejected up front as 422 -- confirmed live.
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

		startTS, endTS := timeWindow(drilldownTimeFilterMap(rangeDays, startDatePtr, endDatePtr))

		var scopeIDs []string
		if scopeID != "" {
			scopeIDs = []string{scopeID}
		}

		params := drilldown.PRParams{
			StartDay:   startTS,
			EndDay:     endTS,
			ScopeLevel: scopeType,
			ScopeIDs:   scopeIDs,
			// what.repos is never populated by _filters_from_query
			// (api/main.py:178-199) -- GET has no query param for it.
			WhatRepos: nil,
			// The GET route has no limit query param at all
			// (api/main.py:940-949): fetch_pull_requests is called
			// without a limit= argument, so its own keyword default (50)
			// applies unconditionally.
			Limit: 50,
		}

		resp, err := drilldown.BuildPRsResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:973-974).
			writeRESTDataUnavailable(w, r, "drilldown_prs", claims.OrgID)
			return
		}

		// Python sets this header only on the GET route (main.py:970-971),
		// steering callers toward POST with an explicit filters body.
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeDrilldownPRsResponse(w, r, claims.OrgID, resp)
	}
}

// newDrilldownPRsPostHandler is the routeswitch-registered handler for
// POST /api/v1/drilldown/prs -- the Go port of drilldown_prs_post
// (api/main.py:908-935).
func newDrilldownPRsPostHandler(reader *drilldown.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only -- see the GET handler's own comment above.
			writeRESTUnauthorized(w, r, "drilldown_prs", "Not authenticated")
			return
		}

		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			// A body-read I/O failure (e.g. a client disconnect mid-upload)
			// has no Python counterpart to match -- ASGI/Starlette's own
			// body-reading machinery fails differently and does not surface
			// a clean HTTP response in that case. This keeps the existing
			// 400 status, only replacing the plain-text wire body with this
			// package's one JSON envelope.
			writeRESTError(w, r, "drilldown_prs", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}

		// Pydantic validates the WHOLE body against DrilldownRequest as
		// one model (api/models/filters.py's `filters: MetricFilter`
		// carries no Field(default_factory=...), so it is REQUIRED).
		// Starlette's own Request.json() treats a genuinely empty body as
		// `None`, not a JSON-decode error, and Pydantic then reports the
		// whole model missing (loc ["body"], not ["body","filters"]) --
		// confirmed live, and true for an explicit JSON `null` body too
		// (byte-identical response either way).
		var decoded any
		bodyIsEmptyOrNull := len(bodyBytes) == 0
		if !bodyIsEmptyOrNull {
			if err := json.Unmarshal(bodyBytes, &decoded); err != nil {
				// Genuinely malformed JSON syntax -- jsonSyntaxErrorDetail
				// (pydantic_json_syntax_error.go) reproduces jiter's own
				// message/position for this shape, not a generic
				// placeholder.
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

		filtersValue, hasFilters := body["filters"]
		if !hasFilters {
			validationErrors = append(validationErrors, missingFieldError([]any{"body", "filters"}, body))
		} else {
			validationErrors = append(validationErrors, validateMetricFilter([]any{"body", "filters"}, filtersValue)...)
		}

		if sortValue, hasSort := body["sort"]; hasSort && sortValue != nil {
			if _, isString := sortValue.(string); !isString {
				validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "sort"}, sortValue))
			}
		}

		// Limit ("payload.limit or 50", api/main.py:930): an absent/null
		// limit, or an explicit 0, both fall back to 50; any other
		// successfully-coerced value (including negative) passes through
		// unchanged -- a negative LIMIT then fails at the ClickHouse
		// layer, degrading to the same generic 503 Python's own
		// except-branch would produce for the equivalent invalid-SQL
		// failure. A limit that does NOT coerce to int at all (Pydantic's
		// own lenient int rules) is the class ruling's own "ill-typed
		// POST body field" case: 422, not a silent fallback to 50. Sort
		// is type-checked above but otherwise unused, matching Python's
		// own dead field (parsed by Pydantic, never read by
		// drilldown_prs_post, api/main.py:910-935).
		limit := 0
		if limitValue, hasLimit := body["limit"]; hasLimit {
			coerced, detail := coerceIntBodyField([]any{"body", "limit"}, limitValue)
			if detail != nil {
				validationErrors = append(validationErrors, *detail)
			} else {
				limit = coerced
			}
		}

		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}
		if limit == 0 {
			limit = 50
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

		startTS, endTS := timeWindow(filters)

		params := drilldown.PRParams{
			StartDay:   startTS,
			EndDay:     endTS,
			ScopeLevel: scopeLevel,
			ScopeIDs:   scopeIDs,
			WhatRepos:  whatRepos,
			Limit:      limit,
		}

		resp, err := drilldown.BuildPRsResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			writeRESTDataUnavailable(w, r, "drilldown_prs", claims.OrgID)
			return
		}
		writeDrilldownPRsResponse(w, r, claims.OrgID, resp)
	}
}
