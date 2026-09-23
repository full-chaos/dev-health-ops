// GET+POST /api/v1/drilldown/issues -- the sibling REST route to
// drilldown_prs_route.go, same shape (quadrant_route.go/
// investment_explain_route.go's REST-route pattern), gated by its own
// routeswitch entries (one per method+path, restendpoints.go's own doc
// comment on why matching is by PATH, not (METHOD, PATH) -- both methods
// share this one mux.HandleFunc registration).
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's routes additionally rate-limit ("60/minute",
// main.py:978,1009); this port carries no rate limiter, the same
// documented gap drilldown_prs_route.go/quadrant_route.go/
// investment_explain_route.go already carry.
//
// Business logic (team-scope resolution, the ClickHouse read, the
// ReplacingMergeTree dedup) lives in internal/drilldown -- see issues.go's
// own doc comments for the full parity contract.
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
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// drilldownIssuesGetOperation/drilldownIssuesPostOperation are this
// route's routeswitch operation names -- same PATH+METHOD-keyed
// convention as drilldownPRsGetOperation/drilldownPRsPostOperation.
const (
	drilldownIssuesGetOperation  = "REST:GET:/api/v1/drilldown/issues"
	drilldownIssuesPostOperation = "REST:POST:/api/v1/drilldown/issues"
)

// drilldownIssuesEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves BOTH operations disabled,
// matching DynamicSwitch's own default and drilldownPRsEnabledEnvVar's own
// one-toggle-for-the-pair reasoning.
const drilldownIssuesEnabledEnvVar = "GO_API_DRILLDOWN_ISSUES_ENABLED"

func drilldownIssuesSwitchFromEnv() *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(os.Getenv(drilldownIssuesEnabledEnvVar)); enabled {
		sw.Set(drilldownIssuesGetOperation, true)
		sw.Set(drilldownIssuesPostOperation, true)
	}
	return sw
}

// loadDrilldownIssuesRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as loadDrilldownPRsRouteConfig's own
// doc comment: this route never touches registry Postgres.
func loadDrilldownIssuesRouteConfig() (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = os.Getenv("CLICKHOUSE_URI")
	jwksPath = os.Getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = os.Getenv("GO_API_ENVELOPE_ISSUER")
	audience = os.Getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildDrilldownIssuesRoute constructs the handler, its own routeswitch
// Mux, and a cleanup function. ok is false when this route's dependencies
// are not configured -- main() only calls mux.HandleFunc when ok is true,
// same "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildDrilldownIssuesRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadDrilldownIssuesRouteConfig()
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

	reader, err := drilldown.NewReader(readClient)
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(drilldownIssuesSwitchFromEnv())
	routeMux.Register(drilldownIssuesGetOperation, newDrilldownIssuesGetHandler(reader))
	routeMux.Register(drilldownIssuesPostOperation, newDrilldownIssuesPostHandler(reader))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		var operation string
		switch r.Method {
		case http.MethodGet:
			operation = drilldownIssuesGetOperation
		case http.MethodPost:
			operation = drilldownIssuesPostOperation
		default:
			writeRESTMethodNotAllowed(w, r, "drilldown_issues")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "drilldown_issues")
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

// writeDrilldownIssuesResponse writes resp as the final 200 JSON body via
// json.NewEncoder(w).Encode -- same house JSON-response path
// writeDrilldownPRsResponse documents, never a raw w.Write of
// pre-marshalled bytes.
func writeDrilldownIssuesResponse(w http.ResponseWriter, r *http.Request, orgID string, resp *drilldown.IssuesResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if encodeErr := json.NewEncoder(w).Encode(resp); encodeErr != nil {
		log.Printf("query-api: drilldown: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
	}
}

// newDrilldownIssuesGetHandler is the routeswitch-registered handler for
// GET /api/v1/drilldown/issues -- the Go port of drilldown_issues
// (api/main.py:1008-1045). Reached only after buildDrilldownIssuesRoute's
// entryHandler has already authenticated the request and attached
// authctx.Claims.
func newDrilldownIssuesGetHandler(reader *drilldown.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildDrilldownIssuesRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches this
			// handler -- see authenticateRESTRequest's own doc comment for
			// the three real 401 shapes this route actually answers.
			writeRESTUnauthorized(w, r, "drilldown_issues", "Not authenticated")
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
				// query-param validation, not a handler-level try/except --
				// same confirmed-live contract drilldown_prs_route.go's own
				// GET handler documents.
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

		params := drilldown.IssueParams{
			StartDay:   startTS,
			EndDay:     endTS,
			ScopeLevel: scopeType,
			ScopeIDs:   scopeIDs,
			// The GET route has no limit query param at all
			// (api/main.py:1010-1019): fetch_issues is called without a
			// limit= argument, so its own keyword default (50) applies
			// unconditionally -- same contract drilldownPRsGetHandler
			// documents for its own sibling call.
			Limit: 50,
		}

		resp, err := drilldown.BuildIssuesResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1044-1045).
			writeRESTDataUnavailable(w, r, "drilldown_issues", claims.OrgID, err)
			return
		}

		// Python sets this header only on the GET route (main.py:1041-1042),
		// steering callers toward POST with an explicit filters body.
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeDrilldownIssuesResponse(w, r, claims.OrgID, resp)
	}
}

// newDrilldownIssuesPostHandler is the routeswitch-registered handler for
// POST /api/v1/drilldown/issues -- the Go port of drilldown_issues_post
// (api/main.py:977-1005). Body parsing/validation is BYTE-FOR-BYTE the
// same shape newDrilldownPRsPostHandler already establishes (both routes
// share the exact same DrilldownRequest/MetricFilter Pydantic models, see
// api/main.py:977-982 vs 908-913): the difference between the two routes
// is entirely in what PARAMS get built from the validated body, not in how
// the body is validated.
func newDrilldownIssuesPostHandler(reader *drilldown.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only -- see the GET handler's own comment above.
			writeRESTUnauthorized(w, r, "drilldown_issues", "Not authenticated")
			return
		}

		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			// A body-read I/O failure has no Python counterpart to match --
			// see drilldown_prs_route.go's own POST handler for the same
			// reasoning. Status unchanged, only the wire body's encoding.
			writeRESTError(w, r, "drilldown_issues", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}

		// See newDrilldownPRsPostHandler's own doc comment for the
		// confirmed-live empty-body/null-body/malformed-JSON contract this
		// block reproduces verbatim.
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

		if sortValue, hasSort := body.Get("sort"); hasSort && sortValue != nil {
			if _, isString := sortValue.(string); !isString {
				validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "sort"}, sortValue))
			}
		}

		// Limit ("payload.limit or 50", api/main.py:1000): same
		// absent/null/zero-falls-back-to-50 contract
		// newDrilldownPRsPostHandler documents; sort is type-checked above
		// but otherwise unused here too (api/main.py:979-1005 never reads
		// payload.sort).
		limit := 0
		if limitValue, hasLimit := body.Get("limit"); hasLimit {
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

		filters, _ := legacyJSON(filtersValue).(map[string]any)
		scope, _ := filters["scope"].(map[string]any)
		scopeLevel, _ := scope["level"].(string)
		if scopeLevel == "" {
			scopeLevel = "org"
		}
		scopeIDs := stringsFromAny(scope["ids"])

		startTS, endTS := timeWindow(filters)

		params := drilldown.IssueParams{
			StartDay:   startTS,
			EndDay:     endTS,
			ScopeLevel: scopeLevel,
			ScopeIDs:   scopeIDs,
			Limit:      limit,
		}

		resp, err := drilldown.BuildIssuesResponse(r.Context(), reader, claims.OrgID, params)
		if err != nil {
			writeRESTDataUnavailable(w, r, "drilldown_issues", claims.OrgID, err)
			return
		}
		writeDrilldownIssuesResponse(w, r, claims.OrgID, resp)
	}
}
