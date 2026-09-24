// GET+POST /api/v1/work-units -- the Go REST handler mirroring
// drilldown_prs_route.go/investment_explain_route.go's shape for a REST
// route mounted on query-api's mux, gated by its own routeswitch entries
// (one per method+path, same convention as every sibling REST route in
// this binary).
//
// Auth: the same bearer-envelope verifier every other REST route in this
// binary uses. Python's work_units/work_units_post carry no rate limiter
// (grepped api/main.py -- no @limiter.limit decorator on either), so
// this port needs no documented rate-limiting gap, unlike investment/
// explain or drilldown's own POST siblings.
//
// Business logic (repo-scope resolution, category-filter split, the
// ClickHouse reads and their ReplacingMergeTree dedup, the per-unit
// evidence/team assembly) lives in internal/queryapi/investmentexplain
// -- (*Reader).BuildWorkUnitInvestments already ports
// build_work_unit_investments (work_units.py:234-460) in full, reused
// here rather than duplicated: it was built for POST /api/v1/investment/
// explain's own single-work-unit lookup (work_unit_id set, limit=1), and
// this route is the SAME function called with a list-shaped limit and no
// work_unit_id filter instead. This file is only the HTTP-layer wiring --
// parameter/body parsing, auth, and the WorkUnitInvestment wire encoding
// investment/explain never needed (it serializes a WorkUnitExplanation,
// not the investment record itself).
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
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// workUnitsGetOperation/workUnitsPostOperation are this route's
// routeswitch operation names -- PATH+METHOD-keyed entries, same
// convention as drilldownPRsGetOperation/drilldownPRsPostOperation (a
// REST route has no GraphQL document to digest).
const (
	workUnitsGetOperation  = "REST:GET:/api/v1/work-units"
	workUnitsPostOperation = "REST:POST:/api/v1/work-units"
)

// workUnitsEnabledEnvVar is the operator-facing toggle -- unset or
// anything other than a true-ish value leaves BOTH operations disabled,
// matching DynamicSwitch's own default. One toggle for the GET/POST
// pair: they share one handler package and one deploy unit (this file),
// same as drilldownPRsEnabledEnvVar's own reasoning.
const workUnitsEnabledEnvVar = "GO_API_WORK_UNITS_ENABLED"

func workUnitsSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(workUnitsEnabledEnvVar)); enabled {
		sw.Set(workUnitsGetOperation, true)
		sw.Set(workUnitsPostOperation, true)
	}
	return sw
}

// workUnitsMaxLimit ports work_units.py's WORK_UNITS_MAX_LIMIT (api/main.py:163).
const workUnitsMaxLimit = 1000

// boundedWorkUnitsLimit ports _bounded_limit_param(limit, WORK_UNITS_MAX_LIMIT)
// (api/main.py:211-214) exactly: a non-positive limit falls back to 50
// (always within range here, since workUnitsMaxLimit > 50), otherwise
// the limit is clamped to [1, workUnitsMaxLimit] -- the caller only ever
// passes a limit already >= 1 down this branch, so the max(limit,1) half
// of Python's min(max(limit,1),max_limit) is a no-op here.
func boundedWorkUnitsLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > workUnitsMaxLimit {
		return workUnitsMaxLimit
	}
	return limit
}

// loadWorkUnitsRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as drilldown_prs_route.go's own
// loadDrilldownPRsRouteConfig doc comment: this route never touches
// registry Postgres.
func loadWorkUnitsRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildWorkUnitsRoute constructs the handler, its own routeswitch Mux,
// and a cleanup function. ok is false when this route's dependencies are
// not configured -- main() only calls mux.HandleFunc when ok is true,
// same "stay unmounted, don't fail to build/start" contract every other
// optionally-configured route in this binary follows.
func buildWorkUnitsRoute(getenv getenvFunc) (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadWorkUnitsRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, false, err
	}

	// Optional -- nil whenever the pod has not been given the edge
	// credential's key material, in which case authenticateRESTRequest
	// falls back to its pre-existing envelope-only behaviour.
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, false, err
	}

	reader, err := investmentexplain.NewReader(analytics.PinInvestmentMembershipScope(readClient))
	if err != nil {
		_ = readClient.Close()
		return nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(workUnitsSwitchFromEnv(getenv))
	routeMux.Register(workUnitsGetOperation, newWorkUnitsGetHandler(reader))
	routeMux.Register(workUnitsPostOperation, newWorkUnitsPostHandler(reader))

	// Auth runs BEFORE Dispatch, same order every other route in this
	// binary uses.
	entryHandler := func(w http.ResponseWriter, r *http.Request) {
		var operation string
		switch r.Method {
		case http.MethodGet:
			operation = workUnitsGetOperation
		case http.MethodPost:
			operation = workUnitsPostOperation
		default:
			writeRESTMethodNotAllowed(w, r, "work_units")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "work_units")
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

// --- wire response shape: api/models/schemas.py's WorkUnitInvestment ---

// workUnitTimeRangeWire/workUnitEffortWire/evidenceQualityWire/
// investmentBreakdownWire/workUnitEvidenceWire/workUnitInvestmentWire
// mirror api/models/schemas.py's WorkUnitTimeRange/WorkUnitEffort/
// EvidenceQuality/InvestmentBreakdown/WorkUnitEvidence/WorkUnitInvestment
// field for field and name for name -- Pydantic emits these with no
// alias generator, so the JSON tags ARE the Python field names
// (confirmed live via jsonable_encoder against a hand-built model, this
// route's own TEST-EVIDENCE).
type workUnitTimeRangeWire struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type workUnitEffortWire struct {
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
}

type evidenceQualityWire struct {
	Value *float64 `json:"value"`
	Band  *string  `json:"band"`
}

type investmentBreakdownWire struct {
	Themes        map[string]float64 `json:"themes"`
	Subcategories map[string]float64 `json:"subcategories"`
}

type workUnitEvidenceWire struct {
	Textual    []map[string]any `json:"textual"`
	Structural []map[string]any `json:"structural"`
	Contextual []map[string]any `json:"contextual"`
}

type workUnitInvestmentWire struct {
	WorkUnitID      string                  `json:"work_unit_id"`
	WorkUnitType    *string                 `json:"work_unit_type"`
	WorkUnitName    *string                 `json:"work_unit_name"`
	TimeRange       workUnitTimeRangeWire   `json:"time_range"`
	Effort          workUnitEffortWire      `json:"effort"`
	Investment      investmentBreakdownWire `json:"investment"`
	EvidenceQuality evidenceQualityWire     `json:"evidence_quality"`
	Evidence        workUnitEvidenceWire    `json:"evidence"`
}

// formatWorkUnitTimestamp ports Pydantic v2's JSON-mode datetime
// serialization for an already-UTC-aware value -- confirmed live (this
// route's own TEST-EVIDENCE): fractional seconds appear as a fixed
// 6-digit, UNSTRIPPED microsecond count only when non-zero, and the
// offset is always the "Z" shorthand for UTC, never "+00:00". This is a
// DIFFERENT wire convention from investmentexplain's own
// pythonIsoFormat (that package's cache-key/explanation-cache text
// matches plain datetime.isoformat()'s "-07:00"-style offset instead) --
// the two are not interchangeable, and this route needs its own because
// it is the first to put a WorkUnitInvestment on the wire at all.
func formatWorkUnitTimestamp(t time.Time) string {
	t = t.UTC()
	base := t.Format("2006-01-02T15:04:05")
	if t.Nanosecond() != 0 {
		base += fmt.Sprintf(".%06d", t.Nanosecond()/1000)
	}
	return base + "Z"
}

// toWorkUnitInvestmentWire converts investmentexplain's already-assembled
// WorkUnitInvestment (BuildWorkUnitInvestments' own return type) into the
// wire shape. Themes/Subcategories fold the ordered []keyValue pairs
// BuildWorkUnitInvestments returns (needed internally for its own
// tie-break logic) into a plain map -- a JSON object's member order is
// not a comparable property (RFC 8259 s6), so collapsing to Go's
// map[string]float64 (alphabetical on the wire, unlike Python's
// insertion-ordered dict) changes nothing any decode-based comparison
// can observe. Every evidence list defaults to an empty, non-nil slice,
// matching WorkUnitEvidence's own Field(default_factory=list) -- Go's
// encoding/json renders a nil slice as `null`, and Python's response
// never does.
func toWorkUnitInvestmentWire(investment investmentexplain.WorkUnitInvestment) workUnitInvestmentWire {
	themes := map[string]float64{}
	for _, kv := range investment.Investment.Themes {
		themes[kv.Key] = kv.Value
	}
	subcategories := map[string]float64{}
	for _, kv := range investment.Investment.Subcategories {
		subcategories[kv.Key] = kv.Value
	}

	textual := investment.Evidence.Textual
	if textual == nil {
		textual = []map[string]any{}
	}
	structural := investment.Evidence.Structural
	if structural == nil {
		structural = []map[string]any{}
	}
	contextual := investment.Evidence.Contextual
	if contextual == nil {
		contextual = []map[string]any{}
	}

	var band *string
	if investment.EvidenceQuality.Band != nil {
		b := *investment.EvidenceQuality.Band
		band = &b
	}

	return workUnitInvestmentWire{
		WorkUnitID:   investment.WorkUnitID,
		WorkUnitType: investment.WorkUnitType,
		WorkUnitName: investment.WorkUnitName,
		TimeRange: workUnitTimeRangeWire{
			Start: formatWorkUnitTimestamp(investment.TimeRange.Start),
			End:   formatWorkUnitTimestamp(investment.TimeRange.End),
		},
		Effort: workUnitEffortWire{
			Metric: investment.Effort.Metric,
			Value:  investment.Effort.Value,
		},
		Investment: investmentBreakdownWire{
			Themes:        themes,
			Subcategories: subcategories,
		},
		EvidenceQuality: evidenceQualityWire{
			Value: investment.EvidenceQuality.Value,
			Band:  band,
		},
		Evidence: workUnitEvidenceWire{
			Textual:    textual,
			Structural: structural,
			Contextual: contextual,
		},
	}
}

// writeWorkUnitsResponse writes investments as the final 200 JSON body --
// a plain JSON array (response_model=list[WorkUnitInvestment], api/
// main.py:607,643 -- no envelope object around it), via
// json.NewEncoder(w).Encode, this repo's JSON-response path, never a raw
// w.Write of pre-marshalled bytes. The list itself is always a non-nil
// slice (built below with make(...,0,...)), so zero results render `[]`,
// matching Python's own `return []` (work_units.py:266).
func writeWorkUnitsResponse(w http.ResponseWriter, r *http.Request, orgID string, investments []investmentexplain.WorkUnitInvestment) {
	wire := make([]workUnitInvestmentWire, 0, len(investments))
	for _, investment := range investments {
		wire = append(wire, toWorkUnitInvestmentWire(investment))
	}
	if encodeErr := writeModelResponse(w, wire); encodeErr != nil {
		log.Printf("query-api: work_units: encode response failed: org_id=%s request_id=%s err=%v",
			orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}

// newWorkUnitsGetHandler is the routeswitch-registered handler for
// GET /api/v1/work-units -- the Go port of work_units (api/main.py:
// 643-685). Reached only after buildWorkUnitsRoute's entryHandler has
// already authenticated the request and attached authctx.Claims.
func newWorkUnitsGetHandler(reader *investmentexplain.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only: buildWorkUnitsRoute's entryHandler always
			// authenticates and attaches claims before Dispatch reaches this
			// handler -- see authenticateRESTRequest's own doc comment for
			// the three real 401 shapes this route actually answers.
			writeRESTUnauthorized(w, r, "work_units", "Not authenticated")
			return
		}

		query := r.URL.Query()
		// scope_type/scope_id: matching drilldown_prs_route.go's own GET
		// handler convention, an explicit empty value collapses to "absent"
		// for scope_type -- str fields have no FastAPI-side empty-string
		// rejection the way an int/bool/date field does, so this loses
		// nothing a real caller would notice.
		scopeType := query.Get("scope_type")
		if scopeType == "" {
			scopeType = "org"
		}
		scopeID := query.Get("scope_id")

		var validationErrors []pydanticErrorDetail

		rangeDays := 14
		if query.Has("range_days") {
			raw := query.Get("range_days")
			parsed, parseErr := parseQueryInt([]any{"query", "range_days"}, raw)
			if parseErr != nil {
				// Confirmed live: an explicit-but-empty value ALSO fails
				// int parsing (FastAPI does not treat "" as absent for an
				// int query param), unlike a bare string field -- query.Has
				// (not a raw != "" check) is what lets an empty value reach
				// strconv.Atoi and fail here instead of silently keeping
				// the default.
				validationErrors = append(validationErrors, *parseErr)
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

		limit := 200
		if query.Has("limit") {
			raw := query.Get("limit")
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				validationErrors = append(validationErrors, intQueryParamError([]any{"query", "limit"}, raw))
			} else {
				limit = parsed
			}
		}

		includeTextual := true
		if query.Has("include_textual") {
			raw := query.Get("include_textual")
			// coerceBoolBodyField's string branch (pydantic_metric_filter.go)
			// reproduces the identical lenient-bool string set Pydantic's
			// `bool` field type accepts, confirmed live to behave the same
			// way whether the string arrives via a JSON body or (as here) a
			// raw query value -- the wire representation is identical, a
			// bare string, and pydantic-core's own bool validator does not
			// branch on which part of the request it came from.
			coerced, _, detail := coerceBoolBodyField([]any{"query", "include_textual"}, raw)
			if detail != nil {
				validationErrors = append(validationErrors, *detail)
			} else {
				includeTextual = coerced
			}
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

		// drilldownTimeFilterMap (drilldown_prs_route.go) builds the exact
		// map[string]any{"time": {...}} shape timeWindow expects from a
		// range-days/start/end triple -- the SAME time_window computation
		// _filters_from_query (api/main.py:178-199) feeds into for this
		// route's own GET handler in Python, reused rather than re-built.
		startTS, endTS, windowErr := timeWindow(drilldownTimeFilterMap(rangeDays, startDatePtr, endDatePtr))
		if windowErr != nil {
			writeTimeWindowOverflow(w, r, "work_units", claims.OrgID)
			return
		}

		var scopeIDs []string
		if scopeID != "" {
			scopeIDs = []string{scopeID}
		}

		// _filters_from_query never populates what.repos for the GET route
		// (no query param for it), matching drilldown_prs' own GET handler.
		repoIDs, err := reader.ResolveRepoFilterIDs(r.Context(), scopeType, scopeIDs, nil, claims.OrgID)
		if err != nil {
			writeRESTDataUnavailable(w, r, "work_units", claims.OrgID, err)
			return
		}
		var teamCondition string
		var teamBindings []dhclickhouse.Binding
		if scopeType == "team" && len(scopeIDs) > 0 {
			teamCondition, teamBindings = teamscope.RepoCondition(claims.OrgID, repoScopeColumn, scopeIDs, time.Now().UTC())
		}

		investments, err := reader.BuildWorkUnitInvestments(r.Context(), investmentexplain.BuildWorkUnitInvestmentsOptions{
			OrgID:              claims.OrgID,
			StartTS:            startTS,
			EndTS:              endTS,
			RepoIDs:            repoIDs,
			TeamScopeCondition: teamCondition,
			TeamScopeBindings:  teamBindings,
			Limit:              boundedWorkUnitsLimit(limit),
			IncludeText:        includeTextual,
			// _filters_from_query never populates why.work_category for the
			// GET route (no query param for it), so ThemeFilters/
			// SubcategoryFilters stay nil/empty here -- every unit passes
			// the category filter unconditionally, matching
			// _matches_category_filter's own true-when-both-empty rule.
		})
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (api/main.py:683-685).
			writeRESTDataUnavailable(w, r, "work_units", claims.OrgID, err)
			return
		}

		// Python sets this header only on the GET route (api/main.py:
		// 680-681), steering callers toward POST with an explicit filters
		// body.
		w.Header().Set("X-DevHealth-Deprecated", "use POST with filters")
		writeWorkUnitsResponse(w, r, claims.OrgID, investments)
	}
}

// decodeWorkUnitsRequestBody decodes bodyBytes into the fields
// WorkUnitRequest declares (api/models/filters.py:90-93), in that exact
// field order (filters, limit, include_textual) -- FastAPI aggregates
// every simultaneously-invalid field into one 422 response in the
// endpoint's own field-declaration order (pydantic_validation_error.go's
// own doc comment), so this function's error-collection order matters,
// not only its correctness.
//
// bodyIsEmptyOrNull/malformed-JSON/non-object handling mirrors
// drilldown_prs_route.go's own POST handler exactly (DrilldownRequest and
// WorkUnitRequest share the identical "a required top-level MetricFilter
// field, nothing else required" shape) -- confirmed live there already,
// not re-derived here.
func decodeWorkUnitsRequestBody(bodyBytes []byte) (filters map[string]any, limit int, includeTextual bool, validationErrors []pydanticErrorDetail, bodyErr *pydanticErrorDetail) {
	decoded, bodyIsEmptyOrNull, syntaxDetail := decodeRequestBody([]any{"body"}, bodyBytes)
	if syntaxDetail != nil {
		return nil, 0, false, nil, syntaxDetail
	}
	if bodyIsEmptyOrNull {
		detail := missingFieldError([]any{"body"}, nil)
		return nil, 0, false, nil, &detail
	}

	body, isObject := decoded.(*pyjson.Object)
	if !isObject {
		detail := modelAttributesTypeError([]any{"body"}, decoded)
		return nil, 0, false, nil, &detail
	}

	var errs []pydanticErrorDetail

	filtersValue, hasFilters := body.Get("filters")
	if !hasFilters {
		errs = append(errs, missingFieldError([]any{"body", "filters"}, body))
	} else {
		errs = append(errs, validateMetricFilter([]any{"body", "filters"}, filtersValue)...)
	}

	// limit: int | None = None (api/models/filters.py:92) -- an absent or
	// null value keeps includeTextualDefault-style fallback (Python's
	// `payload.limit or 200`, api/main.py:620) applied by the caller, not
	// here; this function only reports a present-but-ill-typed value.
	limitSet := false
	if limitValue, hasLimit := body.Get("limit"); hasLimit && limitValue != nil {
		coerced, detail := coerceIntBodyField([]any{"body", "limit"}, limitValue)
		if detail != nil {
			errs = append(errs, *detail)
		} else {
			limit = coerced
			limitSet = true
		}
	}

	includeTextual = true
	if includeTextualValue, hasIncludeTextual := body.Get("include_textual"); hasIncludeTextual && includeTextualValue != nil {
		coerced, _, detail := coerceBoolBodyField([]any{"body", "include_textual"}, includeTextualValue)
		if detail != nil {
			errs = append(errs, *detail)
		} else {
			includeTextual = coerced
		}
	}

	if len(errs) > 0 {
		return nil, 0, false, errs, nil
	}

	filters, _ = legacyJSON(filtersValue).(map[string]any)
	if !limitSet {
		limit = 0
	}
	return filters, limit, includeTextual, nil, nil
}

// newWorkUnitsPostHandler is the routeswitch-registered handler for
// POST /api/v1/work-units -- the Go port of work_units_post (api/main.py:
// 607-640).
func newWorkUnitsPostHandler(reader *investmentexplain.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			// Defensive only -- see the GET handler's own comment above.
			writeRESTUnauthorized(w, r, "work_units", "Not authenticated")
			return
		}

		bodyBytes, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			// A body-read I/O failure has no Python counterpart to match --
			// same reasoning as drilldown_prs_route.go's own POST handler.
			writeRESTError(w, r, "work_units", claims.OrgID, http.StatusBadRequest, "bad request")
			return
		}

		filters, rawLimit, includeTextual, validationErrors, bodyErr := decodeWorkUnitsRequestBody(bodyBytes)
		if bodyErr != nil {
			writePydanticValidationError(w, r, claims.OrgID, *bodyErr)
			return
		}
		if len(validationErrors) > 0 {
			writePydanticValidationError(w, r, claims.OrgID, validationErrors...)
			return
		}

		// `payload.limit or 200` (api/main.py:620): an absent/null/zero
		// limit all fall back to 200 -- Python's `or` treats 0 as falsy,
		// same as decodeWorkUnitsRequestBody's own limitSet=false case
		// (absent/null) folds into rawLimit==0 here.
		if rawLimit == 0 {
			rawLimit = 200
		}

		startTS, endTS, windowErr := timeWindow(filters)
		if windowErr != nil {
			writeTimeWindowOverflow(w, r, "work_units", claims.OrgID)
			return
		}

		// scopeRepoFilter (investment_explain_route.go) already ports
		// resolve_repo_filter_ids's full team-scope branch over a raw
		// filters map -- reused here rather than a second copy.
		repoIDs, teamCondition, teamBindings, err := scopeRepoFilter(r.Context(), reader, filters, claims.OrgID, time.Now().UTC())
		if err != nil {
			writeRESTDataUnavailable(w, r, "work_units", claims.OrgID, err)
			return
		}

		themeFilters, subcategoryFilters := investmentexplain.SplitCategoryFilters(workCategoryFromFilters(filters))

		investments, err := reader.BuildWorkUnitInvestments(r.Context(), investmentexplain.BuildWorkUnitInvestmentsOptions{
			OrgID:              claims.OrgID,
			StartTS:            startTS,
			EndTS:              endTS,
			RepoIDs:            repoIDs,
			TeamScopeCondition: teamCondition,
			TeamScopeBindings:  teamBindings,
			Limit:              boundedWorkUnitsLimit(rawLimit),
			IncludeText:        includeTextual,
			ThemeFilters:       themeFilters,
			SubcategoryFilters: subcategoryFilters,
		})
		if err != nil {
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (api/main.py:638-640).
			writeRESTDataUnavailable(w, r, "work_units", claims.OrgID, err)
			return
		}

		writeWorkUnitsResponse(w, r, claims.OrgID, investments)
	}
}
