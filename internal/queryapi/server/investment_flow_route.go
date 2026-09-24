// POST /api/v1/investment/flow and POST /api/v1/investment/flow/repo-team
// -- the Go REST handlers mirroring sankey_route.go/investment_explain_
// route.go's shape for a REST route pair mounted on query-api's mux, each
// path its own routeswitch operation, sharing one env toggle
// (GO_API_INVESTMENT_FLOW_ENABLED) since both routes always ship
// together in this one PR.
//
// Auth: the same bearer-envelope verifier every REST route in this
// binary uses. Python's /investment/flow additionally rate-limits
// ("20/minute", main.py:1355) and /investment/flow/repo-team does not
// (main.py:1379 carries no @limiter.limit decorator at all) -- this port
// carries no rate limiter for either, the same documented gap every
// sibling REST route in this binary already carries (see sankey_route.go's
// own package doc comment for the identical asymmetric-rate-limit
// precedent).
//
// ERROR SHAPE (genuinely asymmetric between the two routes, main.py:
// 1354-1394): investment_flow's own handler catches ValueError -> 400
// with str(exc) as the detail, everything else -> 503 "Data unavailable";
// investment_flow_repo_team's handler has NO ValueError branch at all,
// `except Exception: 503` catches everything, ValueError included. This
// file's two handlers preserve that asymmetry via internal/investmentflow
// .AsRequestError -- only the /flow handler checks for it.
//
// Business logic (schema-presence guard, flow_mode dispatch, the dynamic
// coverage-driven mode decision, the ClickHouse reads and their
// ReplacingMergeTree dedup fixes) lives in internal/investmentflow -- see
// that package's own doc comment for the full parity contract.
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
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentflow"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/sankey"
)

const (
	investmentFlowOperation         = "REST:POST:/api/v1/investment/flow"
	investmentFlowRepoTeamOperation = "REST:POST:/api/v1/investment/flow/repo-team"
)

const investmentFlowEnabledEnvVar = "GO_API_INVESTMENT_FLOW_ENABLED"

func investmentFlowSwitchFromEnv(getenv getenvFunc) *routeswitch.DynamicSwitch {
	sw := routeswitch.NewDynamicSwitch()
	if enabled, _ := strconv.ParseBool(getenv(investmentFlowEnabledEnvVar)); enabled {
		sw.Set(investmentFlowOperation, true)
		sw.Set(investmentFlowRepoTeamOperation, true)
	}
	return sw
}

// loadInvestmentFlowRouteConfig reads this route's own dependency set --
// ClickHouse plus the envelope verifier trio. Deliberately NOT
// loadQueryRouteConfig, same reasoning as quadrant_route.go/sankey_route.go's
// own loadSankeyRouteConfig doc comment: this route never touches registry
// Postgres.
func loadInvestmentFlowRouteConfig(getenv getenvFunc) (clickHouseURI, jwksPath, issuer, audience string, ok bool) {
	clickHouseURI = getenv("CLICKHOUSE_URI")
	jwksPath = getenv("GO_API_ENVELOPE_JWKS_PATH")
	issuer = getenv("GO_API_ENVELOPE_ISSUER")
	audience = getenv("GO_API_ENVELOPE_AUDIENCE")
	if clickHouseURI == "" || jwksPath == "" || issuer == "" || audience == "" {
		return "", "", "", "", false
	}
	return clickHouseURI, jwksPath, issuer, audience, true
}

// buildInvestmentFlowRoute constructs both routes' handlers off one
// shared ClickHouse read client and envelope verifier, its own
// routeswitch Mux, and one cleanup function -- same "stay unmounted,
// don't fail to build/start" contract every other route file in this
// package follows.
func buildInvestmentFlowRoute(getenv getenvFunc) (flowHandler, repoTeamHandler http.HandlerFunc, cleanup func(), ok bool, err error) {
	clickHouseURI, jwksPath, issuer, audience, cfgOK := loadInvestmentFlowRouteConfig(getenv)
	if !cfgOK {
		return nil, nil, nil, false, nil
	}

	verifier, err := principal.NewVerifier(jwksPath, issuer, audience)
	if err != nil {
		return nil, nil, nil, false, err
	}

	// Optional -- nil whenever the pod has not been given the edge
	// credential's key material, in which case authenticateRESTRequest
	// falls back to its pre-existing envelope-only behaviour. See
	// buildEdgeVerifierFromEnv's own doc comment for the pod env
	// contract this reads.
	edgeVerifier, err := buildEdgeVerifierFromEnv(getenv)
	if err != nil {
		return nil, nil, nil, false, err
	}

	readClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(clickHouseURI))
	if err != nil {
		return nil, nil, nil, false, err
	}

	routeMux := routeswitch.NewMux(investmentFlowSwitchFromEnv(getenv))
	routeMux.Register(investmentFlowOperation, newInvestmentFlowHandler(analytics.PinInvestmentMembershipScope(readClient)))
	routeMux.Register(investmentFlowRepoTeamOperation, newInvestmentFlowRepoTeamHandler(analytics.PinInvestmentMembershipScope(readClient)))

	flowEntry := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTMethodNotAllowed(w, r, "investment_flow")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "investment_flow")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(investmentFlowOperation, w, r)
	}
	repoTeamEntry := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeRESTMethodNotAllowed(w, r, "investment_flow_repo_team")
			return
		}
		claims, ok := authenticateRESTRequest(w, r, verifier, edgeVerifier, "investment_flow_repo_team")
		if !ok {
			return
		}
		r = r.WithContext(authctx.WithClaims(r.Context(), claims))
		routeMux.Dispatch(investmentFlowRepoTeamOperation, w, r)
	}

	cleanupFn := func() {
		_ = readClient.Close()
	}
	return flowEntry, repoTeamEntry, cleanupFn, true, nil
}

// investmentFlowModeValues mirrors InvestmentFlowRequest.flow_mode's
// Literal set (api/models/filters.py:73-78) verbatim.
var investmentFlowModeValues = []string{"team_category_repo", "team_subcategory_repo", "team_category_subcategory_repo"}

// investmentFlowRequestBody is the parsed, validated shape of
// InvestmentFlowRequest (api/models/filters.py:69-80) both routes share
// on the wire -- investment_flow_repo_team only ever reads Filters/Theme
// out of it (main.py:1381-1394), FlowMode/DrillCategory/TopNRepos are
// still validated (a malformed value on either route is still a real
// Pydantic 422 -- the model is shared, not narrowed per-route) but
// silently unused by that route's own handler below.
type investmentFlowRequestBody struct {
	Filters       map[string]any
	Theme         *string
	FlowMode      *string
	DrillCategory *string
	TopNRepos     int
}

// writeInvestmentFlowResponse answers with the shared sankey.Response
// JSON shape both routes emit -- same Content-Type/encode-error-log
// convention as sankey_route.go's own writeSankeyResponse.
func writeInvestmentFlowResponse(w http.ResponseWriter, r *http.Request, component, orgID string, resp *sankey.Response) {
	if encodeErr := writeModelResponse(w, resp); encodeErr != nil {
		log.Printf("query-api: %s: encode response failed: org_id=%s request_id=%s err=%v",
			component, orgID, envelopeRequestID(r), encodeErr)
		writeModelFailure(w)
	}
}

// parseInvestmentFlowRequestBody decodes and validates a request body
// against InvestmentFlowRequest's schema, in the model's own declared
// field order (filters, theme, flow_mode, drill_category, top_n_repos) --
// confirmed live (uncommitted FastAPI TestClient probes against the real
// model, see this file's TEST-EVIDENCE): an absent body is a top-level
// "missing" 422 (input=None); {} is a valid, all-defaults body (200);
// filters: null is model_attributes_type (the field has a default
// FACTORY, not an Optional type, so an explicit null is rejected the
// same way a non-object value is, unlike this binary's shared
// validateMetricFilter's own value==nil no-op branch, which assumes an
// absent-vs-null distinction its caller already made -- see the local
// override below); theme/drill_category: null IS valid (both genuinely
// `str | None = None`); top_n_repos: 12 has a default for an ABSENT key
// but, like filters, is not Optional -- an explicit null is int_type,
// not a silent fall-back to the default (confirmed live, same file's
// TEST-EVIDENCE); a PRESENT, non-null value accepts the same lenient int
// coercion (numeric string, whole-number float, bool) every other
// int-typed body field in this binary already does.
func parseInvestmentFlowRequestBody(w http.ResponseWriter, r *http.Request, orgID string) (investmentFlowRequestBody, bool) {
	bodyBytes, readErr := io.ReadAll(r.Body)
	if readErr != nil {
		writeRESTError(w, r, "investment_flow", orgID, http.StatusBadRequest, "bad request")
		return investmentFlowRequestBody{}, false
	}

	decoded, bodyIsEmptyOrNull, syntaxDetail := decodeRequestBody([]any{"body"}, bodyBytes)
	if syntaxDetail != nil {
		writePydanticValidationError(w, r, orgID, *syntaxDetail)
		return investmentFlowRequestBody{}, false
	}
	if bodyIsEmptyOrNull {
		writePydanticValidationError(w, r, orgID, missingFieldError([]any{"body"}, nil))
		return investmentFlowRequestBody{}, false
	}

	body, isObject := decoded.(*pyjson.Object)
	if !isObject {
		writePydanticValidationError(w, r, orgID, modelAttributesTypeError([]any{"body"}, decoded))
		return investmentFlowRequestBody{}, false
	}

	var validationErrors []pydanticErrorDetail
	result := investmentFlowRequestBody{TopNRepos: 12}

	if filtersValue, hasFilters := body.Get("filters"); hasFilters && filtersValue == nil {
		validationErrors = append(validationErrors, modelAttributesTypeError([]any{"body", "filters"}, nil))
	} else if hasFilters {
		validationErrors = append(validationErrors, validateMetricFilter([]any{"body", "filters"}, filtersValue)...)
		if filtersMap, ok := legacyJSON(filtersValue).(map[string]any); ok {
			result.Filters = filtersMap
		}
	}

	if themeValue, hasTheme := body.Get("theme"); hasTheme && themeValue != nil {
		if s, isString := themeValue.(string); isString {
			result.Theme = &s
		} else {
			validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "theme"}, themeValue))
		}
	}

	if flowModeValue, hasFlowMode := body.Get("flow_mode"); hasFlowMode && flowModeValue != nil {
		if s, isString := flowModeValue.(string); isString && stringInSlice(s, investmentFlowModeValues) {
			result.FlowMode = &s
		} else {
			validationErrors = append(validationErrors, literalErrorDetail([]any{"body", "flow_mode"}, flowModeValue, investmentFlowModeValues))
		}
	}

	if drillValue, hasDrill := body.Get("drill_category"); hasDrill && drillValue != nil {
		if s, isString := drillValue.(string); isString {
			result.DrillCategory = &s
		} else {
			validationErrors = append(validationErrors, stringBodyFieldError([]any{"body", "drill_category"}, drillValue))
		}
	}

	if topNValue, hasTopN := body.Get("top_n_repos"); hasTopN && topNValue == nil {
		// top_n_repos: int = 12 has a default for an ABSENT key but is not
		// Optional -- an explicit null fails the field's own int type check
		// (confirmed live, same shape "filters: null" fails above, not a
		// silent fall-back to the default).
		validationErrors = append(validationErrors, pydanticErrorDetail{
			Type: "int_type", Loc: []any{"body", "top_n_repos"}, Msg: "Input should be a valid integer", Input: nil,
		})
	} else if hasTopN {
		n, detail := coerceIntBodyField([]any{"body", "top_n_repos"}, topNValue)
		if detail != nil {
			validationErrors = append(validationErrors, *detail)
		} else {
			result.TopNRepos = n
		}
	}

	if len(validationErrors) > 0 {
		writePydanticValidationError(w, r, orgID, validationErrors...)
		return investmentFlowRequestBody{}, false
	}
	return result, true
}

// investmentFlowRequestParams reads the scope/what/why fields both
// handlers need out of the parsed body's raw filters map -- shared so the
// two handlers cannot drift apart on this extraction, same convention as
// sankey_route.go's own sankeyParamsFromFilters.
func investmentFlowRequestParams(body investmentFlowRequestBody) (startTS, endTS time.Time, scopeLevel string, scopeIDs, whatRepos, workCategory []string, err error) {
	startTS, endTS, err = timeWindow(body.Filters)
	scope, _ := body.Filters["scope"].(map[string]any)
	scopeLevel, _ = scope["level"].(string)
	if scopeLevel == "" {
		scopeLevel = "org"
	}
	scopeIDs = stringsFromAny(scope["ids"])
	what, _ := body.Filters["what"].(map[string]any)
	whatRepos = stringsFromAny(what["repos"])
	why, _ := body.Filters["why"].(map[string]any)
	workCategory = stringsFromAny(why["work_category"])
	return startTS, endTS, scopeLevel, scopeIDs, whatRepos, workCategory, nil
}

// newInvestmentFlowHandler is the routeswitch-registered handler for
// POST /api/v1/investment/flow -- the Go port of investment_flow
// (api/main.py:1354-1373).
func newInvestmentFlowHandler(client investmentflow.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "investment_flow", "Not authenticated")
			return
		}

		body, ok := parseInvestmentFlowRequestBody(w, r, claims.OrgID)
		if !ok {
			return
		}

		startTS, endTS, scopeLevel, scopeIDs, whatRepos, workCategory, windowErr := investmentFlowRequestParams(body)
		if windowErr != nil {
			writeTimeWindowOverflow(w, r, "investment_flow", claims.OrgID)
			return
		}

		resp, err := investmentflow.BuildFlowResponse(r.Context(), client, investmentflow.Params{
			OrgID: claims.OrgID, StartTS: startTS, EndTS: endTS,
			ScopeLevel: scopeLevel, ScopeIDs: scopeIDs, WhatRepos: whatRepos, WorkCategory: workCategory,
			Theme: body.Theme, FlowMode: body.FlowMode, DrillCategory: body.DrillCategory, TopNRepos: body.TopNRepos,
		})
		if err != nil {
			// Python's `except ValueError: raise HTTPException(400,
			// str(exc))` (main.py:1371-1372).
			if reqErr, isReqErr := investmentflow.AsRequestError(err); isReqErr {
				writeRESTError(w, r, "investment_flow", claims.OrgID, reqErr.Status, reqErr.Message)
				return
			}
			// Python's outer `except Exception: raise HTTPException(503,
			// "Data unavailable")` (main.py:1373).
			writeRESTDataUnavailable(w, r, "investment_flow", claims.OrgID, err)
			return
		}
		writeInvestmentFlowResponse(w, r, "investment_flow", claims.OrgID, resp)
	}
}

// newInvestmentFlowRepoTeamHandler is the routeswitch-registered handler
// for POST /api/v1/investment/flow/repo-team -- the Go port of
// investment_flow_repo_team (api/main.py:1376-1394). Unlike
// newInvestmentFlowHandler, EVERY error BuildRepoTeamFlowResponse can
// return becomes a 503 -- that Python handler has no ValueError branch at
// all (main.py:1391-1394's `except Exception: 503` is the only catch),
// and internal/investmentflow.BuildRepoTeamFlowResponse itself never
// constructs a *RequestError, so there is nothing for AsRequestError to
// find here -- confirmed by this file's own TEST-EVIDENCE, not merely by
// omission.
func newInvestmentFlowRepoTeamHandler(client investmentflow.QueryClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := authctx.FromContext(r.Context())
		if !ok {
			writeRESTUnauthorized(w, r, "investment_flow_repo_team", "Not authenticated")
			return
		}

		body, ok := parseInvestmentFlowRequestBody(w, r, claims.OrgID)
		if !ok {
			return
		}

		startTS, endTS, scopeLevel, scopeIDs, whatRepos, workCategory, windowErr := investmentFlowRequestParams(body)
		if windowErr != nil {
			writeTimeWindowOverflow(w, r, "investment_flow", claims.OrgID)
			return
		}

		resp, err := investmentflow.BuildRepoTeamFlowResponse(r.Context(), client, investmentflow.RepoTeamParams{
			OrgID: claims.OrgID, StartTS: startTS, EndTS: endTS,
			ScopeLevel: scopeLevel, ScopeIDs: scopeIDs, WhatRepos: whatRepos, WorkCategory: workCategory,
			Theme: body.Theme,
		})
		if err != nil {
			writeRESTDataUnavailable(w, r, "investment_flow_repo_team", claims.OrgID, err)
			return
		}
		writeInvestmentFlowResponse(w, r, "investment_flow_repo_team", claims.OrgID, resp)
	}
}
