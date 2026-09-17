// Package investmentflow is the Go port of POST /api/v1/investment/flow
// and POST /api/v1/investment/flow/repo-team (api/main.py:1354-1394,
// e9ea257ff) -- api/services/investment_flow.py's build_investment_flow_
// response and build_investment_repo_team_flow_response, api/queries/
// investment.py's six fetch_investment_{subcategory,team,repo_team,
// team_category_repo,team_subcategory_repo,unassigned_counts} readers,
// and the pure-logic taxonomy helpers both builders import from
// dev_health_ops/core/taxonomy.py.
//
// RESPONSE SHAPE: both routes answer SankeyResponse, the SAME wire shape
// GET+POST /api/v1/sankey already answers -- this package reuses that
// route's own cmd/query-api/internal/sankey.Response/Node/Link types
// rather than declaring a second copy of the node/link structs, even
// though sankey.BuildResponse itself is never called: that package's own
// build_sankey_response is a DIFFERENT Python function (services/
// sankey.py, reading via a DIFFERENT fetcher, fetch_investment_flow_
// items) from the two this package ports.
//
// DEDUP: every read in this package composes analytics.
// LatestWorkUnitInvestmentsSource()/LatestWorkUnitRepoEffortSource()/
// BuildUnitTeamSubquery() -- the shared, already argMax-tuple-fixed CTEs
// -- rather than a fourth unsynced copy of that dedup history (queries.go
// carries the full citation). `repos` is additionally read FINAL with
// org_id inside every JOIN's own ON clause everywhere Python's own
// fetchers read it plain -- a declared Python-plane defect,
// investmentFlowRepoDedupParity, the same class sankey/queries.go's
// fetchInvestmentFlowItems doc comment already established for the
// identical table read from the sibling investment surface.
//
// ERROR SHAPE: the two routes' own error mappings genuinely differ (main.
// py:1354-1394) and this package's two entry points preserve that
// asymmetry rather than sharing one error contract: BuildFlowResponse
// returns a *RequestError{Status: 400} for the one ValueError
// investment_flow.py's own body can raise (a missing drill_category for
// flow_mode="team_subcategory_repo") and a plain error for everything
// else, matching investment_flow's `except ValueError: 400 / except
// Exception: 503`; BuildRepoTeamFlowResponse never returns a
// *RequestError at all -- investment_flow_repo_team's own handler has no
// ValueError branch, `except Exception: 503` catches everything,
// ValueError included.
package investmentflow

import (
	"context"
	"errors"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/sankey"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
)

// QueryClient is the narrow ClickHouse read capability this package
// needs -- same shape/convention as every other ported REST route's
// package-local interface (e.g. cmd/query-api/internal/sankey.QueryClient).
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// ErrUnavailable is returned when BuildFlowResponse/BuildRepoTeamFlow
// Response is called with a nil client.
var ErrUnavailable = errors.New("investmentflow: clickhouse client unavailable")

// RequestError carries an HTTP status the way Python's HTTPException
// does, so the route layer can answer the same status code
// build_investment_flow_response would raise for the same bad input,
// without this package importing net/http -- same convention as
// cmd/query-api/internal/quadrant.RequestError.
type RequestError struct {
	Status  int
	Message string
}

func (e *RequestError) Error() string { return e.Message }

// AsRequestError extracts a *RequestError's status/message, or reports
// ok=false for any other error (including nil).
func AsRequestError(err error) (*RequestError, bool) {
	var reqErr *RequestError
	if errors.As(err, &reqErr) {
		return reqErr, true
	}
	return nil, false
}

// Params is BuildFlowResponse's input -- InvestmentFlowRequest's fields
// (api/models/filters.py:69-80) plus the org id and the already-resolved
// time window, the same "leave time_window's own computation to the
// route layer" convention cmd/query-api/internal/sankey.Params's own doc
// comment establishes (drilldown_prs_route.go's own precedent).
type Params struct {
	OrgID         string
	StartTS       time.Time
	EndTS         time.Time
	ScopeLevel    string // filters.scope.level, default "org"
	ScopeIDs      []string
	WhatRepos     []string
	WorkCategory  []string // filters.why.work_category
	Theme         *string
	FlowMode      *string
	DrillCategory *string
	TopNRepos     int // request body's top_n_repos, default 12 (InvestmentFlowRequest's own Pydantic default) -- the route layer applies that default before calling in, matching every other int-with-a-Pydantic-default field this binary's routes already resolve at the body-parsing layer.
}

// RepoTeamParams is BuildRepoTeamFlowResponse's input --
// InvestmentFlowRequest's fields investment_flow_repo_team actually reads
// (main.py:1381-1394: filters and theme only; flow_mode/drill_category/
// top_n_repos are accepted on the wire but silently ignored by this
// route, confirmed by reading investment_flow_repo_team's own call to
// build_investment_repo_team_flow_response, which passes neither).
type RepoTeamParams struct {
	OrgID        string
	StartTS      time.Time
	EndTS        time.Time
	ScopeLevel   string
	ScopeIDs     []string
	WhatRepos    []string
	WorkCategory []string
	Theme        *string
}

// emptyInvestmentResponse ports every early `return SankeyResponse(mode=
// "investment", nodes=[], links=[], unit=None)` both Python builders emit
// on the _tables_present/_columns_present schema-drift guard -- every
// OTHER field stays at SankeyResponse's own Pydantic default, an explicit
// JSON null, matching sankey.Response's own nilable-with-no-omitempty
// field set.
func emptyInvestmentResponse() *sankey.Response {
	return &sankey.Response{
		Mode:  "investment",
		Nodes: []sankey.Node{},
		Links: []sankey.Link{},
	}
}

// repoScopeFilterClause ports the `if filters.scope.level in {"team",
// "repo"}: repo_ids = await resolve_repo_filter_ids(...); scope_filter,
// scope_params = build_scope_filter_multi("repo", repo_ids, repo_column=
// "repo_id")` block both build_investment_flow_response and
// build_investment_repo_team_flow_response run verbatim before their own
// reads -- resolved down to concrete repo ids at every scope level, then
// always rendered as a "repo" (never "team") build_scope_filter_multi
// clause, matching the fact every fetcher's own FROM projects a scalar/
// fanned repo_id, never a team_id, column.
func repoScopeFilterClause(ctx context.Context, client QueryClient, scopeLevel string, scopeIDs, whatRepos []string, orgID string, asOf time.Time) (string, []dhclickhouse.Binding, error) {
	if scopeLevel != "team" && scopeLevel != "repo" {
		return "", nil, nil
	}
	repoIDs, err := resolveRepoFilterIDs(ctx, client, scopeLevel, scopeIDs, whatRepos, orgID)
	if err != nil {
		return "", nil, err
	}
	explicitSQL, explicitBindings := scopeClauseRepo(repoIDs, "repo_id")

	var teamCondition string
	var teamBindings []dhclickhouse.Binding
	if scopeLevel == "team" && len(scopeIDs) > 0 {
		teamCondition, teamBindings = teamscope.RepoCondition(orgID, "repo_id", scopeIDs, asOf)
	}

	switch {
	case explicitSQL != "" && teamCondition != "":
		return " AND (repo_id IN {scope_ids:Array(String)} OR " + teamCondition + ")",
			append(append([]dhclickhouse.Binding{}, explicitBindings...), teamBindings...), nil
	case explicitSQL != "":
		return explicitSQL, explicitBindings, nil
	case teamCondition != "":
		return " AND " + teamCondition, teamBindings, nil
	}
	return "", nil, nil
}

// flowRequiredColumns is the required-columns list both build_investment_
// flow_response's flow_mode branch (investment_flow.py:229-236) and
// build_investment_repo_team_flow_response (investment_flow.py:530-537)
// check -- byte-identical between the two, confirmed by reading both.
var flowRequiredColumns = []string{
	"from_ts", "to_ts", "repo_id", "effort_value",
	"subcategory_distribution_json", "structural_evidence_json",
}

// dynamicFlowRequiredColumns is build_investment_flow_response's OWN
// dynamic-mode (non-flow_mode) branch's required-columns list
// (investment_flow.py:354-360) -- one column shorter than
// flowRequiredColumns: no structural_evidence_json, since that branch
// never builds a unit_team join.
var dynamicFlowRequiredColumns = []string{
	"from_ts", "to_ts", "repo_id", "effort_value", "subcategory_distribution_json",
}

// BuildFlowResponse ports build_investment_flow_response (api/services/
// investment_flow.py:206-489, e9ea257ff).
func BuildFlowResponse(ctx context.Context, client QueryClient, params Params) (*sankey.Response, error) {
	if client == nil {
		return nil, ErrUnavailable
	}

	themeFilters, subcategoryFilters := splitCategoryFilters(params.WorkCategory)
	if params.Theme != nil && *params.Theme != "" {
		themeFilters = []string{*params.Theme}
	}
	normalizedDrill, drillPresent := normalizeThemeKey(params.DrillCategory)

	flowMode := ""
	if params.FlowMode != nil {
		flowMode = *params.FlowMode
	}

	switch flowMode {
	case "team_category_repo", "team_subcategory_repo", "team_category_subcategory_repo":
		return buildFlowModeResponse(ctx, client, params, flowMode, themeFilters, subcategoryFilters, normalizedDrill, drillPresent)
	default:
		return buildDynamicModeResponse(ctx, client, params, themeFilters, subcategoryFilters)
	}
}

// buildFlowModeResponse ports build_investment_flow_response's `if
// flow_mode in {...}` branch (investment_flow.py:225-343).
func buildFlowModeResponse(ctx context.Context, client QueryClient, params Params, flowMode string, themeFilters, subcategoryFilters []string, normalizedDrill string, drillPresent bool) (*sankey.Response, error) {
	if flowMode == "team_subcategory_repo" && !drillPresent {
		return nil, &RequestError{Status: 400, Message: "drill_category is required for team_subcategory_repo"}
	}
	if drillPresent {
		themeFilters = []string{normalizedDrill}
	}
	topNRepos := params.TopNRepos
	if topNRepos < 1 {
		topNRepos = 1
	}

	if !tablesPresent(ctx, client, []string{"work_unit_investments"}) {
		return emptyInvestmentResponse(), nil
	}
	if !columnsPresent(ctx, client, "work_unit_investments", flowRequiredColumns) {
		return emptyInvestmentResponse(), nil
	}

	scopeFilter, scopeBindings, err := repoScopeFilterClause(ctx, client, params.ScopeLevel, params.ScopeIDs, params.WhatRepos, params.OrgID, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	var nodes []sankey.Node
	var links []sankey.Link
	var label, description string
	var statsRows []teamRepoValueRow

	switch flowMode {
	case "team_category_repo":
		rows, err := fetchInvestmentTeamCategoryRepoEdges(ctx, client, params.StartTS, params.EndTS, scopeFilter, scopeBindings, params.OrgID, themeFilters, subcategoryFilters)
		if err != nil {
			return nil, err
		}
		nodes, links = buildTeamBurdenSankey(teamCategoryRepoRowsToBurden(rows), "category", formatThemeLabel, topNRepos)
		label = "Team → Category → Repo"
		description = "Team burden flow with category rollups."
		statsRows = teamCategoryRepoRowsToStats(rows)
	case "team_category_subcategory_repo":
		rows, err := fetchInvestmentTeamSubcategoryRepoEdges(ctx, client, params.StartTS, params.EndTS, scopeFilter, scopeBindings, params.OrgID, themeFilters, subcategoryFilters)
		if err != nil {
			return nil, err
		}
		nodes, links = buildTeamThemeSubcategoryRepoSankey(rows, topNRepos)
		label = "Team → Category → Subcategory → Repo"
		description = "Full 4-level team burden flow."
		statsRows = teamSubcategoryRepoRowsToStats(rows)
	default: // "team_subcategory_repo" -- normalizedDrill is guaranteed present here (checked above).
		rows, err := fetchInvestmentTeamSubcategoryRepoEdges(ctx, client, params.StartTS, params.EndTS, scopeFilter, scopeBindings, params.OrgID, themeFilters, subcategoryFilters)
		if err != nil {
			return nil, err
		}
		nodes, links = buildTeamBurdenSankey(teamSubcategoryRepoRowsToBurden(rows), "subcategory", formatSubcategoryLabel, topNRepos)
		label = "Team → Subcategory → Repo"
		description = "Showing subcategories within " + formatThemeLabel(normalizedDrill) + "."
		statsRows = teamSubcategoryRepoRowsToStats(rows)
	}

	unassigned, err := fetchInvestmentUnassignedCounts(ctx, client, params.StartTS, params.EndTS, scopeFilter, scopeBindings, params.OrgID, themeFilters, subcategoryFilters)
	if err != nil {
		return nil, err
	}

	teamCoverage, repoCoverage, distinctTeamTargets, distinctRepoTargets := coverageStats(statsRows)

	var drillCategoryPtr *string
	if drillPresent {
		drillCategoryPtr = strPtr(normalizedDrill)
	}

	return &sankey.Response{
		Mode:                "investment",
		Nodes:               nodes,
		Links:               links,
		Unit:                nil,
		Label:               strPtr(label),
		Description:         strPtr(description),
		TeamCoverage:        floatPtr(teamCoverage),
		RepoCoverage:        floatPtr(repoCoverage),
		DistinctTeamTargets: intPtr(distinctTeamTargets),
		DistinctRepoTargets: intPtr(distinctRepoTargets),
		ChosenMode:          strPtr(flowMode),
		Coverage:            map[string]float64{"team_coverage": teamCoverage, "repo_coverage": repoCoverage},
		UnassignedReasons:   map[string]int{"missing_team": int(unassigned.MissingTeam), "missing_repo": int(unassigned.MissingRepo)},
		FlowMode:            strPtr(flowMode),
		DrillCategory:       drillCategoryPtr,
		TopNRepos:           intPtr(topNRepos),
	}, nil
}

// buildDynamicModeResponse ports build_investment_flow_response's else
// branch (investment_flow.py:346-489): the coverage-driven "team" /
// "repo_scope" / "fallback" decision.
func buildDynamicModeResponse(ctx context.Context, client QueryClient, params Params, themeFilters, subcategoryFilters []string) (*sankey.Response, error) {
	if !tablesPresent(ctx, client, []string{"work_unit_investments"}) {
		return emptyInvestmentResponse(), nil
	}
	if !columnsPresent(ctx, client, "work_unit_investments", dynamicFlowRequiredColumns) {
		return emptyInvestmentResponse(), nil
	}

	scopeFilter, scopeBindings, err := repoScopeFilterClause(ctx, client, params.ScopeLevel, params.ScopeIDs, params.WhatRepos, params.OrgID, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	// asyncio.gather in Python; sequential here is behavior-equivalent
	// (no shared mutable state between the two fetches).
	repoRows, err := fetchInvestmentSubcategoryEdges(ctx, client, params.StartTS, params.EndTS, scopeFilter, scopeBindings, params.OrgID, themeFilters, subcategoryFilters)
	if err != nil {
		return nil, err
	}
	teamRows, err := fetchInvestmentTeamEdges(ctx, client, params.StartTS, params.EndTS, scopeFilter, scopeBindings, params.OrgID, themeFilters, subcategoryFilters)
	if err != nil {
		return nil, err
	}

	teamCoverage, distinctTeamTargets := edgeStats(teamRows)
	repoCoverage, distinctRepoTargets := edgeStats(repoRows)

	var chosenMode string
	var rowsToUse []edgeRow
	switch {
	case distinctTeamTargets >= 2 && teamCoverage >= 0.70:
		chosenMode = "team"
		rowsToUse = teamRows
	case distinctRepoTargets >= 2 && repoCoverage >= 0.70:
		chosenMode = "repo_scope"
		rowsToUse = repoRows
	default:
		chosenMode = "fallback"
		rowsToUse = repoRows
	}

	nodes, links := buildDynamicFlowSankey(rowsToUse, chosenMode)

	label := "Investment allocation"
	switch chosenMode {
	case "team":
		label = "Subcategory → Team"
	case "repo_scope":
		label = "Subcategory → Repo scope"
	}
	description := "Dynamic allocation target based on coverage metrics."

	return &sankey.Response{
		Mode:                "investment",
		Nodes:               nodes,
		Links:               links,
		Unit:                nil,
		Label:               strPtr(label),
		Description:         strPtr(description),
		TeamCoverage:        floatPtr(teamCoverage),
		RepoCoverage:        floatPtr(repoCoverage),
		DistinctTeamTargets: intPtr(distinctTeamTargets),
		DistinctRepoTargets: intPtr(distinctRepoTargets),
		ChosenMode:          strPtr(chosenMode),
		// Coverage/UnassignedReasons/FlowMode/DrillCategory/TopNRepos stay
		// nil -- build_investment_flow_response's dynamic branch never sets
		// them (only the flow_mode branch does).
	}, nil
}

// BuildRepoTeamFlowResponse ports build_investment_repo_team_flow_response
// (api/services/investment_flow.py:492-606, e9ea257ff).
func BuildRepoTeamFlowResponse(ctx context.Context, client QueryClient, params RepoTeamParams) (*sankey.Response, error) {
	if client == nil {
		return nil, ErrUnavailable
	}

	themeFilters, subcategoryFilters := splitCategoryFilters(params.WorkCategory)
	if params.Theme != nil && *params.Theme != "" {
		themeFilters = []string{*params.Theme}
	}

	if !tablesPresent(ctx, client, []string{"work_unit_investments"}) {
		return emptyInvestmentResponse(), nil
	}
	if !columnsPresent(ctx, client, "work_unit_investments", flowRequiredColumns) {
		return emptyInvestmentResponse(), nil
	}

	scopeFilter, scopeBindings, err := repoScopeFilterClause(ctx, client, params.ScopeLevel, params.ScopeIDs, params.WhatRepos, params.OrgID, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	rows, err := fetchInvestmentRepoTeamEdges(ctx, client, params.StartTS, params.EndTS, scopeFilter, scopeBindings, params.OrgID, themeFilters, subcategoryFilters)
	if err != nil {
		return nil, err
	}

	nodes, links := buildRepoTeamSankey(rows)

	return &sankey.Response{
		Mode:        "investment",
		Nodes:       nodes,
		Links:       links,
		Unit:        nil,
		Label:       strPtr("Subcategory → Repo → Team"),
		Description: strPtr("Allocation flow with repo-to-team mapping from work items."),
		ChosenMode:  strPtr("repo_team"),
	}, nil
}
