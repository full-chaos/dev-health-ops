package investment

import (
	"context"
	"log"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
)

// Response is the wire shape of InvestmentResponse (api/models/schemas.py:
// 239-245). Unit is always nil (Python never sets it from either builder
// this package ports) and Edges is always a non-nil empty slice
// (build_investment_response's own literal `edges=[]`) -- both encode as
// their Python counterparts do (null, []) via encoding/json's default
// nil-pointer/non-nil-empty-slice behaviour, with no field tagged
// omitempty: Python's response_model always emits every declared field.
//
// EvidenceQualityDistribution/EvidenceQualityStats are left at their Go
// zero values (nil map, nil pointer -- both encode as JSON null) on the
// early "table/columns not present" return path, matching
// InvestmentResponse(theme_distribution={}, subcategory_distribution={},
// edges=[]) leaving those two fields at their own Pydantic defaults
// (None) rather than populating them.
type Response struct {
	ThemeDistribution           map[string]float64    `json:"theme_distribution"`
	SubcategoryDistribution     map[string]float64    `json:"subcategory_distribution"`
	EvidenceQualityDistribution map[string]float64    `json:"evidence_quality_distribution"`
	EvidenceQualityStats        *EvidenceQualityStats `json:"evidence_quality_stats"`
	Unit                        *string               `json:"unit"`
	Edges                       []map[string]any      `json:"edges"`
}

// EvidenceQualityStats is the wire shape of EvidenceQualityStats
// (api/models/schemas.py:312-319).
type EvidenceQualityStats struct {
	Mean           *float64       `json:"mean"`
	Stddev         *float64       `json:"stddev"`
	Total          int            `json:"total"`
	BandCounts     map[string]int `json:"band_counts"`
	QualityDrivers []string       `json:"quality_drivers"`
}

// SunburstSlice is the wire shape of InvestmentSunburstSlice
// (api/models/schemas.py:356-360).
type SunburstSlice struct {
	Theme       string  `json:"theme"`
	Subcategory string  `json:"subcategory"`
	Scope       string  `json:"scope"`
	Value       float64 `json:"value"`
}

// requiredInvestmentColumns is the column list both build_investment_response
// (api/services/investment.py:158-169) and build_investment_sunburst
// (:265-276) check via _columns_present before reading work_unit_investments
// -- build_investment_response's own six-column list is the superset (it
// additionally reads theme_distribution_json, which sunburst never
// selects); using the same superset for both matches Python's own
// per-function difference only in effect: sunburst can never reach a
// state where theme_distribution_json is absent but the other five are
// present without ALSO failing on one of the five it actually needs, so
// gating both builders on the same six-column check changes no observed
// behaviour.
var requiredInvestmentColumns = []string{
	"from_ts", "to_ts", "repo_id", "effort_value",
	"theme_distribution_json", "subcategory_distribution_json",
}

// tableExists ports _tables_present (api/services/investment.py:42-61)
// for the single table this route family ever checks
// ("work_unit_investments"). A query failure fails closed (false),
// matching Python's own `except Exception: return False`.
func tableExists(ctx context.Context, client analytics.QueryClient, table string) bool {
	query := `SELECT name FROM system.tables WHERE database = currentDatabase() AND name = {table:String}`
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{{Name: "table", Value: table}})
	if err != nil {
		return false
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		if name == table {
			found = true
		}
	}
	if rows.Err() != nil {
		return false
	}
	return found
}

// columnsExist ports _columns_present (api/services/investment.py:64-86).
// A query failure fails closed (false), matching Python's own
// `except Exception: return False`.
func columnsExist(ctx context.Context, client analytics.QueryClient, table string, columns []string) bool {
	if len(columns) == 0 {
		return true
	}
	query := `SELECT name FROM system.columns WHERE database = currentDatabase() AND table = {table:String} AND name IN {columns:Array(String)}`
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "table", Value: table},
		{Name: "columns", Value: columns},
	})
	if err != nil {
		return false
	}
	defer rows.Close()
	present := make(map[string]bool, len(columns))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false
		}
		present[name] = true
	}
	if rows.Err() != nil {
		return false
	}
	for _, column := range columns {
		if !present[column] {
			return false
		}
	}
	return true
}

// Params is BuildResponse's input -- the already-window-computed,
// already-map-extracted equivalent of the MetricFilter GET's own query
// parameters or POST's JSON body resolve to, matching
// investmentexplain's investmentExplainRequestBody/buildExplainOptions
// split (route-level parsing stays in cmd/query-api's route file; this
// package receives already-resolved values).
type Params struct {
	OrgID        string
	StartTS      time.Time
	EndTS        time.Time
	ScopeLevel   string
	ScopeIDs     []string
	WhatRepos    []string
	WorkCategory []string
}

func emptyResponse() *Response {
	return &Response{
		ThemeDistribution:       map[string]float64{},
		SubcategoryDistribution: map[string]float64{},
		Edges:                   []map[string]any{},
	}
}

// BuildResponse ports build_investment_response
// (api/services/investment.py:141-246).
func BuildResponse(ctx context.Context, reader *Reader, orgID string, params Params) (*Response, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}

	// _query_investment_dicts fires this stale-membership-scope telemetry
	// check immediately before every investment query Python issues; this
	// port fires it (and the argMax null-transition guard) once per
	// request rather than once per query, an intentional, telemetry-only
	// simplification -- neither call can affect the response this
	// function returns.
	analytics.RecordStaleInvestmentMembershipScope(ctx, reader.client, orgID, queryTimeoutSecs)
	analytics.RecordArgMaxNullTransitionGuard(ctx, reader.client, orgID, queryTimeoutSecs)

	if !tableExists(ctx, reader.client, "work_unit_investments") {
		return emptyResponse(), nil
	}
	if !columnsExist(ctx, reader.client, "work_unit_investments", requiredInvestmentColumns) {
		return emptyResponse(), nil
	}

	themes, subcategories := splitCategoryFilters(params.WorkCategory)

	var repoIDs []string
	if params.ScopeLevel == "team" || params.ScopeLevel == "repo" {
		var err error
		repoIDs, err = reader.ResolveRepoFilterIDs(ctx, params.ScopeLevel, params.ScopeIDs, params.WhatRepos, orgID)
		if err != nil {
			return nil, err
		}
	}
	var teamCondition string
	var teamBindings []dhclickhouse.Binding
	if params.ScopeLevel == "team" && len(params.ScopeIDs) > 0 {
		teamCondition, teamBindings = teamscope.RepoCondition(orgID, "repo_id", params.ScopeIDs, time.Now().UTC())
	}

	breakdownFilters := investmentexplain.BreakdownFilters{
		OrgID: orgID, StartTS: params.StartTS, EndTS: params.EndTS,
		RepoIDs: repoIDs, TeamScopeCondition: teamCondition, TeamScopeBindings: teamBindings,
		Themes: themes, Subcategories: subcategories,
	}
	rows, err := reader.explainR.FetchInvestmentBreakdown(ctx, breakdownFilters)
	if err != nil {
		return nil, err
	}

	mockCount, err := reader.explainR.FetchMockFixtureInvestmentRowCount(ctx, breakdownFilters)
	if err != nil {
		return nil, err
	}
	warnMockFixtureRows(orgID, "investment", mockCount)

	qualityRow, found, err := reader.FetchInvestmentQualityStats(ctx, QualityStatsFilters{
		OrgID: orgID, StartTS: params.StartTS, EndTS: params.EndTS,
		RepoIDs: repoIDs, TeamScopeCondition: teamCondition, TeamScopeBindings: teamBindings,
		Themes: themes, Subcategories: subcategories,
	})
	if err != nil {
		return nil, err
	}

	themeDistribution := map[string]float64{}
	subcategoryDistribution := map[string]float64{}
	for _, row := range rows {
		if row.Theme != "" && row.Value > 0 {
			themeDistribution[row.Theme] += row.Value
		}
		if strings.Contains(row.Subcategory, ".") && row.Value > 0 {
			subcategoryDistribution[row.Subcategory] += row.Value
		}
	}

	qualityStats := computeQualityStats(qualityRow, found)
	evidenceQualityDistribution := make(map[string]float64, len(qualityStats.BandCounts))
	for band, count := range qualityStats.BandCounts {
		evidenceQualityDistribution[band] = float64(count)
	}

	return &Response{
		ThemeDistribution:           themeDistribution,
		SubcategoryDistribution:     subcategoryDistribution,
		EvidenceQualityDistribution: evidenceQualityDistribution,
		EvidenceQualityStats:        &qualityStats,
		Edges:                       []map[string]any{},
	}, nil
}

// SunburstParams is BuildSunburstResponse's input -- see Params' own doc
// comment for the same route-level/package split. Limit mirrors
// investment_sunburst's own `limit: int = 500` query parameter
// (api/main.py:1272-1298); the route file is responsible for applying
// that default before calling in, matching every other int query
// parameter this service already handles the same way.
type SunburstParams struct {
	OrgID        string
	StartTS      time.Time
	EndTS        time.Time
	ScopeLevel   string
	ScopeIDs     []string
	WhatRepos    []string
	WorkCategory []string
	Limit        int
}

// BuildSunburstResponse ports build_investment_sunburst
// (api/services/investment.py:249-321).
func BuildSunburstResponse(ctx context.Context, reader *Reader, orgID string, params SunburstParams) ([]SunburstSlice, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}

	analytics.RecordStaleInvestmentMembershipScope(ctx, reader.client, orgID, queryTimeoutSecs)
	analytics.RecordArgMaxNullTransitionGuard(ctx, reader.client, orgID, queryTimeoutSecs)

	if !tableExists(ctx, reader.client, "work_unit_investments") {
		return []SunburstSlice{}, nil
	}
	if !columnsExist(ctx, reader.client, "work_unit_investments", requiredInvestmentColumns) {
		return []SunburstSlice{}, nil
	}

	themes, subcategories := splitCategoryFilters(params.WorkCategory)

	var repoIDs []string
	if params.ScopeLevel == "team" || params.ScopeLevel == "repo" {
		var err error
		repoIDs, err = reader.ResolveRepoFilterIDs(ctx, params.ScopeLevel, params.ScopeIDs, params.WhatRepos, orgID)
		if err != nil {
			return nil, err
		}
	}
	var teamCondition string
	var teamBindings []dhclickhouse.Binding
	if params.ScopeLevel == "team" && len(params.ScopeIDs) > 0 {
		teamCondition, teamBindings = teamscope.RepoCondition(orgID, "repo_id", params.ScopeIDs, time.Now().UTC())
	}

	mockFilters := investmentexplain.BreakdownFilters{
		OrgID: orgID, StartTS: params.StartTS, EndTS: params.EndTS,
		RepoIDs: repoIDs, TeamScopeCondition: teamCondition, TeamScopeBindings: teamBindings,
		Themes: themes, Subcategories: subcategories,
	}
	mockCount, err := reader.explainR.FetchMockFixtureInvestmentRowCount(ctx, mockFilters)
	if err != nil {
		return nil, err
	}
	warnMockFixtureRows(orgID, "investment_sunburst", mockCount)

	rows, err := reader.FetchInvestmentSunburst(ctx, SunburstFilters{
		OrgID: orgID, StartTS: params.StartTS, EndTS: params.EndTS,
		RepoIDs: repoIDs, TeamScopeCondition: teamCondition, TeamScopeBindings: teamBindings,
		Themes: themes, Subcategories: subcategories, Limit: params.Limit,
	})
	if err != nil {
		return nil, err
	}

	slices := make([]SunburstSlice, 0, len(rows))
	for _, row := range rows {
		theme := row.Theme
		if theme == "" {
			theme = "Unassigned"
		}
		subcategory := row.Subcategory
		if subcategory == "" {
			subcategory = "Other"
		}
		scope := row.Scope
		if scope == "" {
			scope = "Unassigned"
		}
		slices = append(slices, SunburstSlice{Theme: theme, Subcategory: subcategory, Scope: scope, Value: row.Value})
	}
	return slices, nil
}

// computeQualityStats ports _compute_quality_stats
// (api/services/investment.py:89-138). found false mirrors Python's
// `if not quality_row: return EvidenceQualityStats()` -- the all-defaults
// constructor, whose band_counts default_factory is an EMPTY map, not
// the five-key zeroed map the populated branch below builds.
func computeQualityStats(row QualityStatsRow, found bool) EvidenceQualityStats {
	if !found {
		return EvidenceQualityStats{BandCounts: map[string]int{}, QualityDrivers: []string{}}
	}

	bandCounts := map[string]int{
		"high":     row.HighCount,
		"moderate": row.ModerateCount,
		"low":      row.LowCount,
		"very_low": row.VeryLowCount,
		"unknown":  row.UnknownCount,
	}

	var mean, stddev *float64
	if row.QualityKnownCount > 0 {
		m, s := row.QualityMean, row.QualityStddev
		mean, stddev = &m, &s
	}

	totalCount := row.Total
	if totalCount == 0 {
		for _, count := range bandCounts {
			totalCount += count
		}
	}

	drivers := []string{}
	if totalCount > 0 && float64(bandCounts["unknown"])/float64(totalCount) > 0.3 {
		drivers = append(drivers, "missing_evidence_metadata")
	}
	if mean != nil && *mean < 0.4 {
		drivers = append(drivers, "low_text_signal")
	}
	if stddev != nil && *stddev > 0.25 {
		drivers = append(drivers, "high_uncertainty_spread")
	}
	lowPlus := bandCounts["low"] + bandCounts["very_low"]
	if totalCount > 0 && float64(lowPlus)/float64(totalCount) > 0.5 {
		drivers = append(drivers, "weak_cross_links")
	}
	if totalCount > 0 && float64(row.QualityKnownCount)/float64(totalCount) < 0.7 {
		drivers = append(drivers, "thin_component")
	}

	return EvidenceQualityStats{
		Mean: mean, Stddev: stddev, Total: row.Total,
		BandCounts: bandCounts, QualityDrivers: drivers,
	}
}

// warnMockFixtureRows ports warn_once_for_mock_fixture_rows'
// (api/services/provenance.py:28-40) observable effect for this route
// family's one real provenance signal (mockCount > 0 means the fetched
// rows carried a mock/fixture marker on provider or
// categorization_model_version). Logged every call rather than
// deduplicated per (org, surface) process lifetime -- Python's own
// module-level _WARNED_MOCK_FIXTURE_SURFACES set has no Go equivalent
// here; a declared, accepted simplification (more log lines, same
// signal), never a silently dropped one.
func warnMockFixtureRows(orgID, surface string, mockCount int) {
	if mockCount <= 0 {
		return
	}
	log.Printf("query-api: investment: mock/fixture-sourced investment rows served: surface=%s org_id=%s", surface, orgID)
}
