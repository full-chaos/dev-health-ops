// Package investmentexplain is the Go port of CHAOS-4977's
// POST /api/v1/investment/explain -- the LLM-generated explanation of an
// org's investment mix.
//
// This file is the ClickHouse read side: api/queries/investment.py's
// fetch_investment_breakdown and fetch_mock_fixture_investment_row_count,
// two of the three queries api/services/investment.py's
// build_investment_response issues. The third,
// fetch_investment_quality_stats, is NOT re-ported here -- it already has
// a full Go port in internal/queryapi/analytics (CHAOS-4723,
// investmentquality.go's compileInvestmentQualityStats/
// executeInvestmentQualityStats), reused via that package's exported
// FetchInvestmentQualityStats wrapper rather than duplicated, so this
// dedup-critical query has exactly one Go implementation, not two that
// could drift apart.
//
// CLIENT CONVENTION: this package takes analytics.QueryClient (github.com/
// full-chaos/dev-health-go/clickhouse's Binding/RowScanner), matching its
// sibling package under internal/queryapi -- NOT
// internal/jobs/investment/chquery's raw clickhouse-go/v2 driver.Rows,
// which belongs to a different binary (the jobs/worker side). A first
// draft of this file copied chquery's convention by reflex; corrected
// after finding analytics already establishes this service's own client
// shape, complete with a proven fix for a real dev-health-go v0.4.0 defect
// (see dateBindingValue below).
package investmentexplain

import (
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"log/slog"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

// ErrUnavailable is returned when the reader is constructed without a
// client.
var ErrUnavailable = errors.New("investmentexplain: clickhouse client unavailable")

// queryTimeoutSecs is this package's own copy of analytics.queryTimeoutSecs
// (unexported there) -- CHAOS-4730: the trailing SETTINGS max_execution_time
// clause must be a LITERAL integer, never a bound {name:UInt64} parameter,
// because ClickHouse 26.6.1.1193 (the exact digest-pinned image
// internal/testsupport/containers.StartClickHouse runs for every Go
// integration test in this repo) fails to PARSE a bound parameter inside
// a SETTINGS clause (Code: 62), even though 26.7.5.10 (dev-stack/prod)
// parses it fine. Safe as a literal here for the same reason it's safe in
// analytics: timeoutSeconds is always this constant, never
// request-supplied.
const queryTimeoutSecs = 30

func settingsMaxExecutionTime() string {
	return fmt.Sprintf("SETTINGS max_execution_time = %d", queryTimeoutSecs)
}

// Reader reads the ClickHouse data investment-explain needs.
type Reader struct {
	client analytics.QueryClient
}

// NewReader returns a Reader over the given query client.
func NewReader(client analytics.QueryClient) (*Reader, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	return &Reader{client: client}, nil
}

// dateBindingValue formats a time.Time as a bare "YYYY-MM-DD" string for
// binding into a {name:Date}-typed native ClickHouse parameter.
//
// REQUIRED, not cosmetic -- copied from analytics/validate.go's
// dateBindingValue rather than exported and shared, matching this
// package's own repeat-don't-couple convention for small helpers
// (settingsMaxExecutionTime above, dedupeStrings below): dev-health-go
// v0.4.0's clickHouseParameter formats every time.Time value as
// `.UTC().Format("2006-01-02 15:04:05.000")` regardless of the
// placeholder's declared type, so a {name:Date} placeholder bound to a
// raw time.Time fails live ("only 10 of 23 bytes was parsed"). Python's
// own start_ts/end_ts for this exact endpoint are always
// datetime.combine(day, time.min, tzinfo=utc) -- i.e. always midnight UTC
// (api/services/investment.py's build_investment_response, via
// time_window) -- so a bare Date bound against a DateTime64 column
// expresses the identical predicate; no precision is lost by using Date
// here instead of DateTime.
func dateBindingValue(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%04d-%02d-%02d", year, int(month), day)
}

// BreakdownFilters is the shared filter shape fetch_investment_breakdown
// and fetch_mock_fixture_investment_row_count both take -- mirrors the
// (start_ts, end_ts, scope_filter/scope_params, org_id, themes,
// subcategories) parameter list both Python functions repeat verbatim.
//
// RepoIDs replaces Python's generic scope_filter/scope_params pair for
// the EXPLICIT-ref case: build_investment_response (investment.py:
// 141-246) is the ONLY caller of both functions, and it ALWAYS resolves
// filters.scope.level in {"team", "repo"} down to a concrete repo-id
// list before calling build_scope_filter_multi("repo", repo_ids, ...) --
// the literal string "repo" is hardcoded at that call site regardless of
// which scope level the request asked for. RepoIDs here carries only the
// bounded, request-sized explicit refs (scope="repo" ids + what.repos);
// TeamScopeCondition/TeamScopeBindings carry the repositories a team owns
// as a pushed-down SQL condition instead of a second materialized id list
// -- see internal/queryapi/teamscope's own doc comment for the source
// and the semantics. The two are ORed together in scopeClause, so a request
// naming a team and explicit repos sees the union.
type BreakdownFilters struct {
	OrgID              string
	StartTS            time.Time
	EndTS              time.Time
	RepoIDs            []string
	TeamScopeCondition string
	// ReposNamed says the request names repositories (teamscope.NamedRepoRefs):
	// with none resolved the filter matches nothing; with a team scope too, the
	// two narrow (AND), they never widen.
	ReposNamed        bool
	TeamScopeBindings []dhclickhouse.Binding
	// TeamIDs and TeamAsOf are the teams of the team scope and the instant their ownership is read at. They weigh
	// each repository's effort by the share of it the teams hold (teamscope.RepoShare, CHAOS-9413, D5988):
	// TeamScopeCondition only says WHICH repositories are in, so without them a co-owner would read the whole of a
	// repository it owns a fraction of. Empty = no team scope = weight 1.
	TeamIDs       []string
	TeamAsOf      time.Time
	Themes        []string
	Subcategories []string
	// DatedByEnd dates a unit by its END day (to_ts in [start, end)) instead of by the overlap
	// of its [from_ts, to_ts] with the window: the Home tile "Investment by theme" (CHAOS-9195,
	// D5933). Every other rule of the read (latest row of a unit, every categorization status,
	// the scope, the category filters) is the Investment page's own.
	DatedByEnd bool
	// Measure says what a row's value counts, and every read of this package REQUIRES one (ErrMeasureRequired):
	// MeasureCount is the share of the unit the row stands for, the Investment page's COUNT
	// (InvestmentShareMeasure for the REST investment endpoint, the Home tile and the Operating Review).
	Measure string
}

// The measure of a read of this package: the page's own COUNT (analytics.Measure COUNT, THROUGHPUT).
// Lines of code are not a measure of Investment (CHAOS-9695, D6014), and MeasureValueSQL is the one definition of it.
const MeasureCount = "COUNT"

// MeasureIssueSize is Investment measured by work-unit issue size (CHAOS-9619, D5999): each unit weighs the sum of
// its issues' story points, 1 when it has none. Lines of code never.
const MeasureIssueSize = "ISSUE_SIZE"

// NormalizeMeasure maps a request measure to the measure of an explain mix read. THROUGHPUT is
// COUNT on the investment path (analytics dbExpression). ok is false for any other name.
func NormalizeMeasure(name string) (measure string, ok bool) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "COUNT", "THROUGHPUT":
		return MeasureCount, true
	case MeasureIssueSize:
		return MeasureIssueSize, true
	}
	return "", false
}

// dateWindowClause is the window predicate of the work-unit reads of this package:
// the overlap of the unit's range with [start, end) (the Investment page), or the unit's
// end day within it (DatedByEnd).
func (f BreakdownFilters) dateWindowClause() string {
	if f.DatedByEnd {
		return `work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.to_ts < {end_date:Date}`
	}
	return `work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}`
}

func (f BreakdownFilters) categoryClause() (filterSQL string, bindings []dhclickhouse.Binding) {
	var conditions []string
	if len(f.Themes) > 0 {
		conditions = append(conditions, "splitByChar('.', subcategory_kv.1)[1] IN {themes:Array(String)}")
		bindings = append(bindings, dhclickhouse.Binding{Name: "themes", Value: dedupeStrings(f.Themes)})
	}
	if len(f.Subcategories) > 0 {
		conditions = append(conditions, "subcategory_kv.1 IN {subcategories:Array(String)}")
		bindings = append(bindings, dhclickhouse.Binding{Name: "subcategories", Value: dedupeStrings(f.Subcategories)})
	}
	if len(conditions) == 0 {
		return "", bindings
	}
	filterSQL = " AND (" + joinOR(conditions) + ")"
	return filterSQL, bindings
}

// scopeClause ORs an explicit repo-id membership clause with
// TeamScopeCondition's own bare boolean (no "AND"/"OR" of its own) into
// ONE "AND (...)" fragment -- resolve_repo_filter_ids' own Python shape
// unions explicit refs and team-resolved ids into a SINGLE id list
// before filtering, so a repo matches when it is named directly OR
// reachable through a scoped team; this keeps that same union semantics
// across the two different condition shapes (see BreakdownFilters' own
// doc comment).
func (f BreakdownFilters) scopeClause() (filterSQL string, bindings []dhclickhouse.Binding) {
	var explicitCondition string
	var explicitBindings []dhclickhouse.Binding
	if len(f.RepoIDs) > 0 {
		explicitCondition = "repo_id IN {scope_ids:Array(String)}"
		explicitBindings = []dhclickhouse.Binding{{Name: "scope_ids", Value: dedupeStrings(f.RepoIDs)}}
	}

	explicitSQL := ""
	if explicitCondition != "" {
		explicitSQL = " AND " + explicitCondition
	}
	filterSQL, bindings = teamscope.NarrowRepoScope(f.ReposNamed, explicitSQL, explicitBindings, f.TeamScopeCondition, f.TeamScopeBindings)
	return filterSQL, bindings
}

func (f BreakdownFilters) baseBindings() []dhclickhouse.Binding {
	return []dhclickhouse.Binding{
		{Name: "start_date", Value: dateBindingValue(f.StartTS)},
		{Name: "end_date", Value: dateBindingValue(f.EndTS)},
		{Name: "org_id", Value: f.OrgID},
	}
}

// BreakdownRow is one fetch_investment_breakdown result row.
type BreakdownRow struct {
	Subcategory string
	Theme       string
	Value       float64
}

// FetchInvestmentBreakdown ports fetch_investment_breakdown
// (api/queries/investment.py:524-562).
//
// subcategory_distribution_json is Map(String, Float64) in the schema
// (migrations/clickhouse/017_investment_materialize_tables.sql:12), but the
// query CASTs it to Array(Tuple(String, Float32)) before the ARRAY JOIN --
// a DELIBERATE, LOSSY Float64->Float32 narrowing in Python's own query text,
// not a Go-port artifact. Reproduced verbatim (same CAST, same ARRAY JOIN)
// so the sum(Float32 * Float64) aggregation happens in the SAME arithmetic
// on both planes -- the query TEXT is what determines the numeric result,
// not which client sent it, so byte-parity here only requires porting the
// SQL unchanged, never a client-side rounding step.
//
// FROM %s AS work_unit_investments (a derived-table subquery, no WITH
// clause) matches the existing convention every other reader in
// internal/queryapi/analytics uses for LatestWorkUnitInvestmentsSource
// -- see sankeycoverage.go:136 -- rather than Python's
// WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE} name-then-alias form; both
// compile to the same query plan.
func (reader *Reader) FetchInvestmentBreakdown(ctx context.Context, filters BreakdownFilters) ([]BreakdownRow, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	if err := filters.requireMeasure(); err != nil {
		return nil, err
	}

	categorySQL, categoryBindings := filters.categoryClause()
	scopeSQL, scopeBindings := filters.scopeClause()
	shareSQL, shareBindings := filters.teamShare(scopeSQL)
	bindings := filters.baseBindings()
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, shareBindings...)
	bindings = append(bindings, categoryBindings...)

	valueSQL, err := MeasureValueSQL(filters.Measure, filters.unitWeight(scopeSQL), shareSQL)
	if err != nil {
		return nil, err
	}
	source, err := MeasureSource(filters.Measure, filters.unitSource(scopeSQL))
	if err != nil {
		return nil, err
	}
	slog.DebugContext(ctx, "investmentexplain: breakdown measure", "measure", filters.Measure, "scoped", filters.scoped(scopeSQL), "dated_by_end", filters.DatedByEnd)

	query := fmt.Sprintf(`
SELECT
    subcategory_kv.1 AS subcategory,
    splitByChar('.', subcategory_kv.1)[1] AS theme,
    %s AS value
FROM %s
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE %s
  AND work_unit_investments.org_id = {org_id:String}
%s
%s
GROUP BY subcategory, theme
ORDER BY value DESC
%s
`, valueSQL, source, filters.dateWindowClause(), scopeSQL, categorySQL, settingsMaxExecutionTime())

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query investment breakdown: %w", err)
	}
	defer rows.Close()

	results := make([]BreakdownRow, 0)
	for rows.Next() {
		var row BreakdownRow
		if err := rows.Scan(&row.Subcategory, &row.Theme, &row.Value); err != nil {
			return nil, fmt.Errorf("scan investment breakdown row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate investment breakdown rows: %w", err)
	}
	return results, nil
}

// SizingCoverage is how many distinct work units of a read carry an issue size, and how many units it holds
// (CHAOS-9619, D6027): a share in ISSUE_SIZE is read together with it, because with few sized units the mix is
// mostly unsized units, each counted as 1 work unit.
type SizingCoverage struct {
	SizedUnits int
	Units      int
}

// Share is the sized share of the units, nil when the read holds no unit (not measured, never 0).
func (c SizingCoverage) Share() *float64 {
	if c.Units <= 0 {
		return nil
	}
	share := float64(c.SizedUnits) / float64(c.Units)
	return &share
}

// Unsized is the number of units without a size, nil when the read holds no unit.
func (c SizingCoverage) Unsized() *int {
	if c.Units <= 0 {
		return nil
	}
	unsized := c.Units - c.SizedUnits
	return &unsized
}

// FetchSizingCoverage counts the distinct units FetchInvestmentBreakdown reads for the same filters (same source,
// window, scope and category clauses) and the ones among them that carry an issue size.
func (reader *Reader) FetchSizingCoverage(ctx context.Context, filters BreakdownFilters) (SizingCoverage, error) {
	if reader == nil || reader.client == nil {
		return SizingCoverage{}, ErrUnavailable
	}
	categorySQL, categoryBindings := filters.categoryClause()
	scopeSQL, scopeBindings := filters.scopeClause()
	source, err := MeasureSource(MeasureIssueSize, filters.unitSource(scopeSQL))
	if err != nil {
		return SizingCoverage{}, err
	}
	bindings := filters.baseBindings()
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, categoryBindings...)
	query := fmt.Sprintf(`
SELECT
    toUInt64(uniqExactIf(work_unit_investments.work_unit_id, %s > 0)) AS sized_units,
    toUInt64(uniqExact(work_unit_investments.work_unit_id)) AS units
FROM %s
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE %s
  AND work_unit_investments.org_id = {org_id:String}
%s
%s
%s
`, analytics.UnitIssueSizeColumn, source, filters.dateWindowClause(), scopeSQL, categorySQL, settingsMaxExecutionTime())
	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return SizingCoverage{}, fmt.Errorf("query investment sizing coverage: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return SizingCoverage{}, fmt.Errorf("iterate investment sizing coverage: %w", err)
		}
		return SizingCoverage{}, fmt.Errorf("query investment sizing coverage: no row")
	}
	var sized, units uint64
	if err := rows.Scan(&sized, &units); err != nil {
		return SizingCoverage{}, fmt.Errorf("scan investment sizing coverage: %w", err)
	}
	if err := rows.Err(); err != nil {
		return SizingCoverage{}, fmt.Errorf("iterate investment sizing coverage: %w", err)
	}
	coverage := SizingCoverage{SizedUnits: int(sized), Units: int(units)}
	slog.DebugContext(ctx, "investmentexplain: sizing coverage", "sized_units", coverage.SizedUnits, "units", coverage.Units, "scoped", filters.scoped(scopeSQL))
	return coverage, nil
}

// FetchMockFixtureInvestmentRowCount ports
// fetch_mock_fixture_investment_row_count (api/queries/investment.py:
// 563-598) -- a provenance-warning count, not part of the response's
// actual numeric output (build_investment_response only uses it to decide
// whether to fire warn_once_for_mock_fixture_rows). Ported anyway per
// CHAOS-4977 ruling: kept as a distinct reader method so the eventual
// assembly layer can reproduce that same warning behavior.
func (reader *Reader) FetchMockFixtureInvestmentRowCount(ctx context.Context, filters BreakdownFilters) (int, error) {
	if reader == nil || reader.client == nil {
		return 0, ErrUnavailable
	}

	categorySQL, categoryBindings := filters.categoryClause()
	scopeSQL, scopeBindings := filters.scopeClause()
	bindings := filters.baseBindings()
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, categoryBindings...)

	query := fmt.Sprintf(`
SELECT count() AS count
FROM %s AS work_unit_investments
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.org_id = {org_id:String}
  AND (
    lower(ifNull(work_unit_investments.provider, '')) IN ('mock', 'fixture', 'fixtures', 'synthetic')
    OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%%mock%%'
    OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%%synthetic%%'
    OR lower(ifNull(work_unit_investments.categorization_model_version, '')) LIKE '%%fixture%%'
  )
%s
%s
%s
`, analytics.LatestWorkUnitInvestmentsSource(), scopeSQL, categorySQL, settingsMaxExecutionTime())

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return 0, fmt.Errorf("query mock fixture investment row count: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate mock fixture investment row count: %w", err)
		}
		return 0, nil
	}

	var count uint64
	if err := rows.Scan(&count); err != nil {
		return 0, fmt.Errorf("scan mock fixture investment row count: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate mock fixture investment row count: %w", err)
	}
	return int(count), nil
}

// FetchInvestmentDataVersion reads the version of an organization's work-unit
// investments: the newest write and the row count. The explain cache key carries it,
// so an answer built on older work units (other quality, quotes or counts, even with
// the same mix shares) is never served for the same filters.
func (reader *Reader) FetchInvestmentDataVersion(ctx context.Context, orgID string) (string, error) {
	if reader == nil || reader.client == nil {
		return "", ErrUnavailable
	}
	query := fmt.Sprintf(`
SELECT toString(max(computed_at)) AS latest, toString(count()) AS row_count
FROM work_unit_investments
WHERE org_id = {org_id:String}
%s
`, settingsMaxExecutionTime())
	rows, err := reader.client.Query(ctx, query, []dhclickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		return "", fmt.Errorf("query investment data version: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("iterate investment data version: %w", err)
		}
		return "none", nil
	}
	var latest, rowCount string
	if err := rows.Scan(&latest, &rowCount); err != nil {
		return "", fmt.Errorf("scan investment data version: %w", err)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate investment data version: %w", err)
	}
	return latest + "/" + rowCount, nil
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func joinOR(conditions []string) string {
	joined := conditions[0]
	for _, condition := range conditions[1:] {
		joined += " OR " + condition
	}
	return joined
}

// ThemeEffortRow is one theme of FetchInvestmentThemeEffort: the value of the theme in the measure the
// filters name (BreakdownFilters.Measure; InvestmentShareMeasure for the Home tile and the Operating
// Review), the part of the units' effort measured in changed lines of code (effort_metric churn_loc; the
// Home tile's churn_loc line, never a weight and never an Investment measure), and the pull requests of the
// units' evidence weighted by the theme share.
type ThemeEffortRow struct {
	Theme    string
	Value    float64
	ChurnLOC float64
	PRs      float64
}

// FetchInvestmentThemeEffort reads the Investment page's own breakdown, rolled up to the theme,
// with the two extra measures the Home tile "Investment by theme" serves (CHAOS-9195): the
// SAME source (the latest row of each unit), the SAME scope and category clauses and the SAME
// rule for every categorization status (a unit with insufficient evidence counts, as on the
// Investment page); only the dating of a unit may differ (BreakdownFilters.DatedByEnd). The value
// is MeasureValueSQL of the filters' measure, the one definition FetchInvestmentBreakdown reads.
func (reader *Reader) FetchInvestmentThemeEffort(ctx context.Context, filters BreakdownFilters) ([]ThemeEffortRow, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}
	if err := filters.requireMeasure(); err != nil {
		return nil, err
	}
	categorySQL, categoryBindings := filters.categoryClause()
	scopeSQL, scopeBindings := filters.scopeClause()
	shareSQL, shareBindings := filters.teamShare(scopeSQL)
	valueSQL, err := MeasureValueSQL(filters.Measure, filters.unitWeight(scopeSQL), shareSQL)
	if err != nil {
		return nil, err
	}
	source, err := MeasureSource(filters.Measure, filters.unitSource(scopeSQL))
	if err != nil {
		return nil, err
	}
	bindings := filters.baseBindings()
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, shareBindings...)
	bindings = append(bindings, categoryBindings...)

	query := fmt.Sprintf(`
SELECT
    splitByChar('.', subcategory_kv.1)[1] AS theme,
    toFloat64(%[8]s) AS value,
    toFloat64(sum(if(effort_metric = 'churn_loc', subcategory_kv.2 * effort_value * %[1]s, 0))) AS churn_loc,
    toFloat64(sum(subcategory_kv.2 * %[2]s * %[1]s * length(JSONExtractArrayRaw(structural_evidence_json, 'prs')))) AS prs
FROM %[3]s
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE %[4]s
  AND work_unit_investments.org_id = {org_id:String}
%[5]s
%[6]s
GROUP BY theme
ORDER BY value DESC, theme ASC
%[7]s
`, shareSQL, filters.unitWeight(scopeSQL), source, filters.dateWindowClause(), scopeSQL, categorySQL, settingsMaxExecutionTime(), valueSQL)

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query investment theme effort: %w", err)
	}
	defer rows.Close()
	out := make([]ThemeEffortRow, 0, 5)
	for rows.Next() {
		var row ThemeEffortRow
		if err := rows.Scan(&row.Theme, &row.Value, &row.ChurnLOC, &row.PRs); err != nil {
			return nil, fmt.Errorf("scan investment theme effort row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate investment theme effort rows: %w", err)
	}
	return out, nil
}

// SplitWorkCategories splits the work-category filter of a request into the theme and subcategory
// filters of BreakdownFilters, as the Investment page does (core.taxonomy.split_category_filters):
// "theme.leaf" is a subcategory (and names its theme), a bare name is a theme.
func SplitWorkCategories(workCategory []string) (themes, subcategories []string) {
	var themeList, subcategoryList []string
	for _, category := range workCategory {
		trimmed := strings.TrimSpace(category)
		if trimmed == "" {
			continue
		}
		if prefix, _, found := strings.Cut(trimmed, "."); found {
			subcategoryList = append(subcategoryList, trimmed)
			themeList = append(themeList, prefix)
		} else {
			themeList = append(themeList, trimmed)
		}
	}
	return dedupeStrings(themeList), dedupeStrings(subcategoryList)
}

// unitSource is the work-unit source of an effort read: under a repository or team scope, one row per
// (unit, repository) with the repository's ALLOCATED effort (analytics.RepoAllocationInvestmentSource), so the
// repositories of a unit sum to the unit's effort and a team's repositories to the team's; with no scope (or a
// scope that resolved to nothing) the unit's whole effort (CHAOS-9221).
func (f BreakdownFilters) unitSource(scopeSQL string) string {
	if f.scoped(scopeSQL) {
		return analytics.RepoAllocationInvestmentSource()
	}
	return analytics.LatestWorkUnitInvestmentsSource() + " AS work_unit_investments"
}

// unitWeight is the share of the unit a row stands for: the repository's allocation weight under a scope, 1 otherwise.
func (f BreakdownFilters) unitWeight(scopeSQL string) string {
	if f.scoped(scopeSQL) {
		return "allocation_weight"
	}
	return "1.0"
}

// teamShare is the weight of a row's repository: the share of it the teams of the scope hold, 1.0 without a team scope.
// The bindings belong to the expression and are bound only when it is used.
func (f BreakdownFilters) teamShare(scopeSQL string) (expr string, bindings []dhclickhouse.Binding) {
	if !f.scoped(scopeSQL) || len(f.TeamIDs) == 0 {
		return "1.0", nil
	}
	asOf := f.TeamAsOf
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	expr, bindings = teamscope.RepoShare(f.OrgID, "repo_id", f.TeamIDs, asOf)
	if expr == "" {
		return "1.0", nil
	}
	return expr, bindings
}

func (f BreakdownFilters) scoped(scopeSQL string) bool {
	return strings.TrimSpace(scopeSQL) != "" && strings.TrimSpace(strings.TrimPrefix(scopeSQL, " AND ")) != "1 = 0"
}
