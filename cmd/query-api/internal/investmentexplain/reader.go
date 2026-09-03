// Package investmentexplain is the Go port of CHAOS-4977's
// POST /api/v1/investment/explain -- the LLM-generated explanation of an
// org's investment mix.
//
// This file is the ClickHouse read side: api/queries/investment.py's
// fetch_investment_breakdown and fetch_mock_fixture_investment_row_count,
// two of the three queries api/services/investment.py's
// build_investment_response issues. The third,
// fetch_investment_quality_stats, is NOT re-ported here -- it already has
// a full Go port in cmd/query-api/internal/analytics (CHAOS-4723,
// investmentquality.go's compileInvestmentQualityStats/
// executeInvestmentQualityStats), reused via that package's exported
// FetchInvestmentQualityStats wrapper rather than duplicated, so this
// dedup-critical query has exactly one Go implementation, not two that
// could drift apart.
//
// CLIENT CONVENTION: this package takes analytics.QueryClient (github.com/
// full-chaos/dev-health-go/clickhouse's Binding/RowScanner), matching its
// sibling package under cmd/query-api/internal -- NOT
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
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
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
// RepoIDs replaces Python's generic scope_filter/scope_params pair:
// build_investment_response (investment.py:141-246) is the ONLY caller of
// both functions, and it ALWAYS resolves filters.scope.level in
// {"team", "repo"} down to a concrete repo-id list before calling
// build_scope_filter_multi("repo", repo_ids, ...) -- the literal string
// "repo" is hardcoded at that call site regardless of which scope level the
// request asked for (resolve_repo_filter_ids already did the team ->
// repos translation). So for this specific, sole call path the only two
// shapes scope_filter/scope_params ever take are "" (no scope) or
// " AND repo_id IN %(scope_ids)s" -- accepting RepoIDs directly here is
// behaviorally identical to porting build_scope_filter_multi's generic
// team/repo branching, without carrying a dead "team" branch this call path
// can never reach.
type BreakdownFilters struct {
	OrgID         string
	StartTS       time.Time
	EndTS         time.Time
	RepoIDs       []string
	Themes        []string
	Subcategories []string
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

func (f BreakdownFilters) scopeClause() (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(f.RepoIDs) == 0 {
		return "", nil
	}
	return " AND repo_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: dedupeStrings(f.RepoIDs)},
	}
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
// cmd/query-api/internal/analytics uses for LatestWorkUnitInvestmentsSource
// -- see sankeycoverage.go:136 -- rather than Python's
// WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE} name-then-alias form; both
// compile to the same query plan.
func (reader *Reader) FetchInvestmentBreakdown(ctx context.Context, filters BreakdownFilters) ([]BreakdownRow, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}

	categorySQL, categoryBindings := filters.categoryClause()
	scopeSQL, scopeBindings := filters.scopeClause()
	bindings := filters.baseBindings()
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, categoryBindings...)

	query := fmt.Sprintf(`
SELECT
    subcategory_kv.1 AS subcategory,
    splitByChar('.', subcategory_kv.1)[1] AS theme,
    sum(subcategory_kv.2 * effort_value) AS value
FROM %s AS work_unit_investments
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.org_id = {org_id:String}
%s
%s
GROUP BY subcategory, theme
ORDER BY value DESC
%s
`, analytics.LatestWorkUnitInvestmentsSource(), scopeSQL, categorySQL, settingsMaxExecutionTime())

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
