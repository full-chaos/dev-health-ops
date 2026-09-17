// Package investment is the Go port of GET/POST /api/v1/investment and
// GET /api/v1/investment/sunburst -- api/services/investment.py's
// build_investment_response and build_investment_sunburst, backed by
// api/queries/investment.py's fetch_investment_breakdown,
// fetch_mock_fixture_investment_row_count, fetch_investment_quality_stats
// and fetch_investment_sunburst, and api/models/schemas.py's
// InvestmentResponse/InvestmentSunburstSlice/EvidenceQualityStats.
//
// The dedup-critical CTE both build_investment_response and
// build_investment_sunburst read through (LATEST_WORK_UNIT_INVESTMENTS_CTE,
// chained with the membership-scope gate) already has a single Go
// implementation, analytics.LatestWorkUnitInvestmentsSource, reused here
// rather than ported a second time -- the same convention
// investmentexplain already establishes for the sibling
// /api/v1/investment/explain route. That source additionally excludes
// any work unit present in work_unit_supersessions, a read this route's
// own Python source has no knowledge of at all: an investment surface
// answering for a work unit a later regrouping run has already retired
// is the gap, not the exclusion, so the Go behaviour is kept and the
// Python side is the declared baseline defect (see the PR's corpus
// entries and RISK-NOTES).
//
// fetch_investment_breakdown, fetch_mock_fixture_investment_row_count and
// resolve_repo_filter_ids already have exported Go ports on
// investmentexplain.Reader (FetchInvestmentBreakdown,
// FetchMockFixtureInvestmentRowCount, ResolveRepoFilterIDs) built for
// exactly this reuse; this package composes them rather than carrying a
// second, independent copy that could drift. fetch_investment_quality_stats
// and fetch_investment_sunburst have no existing Go port that also
// supports a `subcategories` filter (analytics.FetchInvestmentQualityStats
// only ever receives `themes` from its one caller today), so both are
// ported fresh here, over the same shared CTE.
package investment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// ErrUnavailable is returned when the reader is constructed without a
// client.
var ErrUnavailable = errors.New("investment: clickhouse client unavailable")

// queryTimeoutSecs matches analytics/investmentexplain's own constant --
// the trailing SETTINGS max_execution_time clause must be a literal
// integer, never a bound parameter (see investmentexplain.Reader's own
// doc comment on this exact point); timeoutSeconds here is always this
// constant, never request-supplied.
const queryTimeoutSecs = 30

func settingsMaxExecutionTime() string {
	return fmt.Sprintf("SETTINGS max_execution_time = %d", queryTimeoutSecs)
}

// dateBindingValue formats a time.Time as a bare "YYYY-MM-DD" string for
// binding into a {name:Date}-typed native ClickHouse parameter -- see
// investmentexplain.Reader's own dateBindingValue doc comment for why a
// raw time.Time cannot bind directly against a Date placeholder.
func dateBindingValue(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%04d-%02d-%02d", year, int(month), day)
}

// Reader reads the ClickHouse data this route family needs. It wraps
// investmentexplain.Reader for the three fetches already ported there
// (breakdown, mock-fixture count, repo-filter resolution) rather than
// re-implementing them, and holds its own analytics.QueryClient for the
// quality-stats and sunburst reads this package ports itself.
type Reader struct {
	client   analytics.QueryClient
	explainR *investmentexplain.Reader
}

// NewReader returns a Reader over the given query client.
func NewReader(client analytics.QueryClient) (*Reader, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	explainR, err := investmentexplain.NewReader(client)
	if err != nil {
		return nil, err
	}
	return &Reader{client: client, explainR: explainR}, nil
}

// ResolveRepoFilterIDs ports resolve_repo_filter_ids
// (api/services/filtering.py:95-110), reused directly from
// investmentexplain.Reader's own exported implementation of this exact
// function rather than a second copy.
func (r *Reader) ResolveRepoFilterIDs(ctx context.Context, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	if r == nil || r.explainR == nil {
		return nil, ErrUnavailable
	}
	return r.explainR.ResolveRepoFilterIDs(ctx, scopeLevel, scopeIDs, whatRepos, orgID)
}

// categoryFilters is the (themes, subcategories) pair every fetch in this
// package filters on, plus the shared clause/binding builders every
// query needs -- mirrors investmentexplain.BreakdownFilters' own
// categoryClause/scopeClause split, duplicated rather than imported
// (investmentexplain.BreakdownFilters' own methods are unexported): a
// small, self-contained helper, the same repeat-don't-couple convention
// investmentexplain.Reader's own doc comment already establishes for
// settingsMaxExecutionTime/dedupeStrings.
type categoryFilters struct {
	Themes        []string
	Subcategories []string
}

func (f categoryFilters) clause(themeExpr, subcategoryExpr string) (sql string, bindings []dhclickhouse.Binding) {
	var conditions []string
	if len(f.Themes) > 0 {
		conditions = append(conditions, fmt.Sprintf("%s IN {themes:Array(String)}", themeExpr))
		bindings = append(bindings, dhclickhouse.Binding{Name: "themes", Value: dedupeStrings(f.Themes)})
	}
	if len(f.Subcategories) > 0 {
		conditions = append(conditions, fmt.Sprintf("%s IN {subcategories:Array(String)}", subcategoryExpr))
		bindings = append(bindings, dhclickhouse.Binding{Name: "subcategories", Value: dedupeStrings(f.Subcategories)})
	}
	if len(conditions) == 0 {
		return "", nil
	}
	sql = " AND (" + joinOR(conditions) + ")"
	return sql, bindings
}

func scopeClause(repoIDs []string) (sql string, bindings []dhclickhouse.Binding) {
	if len(repoIDs) == 0 {
		return "", nil
	}
	return " AND repo_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: dedupeStrings(repoIDs)},
	}
}

// combinedScopeClause renders the explicit repo refs and a team scope's own
// ownership condition as one clause. Both present means the union, matching
// what a request naming a team and explicit repos asks for; neither present
// means no clause, which is an org-wide read.
func combinedScopeClause(repoIDs []string, teamCondition string, teamBindings []dhclickhouse.Binding) (sql string, bindings []dhclickhouse.Binding) {
	explicitSQL, explicitBindings := scopeClause(repoIDs)
	switch {
	case explicitSQL != "" && teamCondition != "":
		return " AND (repo_id IN {scope_ids:Array(String)} OR " + teamCondition + ")",
			append(append([]dhclickhouse.Binding{}, explicitBindings...), teamBindings...)
	case explicitSQL != "":
		return explicitSQL, explicitBindings
	case teamCondition != "":
		return " AND " + teamCondition, teamBindings
	}
	return "", nil
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

// splitCategoryFilters ports core.taxonomy.split_category_filters, the
// pure logic api/services/investment.py's own _split_category_filters
// delegates to: a value with a dot is a subcategory AND contributes its
// prefix to themes; a value with no dot is a theme only. Both return
// lists are deduplicated, ordered by first appearance.
func splitCategoryFilters(workCategory []string) (themes, subcategories []string) {
	var themeList, subcategoryList []string
	for _, category := range workCategory {
		trimmed := pythonparity.Strip(category)
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
