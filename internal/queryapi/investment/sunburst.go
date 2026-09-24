package investment

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

// SunburstFilters is fetch_investment_sunburst's own parameter list
// (api/queries/investment.py:959-1005), RepoIDs replacing the generic
// scope_filter/scope_params pair the same way
// investmentexplain.BreakdownFilters.RepoIDs already does for
// fetch_investment_breakdown -- build_investment_sunburst
// (api/services/investment.py:249-321) is the only caller, and it always
// resolves filters.scope.level in {"team","repo"} down to a concrete
// repo-id list first.
type SunburstFilters struct {
	OrgID              string
	StartTS            time.Time
	EndTS              time.Time
	RepoIDs            []string
	TeamScopeCondition string
	TeamScopeBindings  []dhclickhouse.Binding
	Themes             []string
	Subcategories      []string
	Limit              int
}

func (f SunburstFilters) categoryFilters() categoryFilters {
	return categoryFilters{Themes: f.Themes, Subcategories: f.Subcategories}
}

// SunburstRow is one fetch_investment_sunburst result row, before
// build_investment_sunburst's own "Unassigned"/"Other" default-shaping.
type SunburstRow struct {
	Subcategory string
	Theme       string
	Scope       string
	Value       float64
}

// FetchInvestmentSunburst ports fetch_investment_sunburst
// (api/queries/investment.py:959-1005) over
// analytics.LatestWorkUnitInvestmentsSource -- see this package's own doc
// comment for why that shared source is reused rather than ported a
// second time.
//
// GO-ONLY FIX, the same class and the same table already fixed
// elsewhere in this service (analytics.investmentContextFor's own repo
// join): Python's `LEFT JOIN repos AS r ON toString(r.id) =
// toString(repo_id)` reads the raw `repos` table -- ReplacingMergeTree
// keyed (org_id, id) -- with no FINAL and no org scoping, so an unmerged
// repo row can fan this aggregate out by however many physical versions
// are still live for it. This port reads the join FINAL, with the org_id
// predicate carried on the same join, matching the established fix for
// the identical table and identical join shape elsewhere in this
// service. Go is correct; the Python behaviour is the declared baseline
// defect (see the PR's corpus entries and RISK-NOTES).
func (r *Reader) FetchInvestmentSunburst(ctx context.Context, filters SunburstFilters) ([]SunburstRow, error) {
	if r == nil || r.client == nil {
		return nil, ErrUnavailable
	}

	categorySQL, categoryBindings := filters.categoryFilters().clause(
		"splitByChar('.', subcategory_kv.1)[1]", "subcategory_kv.1",
	)
	scopeSQL, scopeBindings := combinedScopeClause(filters.RepoIDs, filters.TeamScopeCondition, filters.TeamScopeBindings)

	query := fmt.Sprintf(`
SELECT
    subcategory_kv.1 AS subcategory,
    splitByChar('.', subcategory_kv.1)[1] AS theme,
    ifNull(r.repo, toString(repo_id)) AS scope,
    sum(subcategory_kv.2 * effort_value) AS value
FROM %s AS work_unit_investments
LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(repo_id) AND r.org_id = {org_id:String}
ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv
WHERE work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.org_id = {org_id:String}
%s
%s
GROUP BY theme, subcategory, scope
ORDER BY value DESC
LIMIT {limit:Int64}
%s
`, analytics.LatestWorkUnitInvestmentsSource(), scopeSQL, categorySQL, settingsMaxExecutionTime())

	// filters.Limit is bound AS GIVEN -- no clamping, no zero/negative
	// default-substitution here. Python's own investment_sunburst query
	// param default (500) applies only when the caller OMITS `limit`
	// entirely (api/main.py:1272-1280); a caller-supplied 0 or negative
	// value is sent to ClickHouse verbatim (LIMIT 0 legitimately answers
	// zero rows; a negative LIMIT is a ClickHouse syntax/type error,
	// which this route's shared try/except degrades to the same generic
	// 503 every other unexpected failure does). The "omitted -> 500"
	// substitution is the route layer's job (the same query-param-default
	// convention every other int query parameter in this service already
	// follows), not this reader's.
	bindings := []dhclickhouse.Binding{
		{Name: "start_date", Value: dateBindingValue(filters.StartTS)},
		{Name: "end_date", Value: dateBindingValue(filters.EndTS)},
		{Name: "org_id", Value: filters.OrgID},
		{Name: "limit", Value: int64(filters.Limit)},
	}
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, categoryBindings...)

	rows, err := r.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query investment sunburst: %w", err)
	}
	defer rows.Close()

	results := make([]SunburstRow, 0)
	for rows.Next() {
		var row SunburstRow
		if err := rows.Scan(&row.Subcategory, &row.Theme, &row.Scope, &row.Value); err != nil {
			return nil, fmt.Errorf("scan investment sunburst row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate investment sunburst rows: %w", err)
	}
	return results, nil
}
