package investment

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
)

// QualityStatsFilters is fetch_investment_quality_stats' own parameter
// list (api/queries/investment.py:1008-1079), minus team_scope_ids:
// neither GET/POST /api/v1/investment nor GET
// /api/v1/investment/sunburst ever passes a team scope through to this
// query (that branch belongs to a different caller, api/services/
// analytics.py's own _resolve_evidence_quality_stats), so this port
// omits the unreachable parameter rather than carrying dead code for it
// -- the same only-port-what's-reachable discipline
// analytics.compileInvestmentQualityStats already documents for the same
// reason.
type QualityStatsFilters struct {
	OrgID         string
	StartTS       time.Time
	EndTS         time.Time
	RepoIDs       []string
	Themes        []string
	Subcategories []string
}

func (f QualityStatsFilters) categoryClause() (sql string, bindings []dhclickhouse.Binding) {
	var conditions []string
	if len(f.Themes) > 0 {
		conditions = append(conditions, "hasAny(mapKeys(CAST(theme_distribution_json AS Map(String, Float32))), {themes:Array(String)})")
		bindings = append(bindings, dhclickhouse.Binding{Name: "themes", Value: dedupeStrings(f.Themes)})
	}
	if len(f.Subcategories) > 0 {
		conditions = append(conditions, "hasAny(mapKeys(CAST(subcategory_distribution_json AS Map(String, Float32))), {subcategories:Array(String)})")
		bindings = append(bindings, dhclickhouse.Binding{Name: "subcategories", Value: dedupeStrings(f.Subcategories)})
	}
	if len(conditions) == 0 {
		return "", nil
	}
	return " AND (" + joinOR(conditions) + ")", bindings
}

// QualityStatsRow is fetch_investment_quality_stats' returned dict
// (api/queries/investment.py:1008-1079), before
// _compute_quality_stats' own shaping.
type QualityStatsRow struct {
	Total             int
	QualityKnownCount int
	QualityMean       float64
	QualityStddev     float64
	HighCount         int
	ModerateCount     int
	LowCount          int
	VeryLowCount      int
	UnknownCount      int
}

// FetchInvestmentQualityStats ports fetch_investment_quality_stats
// (api/queries/investment.py:1008-1079) over
// analytics.LatestWorkUnitInvestmentsSource -- see this package's own doc
// comment for why that shared source (and its work_unit_supersessions
// exclusion) is reused rather than ported a second time. found is false
// only when the query returns zero rows outright, which a scalar
// aggregate with no GROUP BY never actually does over a live ClickHouse
// -- the same defensive-but-practically-unreachable shape
// analytics.executeInvestmentQualityStats documents for its own
// equivalent case.
func (r *Reader) FetchInvestmentQualityStats(ctx context.Context, filters QualityStatsFilters) (QualityStatsRow, bool, error) {
	if r == nil || r.client == nil {
		return QualityStatsRow{}, false, ErrUnavailable
	}

	categorySQL, categoryBindings := filters.categoryClause()
	scopeSQL, scopeBindings := scopeClause(filters.RepoIDs)

	query := fmt.Sprintf(`
SELECT
    count() AS total,
    countIf(evidence_quality IS NOT NULL) AS quality_known_count,
    avgIf(evidence_quality, evidence_quality IS NOT NULL) AS quality_mean,
    stddevPopIf(evidence_quality, evidence_quality IS NOT NULL) AS quality_stddev,
    countIf(evidence_quality_band = 'high') AS high_count,
    countIf(evidence_quality_band = 'moderate') AS moderate_count,
    countIf(evidence_quality_band = 'low') AS low_count,
    countIf(evidence_quality_band = 'very_low') AS very_low_count,
    countIf(evidence_quality IS NULL OR evidence_quality_band = '') AS unknown_count
FROM %s AS work_unit_investments
WHERE work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.org_id = {org_id:String}
%s
%s
%s
`, analytics.LatestWorkUnitInvestmentsSource(), scopeSQL, categorySQL, settingsMaxExecutionTime())

	bindings := []dhclickhouse.Binding{
		{Name: "start_date", Value: dateBindingValue(filters.StartTS)},
		{Name: "end_date", Value: dateBindingValue(filters.EndTS)},
		{Name: "org_id", Value: filters.OrgID},
	}
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, categoryBindings...)

	rows, err := r.client.Query(ctx, query, bindings)
	if err != nil {
		return QualityStatsRow{}, false, fmt.Errorf("query investment quality stats: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return QualityStatsRow{}, false, fmt.Errorf("iterate investment quality stats rows: %w", err)
		}
		return QualityStatsRow{}, false, nil
	}

	var total, known, high, moderate, low, veryLow, unknown uint64
	var mean, stddev float64
	if err := rows.Scan(&total, &known, &mean, &stddev, &high, &moderate, &low, &veryLow, &unknown); err != nil {
		return QualityStatsRow{}, false, fmt.Errorf("scan investment quality stats row: %w", err)
	}
	if err := rows.Err(); err != nil {
		return QualityStatsRow{}, false, fmt.Errorf("iterate investment quality stats rows: %w", err)
	}

	return QualityStatsRow{
		Total:             int(total),
		QualityKnownCount: int(known),
		QualityMean:       mean,
		QualityStddev:     stddev,
		HighCount:         int(high),
		ModerateCount:     int(moderate),
		LowCount:          int(low),
		VeryLowCount:      int(veryLow),
		UnknownCount:      int(unknown),
	}, true, nil
}
