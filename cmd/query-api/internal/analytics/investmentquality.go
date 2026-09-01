package analytics

// Package analytics: CHAOS-4723 port of the evidence-quality path --
// _resolve_evidence_quality_stats (analytics.py:210-234) and
// fetch_investment_quality_stats (investment.py:1008-1079, e9ea257ff),
// the ONE query behind BOTH AnalyticsResult.evidenceQualityDistribution
// and AnalyticsResult.evidenceQualityStats (resolve.go's Resolve wires
// this file's resolveEvidenceQualityStats into Phase 4).
//
// ROOT CAUSE THIS FILE FIXES: resolve.go used to hardcode both fields to
// nil UNCONDITIONALLY, citing this exact gate
// (`if not bool(batch.use_investment): return None`) as its reason --
// but that gate returns None only when use_investment is FALSE, and the
// web client's `investmentBreakdown`/`investmentFull` documents send
// `useInvestment: true` (their entire reason for existing). The gate
// PASSES for the client's real traffic and Python returns real data;
// the port's own doc comment misread its own citation. See
// resolveEvidenceQualityStats below for the gate implemented as Python
// actually has it.
//
// TELEMETRY (root AGENTS.md standing order -- new logic ships with
// telemetry in the same PR): this file adds no NEW counter, deliberately,
// not by omission. Phase 4 is FATAL on error (resolve.go's package doc
// comment), never swallowed -- so unlike sankey/flowMatrix (Phase 2/3,
// telemetry.go's degradedCounter exists BECAUSE they swallow an execute
// failure to an empty result, which would otherwise be indistinguishable
// from a legitimately-empty one), a Phase 4 query failure propagates as a
// real error out of Resolve() exactly like Phase 0/1's (repo-filter
// resolution, timeseries, breakdowns) already do -- and those phases,
// the closest precedent in this same file, carry no dedicated counter
// either. It is already caught by the existing resolver-level
// analyticsCallCounter/analyticsOutcomeCounter
// (internal/graph/telemetry.go's startAnalyticsSpan, wrapping the WHOLE
// Resolve() call, CHAOS-4506) as an "error" outcome -- see that
// counter's own doc comment: only a SWALLOWED failure reports "ok" where
// it shouldn't; a FATAL one already reports "error" correctly with no
// help from this file. The one telemetry call this path DOES make --
// RecordStaleInvestmentMembershipScope -- is pre-existing
// instrumentation, reused (not new), matching every other investment
// query in this package.

import (
	"context"
	"fmt"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqljson"
)

// analyticsQualityWindow ports _analytics_quality_window (analytics.py:
// 175-181) exactly: the evidence-quality window is borrowed from the
// FIRST breakdown request's date range, falling back to the first
// timeseries request's, and is entirely absent (not merely empty) if
// neither is present -- there is no third, independent window. Every
// DateRange this function reads is already validated non-nil by
// Resolve's own validation loop, run before Phase 4, so this never
// re-checks that -- matching Python's own unchecked
// `batch.breakdowns[0].date_range` access, which relies on the same
// upstream guarantee (Strawberry input validation on the Python side).
func analyticsQualityWindow(batch model.AnalyticsRequestInput) (start, end graphqldate.Date, ok bool) {
	if len(batch.Breakdowns) > 0 {
		dr := batch.Breakdowns[0].DateRange
		return dr.StartDate, dr.EndDate, true
	}
	if len(batch.Timeseries) > 0 {
		dr := batch.Timeseries[0].DateRange
		return dr.StartDate, dr.EndDate, true
	}
	return graphqldate.Date{}, graphqldate.Date{}, false
}

// resolveEvidenceQualityStats ports _resolve_evidence_quality_stats
// (analytics.py:210-234) verbatim, including its scope-filter
// construction (analytics.py:222-232) -- which is its OWN bespoke
// inline logic, distinct from filtertranslation.go's translateFilters
// (that machinery serves the breakdown/timeseries/sankey/flowMatrix
// compilers' `ut`/`au`-joined queries; this path never joins `au` at
// all, and joins `unit_team`, not `ut`, only when a team scope is
// active). Notably narrower than translateFilters, faithfully: only
// filters.scope (team or repo level; org/service/developer are silently
// ignored, no error -- Python's `_resolve_evidence_quality_stats` never
// raises here, unlike translateFilters' developer-scope rejection),
// filters.what.repos, and filters.why.work_category are read.
// filters.who and filters.how are NEVER consulted (Python doesn't
// either) -- this is deliberate incompleteness inherited from the
// source, not a gap this port introduces.
func resolveEvidenceQualityStats(ctx context.Context, client QueryClient, orgID string, batch model.AnalyticsRequestInput, useInvestment bool, filters *model.FilterInput) (*model.EvidenceQualityStats, error) {
	// analytics.py:217-218's gate, implemented as Python actually has
	// it: nil ONLY when useInvestment is false. This is the exact line
	// CHAOS-4723's root cause misread.
	if !useInvestment {
		return nil, nil
	}
	startDate, endDate, ok := analyticsQualityWindow(batch)
	if !ok {
		return nil, nil
	}

	var scopeFilter string
	var scopeBindings []clickhouse.Binding
	var teamScopeIDs []string
	var themes []string

	if filters != nil {
		if filters.Scope != nil && len(filters.Scope.Ids) > 0 {
			switch filters.Scope.Level {
			case model.ScopeLevelInputTeam:
				teamScopeIDs = filters.Scope.Ids
			case model.ScopeLevelInputRepo:
				scopeFilter += " AND work_unit_investments.repo_id IN {scope_ids:Array(String)}"
				scopeBindings = append(scopeBindings, clickhouse.Binding{Name: "scope_ids", Value: filters.Scope.Ids})
			}
		}
		if filters.What != nil && len(filters.What.Repos) > 0 {
			scopeFilter += " AND work_unit_investments.repo_id IN {repo_filter_ids:Array(String)}"
			scopeBindings = append(scopeBindings, clickhouse.Binding{Name: "repo_filter_ids", Value: filters.What.Repos})
		}
		if filters.Why != nil && len(filters.Why.WorkCategory) > 0 {
			themes = filters.Why.WorkCategory
		}
	}

	// Ports _query_investment_dicts (investment.py:175-181): EVERY
	// investment query fires the stale-membership-scope telemetry check
	// immediately before its own real query -- fetch_investment_quality_stats
	// is no exception. See resolveOneTimeseries's identical call for the
	// full reasoning (this file's caller reaches here only when
	// useInvestment is true, same precondition).
	RecordStaleInvestmentMembershipScope(ctx, client, orgID, queryTimeoutSecs)
	// CHAOS-4759 transition guard: bounded-cooldown check, see
	// RecordArgMaxNullTransitionGuard's doc comment.
	RecordArgMaxNullTransitionGuard(ctx, client, orgID, queryTimeoutSecs)

	q := compileInvestmentQualityStats(orgID, startDate, endDate, scopeFilter, scopeBindings, themes, teamScopeIDs)
	row, found, err := executeInvestmentQualityStats(ctx, client, q)
	if err != nil {
		return nil, err
	}
	if !found {
		// analytics.py:225-226: `if not row: return EvidenceQualityStats()`
		// -- zero result rows (not "total=0"; an aggregate query with no
		// GROUP BY always emits exactly one row) is the same all-defaults
		// fallback Python returns for a truly empty result set.
		return &model.EvidenceQualityStats{}, nil
	}

	bandCounts := map[string]int{
		"high":     row.HighCount,
		"moderate": row.ModerateCount,
		"low":      row.LowCount,
		"very_low": row.VeryLowCount,
		"unknown":  row.UnknownCount,
	}
	bandCountsJSON, err := graphqljson.FromValue(bandCounts)
	if err != nil {
		return nil, fmt.Errorf("evidenceQualityStats: bandCounts: %w", err)
	}

	// analytics.py:235-240: mean/stddev are only ever populated when at
	// least one row had a known (non-NULL) evidence_quality -- guards a
	// stray avgIf()/stddevPopIf() NaN over zero matching rows from ever
	// reaching the GraphQL Float marshaler (the exact CHAOS-4650-class
	// hazard this epic tracks elsewhere), not merely a None/null
	// preference.
	var mean, stddev *float64
	if row.QualityKnownCount > 0 {
		meanValue, stddevValue := row.QualityMean, row.QualityStddev
		mean, stddev = &meanValue, &stddevValue
	}

	return &model.EvidenceQualityStats{
		Mean:       mean,
		Stddev:     stddev,
		Total:      row.Total,
		BandCounts: bandCountsJSON,
	}, nil
}

// unitTeamWindowFilter ports unit_team_window_filter (api/queries/
// investment.py:513-520) verbatim in shape, substituting this port's
// established {start_date:Date}/{end_date:Date} binding convention for
// Python's raw %(start_ts)s/%(end_ts)s datetime params -- see
// validate.go's dateBindingValue doc comment for why every OTHER
// date-window predicate in this package already binds this way (six
// sites shared the identical {name:Date}-vs-time.Time formatting defect
// before that fix); introducing a fresh DateTime64 binding here instead
// of reusing the proven convention would only reopen that same defect
// class one file later, for no semantic gain -- ClickHouse implicitly
// promotes a bound Date value to midnight UTC when compared against a
// DateTime64 column, so `from_ts < {end_date:Date}` / `to_ts >=
// {start_date:Date}` expresses the same predicate as Python's
// midnight-UTC start_ts/end_ts.
func unitTeamWindowFilter(scopeFilter, categoryFilter string) string {
	return fmt.Sprintf(`                WHERE work_unit_investments.from_ts < {end_date:Date}
                  AND work_unit_investments.to_ts >= {start_date:Date}
                  AND work_unit_investments.org_id = {org_id:String}
                %s
                %s`, scopeFilter, categoryFilter)
}

// compileInvestmentQualityStats ports fetch_investment_quality_stats's
// query construction (investment.py:1008-1079, e9ea257ff) -- the ONE
// query behind both EvidenceQualityDistribution and EvidenceQualityStats.
// Python's `subcategories` keyword (investment.py:1017) is real but
// _resolve_evidence_quality_stats -- the only caller either side of this
// port has -- never passes it (only `themes`, from
// filters.why.work_category); this port omits the parameter rather than
// build unreachable code for it, the same only-port-what's-reachable
// discipline breakdown.go's label-resolution gap already documents.
//
// RESTRUCTURING (this package's standing §9 discipline): Python's
// `WITH {LATEST_WORK_UNIT_INVESTMENTS_CTE} SELECT ... FROM
// latest_work_unit_investments AS work_unit_investments` becomes `FROM
// (SELECT ...) AS work_unit_investments` -- latestWorkUnitInvestmentsSource()
// inlined, no WITH, matching every other caller in this package
// (investmentContextFor's identical non-repo-allocation branch).
func compileInvestmentQualityStats(orgID string, startDate, endDate graphqldate.Date, scopeFilter string, scopeBindings []clickhouse.Binding, themes []string, teamScopeIDs []string) compiledQuery {
	var categoryFilter string
	var categoryBindings []clickhouse.Binding
	if len(themes) > 0 {
		categoryFilter = " AND (hasAny(mapKeys(CAST(theme_distribution_json AS Map(String, Float32))), {themes:Array(String)}))"
		categoryBindings = append(categoryBindings, clickhouse.Binding{Name: "themes", Value: themes})
	}

	var teamJoin, teamFilter string
	var teamBindings []clickhouse.Binding
	if len(teamScopeIDs) > 0 {
		unitTeamSQL := buildUnitTeamSubquery(unitTeamSubqueryOptions{
			Source:         fmt.Sprintf("%s AS work_unit_investments", latestWorkUnitInvestmentsSource()),
			Where:          unitTeamWindowFilter("", ""),
			InnerTeamAlias: "team_label",
			IncludeTeamID:  true,
		})
		teamJoin = fmt.Sprintf("\n        LEFT JOIN (%s        ) AS unit_team ON unit_team.work_unit_id = work_unit_investments.work_unit_id\n        ", unitTeamSQL)
		teamFilter = `
          AND (
              unit_team.team_label IN {team_scope_ids:Array(String)}
              OR unit_team.team_id IN {team_scope_ids:Array(String)}
          )`
		teamBindings = append(teamBindings, clickhouse.Binding{Name: "team_scope_ids", Value: teamScopeIDs})
	}

	sql := fmt.Sprintf(`
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
FROM %s AS work_unit_investments%s
WHERE work_unit_investments.from_ts < {end_date:Date}
  AND work_unit_investments.to_ts >= {start_date:Date}
  AND work_unit_investments.org_id = {org_id:String}
%s%s%s
%s
`, latestWorkUnitInvestmentsSource(), teamJoin, scopeFilter, teamFilter, categoryFilter, settingsMaxExecutionTime(queryTimeoutSecs))

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(startDate.Time())},
		{Name: "end_date", Value: dateBindingValue(endDate.Time())},
	}
	bindings = append(bindings, scopeBindings...)
	bindings = append(bindings, teamBindings...)
	bindings = append(bindings, categoryBindings...)

	return compiledQuery{sql: sql, bindings: bindings}
}

// investmentQualityStatsRow is the raw scanned row from
// compileInvestmentQualityStats's query -- the Go equivalent of
// fetch_investment_quality_stats's returned dict (investment.py:
// 1008-1079). count()/countIf() are UInt64 in ClickHouse; the native Go
// driver rejects scanning an unsigned column into a signed destination
// outright (reviewedges.go:145's identical UInt32 note), so every count
// is scanned into uint64 first and narrowed to int only once the value is
// safely in Go -- see executeInvestmentQualityStats. evidence_quality is
// a non-nullable Float64 column (migration 017), so avgIf/stddevPopIf
// over it need no Nullable-aware scan destination.
type investmentQualityStatsRow struct {
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

// executeInvestmentQualityStats runs compileInvestmentQualityStats's
// query and scans its single aggregate row. found is false only when
// the query returned zero rows outright (see resolveEvidenceQualityStats's
// "if not row" handling) -- an aggregate query with no GROUP BY always
// emits exactly one row even when zero source rows matched its WHERE
// clause, so found is normally true with Total==0 in that case, not
// false.
func executeInvestmentQualityStats(ctx context.Context, client QueryClient, q compiledQuery) (investmentQualityStatsRow, bool, error) {
	rows, err := client.Query(ctx, q.sql, q.bindings)
	if err != nil {
		return investmentQualityStatsRow{}, false, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return investmentQualityStatsRow{}, false, fmt.Errorf("rows: %w", err)
		}
		return investmentQualityStatsRow{}, false, nil
	}

	var total, known, high, moderate, low, veryLow, unknown uint64
	var mean, stddev float64
	if scanErr := rows.Scan(&total, &known, &mean, &stddev, &high, &moderate, &low, &veryLow, &unknown); scanErr != nil {
		return investmentQualityStatsRow{}, false, fmt.Errorf("scan: %w", scanErr)
	}
	if err := rows.Err(); err != nil {
		return investmentQualityStatsRow{}, false, fmt.Errorf("rows: %w", err)
	}

	return investmentQualityStatsRow{
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
