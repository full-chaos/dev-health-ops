package analytics

// sankeycoverage.go: Go port of the SankeyResult.coverage computation
// (resolvers/analytics.py:658-907), the ~250-line sub-feature
// resolve.go's Resolve doc comment recorded as "NOT YET PORTED --
// always nil" and registered_document_field_gate_test.go carried as its
// one ticketed exception. Closing that gap is this file's whole purpose,
// so the exception is removed in the same change.
//
// RESTRUCTURING (same constraint investmentmembershipscope.go documents):
// Python builds this query with a leading `WITH
// LATEST_WORK_UNIT_INVESTMENTS_CTE, LATEST_WORK_UNIT_REPO_EFFORT_CTE[,
// LATEST_WORK_UNIT_AUTHORS_CTE]` clause and references those CTEs by
// name. The dev-health-go v0.4.0 ClickHouse client requires a literal
// SELECT as the first token and REJECTS a leading WITH
// (clickhouse/client.go:190), so every named CTE becomes an inlined
// `(SELECT ...)` subquery here -- and `latest_work_unit_repo_effort`,
// referenced twice in Python (the `wure` join and the `wure_counts`
// aggregate), is therefore embedded twice. That is textual duplication
// with identical semantics, not a divergence; the same trade-off
// investmentContextFor already makes for the team-vote subquery.
//
// TWO deliberate divergences from Python on the non-investment path,
// both because Go is held to Go's own correctness, not to matching a
// known-wrong Python read:
//
//  1. The non-investment FROM source is investmentMetricsDailyDedupSource
//     (timeseries.go), NOT the raw `investment_metrics_daily` table
//     Python's coverage query reads (analytics.py:673-677: `table = ...
//     if request.use_investment else "investment_metrics_daily"`).
//     investment_metrics_daily is a plain MergeTree that does not
//     self-merge duplicate (re)writes of the same natural key -- a
//     row re-synced more than once for the same
//     (org, day, team, repo, theme, subcategory) key is counted once per
//     generation on the raw table, inflating both the numerator and
//     denominator. timeseries.go/breakdown.go's non-investment paths
//     already read this table through the deduped source; the coverage
//     query is now the third, not an intentional exception.
//  2. Python appends an ARRAY JOIN over subcategory_distribution_json
//     (analytics.py:829-834) and this query does not, because that join
//     silently re-weighted every effort-weighted column -- see the long
//     note at the filter site.
//
// Both are computed, direction-known differences (Go corrects a
// generation-count/row-multiplication defect; Python does not), not
// arbitrary drift -- see the two-plane proof declarations for either
// path this reaches.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// investmentCoverageQueryFailedCounter is the Go equivalent of Python's
// INVESTMENT_COVERAGE_QUERY_FAILED_TOTAL
// (investment_coverage_telemetry.py). CHAOS-4241's reason for it applies
// verbatim to this port: the except/degrade-to-nil path once swallowed a
// real SQL bug (an ambiguous `repo_id`) for an unknown period, so the
// fallback itself must be loud even though the UI behaviour stays
// "coverage is null".
var investmentCoverageQueryFailedCounter = mustAnalyticsCounter(
	"devhealth_query_api_investment_coverage_query_failed_total",
	"sankey coverage queries that failed and degraded coverage to null, by resolver and reason",
)

// coverageFailureStage classifies a coverage failure into a LOW-CARDINALITY
// metric label.
//
// DELIBERATE DEVIATION, stated rather than hidden: Python labels this
// counter `reason=type(e).__name__` (analytics.py:891-893), which is
// low-cardinality by construction because it is an exception CLASS name.
// Go has no equivalent -- err.Error() here carries table names, ClickHouse
// error codes and query fragments, so using it as a metric label would be
// an unbounded-cardinality defect. The stage is the faithful
// low-cardinality analogue; the full error text still reaches the log and
// the span, where high cardinality is correct.
type coverageFailureStage string

const (
	coverageStageCompile coverageFailureStage = "compile"
	coverageStageQuery   coverageFailureStage = "query"
	coverageStageScan    coverageFailureStage = "scan"
	coverageStageRows    coverageFailureStage = "rows"
)

// recordInvestmentCoverageFailure is a package var for the same reason
// recordDegradation is (telemetry.go): asserting on the degraded RESULT
// cannot distinguish "coverage failed and was swallowed" from "coverage
// is legitimately absent" -- that indistinguishability is the defect
// CHAOS-4241 addressed -- so the report is the only observable and it has
// to be injectable to be assertable.
var recordInvestmentCoverageFailure = defaultRecordInvestmentCoverageFailure

// defaultRecordInvestmentCoverageFailure carries queryID and, when err is
// (or wraps) a ClickHouse server exception, the exception's code/name --
// CHAOS-6121: the 09-20 STEP 142 incident logged this event 15x with only
// an opaque errorString, and the ClickHouse code (735,
// QUERY_WAS_CANCELLED_BY_CLIENT) was visible only from the ClickHouse side,
// forcing a manual system.query_log search to correlate. queryID is empty
// for coverageStageCompile (compileSankeyCoverage fails before any
// statement is ever sent, so there is no query to match) and is otherwise
// the id resolveSankeyCoverage itself minted and bound to the request via
// clickhousedriver.WithQueryID -- the same id ClickHouse records in
// system.query_log, not a value invented after the fact. The exception
// fields use errors.As over the SAME unwrap chain
// isMissingTable/QueryBudgetExceededCode already rely on (dev-health-go's
// operationError.Unwrap() to the driver's *clickhousedriver.Exception), so
// a non-exception failure (context cancellation, a transport error) simply
// leaves them unset rather than misreporting a code.
func defaultRecordInvestmentCoverageFailure(ctx context.Context, orgID string, measure Measure, useInvestment bool, stage coverageFailureStage, queryID string, err error) {
	investmentCoverageQueryFailedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("resolver", "investment_coverage"),
		attribute.String("reason", string(stage)),
	))

	spanAttrs := []attribute.KeyValue{
		attribute.String("resolver", "investment_coverage"),
		attribute.String("stage", string(stage)),
		attribute.String("error", err.Error()),
		attribute.String("error.cause", rootCause(err).Error()),
	}
	// Mirrors Python's structured logger.error("investment_coverage.query_failed", extra={...})
	// (analytics.py:894-906) field for field, plus the query_id/ClickHouse-code
	// fields Python never had. org_id is an internal tenant UUID, already
	// logged by this package elsewhere.
	logAttrs := []any{
		"resolver", "investment_coverage",
		"org_id", orgID,
		"measure", string(measure),
		"use_investment", useInvestment,
		"stage", string(stage),
		"error", err.Error(),
	}

	if queryID != "" {
		spanAttrs = append(spanAttrs, attribute.String("query_id", queryID))
		logAttrs = append(logAttrs, "query_id", queryID)
	}

	var exception *clickhousedriver.Exception
	if errors.As(err, &exception) {
		spanAttrs = append(spanAttrs,
			attribute.Int64("clickhouse_code", int64(exception.Code)),
			attribute.String("clickhouse_exception", exception.Name),
		)
		logAttrs = append(logAttrs,
			"clickhouse_code", exception.Code,
			"clickhouse_exception", exception.Name,
		)
	}

	trace.SpanFromContext(ctx).AddEvent("investment_coverage.query_failed", trace.WithAttributes(spanAttrs...))
	slog.ErrorContext(ctx, "investment_coverage.query_failed", logAttrs...)
}

// isLocalValidationFailure reports whether err is, or wraps, one of
// dev-health-go's two LOCAL (pre-dispatch) validation sentinels --
// ErrUnsafeStatement (validateReadOnlyStatement) or ErrInvalidBinding
// (translateBindings). Both run inside Client.Query BEFORE it ever calls
// the driver (client.go:111-133): a query failing this way never reached
// ClickHouse, so no query id bound to it was ever sent, and none can
// appear in system.query_log. See resolveSankeyCoverage's call site for
// why that makes the id worth suppressing rather than reporting.
func isLocalValidationFailure(err error) bool {
	return errors.Is(err, clickhouse.ErrUnsafeStatement) || errors.Is(err, clickhouse.ErrInvalidBinding)
}

// compileSankeyCoverage ports the query construction half of
// analytics.py:658-865. Returns the compiled statement plus bindings.
func compileSankeyCoverage(req SankeyRequest, orgID string, timeoutSeconds int, useInvestment bool, filters *model.FilterInput) (compiledQuery, error) {
	teamCol, err := dbColumn(DimensionTeam, useInvestment)
	if err != nil {
		return compiledQuery{}, fmt.Errorf("coverage team column: %w", err)
	}
	repoCol, err := dbColumn(DimensionRepo, useInvestment)
	if err != nil {
		return compiledQuery{}, fmt.Errorf("coverage repo column: %w", err)
	}

	// analytics.py:707 -- note the nesting is Python's own: team_col is
	// already an ifNull(nullIf(...), 'unassigned') display expression on
	// the investment path, and Python wraps it again. Ported as written.
	assignedTeamExpr := fmt.Sprintf("lower(ifNull(nullIf(%s, ''), 'unassigned')) != 'unassigned'", teamCol)

	var baseTable, dateFilter, orgFilter string
	var joins []string
	// count()/countIf() are UInt64 in ClickHouse; the native Go
	// driver's UInt64.ScanRow only recognises *uint64/**uint64 as a scan
	// destination, so a bare *float64 (resolveSankeyCoverage's total/
	// assignedTeam/repoTotal/assignedRepo, unconditionally float64 to match
	// the investment path's sum()/sumIf() Float64 columns) fails the scan
	// outright on a real ClickHouse -- the mock destination-type check
	// accepts anything, which is why this shipped undetected. toFloat64(...)
	// is the same coercion breakdown.go/flowmatrix.go/sankey.go/timeseries.go
	// already apply to every count-shaped measure expression in this
	// package; this query was the one site missing it.
	totalExpr := "toFloat64(count())"
	repoTotalExpr := totalExpr
	assignedTeamCountExpr := fmt.Sprintf("toFloat64(countIf(%s))", assignedTeamExpr)
	assignedRepoCountExpr := fmt.Sprintf("toFloat64(countIf(%s IS NOT NULL))", repoCol)

	// CHAOS-5483: the three split columns. The non-investment path reads
	// the raw investment_metrics_daily table, which has no wure join and
	// therefore no repo_source at all -- so the split is NOT MEASURABLE
	// there, and these are typed NULLs rather than zeros. Zero would read
	// as "no team-fallback effort", which is a different (and false)
	// claim; check 12 of the North Star ("missing is not healthy --
	// unknown/stale/sparse/not-applicable/zero are distinct") is exactly
	// this distinction. The CAST fixes the column type: a bare NULL
	// literal is Nullable(Nothing), which the driver cannot scan into a
	// *float64.
	const notMeasurable = "CAST(NULL AS Nullable(Float64))"
	directRepoExpr := notMeasurable
	teamFallbackRepoExpr := notMeasurable
	fanoutExpr := notMeasurable

	repoFilterColumn := repoCol

	if useInvestment {
		baseTable = fmt.Sprintf("%s AS work_unit_investments", LatestWorkUnitInvestmentsSource())
		// analytics.py:680-683.
		dateFilter = "work_unit_investments.from_ts < {end_date:Date} AND work_unit_investments.to_ts >= {start_date:Date}"
		// analytics.py:814-818 -- qualified only on the investment path,
		// which joins other org_id-carrying tables.
		orgFilter = "work_unit_investments.org_id = {org_id:String}"

		unitTeamSQL := BuildUnitTeamSubquery(UnitTeamSubqueryOptions{
			Source:         fmt.Sprintf("%s AS work_unit_investments", windowedUnitEvidenceSource()),
			InnerTeamAlias: "team",
			OuterTeamAlias: "team_label",
			IncludeTeamID:  true,
		})
		repoEffortSrc := LatestWorkUnitRepoEffortSource()
		joins = append(joins,
			fmt.Sprintf("LEFT JOIN (%s) AS ut ON ut.work_unit_id = work_unit_investments.work_unit_id", unitTeamSQL),
			fmt.Sprintf("LEFT JOIN %s AS wure ON wure.org_id = work_unit_investments.org_id AND wure.work_unit_id = work_unit_investments.work_unit_id", repoEffortSrc),
			// GO-ONLY DIVERGENCE (CHAOS-4773): same unguarded-join defect
			// class as investmentContextFor's repo join (investment.go) --
			// `repos` is ReplacingMergeTree(org_id, id) and this read had no
			// FINAL/dedup, so it silently fanned out (and inflated
			// count()/countIf() coverage denominators) whenever a repo had
			// an unmerged physical version. See investment.go's doc comment
			// on this same fix for the executed repro. Python keeps the
			// defect until CHAOS-2600 retires this read path.
			"LEFT JOIN repos AS r FINAL ON toString(r.id) = toString(wure.repo_id) AND r.org_id = {org_id:String}",
			fmt.Sprintf("LEFT JOIN (SELECT org_id, work_unit_id, count() AS repo_row_count FROM %s GROUP BY org_id, work_unit_id) AS wure_counts ON wure_counts.org_id = work_unit_investments.org_id AND wure_counts.work_unit_id = work_unit_investments.work_unit_id", repoEffortSrc),
		)

		// CHAOS-4241 (analytics.py:713-760): coverage must be weighted the
		// SAME way as the Sankey flow beside it, or the cards visibly
		// disagree with the chart. Two bugs the row-count default carried
		// on this path -- `repo_col IS NOT NULL` is always true because
		// repo_col is the DISPLAY expression (never SQL NULL), and
		// `count()` counts wure-fanned-out JOIN rows so a multi-repo unit
		// counted once per repo -- are avoided by aggregating through the
		// fan-out-safe weighted sum and testing the RAW repo id.
		var repoEffortCol string
		if req.Measure == MeasureChurnLOC {
			repoEffortCol = "if(wure.work_unit_id != '', wure.repo_effort_value, work_unit_investments.effort_value)"
		} else {
			// The 1.0 / wure_counts.repo_row_count arm is CHAOS-4241
			// codex round 2: effort_value <= 0 made every repo-allocation
			// row divide to 0 and the unit vanished from the denominator
			// even though the Sankey's SUM(subcategory_kv.2) still counts
			// it as exactly 1. A window function cannot be nested inside
			// sum() (ClickHouse ILLEGAL_AGGREGATION), hence the joined
			// wure_counts aggregate above.
			repoEffortCol = "if(wure.work_unit_id != '', " +
				"if(work_unit_investments.effort_value > 0, " +
				"wure.repo_effort_value / work_unit_investments.effort_value, " +
				"1.0 / wure_counts.repo_row_count), " +
				"1.0)"
		}
		repoAssignedCol := "if(wure.work_unit_id != '', wure.repo_id, work_unit_investments.repo_id)"

		totalExpr = fmt.Sprintf("sum(%s)", repoEffortCol)
		repoTotalExpr = totalExpr
		assignedTeamCountExpr = fmt.Sprintf("sumIf(%s, %s)", repoEffortCol, assignedTeamExpr)
		assignedRepoCountExpr = fmt.Sprintf("sumIf(%s, %s IS NOT NULL)", repoEffortCol, repoAssignedCol)

		// CHAOS-5483 -- splitting assigned_repo into the two claims it
		// silently conflates.
		//
		// WHY THIS IS NEEDED AT ALL: after the NxM team-ownership fallback
		// (#2394, CHAOS-5460) a work unit with no direct repo evidence but
		// SOME owning team receives 1/N of its effort against EVERY repo
		// that team owns -- ~9 repo rows per unit on the local org. That
		// made the headline repo coverage read 100.0/100.0/99.5 (7d/30d/90d,
		// 2026-09-09 07:51Z) where it had read 53.1/57.5/67.2 the same
		// morning. The number did not get more precise; its MEANING changed.
		// "We know which repo this work touched" and "this belongs to a team
		// that happens to own nine repos" are different claims, and the
		// headline can no longer tell them apart. chris ruled 2026-09-09
		// 10:3xZ ("m1. Sure") that the API and UI must show them separately.
		//
		// THE PARTITION, and why the predicate is the COMPLEMENT of team:%
		// rather than a positive list of the direct sources. The headline
		// numerator above admits a row on `repoAssignedCol IS NOT NULL` --
		// a repo_id test, NOT a repo_source test. A wure row can carry a
		// repo_id with a NULL repo_source (every row written before
		// migration 089 backfilled provenance, and any writer that sets a
		// repo without one). Splitting on a positive list
		// (`repo_source IN (own_edges, ancestor:%, children)`) would drop
		// those rows out of BOTH halves, so direct + fallback would silently
		// undercount the headline -- a partition that does not partition is
		// the "inaccurate coverage claim is worse than an admitted gap"
		// failure root AGENTS.md names. Testing the exact complement of
		// `team:%` over the same rows and the same weights makes
		// direct + fallback == assigned_repo true by construction, which
		// TestResolveSankeyCoverage_SeededRealClickHouse_SplitPartitionsExactly
		// asserts against a real engine. Direct therefore means "not the NxM
		// team fallback": direct evidence, own_edges, ancestor:*, children,
		// and repo-with-unknown-provenance.
		//
		// ifNull(..., 0) is load-bearing, and the reason is MEASURED, not
		// argued -- it is not the type error you would expect. repo_source
		// is Nullable(String) (migration 089), so `repo_source LIKE 'team:%'`
		// is Nullable(UInt8); ClickHouse ACCEPTS that as a sumIf condition
		// without complaint, and then three-valued logic does the damage
		// quietly: for a row with NULL provenance the predicate is NULL, so
		// `NOT (predicate)` is also NULL, and the row is excluded from the
		// fallback half AND the direct half. Dropping this wrapper is worth
		// 1.4 of 5.0 assigned effort on the seeded fixture (direct 4.0 ->
		// 2.6) with no error anywhere -- exactly the silent-undercount shape
		// the split exists to prevent. Executed proof: the red-3-no-ifnull
		// run recorded in this change's TELL.
		// `wure.work_unit_id != ''` is the same LEFT-JOIN-miss test the
		// weight expression above uses -- a unit with no wure row at all
		// falls back to the scalar work_unit_investments.repo_id, which is
		// direct by definition and must never be classified as fallback.
		isTeamFallback := "(wure.work_unit_id != '' AND ifNull(wure.repo_source LIKE 'team:%', 0))"
		assignedRepoPredicate := fmt.Sprintf("%s IS NOT NULL", repoAssignedCol)
		directRepoExpr = fmt.Sprintf("toNullable(sumIf(%s, %s AND NOT %s))", repoEffortCol, assignedRepoPredicate, isTeamFallback)
		teamFallbackRepoExpr = fmt.Sprintf("toNullable(sumIf(%s, %s AND %s))", repoEffortCol, assignedRepoPredicate, isTeamFallback)

		// Fan-out WIDTH, so a saturating fallback is visible as saturation
		// rather than read as precision: team-fallback repo allocations
		// divided by the distinct work units that produced them (~9.06 on
		// the local org at the 07:51Z read -- 5310 rows over 586 units).
		//
		// BOTH halves are uniqExact over a KEY, and the numerator's key is
		// the (work unit, repo) PAIR -- NOT countIf over joined rows, which
		// is what this first shipped as and which was wrong. Executed
		// repro on the real engine, one unit fanned across 3 repos with two
		// subcategories under one theme: no filter -> 3 (correct); with a
		// work-category filter -> 6. At the time, the filter appended
		// `ARRAY JOIN ... subcategory_kv`, which multiplied every joined row
		// by the unit's surviving subcategory count. CHAOS-5498 has since
		// removed that join from this query entirely, so this particular
		// multiplication can no longer occur here -- the distinct-key form is
		// kept anyway, because the merge-state reason below is independent of
		// it. The two SHARE columns were thought immune -- the ARRAY
		// JOIN scales their numerator and denominator alike -- but a raw row
		// COUNT is not, so the width silently reported 2x with a filter
		// applied and 1x without, for identical underlying data. Counting
		// distinct (unit, repo) pairs is invariant under any row-multiplying
		// join, and equals the row count exactly when none is present.
		//
		// uniqExact, not uniq: this is a small per-query figure where an
		// approximate distinct count would make the ratio wobble between
		// identical requests. Zero fallback rows yields 0, not a division by
		// zero -- and 0 here is honest, because "no team-fallback rows" is a
		// measurement, unlike the non-investment path's NULL above.
		//
		// A SECOND reason to count distinct keys rather than rows, found after
		// the first: work_unit_repo_effort is a ReplacingMergeTree, so a raw
		// count() over it is BOTH generation-dependent and merge-state-
		// dependent. Measured on the live org for unchanged logical content:
		// 5310 -> 10626 -> 3612 rows, the last two twenty minutes apart, with
		// a background merge collapsing superseded rows in between (after it,
		// count() == count() FINAL == uniqExact((work_unit_id, repo_id)) ==
		// 3612). No census of that table is reproducible over time without
		// FINAL, an explicit latest-generation filter, or distinct-key
		// counting. That is the reason that still applies after CHAOS-5498:
		// merge state moves under every reader, always, with no query change
		// at all, whereas the ARRAY JOIN is gone from this query.
		fanoutUnits := fmt.Sprintf("uniqExactIf(work_unit_investments.work_unit_id, %s)", isTeamFallback)
		fanoutPairs := fmt.Sprintf("uniqExactIf((work_unit_investments.work_unit_id, wure.repo_id), %s)", isTeamFallback)
		fanoutExpr = fmt.Sprintf("toNullable(if(%[1]s > 0, %[2]s / %[1]s, 0))", fanoutUnits, fanoutPairs)

		// analytics.py:838 -- the repo predicate targets the RAW joined
		// column on the investment path, not the display expression.
		repoFilterColumn = "wure.repo_id"

		// analytics.py:820-827 (CHAOS-2492): resolve developer identity via
		// the au join so who.developers / scope.level=developer coverage is
		// honored instead of forcing coverage=None (CHAOS-2488).
		if needsAuthorJoin(filters) {
			joins = append(joins, fmt.Sprintf("LEFT JOIN %s AS au ON au.work_unit_id = work_unit_investments.work_unit_id", workUnitAuthorsSource()))
		}
		// analytics.py:829-834 appends `ARRAY JOIN CAST(subcategory_distribution_json
		// ...) AS subcategory_kv` here so a work-category filter can read
		// `subcategory_kv.1`. CHAOS-5498 DELIBERATELY DIVERGES: this query does
		// not append it, and filters on unit membership instead (see
		// WorkCategorySelectsUnits in filtertranslation.go).
		//
		// WHY, measured rather than argued: that ARRAY JOIN multiplies each
		// unit's rows by ITS OWN matching-subcategory count, so a work-category
		// filter re-weighted units RELATIVE TO EACH OTHER and every
		// effort-weighted column below inherited it -- including the
		// long-standing teamCoverage and repoCoverage, not just CHAOS-5483's
		// split. Two equal-effort units, one with one matching subcategory and
		// one with two, the second repo-unassigned: repoCoverage read 0.5
		// unfiltered and 0.333 filtered, on identical data. chris ruled the
		// filter must SELECT units, never weight them.
		//
		// The fix is a JOIN removal, not new arithmetic, which is why it needs
		// no per-column changes: with no row multiplication there is nothing
		// for the columns to compensate for. Selection is unchanged -- proven
		// on a real engine over every shape a Map(String, Float64) can hold
		// (empty map, empty key, key without a dot, one match, two matches,
		// mixed): identical membership, differing only in that the ARRAY JOIN
		// emitted the two-match unit twice. A malformed-JSON case cannot exist;
		// the column is a Map despite its _json name.
		//
		// The Python plane still carries the ARRAY JOIN, so the two planes now
		// disagree on FILTERED coverage by design (R60: the Go plane is the
		// source of truth here). Tracked as a follow-up baseline defect for
		// the InvestmentFull operation -- deliberately NOT "registered", since
		// the registry it belongs in ships on CHAOS-5425's branch and does not
		// exist on main yet. Saying "registered" here would have described a
		// file that cannot be opened.
		//
		// UNFILTERED output is byte-identical: no ARRAY JOIN was ever appended
		// without a work-category filter, and the predicate only changes shape
		// inside that same branch. Pinned by
		// TestCompileSankeyCoverage_UnfilteredSQLUnchangedByUnitSelection.
	} else {
		// analytics.py:673-679's own date filter, over the DEDUPED source
		// (this file's own doc comment above) rather than the raw table
		// Python reads -- investmentMetricsDailyDedupSource (timeseries.go)
		// is already an aliased `(SELECT ... argMax(col, computed_at) ...)
		// AS investment_metrics_daily` subquery, so teamCol/repoCol's
		// unqualified "team_id"/"repo_id" references below resolve against
		// its projected columns unchanged; only the FROM source's row
		// multiplicity across duplicate generations changes.
		baseTable = investmentMetricsDailyDedupSource
		dateFilter = "day >= {start_date:Date} AND day <= {end_date:Date}"
		orgFilter = "org_id = {org_id:String}"
	}

	filterClause, err := translateFilters(filters, useInvestment, filterColumns{
		Team:   teamCol,
		Repo:   repoFilterColumn,
		Author: "author_email",
		// CHAOS-5498: this is the ONE caller that never reads
		// subcategory_kv.2 -- it needs the filter only to decide which units
		// are in scope -- so it takes the unit-selecting predicate and skips
		// the row-multiplying ARRAY JOIN above.
		WorkCategorySelectsUnits: useInvestment,
	})
	if err != nil {
		return compiledQuery{}, err
	}

	// CHAOS-5483 appends three columns AFTER the original four. Order is
	// part of the contract with resolveSankeyCoverage's positional Scan --
	// appending keeps the existing four positions untouched.
	sql := fmt.Sprintf(`SELECT
    %s AS total,
    %s AS assigned_team,
    %s AS repo_total,
    %s AS assigned_repo,
    %s AS direct_repo,
    %s AS team_fallback_repo,
    %s AS fanout_repos_per_unit
FROM %s
%s
WHERE %s
  AND %s
  %s
%s`,
		totalExpr, assignedTeamCountExpr, repoTotalExpr, assignedRepoCountExpr,
		directRepoExpr, teamFallbackRepoExpr, fanoutExpr,
		baseTable,
		strings.Join(joins, "\n"),
		dateFilter, orgFilter, filterClause.sql,
		settingsMaxExecutionTime(timeoutSeconds))

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
	}
	bindings = append(bindings, filterClause.bindings...)

	return compiledQuery{sql: sql, bindings: bindings}, nil
}

// resolveSankeyCoverage ports the execution half of
// analytics.py:866-907. Returns nil -- never an error -- on ANY failure:
// coverage degrades to an honest empty state and must never turn a
// working sankey into a 500, exactly as Python's try/except does. Every
// failure is reported through recordInvestmentCoverageFailure first.
func resolveSankeyCoverage(ctx context.Context, client QueryClient, orgID string, req SankeyRequest, timeoutSeconds int, useInvestment bool, filters *model.FilterInput) *model.SankeyCoverage {
	query, err := compileSankeyCoverage(req, orgID, timeoutSeconds, useInvestment, filters)
	if err != nil {
		// Python raises inside the try (the f-string construction and
		// translate_filters both run there, analytics.py:836-865), so a
		// construction failure lands in the same except branch.
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageCompile, "", err)
		return nil
	}

	// analytics.py:867-869 -- the ONE sankey-adjacent
	// record_stale_investment_membership_scope call site in Python lives
	// HERE, inside the coverage try, gated on use_investment. resolve.go's
	// resolveSankey comment records that this call was deliberately not
	// ported at the sankey level and that "if/when coverage is ported, its
	// telemetry call must be added THERE, not here" -- this is there.
	if useInvestment {
		RecordStaleInvestmentMembershipScope(ctx, client, orgID, timeoutSeconds)
		// CHAOS-4759 transition guard: bounded-cooldown check, see
		// RecordArgMaxNullTransitionGuard's doc comment.
		RecordArgMaxNullTransitionGuard(ctx, client, orgID, timeoutSeconds)
		// CHAOS-4773 telemetry: compileSankeyCoverage's investment branch
		// always compiles the same class of repos join investment.go's
		// investmentContextFor does (now FINAL-deduped) -- see
		// investmentrepojointelemetry.go.
		RecordInvestmentRepoJoinDedupCollisions(ctx, client, orgID)
	}

	// CHAOS-6121: mint the query id ourselves and BIND it to the request
	// via clickhousedriver.WithQueryID, rather than reading one back --
	// clickhouse-go sends whatever ID is on the context verbatim
	// (conn_send_query.go) and never surfaces a server-assigned one to the
	// caller, so an unset ID here would leave every failure attribute
	// below with nothing real to report. clickhousedriver.Context merges
	// onto any existing QueryOptions on ctx (see its doc comment), so this
	// only adds the id -- it does not disturb settings/parameters
	// dev-health-go's own Client.Query layers on afterwards.
	queryID := uuid.NewString()
	queryCtx := clickhousedriver.Context(ctx, clickhousedriver.WithQueryID(queryID))

	rows, err := client.Query(queryCtx, query.sql, query.bindings)
	if err != nil {
		// CHAOS-6121 (executed live against a real ClickHouse: an
		// unencodable binding value produces stage=query
		// query_id=<minted> with ZERO matching system.query_log rows):
		// dev-health-go's Client.Query validates the statement shape and
		// translates bindings to native-protocol
		// PARAMETERS entirely LOCALLY, both before it ever calls
		// c.connection.Query (client.go:111-133) -- so
		// ErrUnsafeStatement/ErrInvalidBinding mean the request was never
		// dispatched, and the id we bound to it via WithQueryID was never
		// sent either. Reporting queryID for THIS class sends on-call
		// searching system.query_log for a row that can never exist,
		// which is worse than reporting no id at all (AGENTS.md: "an
		// inaccurate coverage claim is worse than an admitted gap").
		reportedQueryID := queryID
		if isLocalValidationFailure(err) {
			reportedQueryID = ""
		}
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageQuery, reportedQueryID, fmt.Errorf("query: %w", err))
		return nil
	}
	defer rows.Close()

	if !rows.Next() {
		// Python: `if c_rows:` -- zero rows leaves coverage as None with
		// no error and no telemetry, because nothing failed.
		if rowsErr := rows.Err(); rowsErr != nil {
			recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageRows, queryID, fmt.Errorf("rows: %w", rowsErr))
		}
		return nil
	}

	var total, assignedTeam, repoTotal, assignedRepo float64
	// CHAOS-5483: *float64, not float64. The three split columns are
	// Nullable(Float64) on BOTH paths (the non-investment path cannot
	// measure them at all), and clickhouse-go's Float64.ScanRow only
	// recognises **float64 as a nullable-aware destination -- a bare
	// *float64 never observes the NULL and silently leaves 0 behind. Same
	// driver mechanic and the same reason as breakdownRow.Value
	// (CHAOS-4650) and TimeseriesBucket.Value (CHAOS-4657); see
	// breakdown.go's doc comment for the branch-by-branch detail.
	var directRepo, teamFallbackRepo, fanoutReposPerUnit *float64
	if scanErr := rows.Scan(&total, &assignedTeam, &repoTotal, &assignedRepo, &directRepo, &teamFallbackRepo, &fanoutReposPerUnit); scanErr != nil {
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageScan, queryID, fmt.Errorf("scan: %w", scanErr))
		return nil
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageRows, queryID, fmt.Errorf("rows: %w", rowsErr))
		return nil
	}

	// analytics.py:877-882 -- a zero denominator yields 0, NOT null. The
	// SDL types both fields non-nullable (SankeyCoverage.teamCoverage:
	// Float!), so 0 is the only representable answer here anyway.
	coverage := &model.SankeyCoverage{}
	if total > 0 {
		coverage.TeamCoverage = assignedTeam / total
	}
	if repoTotal > 0 {
		coverage.RepoCoverage = assignedRepo / repoTotal
	}

	// CHAOS-5483. Three deliberate asymmetries with the two fields above:
	//
	//  1. These stay NIL when the query could not measure them (the
	//     non-investment path), where the originals are 0. The originals
	//     have no choice -- the SDL types them Float! -- but these are
	//     nullable precisely so "not measurable here" survives to the UI
	//     instead of arriving as a confident 0% team-fallback.
	//  2. The shares are guarded on repoTotal, the SAME denominator
	//     RepoCoverage uses. Using a different denominator would break the
	//     one property this split exists to provide: direct + fallback
	//     reads back as exactly the headline the cards already show.
	//  3. RepoFanoutReposPerUnit is passed through unscaled. It is a WIDTH
	//     (rows per unit, ~9 on the local org), not a share -- dividing it
	//     by anything would make it a ratio of a ratio and it would stop
	//     being the number that shows a saturating fallback for what it is.
	if repoTotal > 0 {
		if directRepo != nil {
			share := *directRepo / repoTotal
			coverage.DirectRepoCoverage = &share
		}
		if teamFallbackRepo != nil {
			share := *teamFallbackRepo / repoTotal
			coverage.TeamFallbackRepoCoverage = &share
		}
	}
	coverage.RepoFanoutReposPerUnit = fanoutReposPerUnit
	return coverage
}
