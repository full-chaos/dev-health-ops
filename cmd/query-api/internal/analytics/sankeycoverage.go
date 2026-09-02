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
// WHAT THIS PORT DELIBERATELY DOES NOT "FIX": Python's coverage query
// reads the RAW `investment_metrics_daily` table on the non-investment
// path, NOT the dedup source timeseries.go's
// nonInvestmentSourceAndDateFilter uses (analytics.py:673-677:
// `table = ... if request.use_investment else "investment_metrics_daily"`).
// Ported as written. Likewise the ARRAY JOIN for a work-category filter
// is appended AFTER the LEFT JOINs, exactly where Python appends it
// (analytics.py:829-834).

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
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

func defaultRecordInvestmentCoverageFailure(ctx context.Context, orgID string, measure Measure, useInvestment bool, stage coverageFailureStage, err error) {
	investmentCoverageQueryFailedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("resolver", "investment_coverage"),
		attribute.String("reason", string(stage)),
	))
	trace.SpanFromContext(ctx).AddEvent("investment_coverage.query_failed", trace.WithAttributes(
		attribute.String("resolver", "investment_coverage"),
		attribute.String("stage", string(stage)),
		attribute.String("error", err.Error()),
		attribute.String("error.cause", rootCause(err).Error()),
	))
	// Mirrors Python's structured logger.error("investment_coverage.query_failed", extra={...})
	// (analytics.py:894-906) field for field. org_id is an internal
	// tenant UUID, already logged by this package elsewhere.
	slog.ErrorContext(ctx, "investment_coverage.query_failed",
		"resolver", "investment_coverage",
		"org_id", orgID,
		"measure", string(measure),
		"use_investment", useInvestment,
		"stage", string(stage),
		"error", err.Error(),
	)
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
	totalExpr := "count()"
	repoTotalExpr := totalExpr
	assignedTeamCountExpr := fmt.Sprintf("countIf(%s)", assignedTeamExpr)
	assignedRepoCountExpr := fmt.Sprintf("countIf(%s IS NOT NULL)", repoCol)

	repoFilterColumn := repoCol

	if useInvestment {
		baseTable = fmt.Sprintf("%s AS work_unit_investments", latestWorkUnitInvestmentsSource())
		// analytics.py:680-683.
		dateFilter = "work_unit_investments.from_ts < {end_date:Date} AND work_unit_investments.to_ts >= {start_date:Date}"
		// analytics.py:814-818 -- qualified only on the investment path,
		// which joins other org_id-carrying tables.
		orgFilter = "work_unit_investments.org_id = {org_id:String}"

		unitTeamSQL := buildUnitTeamSubquery(unitTeamSubqueryOptions{
			Source:         fmt.Sprintf("%s AS work_unit_investments", latestWorkUnitInvestmentsSource()),
			InnerTeamAlias: "team",
			OuterTeamAlias: "team_label",
			IncludeTeamID:  true,
		})
		repoEffortSrc := latestWorkUnitRepoEffortSource()
		joins = append(joins,
			fmt.Sprintf("LEFT JOIN (%s) AS ut ON ut.work_unit_id = work_unit_investments.work_unit_id", unitTeamSQL),
			fmt.Sprintf("LEFT JOIN %s AS wure ON wure.org_id = work_unit_investments.org_id AND wure.work_unit_id = work_unit_investments.work_unit_id", repoEffortSrc),
			"LEFT JOIN repos AS r ON toString(r.id) = toString(wure.repo_id)",
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

		// analytics.py:838 -- the repo predicate targets the RAW joined
		// column on the investment path, not the display expression.
		repoFilterColumn = "wure.repo_id"

		// analytics.py:820-827 (CHAOS-2492): resolve developer identity via
		// the au join so who.developers / scope.level=developer coverage is
		// honored instead of forcing coverage=None (CHAOS-2488).
		if needsAuthorJoin(filters) {
			joins = append(joins, fmt.Sprintf("LEFT JOIN %s AS au ON au.work_unit_id = work_unit_investments.work_unit_id", workUnitAuthorsSource()))
		}
		// analytics.py:829-834 -- appended after the LEFT JOINs, as Python does.
		if hasWorkCategoryFilter(filters) {
			joins = append(joins, "ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv")
		}
	} else {
		// analytics.py:673-679 -- the RAW daily table and its own date filter.
		baseTable = "investment_metrics_daily"
		dateFilter = "day >= {start_date:Date} AND day <= {end_date:Date}"
		orgFilter = "org_id = {org_id:String}"
	}

	filterClause, err := translateFilters(filters, useInvestment, filterColumns{
		Team:   teamCol,
		Repo:   repoFilterColumn,
		Author: "author_email",
	})
	if err != nil {
		return compiledQuery{}, err
	}

	sql := fmt.Sprintf(`SELECT
    %s AS total,
    %s AS assigned_team,
    %s AS repo_total,
    %s AS assigned_repo
FROM %s
%s
WHERE %s
  AND %s
  %s
%s`,
		totalExpr, assignedTeamCountExpr, repoTotalExpr, assignedRepoCountExpr,
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

// hasWorkCategoryFilter ports _has_work_category_filter
// (analytics.py:203-208). Note it tests why.work_category ONLY -- NOT
// why.issue_type, which hasActiveFilters does test. Copying
// hasActiveFilters' condition here would add the subcategory_kv ARRAY
// JOIN for an issue-type-only filter, which Python never does.
func hasWorkCategoryFilter(filters *model.FilterInput) bool {
	return filters != nil && filters.Why != nil && len(filters.Why.WorkCategory) > 0
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
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageCompile, err)
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
	}

	rows, err := client.Query(ctx, query.sql, query.bindings)
	if err != nil {
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageQuery, fmt.Errorf("query: %w", err))
		return nil
	}
	defer rows.Close()

	if !rows.Next() {
		// Python: `if c_rows:` -- zero rows leaves coverage as None with
		// no error and no telemetry, because nothing failed.
		if rowsErr := rows.Err(); rowsErr != nil {
			recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageRows, fmt.Errorf("rows: %w", rowsErr))
		}
		return nil
	}

	var total, assignedTeam, repoTotal, assignedRepo float64
	if scanErr := rows.Scan(&total, &assignedTeam, &repoTotal, &assignedRepo); scanErr != nil {
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageScan, fmt.Errorf("scan: %w", scanErr))
		return nil
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		recordInvestmentCoverageFailure(ctx, orgID, req.Measure, useInvestment, coverageStageRows, fmt.Errorf("rows: %w", rowsErr))
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
	return coverage
}
