// Package testopswindow answers testopsTestWindow (CHAOS-13773): how much of CI
// reports tests, and the pass, failure and flake rates of the TestOps Tests tab
// over one window.
//
// It is Go-only: no Python resolver exists for it.
//
// Two reads, both computed in ClickHouse at read time:
//
//   - Coverage: ci_pipeline_runs (every provider, every workflow) against
//     test_suite_results. The denominator is the COMPLETED runs of the window
//     (success, failure, error or timeout: the same status classes
//     testopsjobfailures reads), because a cancelled, skipped, queued or running
//     run can never bring a report. A run is "with tests" when it has at least
//     one stored suite with total_count > 0: a suite row with no cases reports
//     no tests, and missing is not healthy. A failed run with no report stays in
//     the denominator on purpose: that is the gap this field makes visible.
//   - Rates: the daily rollup testops_test_metrics_daily, summed over the
//     window's repository days. Rates are weighted (summed counts), never a mean
//     of the stored per-repository-day rates; those keep their own formula.
//     failed_count already holds failed + error cases (testops.TestAccumulator).
//
// Team scope is repository OWNERSHIP (teamscope.RepoCondition, from
// team_repo_ownership), as in testopsjobfailures; the team_id column of the
// stored rows is not read.
package testopswindow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/testopsjobfailures"
)

// MaxWindowDays is the most days untilDate may be after sinceDate, as for
// testopsJobFailures.
const MaxWindowDays = 90

// CompletedStatuses are the stored status spellings of a run that reached a
// result: success and failure classes of every provider. Cancelled, skipped,
// queued and running runs are not completed.
func CompletedStatuses() []string {
	out := make([]string, 0, len(testopsjobfailures.SuccessStatuses)+len(testopsjobfailures.FailureStatuses))
	out = append(out, testopsjobfailures.SuccessStatuses...)
	return append(out, testopsjobfailures.FailureStatuses...)
}

// QueryClient is the read-only ClickHouse boundary of this package.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// Scope narrows both reads to a set of repositories. Both fields are optional;
// when both are given, both apply.
type Scope struct {
	// RepoIDs are repository ids or full names.
	RepoIDs []string
	// TeamIDs are team ids: the repositories these teams OWN.
	TeamIDs []string
	// AsOf is the instant the ownership is read at; zero means now (UTC).
	AsOf time.Time
}

func checkWindow(since, until graphqldate.Date) error {
	start, end := since.Time(), until.Time()
	if end.Before(start) {
		return errors.New("untilDate is before sinceDate")
	}
	if days := int(end.Sub(start).Hours() / 24); days > MaxWindowDays {
		return fmt.Errorf("untilDate is %d days after sinceDate; at most %d days are served", days, MaxWindowDays)
	}
	return nil
}

// scopeFilter renders the AND clauses of a scope on the column repo_id, and
// their bindings.
func scopeFilter(orgID string, scope Scope) (string, []clickhouse.Binding) {
	filter := ""
	var bindings []clickhouse.Binding
	if len(scope.RepoIDs) > 0 {
		filter = `
          AND repo_id IN (
              SELECT id FROM repos
              WHERE org_id = {org_id:String}
                AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
          )`
		bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: scope.RepoIDs})
	}
	asOf := scope.AsOf
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	if teamCondition, teamBindings := teamscope.RepoCondition(orgID, "toString(repo_id)", scope.TeamIDs, asOf); teamCondition != "" {
		filter += "\n          AND " + teamCondition
		bindings = append(bindings, teamBindings...)
	}
	return filter, bindings
}

func coverageStatement(filter string) string {
	return `
SELECT
    count() AS completed_runs,
    countIf((repo_id, run_id) IN (
        SELECT repo_id, run_id FROM test_suite_results FINAL
        WHERE org_id = {org_id:String} AND total_count > 0
    )) AS runs_with_tests
FROM ci_pipeline_runs FINAL
WHERE org_id = {org_id:String}
          AND toDate(started_at) >= {since_date:Date}
          AND toDate(started_at) <= {until_date:Date}
          AND lowerUTF8(trimBoth(ifNull(status, ''))) IN {completed_statuses:Array(String)}` + filter
}

// casesStatement reads the newest stored row of each repository day (the daily
// job appends a row per recompute; the other readers dedupe the same way).
func casesStatement(filter string) string {
	return `
SELECT
    sum(passed_count) AS passed,
    sum(failed_count) AS failed,
    sum(skipped_count) AS skipped,
    sum(quarantined_count) AS quarantined,
    sum(total_cases) AS total,
    sum(flake_cases) AS flake_sum,
    sum(distinct_cases) AS distinct_sum,
    countIf(flake_cases IS NULL OR distinct_cases IS NULL) AS rows_without_counts
FROM (
    SELECT *
    FROM testops_test_metrics_daily
    WHERE org_id = {org_id:String}
          AND day >= {since_date:Date}
          AND day <= {until_date:Date}` + filter + `
    ORDER BY computed_at DESC
    LIMIT 1 BY org_id, repo_id, day
)`
}

func percent(numerator, denominator uint64) *float64 {
	if denominator == 0 {
		return nil
	}
	value := float64(numerator) / float64(denominator) * 100
	return &value
}

// Resolve answers testopsTestWindow. orgID must be the AUTHORIZED org.
func Resolve(ctx context.Context, client QueryClient, orgID string, since, until graphqldate.Date, scope Scope) (*model.TestOpsTestWindowResult, error) {
	if client == nil {
		return nil, errors.New("testopswindow: clickhouse client is required")
	}
	if err := checkWindow(since, until); err != nil {
		return nil, err
	}
	filter, scopeBindings := scopeFilter(orgID, scope)
	bindings := append([]clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_date", Value: since.String()},
		{Name: "until_date", Value: until.String()},
	}, scopeBindings...)

	completed, withTests, err := readCoverage(ctx, client, filter, append(append([]clickhouse.Binding{}, bindings...),
		clickhouse.Binding{Name: "completed_statuses", Value: CompletedStatuses()}))
	if err != nil {
		return nil, err
	}
	counts, err := readCases(ctx, client, filter, bindings)
	if err != nil {
		return nil, err
	}

	result := &model.TestOpsTestWindowResult{
		SinceDate: since,
		UntilDate: until,
		Coverage: &model.TestOpsTestCoverage{
			CompletedRuns: int(completed), RunsWithTests: int(withTests), SharePct: percent(withTests, completed),
		},
		Cases: &model.TestOpsTestCaseCounts{
			Passed: int(counts.passed), Failed: int(counts.failed), Skipped: int(counts.skipped),
			Quarantined: int(counts.quarantined), Total: int(counts.total),
		},
		PassRatePct:    percent(counts.passed, counts.passed+counts.failed),
		FailureRatePct: percent(counts.failed, counts.passed+counts.failed),
	}
	if counts.rowsWithoutCounts == 0 && counts.flakeCases != nil && counts.distinctCases != nil {
		result.FlakeRatePct = percent(*counts.flakeCases, *counts.distinctCases)
	}

	attrs := []any{
		"since", logSafe(since.String()), "until", logSafe(until.String()),
		"completed_runs", completed, "runs_with_tests", withTests, "total_cases", counts.total,
		"repo_scoped", len(scope.RepoIDs) > 0, "team_scoped", len(scope.TeamIDs) > 0,
	}
	if parsedOrg, parseErr := uuid.Parse(orgID); parseErr == nil {
		attrs = append(attrs, "organization_id", logSafe(parsedOrg.String()))
	}
	slog.DebugContext(ctx, "testopswindow: resolved", attrs...)
	return result, nil
}

func logSafe(value string) string {
	return strings.NewReplacer("\n", "", "\r", "").Replace(value)
}

func readCoverage(ctx context.Context, client QueryClient, filter string, bindings []clickhouse.Binding) (completed, withTests uint64, err error) {
	rows, err := client.Query(ctx, coverageStatement(filter), bindings)
	if err != nil {
		return 0, 0, fmt.Errorf("testopswindow: coverage query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, 0, fmt.Errorf("testopswindow: coverage rows: %w", err)
		}
		return 0, 0, errors.New("testopswindow: coverage query returned no row")
	}
	if err := rows.Scan(&completed, &withTests); err != nil {
		return 0, 0, fmt.Errorf("testopswindow: coverage scan: %w", err)
	}
	return completed, withTests, rows.Err()
}

type caseSums struct {
	passed, failed, skipped, quarantined, total uint64
	flakeCases, distinctCases                   *uint64
	rowsWithoutCounts                           uint64
}

func readCases(ctx context.Context, client QueryClient, filter string, bindings []clickhouse.Binding) (caseSums, error) {
	var sums caseSums
	rows, err := client.Query(ctx, casesStatement(filter), bindings)
	if err != nil {
		return sums, fmt.Errorf("testopswindow: cases query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return sums, fmt.Errorf("testopswindow: cases rows: %w", err)
		}
		return sums, errors.New("testopswindow: cases query returned no row")
	}
	if err := rows.Scan(&sums.passed, &sums.failed, &sums.skipped, &sums.quarantined, &sums.total,
		&sums.flakeCases, &sums.distinctCases, &sums.rowsWithoutCounts); err != nil {
		return sums, fmt.Errorf("testopswindow: cases scan: %w", err)
	}
	return sums, rows.Err()
}
