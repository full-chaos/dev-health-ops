// Package testopsjobfailures answers testopsJobFailures (CHAOS-8513): the CI
// job names that failed in a window, each with its runs, failed runs and
// failure rate, grouped by workflow and job name.
//
// It is Go-only: no Python resolver exists for it.
//
// Computed in ClickHouse at read time from the raw rows both providers write
// (GitHub Actions and GitLab CI, through the one TestOps sink): ci_job_runs for
// the job name and status, ci_pipeline_runs for the workflow name and provider.
// No rollup table exists for job names. ci_job_runs is sorted by (repo_id,
// run_id, job_id) with no day in its key, so a window read scans the org's job
// rows: the window is capped at MaxWindowDays and the list at MaxLimit. A daily
// rollup in the TestOps metric family is the follow-up if the read is too slow.
//
// Team scope is repository OWNERSHIP (teamscope.RepoCondition, from
// team_repo_ownership). The team_id column of the pipeline rows is not read.
package testopsjobfailures

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

const (
	// MaxWindowDays is the longest window served, first and last day included.
	MaxWindowDays = 90
	// MaxLimit is the most groups served in one answer.
	MaxLimit = 100
)

// The stored status spellings of each class. Providers write their own words
// (GitHub "failure" and "timed_out", GitLab "failed" and "canceled"); these are
// the classes the pipeline rollup gives them (testops.NormalizeStatus), and a
// test pins the lists to that function. A status of no class (skipped, queued,
// running, manual, ...) is not a run: the job did not reach a result.
var (
	SuccessStatuses   = []string{"success", "succeeded", "passed"}
	FailureStatuses   = []string{"failure", "failed", "error", "errors", "timeout", "timed_out"}
	CancelledStatuses = []string{"cancelled", "canceled", "cancel"}
)

// terminalStatuses are the statuses of a job run that reached a result.
func terminalStatuses() []string {
	out := make([]string, 0, len(SuccessStatuses)+len(FailureStatuses)+len(CancelledStatuses))
	out = append(out, SuccessStatuses...)
	out = append(out, FailureStatuses...)
	return append(out, CancelledStatuses...)
}

// QueryClient is the read-only ClickHouse boundary of this package.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// Scope narrows the job runs to a set of repositories. Both fields are
// optional; when both are given, both apply.
type Scope struct {
	// RepoIDs are repository ids or full names.
	RepoIDs []string
	// TeamIDs are team ids: the repositories these teams OWN.
	TeamIDs []string
	// AsOf is the instant the ownership is read at; zero means now (UTC).
	AsOf time.Time
}

func clampLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > MaxLimit {
		return MaxLimit
	}
	return limit
}

// checkWindow refuses a window that ends before it starts or is longer than
// MaxWindowDays. A window is never cut silently: a cut window is the answer to
// another question.
func checkWindow(since, until graphqldate.Date) error {
	start, end := since.Time(), until.Time()
	if end.Before(start) {
		return errors.New("untilDate is before sinceDate")
	}
	if days := int(end.Sub(start).Hours()/24) + 1; days > MaxWindowDays {
		return fmt.Errorf("the window is %d days; at most %d days are served", days, MaxWindowDays)
	}
	return nil
}

// failureRate is failedRuns / runs, a share from 0 to 1; nil when there is no
// run to divide by.
func failureRate(failedRuns, runs int) *float64 {
	if runs <= 0 {
		return nil
	}
	rate := float64(failedRuns) / float64(runs)
	return &rate
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// groupsStatement is the one text both the list and its count are built from:
// one row per (workflow, job name, provider) with at least one failed run.
//
// The job rows are read with FINAL (a re-sync writes a newer version of a job
// run; it counts once) and filtered to the org, the window (the day the job
// started, UTC) and the scope before the join. A job run that never started has
// no day and is not counted. The pipeline row gives the workflow name and the
// provider; a job run with no pipeline row keeps both as NULL.
func groupsStatement(scopeFilter string) string {
	return `SELECT
                nullIf(trimBoth(ifNull(p.pipeline_name, '')), '') AS workflow_name,
                j.job_name AS job_name,
                nullIf(toString(p.provider), '') AS provider,
                count() AS runs,
                countIf(j.status IN {failure_statuses:Array(String)}) AS failed_runs
            FROM (
                SELECT repo_id, run_id, job_name, lowerUTF8(trimBoth(ifNull(status, ''))) AS status
                FROM ci_job_runs FINAL
                WHERE org_id = {org_id:String}
                  AND toDate(started_at) >= {since_date:Date}
                  AND toDate(started_at) <= {until_date:Date}` + scopeFilter + `
            ) AS j
            LEFT JOIN (
                SELECT repo_id, run_id, pipeline_name, provider
                FROM ci_pipeline_runs FINAL
                WHERE org_id = {org_id:String}` + scopeFilter + `
            ) AS p ON p.repo_id = j.repo_id AND p.run_id = j.run_id
            WHERE j.status IN {terminal_statuses:Array(String)}
            GROUP BY workflow_name, job_name, provider
            HAVING failed_runs > 0`
}

func listStatement(groups string) string {
	return `
        SELECT workflow_name, job_name, provider, runs, failed_runs
        FROM (
            ` + groups + `
        )
        ORDER BY failed_runs DESC, job_name, ifNull(workflow_name, ''), ifNull(provider, '')
        LIMIT {limit:UInt64}`
}

func countStatement(groups string) string {
	return "SELECT count() FROM (\n            " + groups + "\n        )"
}

// Resolve answers testopsJobFailures. orgID must be the AUTHORIZED org.
func Resolve(ctx context.Context, client QueryClient, orgID string, since, until graphqldate.Date, scope Scope, limit int) (*model.TestOpsJobFailuresResult, error) {
	if client == nil {
		return nil, errors.New("testopsjobfailures: clickhouse client is required")
	}
	if err := checkWindow(since, until); err != nil {
		return nil, err
	}

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_date", Value: since.String()},
		{Name: "until_date", Value: until.String()},
		{Name: "failure_statuses", Value: FailureStatuses},
		{Name: "terminal_statuses", Value: terminalStatuses()},
	}
	scopeFilter := ""
	if len(scope.RepoIDs) > 0 {
		scopeFilter = `
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
		scopeFilter += "\n                  AND " + teamCondition
		bindings = append(bindings, teamBindings...)
	}
	groups := groupsStatement(scopeFilter)

	rows, err := client.Query(ctx, listStatement(groups), append(append([]clickhouse.Binding{}, bindings...), clickhouse.Binding{Name: "limit", Value: clampLimit(limit)}))
	if err != nil {
		return nil, fmt.Errorf("testopsjobfailures: query: %w", err)
	}
	defer rows.Close()
	out := []model.TestOpsJobFailureGroup{}
	for rows.Next() {
		var (
			workflowName, provider *string
			jobName                string
			runs, failedRuns       uint64
		)
		if scanErr := rows.Scan(&workflowName, &jobName, &provider, &runs, &failedRuns); scanErr != nil {
			return nil, fmt.Errorf("testopsjobfailures: scan: %w", scanErr)
		}
		group := model.TestOpsJobFailureGroup{
			JobName: jobName, Runs: int(runs), FailedRuns: int(failedRuns),
			FailureRate: failureRate(int(failedRuns), int(runs)),
		}
		if workflowName != nil {
			group.WorkflowName = optional(*workflowName)
		}
		if provider != nil {
			group.Provider = optional(*provider)
		}
		out = append(out, group)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("testopsjobfailures: rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("testopsjobfailures: close rows: %w", err)
	}

	total, err := countGroups(ctx, client, groups, bindings)
	if err != nil {
		return nil, err
	}
	// The count and the list are two reads; never report fewer groups than were served.
	if total < len(out) {
		total = len(out)
	}
	return &model.TestOpsJobFailuresResult{Groups: out, TotalCount: total, Truncated: total > len(out)}, nil
}

func countGroups(ctx context.Context, client QueryClient, groups string, bindings []clickhouse.Binding) (int, error) {
	rows, err := client.Query(ctx, countStatement(groups), bindings)
	if err != nil {
		return 0, fmt.Errorf("testopsjobfailures: count query: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("testopsjobfailures: count rows: %w", err)
		}
		return 0, errors.New("testopsjobfailures: count query returned no row")
	}
	var total uint64
	if err := rows.Scan(&total); err != nil {
		return 0, fmt.Errorf("testopsjobfailures: count scan: %w", err)
	}
	return int(total), rows.Err()
}
