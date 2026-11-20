package coveragebaselines

import (
	"context"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// scopeStatement reads the baseline of a whole scope (CHAOS-8541) in one
// statement of three levels:
//
//  1. newestPerRepoDay: the same coverage read as the per-repository baseline.
//  2. One row per day: the covered and total lines of the counted repo-days of
//     the day, the number of those repo-days, and the branch value of the day
//     (the mean over the repositories that hold a branch value that day; NULL
//     when none does).
//  3. Line coverage: one ratio, covered lines over total lines of every counted
//     repo-day of the scope in the window (the pooled rule of the coverage
//     trend), and the number of days with at least one counted repo-day. Branch
//     coverage: the mean of the day values, and the number of days that hold
//     one.
//
// Neither is a mean of the repository baselines: a repository with few days
// would then weigh as much as one with thirty.
//
// The level-2 aliases differ from the column names of level 1 on purpose: an
// aggregate whose alias is the name of its own argument is an aggregate inside
// an aggregate for ClickHouse (code 184).
func scopeStatement(scopeFilter string) string {
	return `
        SELECT
            100 * sum(d.day_covered) / nullIf(sum(d.day_total), 0) AS line_value,
            countIf(d.day_counted > 0) AS line_days,
            avg(d.day_branch_pct) AS branch_mean,
            countIf(d.day_branch_pct IS NOT NULL) AS branch_days
        FROM (
            SELECT
                b.day AS scope_day,
                sumIf(b.day_lines_covered, b.line_counted) AS day_covered,
                sumIf(b.day_lines_total, b.line_counted) AS day_total,
                countIf(b.line_counted) AS day_counted,
                avg(b.branch_pct) AS day_branch_pct
            FROM (` + newestPerRepoDay(scopeFilter) + `
            ) AS b
            GROUP BY b.day
        ) AS d`
}

// ResolveScope answers coverageScopeBaseline. orgID must be the AUTHORIZED org.
// The window, the seven-day minimum and the scope arguments are the ones of
// Resolve. A scope with no stored row in the window gives no baseline over 0
// days, not an error: nothing was measured.
func ResolveScope(ctx context.Context, client QueryClient, orgID string, endDate graphqldate.Date, scope Scope) (*model.ScopeCoverageBaseline, error) {
	if client == nil {
		return nil, errors.New("coveragebaselines: clickhouse client is required")
	}
	scopeFilter, bindings := windowAndScope(orgID, endDate, scope)

	rows, err := client.Query(ctx, scopeStatement(scopeFilter), bindings)
	if err != nil {
		return nil, fmt.Errorf("coveragebaselines: scope query: %w", err)
	}
	defer rows.Close()
	out := &model.ScopeCoverageBaseline{}
	read := 0
	for rows.Next() {
		var (
			lineValue, branchMean *float64
			lineDays, branchDays  uint64
		)
		if scanErr := rows.Scan(&lineValue, &lineDays, &branchMean, &branchDays); scanErr != nil {
			return nil, fmt.Errorf("coveragebaselines: scope scan: %w", scanErr)
		}
		read++
		out = &model.ScopeCoverageBaseline{
			LineBaselinePct:   baseline(lineValue, int(lineDays)),
			LineDays:          int(lineDays),
			BranchBaselinePct: baseline(branchMean, int(branchDays)),
			BranchDays:        int(branchDays),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("coveragebaselines: scope rows: %w", err)
	}
	if read > 1 {
		return nil, fmt.Errorf("coveragebaselines: scope: %d rows, want one", read)
	}
	return out, nil
}
