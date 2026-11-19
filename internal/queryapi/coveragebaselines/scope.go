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
//  2. The scope's value of one day. Line: the mean over the repositories that
//     hold a value that day (a day on which none does is NULL). Branch: the
//     day's summed branches covered and branches total over its counted
//     repository-days (branchCounted).
//  3. The baseline rule of this package over those days. Line: the mean of the
//     day values and the number of days that hold one. Branch: the sums of
//     every day, so one pooled ratio over all counted repository-days of the
//     window, and the number of days that hold a counted repository-day.
//
// It is NOT the mean of the repository baselines: a repository with few days
// would then weigh as much as one with thirty. The branch value is NOT the
// mean of the day values either: a day weighs by its branches.
//
// The level-2 aliases differ from the column names of level 1 on purpose: an
// aggregate whose alias is the name of its own argument is an aggregate inside
// an aggregate for ClickHouse (code 184).
func scopeStatement(scopeFilter string) string {
	return `
        SELECT
            avg(d.day_line_pct) AS line_mean,
            countIf(d.day_line_pct IS NOT NULL) AS line_days,
            sum(d.day_branch_num) / nullIf(sum(d.day_branch_den), 0) AS branch_mean,
            countIf(d.day_branch_den > 0) AS branch_days
        FROM (
            SELECT
                b.day AS scope_day,
                avg(b.line_pct) AS day_line_pct,
                sumIf(100 * b.branches_covered, ` + branchCounted + `) AS day_branch_num,
                sumIf(b.branches_total, ` + branchCounted + `) AS day_branch_den
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
			lineMean, branchMean *float64
			lineDays, branchDays uint64
		)
		if scanErr := rows.Scan(&lineMean, &lineDays, &branchMean, &branchDays); scanErr != nil {
			return nil, fmt.Errorf("coveragebaselines: scope scan: %w", scanErr)
		}
		read++
		out = &model.ScopeCoverageBaseline{
			LineBaselinePct:   baseline(lineMean, int(lineDays)),
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
