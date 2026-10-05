// Package coveragebaselines answers coverageBaselines (CHAOS-8111): each
// repository's coverage baseline, which is its own mean coverage over the 30
// days before a given day (chris, 2026-10-03: "the running 30 day average"). It
// is not a set target.
//
// It is Go-only: no Python resolver exists for it.
//
// Computed in ClickHouse in one grouped statement over
// testops_coverage_metrics_daily (one row per org, repository and day; a plain
// MergeTree, so the newest version of a day is chosen here by computed_at).
//
// A baseline needs MinDays days with a value inside the 30. With fewer, the
// mean of one or two days is the current value under another name, and a
// baseline that equals the value it is compared with says nothing. Then the
// baseline is null, never 0 and never the current value. The number of days
// used is served beside it.
//
// Team scope is repository OWNERSHIP (teamscope.RepoCondition).
package coveragebaselines

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
	// WindowDays is the length of the baseline window: the days before the end
	// day, the end day itself not included.
	WindowDays = 30
	// MinDays is the least number of days with a value a baseline needs: one
	// week of daily rows.
	MinDays = 7
)

// QueryClient is the read-only ClickHouse boundary of this package.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// Scope narrows the repositories. Both fields are optional; when both are
// given, both apply.
type Scope struct {
	// RepoIDs are repository ids or full names.
	RepoIDs []string
	// TeamIDs are team ids: the repositories these teams OWN.
	TeamIDs []string
	// AsOf is the instant the ownership is read at; zero means now (UTC).
	AsOf time.Time
}

// baseline is the mean when enough days hold a value, else nil. A mean with no
// day behind it (NULL) is nil whatever the count says.
func baseline(mean *float64, days int) *float64 {
	if mean == nil || days < MinDays {
		return nil
	}
	value := *mean
	return &value
}

// statement reads, per repository, the mean of each measure over the window and
// the number of days that hold a value. The inner query keeps the newest version
// of each (repository, day); the tuple keeps a NULL of the newest version from
// being skipped for an older value. avg() over a Nullable column leaves the NULL
// days out and is NULL when no day holds a value.
func statement(scopeFilter string) string {
	return `
        SELECT
            toString(b.repo_id) AS repo_id,
            nullIf(r.repo, '') AS repo_name,
            avg(b.line_pct) AS line_mean,
            countIf(b.line_pct IS NOT NULL) AS line_days,
            avg(b.branch_pct) AS branch_mean,
            countIf(b.branch_pct IS NOT NULL) AS branch_days
        FROM (
            SELECT
                repo_id,
                day,
                (argMax(tuple(line_coverage_pct), computed_at)).1 AS line_pct,
                (argMax(tuple(branch_coverage_pct), computed_at)).1 AS branch_pct
            FROM testops_coverage_metrics_daily
            WHERE org_id = {org_id:String}
              AND day >= {start_day:Date}
              AND day < {end_day:Date}` + scopeFilter + `
            GROUP BY repo_id, day
        ) AS b
        LEFT JOIN (
            SELECT id, argMax(repo, last_synced) AS repo
            FROM repos
            WHERE org_id = {org_id:String}
            GROUP BY id
        ) AS r ON r.id = b.repo_id
        GROUP BY b.repo_id, r.repo
        ORDER BY repo_id`
}

// Resolve answers coverageBaselines. orgID must be the AUTHORIZED org. endDate
// is the day the window ends before: the window is the WindowDays days before
// it.
func Resolve(ctx context.Context, client QueryClient, orgID string, endDate graphqldate.Date, scope Scope) ([]model.RepoCoverageBaseline, error) {
	if client == nil {
		return nil, errors.New("coveragebaselines: clickhouse client is required")
	}
	end := endDate.Time()
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: end.AddDate(0, 0, -WindowDays).Format("2006-01-02")},
		{Name: "end_day", Value: end.Format("2006-01-02")},
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
		scopeFilter += "\n              AND " + teamCondition
		bindings = append(bindings, teamBindings...)
	}

	rows, err := client.Query(ctx, statement(scopeFilter), bindings)
	if err != nil {
		return nil, fmt.Errorf("coveragebaselines: query: %w", err)
	}
	defer rows.Close()
	out := []model.RepoCoverageBaseline{}
	for rows.Next() {
		var (
			repoID               string
			repoName             *string
			lineMean, branchMean *float64
			lineDays, branchDays uint64
		)
		if scanErr := rows.Scan(&repoID, &repoName, &lineMean, &lineDays, &branchMean, &branchDays); scanErr != nil {
			return nil, fmt.Errorf("coveragebaselines: scan: %w", scanErr)
		}
		row := model.RepoCoverageBaseline{
			RepoID:            repoID,
			LineBaselinePct:   baseline(lineMean, int(lineDays)),
			LineDays:          int(lineDays),
			BranchBaselinePct: baseline(branchMean, int(branchDays)),
			BranchDays:        int(branchDays),
		}
		if repoName != nil && *repoName != "" {
			name := *repoName
			row.RepoName = &name
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("coveragebaselines: rows: %w", err)
	}
	return out, nil
}
