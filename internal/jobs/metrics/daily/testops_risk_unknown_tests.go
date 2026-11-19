package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// loadTestopsRiskTestsUnknown returns, for one compute invocation, the repositories whose day has no test row but is not a
// repo without tests: the repository kept a test suite on a day of [historyStart, start), or a run of the day itself has a
// newest attempt whose report completeness is complete = 0. The run's day is the run's started_at day in ci_pipeline_runs,
// never the day the completeness row was synced. A repository in neither set has no tests: its flake is a true 0.
func loadTestopsRiskTestsUnknown(
	ctx context.Context, conn testopsNativeConn, orgID string, repoIDs []uuid.UUID, historyStart, start, end time.Time,
) (map[uuid.UUID]struct{}, error) {
	unknown := make(map[uuid.UUID]struct{})
	if len(repoIDs) == 0 {
		return unknown, nil
	}
	collect := func(query string, args []any, what string) error {
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("load testops risk %s: %w", what, err)
		}
		defer rows.Close()
		for rows.Next() {
			var repoID uuid.UUID
			if err := rows.Scan(&repoID); err != nil {
				return fmt.Errorf("scan testops risk %s: %w", what, err)
			}
			unknown[repoID] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate testops risk %s: %w", what, err)
		}
		return nil
	}
	if err := collect("SELECT DISTINCT repo_id"+testopsKeptSuitesToday,
		testopsSourceArgs(orgID, repoIDs, historyStart, start), "suite history"); err != nil {
		return nil, err
	}
	if err := collect(`
SELECT DISTINCT repo_id
FROM ci_pipeline_runs FINAL
WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
  AND started_at >= {start:DateTime64(3, 'UTC')} AND started_at < {end:DateTime64(3, 'UTC')}
  AND (repo_id, run_id) IN (
    SELECT repo_id, run_id
    FROM testops_run_report_completeness FINAL
    WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
    GROUP BY repo_id, run_id
    HAVING argMax(complete, run_attempt) = 0
  )`, testopsSourceArgs(orgID, repoIDs, start, end), "report completeness"); err != nil {
		return nil, err
	}
	return unknown, nil
}

// unknownTestMetric is the test row of a repo-day that has CI runs and no test row where tests exist but were not read: a row
// that measured no case, which the risk computations leave out of the flake and recurrence factors.
func unknownTestMetric(repoID uuid.UUID, pipe *testops.PipelineMetric) *testops.TestMetric {
	return &testops.TestMetric{RepoID: repoID, OrgID: pipe.OrgID, TeamID: pipe.TeamID, ServiceID: pipe.ServiceID}
}
