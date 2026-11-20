package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

// testopsReportCompletenessStatement reads, per run with a kept suite on the repo-day (testopsKeptSuitesToday: the rows the
// pass and fail counts sum), the attempts of its kept suites and every completeness row of the run (FINAL: verdict_version
// keeps a complete row over a later partial walk of the same attempt). After FINAL, (org_id, repo_id, run_id, run_attempt) is
// the table's sorting key, so each attempt has one row. A run with no completeness row gets two empty arrays (LEFT JOIN
// default). The verdict is taken in Go (reportRunVerdict).
var testopsReportCompletenessStatement = `
WITH kept AS (
    SELECT run_id, groupUniqArray(run_attempt) AS counted_attempts` + testopsKeptSuitesToday + `
    GROUP BY run_id
)
SELECT kept.run_id, kept.counted_attempts, marks.mark_attempts, marks.mark_completes
FROM kept
LEFT JOIN (
    SELECT run_id, groupArray(run_attempt) AS mark_attempts, groupArray(complete) AS mark_completes
    FROM testops_run_report_completeness FINAL
    WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}
      AND run_id IN (SELECT run_id FROM kept)
    GROUP BY run_id
) AS marks ON marks.run_id = kept.run_id
ORDER BY kept.run_id`

type reportRunState uint8

const (
	reportRunComplete reportRunState = iota
	reportRunPartial
	reportRunUnmarked
)

// reportRunVerdict names a run partial, unmarked or complete. The considered attempts are the counted ones (attempts of its
// kept suites of the day) and the newest attempt known (counted or with a completeness row). Partial when any considered
// attempt has a row with complete = 0: the counted data holds a partial attempt, or the newest attempt lost reports (a re-run
// of failed jobs that kept attempt-1 slots of a partial attempt 1 is partial). Else unmarked when any considered attempt has
// no row: a row of an older attempt never vouches for a newer counted attempt. Else complete.
func reportRunVerdict(counted, markAttempts []uint32, markCompletes []uint8) reportRunState {
	marks := make(map[uint32]uint8, len(markAttempts))
	var newest uint32
	for index, attempt := range markAttempts {
		marks[attempt] = markCompletes[index]
		newest = max(newest, attempt)
	}
	for _, attempt := range counted {
		newest = max(newest, attempt)
	}
	unmarked := false
	for _, attempt := range append(append([]uint32{}, counted...), newest) {
		complete, ok := marks[attempt]
		if !ok {
			unmarked = true
			continue
		}
		if complete == 0 {
			return reportRunPartial
		}
	}
	if unmarked {
		return reportRunUnmarked
	}
	return reportRunComplete
}

// loadNativeTestopsReportRunCounts counts the report completeness of the runs with a kept suite on one repo-day. A failed
// read returns its error: the family then writes no row, never 0 or NULL in place of the counts.
func loadNativeTestopsReportRunCounts(
	ctx context.Context, conn testopsNativeConn, orgID string, repoID uuid.UUID, start, end time.Time,
) (*testops.ReportRunCounts, error) {
	rows, err := conn.Query(ctx, testopsReportCompletenessStatement, testopsSourceArgs(orgID, []uuid.UUID{repoID}, start, end)...)
	if err != nil {
		return nil, fmt.Errorf("load testops run report completeness: %w", err)
	}
	defer rows.Close()
	counts := &testops.ReportRunCounts{}
	for rows.Next() {
		var runID string
		var counted, markAttempts []uint32
		var markCompletes []uint8
		if err := rows.Scan(&runID, &counted, &markAttempts, &markCompletes); err != nil {
			return nil, fmt.Errorf("scan testops run report completeness: %w", err)
		}
		if len(markAttempts) != len(markCompletes) {
			return nil, fmt.Errorf("testops run report completeness of run %q: %d attempts, %d verdicts", runID, len(markAttempts), len(markCompletes))
		}
		if attempt, twice := repeatedAttempt(markAttempts); twice {
			return nil, fmt.Errorf("testops run report completeness of run %q: attempt %d read twice (the read is not FINAL)", runID, attempt)
		}
		counts.Runs++
		switch reportRunVerdict(counted, markAttempts, markCompletes) {
		case reportRunPartial:
			counts.Partial++
		case reportRunUnmarked:
			counts.Unmarked++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate testops run report completeness: %w", err)
	}
	return counts, nil
}

func repeatedAttempt(attempts []uint32) (uint32, bool) {
	seen := make(map[uint32]struct{}, len(attempts))
	for _, attempt := range attempts {
		if _, ok := seen[attempt]; ok {
			return attempt, true
		}
		seen[attempt] = struct{}{}
	}
	return 0, false
}
