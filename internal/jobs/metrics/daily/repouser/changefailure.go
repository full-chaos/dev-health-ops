package repouser

import (
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
)

// ApplyChangeFailure adds the day's change-failure counts to a Compute result
// (CHAOS-8981): one ChangeFailureDaily row per repository that has a
// deployment or an incident, and the day's ChangeFailureRateIncident on each
// repository row that Compute produced. A repository with counts but no
// commit or pull-request activity gets a ChangeFailureDaily row only: adding a
// repo_metrics_daily row for it would zero-fill every other column of that
// table.
//
// stored names the repositories that already have a repo_change_failure_daily
// row for the day. One of them that has nothing to count now (its incident was
// deleted, its service mapping moved, its deployment is gone) gets a row of
// zeros: the table keeps the newest row per key and cannot drop one, so the
// old counts stay the answer until a newer row replaces them. Zeros read as
// "no deployment, no incident", the same as no row. A repository that never
// had a row and has nothing to count gets none.
func ApplyChangeFailure(result *Result, day time.Time, counts map[uuid.UUID]changefailure.Counts, stored []uuid.UUID, computedAt time.Time) {
	if result == nil {
		return
	}
	dayStart, _ := utcDayWindow(day)
	rows := make(map[uuid.UUID]changefailure.Counts, len(counts)+len(stored))
	for repoID, c := range counts {
		if !c.Empty() {
			rows[repoID] = c
		}
	}
	for _, repoID := range stored {
		if _, counted := rows[repoID]; !counted {
			rows[repoID] = changefailure.Counts{}
		}
	}
	for _, repoID := range changefailure.SortedRepoIDs(rows) {
		result.ChangeFailure = append(result.ChangeFailure, ChangeFailureDaily{
			RepoID: repoID, Day: dayStart, Counts: rows[repoID], ComputedAt: computedAt.UTC(),
		})
	}
	for i := range result.RepoMetrics {
		// The one-day column holds the value only: NULL for every state that is
		// not measured. A reader that needs the state reads the counts.
		result.RepoMetrics[i].ChangeFailureRateIncident = changefailure.Rate(counts[result.RepoMetrics[i].RepoID]).Value
	}
}
