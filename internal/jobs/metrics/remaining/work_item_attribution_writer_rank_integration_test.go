//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestWriterRankBreaksExactComputedAtTieRegardlessOfArrivalOrder is the
// red-first test for the producer rank fold: two producers writing the
// SAME (org, repo, work_item, team, source) key at the IDENTICAL computed_at
// must always resolve to the same survivor, in EITHER arrival order. Before
// the fold, ReplacingMergeTree(computed_at) sees a version tie and the
// survivor is whichever part the merge happens to keep -- observed as
// "whichever producer wrote last" (see TestCrossWriterCollisionSurvivorNamesItsProducer's
// prior form). This test drives both orders and requires the daily family's
// higher rank to win each time.
func TestWriterRankBreaksExactComputedAtTieRegardlessOfArrivalOrder(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionMigratedClickHouse(t, ctx)
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	repoID := uuid.New()
	stamp := time.Now().UTC().Truncate(time.Millisecond)

	cases := []struct {
		name  string
		first WorkItemAttributionProducer
		last  WorkItemAttributionProducer
	}{
		{
			name:  "backstop then daily",
			first: WorkItemAttributionProducer{Writer: WorkItemAttributionWriterBackstop, RunID: uuid.NewString()},
			last:  WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: uuid.NewString()},
		},
		{
			name:  "daily then backstop",
			first: WorkItemAttributionProducer{Writer: WorkItemAttributionWriterDaily, RunID: uuid.NewString()},
			last:  WorkItemAttributionProducer{Writer: WorkItemAttributionWriterBackstop, RunID: uuid.NewString()},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			orgID := "org-rank-" + uuid.NewString()
			dailyRunID := testCase.last.RunID
			if testCase.first.Writer == WorkItemAttributionWriterDaily {
				dailyRunID = testCase.first.RunID
			}

			writeAttributionProbeRow(t, ctx, writer, testCase.first,
				orgID, repoID, "W", "repo_ownership", "team-x", "First Team", 1, stamp)
			writeAttributionProbeRow(t, ctx, writer, testCase.last,
				orgID, repoID, "W", "repo_ownership", "team-x", "Last Team", 1, stamp)

			optimizeAttributionTable(t, ctx, conn)

			probes := readAttributionProbeRows(t, ctx, conn, orgID, "W")
			if len(probes) != 1 {
				t.Fatalf("got %d surviving rows for W, want exactly 1. Rows: %+v", len(probes), probes)
			}
			if probes[0].writer != WorkItemAttributionWriterDaily || probes[0].runID != dailyRunID {
				t.Fatalf("arrival order %q: survivor is writer=%q run_id=%q, want writer=%q "+
					"run_id=%q -- the daily family's rank must win this exact tie regardless of "+
					"which producer wrote last. Row: %+v",
					testCase.name, probes[0].writer, probes[0].runID,
					WorkItemAttributionWriterDaily, dailyRunID, probes[0])
			}
		})
	}
}
