package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// Red cell: three distinct events (A->B, B->A, A->B) of one item, one actor,
// one millisecond. Each is a real event, so three transitions must be kept.
// rankTransitionTies drops the second A->B because its content key repeats.
func TestRankTransitionTiesKeepsDistinctSameMillisecondEvents(t *testing.T) {
	at := time.Date(2026, 10, 3, 0, 0, 0, 5_000_000, time.UTC)
	repo := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	mk := func(from, to string) workItemTransitionStoredRow {
		return workItemTransitionStoredRow{
			RepoID: repo, WorkItemID: "synthetic#1", OccurredAt: at, Provider: "github",
			FromStatus: from, ToStatus: to, FromStatusRaw: from, ToStatusRaw: to,
			Actor: "synthetic-actor", OrgID: "synthetic-org",
		}
	}
	got := rankTransitionTies([]workItemTransitionStoredRow{
		mk("todo", "in_progress"), mk("in_progress", "todo"), mk("todo", "in_progress"),
	})
	if len(got) != 3 {
		t.Fatalf("kept %d transitions, want 3 (A->B, B->A, A->B are three events)", len(got))
	}
	seen := map[uint32]bool{}
	for _, row := range got {
		if seen[row.TieOrdinal] {
			t.Fatalf("tie_ordinal %d repeats; rows=%+v", row.TieOrdinal, got)
		}
		seen[row.TieOrdinal] = true
	}
}
