package daily

import (
	"context"
	"testing"
	"time"
)

// TestWriteWorkItemBlockedDurationsDailyAppendsEverySnapshot pins the detail
// table's column order and the zero-row rule. A zero is data, rather than a
// skipped write: it replaces a previously blocked item when a recompute finds
// no blocked interval for that day.
func TestWriteWorkItemBlockedDurationsDailyAppendsEverySnapshot(t *testing.T) {
	batch := &recordingBatch{}
	day := time.Date(2026, 8, 24, 12, 30, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	computedAt := time.Date(2026, 8, 25, 1, 2, 3, 4, time.FixedZone("UTC-5", -5*60*60))
	rows := []workItemBlockedDurationDailyRow{
		{Provider: "github", WorkScopeID: "full-chaos/dev-health", TeamID: "team-a", TeamName: "Team A", WorkItemID: "github:full-chaos/dev-health#1", DurationHours: 4},
		{Provider: "linear", WorkScopeID: "ENG", TeamID: "team-b", TeamName: "Team B", WorkItemID: "linear:ENG-2", DurationHours: 0},
	}

	written, err := WriteWorkItemBlockedDurationsDaily(context.Background(), &recordingBatchConn{batch: batch}, "org-42", day, rows, computedAt)
	if err != nil {
		t.Fatal(err)
	}
	if written != len(rows) || len(batch.appended) != len(rows) || !batch.sent {
		t.Fatalf("written=%d appended=%d sent=%t, want %d/%d/true", written, len(batch.appended), batch.sent, len(rows), len(rows))
	}

	wantDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	wantComputedAt := computedAt.UTC()
	for index, row := range rows {
		appended := batch.appended[index]
		if len(appended) != 9 {
			t.Fatalf("row %d has %d values, want 9: %#v", index, len(appended), appended)
		}
		if appended[0] != wantDay || appended[1] != row.Provider || appended[2] != row.WorkScopeID || appended[3] != row.TeamID || appended[4] != row.TeamName || appended[5] != row.WorkItemID || appended[6] != row.DurationHours || appended[7] != wantComputedAt || appended[8] != "org-42" {
			t.Fatalf("row %d = %#v, want day/provider/scope/team/id/duration/computed_at/org in migration column order", index, appended)
		}
	}
}
