package daily

import (
	"context"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

func TestWorkItemWritersWriteEveryRowUnderTheOrgAndTheScopeView(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name  string
		write func(conn workItemBatchConn) (int, error)
	}{
		{"work_item_metrics_daily", func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemMetricsDaily(ctx, conn, "org-a", day,
				[]workitemmetrics.MetricsDailyRow{{Day: day, Provider: "github", WorkScopeID: "s", TeamID: "t"}}, computedAt)
		}},
		{"work_item_user_metrics_daily", func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemUserMetricsDaily(ctx, conn, "org-a", day,
				[]workitemmetrics.UserMetricsDailyRow{{Day: day, Provider: "github", WorkScopeID: "s", UserIdentity: "u", TeamID: "t"}}, computedAt)
		}},
		{"work_item_state_durations_daily", func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemStateDurationsDaily(ctx, conn, "org-a", day,
				[]workItemStateDailyRow{{Provider: "github", WorkScopeID: "s", TeamID: "t", Status: "open"}}, computedAt)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			batch := &recordingBatch{}
			written, err := testCase.write(&recordingBatchConn{batch: batch})
			if err != nil {
				t.Fatal(err)
			}
			if written != 1 || len(batch.appended) != 2 {
				t.Fatalf("written=%d appended=%d, want 1 logical row and 2 appended rows", written, len(batch.appended))
			}
			views := []any{batch.appended[0][len(batch.appended[0])-1], batch.appended[1][len(batch.appended[1])-1]}
			if views[0] != "org" || views[1] != "scope" {
				t.Fatalf("row_view of the appended rows = %v, want [org scope]", views)
			}
			if !batch.sent {
				t.Fatal("batch not sent")
			}
		})
	}
}

// A writer that names the table and not row_view writes a legacy row, which a
// reader that keeps the legacy rows and one view counts a second time.
func TestEveryInsertIntoAWorkItemViewTableNamesRowView(t *testing.T) {
	insert := regexp.MustCompile("(?s)INSERT INTO (work_item_metrics_daily|work_item_state_durations_daily|work_item_user_metrics_daily) \\((.*?)\\)`")
	found := 0
	for _, file := range []string{"work_item_native_clickhouse.go", "work_item_state_native_clickhouse.go"} {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range insert.FindAllStringSubmatch(string(source), -1) {
			found++
			if !regexp.MustCompile(`row_view\s*$`).MatchString(match[2]) {
				t.Fatalf("%s: INSERT INTO %s does not end its column list with row_view", file, match[1])
			}
		}
	}
	if found != 3 {
		t.Fatalf("found %d inserts into the three tables, want 3", found)
	}
}
