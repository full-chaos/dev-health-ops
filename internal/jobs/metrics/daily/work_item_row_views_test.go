package daily

import (
	"context"
	"errors"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

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
			viewAt := 1
			if testCase.name == "work_item_user_metrics_daily" {
				viewAt = 2
			}
			views := []any{batch.appended[0][len(batch.appended[0])-viewAt], batch.appended[1][len(batch.appended[1])-viewAt]}
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
			if !regexp.MustCompile(`row_view(,\s*retracted)?\s*$`).MatchString(match[2]) {
				t.Fatalf("%s: INSERT INTO %s does not end its column list with row_view", file, match[1])
			}
		}
	}
	if found != 3 {
		t.Fatalf("found %d inserts into the three tables, want 3", found)
	}
}

type liveLegacyRows struct {
	driver.Rows
	keys    [][]string
	version time.Time
	index   int
}

func (rows *liveLegacyRows) Next() bool { rows.index++; return rows.index <= len(rows.keys) }
func (rows *liveLegacyRows) Scan(dest ...any) error {
	key := rows.keys[rows.index-1]
	for position, value := range key {
		*(dest[position].(*string)) = value
	}
	*(dest[len(key)].(*time.Time)) = rows.version
	return nil
}
func (rows *liveLegacyRows) Err() error   { return nil }
func (rows *liveLegacyRows) Close() error { return nil }

type liveLegacyConn struct {
	batch   *sendFailingBatch
	keys    [][]string
	version time.Time
}

func (conn *liveLegacyConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return conn.batch, nil
}

func (conn *liveLegacyConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &liveLegacyRows{keys: conn.keys, version: conn.version}, nil
}

type sendFailingBatch struct {
	recordingBatch
	sendErr error
}

func (batch *sendFailingBatch) Send() error { batch.sent = true; return batch.sendErr }

// The row of zeros over the live legacy key is stored in the SAME insert block as
// the org and scope rows, so an ambiguous Send error leaves either all of them or
// none: a live legacy row never sits beside a live view row.
func TestWorkItemWriterStoresTheLegacyRetractionInTheBlockOfTheViewRows(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	stored := time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
	for _, sendErr := range []error{nil, errors.New("ambiguous send")} {
		conn := &liveLegacyConn{
			batch:   &sendFailingBatch{sendErr: sendErr},
			keys:    [][]string{{"github", "s", "t", ""}},
			version: stored,
		}
		written, err := WriteWorkItemMetricsDaily(ctx, conn, "org-a", day,
			[]workitemmetrics.MetricsDailyRow{{Day: day, Provider: "github", WorkScopeID: "s", TeamID: "t", ItemsCompleted: 3}}, computedAt)
		if (err != nil) != (sendErr != nil) {
			t.Fatalf("send error %v returned %v", sendErr, err)
		}
		if written != 1 {
			t.Fatalf("written = %d, want 1 logical row also on an ambiguous send", written)
		}
		appended := conn.batch.appended
		if len(appended) != 3 {
			t.Fatalf("appended %d rows to the block, want org + scope + the legacy row of zeros", len(appended))
		}
		last := appended[2]
		if view := last[len(last)-1]; view != "" {
			t.Fatalf("third row of the block has row_view %q, want the legacy view", view)
		}
		if at := last[len(last)-3].(time.Time); !at.After(stored) {
			t.Fatalf("legacy row of zeros version %v is not newer than the stored legacy row %v", at, stored)
		}
		if completed := last[6]; completed != uint32(0) {
			t.Fatalf("legacy row of zeros holds items_completed %v", completed)
		}
	}
}
