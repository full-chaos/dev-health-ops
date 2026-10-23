package daily

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

type liveLegacyRows struct {
	driver.Rows
	rows  [][]any
	index int
}

func (rows *liveLegacyRows) Next() bool { rows.index++; return rows.index <= len(rows.rows) }
func (rows *liveLegacyRows) Scan(dest ...any) error {
	for position, value := range rows.rows[rows.index-1] {
		switch target := dest[position].(type) {
		case *string:
			*target = value.(string)
		case *time.Time:
			*target = value.(time.Time)
		}
	}
	return nil
}
func (rows *liveLegacyRows) Err() error   { return nil }
func (rows *liveLegacyRows) Close() error { return nil }

// liveLegacyConn answers the two reads of the legacy retraction (the live keys,
// and the values the table carries) and counts what the writer does with its batch.
type liveLegacyConn struct {
	batch    *countingBatch
	live     [][]any
	carried  [][]any
	queryErr error
	prepares int
	queries  []string
}

func (conn *liveLegacyConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.prepares++
	return conn.batch, nil
}

func (conn *liveLegacyConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	if conn.queryErr != nil {
		return nil, conn.queryErr
	}
	if !strings.Contains(query, "max(computed_at)") {
		return &liveLegacyRows{rows: conn.carried}, nil
	}
	return &liveLegacyRows{rows: conn.live}, nil
}

type countingBatch struct {
	recordingBatch
	sendErr   error
	appendErr error
	sends     int
	aborts    int
}

func (batch *countingBatch) Append(values ...any) error {
	if batch.appendErr != nil {
		return batch.appendErr
	}
	return batch.recordingBatch.Append(values...)
}
func (batch *countingBatch) Send() error  { batch.sends++; batch.sent = true; return batch.sendErr }
func (batch *countingBatch) Abort() error { batch.aborts++; return nil }

var (
	rowViewsDay        = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	rowViewsStored     = time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
	rowViewsComputedAt = time.Date(2026, 8, 25, 6, 0, 0, 0, time.UTC)
)

func TestWorkItemWritersWriteEveryRowUnderTheOrgAndTheScopeView(t *testing.T) {
	ctx := context.Background()
	for _, testCase := range []struct {
		name   string
		viewAt int
		write  func(conn workItemBatchConn) (int, error)
	}{
		{"work_item_metrics_daily", 1, func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemMetricsDaily(ctx, conn, "org-a", rowViewsDay,
				[]workitemmetrics.MetricsDailyRow{{Day: rowViewsDay, Provider: "github", WorkScopeID: "s", TeamID: "t"}}, rowViewsComputedAt)
		}},
		{"work_item_user_metrics_daily", 2, func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemUserMetricsDaily(ctx, conn, "org-a", rowViewsDay,
				[]workitemmetrics.UserMetricsDailyRow{{Day: rowViewsDay, Provider: "github", WorkScopeID: "s", UserIdentity: "u", TeamID: "t"}}, rowViewsComputedAt)
		}},
		{"work_item_state_durations_daily", 1, func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemStateDurationsDaily(ctx, conn, "org-a", rowViewsDay,
				[]workItemStateDailyRow{{Provider: "github", WorkScopeID: "s", TeamID: "t", Status: "open"}}, rowViewsComputedAt)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			conn := &liveLegacyConn{batch: &countingBatch{}}
			written, err := testCase.write(conn)
			if err != nil {
				t.Fatal(err)
			}
			appended := conn.batch.appended
			if written != 1 || len(appended) != 2 {
				t.Fatalf("written=%d appended=%d, want 1 row stored under 2 views", written, len(appended))
			}
			views := []any{appended[0][len(appended[0])-testCase.viewAt], appended[1][len(appended[1])-testCase.viewAt]}
			if views[0] != "org" || views[1] != "scope" {
				t.Fatalf("row_view of the appended rows = %v, want [org scope]", views)
			}
			if conn.batch.sends != 1 {
				t.Fatalf("sends = %d, want 1", conn.batch.sends)
			}
		})
	}
}

// A writer whose connection cannot read the live legacy keys fails closed: it
// stores nothing rather than leave a legacy row beside the view rows.
func TestWorkItemWritersFailClosedWithoutAReadingConnection(t *testing.T) {
	batch := &recordingBatch{}
	_, err := WriteWorkItemMetricsDaily(context.Background(), &recordingBatchConn{batch: batch}, "org-a", rowViewsDay,
		[]workitemmetrics.MetricsDailyRow{{Provider: "github", WorkScopeID: "s", TeamID: "t"}}, rowViewsComputedAt)
	if !errors.Is(err, ErrInvalidState) || batch.sent {
		t.Fatalf("err = %v, sent = %v, want ErrInvalidState and nothing stored", err, batch.sent)
	}
}

// The read of the live legacy keys comes BEFORE the batch is prepared: a failed
// read leaves no prepared batch holding a connection of the pool.
func TestWorkItemWritersDoNotPrepareABatchWhenTheLegacyReadFails(t *testing.T) {
	ctx := context.Background()
	for name, write := range map[string]func(conn workItemBatchConn) (int, error){
		"metrics": func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemMetricsDaily(ctx, conn, "org-a", rowViewsDay,
				[]workitemmetrics.MetricsDailyRow{{Provider: "github", WorkScopeID: "s", TeamID: "t"}}, rowViewsComputedAt)
		},
		"user": func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemUserMetricsDaily(ctx, conn, "org-a", rowViewsDay,
				[]workitemmetrics.UserMetricsDailyRow{{Provider: "github", WorkScopeID: "s", UserIdentity: "u"}}, rowViewsComputedAt)
		},
		"state": func(conn workItemBatchConn) (int, error) {
			return WriteWorkItemStateDurationsDaily(ctx, conn, "org-a", rowViewsDay,
				[]workItemStateDailyRow{{Provider: "github", WorkScopeID: "s", TeamID: "t", Status: "open"}}, rowViewsComputedAt)
		},
	} {
		conn := &liveLegacyConn{batch: &countingBatch{}, queryErr: errors.New("read failed")}
		if _, err := write(conn); err == nil {
			t.Fatalf("%s: a failed legacy read returned no error", name)
		}
		if conn.prepares != 0 {
			t.Fatalf("%s: %d batch(es) prepared before the failed read", name, conn.prepares)
		}
	}
}

// Every prepared batch is released: an append that fails aborts it.
func TestWorkItemWritersAbortTheBatchWhenAnAppendFails(t *testing.T) {
	conn := &liveLegacyConn{batch: &countingBatch{appendErr: errors.New("append failed")}}
	if _, err := WriteWorkItemMetricsDaily(context.Background(), conn, "org-a", rowViewsDay,
		[]workitemmetrics.MetricsDailyRow{{Provider: "github", WorkScopeID: "s", TeamID: "t"}}, rowViewsComputedAt); err == nil {
		t.Fatal("a failed append returned no error")
	}
	if conn.batch.aborts != 1 {
		t.Fatalf("aborts = %d, want 1", conn.batch.aborts)
	}
}

// The row of zeros over the live legacy key is stored in the SAME insert block as
// the org and scope rows, so an ambiguous Send error leaves all of them or none.
func TestWorkItemWriterStoresTheLegacyRetractionInTheBlockOfTheViewRows(t *testing.T) {
	ctx := context.Background()
	for _, sendErr := range []error{nil, errors.New("ambiguous send")} {
		conn := &liveLegacyConn{
			batch: &countingBatch{sendErr: sendErr},
			live:  [][]any{{"github", "s", "t", "", rowViewsStored}},
		}
		written, err := WriteWorkItemMetricsDaily(ctx, conn, "org-a", rowViewsDay,
			[]workitemmetrics.MetricsDailyRow{{Day: rowViewsDay, Provider: "github", WorkScopeID: "s", TeamID: "t", ItemsCompleted: 3}}, rowViewsComputedAt)
		if (err != nil) != (sendErr != nil) {
			t.Fatalf("send error %v returned %v", sendErr, err)
		}
		if written != 2 {
			t.Fatalf("written = %d, want the row and its row of zeros (2), also on an ambiguous send", written)
		}
		appended := conn.batch.appended
		if len(appended) != 3 {
			t.Fatalf("appended %d rows to the block, want org + scope + the legacy row of zeros", len(appended))
		}
		last := appended[2]
		if view := last[len(last)-1]; view != "" {
			t.Fatalf("third row of the block has row_view %q, want the legacy view", view)
		}
		if at := last[len(last)-3].(time.Time); !at.After(rowViewsStored) {
			t.Fatalf("legacy row of zeros version %v is not newer than the stored legacy row %v", at, rowViewsStored)
		}
		if completed := last[6]; completed != uint32(0) {
			t.Fatalf("legacy row of zeros holds items_completed %v", completed)
		}
		if conn.prepares != 1 || conn.batch.sends != 1 {
			t.Fatalf("prepares=%d sends=%d, want one block", conn.prepares, conn.batch.sends)
		}
	}
}

func TestWorkItemStateWriterStoresTheLegacyRetractionInTheBlock(t *testing.T) {
	conn := &liveLegacyConn{
		batch: &countingBatch{},
		live:  [][]any{{"github", "s", "t", "open", "", rowViewsStored}},
	}
	written, err := WriteWorkItemStateDurationsDaily(context.Background(), conn, "org-a", rowViewsDay,
		[]workItemStateDailyRow{{Provider: "github", WorkScopeID: "s", TeamID: "t", TeamName: "T", Status: "open", DurationHours: 5, ItemsTouched: 2}}, rowViewsComputedAt)
	if err != nil || written != 2 {
		t.Fatalf("written = %d, err = %v, want the row and its row of zeros", written, err)
	}
	appended := conn.batch.appended
	if len(appended) != 3 {
		t.Fatalf("appended %d rows, want 3", len(appended))
	}
	zero := appended[2]
	if zero[len(zero)-1] != "" || zero[6] != float64(0) || zero[7] != uint32(0) {
		t.Fatalf("third row is not the legacy row of zeros: %v", zero)
	}
}

// A person moved from team T1 to T2: the row of zeros carries the team of the
// newest STORED row (T1), not the team of the new row, as the run-level rule does.
func TestWorkItemUserWriterRetractionCarriesTheStoredTeamAndSetsTheMarker(t *testing.T) {
	conn := &liveLegacyConn{
		batch:   &countingBatch{},
		live:    [][]any{{"github", "s", "u", "", rowViewsStored}},
		carried: [][]any{{"github", "s", "u", "", "T1", "Team One"}},
	}
	written, err := WriteWorkItemUserMetricsDaily(context.Background(), conn, "org-a", rowViewsDay,
		[]workitemmetrics.UserMetricsDailyRow{{Day: rowViewsDay, Provider: "github", WorkScopeID: "s", UserIdentity: "u", TeamID: "T2", TeamName: "Team Two", ItemsCompleted: 1}}, rowViewsComputedAt)
	if err != nil || written != 2 {
		t.Fatalf("written = %d, err = %v", written, err)
	}
	appended := conn.batch.appended
	if len(appended) != 3 {
		t.Fatalf("appended %d rows, want 3", len(appended))
	}
	if marker := appended[0][len(appended[0])-1]; marker != uint8(0) {
		t.Fatalf("a view row carries retracted = %v, want 0", marker)
	}
	zero := appended[2]
	if zero[len(zero)-1] != uint8(1) || zero[len(zero)-2] != "" {
		t.Fatalf("legacy row of zeros has retracted=%v row_view=%v, want 1 and the legacy view", zero[len(zero)-1], zero[len(zero)-2])
	}
	if zero[4] != "T1" || zero[5] != "Team One" {
		t.Fatalf("legacy row of zeros carries team %v/%v, want the stored T1/Team One", zero[4], zero[5])
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
