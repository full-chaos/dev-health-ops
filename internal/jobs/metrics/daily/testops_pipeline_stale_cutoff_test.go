package daily

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// staleCutoffConn serves one ci_pipeline_runs row to the pipeline loader, no repository owner, and records the write.
type staleCutoffConn struct {
	driver.Conn
	queryRecordingBatchConn
	status     string
	startedAt  time.Time
	lastSynced time.Time
}

func (conn *staleCutoffConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM ci_pipeline_runs FINAL") && strings.Contains(query, "run_id, last_synced"):
		return &staleCutoffRows{conn: conn}, nil
	case strings.Contains(query, "FROM ci_pipeline_runs"):
		return nil, fmt.Errorf("unexpected pipeline query %q", query)
	default:
		return &staleCutoffRows{}, nil
	}
}

func (conn *staleCutoffConn) PrepareBatch(ctx context.Context, query string, options ...driver.PrepareBatchOption) (driver.Batch, error) {
	return conn.queryRecordingBatchConn.PrepareBatch(ctx, query, options...)
}

type staleCutoffRows struct {
	driver.Rows
	conn *staleCutoffConn
	read bool
}

func (rows *staleCutoffRows) Next() bool {
	available := rows.conn != nil && !rows.read
	rows.read = true
	return available
}

func (*staleCutoffRows) Close() error { return nil }
func (*staleCutoffRows) Err() error   { return nil }

func (rows *staleCutoffRows) Scan(destinations ...any) error {
	if len(destinations) != 12 {
		return fmt.Errorf("unexpected pipeline column count %d", len(destinations))
	}
	*destinations[0].(**string) = &rows.conn.status
	*destinations[2].(*time.Time) = rows.conn.startedAt
	*destinations[9].(*string) = "org-14019"
	*destinations[10].(*string) = "run-1"
	*destinations[11].(*time.Time) = rows.conn.lastSynced
	return nil
}

// staleCutoffColumn is the driver codec of a clock column, typed as the baseline declares it.
func staleCutoffColumn(t *testing.T, table, name string) column.Interface {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile("`" + name + "` (DateTime(?:64)?\\([^)]*\\))")
	for _, object := range baseline.Objects {
		if object.Name != table {
			continue
		}
		match := pattern.FindStringSubmatch(object.Create)
		if match == nil {
			t.Fatalf("no clock column %s in %s", name, object.Create)
		}
		clock, err := column.Type(match[1]).Column(name, &column.ServerContext{})
		if err != nil {
			t.Fatal(err)
		}
		return clock
	}
	t.Fatalf("no table %s in the baseline", table)
	return nil
}

// staleCutoffStored is stamp as the column stores it.
func staleCutoffStored(t *testing.T, clock column.Interface, stamp time.Time) time.Time {
	t.Helper()
	if err := clock.AppendRow(stamp); err != nil {
		t.Fatal(err)
	}
	return clock.Row(clock.Rows()-1, false).(time.Time)
}

// The stale cutoff is the one the stored row states: stored computed_at (a whole-second DateTime) minus 6 h. A worker
// clock with a fraction of a second must not count a run synced at that cutoff, or up to the clock's fraction after it,
// as stale.
func TestTestopsPipelineStaleCutoffIsTheStoredComputedAtMinusTheLookback(t *testing.T) {
	computedColumn := staleCutoffColumn(t, "testops_pipeline_metrics_daily", "computed_at")
	syncedColumn := staleCutoffColumn(t, "ci_pipeline_runs", "last_synced")
	base := time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)
	cutoff := base.Add(-6 * time.Hour)
	day := base.AddDate(0, 0, -1).Truncate(24 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-000000014019")
	for _, status := range []string{"running", "queued"} {
		for _, clock := range []time.Duration{0, 750 * time.Millisecond} {
			for _, sync := range []time.Duration{-time.Millisecond, 0, time.Millisecond, 749 * time.Millisecond, 750 * time.Millisecond, 751 * time.Millisecond} {
				t.Run(fmt.Sprintf("%s/clock=+%s/sync=%s", status, clock, sync), func(t *testing.T) {
					conn := &staleCutoffConn{
						queryRecordingBatchConn: queryRecordingBatchConn{batch: &recordingBatch{}},
						status:                  status, startedAt: day.Add(12 * time.Hour),
						lastSynced: staleCutoffStored(t, syncedColumn, cutoff.Add(sync)),
					}
					executor, err := NewTestopsPipelineExecutor(conn)
					if err != nil {
						t.Fatal(err)
					}
					executor.nowUTC = func() time.Time { return base.Add(clock) }
					written, err := executor.ComputeFamily(context.Background(),
						Run{OrganizationID: "org-14019", TargetDay: day},
						Partition{RepoIDs: []RepositoryID{RepositoryID(repo.String())}})
					if err != nil || written != 1 || !conn.batch.sent {
						t.Fatalf("compute and write: rows %d, sent %t, err %v", written, conn.batch.sent, err)
					}
					if !strings.HasSuffix(strings.Join(strings.Fields(conn.query), " "), "computed_at, skipped_count, unfinished_count, other_count, unfinished_stale_count, pooled_service_ids, pooled_durations, pooled_queues, pooled_samples_stored)") {
						t.Fatalf("unexpected INSERT %q", conn.query)
					}
					row := conn.batch.appended[0]
					sentComputedAt, ok := row[17].(time.Time)
					if !ok {
						t.Fatalf("computed_at value is %T", row[17])
					}
					stored := staleCutoffStored(t, computedColumn, sentComputedAt)
					if !sentComputedAt.Equal(stored) {
						t.Errorf("the writer sends computed_at %s, the column stores %s", sentComputedAt.Format(time.RFC3339Nano), stored.Format(time.RFC3339Nano))
					}
					var want uint32
					if sync < 0 {
						want = 1
					}
					if fromStored := conn.lastSynced.Before(stored.Add(-6 * time.Hour)); fromStored != (want == 1) {
						t.Fatalf("the cell expects stale %d, the stored row implies %t", want, fromStored)
					}
					if stale := *row[21].(*uint32); stale != want {
						t.Errorf("stored computed_at %s, synced %s: stale %d, want %d",
							stored.Format(time.RFC3339Nano), conn.lastSynced.Format(time.RFC3339Nano), stale, want)
					}
				})
			}
		}
	}
}
