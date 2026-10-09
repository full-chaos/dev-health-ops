package daily

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

type capabilityRecordingConn struct {
	stubDriverConn
	prepared int
	batch    *capabilityRecordingBatch
}

func (conn *capabilityRecordingConn) PrepareBatch(context.Context, string, ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	conn.prepared++
	return conn.batch, nil
}

type capabilityRecordingBatch struct {
	rows    [][]any
	sendErr error
}

func (batch *capabilityRecordingBatch) Append(values ...any) error {
	batch.rows = append(batch.rows, values)
	return nil
}
func (batch *capabilityRecordingBatch) Send() error                     { return batch.sendErr }
func (batch *capabilityRecordingBatch) Abort() error                    { return nil }
func (batch *capabilityRecordingBatch) AppendStruct(any) error          { return errors.New("unused") }
func (batch *capabilityRecordingBatch) Column(int) chdriver.BatchColumn { return nil }
func (batch *capabilityRecordingBatch) Flush() error                    { return nil }
func (batch *capabilityRecordingBatch) IsSent() bool                    { return false }
func (batch *capabilityRecordingBatch) Rows() int                       { return len(batch.rows) }
func (batch *capabilityRecordingBatch) Columns() []column.Interface     { return nil }
func (batch *capabilityRecordingBatch) Close() error                    { return nil }

var capabilityTestDay = time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)

func capabilityTestRows() []workitemmetrics.CapabilityRow {
	first, last := workitemmetrics.CapabilityWindow(capabilityTestDay)
	return []workitemmetrics.CapabilityRow{
		{Provider: "jira", Measure: workitemmetrics.MeasureBugCompletedRatio, Tracked: false,
			EvidenceCount: 0, ItemCount: 4, WindowStart: first, WindowEnd: last},
		{Provider: "jira", Measure: workitemmetrics.MeasureStoryPointsCompleted, Tracked: true,
			EvidenceCount: 3, ItemCount: 4, WindowStart: first, WindowEnd: last},
	}
}

func TestWriteWorkItemMeasureCapabilityAppendsEveryColumn(t *testing.T) {
	conn := &capabilityRecordingConn{batch: &capabilityRecordingBatch{}}
	computedAt := time.Date(2026, 9, 4, 1, 2, 3, 0, time.FixedZone("x", 3600))
	written, err := WriteWorkItemMeasureCapability(context.Background(), conn, "org-1", capabilityTestRows(), computedAt)
	if err != nil || written != 2 {
		t.Fatalf("written=%d err=%v, want 2 nil", written, err)
	}
	first, last := workitemmetrics.CapabilityWindow(capabilityTestDay)
	want := [][]any{
		{"org-1", "jira", "bug_completed_ratio", uint8(0), uint32(0), uint32(4), first, last, computedAt.UTC()},
		{"org-1", "jira", "story_points_completed", uint8(1), uint32(3), uint32(4), first, last, computedAt.UTC()},
	}
	if !reflect.DeepEqual(conn.batch.rows, want) {
		t.Fatalf("appended:\n got %#v\nwant %#v", conn.batch.rows, want)
	}
}

func TestWriteWorkItemMeasureCapabilityGuards(t *testing.T) {
	conn := &capabilityRecordingConn{batch: &capabilityRecordingBatch{}}
	if written, err := WriteWorkItemMeasureCapability(context.Background(), conn, "org-1", nil, capabilityTestDay); written != 0 || err != nil || conn.prepared != 0 {
		t.Fatalf("no rows: written=%d err=%v prepared=%d, want 0 nil 0", written, err, conn.prepared)
	}
	if _, err := WriteWorkItemMeasureCapability(context.Background(), conn, " ", capabilityTestRows(), capabilityTestDay); !errors.Is(err, ErrInvalidState) || conn.prepared != 0 {
		t.Fatalf("blank organization: err=%v prepared=%d, want ErrInvalidState before a batch", err, conn.prepared)
	}
	if _, err := WriteWorkItemMeasureCapability(context.Background(), nil, "org-1", capabilityTestRows(), capabilityTestDay); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("nil connection: err=%v, want ErrInvalidState", err)
	}
	failing := &capabilityRecordingConn{batch: &capabilityRecordingBatch{sendErr: errors.New("ambiguous send")}}
	written, err := WriteWorkItemMeasureCapability(context.Background(), failing, "org-1", capabilityTestRows(), capabilityTestDay)
	if err == nil || written != 2 {
		t.Fatalf("send failure: written=%d err=%v, want the true count 2 and the error", written, err)
	}
}

type capabilityFinalizeConn struct {
	capabilityRecordingConn
	queries int
}

func (conn *capabilityFinalizeConn) Query(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
	conn.queries++
	if strings.Contains(query, "work_items FINAL") {
		return &oneWorkItemScopedRow{}, nil
	}
	return &emptyWorkItemRelatedRows{}, nil
}

// The finalize family reads the organization's items once and writes the
// two rows of the one github item; an ambiguous Send reports both rows.
func TestWorkItemMeasureCapabilityFinalizeWritesAndCountsOnSendAmbiguity(t *testing.T) {
	for _, sendErr := range []error{nil, errors.New("ambiguous send")} {
		conn := &capabilityFinalizeConn{capabilityRecordingConn: capabilityRecordingConn{batch: &capabilityRecordingBatch{sendErr: sendErr}}}
		executor := &WorkItemMeasureCapabilityExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
		written, err := executor.ComputeFinalizeFamily(context.Background(), Run{OrganizationID: "org-42", TargetDay: capabilityTestDay})
		if written != 2 || (err != nil) != (sendErr != nil) {
			t.Fatalf("send error %v: written=%d err=%v, want 2 rows and the send error", sendErr, written, err)
		}
		if conn.queries != 1 {
			t.Fatalf("queries = %d, want one read of the organization's items", conn.queries)
		}
	}
}

func TestWorkItemMeasureCapabilityFinalizeGuards(t *testing.T) {
	conn := &capabilityFinalizeConn{capabilityRecordingConn: capabilityRecordingConn{batch: &capabilityRecordingBatch{}}}
	executor := &WorkItemMeasureCapabilityExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	for name, run := range map[string]Run{
		"no organization": {TargetDay: capabilityTestDay},
		"no target day":   {OrganizationID: "org-42"},
	} {
		if _, err := executor.ComputeFinalizeFamily(context.Background(), run); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("%s: err = %v, want ErrInvalidState", name, err)
		}
	}
	if conn.queries != 0 || conn.prepared != 0 {
		t.Fatalf("a refused run read %d time(s) and prepared %d batch(es), want none", conn.queries, conn.prepared)
	}
	if _, err := (&WorkItemMeasureCapabilityExecutor{}).ComputeFinalizeFamily(context.Background(), Run{OrganizationID: "org-42", TargetDay: capabilityTestDay}); err == nil {
		t.Fatal("an executor without a connection computed")
	}
	if _, err := NewWorkItemMeasureCapabilityExecutor(nil); err == nil {
		t.Fatal("NewWorkItemMeasureCapabilityExecutor(nil) built an executor")
	}
}
