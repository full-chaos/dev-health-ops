package daily

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// #2276 confirmation-pass P1 (team-lead-required live proof before any fix):
// work_item/work_item_estimate/work_item_state's ComputeFamily loops each
// call one or more Write* functions whose OWN batch.Send() branch already
// reports its true row count on an ambiguous network error (the F1 sweep),
// but the loop only added that count to the running `total` AFTER a
// confirmed success -- discarding the failing write's own truthful count a
// second time. Fixed by moving `total += written` before the error check,
// mirroring work_graph_edges_native_executor.go's established idiom. These
// tests drive the REAL ComputeFamily for all three executors, forcing the
// LAST write in each executor's sequence to fail on its own Send()
// ambiguity, so a revert of the fix would not be caught by any
// helper-only test (wrapWorkItemPartialWrite/wrapWorkItemStatePartialWrite
// direct-call tests elsewhere in this package do not reach this loop at
// all).

// oneWorkItemRow yields exactly one work item shaped to produce a nonzero
// row in every one of work_item/work_item_estimate/work_item_state's
// downstream tables: a COMPLETED item (CompletedAt set, needed for
// ComputeDailyTriplet's CycleTimes -- only completed items produce a
// CycleTimeRecord) with a StoryPoints value (drives estimate coverage) and
// a CreatedAt/CompletedAt both inside the target day's window.
type oneWorkItemRow struct {
	done bool
}

func (rows *oneWorkItemRow) Next() bool {
	if rows.done {
		return false
	}
	rows.done = true
	return true
}
func (rows *oneWorkItemRow) Err() error   { return nil }
func (rows *oneWorkItemRow) Close() error { return nil }
func (rows *oneWorkItemRow) Scan(dest ...any) error {
	if len(dest) != 14 {
		return errors.New("unexpected work_item metrics column count")
	}
	*(dest[0].(*string)) = "wi-1"
	*(dest[1].(*string)) = "github"
	*(dest[2].(*string)) = "done"
	*(dest[3].(*string)) = ""
	*(dest[4].(*string)) = "proj-1"
	*(dest[5].(*string)) = ""
	*(dest[6].(*string)) = "Project One"
	*(dest[7].(*time.Time)) = time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	completedAt := time.Date(2026, 9, 3, 14, 0, 0, 0, time.UTC)
	*(dest[8].(**time.Time)) = &completedAt
	*(dest[9].(*string)) = "issue"
	*(dest[10].(*[]string)) = []string{"alice"}
	startedAt := time.Date(2026, 9, 3, 9, 0, 0, 0, time.UTC)
	*(dest[11].(**time.Time)) = &startedAt
	*(dest[12].(**time.Time)) = &completedAt
	storyPoints := 3.0
	*(dest[13].(**float64)) = &storyPoints
	return nil
}

// oneWorkItemTransitionRow yields exactly one state transition for "wi-1",
// occurring inside its created/completed window -- work_item_state's
// computeWorkItemStateDurationsForRepo skips any item with zero transitions
// entirely (segmentWorkItemStatuses needs at least one to produce a
// segment), so this is required for that executor's write to be reached at
// all.
type oneWorkItemTransitionRow struct {
	done bool
}

func (rows *oneWorkItemTransitionRow) Next() bool {
	if rows.done {
		return false
	}
	rows.done = true
	return true
}
func (rows *oneWorkItemTransitionRow) Err() error   { return nil }
func (rows *oneWorkItemTransitionRow) Close() error { return nil }
func (rows *oneWorkItemTransitionRow) Scan(dest ...any) error {
	if len(dest) != 4 {
		return errors.New("unexpected work_item transition column count")
	}
	*(dest[0].(*string)) = "wi-1"
	*(dest[1].(*time.Time)) = time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
	*(dest[2].(*string)) = ""
	*(dest[3].(*string)) = "in_progress"
	return nil
}
func (*oneWorkItemTransitionRow) HasData() bool                      { return true }
func (*oneWorkItemTransitionRow) Columns() []string                  { return nil }
func (*oneWorkItemTransitionRow) ColumnTypes() []chdriver.ColumnType { return nil }
func (*oneWorkItemTransitionRow) Totals(...any) error                { return errors.New("no totals") }
func (*oneWorkItemTransitionRow) ScanStruct(any) error               { return errors.New("no rows") }

// emptyWorkItemRelatedRows is a real driver.Rows yielding nothing -- used for
// the state-transitions and team-attributions queries, both legitimately
// empty for this fixture.
type emptyWorkItemRelatedRows struct{}

func (*emptyWorkItemRelatedRows) Next() bool                         { return false }
func (*emptyWorkItemRelatedRows) Scan(...any) error                  { return errors.New("no rows") }
func (*emptyWorkItemRelatedRows) Err() error                         { return nil }
func (*emptyWorkItemRelatedRows) Close() error                       { return nil }
func (*emptyWorkItemRelatedRows) HasData() bool                      { return false }
func (*emptyWorkItemRelatedRows) Columns() []string                  { return nil }
func (*emptyWorkItemRelatedRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (*emptyWorkItemRelatedRows) Totals(...any) error                { return errors.New("no totals") }
func (*emptyWorkItemRelatedRows) ScanStruct(any) error               { return errors.New("no rows") }

func (*oneWorkItemRow) HasData() bool                      { return true }
func (*oneWorkItemRow) Columns() []string                  { return nil }
func (*oneWorkItemRow) ColumnTypes() []chdriver.ColumnType { return nil }
func (*oneWorkItemRow) Totals(...any) error                { return errors.New("no totals") }
func (*oneWorkItemRow) ScanStruct(any) error               { return errors.New("no rows") }

// workItemSendFailingConn dispatches Query() by table substring (one real
// work item for "work_items FINAL", empty for anything else) and
// PrepareBatch() by table substring, failing Send() only for failTable.
type workItemSendFailingConn struct {
	stubDriverConn
	failTable string
	targets   []string
}

func (conn *workItemSendFailingConn) Query(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
	switch {
	// LoadWorkItemMetricsWorkItems (14 columns, used by work_item/
	// work_item_estimate) and LoadWorkItemStateWorkItems (9 columns, used
	// by work_item_state) both query "work_items FINAL" with an almost
	// identical WHERE clause -- distinguished here by "assignees", which
	// only the 14-column query selects.
	case strings.Contains(query, "work_items FINAL") && strings.Contains(query, "assignees"):
		return &oneWorkItemRow{}, nil
	case strings.Contains(query, "work_items FINAL"):
		return &oneWorkItemStateWorkItemRow{}, nil
	case strings.Contains(query, "work_item_transitions"):
		return &oneWorkItemTransitionRow{}, nil
	default:
		return &emptyWorkItemRelatedRows{}, nil
	}
}

// oneWorkItemStateWorkItemRow mirrors oneWorkItemRow's item ("wi-1",
// completed inside the target day) for LoadWorkItemStateWorkItems' narrower
// 9-column SELECT.
type oneWorkItemStateWorkItemRow struct {
	done bool
}

func (rows *oneWorkItemStateWorkItemRow) Next() bool {
	if rows.done {
		return false
	}
	rows.done = true
	return true
}
func (rows *oneWorkItemStateWorkItemRow) Err() error   { return nil }
func (rows *oneWorkItemStateWorkItemRow) Close() error { return nil }
func (rows *oneWorkItemStateWorkItemRow) Scan(dest ...any) error {
	if len(dest) != 9 {
		return errors.New("unexpected work_item_state work item column count")
	}
	*(dest[0].(*string)) = "wi-1"
	*(dest[1].(*string)) = "github"
	*(dest[2].(*string)) = "done"
	*(dest[3].(*string)) = ""
	*(dest[4].(*string)) = "proj-1"
	*(dest[5].(*string)) = ""
	*(dest[6].(*string)) = "Project One"
	*(dest[7].(*time.Time)) = time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
	completedAt := time.Date(2026, 9, 3, 14, 0, 0, 0, time.UTC)
	*(dest[8].(**time.Time)) = &completedAt
	return nil
}
func (*oneWorkItemStateWorkItemRow) HasData() bool                      { return true }
func (*oneWorkItemStateWorkItemRow) Columns() []string                  { return nil }
func (*oneWorkItemStateWorkItemRow) ColumnTypes() []chdriver.ColumnType { return nil }
func (*oneWorkItemStateWorkItemRow) Totals(...any) error                { return errors.New("no totals") }
func (*oneWorkItemStateWorkItemRow) ScanStruct(any) error               { return errors.New("no rows") }

func (conn *workItemSendFailingConn) PrepareBatch(_ context.Context, query string, _ ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	table := "unknown"
	for _, candidate := range []string{
		"work_item_metrics_daily", "work_item_user_metrics_daily", "work_item_cycle_times",
		"estimate_coverage_metrics_daily", "work_item_state_durations_daily",
	} {
		if strings.Contains(query, candidate) {
			table = candidate
			break
		}
	}
	conn.targets = append(conn.targets, table)
	return &workItemSendFailingBatch{fail: table == conn.failTable}, nil
}

type workItemSendFailingBatch struct {
	fail     bool
	appended int
	sent     bool
}

func (batch *workItemSendFailingBatch) Append(...any) error { batch.appended++; return nil }
func (batch *workItemSendFailingBatch) Send() error {
	if batch.fail {
		return errors.New("simulated ambiguous ClickHouse Send() failure")
	}
	batch.sent = true
	return nil
}
func (batch *workItemSendFailingBatch) Abort() error                    { return nil }
func (batch *workItemSendFailingBatch) AppendStruct(any) error          { return errors.New("unused") }
func (batch *workItemSendFailingBatch) Column(int) chdriver.BatchColumn { return nil }
func (batch *workItemSendFailingBatch) Flush() error                    { return nil }
func (batch *workItemSendFailingBatch) IsSent() bool                    { return batch.sent }
func (batch *workItemSendFailingBatch) Rows() int                       { return batch.appended }
func (batch *workItemSendFailingBatch) Columns() []column.Interface     { return nil }
func (batch *workItemSendFailingBatch) Close() error                    { return nil }

var workItemFamilyTestRepoID = uuid.MustParse("44444444-4444-4444-4444-444444444444")

func TestWorkItemComputeFamilyReportsTheFailingWritesOwnCountOnSendAmbiguity(t *testing.T) {
	conn := &workItemSendFailingConn{failTable: "work_item_cycle_times"}
	executor := &WorkItemExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(workItemFamilyTestRepoID.String())}}

	written, err := executor.ComputeFamily(context.Background(), run, partition)
	if err == nil {
		t.Fatal("ComputeFamily err = nil, want the forced work_item_cycle_times Send() failure to surface")
	}
	// work_item_metrics_daily (1 row) + work_item_user_metrics_daily (1 row,
	// one assignee) both land (total=2 after them), then
	// work_item_cycle_times' own Send() ambiguity must ALSO contribute its
	// own attempted row (1 completed item = 1 CycleTimeRecord) rather than
	// being discarded as 0 -- exact equality, not a loose lower bound: a
	// weak `>= 2` assertion does NOT discriminate the bug this test exists
	// to catch, since the OLD (broken) code also returns exactly 2 here
	// (the two prior writes' total, with cycle_times' own count silently
	// dropped) -- verified this exact assertion goes red on the mutant.
	if written != 3 {
		t.Fatalf("written=%d, want 3 (2 prior rows + 1 from this failing write's own count) -- "+
			"got a value matching what the OLD (broken) code would report, meaning the fix did "+
			"not take effect", written)
	}
	if len(conn.targets) != 3 || conn.targets[2] != "work_item_cycle_times" {
		t.Fatalf("write targets=%v, want exactly 3 ending with work_item_cycle_times", conn.targets)
	}
}

func TestWorkItemEstimateComputeFamilyReportsTheFailingWritesOwnCountOnSendAmbiguity(t *testing.T) {
	conn := &workItemSendFailingConn{failTable: "estimate_coverage_metrics_daily"}
	executor := &WorkItemEstimateExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(workItemFamilyTestRepoID.String())}}

	written, err := executor.ComputeFamily(context.Background(), run, partition)
	if err == nil {
		t.Fatal("ComputeFamily err = nil, want the forced estimate_coverage_metrics_daily Send() failure to surface")
	}
	if written != 1 {
		t.Fatalf("written=%d, want 1 -- the ONE row this ambiguous Send() failure may have already "+
			"landed (this is the ONLY write in this executor's loop, so `total` before this write "+
			"is always 0 -- reporting 0 here is exactly the bug this pass exists to catch)", written)
	}
}

func TestWorkItemStateComputeFamilyReportsTheFailingWritesOwnCountOnSendAmbiguity(t *testing.T) {
	conn := &workItemSendFailingConn{failTable: "work_item_state_durations_daily"}
	executor := &WorkItemStateExecutor{conn: conn, nowUTC: func() time.Time { return time.Unix(0, 0).UTC() }}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(workItemFamilyTestRepoID.String())}}

	written, err := executor.ComputeFamily(context.Background(), run, partition)
	if err == nil {
		t.Fatal("ComputeFamily err = nil, want the forced work_item_state_durations_daily Send() failure to surface")
	}
	// The fixture's one transition (""->"in_progress" at 10:00, between the
	// item's 08:00 created_at and 14:00 completed_at) splits into TWO status
	// segments -- one row per segment -- so this ambiguous Send() failure's
	// own true count is 2, not 1. Verified against the real
	// segmentWorkItemStatuses output, not assumed.
	if written != 2 {
		t.Fatalf("written=%d, want 2 -- the two segment rows this ambiguous Send() failure may have "+
			"already landed (this is the ONLY write in this executor's loop) -- reporting 0 here is "+
			"exactly the bug this pass exists to catch", written)
	}
}
