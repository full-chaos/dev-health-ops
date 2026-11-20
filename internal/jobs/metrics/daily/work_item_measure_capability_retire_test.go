package daily

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// retireTestRows yields fixed rows; Scan sets each destination from the value
// at its position.
type retireTestRows struct {
	values [][]any
	next   int
}

func (rows *retireTestRows) Next() bool {
	if rows.next >= len(rows.values) {
		return false
	}
	rows.next++
	return true
}
func (rows *retireTestRows) Scan(dest ...any) error {
	row := rows.values[rows.next-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan: %d destination(s) for a row of %d value(s)", len(dest), len(row))
	}
	for index, value := range row {
		target := reflect.ValueOf(dest[index]).Elem()
		source := reflect.ValueOf(value)
		if !source.Type().AssignableTo(target.Type()) {
			return fmt.Errorf("scan: column %d is %s, the destination is %s", index, source.Type(), target.Type())
		}
		target.Set(source)
	}
	return nil
}
func (rows *retireTestRows) ScanStruct(any) error               { return errors.New("unused") }
func (rows *retireTestRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (rows *retireTestRows) Totals(...any) error                { return nil }
func (rows *retireTestRows) Columns() []string                  { return nil }
func (rows *retireTestRows) Close() error                       { return nil }
func (rows *retireTestRows) Err() error                         { return nil }
func (rows *retireTestRows) HasData() bool                      { return len(rows.values) > 0 }

// retireTestBatch records the rows appended to one prepared INSERT.
type retireTestBatch struct {
	query string
	rows  [][]any
}

func (batch *retireTestBatch) Append(values ...any) error {
	batch.rows = append(batch.rows, values)
	return nil
}
func (batch *retireTestBatch) Send() error                     { return nil }
func (batch *retireTestBatch) Abort() error                    { return nil }
func (batch *retireTestBatch) AppendStruct(any) error          { return errors.New("unused") }
func (batch *retireTestBatch) Column(int) chdriver.BatchColumn { return nil }
func (batch *retireTestBatch) Flush() error                    { return nil }
func (batch *retireTestBatch) IsSent() bool                    { return false }
func (batch *retireTestBatch) Rows() int                       { return len(batch.rows) }
func (batch *retireTestBatch) Columns() []column.Interface     { return nil }
func (batch *retireTestBatch) Close() error                    { return nil }

// retireTestConn answers each read by the first route whose every needle is in
// the query text, and records every query and every prepared batch.
type retireTestConn struct {
	stubDriverConn
	routes  []retireTestRoute
	queries []string
	batches []*retireTestBatch
}

type retireTestRoute struct {
	needles []string
	rows    func() [][]any
	driver  func() chdriver.Rows
	err     error
}

func (conn *retireTestConn) Query(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
	conn.queries = append(conn.queries, query)
	for _, route := range conn.routes {
		matched := true
		for _, needle := range route.needles {
			if !strings.Contains(query, needle) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if route.err != nil {
			return nil, route.err
		}
		if route.driver != nil {
			return route.driver(), nil
		}
		if route.rows == nil {
			return &retireTestRows{}, nil
		}
		return &retireTestRows{values: route.rows()}, nil
	}
	return nil, fmt.Errorf("retireTestConn: no route for query %q", query)
}

func (conn *retireTestConn) PrepareBatch(_ context.Context, query string, _ ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	batch := &retireTestBatch{query: query}
	conn.batches = append(conn.batches, batch)
	return batch, nil
}

func (conn *retireTestConn) QueryFormat(context.Context, string, string, ...any) (io.ReadCloser, error) {
	return nil, errors.New("unused")
}

func (conn *retireTestConn) appended(table string) [][]any {
	var rows [][]any
	for _, batch := range conn.batches {
		if strings.Contains(batch.query, table) {
			rows = append(rows, batch.rows...)
		}
	}
	return rows
}

// capabilityRowOf finds the appended capability row of (provider, measure).
func capabilityRowOf(rows [][]any, provider, measure string) []any {
	for _, row := range rows {
		if len(row) >= 3 && row[1] == provider && row[2] == measure {
			return row
		}
	}
	return nil
}

const capabilityRetireOrg = "org-14052"

func capabilityRetireExecutor(conn *retireTestConn, now time.Time) *WorkItemMeasureCapabilityExecutor {
	return &WorkItemMeasureCapabilityExecutor{conn: conn, nowUTC: func() time.Time { return now }}
}

// A key of the day that an earlier compute wrote and this compute does not
// produce (its provider has no item in the window any more) gets a row with
// item_count 0, strictly newer than the stored row. The keys this compute
// produces are written as computed and get no retirement row.
func TestWorkItemMeasureCapabilityRetiresALiveKeyItDidNotProduce(t *testing.T) {
	now := time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC)
	stored := now.Add(-time.Hour)
	ahead := now.Add(5 * time.Second)
	conn := &retireTestConn{routes: []retireTestRoute{
		{needles: []string{"FROM work_item_measure_capability"}, rows: func() [][]any {
			return [][]any{
				{"github", "bug_completed_ratio", stored},
				{"jira", "bug_completed_ratio", stored},
				{"jira", "story_points_completed", ahead},
			}
		}},
		{needles: []string{"work_items FINAL", "AS scopes"}, driver: func() chdriver.Rows { return &oneWorkItemScopedRow{} }},
		{needles: []string{workItemCopiesReadSQL}, driver: func() chdriver.Rows { return &oneWorkItemCopyRow{} }},
	}}
	executor := capabilityRetireExecutor(conn, now)
	written, err := executor.ComputeFinalizeFamily(context.Background(), Run{OrganizationID: capabilityRetireOrg, TargetDay: capabilityTestDay})
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	rows := conn.appended("work_item_measure_capability")
	t.Logf("written=%d appended=%v", written, rows)
	first, last := workitemmetrics.CapabilityWindow(capabilityTestDay)
	for _, retired := range []struct {
		measure string
		version time.Time
	}{
		{"bug_completed_ratio", now},
		{"story_points_completed", ahead.Add(time.Millisecond)},
	} {
		row := capabilityRowOf(rows, "jira", retired.measure)
		if row == nil {
			t.Errorf("jira %s: no retirement row; the key of the earlier compute stays live", retired.measure)
			continue
		}
		want := []any{capabilityRetireOrg, "jira", retired.measure, uint8(0), uint32(0), uint32(0), first, last, retired.version}
		if !reflect.DeepEqual(row, want) {
			t.Errorf("jira %s retirement row:\n got %#v\nwant %#v", retired.measure, row, want)
		}
	}
	for _, measure := range []string{"bug_completed_ratio", "story_points_completed"} {
		row := capabilityRowOf(rows, "github", measure)
		if row == nil || row[5] == uint32(0) {
			t.Errorf("github %s: want the computed row (item_count >= 1), got %#v", measure, row)
		}
	}
	if written != 4 {
		t.Errorf("written = %d, want 4 (2 computed + 2 retired)", written)
	}
}

// A compute whose read gives no item would retire every live key of the day.
// It first proves, by a second read of its own, that the organization has no
// work item in the window; a count above 0 retires nothing and fails the run.
func TestWorkItemMeasureCapabilityProvesNoItemBeforeItRetiresEveryKey(t *testing.T) {
	now := time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC)
	stored := now.Add(-time.Hour)
	for _, proof := range []struct {
		count       uint64
		wantRetired int
		wantErr     bool
	}{
		{count: 0, wantRetired: 2},
		{count: 3, wantErr: true},
	} {
		t.Run(fmt.Sprintf("proof_count_%d", proof.count), func(t *testing.T) {
			count := proof.count
			conn := &retireTestConn{routes: []retireTestRoute{
				{needles: []string{"FROM work_item_measure_capability"}, rows: func() [][]any {
					return [][]any{
						{"jira", "bug_completed_ratio", stored},
						{"jira", "story_points_completed", stored},
					}
				}},
				{needles: []string{"SELECT count()", "work_items FINAL"}, rows: func() [][]any { return [][]any{{count}} }},
				{needles: []string{"work_items FINAL"}},
			}}
			written, err := capabilityRetireExecutor(conn, now).ComputeFinalizeFamily(
				context.Background(), Run{OrganizationID: capabilityRetireOrg, TargetDay: capabilityTestDay})
			rows := conn.appended("work_item_measure_capability")
			proved := false
			for _, query := range conn.queries {
				if strings.Contains(query, "SELECT count()") && strings.Contains(query, "work_items FINAL") {
					proved = true
				}
			}
			t.Logf("written=%d err=%v proof_read=%t appended=%v", written, err, proved, rows)
			if !proved {
				t.Errorf("no proof read of the window's work items before the retirement decision")
			}
			if (err != nil) != proof.wantErr {
				t.Errorf("err = %v, want error %t", err, proof.wantErr)
			}
			if len(rows) != proof.wantRetired {
				t.Errorf("appended %d row(s), want %d", len(rows), proof.wantRetired)
			}
		})
	}
}

// A second compute of the same day finds no live key it did not produce and
// writes no retirement row.
func TestWorkItemMeasureCapabilityRetiresNothingWhenEveryLiveKeyIsProduced(t *testing.T) {
	now := time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC)
	conn := &retireTestConn{routes: []retireTestRoute{
		{needles: []string{"FROM work_item_measure_capability"}},
		{needles: []string{"SELECT count()", "work_items FINAL"}, rows: func() [][]any { return [][]any{{uint64(0)}} }},
		{needles: []string{"work_items FINAL"}},
	}}
	written, err := capabilityRetireExecutor(conn, now).ComputeFinalizeFamily(
		context.Background(), Run{OrganizationID: capabilityRetireOrg, TargetDay: capabilityTestDay})
	if err != nil || written != 0 || len(conn.appended("work_item_measure_capability")) != 0 {
		t.Fatalf("written=%d err=%v appended=%d, want 0 nil 0", written, err, len(conn.appended("work_item_measure_capability")))
	}
}
