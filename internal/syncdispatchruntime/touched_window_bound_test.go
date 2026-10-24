package syncdispatchruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type windowFakeRow struct{ value any }

func (row windowFakeRow) Err() error           { return nil }
func (row windowFakeRow) ScanStruct(any) error { return nil }
func (row windowFakeRow) Scan(dest ...any) error {
	switch target := dest[0].(type) {
	case *int64:
		*target = row.value.(int64)
	case *uint64:
		*target = row.value.(uint64)
	}
	return nil
}

type windowFakeConn struct {
	driver.Conn
	repos     uint64
	execCount int
	execDays  int
}

func (conn *windowFakeConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	if strings.Contains(query, "uniqExact(repo_id)") {
		return windowFakeRow{value: conn.repos}
	}
	if strings.Contains(query, "now64") {
		return windowFakeRow{value: int64(1_700_000_000_000)}
	}
	return windowFakeRow{value: uint64(0)}
}

func (conn *windowFakeConn) Exec(_ context.Context, query string, args ...any) error {
	conn.execCount++
	if strings.Contains(query, "arrayJoin(arrayMap(value -> toDate(value), ?)) AS day, repo_id") {
		conn.execDays += len(args[1].([]string))
	}
	return nil
}

func recordWindowForTest(t *testing.T, repos uint64, days int) (TouchedWindowReport, *windowFakeConn) {
	t.Helper()
	conn := &windowFakeConn{repos: repos}
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	window := unitWindowTestDays(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), days)
	var report TouchedWindowReport
	ctx := WithTouchedWindowSink(context.Background(), &report)
	if _, err := store.RecordTouched(ctx, "org", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), window); err != nil {
		t.Fatal(err)
	}
	return report, conn
}

// A 366-day backfill of 1000 repositories marks 366,000 keys per record: the
// bound. One more repository is over it, and every day is still written.
func TestTouchedWindowBoundIs366DaysTimes1000Repositories(t *testing.T) {
	if TouchedWindowKeysBound != 366000 {
		t.Fatalf("bound = %d, want 366000", TouchedWindowKeysBound)
	}
	report, conn := recordWindowForTest(t, 1000, 366)
	if report.Keys != 366000 || report.Days != 366 || report.Repos != 1000 || report.OverBound() {
		t.Fatalf("report = %+v, want 366 x 1000 = 366000 within the bound", report)
	}
	if conn.execDays != 366 {
		t.Fatalf("days written = %d, want 366", conn.execDays)
	}
	over, conn := recordWindowForTest(t, 1001, 366)
	if over.Keys != 366*1001 || !over.OverBound() {
		t.Fatalf("report = %+v, want over the bound", over)
	}
	if conn.execDays != 366 {
		t.Fatalf("days written over the bound = %d, want all 366 (no mark is dropped)", conn.execDays)
	}
}

// A retry appends the same window again: the record states the same cost each
// time and writes every day each time (one extra recompute, no skipped mark).
func TestTouchedWindowRetryWritesTheSameMarksAgain(t *testing.T) {
	first, firstConn := recordWindowForTest(t, 3, 1097)
	second, secondConn := recordWindowForTest(t, 3, 1097)
	if first != second || first.Keys != 3291 {
		t.Fatalf("reports = %+v and %+v, want equal with 1097 x 3 = 3291 keys", first, second)
	}
	if firstConn.execDays != 1097 || secondConn.execDays != 1097 {
		t.Fatalf("days written = %d and %d, want 1097 each", firstConn.execDays, secondConn.execDays)
	}
}
