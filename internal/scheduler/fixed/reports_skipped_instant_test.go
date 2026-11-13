package fixed

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const skipTestReportID = "00000000-0000-4000-8000-0000000000a1"

type skipTestWindow struct {
	base         time.Time
	nextDueAt    time.Time
	materialized bool
}

// skipTestTx answers the report producer's statements from one in-memory
// window and records the cron instants the producer claims.
type skipTestTx struct {
	pgx.Tx
	window  skipTestWindow
	claimed []time.Time
}

type skipTestRow struct {
	values []any
	err    error
}

func skipTestAssign(dest any, value any) {
	target := reflect.ValueOf(dest).Elem()
	if value == nil {
		target.Set(reflect.Zero(target.Type()))
		return
	}
	source := reflect.ValueOf(value)
	if target.Kind() == reflect.Ptr && source.Kind() != reflect.Ptr {
		pointer := reflect.New(target.Type().Elem())
		pointer.Elem().Set(source)
		target.Set(pointer)
		return
	}
	target.Set(source)
}

func (row skipTestRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	for index := range dest {
		skipTestAssign(dest[index], row.values[index])
	}
	return nil
}

type skipTestRows struct {
	pgx.Rows
	rows [][]any
	next int
}

func (rows *skipTestRows) Next() bool { rows.next++; return rows.next <= len(rows.rows) }
func (rows *skipTestRows) Close()     {}
func (rows *skipTestRows) Err() error { return nil }
func (rows *skipTestRows) Scan(dest ...any) error {
	return skipTestRow{values: rows.rows[rows.next-1]}.Scan(dest...)
}

func (tx *skipTestTx) Query(_ context.Context, statement string, _ ...any) (pgx.Rows, error) {
	if statement != dueScheduledReportsSQL {
		panic("unexpected Query: " + statement)
	}
	next := tx.window.nextDueAt
	return &skipTestRows{rows: [][]any{{
		"job-1", "default", "0 * * * *", "UTC", skipTestReportID,
		tx.window.base, &next, tx.window.materialized,
	}}}, nil
}

func (tx *skipTestTx) QueryRow(_ context.Context, statement string, args ...any) pgx.Row {
	switch statement {
	case ambiguousReportSchedulesSQL:
		return skipTestRow{err: pgx.ErrNoRows}
	case insertScheduledReportOccurrenceSQL:
		tx.claimed = append(tx.claimed, args[5].(time.Time).UTC())
		return skipTestRow{values: []any{args[0]}}
	case replayedReportRunSQL:
		return skipTestRow{values: []any{"run-open", pendingReportRunStatus, "sent", false}}
	}
	panic("unexpected QueryRow: " + statement)
}

func (tx *skipTestTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func skipTestProduce(t *testing.T, tx *skipTestTx, window skipTestWindow, dueAt time.Time) {
	t.Helper()
	producer, err := NewScheduledReportsProducer(schedulersync.NextOccurrence)
	if err != nil {
		t.Fatal(err)
	}
	schedule := scheduleByID(t, scheduledReportsScheduleID)
	occurrence := NewOccurrence(schedule, dueAt, dueAt.Add(30*time.Second))
	tx.window = window
	if _, err := producer.Produce(context.Background(), tx, schedule, occurrence); err != nil {
		t.Fatal(err)
	}
}

func skipTestCaptureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func skipTestInstants(instants []time.Time) string {
	names := make([]string, 0, len(instants))
	for _, instant := range instants {
		names = append(names, instant.Format(time.RFC3339))
	}
	return strings.Join(names, ",")
}

// Red cell. The 22:00 run of an hourly report is still open at 23:00 and ends
// at 23:12. Base is the last completed run, so the 23:00 sweep only replays the
// 22:00 occurrence, and the 00:00 sweep rebases onto 23:12. 23:00 never reaches
// the occurrence table. The skip is kept as policy; this cell requires that
// the skipped instant is named in a WARN line. Today nothing is logged.
func TestReportRunSpanningACronInstantNamesTheSkippedInstant(t *testing.T) {
	logs := skipTestCaptureLogs(t)
	tx := &skipTestTx{}
	skipTestProduce(t, tx, skipTestWindow{
		base:         time.Date(2026, 8, 19, 21, 20, 0, 0, time.UTC),
		nextDueAt:    time.Date(2026, 8, 19, 23, 0, 0, 0, time.UTC),
		materialized: true,
	}, time.Date(2026, 8, 19, 23, 0, 0, 0, time.UTC))
	skipTestProduce(t, tx, skipTestWindow{
		base:      time.Date(2026, 8, 19, 23, 12, 0, 0, time.UTC),
		nextDueAt: time.Date(2026, 8, 19, 23, 0, 0, 0, time.UTC),
	}, time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))

	if got := skipTestInstants(tx.claimed); got != "2026-08-20T00:00:00Z" {
		t.Fatalf("claimed instants = %s, want only 00:00 (23:00 skipped)", got)
	}
	if !strings.Contains(logs.String(), "2026-08-19T23:00:00Z") {
		t.Errorf("the skipped cron instant 2026-08-19T23:00:00Z is not named in any WARN line; logs = %q", logs.String())
	}
}

// Control cell. The 22:00 run ended at 22:20, before the next instant, so the
// 23:00 sweep claims 23:00 and nothing is skipped or logged. Passes on main and
// must keep passing after the fix.
func TestReportRunEndingBeforeTheNextInstantClaimsIt(t *testing.T) {
	logs := skipTestCaptureLogs(t)
	tx := &skipTestTx{}
	skipTestProduce(t, tx, skipTestWindow{
		base:      time.Date(2026, 8, 19, 22, 20, 0, 0, time.UTC),
		nextDueAt: time.Date(2026, 8, 19, 23, 0, 0, 0, time.UTC),
	}, time.Date(2026, 8, 19, 23, 0, 0, 0, time.UTC))

	if got := skipTestInstants(tx.claimed); got != "2026-08-19T23:00:00Z" {
		t.Fatalf("claimed instants = %s, want 23:00", got)
	}
	if logs.Len() != 0 {
		t.Errorf("no instant was skipped, but a WARN line was written: %q", logs.String())
	}
}
