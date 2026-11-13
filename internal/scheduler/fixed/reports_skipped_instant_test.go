package fixed

import (
	"context"
	"errors"
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
	// cron defaults to hourly.
	cron string
	// previousCreatedAt is when the report's newest earlier occurrence was
	// written; nil for a report that never had one.
	previousCreatedAt *time.Time
}

func skipTestAt(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed.UTC()
}

func skipTestPtr(value string) *time.Time {
	parsed := skipTestAt(value)
	return &parsed
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
	cron := tx.window.cron
	if cron == "" {
		cron = "0 * * * *"
	}
	return &skipTestRows{rows: [][]any{{
		"job-1", "default", cron, "UTC", skipTestReportID,
		tx.window.base, &next, tx.window.materialized, tx.window.previousCreatedAt,
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

func skipTestProduce(t *testing.T, tx *skipTestTx, window skipTestWindow, dueAt time.Time) Outcome {
	t.Helper()
	return skipTestProduceWith(t, schedulersync.NextOccurrence, tx, window, dueAt)
}

func skipTestProduceWith(
	t *testing.T,
	evaluator ScheduledReportCronEvaluator,
	tx *skipTestTx,
	window skipTestWindow,
	dueAt time.Time,
) Outcome {
	t.Helper()
	producer, err := NewScheduledReportsProducer(evaluator)
	if err != nil {
		t.Fatal(err)
	}
	schedule := scheduleByID(t, scheduledReportsScheduleID)
	occurrence := NewOccurrence(schedule, dueAt, dueAt.Add(30*time.Second))
	tx.window = window
	outcome, err := producer.Produce(context.Background(), tx, schedule, occurrence)
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func skipTestNamed(unminted []UnmintedInstants) string {
	var names []string
	for _, entry := range unminted {
		names = append(names, skipTestInstants(entry.Named))
	}
	return strings.Join(names, ";")
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
// the occurrence table. The skip is kept as policy; this cell requires that the
// outcome names the skipped instant for the loop to log and count.
func TestReportRunSpanningACronInstantNamesTheSkippedInstant(t *testing.T) {
	tx := &skipTestTx{}
	replay := skipTestProduce(t, tx, skipTestWindow{
		base:              skipTestAt("2026-08-19T21:20:00Z"),
		nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
		materialized:      true,
		previousCreatedAt: skipTestPtr("2026-08-19T22:00:30Z"),
	}, skipTestAt("2026-08-19T23:00:00Z"))
	if len(replay.Unminted) != 0 {
		t.Fatalf("a replay named skipped instants: %v", replay.Unminted)
	}
	outcome := skipTestProduce(t, tx, skipTestWindow{
		base:              skipTestAt("2026-08-19T23:12:00Z"),
		nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
		previousCreatedAt: skipTestPtr("2026-08-19T22:00:30Z"),
	}, skipTestAt("2026-08-20T00:00:00Z"))

	if got := skipTestInstants(tx.claimed); got != "2026-08-20T00:00:00Z" {
		t.Fatalf("claimed instants = %s, want only 00:00 (23:00 skipped)", got)
	}
	if len(outcome.Unminted) != 1 || outcome.Unminted[0].Count != 1 ||
		skipTestNamed(outcome.Unminted) != "2026-08-19T23:00:00Z" {
		t.Fatalf("the skipped cron instant 2026-08-19T23:00:00Z is not named; unminted = %+v", outcome.Unminted)
	}
	if outcome.Unminted[0].ReportID != skipTestReportID ||
		!outcome.Unminted[0].MintedFor.Equal(skipTestAt("2026-08-20T00:00:00Z")) {
		t.Errorf("unminted entry names the wrong report or minted instant: %+v", outcome.Unminted[0])
	}
}

// Control cell. The 22:00 run ended at 22:20, before the next instant, so the
// 23:00 sweep claims 23:00 and nothing is skipped. Passes on main and must keep
// passing after the fix.
func TestReportRunEndingBeforeTheNextInstantClaimsIt(t *testing.T) {
	tx := &skipTestTx{}
	outcome := skipTestProduce(t, tx, skipTestWindow{
		base:              skipTestAt("2026-08-19T22:20:00Z"),
		nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
		previousCreatedAt: skipTestPtr("2026-08-19T22:00:30Z"),
	}, skipTestAt("2026-08-19T23:00:00Z"))

	if got := skipTestInstants(tx.claimed); got != "2026-08-19T23:00:00Z" {
		t.Fatalf("claimed instants = %s, want 23:00", got)
	}
	if len(outcome.Unminted) != 0 || len(outcome.UnmintedCountFailed) != 0 {
		t.Errorf("no instant was skipped, but the outcome reports %+v / %+v", outcome.Unminted, outcome.UnmintedCountFailed)
	}
}

// C1 (i). A report that never had an occurrence has no window to count in.
func TestReportFirstOccurrenceCountsNoSkippedInstants(t *testing.T) {
	tx := &skipTestTx{}
	outcome := skipTestProduce(t, tx, skipTestWindow{
		base:      skipTestAt("2026-08-19T00:00:00Z"),
		nextDueAt: skipTestAt("2026-08-19T01:00:00Z"),
	}, skipTestAt("2026-08-19T05:00:00Z"))
	if got := skipTestInstants(tx.claimed); got != "2026-08-19T01:00:00Z" {
		t.Fatalf("claimed instants = %s, want 01:00", got)
	}
	if len(outcome.Unminted) != 0 || len(outcome.UnmintedCountFailed) != 0 {
		t.Errorf("a first occurrence counted skipped instants: %+v", outcome.Unminted)
	}
}

// C1 (ii). The report was paused for a week and enabled again. The first sweep
// claims the oldest owed instant. Nothing in the pause counts, on that sweep or
// on the one after that run ends. The data holds no pause stamp; the window
// starts at the previous occurrence's creation.
func TestReportPausedThenEnabledCountsNoPauseSpan(t *testing.T) {
	tx := &skipTestTx{}
	first := skipTestProduce(t, tx, skipTestWindow{
		base:              skipTestAt("2026-08-19T22:10:00Z"),
		nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
		previousCreatedAt: skipTestPtr("2026-08-19T22:00:30Z"),
	}, skipTestAt("2026-08-26T10:00:00Z"))
	second := skipTestProduce(t, tx, skipTestWindow{
		base:              skipTestAt("2026-08-26T10:12:00Z"),
		nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
		previousCreatedAt: skipTestPtr("2026-08-26T10:00:30Z"),
	}, skipTestAt("2026-08-26T11:00:00Z"))
	if got := skipTestInstants(tx.claimed); got != "2026-08-19T23:00:00Z,2026-08-26T11:00:00Z" {
		t.Fatalf("claimed instants = %s", got)
	}
	if len(first.Unminted)+len(second.Unminted) != 0 {
		t.Errorf("the pause span was counted as skips: %+v / %+v", first.Unminted, second.Unminted)
	}
}

// C1 (iii). A daily report becomes hourly days after its last run. The first
// sweep claims the oldest instant of the new cron after Base. Days of the old
// cadence are not counted.
func TestReportCronChangeCountsNoInstantBeforeTheChange(t *testing.T) {
	tx := &skipTestTx{}
	outcome := skipTestProduce(t, tx, skipTestWindow{
		base:              skipTestAt("2026-08-19T00:10:00Z"),
		nextDueAt:         skipTestAt("2026-08-22T10:00:00Z"),
		previousCreatedAt: skipTestPtr("2026-08-19T00:00:30Z"),
	}, skipTestAt("2026-08-22T10:00:00Z"))
	if got := skipTestInstants(tx.claimed); got != "2026-08-19T01:00:00Z" {
		t.Fatalf("claimed instants = %s, want 01:00 of the last run day", got)
	}
	if len(outcome.Unminted) != 0 {
		t.Errorf("instants before the cron change were counted: %+v", outcome.Unminted)
	}
}

// A gap of four instants names the first and the last and counts four.
func TestReportSkippedInstantsAboveTheNameLimitNameFirstAndLast(t *testing.T) {
	tx := &skipTestTx{}
	outcome := skipTestProduce(t, tx, skipTestWindow{
		base:              skipTestAt("2026-08-20T02:10:00Z"),
		nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
		previousCreatedAt: skipTestPtr("2026-08-19T22:00:30Z"),
	}, skipTestAt("2026-08-20T03:00:00Z"))
	if len(outcome.Unminted) != 1 || outcome.Unminted[0].Count != 4 || outcome.Unminted[0].Capped {
		t.Fatalf("unminted = %+v, want one entry of count 4", outcome.Unminted)
	}
	if got := skipTestNamed(outcome.Unminted); got != "2026-08-19T23:00:00Z,2026-08-20T02:00:00Z" {
		t.Errorf("named = %s, want the first and the last instant", got)
	}
}

// C2. The scan stops at its cap. With cap+1 instants owed the entry counts the
// cap, is marked capped, and still names the first and the last scanned.
func TestReportSkippedInstantScanStopsAtItsCap(t *testing.T) {
	tx := &skipTestTx{}
	outcome := skipTestProduce(t, tx, skipTestWindow{
		cron:              "* * * * *",
		base:              skipTestAt("2026-08-19T17:05:30Z"),
		nextDueAt:         skipTestAt("2026-08-19T17:00:00Z"),
		previousCreatedAt: skipTestPtr("2026-08-19T00:00:30Z"),
	}, skipTestAt("2026-08-19T17:10:00Z"))
	if got := skipTestInstants(tx.claimed); got != "2026-08-19T17:06:00Z" {
		t.Fatalf("claimed instants = %s, want 17:06", got)
	}
	if len(outcome.Unminted) != 1 || outcome.Unminted[0].Count != unmintedScanLimit || !outcome.Unminted[0].Capped {
		t.Fatalf("unminted = %+v, want count %d and capped", outcome.Unminted, unmintedScanLimit)
	}
	if got := skipTestNamed(outcome.Unminted); got != "2026-08-19T00:01:00Z,2026-08-19T17:04:00Z" {
		t.Errorf("named = %s, want the first and the last scanned instant", got)
	}
}

// C3. A count that fails never rolls back the claim. The occurrence is claimed
// and handed off, and the outcome carries the failure for the loop to log.
func TestReportUnmintedCountFailureKeepsTheMint(t *testing.T) {
	previousCreated := skipTestAt("2026-08-19T22:00:30Z")
	evaluator := func(expression string, base time.Time, timezone string) (time.Time, bool, error) {
		if base.Equal(previousCreated) {
			return time.Time{}, false, errors.New("count scan failed")
		}
		return schedulersync.NextOccurrence(expression, base, timezone)
	}
	tx := &skipTestTx{}
	outcome := skipTestProduceWith(t, evaluator, tx, skipTestWindow{
		base:              skipTestAt("2026-08-19T23:12:00Z"),
		nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
		previousCreatedAt: &previousCreated,
	}, skipTestAt("2026-08-20T00:00:00Z"))
	if got := skipTestInstants(tx.claimed); got != "2026-08-20T00:00:00Z" || len(outcome.Requests) != 1 {
		t.Fatalf("claimed = %s, requests = %d; a failed count must keep the mint", got, len(outcome.Requests))
	}
	if len(outcome.UnmintedCountFailed) != 1 || outcome.UnmintedCountFailed[0].ReportID != skipTestReportID ||
		len(outcome.Unminted) != 0 {
		t.Errorf("count failure not reported: %+v / %+v", outcome.UnmintedCountFailed, outcome.Unminted)
	}
}

// C5. An on-demand run that ends at 23:12 moves Base the same way a scheduled
// run does. The instant it passed is counted once: by the sweep that mints the
// next occurrence, not by the replay sweep after it, and not by the sweep after
// that.
func TestReportInstantPassedByAnOnDemandRunIsCountedOnce(t *testing.T) {
	tx := &skipTestTx{}
	var counted []UnmintedInstants
	for _, step := range []struct {
		dueAt  string
		window skipTestWindow
	}{
		{"2026-08-20T00:00:00Z", skipTestWindow{
			base:              skipTestAt("2026-08-19T23:12:00Z"),
			nextDueAt:         skipTestAt("2026-08-19T23:00:00Z"),
			previousCreatedAt: skipTestPtr("2026-08-19T22:00:30Z"),
		}},
		{"2026-08-20T00:05:00Z", skipTestWindow{
			base:              skipTestAt("2026-08-19T23:12:00Z"),
			nextDueAt:         skipTestAt("2026-08-20T01:00:00Z"),
			materialized:      true,
			previousCreatedAt: skipTestPtr("2026-08-20T00:00:30Z"),
		}},
		{"2026-08-20T01:00:00Z", skipTestWindow{
			base:              skipTestAt("2026-08-20T00:12:00Z"),
			nextDueAt:         skipTestAt("2026-08-20T00:00:00Z"),
			previousCreatedAt: skipTestPtr("2026-08-20T00:00:30Z"),
		}},
	} {
		counted = append(counted, skipTestProduce(t, tx, step.window, skipTestAt(step.dueAt)).Unminted...)
	}
	if len(counted) != 1 || counted[0].Count != 1 || skipTestNamed(counted) != "2026-08-19T23:00:00Z" {
		t.Errorf("the instant must be counted once across three sweeps: %+v", counted)
	}
}
