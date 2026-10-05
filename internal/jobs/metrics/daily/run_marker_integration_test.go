//go:build integration

package daily

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-8710: the ClickHouse run marker, on real Postgres and real ClickHouse
// (the table is created from the shipped migration file, not a copy).

// switchableMarkerWriter wraps the real writer and fails on demand, to stand
// in for a ClickHouse outage at the moment of a marker append.
type switchableMarkerWriter struct {
	inner RunMarkerWriter
	mu    sync.Mutex
	fail  bool
}

func (writer *switchableMarkerWriter) setFail(fail bool) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.fail = fail
}

func (writer *switchableMarkerWriter) AppendRunMarker(ctx context.Context, marker RunMarker) error {
	writer.mu.Lock()
	fail := writer.fail
	writer.mu.Unlock()
	if fail {
		return errors.New("injected clickhouse outage")
	}
	return writer.inner.AppendRunMarker(ctx, marker)
}

type countingMarkerObserver struct {
	mu     sync.Mutex
	counts map[string]int
}

func (observer *countingMarkerObserver) ObserveDailyMetricsRunMarker(state, outcome string) error {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.counts == nil {
		observer.counts = map[string]int{}
	}
	observer.counts[state+"/"+outcome]++
	return nil
}

func (observer *countingMarkerObserver) count(key string) int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.counts[key]
}

type markerStack struct {
	pool      *pgxpool.Pool
	store     *PostgresStore
	publisher *PostgresPublisher
	writer    *switchableMarkerWriter
	reader    *ClickHouseRunMarkerStore
	observer  *countingMarkerObserver
	rawCount  func(orgID string) int
}

func newMarkerStack(t *testing.T) *markerStack {
	t.Helper()
	pool, store, publisher := newFinalizeRedriveTestStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ddl, err := os.ReadFile("../../../chmigrate/sql/105_daily_metrics_run_marker.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range chmigrate.SplitStatements(string(ddl)) {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	reader, err := NewClickHouseRunMarkerStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	stack := &markerStack{
		pool: pool, store: store, publisher: publisher, reader: reader,
		writer:   &switchableMarkerWriter{inner: reader},
		observer: &countingMarkerObserver{},
	}
	store.SetRunMarkerWriter(stack.writer)
	store.SetRunMarkerObserver(stack.observer)
	// A clock that moves one second per read, so marker versions order the way
	// the events happened.
	var tick int64
	var tickMu sync.Mutex
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		tickMu.Lock()
		defer tickMu.Unlock()
		tick++
		return base.Add(time.Duration(tick) * time.Second)
	}
	stack.rawCount = func(orgID string) int {
		var count uint64
		if err := conn.QueryRow(context.Background(), `SELECT count() FROM daily_metrics_run_marker WHERE org_id = ?`, orgID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return int(count)
	}
	return stack
}

func (stack *markerStack) states(t *testing.T, orgID string, from, to time.Time) map[string]RunMarkerState {
	t.Helper()
	states, err := stack.reader.RunMarkerStates(context.Background(), orgID, from, to)
	if err != nil {
		t.Fatal(err)
	}
	return states
}

// seedRun inserts one run for orgID on day with one succeeded partition, ready
// to finalize.
func (stack *markerStack) seedRun(t *testing.T, runID, partitionID, orgID string, day time.Time) {
	t.Helper()
	ctx := context.Background()
	insertFinalizeTestRun(t, ctx, stack.pool, runID, orgID, day, time.Date(2026, 10, 5, 11, 0, 0, 0, time.UTC))
	insertFinalizeTestPartition(t, ctx, stack.pool, partitionID, runID, 0, "succeeded", time.Date(2026, 10, 5, 11, 0, 0, 0, time.UTC))
}

const (
	markerOrgA = "00000000-0000-4000-8000-000000008710"
	markerOrgB = "00000000-0000-4000-8000-000000008711"
)

func markerDay(day int) time.Time { return time.Date(2026, 9, day, 0, 0, 0, 0, time.UTC) }

// Completion writes the marker for that day only. A day whose run has not
// finalized, and a day with no run at all, have no row: unknown, never zero.
func TestRunMarkerCompletionWritesOnlyTheCompletedDay(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	stack.seedRun(t, "00000000-0000-4000-8000-000000087101", "00000000-0000-4000-8000-000000087102", markerOrgA, markerDay(1))
	stack.seedRun(t, "00000000-0000-4000-8000-000000087103", "00000000-0000-4000-8000-000000087104", markerOrgA, markerDay(2))
	// markerDay(3) has no run.

	processFinalizeJob(t, ctx, stack.store, "00000000-0000-4000-8000-000000087101")

	states := stack.states(t, markerOrgA, markerDay(1), markerDay(3))
	if len(states) != 1 || states[markerDay(1).Format("2006-01-02")] != RunMarkerSucceeded {
		t.Fatalf("states = %v, want only 2026-09-01 succeeded (running day and no-run day unknown)", states)
	}
	if got := stack.observer.count("succeeded/ok"); got != 1 {
		t.Fatalf("succeeded/ok count = %d, want 1", got)
	}
}

// One organization's marker is never visible under another organization.
func TestRunMarkerIsolatesOrganizations(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	stack.seedRun(t, "00000000-0000-4000-8000-000000087111", "00000000-0000-4000-8000-000000087112", markerOrgA, markerDay(1))
	stack.seedRun(t, "00000000-0000-4000-8000-000000087113", "00000000-0000-4000-8000-000000087114", markerOrgB, markerDay(2))
	processFinalizeJob(t, ctx, stack.store, "00000000-0000-4000-8000-000000087111")

	if states := stack.states(t, markerOrgB, markerDay(1), markerDay(2)); len(states) != 0 {
		t.Fatalf("org B sees %v, want nothing (org A finalized, org B did not)", states)
	}
	processFinalizeJob(t, ctx, stack.store, "00000000-0000-4000-8000-000000087113")
	a := stack.states(t, markerOrgA, markerDay(1), markerDay(2))
	b := stack.states(t, markerOrgB, markerDay(1), markerDay(2))
	if len(a) != 1 || a["2026-09-01"] != RunMarkerSucceeded || len(b) != 1 || b["2026-09-02"] != RunMarkerSucceeded {
		t.Fatalf("org A = %v, org B = %v, want each only its own day", a, b)
	}
}

// A redrive that reopens a succeeded run hides the marker; finalizing again
// restores it with a later version.
func TestRunMarkerReopenHidesTheDayUntilItFinalizesAgain(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000087121"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087122", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, runID)
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("state before reopen = %q, want succeeded", got)
	}

	outcome, err := stack.store.RedriveFinalizeForRange(
		ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-reopen-1", true, testFinalizeRedriveReason, false)
	if err != nil || len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven_reset_from_succeeded" {
		t.Fatalf("redrive outcome = %#v, err = %v, want redriven_reset_from_succeeded", outcome, err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("state after reopen = %q, want reopened", got)
	}

	processFinalizeJob(t, ctx, stack.store, runID)
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("state after the redriven finalize = %q, want succeeded again", got)
	}
}

// A reopen whose marker cannot be written is refused: the day must never keep
// reading as succeeded while its run is reopened.
func TestRunMarkerReopenIsRefusedWhenTheMarkerCannotBeWritten(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000087131"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087132", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, runID)

	stack.writer.setFail(true)
	_, err := stack.store.RedriveFinalizeForRange(
		ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-reopen-refused", true, testFinalizeRedriveReason, false)
	if err == nil {
		t.Fatal("RedriveFinalizeForRange succeeded with the marker writer down, want an error")
	}
	var status, finalization string
	if err := stack.pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" || finalization != "succeeded" {
		t.Fatalf("run = %s/%s after a refused reopen, want succeeded/succeeded (nothing changed)", status, finalization)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("marker = %q after a refused reopen, want succeeded (still true)", got)
	}
	if got := stack.observer.count("reopened/failed"); got != 1 {
		t.Fatalf("reopened/failed count = %d, want 1", got)
	}
}

// A ClickHouse failure at completion never fails the run. The day stays
// unknown, the failure is counted, and the backfill restores the row. A second
// backfill appends nothing.
func TestRunMarkerCompletionSurvivesAClickHouseFailureAndBackfillRestoresIt(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000087141"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087142", markerOrgA, markerDay(1))

	stack.writer.setFail(true)
	processFinalizeJob(t, ctx, stack.store, runID) // fails the test if CompleteFinalize errors
	var status string
	if err := stack.pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" {
		t.Fatalf("run status = %q, want succeeded (the marker never fails the run)", status)
	}
	if states := stack.states(t, markerOrgA, markerDay(1), markerDay(1)); len(states) != 0 {
		t.Fatalf("states = %v, want none (the append failed, the day is unknown)", states)
	}
	if got := stack.observer.count("succeeded/failed"); got != 1 {
		t.Fatalf("succeeded/failed count = %d, want 1", got)
	}

	stack.writer.setFail(false)
	outcome, err := stack.store.BackfillRunMarkers(ctx, stack.reader, markerOrgA, markerDay(1), markerDay(5), false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Appended != 1 || outcome.Succeeded != 1 {
		t.Fatalf("backfill outcome = %+v, want one succeeded row appended", outcome)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(5)); len(got) != 1 || got["2026-09-01"] != RunMarkerSucceeded {
		t.Fatalf("states after backfill = %v, want 2026-09-01 succeeded only (days 2-5 have no run)", got)
	}
	rows := stack.rawCount(markerOrgA)
	again, err := stack.store.BackfillRunMarkers(ctx, stack.reader, markerOrgA, markerDay(1), markerDay(5), false)
	if err != nil {
		t.Fatal(err)
	}
	if again.Appended != 0 || stack.rawCount(markerOrgA) != rows {
		t.Fatalf("second backfill = %+v, rows %d -> %d, want no new rows", again, rows, stack.rawCount(markerOrgA))
	}
}

// The backfill also corrects a stale 'succeeded' marker: when Postgres says
// the day's latest run is not succeeded, it appends 'reopened'. A day with no
// run is never given a row.
func TestRunMarkerBackfillHidesADayWhosePostgresRunIsNotSucceeded(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000087151"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087152", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, runID)
	if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_runs SET status = 'running', finalization_status = 'pending' WHERE id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
	outcome, err := stack.store.BackfillRunMarkers(ctx, stack.reader, markerOrgA, markerDay(1), markerDay(3), false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Reopened != 1 || outcome.DaysExamined != 1 {
		t.Fatalf("backfill outcome = %+v, want one reopened row over one day with a run", outcome)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(3)); len(got) != 1 || got["2026-09-01"] != RunMarkerReopened {
		t.Fatalf("states = %v, want 2026-09-01 reopened only", got)
	}
}

// The partition recompute reopens a succeeded run through its own path, so it
// must append 'reopened' too, and be refused when the append fails.
func TestRunMarkerPartitionRecomputeReopensAndIsRefusedWhenTheMarkerFails(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000087161"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087162", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, runID)
	// The recompute verb only takes a scheduled fan-out run.
	if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_runs SET generation = 'fixed-schedule:daily_metrics_fanout:2026-09-02T01:00:00Z' WHERE id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}

	stack.writer.setFail(true)
	if _, err := stack.store.RedrivePartitionsForRange(
		ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-recompute-refused", "repo_user_commit", testPartitionRecomputeReason, false); err == nil {
		t.Fatal("RedrivePartitionsForRange succeeded with the marker writer down, want an error")
	}
	var status string
	if err := stack.pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" {
		t.Fatalf("run status = %q after a refused recompute, want succeeded", status)
	}

	stack.writer.setFail(false)
	outcome, err := stack.store.RedrivePartitionsForRange(
		ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-recompute-ok", "repo_user_commit", testPartitionRecomputeReason, false)
	if err != nil || len(outcome.Days) != 1 || outcome.Days[0].Outcome != "redriven" {
		t.Fatalf("recompute outcome = %#v, err = %v, want one redriven day", outcome, err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("state after recompute = %q, want reopened", got)
	}
}
