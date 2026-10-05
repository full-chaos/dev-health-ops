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
	rawExec   func(statement string) error
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
	store.SetRunMarkerReader(reader)
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
	stack.rawExec = func(statement string) error { return conn.Exec(context.Background(), statement) }
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
	observed, err := stack.reader.RunMarkerStates(context.Background(), orgID, from, to)
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]RunMarkerState, len(observed))
	for day, observation := range observed {
		states[day] = observation.State
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
	// The run computes the whole organization (what StartScheduledFanoutRunTx
	// and a repository-list-free StartRunTx record).
	if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_runs SET full_org = true WHERE id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
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
	outcome, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(5), false)
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
	again, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(5), false)
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
	if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_runs SET status = 'running', finalization_status = 'pending', finalized_at = NULL, updated_at = '2020-01-01T00:00:00Z' WHERE id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
	outcome, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(3), false)
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

// startFullOrgRun creates a deferred-discovery run (no repository list), the
// shape of the post-sync run, and returns its id.
func (stack *markerStack) startRun(t *testing.T, orgID string, day time.Time, generation string, repos []RepositoryID) string {
	t.Helper()
	ctx := context.Background()
	tx, err := stack.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := stack.store.StartRunTx(ctx, tx, StartRunRequest{
		OrganizationID: orgID, TargetDay: day, Generation: generation, RepositoryIDs: repos,
	}, stack.publisher)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return run.ID
}

// F1 (vet 2): a run started with an explicit repository list computes only
// those repositories. It must never certify the org-day, live or by backfill.
func TestRunMarkerPartialScopeRunNeverCertifiesTheDay(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	runID := stack.startRun(t, markerOrgA, markerDay(1),
		"ext-recompute:00000000-0000-4000-8000-0000000a7609", []RepositoryID{"00000000-0000-4000-8000-0000000a7601"})
	if _, err := stack.store.ClaimDispatch(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_partitions SET status = 'succeeded' WHERE run_id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
	processFinalizeJob(t, ctx, stack.store, runID)

	var status string
	if err := stack.pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&status); err != nil || status != "succeeded" {
		t.Fatalf("partial run status = %q, err = %v, want succeeded (the run itself finished)", status, err)
	}
	if states := stack.states(t, markerOrgA, markerDay(1), markerDay(1)); len(states) != 0 {
		t.Fatalf("live: states = %v, want none (a partial-scope run must not certify the day)", states)
	}
	outcome, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Appended != 0 || stack.rawCount(markerOrgA) != 0 {
		t.Fatalf("backfill = %+v, rows = %d, want nothing appended for a partial-scope run", outcome, stack.rawCount(markerOrgA))
	}
	// Reopening a partial-scope run writes nothing either: it never certified.
	reopen, err := stack.store.RedriveFinalizeForRange(
		ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-partial-reopen", true, testFinalizeRedriveReason, false)
	if err != nil || len(reopen.Days) != 1 || reopen.Days[0].Outcome != "redriven_reset_from_succeeded" {
		t.Fatalf("reopen of the partial run = %#v, err = %v, want redriven_reset_from_succeeded", reopen, err)
	}
	if rows := stack.rawCount(markerOrgA); rows != 0 {
		t.Fatalf("rows after reopening a partial-scope run = %d, want 0", rows)
	}
}

// A partial-scope run on a day a full-org run already certified leaves the
// certification alone, and a partial run that is the latest run of the day does
// not hide it in the backfill either.
func TestRunMarkerPartialScopeRunDoesNotDisturbAFullOrgDay(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const fullRun = "00000000-0000-4000-8000-000000087201"
	stack.seedRun(t, fullRun, "00000000-0000-4000-8000-000000087202", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, fullRun)
	partial := stack.startRun(t, markerOrgA, markerDay(1),
		"manual-daily:00000000-0000-4000-8000-0000000a7609", []RepositoryID{"00000000-0000-4000-8000-0000000a7601"})
	if _, err := stack.store.ClaimDispatch(ctx, partial); err != nil {
		t.Fatal(err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("state with a partial run in flight = %q, want succeeded (it is not a full-org run)", got)
	}
	outcome, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false)
	if err != nil || outcome.Appended != 0 {
		t.Fatalf("backfill = %+v, err = %v, want nothing appended", outcome, err)
	}
}

// F2 / F9 (vet 2): the backfill and every reopen serialize on the (org, day)
// marker lock. A reopen started while the backfill is between its reads blocks
// until the backfill commits, then reopens with a later version, so the day
// never ends up certified. Forced at both read boundaries.
func TestRunMarkerBackfillRacingAReopenNeverCertifiesAReopenedDay(t *testing.T) {
	for _, stage := range []string{"marker_read", "postgres_read"} {
		t.Run(stage, func(t *testing.T) {
			stack := newMarkerStack(t)
			ctx := context.Background()
			const runID = "00000000-0000-4000-8000-000000087211"
			stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087212", markerOrgA, markerDay(1))
			processFinalizeJob(t, ctx, stack.store, runID)
			// Remove the live row so the backfill wants to append 'succeeded'.
			stack.clearMarkers(t, markerOrgA)
			reopenDone := make(chan error, 1)
			stack.store.syncHook = func(at string) {
				if at != stage {
					return
				}
				stack.store.syncHook = nil
				go func() {
					_, err := stack.store.RedriveFinalizeForRange(
						ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-race-"+stage, true, testFinalizeRedriveReason, false)
					reopenDone <- err
				}()
				select {
				case err := <-reopenDone:
					t.Errorf("the reopen finished inside the backfill window (err=%v): it must wait for the marker lock", err)
				case <-time.After(500 * time.Millisecond):
				}
			}
			if _, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-reopenDone:
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the reopen never finished after the backfill committed")
			}
			var status, finalization string
			if err := stack.pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
				Scan(&status, &finalization); err != nil {
				t.Fatal(err)
			}
			state := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]
			if status != "running" || finalization != "pending" || state != RunMarkerReopened {
				t.Fatalf("run is %s/%s and the marker reads %q, want running/pending and reopened", status, finalization, state)
			}
		})
	}
}

// F9 (vet 2): a backfill that starts while a reopen is inside its transaction
// (the 'reopened' marker is appended, the reset not yet committed) must wait for
// that transaction. It then reads the reopened run and appends nothing false.
// Both reopen paths.
func TestRunMarkerBackfillWaitsForAReopenTransaction(t *testing.T) {
	for _, path := range []string{"finalize-redrive", "partition-recompute"} {
		t.Run(path, func(t *testing.T) {
			stack := newMarkerStack(t)
			ctx := context.Background()
			const runID = "00000000-0000-4000-8000-000000087261"
			stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087262", markerOrgA, markerDay(1))
			processFinalizeJob(t, ctx, stack.store, runID)
			if path == "partition-recompute" {
				if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_runs SET generation = 'fixed-schedule:daily_metrics_fanout:2026-09-02T01:00:00Z' WHERE id = $1::uuid`, runID); err != nil {
					t.Fatal(err)
				}
			}
			backfillDone := make(chan error, 1)
			var fired bool
			stack.store.SetRunMarkerWriter(writerFunc(func(c context.Context, m RunMarker) error {
				err := stack.writer.AppendRunMarker(c, m)
				if err == nil && m.State == RunMarkerReopened && !fired {
					fired = true
					go func() {
						_, e := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false)
						backfillDone <- e
					}()
					select {
					case e := <-backfillDone:
						t.Errorf("the backfill finished inside the reopen transaction (err=%v): it must wait for the marker lock", e)
					case <-time.After(500 * time.Millisecond):
					}
				}
				return err
			}))
			var err error
			if path == "finalize-redrive" {
				_, err = stack.store.RedriveFinalizeForRange(ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1),
					"marker-window", true, testFinalizeRedriveReason, false)
			} else {
				_, err = stack.store.RedrivePartitionsForRange(ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1),
					"marker-window", "repo_user_commit", testPartitionRecomputeReason, false)
			}
			stack.store.SetRunMarkerWriter(stack.writer)
			if err != nil || !fired {
				t.Fatalf("setup: reopen err=%v fired=%v", err, fired)
			}
			select {
			case e := <-backfillDone:
				if e != nil {
					t.Fatalf("backfill: %v", e)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the backfill never finished after the reopen committed")
			}
			var status, finalization string
			if err := stack.pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
				Scan(&status, &finalization); err != nil {
				t.Fatal(err)
			}
			if state := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; status == "succeeded" || state == RunMarkerSucceeded {
				t.Fatalf("FALSE SUCCEEDED: run is %s/%s, marker reads %q", status, finalization, state)
			}
		})
	}
}

// The dispatch claim takes the same lock: a claim that arrives while the
// backfill is between its reads waits for it, then appends 'reopened' with a
// later version.
func TestRunMarkerDispatchClaimWaitsForTheBackfill(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const firstRun = "00000000-0000-4000-8000-000000087271"
	stack.seedRun(t, firstRun, "00000000-0000-4000-8000-000000087272", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, firstRun)
	second := stack.startRun(t, markerOrgA, markerDay(1), "post-sync:00000000-0000-4000-8000-000000087273", nil)
	stack.clearMarkers(t, markerOrgA)
	claimDone := make(chan error, 1)
	stack.store.syncHook = func(at string) {
		if at != "postgres_read" {
			return
		}
		stack.store.syncHook = nil
		go func() {
			_, err := stack.store.ClaimDispatch(ctx, second)
			claimDone <- err
		}()
		select {
		case err := <-claimDone:
			t.Errorf("the claim finished inside the backfill window (err=%v): it must wait for the marker lock", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
	if _, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-claimDone:
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the claim never finished after the backfill committed")
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("state = %q, want reopened (a run is in flight)", got)
	}
}

type writerFunc func(context.Context, RunMarker) error

func (fn writerFunc) AppendRunMarker(ctx context.Context, marker RunMarker) error {
	return fn(ctx, marker)
}

// F3 (vet 2): the version is the Postgres clock. A worker whose clock is a year
// ahead finalizes, then an operator reopens: the reopen must outrank it.
func TestRunMarkerWorkerClockSkewCannotOutrankALaterReopen(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000087221"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087222", markerOrgA, markerDay(1))
	ahead := time.Now().UTC().AddDate(1, 0, 0)
	stack.store.now = func() time.Time { return ahead }
	processFinalizeJob(t, ctx, stack.store, runID)
	stack.store.now = func() time.Time { return time.Now().UTC() }
	if _, err := stack.store.RedriveFinalizeForRange(
		ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-skew", true, testFinalizeRedriveReason, false); err != nil {
		t.Fatal(err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("state after a later reopen = %q, want reopened (a skewed worker clock must not win)", got)
	}
}

// F3 (vet 2): two events with the same version never read as succeeded, in
// either insertion order.
func TestRunMarkerSameVersionTieReadsReopened(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	for index, order := range [][]RunMarkerState{
		{RunMarkerSucceeded, RunMarkerReopened}, {RunMarkerReopened, RunMarkerSucceeded},
	} {
		day := markerDay(1 + index)
		for _, state := range order {
			if err := stack.reader.AppendRunMarker(ctx, RunMarker{
				OrganizationID: markerOrgA, TargetDay: day, Generation: "post-sync:tie",
				State: state, FinalizedAt: at, Version: uint64(at.UnixMilli()),
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, day := range []string{"2026-09-01", "2026-09-02"} {
		if got := stack.states(t, markerOrgA, markerDay(1), markerDay(2))[day]; got != RunMarkerReopened {
			t.Fatalf("%s: tie reads %q, want reopened", day, got)
		}
	}
}

// F4 / vet 1: a new full-org generation starting for a day that succeeded makes
// the day unknown until that generation finalizes.
func TestRunMarkerNewGenerationInFlightMakesTheDayUnknown(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const firstRun = "00000000-0000-4000-8000-000000087231"
	stack.seedRun(t, firstRun, "00000000-0000-4000-8000-000000087232", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, firstRun)
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("state before the new generation = %q, want succeeded", got)
	}

	second := stack.startRun(t, markerOrgA, markerDay(1), "post-sync:00000000-0000-4000-8000-000000087233", nil)
	if _, err := stack.store.ClaimDispatch(ctx, second); err != nil {
		t.Fatal(err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("state with a new generation in flight = %q, want reopened (unknown)", got)
	}
	// Live and backfill now agree on this PG state.
	outcome, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false)
	if err != nil || outcome.Appended != 0 {
		t.Fatalf("backfill = %+v, err = %v, want nothing to append (live already says reopened)", outcome, err)
	}

	// The claim is refused, not skipped, when the marker cannot be written.
	stack.writer.setFail(true)
	third := stack.startRun(t, markerOrgA, markerDay(2), "post-sync:00000000-0000-4000-8000-000000087234", nil)
	if _, err := stack.store.ClaimDispatch(ctx, third); err == nil {
		t.Fatal("ClaimDispatch succeeded with the marker writer down, want an error")
	}
}

func (stack *markerStack) clearMarkers(t *testing.T, orgID string) {
	t.Helper()
	if err := stack.rawExec(`ALTER TABLE daily_metrics_run_marker DELETE WHERE org_id = '` + orgID + `' SETTINGS mutations_sync = 2`); err != nil {
		t.Fatal(err)
	}
}

// A day whose run is not succeeded and that has no marker row is already
// unknown: the backfill adds nothing for it.
func TestRunMarkerBackfillAddsNothingForAnUnfinishedDayWithNoMarker(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	stack.seedRun(t, "00000000-0000-4000-8000-000000087241", "00000000-0000-4000-8000-000000087242", markerOrgA, markerDay(1))
	outcome, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.DaysExamined != 1 || outcome.Unchanged != 1 || outcome.Appended != 0 || stack.rawCount(markerOrgA) != 0 {
		t.Fatalf("backfill = %+v, rows = %d, want one day unchanged and no row", outcome, stack.rawCount(markerOrgA))
	}
}

// A reopen whose marker landed but whose transaction rolled back leaves a
// 'reopened' row newer than the stored succeeded state. The backfill must still
// restore the day (its version is raised above the row it observed).
func TestRunMarkerBackfillHealsARolledBackReopen(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000087251"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087252", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, runID)
	later := time.Now().UTC().Add(time.Hour)
	if err := stack.reader.AppendRunMarker(ctx, RunMarker{
		OrganizationID: markerOrgA, TargetDay: markerDay(1), Generation: "post-sync:rolled-back",
		State: RunMarkerReopened, FinalizedAt: later, Version: uint64(later.UnixMilli()),
	}); err != nil {
		t.Fatal(err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("setup: state = %q, want reopened", got)
	}
	if _, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false); err != nil {
		t.Fatal(err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("state after the backfill = %q, want succeeded (Postgres says the run is succeeded)", got)
	}
}

// lockProbeReader records how many advisory locks exist at the instant the
// backfill reads ClickHouse.
type lockProbeReader struct {
	inner RunMarkerReader
	probe func()
}

func (r *lockProbeReader) RunMarkerStates(ctx context.Context, org string, from, to time.Time) (map[string]RunMarkerObservation, error) {
	r.probe()
	return r.inner.RunMarkerStates(ctx, org, from, to)
}

// The backfill takes the (org, day) lock BEFORE it reads ClickHouse: when the
// ClickHouse read starts, an advisory lock is already held.
func TestRunMarkerBackfillHoldsTheLockBeforeItReadsClickHouse(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	stack.seedRun(t, "00000000-0000-4000-8000-000000087281", "00000000-0000-4000-8000-000000087282", markerOrgA, markerDay(1))
	held := -1
	reader := &lockProbeReader{inner: stack.reader, probe: func() {
		if err := stack.pool.QueryRow(ctx, `
SELECT count(*) FROM pg_locks
WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&held); err != nil {
			t.Error(err)
		}
	}}
	stack.store.SetRunMarkerReader(reader)
	if _, err := stack.store.BackfillRunMarkers(ctx, markerOrgA, markerDay(1), markerDay(1), false); err != nil {
		t.Fatal(err)
	}
	if held < 1 {
		t.Fatalf("advisory locks held when the ClickHouse read started = %d, want the day's lock already held", held)
	}
}

// finishFullOrgRun drives a repository-list-free run (the deferred-discovery
// shape) through claim, materialization, partition success and finalize.
func (stack *markerStack) finishFullOrgRun(t *testing.T, orgID string, day time.Time, generation string) string {
	t.Helper()
	ctx := context.Background()
	runID := stack.startRun(t, orgID, day, generation, nil)
	run, err := stack.store.ClaimDispatch(ctx, runID)
	if err != nil || run == nil {
		t.Fatalf("ClaimDispatch: run=%v err=%v", run, err)
	}
	if !run.FullOrg {
		t.Fatalf("a run created without a repository list must be full-org, got FullOrg=%v", run.FullOrg)
	}
	if _, err := stack.store.MaterializeScheduledFanout(ctx, *run, []RepositoryID{"00000000-0000-4000-8000-0000000a7601"}); err != nil {
		t.Fatal(err)
	}
	if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_partitions SET status = 'succeeded' WHERE run_id = $1::uuid`, runID); err != nil {
		t.Fatal(err)
	}
	processFinalizeJob(t, ctx, stack.store, runID)
	return runID
}

// P1-c (r1): the scope is recorded at creation, not read from the generation
// text. A manual run without --repo-id and the external-recompute
// all-repository fallback are full-org and certify the day.
func TestRunMarkerRepositoryListFreeRunsCertifyWhateverTheirGeneration(t *testing.T) {
	for name, generation := range map[string]string{
		"manual daily-start without a repository list": "manual-daily:00000000-0000-4000-8000-0000000a7611",
		"external recompute all-repository fallback":   "ext-recompute:00000000-0000-4000-8000-0000000a7612",
	} {
		t.Run(name, func(t *testing.T) {
			stack := newMarkerStack(t)
			stack.finishFullOrgRun(t, markerOrgA, markerDay(1), generation)
			if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
				t.Fatalf("state = %q, want succeeded (the run computed the whole organization)", got)
			}
		})
	}
}

// P1-a (r1): the dispatch claim and its marker are one transaction. When the
// marker cannot be written the claim does not commit: the run stays pending, so
// Postgres and the marker cannot disagree.
func TestRunMarkerFailedClaimMarkerLeavesTheRunUnclaimed(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	stack.seedRun(t, "00000000-0000-4000-8000-000000087301", "00000000-0000-4000-8000-000000087302", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, "00000000-0000-4000-8000-000000087301")
	second := stack.startRun(t, markerOrgA, markerDay(1), "post-sync:00000000-0000-4000-8000-000000087303", nil)
	stack.writer.setFail(true)
	if _, err := stack.store.ClaimDispatch(ctx, second); err == nil {
		t.Fatal("ClaimDispatch succeeded with the marker writer down, want an error")
	}
	var status string
	if err := stack.pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1::uuid`, second).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("run status after a refused claim = %q, want pending (the claim rolled back)", status)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("marker = %q, want succeeded (nothing changed, so it is still true)", got)
	}
}

// P1-b (r1): an older run's finalize that completes after a newer full-org run
// was claimed must not certify the day. markerSync reads the day's LATEST
// full-org run.
func TestRunMarkerOlderFinalizeCannotCertifyAfterANewerClaim(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const olderRun = "00000000-0000-4000-8000-000000087311"
	stack.seedRun(t, olderRun, "00000000-0000-4000-8000-000000087312", markerOrgA, markerDay(1))
	claim, err := stack.store.ClaimFinalize(ctx, olderRun)
	if err != nil || claim == nil {
		t.Fatalf("ClaimFinalize: claim=%v err=%v", claim, err)
	}
	newer := stack.startRun(t, markerOrgA, markerDay(1), "post-sync:00000000-0000-4000-8000-000000087313", nil)
	if _, err := stack.store.ClaimDispatch(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := stack.store.CompleteFinalize(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := stack.pool.QueryRow(ctx, `SELECT status FROM daily_metrics_runs WHERE id = $1::uuid`, olderRun).Scan(&status); err != nil || status != "succeeded" {
		t.Fatalf("older run status = %q, err = %v, want succeeded (it did finish)", status, err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("state = %q, want reopened (a newer full-org run is in flight)", got)
	}
}

// r1 P3: the reopen's marker lands in ClickHouse and then the Postgres commit
// fails. The day reads unknown (never a false succeeded) and markerSync heals
// it because Postgres still says the run is succeeded.
func TestRunMarkerPostgresCommitFailureAfterTheAppendHeals(t *testing.T) {
	stack := newMarkerStack(t)
	const runID = "00000000-0000-4000-8000-000000087321"
	stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087322", markerOrgA, markerDay(1))
	processFinalizeJob(t, context.Background(), stack.store, runID)

	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stack.store.SetRunMarkerWriter(writerFunc(func(c context.Context, m RunMarker) error {
		err := stack.writer.AppendRunMarker(c, m)
		if err == nil && m.State == RunMarkerReopened {
			cancel() // the request dies after the append and before the commit
		}
		return err
	}))
	_, err := stack.store.RedriveFinalizeForRange(
		cancelCtx, stack.publisher, markerOrgA, markerDay(1), markerDay(1), "marker-commit-fails", true, testFinalizeRedriveReason, false)
	stack.store.SetRunMarkerWriter(stack.writer)
	if err == nil {
		t.Fatal("the reopen reported success although its request was cancelled before the commit")
	}
	var status, finalization string
	if err := stack.pool.QueryRow(context.Background(), `SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID).
		Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" || finalization != "succeeded" {
		t.Fatalf("run = %s/%s, want succeeded/succeeded (the reopen rolled back)", status, finalization)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerReopened {
		t.Fatalf("state after the failed commit = %q, want reopened (unknown, never a false succeeded)", got)
	}
	if _, err := stack.store.BackfillRunMarkers(context.Background(), markerOrgA, markerDay(1), markerDay(1), false); err != nil {
		t.Fatal(err)
	}
	if got := stack.states(t, markerOrgA, markerDay(1), markerDay(1))["2026-09-01"]; got != RunMarkerSucceeded {
		t.Fatalf("state after markerSync = %q, want succeeded (Postgres says the run is succeeded)", got)
	}
}

// The scheduled fan-out records full-org scope at creation; a StartRunTx with an
// explicit repository list records partial scope, one without records full-org.
func TestRunMarkerRunScopeIsRecordedAtCreation(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	scope := func(runID string) bool {
		var fullOrg bool
		if err := stack.pool.QueryRow(ctx, `SELECT full_org FROM daily_metrics_runs WHERE id = $1::uuid`, runID).Scan(&fullOrg); err != nil {
			t.Fatal(err)
		}
		return fullOrg
	}
	tx, err := stack.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fanout, err := stack.store.StartScheduledFanoutRunTx(ctx, tx, ScheduledFanoutRequest{
		OrganizationID: markerOrgA, TargetDay: markerDay(1),
		Generation: ScheduledFanoutGenerationPrefix + "2026-09-02T01:00:00Z",
	}, stack.publisher)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	listed := stack.startRun(t, markerOrgA, markerDay(2), "manual-daily:00000000-0000-4000-8000-0000000a7621",
		[]RepositoryID{"00000000-0000-4000-8000-0000000a7601"})
	free := stack.startRun(t, markerOrgA, markerDay(3), "manual-daily:00000000-0000-4000-8000-0000000a7622", nil)
	if !scope(fanout.ID) || scope(listed) || !scope(free) {
		t.Fatalf("full_org: fan-out=%v listed=%v list-free=%v, want true false true", scope(fanout.ID), scope(listed), scope(free))
	}
}

// Lock order (vet 1 P2): every marker path takes the (org, day) advisory lock
// FIRST and the run row lock SECOND. While another transaction holds the day's
// lock, a reopen must wait on it without holding the run's row lock; with the
// old order (row lock, then advisory lock) it would hold the row lock while it
// waits, which is the half of a deadlock cycle with the dispatch claim.
func TestRunMarkerReopenWaitsForTheDayLockWithoutHoldingTheRunRow(t *testing.T) {
	for _, path := range []string{"finalize-redrive", "partition-recompute"} {
		t.Run(path, func(t *testing.T) {
			stack := newMarkerStack(t)
			ctx := context.Background()
			const runID = "00000000-0000-4000-8000-000000087331"
			stack.seedRun(t, runID, "00000000-0000-4000-8000-000000087332", markerOrgA, markerDay(1))
			processFinalizeJob(t, ctx, stack.store, runID)
			if path == "partition-recompute" {
				if _, err := stack.pool.Exec(ctx, `UPDATE daily_metrics_runs SET generation = 'fixed-schedule:daily_metrics_fanout:2026-09-02T01:00:00Z' WHERE id = $1::uuid`, runID); err != nil {
					t.Fatal(err)
				}
			}
			holder, err := stack.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// A failing assertion must not leave this transaction open: it would
			// hang the container cleanup.
			t.Cleanup(func() { _ = holder.Rollback(ctx) })
			if err := lockMarkerDay(ctx, holder, markerOrgA, "2026-09-01"); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				var err error
				if path == "finalize-redrive" {
					_, err = stack.store.RedriveFinalizeForRange(ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1),
						"marker-order", true, testFinalizeRedriveReason, false)
				} else {
					_, err = stack.store.RedrivePartitionsForRange(ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1),
						"marker-order", "repo_user_commit", testPartitionRecomputeReason, false)
				}
				done <- err
			}()
			// Wait until the reopen is blocked on the advisory lock.
			blocked := false
			for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
				var waiting int
				if err := stack.pool.QueryRow(ctx, `
SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting > 0 {
					blocked = true
					break
				}
			}
			if !blocked {
				_ = holder.Rollback(ctx)
				<-done
				t.Fatal("the reopen never waited on the day lock")
			}
			probe, err := stack.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, rowErr := probe.Exec(ctx, `SELECT 1 FROM daily_metrics_runs WHERE id = $1::uuid FOR UPDATE NOWAIT`, runID)
			_ = probe.Rollback(ctx)
			if rowErr != nil {
				t.Errorf("the reopen holds the run row while it waits for the day lock (%v): lock order is row then advisory", rowErr)
			}
			if err := holder.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the reopen never finished after the lock was released")
			}
		})
	}
}

// No deadlock between a dispatch claim and a reopen of the same org-day, run
// concurrently many times.
func TestRunMarkerClaimAndReopenOfOneDayNeverDeadlock(t *testing.T) {
	stack := newMarkerStack(t)
	ctx := context.Background()
	const firstRun = "00000000-0000-4000-8000-000000087341"
	stack.seedRun(t, firstRun, "00000000-0000-4000-8000-000000087342", markerOrgA, markerDay(1))
	processFinalizeJob(t, ctx, stack.store, firstRun)
	second := stack.startRun(t, markerOrgA, markerDay(1), "post-sync:00000000-0000-4000-8000-000000087343", nil)
	errs := make(chan error, 2)
	go func() {
		_, err := stack.store.ClaimDispatch(ctx, second)
		errs <- err
	}()
	go func() {
		_, err := stack.store.RedriveFinalizeForRange(ctx, stack.publisher, markerOrgA, markerDay(1), markerDay(1),
			"marker-concurrent", true, testFinalizeRedriveReason, false)
		errs <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent claim and reopen: %v", err)
		}
	}
}
