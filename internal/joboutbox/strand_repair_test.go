package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river/rivertype"
)

type fakeStrandRepair struct {
	result StrandRepairResult
	err    error
	calls  int
}

func (repair *fakeStrandRepair) Step(context.Context, time.Time, int) (StrandRepairResult, error) {
	repair.calls++
	return repair.result, repair.err
}

func TestNewStrandRepairRejectsUnusableConfiguration(t *testing.T) {
	if _, err := NewStrandRepair(nil, "river"); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil pool error = %v, want ErrInvalidConfiguration", err)
	}
	for _, schema := range []string{"", "Public", "river-schema", "river;drop", strings.Repeat("a", 64)} {
		if _, err := NewStrandRepair(nil, schema); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("schema %q error = %v, want ErrInvalidConfiguration", schema, err)
		}
	}
}

func TestStrandRepairStepRejectsUnusableRequests(t *testing.T) {
	// A nil or zero-value repair must refuse rather than dereference: the loop
	// calls Step every second, and a seam that panicked would take the whole
	// reconciler down rather than failing one step.
	var uninitialised *StrandRepair
	if _, err := uninitialised.Step(context.Background(), time.Now(), 1); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil repair error = %v, want ErrInvalidConfiguration", err)
	}
	if _, err := (&StrandRepair{}).Step(context.Background(), time.Now(), 1); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("zero-value repair error = %v, want ErrInvalidConfiguration", err)
	}
	// Bounds are part of the contract: an unbounded pass would hold locks over
	// an arbitrary number of outbox rows.
	configured := &StrandRepair{
		begin:  func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		client: riverDeleteAdapter{},
	}
	for _, limit := range []int{0, -1, maxReconcilerLimit + 1} {
		if _, err := configured.Step(context.Background(), time.Now(), limit); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("limit %d error = %v, want ErrInvalidConfiguration", limit, err)
		}
	}
	if _, err := configured.Step(context.Background(), time.Time{}, 1); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("zero time error = %v, want ErrInvalidConfiguration", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := configured.Step(cancelled, time.Now(), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error = %v, want context.Canceled", err)
	}
}

func TestTerminalRiverStateAcceptsOnlyTheThreeTerminalStates(t *testing.T) {
	// This predicate is what keeps the repair off a job River may still
	// rescue. Completed is deliberately included: CHAOS-3991 strands work by
	// making a job report success, so excluding it would miss every row this
	// repair exists for. The table is exhaustive over rivertype's states so a
	// newly added state cannot be silently treated as terminal.
	terminal := map[rivertype.JobState]bool{
		rivertype.JobStateCompleted: true,
		rivertype.JobStateDiscarded: true,
		rivertype.JobStateCancelled: true,
		rivertype.JobStateAvailable: false,
		rivertype.JobStateRetryable: false,
		rivertype.JobStateRunning:   false,
		rivertype.JobStateScheduled: false,
		rivertype.JobStatePending:   false,
	}
	for state, want := range terminal {
		if terminalRiverState(state) != want {
			t.Fatalf("terminalRiverState(%q) = %v, want %v", state, !want, want)
		}
	}
}

func TestClassifyStrandErrorSeparatesDeniedFromUnavailable(t *testing.T) {
	// A missing grant and a database outage demand different operator
	// actions, and this repair is the first queue-role reader of the
	// daily-metrics and work-graph tables, so the denied case is a live
	// deployment risk rather than a theoretical one.
	denied := &pgconn.PgError{Code: insufficientPrivilege, Message: "permission denied for table daily_metrics_runs"}
	if err := classifyStrandError(denied); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("42501 classified as %v, want ErrNotAuthorized", err)
	}
	if err := classifyStrandError(fmt.Errorf("wrapped: %w", denied)); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("wrapped 42501 classified as %v, want ErrNotAuthorized", err)
	}
	for _, other := range []error{
		nil,
		errors.New("connection refused"),
		&pgconn.PgError{Code: "40001", Message: "serialization failure"},
		&pgconn.PgError{Code: "42P01", Message: "relation does not exist"},
	} {
		err := classifyStrandError(other)
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("classifyStrandError(%v) = %v, want ErrUnavailable", other, err)
		}
		if errors.Is(err, ErrNotAuthorized) {
			t.Fatalf("classifyStrandError(%v) must not report a denied statement", other)
		}
	}
}

// TestStrandRepairQueriesNeverReadExecutionState is the standing guard on the
// invariant the whole design rests on: the queue-control role must never be
// granted access to execution state or external-effect evidence. If a future
// edit reaches for worker_job_runs to answer the idempotency question
// directly, this fails before the posture assertion does.
func TestStrandRepairQueriesNeverReadExecutionState(t *testing.T) {
	forbidden := []string{
		"worker_job_runs",
		"worker_job_delivery_abandonments",
		"integration_credentials",
	}
	for name, query := range strandQueriesUnderTest() {
		for _, table := range forbidden {
			if strings.Contains(query, table) {
				t.Fatalf("the %s query reads %s; the queue-control role must never see "+
					"execution state or external-effect evidence", name, table)
			}
		}
	}
}

// TestStrandRepairQueriesCarryTheirLoadBearingGuards pins the four predicates
// whose removal would each turn this repair into a strand generator. Each is
// asserted by the exact text the SQL uses, so a silent deletion fails here
// rather than in production.
func TestStrandRepairQueriesCarryTheirLoadBearingGuards(t *testing.T) {
	queries := strandQueriesUnderTest()
	for name, query := range queries {
		// The terminal predicate: without it, a job River will still run is
		// deleted and its work double-driven.
		if !strings.Contains(query, "'completed', 'discarded', 'cancelled'") {
			t.Fatalf("the %s query lost its terminal-state predicate", name)
		}
		// The finalized_at grace: without it, a never-claimed row can be
		// rearmed while an idempotency claim is still live.
		if !strings.Contains(query, "job.finalized_at <= $3") ||
			!strings.Contains(query, "job.finalized_at IS NOT NULL") {
			t.Fatalf("the %s query lost its finalized_at grace guard", name)
		}
		// Bounded, oldest-first, and serialized against sibling replicas.
		if !strings.Contains(query, "FOR UPDATE OF outbox, job SKIP LOCKED") ||
			!strings.Contains(query, "LIMIT $2::int") ||
			!strings.Contains(query, "ORDER BY outbox.delivered_at, outbox.id") {
			t.Fatalf("the %s query lost its bounding or locking clause", name)
		}
		// Only a delivered row is a strand; a pending row is already armed.
		if !strings.Contains(query, "outbox.status = 'delivered'") {
			t.Fatalf("the %s query lost its delivered-status predicate", name)
		}
	}

	// The domain-lease predicate, per shape.
	if !strings.Contains(queries["partition"], "partition.lease_expires_at <= $1") ||
		!strings.Contains(queries["partition"], "partition.status = 'running'") {
		t.Fatal("the partition query lost its domain-lease predicate")
	}
	// The finalize shape must keep ClaimFinalize's own eligibility guard, or it
	// rearms a finalizer that can only no-op.
	if !strings.Contains(queries["finalize"], "sibling.status <> 'succeeded'") ||
		!strings.Contains(queries["finalize"], "NOT EXISTS") {
		t.Fatal("the finalize query lost its partitions-all-succeeded guard")
	}
	// The work-graph shape must bind kind as well as id, and accept only the
	// two states PostgresStore.Claim will reclaim.
	workGraph := queries["workgraph"]
	if !strings.Contains(workGraph, "request.kind = outbox.job_kind") {
		t.Fatal("the workgraph query no longer binds the request kind")
	}
	if !strings.Contains(workGraph, "request.state = 'running'") ||
		!strings.Contains(workGraph, "request.state = 'pending'") {
		t.Fatal("the workgraph query lost one of the two reclaimable states")
	}
	for _, excluded := range []string{"'ambiguous'", "'succeeded'", "'failed'", "'canceled'"} {
		if strings.Contains(workGraph, excluded) {
			t.Fatalf("the workgraph query mentions %s; those states must be excluded "+
				"structurally by listing only the reclaimable ones", excluded)
		}
	}
}

func strandQueriesUnderTest() map[string]string {
	return map[string]string{
		"partition": repairStrandedPartitionSQL,
		"finalize":  repairStrandedFinalizeSQL,
		"workgraph": repairStrandedWorkGraphSQL,
	}
}

// TestRelayStepFailsClosedOnStrandRepairError proves the strand sweep runs
// before any row is claimed and that its failure aborts the step. A denied
// statement must stop the pass, not be swallowed into a quiet zero while the
// relay carries on: the whole point of ErrNotAuthorized is that an operator
// sees it.
func TestRelayStepFailsClosedOnStrandRepairError(t *testing.T) {
	strand := &fakeStrandRepair{err: ErrNotAuthorized}
	// repository and inserter are deliberately nil. If the strand seam did not
	// run first, or its error did not abort the step, this would panic
	// dereferencing them instead of returning.
	relay := &Relay{repair: fakeTerminalRepair{}, strandRepair: strand}
	_, err := relay.Step(context.Background(), time.Now(), 1)
	if !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("Step error = %v, want ErrNotAuthorized", err)
	}
	if strand.calls != 1 {
		t.Fatalf("strand repair called %d times, want 1", strand.calls)
	}
}

// TestRelayStepCarriesStrandCountsIntoTheResult keeps the two seams' numbers
// distinct all the way to StepResult.
func TestRelayStepCarriesStrandCountsIntoTheResult(t *testing.T) {
	strand := &fakeStrandRepair{result: StrandRepairResult{Rearmed: 4, SkippedJobLive: 2}}
	relay := &Relay{repair: fakeTerminalRepair{}, strandRepair: strand}
	// The step is expected to fail once it reaches the nil repository; the
	// assertion is on what the recovery seams reported before that point,
	// which Step returns alongside the error.
	result, _ := relay.stepRecovery(context.Background(), time.Now(), 1)
	if result.StrandsRearmed != 4 || result.StrandJobsSkippedLive != 2 {
		t.Fatalf("result = %+v, want 4 rearmed and 2 skipped", result)
	}
	if result.Recovered != 0 {
		t.Fatalf("strand rearms leaked into the terminal-delivery counter: %+v", result)
	}
}

func TestReconcilerLoopExportsStrandCountersSeparately(t *testing.T) {
	// The two recovery seams must stay distinguishable. If a strand rearm were
	// folded into the terminal-delivery counter, the CHAOS-3997 heal could not
	// be verified: the ticket's falsification line is that rows draining
	// without the reclaim counter moving means the work completed by some
	// other route.
	clock := &testReconcilerClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	results := []StepResult{
		{Recovered: 1, StrandsRearmed: 4, StrandJobsSkippedLive: 2},
		{StrandsRearmed: 1, StrandJobsSkippedLive: 1},
	}
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		result := results[0]
		results = results[1:]
		return result, nil
	}), clock)
	ctx := context.Background()
	for step := 0; step < 2; step++ {
		if err := loop.step(ctx, clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_strands_rearmed_total 5",
		"worker_outbox_reconciler_strand_jobs_skipped_live_total 3",
		"worker_outbox_reconciler_terminal_deliveries_recovered_total 1",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
	if strings.Contains(metrics.String(), "{") {
		t.Fatalf("reconciler metrics must not expose labels:\n%s", metrics.String())
	}
}
