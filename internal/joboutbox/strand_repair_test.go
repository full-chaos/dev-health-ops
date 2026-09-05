package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river/rivertype"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
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
	if _, err := NewStrandRepair(nil, nil, "river"); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil pools error = %v, want ErrInvalidConfiguration", err)
	}
	for _, schema := range []string{"", "Public", "river-schema", "river;drop", strings.Repeat("a", 64)} {
		if _, err := NewStrandRepair(nil, nil, schema); !errors.Is(err, ErrInvalidConfiguration) {
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
		beginQueue:  func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		queryQueue:  func(context.Context, string, ...any) (pgx.Rows, error) { return nil, errors.New("unused") },
		queryDomain: func(context.Context, string, ...any) (pgx.Rows, error) { return nil, errors.New("unused") },
		client:      riverDeleteAdapter{},
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
		// The disposition must be REPORTED, not filtered. Filtering a refusal
		// out of the result set makes the skip counters unreachable, and a
		// rescuer that has stopped running then looks exactly like an empty
		// queue.
		for _, disposition := range []string{"'skip_job_live'", "'rearm'"} {
			if !strings.Contains(query, disposition) {
				t.Fatalf("the %s query lost the %s disposition", name, disposition)
			}
		}
		// The candidate identity the execution-state read needs. Without
		// job_kind and dedupe_key the claim lookup cannot be keyed at all.
		if !strings.Contains(query, "outbox.job_kind, outbox.dedupe_key") {
			t.Fatalf("the %s query no longer projects the claim-lookup identity", name)
		}
		// Bounded and oldest-first in both forms; the locked form additionally
		// serializes against sibling replicas.
		if !strings.Contains(query, "LIMIT $2::int") ||
			!strings.Contains(query, "ORDER BY outbox.delivered_at, outbox.id") {
			t.Fatalf("the %s query lost its bounding clause", name)
		}
		// Only a delivered row is a strand; a pending row is already armed.
		if !strings.Contains(query, "outbox.status = 'delivered'") {
			t.Fatalf("the %s query lost its delivered-status predicate", name)
		}
	}

	// codex review 2026-08-20: every domain join must be organization-scoped.
	// Without it a contract-valid envelope naming another tenant's UUID lets
	// this repair rearm across the tenant boundary. The provider-unit prior
	// art has carried this predicate since it was written.
	for name, query := range queries {
		if !strings.Contains(query, "outbox.args ->> 'organization_id'") {
			t.Fatalf("the %s query no longer scopes its domain join by organization", name)
		}
	}

	// The domain-lease predicate, per shape.
	if !strings.Contains(queries["partition"], "partition.lease_expires_at <= $1") ||
		!strings.Contains(queries["partition"], "partition.status = 'running'") {
		t.Fatal("the partition query lost its domain-lease predicate")
	}
	// The finalize shape must keep ClaimFinalize's own eligibility guard, or it
	// rearms a finalizer that can only no-op.
	finalize := queries["finalize"]
	if !strings.Contains(finalize, "sibling.status <> 'succeeded'") ||
		!strings.Contains(finalize, "NOT EXISTS") {
		t.Fatal("the finalize query lost its partitions-all-succeeded guard")
	}
	// codex review 2026-08-20: the finalize status and lease predicates were
	// unasserted, so deleting them would have passed this test. They mirror
	// classifyLease exactly: a 'running' finalization is reclaimable ONLY
	// through the non-NULL expired-lease branch; 'pending'/'failed' are
	// claimable outright. Accepting 'running' with a NULL lease would rearm a
	// finalizer ClaimFinalize then refuses.
	if !strings.Contains(finalize, "run.finalization_status = 'running'") ||
		!strings.Contains(finalize, "run.finalization_lease_expires_at IS NOT NULL") ||
		!strings.Contains(finalize, "run.finalization_lease_expires_at <= $1") {
		t.Fatal("the finalize query lost the running-with-expired-lease branch")
	}
	if !strings.Contains(finalize, "run.finalization_status IN ('pending', 'failed')") {
		t.Fatal("the finalize query lost its claimable-status branch")
	}
	if strings.Contains(finalize, "run.finalization_status IN ('pending', 'running', 'failed')") {
		t.Fatal("the finalize query treats 'running' as claimable outright; classifyLease does not")
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
	// codex review 2026-08-20: the lease predicates were unasserted, so
	// deleting `request.lease_expires_at <= $1` would have passed. Without it
	// a request with a LIVE lease is rearmed under its current owner.
	if !strings.Contains(workGraph, "request.lease_expires_at IS NOT NULL") ||
		!strings.Contains(workGraph, "request.lease_expires_at <= $1") {
		t.Fatal("the workgraph query lost its expired-lease predicate")
	}
	if !strings.Contains(workGraph, "request.claim_token IS NULL") ||
		!strings.Contains(workGraph, "request.lease_expires_at IS NULL") {
		t.Fatal("the workgraph query lost the never-claimed shape of its pending branch")
	}
	for _, excluded := range []string{"'ambiguous'", "'succeeded'", "'failed'", "'canceled'"} {
		if strings.Contains(workGraph, excluded) {
			t.Fatalf("the workgraph query mentions %s; those states must be excluded "+
				"structurally by listing only the reclaimable ones", excluded)
		}
	}
}

// TestStrandShapeFormsDifferOnlyInLocking pins the two forms newStrandShape
// generates from each template.
//
// codex review 2026-08-20 round 2: phase 3 must bind the surveyed DELIVERY
// GENERATION, not just the outbox id. Matching on the id alone is an ABA hole
// -- between the survey and the lock another replica can rearm the row and the
// relay can mint a REPLACEMENT delivery in the same pass, so an id-only match
// would re-read whatever job is current and delete a delivery this pass never
// surveyed.
func TestStrandShapeFormsDifferOnlyInLocking(t *testing.T) {
	for name, template := range strandQueriesUnderTest() {
		shape := newStrandShape(name, template, `"river"."river_job"`)

		// The survey must NOT lock and must NOT reference the approved-set
		// parameters -- it is called with two parameters only, so a stray $3
		// or $4 would be a runtime bind error on every pass.
		if strings.Contains(shape.survey, "FOR UPDATE") {
			t.Fatalf("the %s survey form locks; it runs before the domain round-trip and must not "+
				"hold outbox rows across it", name)
		}
		for _, parameter := range []string{"$3", "$4"} {
			if strings.Contains(shape.survey, parameter) {
				t.Fatalf("the %s survey form references %s but is called with two parameters", name, parameter)
			}
		}

		// The locked form must serialize replicas AND pin the generation.
		if !strings.Contains(shape.lock, "FOR UPDATE OF outbox, job SKIP LOCKED") {
			t.Fatalf("the %s locked form lost its row lock", name)
		}
		if !strings.Contains(shape.lock, "approved.river_job_id = outbox.river_job_id") {
			t.Fatalf("the %s locked form lost its delivery-generation CAS; matching the outbox id "+
				"alone would let it delete a replacement delivery it never surveyed", name)
		}
		if !strings.Contains(shape.lock, "approved.outbox_id = outbox.id") {
			t.Fatalf("the %s locked form no longer binds the approved outbox id", name)
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
	strand := &fakeStrandRepair{result: StrandRepairResult{
		Rearmed: 4, SkippedJobLive: 2, SkippedClaimLive: 1, SkippedClaimSettled: 3, SkippedRaceLost: 2,
	}}
	relay := &Relay{repair: fakeTerminalRepair{}, strandRepair: strand}
	// The step is expected to fail once it reaches the nil repository; the
	// assertion is on what the recovery seams reported before that point,
	// which Step returns alongside the error.
	result, _ := relay.stepRecovery(context.Background(), time.Now(), 1)
	if result.StrandsRearmed != 4 || result.StrandJobsSkippedLive != 2 ||
		result.StrandClaimsLive != 1 || result.StrandClaimsSettled != 3 ||
		result.StrandRaceLost != 2 {
		t.Fatalf("result = %+v, want 4 rearmed, 2 job-live, 1 claim-live, 3 claim-settled, 2 race-lost", result)
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
		{Recovered: 1, StrandsRearmed: 4, StrandJobsSkippedLive: 2, StrandClaimsLive: 1, StrandClaimsSettled: 5, StrandRaceLost: 4},
		{StrandsRearmed: 1, StrandJobsSkippedLive: 1, StrandClaimsLive: 2, StrandClaimsSettled: 1, StrandRaceLost: 3},
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
		"worker_outbox_reconciler_strand_claims_live_total 3",
		"worker_outbox_reconciler_strand_claims_settled_total 6",
		"worker_outbox_reconciler_strand_race_lost_total 7",
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

// TestReconcilerLoopLogsEachRetiredKindObservation is the red/green proof for
// r1 finding F1 (P2, codex, CHAOS-4438): a worker_job_outbox row naming a
// kind with no Go handler at all (investment.dispatch/chunk/finalize, or any
// future retirement) must be logged with enough identity for an operator to
// find and resolve it manually, not left invisible now that none of
// StrandRepair's three shapes select these kinds any more.
func TestReconcilerLoopLogsEachRetiredKindObservation(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)}

	// GREEN: a step carrying one observation logs it with all three fields.
	var buf bytes.Buffer
	stepper := loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return StepResult{RetiredKindObservations: []RetiredKindObservation{
			{
				OutboxID:       "11111111-1111-1111-1111-111111111111",
				JobKind:        "investment.chunk",
				OrganizationID: "22222222-2222-2222-2222-222222222222",
			},
		}}, nil
	})
	loop, err := newReconcilerLoop(stepper, ReconcilerLoopConfig{
		PollInterval: minReconcilerPollInterval,
		Limit:        7,
		Registry:     health.NewRegistry(time.Second),
		Logger:       slog.New(slog.NewJSONHandler(&buf, nil)),
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	logged := buf.String()
	for _, want := range []string{
		`"msg":"outbox row references a retired job kind with no handler"`,
		`"outbox_id":"11111111-1111-1111-1111-111111111111"`,
		`"job_kind":"investment.chunk"`,
		`"organization_id":"22222222-2222-2222-2222-222222222222"`,
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log line missing %q:\n%s", want, logged)
		}
	}

	// RED / negative control: an otherwise-identical step reporting NO
	// observations must not log this line at all -- proves the line above
	// is conditioned on real data, not emitted unconditionally every step.
	var cleanBuf bytes.Buffer
	cleanStepper := loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return StepResult{}, nil
	})
	cleanLoop, err := newReconcilerLoop(cleanStepper, ReconcilerLoopConfig{
		PollInterval: minReconcilerPollInterval,
		Limit:        7,
		Registry:     health.NewRegistry(time.Second),
		Logger:       slog.New(slog.NewJSONHandler(&cleanBuf, nil)),
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanLoop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cleanBuf.String(), "retired job kind") {
		t.Fatalf("logged a retired-kind line with zero observations present:\n%s", cleanBuf.String())
	}
}

// TestRelayStepRecoveryPreservesStrandResultOnObservationError is the
// reproduction + fix for r2 finding F2 (P2, codex, CHAOS-4438): the 3 real
// repair shapes inside StrandRepair.Step commit their rearms in their own
// queue-pool transactions BEFORE the trailing retired-kind observation
// query runs. If that read-only query then fails, StrandRepair.Step still
// returns the already-committed counts alongside the error (see its own
// fix); this test proves Relay.stepRecovery does not throw that away a
// second time by copying fields only after checking err.
func TestRelayStepRecoveryPreservesStrandResultOnObservationError(t *testing.T) {
	observationErr := errors.New("retired-kind observation query failed")
	strand := &fakeStrandRepair{
		result: StrandRepairResult{
			Rearmed:             4,
			SkippedJobLive:      2,
			SkippedClaimLive:    1,
			SkippedClaimSettled: 3,
			SkippedRaceLost:     2,
		},
		err: observationErr,
	}
	relay := &Relay{repair: fakeTerminalRepair{}, strandRepair: strand}
	result, err := relay.stepRecovery(context.Background(), time.Now(), 1)
	if !errors.Is(err, observationErr) {
		t.Fatalf("stepRecovery error = %v, want observationErr", err)
	}
	if result.StrandsRearmed != 4 || result.StrandJobsSkippedLive != 2 ||
		result.StrandClaimsLive != 1 || result.StrandClaimsSettled != 3 ||
		result.StrandRaceLost != 2 {
		t.Fatalf("result = %+v, want the already-committed strand counts preserved despite the error", result)
	}
}

// TestRetiredKindObservationsLikelyTruncated is the red/green proof for r2
// finding F3 (P2, codex, CHAOS-4438): the observation query has a fixed cap
// with no cursor between ticks, so a count that never reaches the cap
// proves nothing was missed, while a count that DOES reach it must be
// flagged -- an occasional false alarm at the exact boundary is the correct
// fail-closed direction (see the function's own doc comment).
func TestRetiredKindObservationsLikelyTruncated(t *testing.T) {
	for _, tc := range []struct {
		observed int
		want     bool
	}{
		{observed: 0, want: false},
		{observed: retiredKindsObservationCap - 1, want: false},
		{observed: retiredKindsObservationCap, want: true},
	} {
		if got := retiredKindObservationsLikelyTruncated(tc.observed); got != tc.want {
			t.Fatalf("retiredKindObservationsLikelyTruncated(%d) = %v, want %v", tc.observed, got, tc.want)
		}
	}
}

// TestReconcilerLoopLogsRetiredKindTruncation is the red/green proof that
// the truncation signal (r2 finding F3) actually reaches the operator, the
// same way TestReconcilerLoopLogsEachRetiredKindObservation proves the
// per-row signal does.
func TestReconcilerLoopLogsRetiredKindTruncation(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.September, 5, 12, 0, 0, 0, time.UTC)}

	// GREEN: a step reporting truncation logs the distinct error line.
	var buf bytes.Buffer
	stepper := loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return StepResult{RetiredKindObservationsTruncated: true}, nil
	})
	loop, err := newReconcilerLoop(stepper, ReconcilerLoopConfig{
		PollInterval: minReconcilerPollInterval,
		Limit:        7,
		Registry:     health.NewRegistry(time.Second),
		Logger:       slog.New(slog.NewJSONHandler(&buf, nil)),
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "hit its cap") {
		t.Fatalf("truncation was not logged:\n%s", buf.String())
	}

	// RED / negative control: an otherwise-identical step reporting NO
	// truncation must not log this line.
	var cleanBuf bytes.Buffer
	cleanStepper := loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		return StepResult{}, nil
	})
	cleanLoop, err := newReconcilerLoop(cleanStepper, ReconcilerLoopConfig{
		PollInterval: minReconcilerPollInterval,
		Limit:        7,
		Registry:     health.NewRegistry(time.Second),
		Logger:       slog.New(slog.NewJSONHandler(&cleanBuf, nil)),
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanLoop.step(context.Background(), clock.Now()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cleanBuf.String(), "hit its cap") {
		t.Fatalf("logged truncation with none reported:\n%s", cleanBuf.String())
	}
}

// fakeStrandSurveyRows is a minimal pgx.Rows fake covering exactly the
// methods StrandRepair.survey calls (Next/Scan/Close/Err) -- enough to
// drive one strandCandidate row through without needing a real database.
type fakeStrandSurveyRows struct {
	rows [][]any
	idx  int
}

func (f *fakeStrandSurveyRows) Close()                                       {}
func (f *fakeStrandSurveyRows) Err() error                                   { return nil }
func (f *fakeStrandSurveyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (f *fakeStrandSurveyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (f *fakeStrandSurveyRows) Values() ([]any, error)                       { return nil, nil }
func (f *fakeStrandSurveyRows) RawValues() [][]byte                          { return nil }
func (f *fakeStrandSurveyRows) Conn() *pgx.Conn                              { return nil }

func (f *fakeStrandSurveyRows) Next() bool {
	if f.idx >= len(f.rows) {
		return false
	}
	f.idx++
	return true
}

func (f *fakeStrandSurveyRows) Scan(dest ...any) error {
	row := f.rows[f.idx-1]
	for i, d := range dest {
		switch v := d.(type) {
		case *string:
			*v, _ = row[i].(string)
		case *int64:
			*v, _ = row[i].(int64)
		default:
			return fmt.Errorf("fakeStrandSurveyRows.Scan: unsupported dest type %T", d)
		}
	}
	return nil
}

// TestStrandRepairStepPreservesPriorShapeResultOnLaterShapeError is the
// reproduction + fix for r3 finding F2 (P2, codex, CHAOS-4438): F2's
// original fix only covered the trailing observeRetiredKinds error --
// Step's own shape loop still discarded an EARLIER shape's real counts
// when a LATER shape failed. Shape "a" surveys one skip_job_live
// candidate (counted in phase 1, no phase 2/3 needed); shape "b"'s survey
// then errors. The fix must return shape "a"'s count alongside the error,
// not a fresh zero result.
func TestStrandRepairStepPreservesPriorShapeResultOnLaterShapeError(t *testing.T) {
	call := 0
	queryQueue := func(context.Context, string, ...any) (pgx.Rows, error) {
		call++
		switch call {
		case 1:
			return &fakeStrandSurveyRows{rows: [][]any{
				{"11111111-1111-1111-8111-111111111111", int64(1), "workgraph.build", "dedupe-a", dispositionSkipJobLive},
			}}, nil
		case 2:
			return nil, errors.New("shape b survey failed")
		default:
			t.Fatalf("unexpected queryQueue call #%d", call)
			return nil, nil
		}
	}
	repair := &StrandRepair{
		beginQueue:  func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		queryQueue:  queryQueue,
		queryDomain: func(context.Context, string, ...any) (pgx.Rows, error) { return nil, errors.New("unused") },
		client:      riverDeleteAdapter{},
		shapes: []strandShape{
			{name: "a", survey: "SELECT 1", lock: "SELECT 1"},
			{name: "b", survey: "SELECT 1", lock: "SELECT 1"},
		},
	}

	result, err := repair.Step(context.Background(), time.Now(), 1)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Step() error = %v, want shape b's survey error classified as ErrUnavailable", err)
	}
	// team-lead ruling (CHAOS-4438, discard-on-error sweep): the error must
	// name the shape it happened in, not just classify to a bare sentinel.
	if !strings.Contains(err.Error(), `shape "b"`) {
		t.Fatalf("Step() error = %v, want it to name shape %q", err, "b")
	}
	if result.SkippedJobLive != 1 {
		t.Fatalf("Step() result = %+v, want shape a's SkippedJobLive=1 preserved despite shape b's error", result)
	}
}

// TestStrandRepairStepShapeDefaultDispositionPreservesEarlierCounts is the
// reproduction + fix for the confirmation pass's F1 (P3, codex, CHAOS-4438):
// stepShape's phase-1 classification loop increments result.SkippedJobLive
// for an EARLIER candidate, then discarded it by returning a fresh zero
// StrandRepairResult when a LATER candidate in the same loop hit the
// unknown-disposition default branch. "Nothing is counted yet at phase 1" was
// only true up to the first classified candidate, not the whole loop.
func TestStrandRepairStepShapeDefaultDispositionPreservesEarlierCounts(t *testing.T) {
	call := 0
	queryQueue := func(context.Context, string, ...any) (pgx.Rows, error) {
		call++
		if call != 1 {
			t.Fatalf("unexpected queryQueue call #%d", call)
		}
		return &fakeStrandSurveyRows{rows: [][]any{
			{"11111111-1111-1111-8111-111111111111", int64(1), "workgraph.build", "dedupe-a", dispositionSkipJobLive},
			{"22222222-2222-2222-8222-222222222222", int64(2), "workgraph.build", "dedupe-b", "unexpected-disposition"},
		}}, nil
	}
	repair := &StrandRepair{
		beginQueue:  func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		queryQueue:  queryQueue,
		queryDomain: func(context.Context, string, ...any) (pgx.Rows, error) { return nil, errors.New("unused") },
		client:      riverDeleteAdapter{},
	}
	shape := strandShape{name: "a", survey: "SELECT 1", lock: "SELECT 1"}

	// limit=2: survey() itself refuses (len(candidates) > limit) if the fake
	// returns more rows than the caller's own bound -- 2 rows need limit>=2.
	result, err := repair.stepShape(context.Background(), shape, time.Now(), 2)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stepShape() error = %v, want ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), `shape "a"`) ||
		!strings.Contains(err.Error(), "22222222-2222-2222-8222-222222222222") {
		t.Fatalf("stepShape() error = %v, want it to name shape %q and the offending outbox id", err, "a")
	}
	if result.SkippedJobLive != 1 {
		t.Fatalf(
			"stepShape() result = %+v, want the earlier skip_job_live candidate's count preserved despite the later unexpected-disposition candidate",
			result,
		)
	}
}

// TestRelayStepPreservesRecoveryResultOnClaimDueExceptError is the
// reproduction + fix for the confirmation pass's F2/withdrawn-N/A finding (P2,
// codex, CHAOS-4438): the original claim that claimDueExcept has no
// deterministic error path reachable without new production surface was
// wrong. Repository.claimDueExcept's OWN validation guard (repository.go,
// first line of the function body: "repository == nil || repository.pool ==
// nil || ...") already returns ErrInvalidConfiguration deterministically for
// a zero-value *Repository{} -- no fault-injection hook, no DB, no new
// production code needed. This drives Relay.Step's claimDueExcept error path
// directly (not just stepRecovery), proving the recovery seams' counts
// survive it.
func TestRelayStepPreservesRecoveryResultOnClaimDueExceptError(t *testing.T) {
	strand := &fakeStrandRepair{result: StrandRepairResult{
		Rearmed: 4, SkippedJobLive: 2, SkippedClaimLive: 1, SkippedClaimSettled: 3, SkippedRaceLost: 2,
	}}
	relay := &Relay{
		repository:   &Repository{}, // zero value: nil pool -- claimDueExcept fails closed on its own guard
		repair:       fakeTerminalRepair{},
		strandRepair: strand,
		config:       DefaultRelayConfig(),
	}

	result, err := relay.Step(context.Background(), time.Now(), 1)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("Step() error = %v, want ErrInvalidConfiguration (from claimDueExcept's nil-pool guard)", err)
	}
	if result.StrandsRearmed != 4 || result.StrandJobsSkippedLive != 2 ||
		result.StrandClaimsLive != 1 || result.StrandClaimsSettled != 3 || result.StrandRaceLost != 2 {
		t.Fatalf(
			"Step() result = %+v, want stepRecovery's strand-repair counts preserved despite claimDueExcept's error",
			result,
		)
	}
}

// TestStrandRepairStepPreservesShapeResultOnObserveRetiredKindsError closes a
// coverage gap found while building confirmation pass 2's context (team-lead
// ruling, CHAOS-4438: every error branch of a touched function must be
// exercised, or fixed, before the next round launches). r2's F2 fix at
// strand_repair.go's trailing observeRetiredKinds error return was pinned
// ONLY via a fake at Relay.stepRecovery (TestRelayStepRecoveryPreservesStrand
// ResultOnObservationError) -- the REAL StrandRepair.Step never had a direct
// test driving this exact path. Same fix shape as this file's other Step
// tests: one shape produces a real, non-zero result; the trailing
// observeRetiredKinds call then errors; the shape's result must survive.
func TestStrandRepairStepPreservesShapeResultOnObserveRetiredKindsError(t *testing.T) {
	call := 0
	queryQueue := func(context.Context, string, ...any) (pgx.Rows, error) {
		call++
		switch call {
		case 1:
			return &fakeStrandSurveyRows{rows: [][]any{
				{"11111111-1111-1111-8111-111111111111", int64(1), "workgraph.build", "dedupe-a", dispositionSkipJobLive},
			}}, nil
		case 2:
			return nil, errors.New("observe retired kinds query failed")
		default:
			t.Fatalf("unexpected queryQueue call #%d", call)
			return nil, nil
		}
	}
	repair := &StrandRepair{
		beginQueue:  func(context.Context) (pgx.Tx, error) { return nil, errors.New("unused") },
		queryQueue:  queryQueue,
		queryDomain: func(context.Context, string, ...any) (pgx.Rows, error) { return nil, errors.New("unused") },
		client:      riverDeleteAdapter{},
		shapes: []strandShape{
			{name: "a", survey: "SELECT 1", lock: "SELECT 1"},
		},
	}

	result, err := repair.Step(context.Background(), time.Now(), 1)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Step() error = %v, want ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "observe retired kinds") {
		t.Fatalf("Step() error = %v, want it to name the observe-retired-kinds stage", err)
	}
	if result.SkippedJobLive != 1 {
		t.Fatalf(
			"Step() result = %+v, want shape a's SkippedJobLive=1 preserved despite observeRetiredKinds' error",
			result,
		)
	}
}
