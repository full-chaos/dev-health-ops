//go:build integration

package fixed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The DDL mirrors the shape alembic revisions 0001, 0005, 0053 and 0056 leave
// behind for the four tables this producer touches. It is repeated here rather
// than executed through Alembic so the Go integration test has no Python runtime
// dependency, and it deliberately keeps the parts that constrain the producer:
//
//   - the circular foreign keys between report_runs.scheduled_occurrence_id and
//     scheduled_report_occurrences.report_run_id, which are immediate, so they
//     pin the write ORDER the producer must use;
//   - unique (report_id, scheduled_for) and unique report_run_id on the
//     occurrence table, which are what make a second materialization of one due
//     time impossible rather than merely unlikely;
//   - saved_reports.schedule_id having NO unique constraint, because the
//     ambiguous-schedule case the producer refuses is only reachable without it.
const scheduledReportDDL = `
CREATE TABLE public.organizations (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE public.scheduled_jobs (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    job_type TEXT NOT NULL,
    provider TEXT NOT NULL DEFAULT '',
    schedule_cron TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    job_config JSON NOT NULL DEFAULT '{}',
    status INTEGER NOT NULL DEFAULT 0,
    is_running BOOLEAN NOT NULL DEFAULT FALSE,
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE public.saved_reports (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    report_plan JSON NOT NULL DEFAULT '{}',
    is_template BOOLEAN NOT NULL DEFAULT FALSE,
    schedule_id UUID REFERENCES public.scheduled_jobs (id) ON DELETE SET NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at TIMESTAMPTZ,
    last_run_status TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE public.report_runs (
    id UUID PRIMARY KEY,
    report_id UUID NOT NULL REFERENCES public.saved_reports (id) ON DELETE CASCADE,
    scheduled_occurrence_id TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    rendered_markdown TEXT,
    artifact_url TEXT,
    provenance_records JSON,
    error TEXT,
    error_traceback TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    artifact_fingerprint TEXT,
    notification_key TEXT UNIQUE,
    notification_status TEXT NOT NULL DEFAULT 'pending',
    notification_sent_at TIMESTAMPTZ,
    notification_claim_token UUID,
    notification_lease_expires_at TIMESTAMPTZ,
    triggered_by TEXT NOT NULL DEFAULT 'manual',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE public.scheduled_report_occurrences (
    occurrence_id TEXT PRIMARY KEY,
    identity_version TEXT NOT NULL,
    org_id TEXT NOT NULL,
    report_id UUID NOT NULL REFERENCES public.saved_reports (id) ON DELETE CASCADE,
    scheduled_job_id UUID NOT NULL REFERENCES public.scheduled_jobs (id) ON DELETE CASCADE,
    scheduled_for TIMESTAMPTZ NOT NULL,
    report_run_id UUID UNIQUE REFERENCES public.report_runs (id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_scheduled_report_occurrence_report_time UNIQUE (report_id, scheduled_for)
);
ALTER TABLE public.report_runs
    ADD CONSTRAINT fk_report_runs_scheduled_occurrence
    FOREIGN KEY (scheduled_occurrence_id)
    REFERENCES public.scheduled_report_occurrences (occurrence_id) ON DELETE SET NULL;
CREATE TABLE public.worker_job_outbox (
    id UUID PRIMARY KEY,
    dedupe_key TEXT NOT NULL UNIQUE,
    job_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

const (
	testOrganizationID = "2f1a5c88-9d0e-4b3a-8c71-5e6f7a8b9c01"
	testJobID          = "6b3c1d2e-4f50-4a6b-9c8d-0e1f2a3b4c5d"
	testReportID       = "8d4e2f10-5a61-4b7c-8d9e-1f2a3b4c5d6e"
)

func startScheduledReportPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := startLedgerPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, scheduledReportDDL); err != nil {
		t.Fatal(err)
	}
	return pool
}

// seedScheduledReport installs one active organization, one active report
// schedule, and one active report that has never run.
func seedScheduledReport(t *testing.T, pool *pgxpool.Pool, cron, timezone string, createdAt time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.organizations (id, name, is_active) VALUES ($1::uuid, 'acme', TRUE)`,
		testOrganizationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_jobs
    (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
VALUES ($1::uuid, $2, 'report:weekly', 'report', $3, $4, 0, FALSE, $5, $5)`,
		testJobID, testOrganizationID, cron, timezone, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'weekly health', $3::uuid, TRUE, $4, $4)`,
		testReportID, testOrganizationID, testJobID, createdAt); err != nil {
		t.Fatal(err)
	}
}

func reportsProducer(t *testing.T) *ScheduledReportsProducer {
	t.Helper()
	producer, err := NewScheduledReportsProducer(schedulersync.NextOccurrence)
	if err != nil {
		t.Fatal(err)
	}
	concrete, ok := producer.(*ScheduledReportsProducer)
	if !ok {
		t.Fatalf("producer type = %T", producer)
	}
	return concrete
}

// reportOccurrence builds the fixed-schedule occurrence for a chosen due time on
// the real declared schedule, so cadence, target kind and identity version all
// come from the checked-in table rather than from the test.
func reportOccurrence(t *testing.T, dueAt time.Time) (Schedule, Occurrence) {
	t.Helper()
	schedule := scheduleByID(t, scheduledReportsScheduleID)
	return schedule, NewOccurrence(schedule, dueAt, dueAt.Add(30*time.Second))
}

func produceInTransaction(
	t *testing.T,
	pool *pgxpool.Pool,
	schedule Schedule,
	occurrence Occurrence,
) (Outcome, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outcome, produceErr := reportsProducer(t).Produce(ctx, tx, schedule, occurrence)
	if produceErr != nil {
		_ = tx.Rollback(ctx)
		return Outcome{}, produceErr
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return outcome, nil
}

type persistedReportState struct {
	Occurrences     int
	Runs            int
	OccurrenceID    string
	IdentityVersion string
	RunID           string
	ScheduledFor    time.Time
	Status          string
	TriggeredBy     string
	LinkedRunID     *string
	NextRunAt       *time.Time
}

func readReportState(t *testing.T, pool *pgxpool.Pool) persistedReportState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var state persistedReportState
	if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM public.scheduled_report_occurrences),
       (SELECT count(*) FROM public.report_runs),
       (SELECT next_run_at FROM public.scheduled_jobs WHERE id = $1::uuid)`,
		testJobID).Scan(&state.Occurrences, &state.Runs, &state.NextRunAt); err != nil {
		t.Fatal(err)
	}
	if state.Occurrences == 0 {
		return state
	}
	if err := pool.QueryRow(ctx, `
SELECT occurrence.occurrence_id, occurrence.identity_version, occurrence.scheduled_for,
       occurrence.report_run_id::text, run.id::text, run.status, run.triggered_by
FROM public.scheduled_report_occurrences AS occurrence
LEFT JOIN public.report_runs AS run ON run.scheduled_occurrence_id = occurrence.occurrence_id
ORDER BY occurrence.scheduled_for
LIMIT 1`).Scan(
		&state.OccurrenceID, &state.IdentityVersion, &state.ScheduledFor, &state.LinkedRunID,
		&state.RunID, &state.Status, &state.TriggeredBy,
	); err != nil {
		t.Fatal(err)
	}
	state.ScheduledFor = state.ScheduledFor.UTC()
	return state
}

// A due report must produce exactly one occurrence, one pending scheduler run
// linked to it in both directions, an advanced next_run_at, and one handoff
// whose envelope satisfies the compiled contract.
func TestDueScheduledReportMaterializesOneRunAndOneHandoff(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	dueAt := time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC)
	schedule, occurrence := reportOccurrence(t, dueAt)
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce(): %v", err)
	}
	if len(outcome.Requests) != 1 || outcome.Handoffs != 0 || outcome.SkipReason != "" {
		t.Fatalf("outcome = %+v, want exactly one request", outcome)
	}
	request := outcome.Requests[0]
	if request.Kind != jobcontract.KindReportExecuteScheduled {
		t.Fatalf("request kind = %q", request.Kind)
	}
	if request.Kind != schedule.TargetKind {
		t.Fatalf("request kind %q does not match the schedule target %q", request.Kind, schedule.TargetKind)
	}
	// Same strict round-trip the other producers are held to: canonical marshal
	// then a decode that enforces the compiled payload schema.
	encoded, err := jobcontract.MarshalCanonical(request.Envelope)
	if err != nil {
		t.Fatalf("produced envelope is not contract-valid: %v", err)
	}
	if _, err := jobcontract.Decode(request.Kind, encoded); err != nil {
		t.Fatalf("produced envelope failed strict decode: %v", err)
	}

	state := readReportState(t, pool)
	if state.Occurrences != 1 || state.Runs != 1 {
		t.Fatalf("persisted %d occurrences and %d runs, want one of each", state.Occurrences, state.Runs)
	}
	// Python schedules the occurrence at the cron instant, not at the tick that
	// noticed it. 06:00 UTC on the 25th is the next fire after the 24th 06:00
	// creation, and the 06:05 tick is what observed it.
	wantScheduledFor := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	if !state.ScheduledFor.Equal(wantScheduledFor) {
		t.Fatalf("occurrence scheduled_for = %s, want %s", state.ScheduledFor, wantScheduledFor)
	}
	// From the checked-in Python oracle, NOT from a second call to the Go
	// derivation: comparing the producer's output against the same function that
	// produced it cannot fail.
	if want := pythonOccurrenceID(t, testReportID, wantScheduledFor); state.OccurrenceID != want {
		t.Fatalf("occurrence id = %s, Python authority = %s", state.OccurrenceID, want)
	}
	if state.IdentityVersion != "report_scheduler_occurrence_v1" {
		t.Fatalf(
			"persisted identity version = %q, want the Python wire value",
			state.IdentityVersion,
		)
	}
	if state.LinkedRunID == nil || *state.LinkedRunID != state.RunID {
		t.Fatalf("occurrence links run %v but the run is %s", state.LinkedRunID, state.RunID)
	}
	// Literals, not the production constants: comparing against
	// scheduledReportTriggeredBy would make this assertion follow the value it is
	// supposed to pin. "scheduler" is behavioral -- reports.execution_trigger's
	// _payload_for_run selects the scheduled payload shape from it.
	if state.Status != "pending" || state.TriggeredBy != "scheduler" {
		t.Fatalf("run status/trigger = %q/%q, want pending/scheduler", state.Status, state.TriggeredBy)
	}
	if request.Envelope.IdempotencyKey != "report.run:"+state.RunID {
		t.Fatalf(
			"idempotency key = %q, want the Python outbox dedupe key for run %s",
			request.Envelope.IdempotencyKey, state.RunID,
		)
	}
	if request.Envelope.Domain.ID != state.RunID || request.Envelope.Domain.Type != "report_run" {
		t.Fatalf("domain link = %+v, want the report run", request.Envelope.Domain)
	}
	// next_run_at must be the fire AFTER the one just scheduled, matching
	// report_scheduler.py, and must never be left at the due time itself.
	wantNextRun := time.Date(2026, time.July, 26, 6, 0, 0, 0, time.UTC)
	if state.NextRunAt == nil || !state.NextRunAt.UTC().Equal(wantNextRun) {
		t.Fatalf("next_run_at = %v, want %s", state.NextRunAt, wantNextRun)
	}
}

// Replaying the same occurrence window must write nothing further. This is the
// crash-window case: the engine's claim makes a repeat rare, not impossible.
func TestRepeatedOccurrenceWindowMaterializesNothingFurther(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	dueAt := time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC)
	schedule, occurrence := reportOccurrence(t, dueAt)
	if _, err := produceInTransaction(t, pool, schedule, occurrence); err != nil {
		t.Fatalf("first Produce(): %v", err)
	}
	first := readReportState(t, pool)
	// The engine, not the producer, publishes the handoff, so a direct Produce
	// leaves none behind. Seed the live row the engine would have written:
	// without it replay would correctly RE-ARM the pruned handoff, which is a
	// different property (TestReplayRearmsAPendingRunWhoseHandoffWasPruned).
	seedHandoff(t, pool, first.RunID, "pending")

	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("second Produce(): %v", err)
	}
	if len(outcome.Requests) != 0 {
		t.Fatalf("replay produced %d handoffs, want none", len(outcome.Requests))
	}
	if outcome.SkipReason != SkipNoDueScheduledReportsClaimed {
		t.Fatalf("replay skip reason = %q, want %q", outcome.SkipReason, SkipNoDueScheduledReportsClaimed)
	}
	second := readReportState(t, pool)
	if second.Occurrences != 1 || second.Runs != 1 {
		t.Fatalf("replay persisted %d occurrences and %d runs", second.Occurrences, second.Runs)
	}
	if second.RunID != first.RunID || second.OccurrenceID != first.OccurrenceID {
		t.Fatalf("replay re-identified the work: %+v then %+v", first, second)
	}
}

// A LATER tick, still before the report completes, must also produce nothing.
// last_run_at only advances on completion, so due-ness keeps resolving to the
// same cron instant; the occurrence table is the only thing standing between
// that and a dispatch every five minutes forever.
func TestLaterTickBeforeCompletionDoesNotRedispatch(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	if _, err := produceInTransaction(t, pool, schedule, occurrence); err != nil {
		t.Fatalf("first Produce(): %v", err)
	}
	// Stand in for the engine's publication, so this test measures re-dispatch
	// suppression rather than the pruned-handoff re-arm path.
	seedHandoff(t, pool, readReportState(t, pool).RunID, "pending")
	_, laterOccurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 20, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, laterOccurrence)
	if err != nil {
		t.Fatalf("later Produce(): %v", err)
	}
	if len(outcome.Requests) != 0 || outcome.SkipReason != SkipNoDueScheduledReportsClaimed {
		t.Fatalf("a later tick re-dispatched: %+v", outcome)
	}
	if state := readReportState(t, pool); state.Runs != 1 {
		t.Fatalf("persisted %d runs after a later tick, want one", state.Runs)
	}
}

// After the report completes, the NEXT cron instant is a distinct occurrence and
// must produce. Without this, proving non-redispatch above would be satisfied by
// a producer that never fires twice at all.
func TestNextCronInstantAfterCompletionProducesAgain(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	if _, err := produceInTransaction(t, pool, schedule, occurrence); err != nil {
		t.Fatalf("first Produce(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// The report handler is what advances last_run_at; simulate its completion.
	if _, err := pool.Exec(ctx, `
UPDATE public.saved_reports SET last_run_at = $2, last_run_status = 'success' WHERE id = $1::uuid`,
		testReportID, time.Date(2026, time.July, 25, 6, 1, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	_, tomorrow := reportOccurrence(t, time.Date(2026, time.July, 26, 6, 5, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, tomorrow)
	if err != nil {
		t.Fatalf("second day Produce(): %v", err)
	}
	if len(outcome.Requests) != 1 {
		t.Fatalf("the next cron instant produced %d handoffs, want one", len(outcome.Requests))
	}
	var occurrences, runs int
	if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM public.scheduled_report_occurrences),
       (SELECT count(*) FROM public.report_runs)`).Scan(&occurrences, &runs); err != nil {
		t.Fatal(err)
	}
	if occurrences != 2 || runs != 2 {
		t.Fatalf("persisted %d occurrences and %d runs across two days", occurrences, runs)
	}
}

// Two transactions racing one due time is the replica-safety case, and also the
// coexistence case: the Python dispatcher writes into this same table for the
// whole cutover window. Exactly one must materialize.
//
// This is what replaces the Python path's read-then-insert under a lock. A
// producer that had checked for an existing occurrence and then inserted would
// fail here with a duplicate key instead of reporting a clean skip.
func TestTwoConcurrentSweepsMaterializeOneRunForOneDueTime(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))

	// TRUE concurrency, with a barrier. An earlier version of this test began both
	// transactions up front and then ran them one after the other, which is not a
	// race at all: under READ COMMITTED every statement takes a fresh snapshot, so
	// beginning tx2 early preserves nothing, and by the time it ran its first
	// statement tx1 had already committed. It saw the winning row like any ordinary
	// sequential reader, and an unsafe check-then-insert implementation passed it.
	//
	// The discriminating condition is that both transactions must have issued their
	// occurrence INSERT before either commits, so the second blocks on the first's
	// uncommitted unique-key row. A check-then-insert implementation then finds no
	// row in its check, inserts anyway, and fails with a unique violation instead of
	// reporting a clean duplicate.
	producer := reportsProducer(t)
	type sweepResult struct {
		outcome Outcome
		err     error
	}
	results := make(chan sweepResult, 2)
	started := make(chan struct{}, 2)
	release := make(chan struct{})

	for range 2 {
		go func() {
			tx, err := pool.Begin(ctx)
			if err != nil {
				started <- struct{}{}
				results <- sweepResult{err: err}
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			// Establish this transaction and take its report lock BEFORE announcing
			// readiness, so both are genuinely in flight at the barrier.
			outcome, produceErr := producer.Produce(ctx, tx, schedule, occurrence)
			started <- struct{}{}
			<-release
			if produceErr != nil {
				results <- sweepResult{err: produceErr}
				return
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				results <- sweepResult{err: commitErr}
				return
			}
			results <- sweepResult{outcome: outcome}
		}()
	}

	// Both goroutines have produced and neither has committed: one of them is
	// blocked on the other's uncommitted occurrence row. Releasing lets both commit.
	<-started
	<-started
	close(release)

	var outcomes []Outcome
	for range 2 {
		result := <-results
		if result.err != nil {
			// One sweep legitimately losing the report lock (SKIP LOCKED) or the
			// occurrence claim is fine; an unexpected error is not.
			t.Fatalf("a racing sweep failed: %v", result.err)
		}
		outcomes = append(outcomes, result.outcome)
	}

	// Exactly one sweep may MATERIALIZE the occurrence, and the durable graph is
	// what must be singular. A loser may still emit a handoff for the winner's run
	// through the re-arm path, which is harmless: the outbox dedupe key is the run
	// id, so both publications collapse to one row.
	state := readReportState(t, pool)
	if state.Occurrences != 1 || state.Runs != 1 {
		t.Fatalf("racing sweeps persisted %d occurrences and %d runs", state.Occurrences, state.Runs)
	}
	published := 0
	for _, outcome := range outcomes {
		for _, request := range outcome.Requests {
			published++
			if request.Envelope.IdempotencyKey != "report.run:"+state.RunID {
				t.Fatalf(
					"a racing sweep published %q, not the single durable run %s",
					request.Envelope.IdempotencyKey, state.RunID,
				)
			}
		}
	}
	if published == 0 {
		t.Fatal("two racing sweeps produced no handoff at all")
	}
}

// The saved_reports FOR UPDATE lock is real and observable. An on-demand trigger
// holding it defers this report to the next tick rather than letting the sweep
// read a report another transaction is about to deactivate.
//
// This is the test that keeps the lock honest: delete `, report` from the sweep's
// FOR UPDATE clause and the sweep stops skipping, producing a handoff for a
// report whose row is concurrently held.
func TestReportLockedByAnOnDemandTriggerIsDeferredNotProduced(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	onDemand, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Exactly what reports.execution_trigger._lock_report does.
	if _, err := onDemand.Exec(ctx, `
SELECT id FROM public.saved_reports WHERE id = $1::uuid FOR UPDATE`, testReportID); err != nil {
		_ = onDemand.Rollback(ctx)
		t.Fatal(err)
	}

	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	sweep, err := pool.Begin(ctx)
	if err != nil {
		_ = onDemand.Rollback(ctx)
		t.Fatal(err)
	}
	outcome, produceErr := reportsProducer(t).Produce(ctx, sweep, schedule, occurrence)
	if produceErr != nil {
		_ = sweep.Rollback(ctx)
		_ = onDemand.Rollback(ctx)
		t.Fatalf("Produce() while the report was locked: %v", produceErr)
	}
	if err := sweep.Commit(ctx); err != nil {
		_ = onDemand.Rollback(ctx)
		t.Fatal(err)
	}
	if err := onDemand.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if len(outcome.Requests) != 0 {
		t.Fatalf("a locked report produced %d handoffs, want none", len(outcome.Requests))
	}
	if outcome.SkipReason != SkipNoDueScheduledReports {
		t.Fatalf("skip reason = %q, want %q", outcome.SkipReason, SkipNoDueScheduledReports)
	}
	if state := readReportState(t, pool); state.Occurrences != 0 || state.Runs != 0 {
		t.Fatalf("a locked report was materialized: %+v", state)
	}
	// The next tick, with the lock released, must pick it up: deferral is not loss.
	_, later := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 10, 0, 0, time.UTC))
	recovered, err := produceInTransaction(t, pool, schedule, later)
	if err != nil {
		t.Fatalf("recovery Produce(): %v", err)
	}
	if len(recovered.Requests) != 1 {
		t.Fatalf("the deferred report was not recovered on the next tick: %+v", recovered)
	}
}

// Every gate the Python sweep applies must exclude the row, and the sweep must
// report "nothing due" rather than failing.
func TestScheduledReportSweepGates(t *testing.T) {
	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name    string
		mutate  string
		produce bool
	}{
		{"paused schedule", `UPDATE public.scheduled_jobs SET status = 1`, false},
		{"disabled schedule", `UPDATE public.scheduled_jobs SET status = 2`, false},
		{"schedule already running", `UPDATE public.scheduled_jobs SET is_running = TRUE`, false},
		{"non-report job type", `UPDATE public.scheduled_jobs SET job_type = 'sync'`, false},
		{"inactive report", `UPDATE public.saved_reports SET is_active = FALSE`, false},
		{"report detached from its schedule", `UPDATE public.saved_reports SET schedule_id = NULL`, false},
		{"deleted organization", `DELETE FROM public.organizations`, false},
		// organization_exists_sync is fail-open for these three, so the row must
		// still be swept. A stricter Go gate would silently stop dispatching for
		// every single-tenant and legacy installation.
		// Both sides move together. The job and its report must stay in the SAME
		// organization or the cross-tenant guard excludes the row first and these
		// subtests would pass for the wrong reason -- they are about organization
		// EXISTENCE, not ownership, which TestCrossTenantJobAndReportAreNeverPaired
		// covers separately.
		{"default organization sentinel", `UPDATE public.scheduled_jobs SET org_id = 'default';
			UPDATE public.saved_reports SET org_id = 'default'`, true},
		{"empty organization", `UPDATE public.scheduled_jobs SET org_id = '';
			UPDATE public.saved_reports SET org_id = ''`, true},
		{"non-uuid organization", `UPDATE public.scheduled_jobs SET org_id = 'acme-legacy';
			UPDATE public.saved_reports SET org_id = 'acme-legacy'`, true},
		// Uppercase is UUID-shaped, so it takes the EXISTS arm and must still
		// resolve through the lower() normalization rather than silently missing.
		{"uppercase organization uuid", `UPDATE public.scheduled_jobs SET org_id = upper(org_id);
			UPDATE public.saved_reports SET org_id = upper(org_id)`, true},
		{"unchanged control", `SELECT 1`, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pool := startScheduledReportPostgres(t)
			seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := pool.Exec(ctx, testCase.mutate); err != nil {
				t.Fatal(err)
			}
			outcome, err := produceInTransaction(t, pool, schedule, occurrence)
			if err != nil {
				t.Fatalf("Produce(): %v", err)
			}
			produced := len(outcome.Requests) == 1
			if produced != testCase.produce {
				t.Fatalf("produced %d handoffs, want produce=%v", len(outcome.Requests), testCase.produce)
			}
			if !testCase.produce && outcome.SkipReason != SkipNoDueScheduledReports {
				t.Fatalf("skip reason = %q, want %q", outcome.SkipReason, SkipNoDueScheduledReports)
			}
		})
	}
}

// A report whose cron has not yet come due is a bounded skip, not work. This is
// the overwhelmingly common outcome of a 300 second sweep.
func TestNotYetDueScheduledReportIsABoundedSkip(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce(): %v", err)
	}
	if len(outcome.Requests) != 0 || outcome.SkipReason != SkipNoDueScheduledReports {
		t.Fatalf("outcome = %+v, want a bounded not-due skip", outcome)
	}
	if state := readReportState(t, pool); state.Occurrences != 0 || state.Runs != 0 {
		t.Fatalf("a not-due report was materialized: %+v", state)
	}
}

// A cron expression the evaluator cannot resolve fails the occurrence loudly and
// names the offending schedule. The report write path validates the timezone and
// not the cron, so this is reachable tenant data; silently skipping it would let
// one tenant's reports stop while the schedule stayed green.
func TestUnevaluableCronFailsTheOccurrence(t *testing.T) {
	for _, expression := range []string{"not-a-cron", "", "0 6 * * * *", "@daily", "99 * * * *"} {
		t.Run(expression, func(t *testing.T) {
			pool := startScheduledReportPostgres(t)
			createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
			seedScheduledReport(t, pool, expression, "UTC", createdAt)

			schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
			_, err := produceInTransaction(t, pool, schedule, occurrence)
			if err == nil {
				t.Fatal("an unevaluable cron produced a result instead of failing")
			}
			if !errors.Is(err, ErrScheduledReportConfiguration) {
				t.Fatalf("error = %v, want a scheduled report configuration failure", err)
			}
			if state := readReportState(t, pool); state.Occurrences != 0 || state.Runs != 0 {
				t.Fatalf("a failed occurrence left rows behind: %+v", state)
			}
		})
	}
}

// An unknown timezone must fall back to UTC and still dispatch, matching
// cron_next_run's documented runtime defense. Failing instead would stop
// dispatch for a legacy or corrupt row that Python still serves.
func TestUnknownTimezoneFallsBackToUTCRatherThanFailing(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "Mars/Olympus", createdAt)

	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce(): %v", err)
	}
	if len(outcome.Requests) != 1 {
		t.Fatalf("outcome = %+v, want one handoff under the UTC fallback", outcome)
	}
	state := readReportState(t, pool)
	wantScheduledFor := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	if !state.ScheduledFor.Equal(wantScheduledFor) {
		t.Fatalf("scheduled_for = %s, want the UTC interpretation %s", state.ScheduledFor, wantScheduledFor)
	}
}

// A schedule owning two active reports is refused against real PostgreSQL, where
// the missing unique constraint on saved_reports.schedule_id makes it reachable.
func TestScheduleOwningTwoActiveReportsFailsTheOccurrence(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.saved_reports (id, org_id, name, schedule_id, is_active, created_at, updated_at)
VALUES ('7c3d1e2f-4a5b-4c6d-8e9f-0a1b2c3d4e5f'::uuid, $1, 'second', $2::uuid, TRUE, $3, $3)`,
		testOrganizationID, testJobID, createdAt); err != nil {
		t.Fatal(err)
	}
	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	_, err := produceInTransaction(t, pool, schedule, occurrence)
	if err == nil {
		t.Fatal("an ambiguous report schedule produced a result")
	}
	if !errors.Is(err, ErrScheduledReportConflict) {
		t.Fatalf("error = %v, want a scheduled report conflict", err)
	}
}

// A persisted occurrence whose identity fields disagree with the derived one is a
// derivation change, never a retryable condition.
func TestConflictingPersistedOccurrenceIsRefused(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	wantScheduledFor := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	// A foreign key under the correct identity, but claiming a different
	// organization: exactly what a changed derivation input looks like.
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_report_occurrences
    (occurrence_id, identity_version, org_id, report_id, scheduled_job_id, scheduled_for)
VALUES ($1, $2, 'some-other-org', $3::uuid, $4::uuid, $5)`,
		ScheduledReportOccurrenceID(testReportID, wantScheduledFor),
		scheduledReportOccurrenceIdentityVersion, testReportID, testJobID, wantScheduledFor,
	); err != nil {
		t.Fatal(err)
	}
	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	_, err := produceInTransaction(t, pool, schedule, occurrence)
	if err == nil {
		t.Fatal("a conflicting persisted occurrence was accepted")
	}
	if !errors.Is(err, ErrScheduledReportConflict) {
		t.Fatalf("error = %v, want a scheduled report conflict", err)
	}
}

// The whole occurrence must commit together or not at all: a rolled-back
// transaction leaves no run, no occurrence, and no advanced next_run_at, so the
// next tick is free to produce the work for real.
func TestRolledBackOccurrenceLeavesNoPartialReportGraph(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := reportsProducer(t).Produce(ctx, tx, schedule, occurrence)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.Requests) != 1 {
		t.Fatalf("outcome = %+v, want one handoff before the rollback", outcome)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if state := readReportState(t, pool); state.Occurrences != 0 || state.Runs != 0 || state.NextRunAt != nil {
		t.Fatalf("a rolled-back occurrence left state behind: %+v", state)
	}
	recovered, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce() after rollback: %v", err)
	}
	if len(recovered.Requests) != 1 {
		t.Fatalf("the rolled-back occurrence was not recoverable: %+v", recovered)
	}
}

// The whole engine path, on real PostgreSQL, through the real occurrence ledger:
// claim, produce, publish, and record must all commit in one transaction, and a
// second window at the same instant must be a duplicate rather than a second
// dispatch.
func TestEngineCommitsTheReportGraphAndTheOccurrenceTogether(t *testing.T) {
	// startLedgerPostgres already applies fixedScheduleOccurrenceDDL, so the
	// real occurrence ledger is available without repeating it here.
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	schedule := scheduleByID(t, scheduledReportsScheduleID)
	location, err := schedule.Location()
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.July, 25, 6, 5, 30, 0, time.UTC)
	dueAt, ok := schedule.Cadence.Previous(observedAt, location)
	if !ok {
		t.Fatal("the report schedule cadence resolved no due time")
	}
	// Seed the anchor so the window is real work rather than a cold-start
	// baseline, one full interval back so the interval anchoring rule is met.
	previous := NewOccurrence(schedule, dueAt.Add(-schedule.Cadence.Period()), dueAt.Add(-schedule.Cadence.Period()))
	seedRecordedOccurrence(t, ctx, pool, previous)

	engine, publisher := newReportEngine(t, pool, schedule)
	result, err := engine.Step(ctx, observedAt)
	if err != nil {
		t.Fatalf("Step(): %v", err)
	}
	scheduleResult := resultFor(t, result, schedule.ID)
	if scheduleResult.Err != nil {
		t.Fatalf("schedule failed: %v", scheduleResult.Err)
	}
	if scheduleResult.Claimed != 1 || scheduleResult.Handoffs != 1 || scheduleResult.Skipped != 0 {
		t.Fatalf("schedule result = %+v, want one claim and one handoff", scheduleResult)
	}
	if publisher.count() != 1 {
		t.Fatalf("published %d handoffs, want one", publisher.count())
	}
	state := readReportState(t, pool)
	if state.Occurrences != 1 || state.Runs != 1 {
		t.Fatalf("engine persisted %d occurrences and %d runs", state.Occurrences, state.Runs)
	}
	if status := recordedOccurrenceStatus(t, ctx, pool, NewOccurrence(schedule, dueAt, observedAt).Key); status != OccurrenceMaterialized {
		t.Fatalf("ledger status = %q, want %q", status, OccurrenceMaterialized)
	}

	// The same window again is already owned: no second claim, no second run.
	repeat, err := engine.Step(ctx, observedAt)
	if err != nil {
		t.Fatalf("second Step(): %v", err)
	}
	repeatResult := resultFor(t, repeat, schedule.ID)
	if repeatResult.Due != 0 || repeatResult.Claimed != 0 {
		t.Fatalf("the second window re-ran an owned occurrence: %+v", repeatResult)
	}
	if publisher.count() != 1 {
		t.Fatalf("published %d handoffs across two windows, want one", publisher.count())
	}
	if state := readReportState(t, pool); state.Runs != 1 {
		t.Fatalf("persisted %d runs across two windows", state.Runs)
	}
}

func newReportEngine(t *testing.T, pool *pgxpool.Pool, schedule Schedule) (*Engine, *recordingPublisher) {
	t.Helper()
	publisher := &recordingPublisher{}
	producer, err := NewScheduledReportsProducer(schedulersync.NextOccurrence)
	if err != nil {
		t.Fatal(err)
	}
	producers, err := NewProducerSet(producer)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(EngineConfig{
		Schedules: []Schedule{schedule},
		Producers: producers,
		Ledger:    NewPostgresLedger(),
		Publisher: publisher,
		Registry:  testRegistry(t),
		Pool:      pool,
	})
	if err != nil {
		t.Fatal(err)
	}
	return engine, publisher
}

func seedRecordedOccurrence(t *testing.T, ctx context.Context, pool *pgxpool.Pool, occurrence Occurrence) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.fixed_schedule_occurrences
    (occurrence_key, identity_version, schedule_id, target_kind, scheduled_for, observed_at,
     status, handoff_count, skip_reason, completed_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 'skipped', 0, 'seeded_anchor', $6, $6, $6)`,
		occurrence.Key, occurrence.IdentityVersion, occurrence.ScheduleID,
		occurrence.TargetKind, occurrence.ScheduledFor, occurrence.ObservedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func recordedOccurrenceStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `
SELECT status FROM public.fixed_schedule_occurrences WHERE occurrence_key = $1`, key).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ""
		}
		t.Fatal(err)
	}
	return status
}

func resultFor(t *testing.T, window WindowResult, scheduleID string) ScheduleResult {
	t.Helper()
	for _, result := range window.Schedules {
		if result.ScheduleID == scheduleID {
			return result
		}
	}
	t.Fatalf("window carried no result for schedule %s", scheduleID)
	return ScheduleResult{}
}
