//go:build integration

package fixed

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// seedHandoff writes the outbox row the engine would have published for a run, in
// the state under test. The producer never writes this table itself — the engine's
// publisher owns it — so seeding is how these tests reach the states the relay and
// outbox retention produce over time.
func seedHandoff(t *testing.T, pool *pgxpool.Pool, runID, status string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.worker_job_outbox (id, dedupe_key, job_kind, status)
VALUES (gen_random_uuid(), $1, 'report.execute_scheduled', $2)`,
		"report.run:"+runID, status); err != nil {
		t.Fatal(err)
	}
}

func handoffCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.worker_job_outbox`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func clearHandoffs(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, `DELETE FROM public.worker_job_outbox`); err != nil {
		t.Fatal(err)
	}
}

// dueScheduledReport materializes one report and returns its run id, leaving the
// occurrence durable so the replay paths below can be exercised against it.
func dueScheduledReport(t *testing.T, pool *pgxpool.Pool) (Schedule, Occurrence, string) {
	t.Helper()
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("initial Produce(): %v", err)
	}
	// Checked here too so a setup failure names the real cause. Without it, a defect
	// that duplicates handoffs fails this helper with "want one handoff", which reads
	// like a seeding problem and hides that the duplicate is the finding.
	assertNoDuplicateHandoffs(t, outcome)
	if len(outcome.Requests) != 1 {
		t.Fatalf("initial outcome = %+v, want one handoff", outcome)
	}
	return schedule, occurrence, readReportState(t, pool).RunID
}

// A pending run with no durable evidence that its handoff was published is
// re-armed. The abandonment ledger makes this absence unambiguous: terminal
// retention cannot produce the same state.
func TestReplayRearmsAPendingRunWhoseHandoffWasNeverPublished(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	schedule, occurrence, runID := dueScheduledReport(t, pool)
	// Calling the producer directly leaves both delivery stores empty and models
	// the pre-existing/coexistence state replay must repair.
	if handoffCount(t, pool) != 0 {
		t.Fatal("the producer wrote an outbox row itself; the engine owns that")
	}
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("replay Produce(): %v", err)
	}
	if len(outcome.Requests) != 1 {
		t.Fatalf("replay outcome = %+v, want the handoff re-armed", outcome)
	}
	if outcome.Requests[0].Envelope.IdempotencyKey != "report.run:"+runID {
		t.Fatalf("re-armed handoff targets %q, not run %s",
			outcome.Requests[0].Envelope.IdempotencyKey, runID)
	}
	// Re-arming must repair delivery without duplicating the durable graph.
	state := readReportState(t, pool)
	if state.Occurrences != 1 || state.Runs != 1 || state.RunID != runID {
		t.Fatalf("re-arming duplicated the report graph: %+v", state)
	}
}

// A pending run whose handoff is still live must NOT be re-armed: the relay owns
// it, and a second publication is a second delivery path for guaranteed work.
func TestReplayLeavesALiveHandoffAlone(t *testing.T) {
	for _, status := range []string{"pending", "delivered"} {
		t.Run(status, func(t *testing.T) {
			pool := startScheduledReportPostgres(t)
			schedule, occurrence, runID := dueScheduledReport(t, pool)
			clearHandoffs(t, pool)
			seedHandoff(t, pool, runID, status)
			outcome, err := produceInTransaction(t, pool, schedule, occurrence)
			if err != nil {
				t.Fatalf("replay Produce(): %v", err)
			}
			if len(outcome.Requests) != 0 {
				t.Fatalf("a %s handoff was re-armed: %+v", status, outcome)
			}
			if outcome.SkipReason != SkipNoDueScheduledReportsClaimed {
				t.Fatalf("skip reason = %q, want %q", outcome.SkipReason, SkipNoDueScheduledReportsClaimed)
			}
		})
	}
}

// A pending run whose handoff spent its delivery budget is reported as a DEGRADED
// condition, not as an ordinary skip and not as a failure.
//
// It must not be re-armed: publishing against a spent budget collapses on the
// outbox dedupe key and delivers nothing while reporting success.
func TestSpentHandoffIsReportedAsDegradedNotSkippedSilently(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	schedule, occurrence, runID := dueScheduledReport(t, pool)
	seedHandoff(t, pool, runID, "dead")
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("a spent handoff failed the occurrence: %v", err)
	}
	if len(outcome.Requests) != 0 {
		t.Fatalf("a spent handoff was re-armed: %+v", outcome)
	}
	if outcome.Degraded != DegradedScheduledReportsUndeliverable {
		t.Fatalf("degraded = %q, want %q", outcome.Degraded, DegradedScheduledReportsUndeliverable)
	}
	// It must NOT look like a routine idle tick.
	if outcome.SkipReason == SkipNoDueScheduledReports {
		t.Fatal("a stranded run was recorded as though nothing had been due")
	}
	_ = runID
}

// THE REGRESSION FIX: one tenant's stranded run must not fail another tenant's
// occurrence.
//
// Before this, ErrScheduledReportUndeliverable propagated out of Produce, so the
// engine rolled the transaction back and discarded the healthy tenant's run and
// handoff — on every tick, permanently, because last_run_at never advances while a
// run is pending. A single stranded row closed the schedule's readiness for
// everyone. Python is strictly more available here: it dispatches each row
// independently and bypasses the outbox entirely.
func TestOneTenantsStrandedRunDoesNotBlockAnotherTenant(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	// Tenant A: a due report whose handoff is dead, seeded via the normal path.
	schedule, occurrence, strandedRunID := dueScheduledReport(t, pool)
	seedHandoff(t, pool, strandedRunID, "dead")

	// Tenant B: an independent organization, schedule and report, also due.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const (
		otherOrganizationID = "5e3c7f11-2d4b-4a6c-9e8f-3a1b2c4d5e60"
		otherJobID          = "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5e"
		otherReportID       = "2b3c4d5e-6f70-4b8c-9d0e-1f2a3b4c5d61"
	)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	for _, seed := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.organizations (id, name, is_active) VALUES ($1::uuid, 'tenant-b', TRUE)`,
			[]any{otherOrganizationID}},
		{`INSERT INTO public.scheduled_jobs
    (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
VALUES ($1::uuid, $2, 'report:b', 'report', '0 6 * * *', 'UTC', 0, FALSE, $3, $3)`,
			[]any{otherJobID, otherOrganizationID, createdAt}},
		{`INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'report b', $3::uuid, TRUE, $4, $4)`,
			[]any{otherReportID, otherOrganizationID, otherJobID, createdAt}},
	} {
		if _, err := pool.Exec(ctx, seed.sql, seed.args...); err != nil {
			t.Fatal(err)
		}
	}

	// A later tick, so tenant A's occurrence is already durable and replayed.
	_, later := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 20, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, later)
	if err != nil {
		t.Fatalf("a stranded tenant failed the whole occurrence: %v", err)
	}
	// Tenant B's report must have been materialized and published.
	if len(outcome.Requests) != 1 {
		t.Fatalf("outcome = %+v, want tenant B's handoff despite tenant A being stranded", outcome)
	}
	if outcome.Degraded != DegradedScheduledReportsUndeliverable {
		t.Fatalf("degraded = %q, want the stranded condition still reported", outcome.Degraded)
	}
	var otherRuns int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.report_runs WHERE report_id = $1::uuid`,
		otherReportID).Scan(&otherRuns); err != nil {
		t.Fatal(err)
	}
	if otherRuns != 1 {
		t.Fatalf("tenant B has %d runs, want 1 — its work was rolled back", otherRuns)
	}
	_ = occurrence
}

// The per-occurrence work bound DEFERS its remainder instead of failing, and the
// deferred reports are picked up by the next tick.
//
// The bound previously counted every ACTIVE schedule and aborted, so one tenant
// with 501 dormant schedules stopped every tenant's dispatch on every tick.
func TestWorkBoundDefersRemainderInsteadOfFailing(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// One more due report than a single occurrence will materialize.
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_jobs
    (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
SELECT gen_random_uuid(), $1, 'report:bulk', 'report', '0 6 * * *', 'UTC', 0, FALSE, $2, $2
FROM generate_series(1, $3)`,
		testOrganizationID, createdAt, maximumScheduledReportsPerOccurrence); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.saved_reports (id, org_id, name, schedule_id, is_active, created_at, updated_at)
SELECT gen_random_uuid(), $1, 'bulk', job.id, TRUE, $2, $2
FROM public.scheduled_jobs AS job
WHERE job.id <> $3::uuid`, testOrganizationID, createdAt, testJobID); err != nil {
		t.Fatal(err)
	}

	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("exceeding the work bound failed the occurrence: %v", err)
	}
	if len(outcome.Requests) != maximumScheduledReportsPerOccurrence {
		t.Fatalf("materialized %d, want exactly the bound %d",
			len(outcome.Requests), maximumScheduledReportsPerOccurrence)
	}
	if outcome.Degraded != DegradedScheduledReportsDeferred {
		t.Fatalf("degraded = %q, want %q", outcome.Degraded, DegradedScheduledReportsDeferred)
	}

	// The deferred remainder stays due, so the next tick makes progress on it.
	_, next := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 10, 0, 0, time.UTC))
	followUp, err := produceInTransaction(t, pool, schedule, next)
	if err != nil {
		t.Fatalf("the follow-up tick failed: %v", err)
	}
	if len(followUp.Requests) == 0 {
		t.Fatal("the deferred remainder was never picked up — it was lost, not deferred")
	}
	var runs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.report_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != maximumScheduledReportsPerOccurrence+1 {
		t.Fatalf("persisted %d runs across two ticks, want all %d due reports",
			runs, maximumScheduledReportsPerOccurrence+1)
	}
}

// A run that has moved past pending is the report handler's business. Re-arming
// any of these would duplicate execution or resurrect cancelled work.
func TestReplayIgnoresARunThatIsNoLongerPending(t *testing.T) {
	for _, status := range []string{"running", "success", "failed", "canceled"} {
		t.Run(status, func(t *testing.T) {
			pool := startScheduledReportPostgres(t)
			schedule, occurrence, runID := dueScheduledReport(t, pool)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := pool.Exec(ctx,
				`UPDATE public.report_runs SET status = $2 WHERE id = $1::uuid`,
				runID, status); err != nil {
				t.Fatal(err)
			}
			outcome, err := produceInTransaction(t, pool, schedule, occurrence)
			if err != nil {
				t.Fatalf("replay Produce(): %v", err)
			}
			if len(outcome.Requests) != 0 {
				t.Fatalf("a %s run was re-armed: %+v", status, outcome)
			}
			if outcome.SkipReason != SkipNoDueScheduledReportsClaimed {
				t.Fatalf("skip reason = %q", outcome.SkipReason)
			}
		})
	}
}

// Two active reports on one schedule must be refused EVEN WHEN one of them is
// locked by a concurrent transaction.
//
// This is the SKIP LOCKED concealment Codex found. The sweep's lock removes the
// locked sibling from the result set, so a duplicate check over that result would
// see one row, pass, and materialize an ARBITRARY sibling while advancing the
// shared job's next_run_at. The ambiguity check therefore runs unlocked, and
// before the sweep.
func TestAmbiguousScheduleIsRefusedEvenWhenOneReportIsLocked(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const siblingID = "7c3d1e2f-4a5b-4c6d-8e9f-0a1b2c3d4e5f"
	if _, err := pool.Exec(ctx, `
INSERT INTO public.saved_reports (id, org_id, name, schedule_id, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'sibling', $3::uuid, TRUE, $4, $4)`,
		siblingID, testOrganizationID, testJobID, createdAt); err != nil {
		t.Fatal(err)
	}
	// Lock the sibling exactly as an on-demand trigger would, so the sweep's SKIP
	// LOCKED drops it and leaves a single, apparently unambiguous row.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx,
		`SELECT id FROM public.saved_reports WHERE id = $1::uuid FOR UPDATE`, siblingID); err != nil {
		t.Fatal(err)
	}

	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	_, produceErr := produceInTransaction(t, pool, schedule, occurrence)
	if produceErr == nil {
		t.Fatal("an ambiguous schedule was accepted because one report was locked")
	}
	if !errors.Is(produceErr, ErrScheduledReportConflict) {
		t.Fatalf("error = %v, want a scheduled report conflict", produceErr)
	}
	if state := readReportState(t, pool); state.Occurrences != 0 || state.Runs != 0 {
		t.Fatalf("an ambiguous schedule materialized work: %+v", state)
	}
}

// A job and a report belonging to DIFFERENT organizations must never be paired.
// The schedule foreign key and unique constraint do not enforce organization
// agreement, so this is reachable data, and Python's _require_schedule rejects
// it explicitly. Pairing them would file
// the occurrence under the job's tenant while the global report worker executed
// the other tenant's data.
func TestCrossTenantJobAndReportAreNeverPaired(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const otherOrganizationID = "4d2b6e99-1c3a-4f5b-8a7c-6d5e4f3a2b10"
	if _, err := pool.Exec(ctx, `
INSERT INTO public.organizations (id, name, is_active) VALUES ($1::uuid, 'other', TRUE)`,
		otherOrganizationID); err != nil {
		t.Fatal(err)
	}
	// The report now belongs to a different tenant than its own schedule.
	if _, err := pool.Exec(ctx,
		`UPDATE public.saved_reports SET org_id = $2 WHERE id = $1::uuid`,
		testReportID, otherOrganizationID); err != nil {
		t.Fatal(err)
	}
	schedule, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce(): %v", err)
	}
	if len(outcome.Requests) != 0 {
		t.Fatalf("a cross-tenant pairing produced %d handoffs: %+v", len(outcome.Requests), outcome)
	}
	if state := readReportState(t, pool); state.Occurrences != 0 || state.Runs != 0 {
		t.Fatalf("a cross-tenant pairing materialized work: %+v", state)
	}
}

// An occurrence whose run link was severed must fail rather than re-arm.
//
// This is reachable, not hypothetical: scheduled_report_occurrences.report_run_id
// is declared ON DELETE SET NULL, so deleting a report run nulls the link and
// leaves the occurrence pointing at nothing. Without the guard the replay path
// would read a NULL run status and dereference it, and a re-armed handoff would
// name a run id that no longer exists — a permanent worker failure instead of a
// visible scheduler one.
//
// Added because mutation M22 (disabling the guard) survived the first pass: the
// guard was correct and simply had no assertion behind it.
func TestReplayFailsWhenTheOccurrenceNoLongerLinksItsRun(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate string
	}{
		{"link severed", `UPDATE public.scheduled_report_occurrences SET report_run_id = NULL`},
		{"run deleted", `DELETE FROM public.report_runs`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pool := startScheduledReportPostgres(t)
			schedule, occurrence, _ := dueScheduledReport(t, pool)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := pool.Exec(ctx, testCase.mutate); err != nil {
				t.Fatal(err)
			}
			_, err := produceInTransaction(t, pool, schedule, occurrence)
			if err == nil {
				t.Fatal("an occurrence with no linked run was accepted")
			}
			if !errors.Is(err, ErrScheduledReportConflict) {
				t.Fatalf("error = %v, want a scheduled report conflict", err)
			}
		})
	}
}

// A PYTHON-authored occurrence must be recognised, not rejected.
//
// This is the coexistence case the byte-exact occurrence identity exists to
// enable, and an earlier version of the replay path broke it. Python's
// reports.execution_trigger allocates ReportRun.id with uuid4, so a
// Python-authored occurrence links a run id the Go derivation can never produce.
// Requiring the link to equal the derived id therefore failed EVERY subsequent Go
// sweep for that report with a conflict, closing the schedule's readiness for as
// long as a Python-authored occurrence existed for a still-due report.
//
// Found by mutation M22a SURVIVING: removing the equality check broke no test,
// which is how a wrong check with no coverage looks. The authoritative run is
// whatever the occurrence links; the derived identity is only ever used to create.
func TestPythonAuthoredOccurrenceIsRecognisedNotRejected(t *testing.T) {
	// A uuid4-shaped id deliberately NOT equal to the Go derivation.
	const pythonRunID = "c9a3f5d1-8b2e-4c7a-9d6f-1e2b3c4d5e6f"
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	scheduledFor := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name         string
		handoff      string
		wantRequest  bool
		wantDegraded string
	}{
		// No durable handoff evidence: re-arm, and the re-armed request must carry
		// PYTHON's run id so the outbox dedupe key matches its run identity.
		{"handoff absent", "", true, ""},
		{"handoff live", "pending", false, ""},
		// A spent handoff is a per-report degraded condition, NOT a failure: it must
		// not roll back other tenants' work. It also must not be re-armed, since
		// publishing against a spent budget collapses on the dedupe key.
		{"handoff dead", "dead", false, DegradedScheduledReportsUndeliverable},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pool := startScheduledReportPostgres(t)
			seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			// Exactly what the Python dispatcher leaves behind: the shared
			// occurrence identity, and a run whose id it chose at random.
			//
			// The three statements are in Python's order, which the circular
			// foreign keys force: the occurrence first with a NULL run link, then
			// the run referencing it, then the link back. Seeding run-first fails
			// fk_report_runs_scheduled_occurrence — the same ordering constraint
			// the production producer obeys.
			// The identity comes from the Python oracle, not from the Go function.
			// Seeding a "Python-authored" row with the Go derivation would make this
			// test pass even if the two runtimes disagreed — which is the single
			// thing it exists to rule out.
			occurrenceID := pythonOccurrenceID(t, testReportID, scheduledFor)
			if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_report_occurrences
    (occurrence_id, identity_version, org_id, report_id, scheduled_job_id, scheduled_for)
VALUES ($1, $2, $3, $4::uuid, $5::uuid, $6)`,
				occurrenceID, scheduledReportOccurrenceIdentityVersion, testOrganizationID,
				testReportID, testJobID, scheduledFor); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `
INSERT INTO public.report_runs (id, report_id, scheduled_occurrence_id, status, triggered_by, created_at)
VALUES ($1::uuid, $2::uuid, $3, 'pending', 'scheduler', $4)`,
				pythonRunID, testReportID, occurrenceID, createdAt); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `
UPDATE public.scheduled_report_occurrences SET report_run_id = $2::uuid
WHERE occurrence_id = $1`, occurrenceID, pythonRunID); err != nil {
				t.Fatal(err)
			}
			if testCase.handoff != "" {
				seedHandoff(t, pool, pythonRunID, testCase.handoff)
			}

			schedule, occurrence := reportOccurrence(
				t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
			outcome, err := produceInTransaction(t, pool, schedule, occurrence)
			if err != nil {
				t.Fatalf("a Python-authored occurrence was rejected: %v", err)
			}
			if outcome.Degraded != testCase.wantDegraded {
				t.Fatalf("degraded = %q, want %q", outcome.Degraded, testCase.wantDegraded)
			}
			if testCase.wantRequest {
				if len(outcome.Requests) != 1 {
					t.Fatalf("outcome = %+v, want the Python run re-armed", outcome)
				}
				// The dedupe key must be Python's run, not the Go derivation, or the
				// handoff would name a run that does not exist.
				if outcome.Requests[0].Envelope.IdempotencyKey != "report.run:"+pythonRunID {
					t.Fatalf("re-armed handoff key = %q, want Python's run %s",
						outcome.Requests[0].Envelope.IdempotencyKey, pythonRunID)
				}
				if outcome.Requests[0].Envelope.Domain.ID != pythonRunID {
					t.Fatalf("domain link = %q, want Python's run",
						outcome.Requests[0].Envelope.Domain.ID)
				}
			} else if len(outcome.Requests) != 0 {
				t.Fatalf("outcome = %+v, want no handoff", outcome)
			}
			// Go must never allocate a second run for an occurrence Python owns.
			var runs int
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM public.report_runs`).Scan(&runs); err != nil {
				t.Fatal(err)
			}
			if runs != 1 {
				t.Fatalf("persisted %d runs, want only Python's", runs)
			}
		})
	}
}

// The occurrence claim is atomic under true contention, tested WITHOUT the report
// lock in the way.
//
// This exists because the full-sweep concurrency test cannot prove it. That test
// takes `FOR UPDATE OF job, report SKIP LOCKED`, so the second sweep finds no
// candidates and never reaches the occurrence INSERT at all — the row lock
// serializes the two sweeps well before the claim is exercised. It therefore
// demonstrates "one durable graph under concurrent sweeps" via the LOCK, which is
// worth asserting but is a different property.
//
// The claim is the defence behind that lock: it protects against a writer that does
// not take the Go lock ordering, and against the lock being changed later.
//
// The interleaving is explicit and is the whole test. The loser must issue its
// INSERT while the winner's row is still uncommitted, because that is the only
// ordering a check-then-insert implementation gets wrong: its check would see no
// row, it would insert anyway, and on release it would raise a unique violation
// instead of reporting a clean duplicate. A first attempt at this test used a
// two-way barrier and deadlocked, which is itself the proof that the second INSERT
// genuinely blocks inside the call — a blocked claim cannot signal that it has
// started, so the ordering has to be driven from outside.
func TestOccurrenceClaimIsAtomicUnderTrueContention(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	scheduledFor := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	occurrenceID := pythonOccurrenceID(t, testReportID, scheduledFor)
	_, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC))
	candidate := dueReportCandidate{
		JobID:          testJobID,
		OrganizationID: testOrganizationID,
		ReportID:       testReportID,
	}
	producer := reportsProducer(t)

	claim := func(tx pgx.Tx) (bool, error) {
		return producer.claimOccurrence(ctx, tx, occurrence, candidate, scheduledFor, occurrenceID)
	}

	// The winner inserts and HOLDS, so its row is uncommitted.
	winnerTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = winnerTx.Rollback(ctx) }()
	winnerClaimed, err := claim(winnerTx)
	if err != nil {
		t.Fatalf("the winning claim failed: %v", err)
	}
	if !winnerClaimed {
		t.Fatal("the first claim on an empty table did not claim")
	}

	// The loser now contends against that uncommitted row and blocks inside its
	// INSERT until the winner commits.
	type claimResult struct {
		claimed bool
		err     error
	}
	loser := make(chan claimResult, 1)
	go func() {
		loserTx, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			loser <- claimResult{err: beginErr}
			return
		}
		defer func() { _ = loserTx.Rollback(ctx) }()
		claimed, claimErr := claim(loserTx)
		if claimErr != nil {
			loser <- claimResult{err: claimErr}
			return
		}
		if commitErr := loserTx.Commit(ctx); commitErr != nil {
			loser <- claimResult{err: commitErr}
			return
		}
		loser <- claimResult{claimed: claimed}
	}()

	// Give the loser time to reach and block on its INSERT. If it has not blocked
	// by now it will simply observe the committed row instead, and the assertions
	// below still hold — so this bounds the test rather than deciding it.
	select {
	case early := <-loser:
		t.Fatalf("the contending claim returned before the winner committed: %+v", early)
	case <-time.After(750 * time.Millisecond):
	}

	if err := winnerTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	result := <-loser
	if result.err != nil {
		t.Fatalf("the contending claim errored instead of reporting a duplicate: %v", result.err)
	}
	if result.claimed {
		t.Fatal("both transactions claimed the same occurrence")
	}

	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.scheduled_report_occurrences`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("persisted %d occurrence rows for one due time, want 1", rows)
	}
}

// assertNoDuplicateHandoffs pins an invariant of one occurrence: it must never
// publish the same run twice.
//
// This is what pins the two-pass ordering. Making pass two iterate every due entry
// rather than only the unclaimed ones re-arms occurrences pass one just claimed, so
// the same run id is published from both passes — and no count or total-runs
// assertion notices, because the durable graph is unchanged. Only the key collision
// shows it.
func assertNoDuplicateHandoffs(t *testing.T, outcome Outcome) {
	t.Helper()
	seen := make(map[string]int, len(outcome.Requests))
	for _, request := range outcome.Requests {
		seen[request.Envelope.IdempotencyKey]++
	}
	for key, count := range seen {
		if count > 1 {
			t.Fatalf("one occurrence published %s %d times; a run must be handed off once", key, count)
		}
	}
}

// A carried-over report must land within a bounded number of ticks EVEN WHILE new
// reports keep coming due — the per-report property, not the aggregate one.
//
// The aggregate property ("all N eventually land" for a fixed due set) is satisfied
// by an unfair sweep: with the storage order (job id, report id) a deferred entry
// that sorts after a budget's worth of newly-due entries is passed over every tick,
// forever, if arrivals keep out-sorting it. This test gives the victim the HIGHEST
// report id, so storage order puts it last and only most-overdue-first ordering
// reaches it.
//
// Shaped after the starvation bug this producer already had once: the assertion is
// the end state (this specific report is materialized) under continuous arrival, not
// that a tick behaved.
func TestCarriedOverReportLandsUnderContinuousArrival(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// The seeded report from the helper is unused here; drop it so the population is
	// exactly what this test builds.
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", time.Date(2026, 7, 24, 6, 0, 0, 0, time.UTC))
	if _, err := pool.Exec(ctx, `DELETE FROM public.saved_reports WHERE id = $1::uuid`, testReportID); err != nil {
		t.Fatal(err)
	}

	// Every report uses the same cron. Due order is therefore driven by last_run_at:
	// an older last run means an older cron instant, i.e. more overdue.
	const cron = "0 6 * * *"
	// The victim's id sorts LAST of all, so any id-ordered sweep reaches it last.
	const victimReportID = "ffffffff-ffff-4fff-bfff-ffffffffffff"
	victimLastRun := time.Date(2026, 7, 20, 6, 0, 0, 0, time.UTC)

	seedBatch := func(count int, lastRun time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
WITH new_jobs AS (
    INSERT INTO public.scheduled_jobs
        (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, next_run_at, created_at, updated_at)
    SELECT gen_random_uuid(), $1, 'report:fill', 'report', $2, 'UTC', 0, FALSE, $5, $3, $3
    FROM generate_series(1, $4)
    RETURNING id
)
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, last_run_at, created_at, updated_at)
SELECT gen_random_uuid(), $1, 'fill', new_jobs.id, TRUE, $3, $3, $3
FROM new_jobs`, testOrganizationID, cron, lastRun, count, lastRun.Add(24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	// A budget's worth plus a margin of reports MORE overdue than the victim, so the
	// victim cannot be reached on the first tick.
	seedBatch(maximumScheduledReportsPerOccurrence+100, time.Date(2026, 7, 19, 6, 0, 0, 0, time.UTC))

	// The victim: less overdue than the first batch, more overdue than anything that
	// arrives later, and the highest report id in the population.
	const victimJobID = "3c4d5e6f-7081-4c9d-8e0f-2a3b4c5d6e72"
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_jobs
    (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, next_run_at, created_at, updated_at)
VALUES ($1::uuid, $2, 'report:victim', 'report', $3, 'UTC', 0, FALSE, $5, $4, $4)`,
		victimJobID, testOrganizationID, cron, victimLastRun, victimLastRun.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, last_run_at, created_at, updated_at)
VALUES ($1::uuid, $2, 'victim', $3::uuid, TRUE, $4, $4, $4)`,
		victimReportID, testOrganizationID, victimJobID, victimLastRun); err != nil {
		t.Fatal(err)
	}

	victimRuns := func() int {
		t.Helper()
		var runs int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM public.report_runs WHERE report_id = $1::uuid`,
			victimReportID).Scan(&runs); err != nil {
			t.Fatal(err)
		}
		return runs
	}

	schedule := scheduleByID(t, scheduledReportsScheduleID)
	base := time.Date(2026, 7, 25, 6, 5, 0, 0, time.UTC)

	// Tick one cannot reach the victim: the more-overdue batch fills the budget.
	_, first := reportOccurrence(t, base)
	firstOutcome, err := produceInTransaction(t, pool, schedule, first)
	if err != nil {
		t.Fatalf("tick one: %v", err)
	}
	assertNoDuplicateHandoffs(t, firstOutcome)
	if victimRuns() != 0 {
		t.Fatal("test is not exercising deferral: the victim landed on tick one")
	}
	if firstOutcome.Degraded != DegradedScheduledReportsDeferred {
		t.Fatalf("tick one degraded = %q, want the deferred signal", firstOutcome.Degraded)
	}

	// CONTINUOUS ARRIVAL: a fresh budget's worth of reports comes due every tick,
	// all LESS overdue than the victim. Under an unfair sweep these keep displacing
	// it; under most-overdue-first they sort behind it.
	const ticks = 4
	for tick := 1; tick <= ticks; tick++ {
		seedBatch(maximumScheduledReportsPerOccurrence, time.Date(2026, 7, 24, 6, 0, 0, 0, time.UTC))
		_, occurrence := reportOccurrence(t, base.Add(time.Duration(tick)*5*time.Minute))
		outcome, err := produceInTransaction(t, pool, schedule, occurrence)
		if err != nil {
			t.Fatalf("tick %d: %v", tick+1, err)
		}
		assertNoDuplicateHandoffs(t, outcome)
		if victimRuns() == 1 {
			return
		}
	}
	t.Fatalf(
		"the carried-over report was still not materialized after %d ticks of "+
			"continuous arrival; deferral is bounded in aggregate but not per report",
		ticks+1,
	)
}

// A durable replay must not disappear behind a continuously full page of new
// reports. The selector and the in-memory work budget are separate fairness
// boundaries: selecting the replay is insufficient if pass one still spends all
// 500 request slots on new claims before replayExisting runs.
func TestDurableReplayProgressesWhenTheNewReportPageIsFull(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	schedule, _, replayedRunID := dueScheduledReport(t, pool)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	nextRunAt := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
WITH new_jobs AS (
    INSERT INTO public.scheduled_jobs
        (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, next_run_at, created_at, updated_at)
    SELECT gen_random_uuid(), $1, 'report:new-fill', 'report', '0 6 * * *', 'UTC', 0, FALSE, $2, $3, $3
    FROM generate_series(1, $4)
    RETURNING id
)
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
SELECT gen_random_uuid(), $1, 'new-fill', new_jobs.id, TRUE, $3, $3
FROM new_jobs`, testOrganizationID, nextRunAt, createdAt,
		maximumScheduledReportCandidatesPerOccurrence); err != nil {
		t.Fatal(err)
	}

	// This canonical tick is the replay-priority half of the alternating policy.
	// The test fixes the timestamp independently so changing or removing that
	// production policy cannot make the fixture silently follow the defect.
	_, occurrence := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 15, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("saturated replay sweep: %v", err)
	}
	assertNoDuplicateHandoffs(t, outcome)

	replayedKey := "report.run:" + replayedRunID
	foundReplay := false
	for _, request := range outcome.Requests {
		if request.Envelope.IdempotencyKey == replayedKey {
			foundReplay = true
			break
		}
	}
	if !foundReplay {
		t.Fatalf("durable replay %s was starved behind %d new reports",
			replayedKey, maximumScheduledReportCandidatesPerOccurrence)
	}
	if len(outcome.Requests) != maximumScheduledReportsPerOccurrence {
		t.Fatalf("published %d requests, want the bounded work budget %d",
			len(outcome.Requests), maximumScheduledReportsPerOccurrence)
	}
	if outcome.Degraded != DegradedScheduledReportsDeferred {
		t.Fatalf("degraded = %q, want %q", outcome.Degraded, DegradedScheduledReportsDeferred)
	}
}

// The sweep must lock exactly one bounded page plus one row that proves a due
// remainder exists. The extra row is what raises the deferred signal; reading all
// 621 rows would restore the transaction-footprint bug this page replaced.
func TestSweepReadsOneBoundedPageAndObservesTheRemainder(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	// Comfortably more active report schedules than one occurrence will materialize.
	const excess = 120
	if _, err := pool.Exec(ctx, `
WITH new_jobs AS (
    INSERT INTO public.scheduled_jobs
        (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
    SELECT gen_random_uuid(), $1, 'report:read', 'report', '0 6 * * *', 'UTC', 0, FALSE, $2, $2
    FROM generate_series(1, $3)
    RETURNING id
)
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
SELECT gen_random_uuid(), $1, 'read', new_jobs.id, TRUE, $2, $2
FROM new_jobs`,
		testOrganizationID, createdAt, maximumScheduledReportsPerOccurrence+excess); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	candidates, err := reportsProducer(t).lockDueCandidates(
		ctx, tx, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC), false,
	)
	if err != nil {
		t.Fatalf("lockDueCandidates(): %v", err)
	}
	// This is the external transaction-footprint contract, not an oracle copied
	// from the production constant. If the lock page grows, this assertion must
	// fail and force an explicit operational decision.
	const want = 501
	if len(candidates) != want {
		t.Fatalf(
			"the sweep read %d candidates, want the bounded %d-row page for a %d-row population",
			len(candidates), want, maximumScheduledReportsPerOccurrence+excess+1,
		)
	}
}

// The replay selector uses a strict comparison against the report's terminal
// base. A cancellation records last_run_at as the exact scheduled occurrence,
// so equality means the OLD occurrence is complete and must not occupy the
// bounded replay page while the paging marker is invalidated for recomputation.
func TestTerminalBaseEqualityIsNotSelectedAsReplay(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	schedule, _, _ := dueScheduledReport(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	scheduledFor := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
UPDATE public.saved_reports
SET last_run_at = $1, last_run_status = 'canceled'
	WHERE id = $2::uuid`, scheduledFor, testReportID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE public.scheduled_jobs SET next_run_at = NULL WHERE id = $1::uuid`, testJobID); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	candidates, err := reportsProducer(t).lockDueCandidates(
		ctx, tx, time.Date(2026, time.July, 26, 6, 5, 0, 0, time.UTC), false,
	)
	if err != nil {
		t.Fatalf("lockDueCandidates(): %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d, want one", len(candidates))
	}
	if candidates[0].AlreadyMaterialized {
		t.Fatal("terminal occurrence equal to last_run_at was selected as replay")
	}
	if candidates[0].NextDueAt != nil {
		t.Fatalf("invalidated next_run_at = %v, want NULL for recomputation", candidates[0].NextDueAt)
	}
	if schedule.ID != scheduledReportsScheduleID {
		t.Fatalf("test schedule = %q", schedule.ID)
	}
}

// One tenant's active schedule population must not abort the global sweep.
//
// The old allocation guard read every active report schedule in job-id order and
// failed after 20,000 rows. That made volume itself a cross-tenant kill switch:
// these attacker schedules are not due, but they still filled the read and kept
// the due victim out of the transaction on every retry. The product property is
// the victim's durable run, not whether an internal limit happened to trigger.
func TestOneTenantsRowsBeyondTheFormerReadGuardDoNotBlockAnotherTenant(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const (
		formerMaximumScheduledReportRowsRead = 20000
		attackerOrganizationID               = "11111111-1111-4111-8111-111111111111"
		victimOrganizationID                 = "22222222-2222-4222-8222-222222222222"
		victimJobID                          = "ffffffff-ffff-4fff-bfff-fffffffffff0"
		victimReportID                       = "ffffffff-ffff-4fff-bfff-fffffffffff1"
	)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	attackerBase := time.Date(2026, time.July, 26, 6, 0, 0, 0, time.UTC)
	attackerNextDue := time.Date(2026, time.July, 27, 6, 0, 0, 0, time.UTC)
	victimNextDue := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.organizations (id, name, is_active)
VALUES ($1::uuid, 'attacker', TRUE), ($2::uuid, 'victim', TRUE)`,
		attackerOrganizationID, victimOrganizationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
WITH attacker_jobs AS (
    INSERT INTO public.scheduled_jobs
        (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, next_run_at, created_at, updated_at)
    SELECT gen_random_uuid(), $1, 'report:attacker', 'report', '0 6 * * *', 'UTC', 0, FALSE, $4, $2, $2
    FROM generate_series(1, $3)
    RETURNING id
)
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
SELECT gen_random_uuid(), $1, 'attacker', attacker_jobs.id, TRUE, $2, $2
FROM attacker_jobs`, attackerOrganizationID, attackerBase, formerMaximumScheduledReportRowsRead+1, attackerNextDue); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_jobs
    (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, next_run_at, created_at, updated_at)
VALUES ($1::uuid, $2, 'report:victim', 'report', '0 6 * * *', 'UTC', 0, FALSE, $4, $3, $3)`,
		victimJobID, victimOrganizationID, createdAt, victimNextDue); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
VALUES ($4::uuid, $2, 'victim', $1::uuid, TRUE, $3, $3)`,
		victimJobID, victimOrganizationID, createdAt, victimReportID); err != nil {
		t.Fatal(err)
	}

	schedule, occurrence := reportOccurrence(
		t, time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC),
	)
	outcome, err := produceInTransaction(t, pool, schedule, occurrence)
	if err != nil {
		t.Fatalf("Produce(): %v", err)
	}
	assertNoDuplicateHandoffs(t, outcome)

	var victimRuns int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM public.report_runs WHERE report_id = $1::uuid`,
		victimReportID).Scan(&victimRuns); err != nil {
		t.Fatal(err)
	}
	if victimRuns != 1 {
		t.Fatalf("victim report runs = %d, want 1", victimRuns)
	}
}

// A degraded reason must SURVIVE the many polls that do not evaluate the schedule.
//
// This is the observability half of the per-report degraded fix, and without it that
// fix is useless in practice. The loop polls every few seconds while this schedule is
// due once per 300 seconds, so roughly twenty consecutive windows produce a result
// for it with no occurrence due and therefore an empty Degraded. Overwriting the
// stored reason on those windows reduced a permanent fault to a single-poll blip that
// any realistic scrape interval misses entirely.
//
// The assertion is the end state an operator depends on — the reason is still
// readable after non-evaluating windows — not that one call returned the right value.
// It drives the real Engine and the real ledger so the Evaluated plumbing is
// exercised end to end rather than mocked.
func TestDegradedReasonSurvivesNonEvaluatingWindows(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// A due report whose handoff is spent: a permanent, operator-repairable fault.
	schedule, _, strandedRunID := dueScheduledReport(t, pool)
	seedHandoff(t, pool, strandedRunID, "dead")

	location, err := schedule.Location()
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.July, 25, 6, 20, 30, 0, time.UTC)
	dueAt, ok := schedule.Cadence.Previous(observedAt, location)
	if !ok {
		t.Fatal("the report schedule cadence resolved no due time")
	}
	previous := NewOccurrence(
		schedule,
		dueAt.Add(-schedule.Cadence.Period()),
		dueAt.Add(-schedule.Cadence.Period()),
	)
	seedRecordedOccurrence(t, ctx, pool, previous)

	engine, _ := newReportEngine(t, pool, schedule)

	// The evaluating window: an occurrence is due, the producer runs, and it reports
	// the stranded run.
	evaluating, err := engine.Step(ctx, observedAt)
	if err != nil {
		t.Fatalf("evaluating Step(): %v", err)
	}
	result := resultFor(t, evaluating, schedule.ID)
	if result.Err != nil {
		t.Fatalf("evaluating window failed: %v", result.Err)
	}
	if !result.Evaluated {
		t.Fatal("the window that ran the producer did not report Evaluated")
	}
	if result.Degraded != DegradedScheduledReportsUndeliverable {
		t.Fatalf("evaluating window degraded = %q, want the stranded reason", result.Degraded)
	}

	// The polls that follow: no occurrence is due, so the producer never runs. Each
	// must report Evaluated false and must not assert a verdict of its own.
	for poll := 1; poll <= 3; poll++ {
		nonEvaluating, err := engine.Step(ctx, observedAt.Add(time.Duration(poll)*15*time.Second))
		if err != nil {
			t.Fatalf("poll %d Step(): %v", poll, err)
		}
		quiet := resultFor(t, nonEvaluating, schedule.ID)
		if quiet.Err != nil {
			t.Fatalf("poll %d failed: %v", poll, quiet.Err)
		}
		if quiet.Due != 0 {
			t.Fatalf("poll %d unexpectedly had work due; the test is not exercising a quiet poll", poll)
		}
		if quiet.Evaluated {
			t.Fatalf("poll %d claimed to have evaluated the schedule without an occurrence", poll)
		}
		if quiet.Degraded != "" {
			t.Fatalf("poll %d asserted degraded %q without running the producer", poll, quiet.Degraded)
		}
	}
}

// A MIXED sweep — some occurrences new, some already durable, budget NOT exhausted —
// must publish each run exactly once.
//
// This is the scenario that actually distinguishes the two-pass split, and both
// earlier tests missed it for the same reason: each seeded more due reports than the
// work bound, so pass one filled the budget and every pass-two iteration short-
// circuited on the budget check. The mutation making pass two iterate all due entries
// therefore changed nothing observable, and survived.
//
// With budget left over, pass two actually runs. Iterating all due entries then
// re-arms the occurrences pass one just claimed, publishing those run ids a second
// time from the same occurrence — while leaving the durable graph identical, which is
// why only the duplicate-handoff invariant can see it.
func TestMixedNewAndReplayedSweepPublishesEachRunOnce(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Report A: materialized on an earlier tick, and its handoff was pruned, so replay
	// legitimately owes it a re-arm on this tick.
	schedule, _, replayedRunID := dueScheduledReport(t, pool)

	// Report B: brand new, never materialized, and due at the same instant.
	const (
		newJobID    = "4d5e6f70-8192-4d0e-9f10-3b4c5d6e7f83"
		newReportID = "5e6f7081-92a3-4e1f-8021-4c5d6e7f8094"
	)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_jobs
    (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
VALUES ($1::uuid, $2, 'report:new', 'report', '0 6 * * *', 'UTC', 0, FALSE, $3, $3)`,
		newJobID, testOrganizationID, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'new', $3::uuid, TRUE, $4, $4)`,
		newReportID, testOrganizationID, newJobID, createdAt); err != nil {
		t.Fatal(err)
	}

	// A later tick: report A replays, report B is claimed, and only two of the 500
	// budget slots are used — so pass two genuinely executes.
	_, later := reportOccurrence(t, time.Date(2026, time.July, 25, 6, 20, 0, 0, time.UTC))
	outcome, err := produceInTransaction(t, pool, schedule, later)
	if err != nil {
		t.Fatalf("mixed sweep: %v", err)
	}
	assertNoDuplicateHandoffs(t, outcome)

	if len(outcome.Requests) != 2 {
		t.Fatalf("mixed sweep published %d handoffs, want one new claim and one re-arm", len(outcome.Requests))
	}
	keys := map[string]bool{}
	for _, request := range outcome.Requests {
		keys[request.Envelope.IdempotencyKey] = true
	}
	if !keys["report.run:"+replayedRunID] {
		t.Fatalf("the replayed report was not re-armed; keys = %v", keys)
	}
	var newRunID string
	if err := pool.QueryRow(ctx,
		`SELECT id::text FROM public.report_runs WHERE report_id = $1::uuid`,
		newReportID).Scan(&newRunID); err != nil {
		t.Fatal(err)
	}
	if !keys["report.run:"+newRunID] {
		t.Fatalf("the newly claimed report was not published; keys = %v", keys)
	}

	// Exactly two runs exist: the re-arm must not have created a second one.
	var runs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.report_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 2 {
		t.Fatalf("persisted %d runs, want exactly one per report", runs)
	}
}

// A window where an occurrence IS due but the producer never runs must report
// Evaluated false.
//
// This is the engine-side half of the degraded-persistence guard, and it had NO test
// until a mutation exposed that. The gap was hidden by the mutation failing to compile
// — `if evaluated` -> `if true` orphaned the variable, so the package did not build and
// the harness recorded a kill that proved nothing. Made compilable, the mutation
// survived, which is the honest signal.
//
// A cold start is the reachable case: the ledger has no history, so the window records
// a baseline and deliberately produces nothing. `Due` is 1 and the producer is never
// invoked, so there is no verdict — and claiming one would let a baseline window clear
// a degraded reason a real evaluation had raised.
func TestColdStartWindowReportsNoEvaluation(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	// No seeded anchor: the ledger is empty, so this window is a cold-start baseline.
	schedule := scheduleByID(t, scheduledReportsScheduleID)
	engine, publisher := newReportEngine(t, pool, schedule)

	result := resultFor(t, mustStep(t, ctx, engine, time.Date(2026, time.July, 25, 6, 5, 30, 0, time.UTC)), schedule.ID)
	if result.Err != nil {
		t.Fatalf("cold-start window failed: %v", result.Err)
	}
	if !result.ColdStart || result.Due != 1 {
		t.Fatalf("result = %+v, want a cold-start window with one due occurrence", result)
	}
	if result.Evaluated {
		t.Fatal(
			"a cold-start baseline claimed to have evaluated the schedule; the producer " +
				"never ran, so it has no verdict and must not be allowed to clear one",
		)
	}
	if result.Degraded != "" {
		t.Fatalf("cold-start window asserted degraded %q without running the producer", result.Degraded)
	}
	if publisher.count() != 0 {
		t.Fatalf("a cold-start baseline published %d handoffs, want none", publisher.count())
	}
	// And the report must still be materialized once a real window runs, so the
	// baseline defers work rather than consuming it.
	if state := readReportState(t, pool); state.Runs != 0 {
		t.Fatalf("cold-start baseline materialized %d runs", state.Runs)
	}
}

func mustStep(t *testing.T, ctx context.Context, engine *Engine, at time.Time) WindowResult {
	t.Helper()
	result, err := engine.Step(ctx, at)
	if err != nil {
		t.Fatalf("Step(%s): %v", at.Format(time.RFC3339), err)
	}
	return result
}

// The SEAM: a real produced-work-with-degradation outcome must reach the exported
// gauge, driven Engine -> Loop -> WritePrometheus.
//
// Nothing covered this. One test injects synthetic ScheduleResults into Loop.record
// with Stepper.Step never called; another drives the real engine but asserts only the
// values Step returns. Both halves were covered and the join between them was not, so
// a mutation dropping Degraded from the materialized branch of runOccurrence could
// survive — the same shape as M29 and M31, where one half of a pair passing disguised
// the other half having no reaching test.
//
// It also pins the property the whole degraded mechanism exists for and which no other
// test asserts end to end: a degraded reason reported ALONGSIDE successful work
// survives into telemetry. A skip reason is deliberately NOT promoted, so this is the
// only route by which anything reaches that gauge.
func TestProducedWorkWithDegradationReachesTheExportedGauge(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Tenant A: stranded, so the occurrence is degraded. Tenant B: healthy and due, so
	// the SAME occurrence also produces work. Degraded must coexist with a handoff.
	schedule, _, strandedRunID := dueScheduledReport(t, pool)
	seedHandoff(t, pool, strandedRunID, "dead")

	const (
		otherOrganizationID = "7a8b9c0d-1e2f-4a3b-8c4d-5e6f7a8b9c0e"
		otherJobID          = "8b9c0d1e-2f30-4b4c-9d5e-6f7a8b9c0d1f"
		otherReportID       = "9c0d1e2f-3041-4c5d-8e6f-7a8b9c0d1e20"
	)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	for _, seed := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO public.organizations (id, name, is_active) VALUES ($1::uuid, 'seam', TRUE)`,
			[]any{otherOrganizationID}},
		{`INSERT INTO public.scheduled_jobs
    (id, org_id, name, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
VALUES ($1::uuid, $2, 'report:seam', 'report', '0 6 * * *', 'UTC', 0, FALSE, $3, $3)`,
			[]any{otherJobID, otherOrganizationID, createdAt}},
		{`INSERT INTO public.saved_reports
    (id, org_id, name, schedule_id, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'seam', $3::uuid, TRUE, $4, $4)`,
			[]any{otherReportID, otherOrganizationID, otherJobID, createdAt}},
	} {
		if _, err := pool.Exec(ctx, seed.sql, seed.args...); err != nil {
			t.Fatal(err)
		}
	}

	location, err := schedule.Location()
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.July, 25, 6, 20, 30, 0, time.UTC)
	dueAt, ok := schedule.Cadence.Previous(observedAt, location)
	if !ok {
		t.Fatal("cadence resolved no due time")
	}
	previous := NewOccurrence(
		schedule, dueAt.Add(-schedule.Cadence.Period()), dueAt.Add(-schedule.Cadence.Period()))
	seedRecordedOccurrence(t, ctx, pool, previous)

	engine, publisher := newReportEngine(t, pool, schedule)
	loop, err := NewLoop(engine, DefaultLoopConfig(health.NewRegistry(100*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}

	// Drive the REAL engine, then hand the real WindowResult to the real loop.
	window, err := engine.Step(ctx, observedAt)
	if err != nil {
		t.Fatalf("Step(): %v", err)
	}
	result := resultFor(t, window, schedule.ID)
	if result.Err != nil {
		t.Fatalf("window failed: %v", result.Err)
	}
	// The premise: work WAS produced and the occurrence is degraded at once.
	if result.Handoffs != 1 || publisher.count() != 1 {
		t.Fatalf("result = %+v (published %d), want exactly tenant B's handoff",
			result, publisher.count())
	}
	if result.Degraded != DegradedScheduledReportsUndeliverable {
		t.Fatalf("degraded = %q, want the stranded condition alongside produced work",
			result.Degraded)
	}
	if !result.Evaluated {
		t.Fatal("a committed producing window did not report Evaluated")
	}

	loop.record(window, observedAt)

	var exported strings.Builder
	if err := loop.WritePrometheus(&exported); err != nil {
		t.Fatal(err)
	}
	var gauge string
	for _, line := range strings.Split(exported.String(), "\n") {
		if strings.HasPrefix(line, `fixed_scheduler_schedule_degraded{schedule="`+schedule.ID+`"`) {
			gauge = line
		}
	}
	if gauge == "" {
		t.Fatalf("no degraded gauge for %s in:\n%s", schedule.ID, exported.String())
	}
	if !strings.Contains(gauge, DegradedScheduledReportsUndeliverable) || !strings.HasSuffix(gauge, " 1") {
		t.Fatalf(
			"gauge = %q; a degraded reason reported alongside produced work did not "+
				"survive the Engine -> Loop -> WritePrometheus seam", gauge,
		)
	}
	// And the handoff count still reflects the work, so degradation did not suppress it.
	if !strings.Contains(exported.String(),
		`fixed_scheduler_handoffs_total{schedule="`+schedule.ID+`"} 1`) {
		t.Fatalf("handoff counter did not record the produced work:\n%s", exported.String())
	}
}

// A SkipReason must not reach the degraded gauge THROUGH THE ENGINE.
//
// There is a unit test asserting Loop.record does not invent a degraded reason from a
// skipped result. It is not sufficient, and the gap is the third instance of one
// structural mistake in this change: the promotion being guarded against lives in
// runOccurrence, so a test that hands Loop.record a synthetic result never crosses the
// boundary where the defect would be. Covering each side of a boundary is not covering
// the crossing.
//
// This drives the real producer on a report that is not yet due — the ordinary case,
// which is most ticks of a 300 second sweep — through the real engine into the real
// loop, and demands the gauge stay clear. With the promotion restored, "nothing was
// due" is displayed as a fault and latches until the schedule next evaluates.
func TestSkipReasonDoesNotReachTheGaugeThroughTheEngine(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Created at 06:00 with a 06:00 daily cron: the next fire is tomorrow, so the
	// sweep finds a report and correctly reports that nothing is due.
	createdAt := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	schedule := scheduleByID(t, scheduledReportsScheduleID)
	location, err := schedule.Location()
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.July, 25, 6, 20, 30, 0, time.UTC)
	dueAt, ok := schedule.Cadence.Previous(observedAt, location)
	if !ok {
		t.Fatal("cadence resolved no due time")
	}
	previous := NewOccurrence(
		schedule, dueAt.Add(-schedule.Cadence.Period()), dueAt.Add(-schedule.Cadence.Period()))
	seedRecordedOccurrence(t, ctx, pool, previous)

	engine, publisher := newReportEngine(t, pool, schedule)
	loop, err := NewLoop(engine, DefaultLoopConfig(health.NewRegistry(100*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}

	window, err := engine.Step(ctx, observedAt)
	if err != nil {
		t.Fatalf("Step(): %v", err)
	}
	result := resultFor(t, window, schedule.ID)
	if result.Err != nil {
		t.Fatalf("window failed: %v", result.Err)
	}
	// The premise: a real, committed, zero-work occurrence.
	if result.Skipped != 1 || publisher.count() != 0 {
		t.Fatalf("result = %+v (published %d), want a skipped occurrence with no work",
			result, publisher.count())
	}
	if !result.Evaluated {
		t.Fatal("a committed skipped window did not report Evaluated")
	}
	if result.Degraded != "" {
		t.Fatalf(
			"degraded = %q; an ordinary skip reason was promoted, and it would now latch "+
				"until this schedule next evaluates", result.Degraded,
		)
	}

	loop.record(window, observedAt)
	var exported strings.Builder
	if err := loop.WritePrometheus(&exported); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(exported.String(), "\n") {
		if !strings.HasPrefix(line, `fixed_scheduler_schedule_degraded{schedule="`+schedule.ID+`"`) {
			continue
		}
		if !strings.Contains(line, `reason="none"`) || !strings.HasSuffix(line, " 0") {
			t.Fatalf("gauge = %q, want it clear after an ordinary skip", line)
		}
		// The skip is still visible where it belongs.
		if !strings.Contains(exported.String(),
			`fixed_scheduler_occurrences_total{schedule="`+schedule.ID+`",result="skipped"} 1`) {
			t.Fatalf("the skip was not counted as an occurrence outcome:\n%s", exported.String())
		}
		return
	}
	t.Fatalf("no degraded gauge for %s in:\n%s", schedule.ID, exported.String())
}
