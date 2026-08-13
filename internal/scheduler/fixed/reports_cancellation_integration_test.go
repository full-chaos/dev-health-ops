//go:build integration

package fixed

import (
	"context"
	"testing"
	"time"
)

// Cancellation owns a terminal report state just like completion and failure.
// The Python mutation path proves that it writes this exact state atomically;
// this real-PostgreSQL test proves the Go producer consumes that state and can
// materialize the next cron occurrence. Checking only the canceled row would
// permit the permanent schedule stall that CHAOS-3158 repairs.
func TestNextCronInstantAfterScheduledCancellationProducesAgain(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	createdAt := time.Date(2026, time.July, 24, 6, 0, 0, 0, time.UTC)
	seedScheduledReport(t, pool, "0 6 * * *", "UTC", createdAt)

	schedule, firstOccurrence := reportOccurrence(
		t,
		time.Date(2026, time.July, 25, 6, 5, 0, 0, time.UTC),
	)
	if _, err := produceInTransaction(t, pool, schedule, firstOccurrence); err != nil {
		t.Fatalf("first Produce(): %v", err)
	}
	first := readReportState(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.report_runs
SET status = 'canceled', completed_at = $2
WHERE id = $1::uuid`, first.RunID, first.ScheduledFor); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.saved_reports
SET last_run_at = $2, last_run_status = 'canceled', updated_at = $3
WHERE id = $1::uuid`, testReportID, first.ScheduledFor, first.ScheduledFor.Add(time.Minute)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.scheduled_jobs
	SET next_run_at = NULL, updated_at = $2
	WHERE id = $1::uuid`, testJobID, first.ScheduledFor.Add(time.Minute)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	_, nextOccurrence := reportOccurrence(
		t,
		time.Date(2026, time.July, 26, 6, 5, 0, 0, time.UTC),
	)
	outcome, err := produceInTransaction(t, pool, schedule, nextOccurrence)
	if err != nil {
		t.Fatalf("next-day Produce(): %v", err)
	}
	if len(outcome.Requests) != 1 {
		t.Fatalf("next cron occurrence produced %d handoffs, want one", len(outcome.Requests))
	}
	state := readReportState(t, pool)
	if state.Occurrences != 2 || state.Runs != 2 {
		t.Fatalf("after cancellation persisted %d occurrences and %d runs, want two each",
			state.Occurrences, state.Runs)
	}
}
