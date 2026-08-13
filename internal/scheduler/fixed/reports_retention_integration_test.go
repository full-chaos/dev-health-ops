//go:build integration

package fixed

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

func installTerminalRetentionFixture(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, `
ALTER TABLE public.worker_job_outbox
    ADD COLUMN last_error_code VARCHAR(64),
    ADD COLUMN delivered_at TIMESTAMPTZ,
    ADD COLUMN prerequisite_completion_key TEXT;
CREATE TABLE public.worker_job_completion_fences (
    completion_key TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
)`); err != nil {
		t.Fatal(err)
	}
}

// A dead handoff must stay distinguishable from one that was never published
// after terminal outbox retention removes the full row. This test uses the real
// retention repository between abandonment and replay. Omitting that step does
// not reproduce CHAOS-3160.
func TestDeadHandoffRemainsUndeliverableAfterTerminalRetention(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	installTerminalRetentionFixture(t, pool)
	schedule, occurrence, runID := dueScheduledReport(t, pool)

	terminalAt := time.Date(2026, time.July, 25, 6, 10, 0, 0, time.UTC)
	seedHandoff(t, pool, runID, "dead")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := pool.Exec(ctx, `
UPDATE public.worker_job_outbox
SET attempt_count = 4,
    last_error_code = 'contract_rejected',
    updated_at = $2
WHERE dedupe_key = $1`, "report.run:"+runID, terminalAt); err != nil {
		t.Fatal(err)
	}

	repository, err := joboutbox.NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := repository.DeleteTerminalBefore(ctx, terminalAt.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("DeleteTerminalBefore(): %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteTerminalBefore() deleted %d rows, want 1", deleted)
	}
	if got := handoffCount(t, pool); got != 0 {
		t.Fatalf("terminal handoffs after retention = %d, want 0", got)
	}

	for replay := 1; replay <= 2; replay++ {
		outcome, replayErr := produceInTransaction(t, pool, schedule, occurrence)
		if replayErr != nil {
			t.Fatalf("replay %d Produce(): %v", replay, replayErr)
		}
		if len(outcome.Requests) != 0 {
			t.Errorf("replay %d re-armed an abandoned handoff: %+v", replay, outcome.Requests)
		}
		if outcome.Degraded != DegradedScheduledReportsUndeliverable {
			t.Errorf(
				"replay %d degraded = %q, want %q",
				replay, outcome.Degraded, DegradedScheduledReportsUndeliverable,
			)
		}
	}

	var abandonedAt time.Time
	var attempts int
	var errorCode *string
	if err := pool.QueryRow(ctx, `
SELECT abandoned_at, attempt_count, last_error_code
FROM public.worker_job_delivery_abandonments
WHERE dedupe_key = $1`, "report.run:"+runID).Scan(
		&abandonedAt, &attempts, &errorCode,
	); err != nil {
		t.Errorf("read durable delivery abandonment: %v", err)
	} else {
		if !abandonedAt.Equal(terminalAt) {
			t.Errorf("abandoned_at = %s, want %s", abandonedAt, terminalAt)
		}
		if attempts != 4 {
			t.Errorf("attempt_count = %d, want 4", attempts)
		}
		if errorCode == nil || *errorCode != "contract_rejected" {
			t.Errorf("last_error_code = %v, want contract_rejected", errorCode)
		}
	}
}
