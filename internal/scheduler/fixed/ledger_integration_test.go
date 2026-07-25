//go:build integration

package fixed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The DDL mirrors alembic revision 0065. It is repeated here rather than
// executed through Alembic so the Go integration test has no Python runtime
// dependency; the terminal-state check constraint is the part that matters,
// because it is what proves the engine never records an ambiguous occurrence.
const fixedScheduleOccurrenceDDL = `
CREATE TABLE public.fixed_schedule_occurrences (
    occurrence_key TEXT NOT NULL,
    identity_version TEXT NOT NULL,
    schedule_id TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    scheduled_for TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'claimed',
    handoff_count INTEGER NOT NULL DEFAULT 0,
    skip_reason VARCHAR(64),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_fixed_schedule_occurrences PRIMARY KEY (occurrence_key),
    CONSTRAINT uq_fixed_schedule_occurrence_schedule_time UNIQUE (schedule_id, scheduled_for),
    CONSTRAINT ck_fixed_schedule_occurrence_status
        CHECK (status IN ('claimed', 'materialized', 'skipped')),
    CONSTRAINT ck_fixed_schedule_occurrence_handoff_count CHECK (handoff_count >= 0),
    CONSTRAINT ck_fixed_schedule_occurrence_terminal_state CHECK (
        (status = 'claimed' AND completed_at IS NULL AND handoff_count = 0 AND skip_reason IS NULL)
        OR (status = 'materialized' AND completed_at IS NOT NULL AND handoff_count > 0 AND skip_reason IS NULL)
        OR (status = 'skipped' AND completed_at IS NOT NULL AND handoff_count = 0 AND skip_reason IS NOT NULL)
    )
);
CREATE INDEX ix_fixed_schedule_occurrence_schedule_time
    ON public.fixed_schedule_occurrences (schedule_id, scheduled_for DESC);
`

func startLedgerPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	// pgxpool.New leaves MaxConns at its default, max(4, runtime.NumCPU()).
	// TestTwoReplicasClaimOneOccurrencePerDueTime deliberately opens
	// `replicas` (6) concurrent transactions and holds every one of them
	// open until all six have started, so the pool must be able to seat all
	// six at once. Dev workstations commonly have >6 CPUs, so the default
	// happens to be large enough there; GitHub-hosted ubuntu-latest runners
	// have 4 vCPUs, so the default pins MaxConns at 4 and replicas 5 and 6
	// deadlock forever waiting for a connection that can only free up once
	// all six have already acquired one — a real deadlock, not a flake, that
	// nothing releases until `go test`'s -timeout=20m panics the whole run.
	// Pin MaxConns explicitly so pool sizing never depends on host CPU
	// count, matching the convention already used in internal/joboutbox and
	// internal/joboperator's integration suites.
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, fixedScheduleOccurrenceDDL); err != nil {
		t.Fatal(err)
	}
	return pool
}

func integrationSchedule(t *testing.T) Schedule {
	t.Helper()
	schedules, err := Schedules()
	if err != nil {
		t.Fatal(err)
	}
	for _, schedule := range schedules {
		if schedule.ID == "phone_home_heartbeat" {
			return schedule
		}
	}
	t.Fatal("phone_home_heartbeat is not declared")
	return Schedule{}
}

func occurrenceCount(t *testing.T, pool *pgxpool.Pool, scheduleID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(
		context.Background(),
		"SELECT count(*) FROM public.fixed_schedule_occurrences WHERE schedule_id = $1",
		scheduleID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

// TestTwoReplicasClaimOneOccurrencePerDueTime is the CUT-05 acceptance case:
// two scheduler replicas race the same due time against a real PostgreSQL
// primary key and exactly one wins.
func TestTwoReplicasClaimOneOccurrencePerDueTime(t *testing.T) {
	ctx := context.Background()
	pool := startLedgerPostgres(t)
	schedule := integrationSchedule(t)
	ledger := NewPostgresLedger()
	occurrence := NewOccurrence(
		schedule,
		mustTime(t, "2026-07-24T00:00:00Z"),
		mustTime(t, "2026-07-24T00:00:07Z"),
	)

	const replicas = 6
	results := make([]ClaimResult, replicas)
	errs := make([]error, replicas)
	release := make(chan struct{})
	var ready, finished sync.WaitGroup
	ready.Add(replicas)
	finished.Add(replicas)
	for index := 0; index < replicas; index++ {
		go func(index int) {
			defer finished.Done()
			// Bound the acquire step on its own so a pool-sizing regression
			// (see startLedgerPostgres) fails in seconds with a clear
			// "context deadline exceeded" instead of hanging every replica
			// until go test's -timeout=20m panics the whole run.
			beginCtx, beginCancel := context.WithTimeout(ctx, 15*time.Second)
			tx, err := pool.Begin(beginCtx)
			beginCancel()
			if err != nil {
				errs[index] = err
				ready.Done()
				return
			}
			ready.Done()
			<-release
			claim, claimErr := ledger.Claim(ctx, tx, occurrence)
			if claimErr != nil {
				errs[index] = claimErr
				_ = tx.Rollback(ctx)
				return
			}
			results[index] = claim
			if claim == ClaimDuplicate {
				// The loser must not carry any production work forward.
				_ = tx.Rollback(ctx)
				return
			}
			if err := ledger.Complete(ctx, tx, occurrence, OccurrenceMaterialized, 1, ""); err != nil {
				errs[index] = err
				_ = tx.Rollback(ctx)
				return
			}
			errs[index] = tx.Commit(ctx)
		}(index)
	}
	ready.Wait()
	close(release)
	finished.Wait()

	inserted, duplicate := 0, 0
	for index, err := range errs {
		if err != nil {
			t.Fatalf("replica %d: %v", index, err)
		}
		switch results[index] {
		case ClaimInserted:
			inserted++
		case ClaimDuplicate:
			duplicate++
		default:
			t.Fatalf("replica %d produced claim %q", index, results[index])
		}
	}
	if inserted != 1 || duplicate != replicas-1 {
		t.Fatalf("inserted=%d duplicate=%d across %d replicas, want exactly one winner",
			inserted, duplicate, replicas)
	}
	if count := occurrenceCount(t, pool, schedule.ID); count != 1 {
		t.Fatalf("ledger holds %d occurrences for one due time", count)
	}
}

// TestCrashBeforeCommitLeavesNoPartialOccurrence proves the crash window
// between the claim and the commit is safe: nothing durable survives, and the
// next window re-claims the same due time.
func TestCrashBeforeCommitLeavesNoPartialOccurrence(t *testing.T) {
	ctx := context.Background()
	pool := startLedgerPostgres(t)
	schedule := integrationSchedule(t)
	ledger := NewPostgresLedger()
	occurrence := NewOccurrence(
		schedule,
		mustTime(t, "2026-07-24T00:00:00Z"),
		mustTime(t, "2026-07-24T00:00:07Z"),
	)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := ledger.Claim(ctx, tx, occurrence); err != nil || claim != ClaimInserted {
		t.Fatalf("Claim() = %v / %v", claim, err)
	}
	if err := ledger.Complete(ctx, tx, occurrence, OccurrenceMaterialized, 1, ""); err != nil {
		t.Fatal(err)
	}
	// The process dies here; pgx rolls the transaction back on connection loss.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if count := occurrenceCount(t, pool, schedule.ID); count != 0 {
		t.Fatalf("rolled-back claim left %d durable occurrences", count)
	}

	retry, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = retry.Rollback(ctx) }()
	claim, err := ledger.Claim(ctx, retry, occurrence)
	if err != nil || claim != ClaimInserted {
		t.Fatalf("recovery Claim() = %v / %v; the due time must stay eligible", claim, err)
	}
	if err := ledger.Complete(ctx, retry, occurrence, OccurrenceMaterialized, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err := retry.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if count := occurrenceCount(t, pool, schedule.ID); count != 1 {
		t.Fatalf("recovery produced %d occurrences, want 1", count)
	}
}

// TestCrashAfterCommitKeepsTheOccurrenceClaimed proves the other crash window:
// once committed, a restart observes the occurrence and does not repeat it.
func TestCrashAfterCommitKeepsTheOccurrenceClaimed(t *testing.T) {
	ctx := context.Background()
	pool := startLedgerPostgres(t)
	schedule := integrationSchedule(t)
	ledger := NewPostgresLedger()
	occurrence := NewOccurrence(
		schedule,
		mustTime(t, "2026-07-24T00:00:00Z"),
		mustTime(t, "2026-07-24T00:00:07Z"),
	)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Claim(ctx, tx, occurrence); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Complete(ctx, tx, occurrence, OccurrenceMaterialized, 3, ""); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	restarted, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Rollback(ctx) }()
	claim, err := ledger.Claim(ctx, restarted, occurrence)
	if err != nil || claim != ClaimDuplicate {
		t.Fatalf("restart Claim() = %v / %v, want a duplicate", claim, err)
	}
	anchor, present, err := ledger.LastOccurrence(ctx, restarted, schedule.ID)
	if err != nil || !present || !anchor.ScheduledFor.Equal(occurrence.ScheduledFor) {
		t.Fatalf("LastOccurrence() = %+v present=%t err=%v", anchor, present, err)
	}
	// The observation instant is what an interval cadence anchors its next
	// eligible boundary on, so it has to survive the round trip too.
	if !anchor.ObservedAt.Equal(occurrence.ObservedAt) {
		t.Fatalf("anchor observed_at = %s, want %s", anchor.ObservedAt, occurrence.ObservedAt)
	}
}

// TestChangedIdentityDerivationFailsInsteadOfDuplicating proves the second
// guard: if the occurrence key derivation ever changed, the unique constraint
// on (schedule_id, scheduled_for) turns a would-be duplicate product effect
// into a hard conflict.
func TestChangedIdentityDerivationFailsInsteadOfDuplicating(t *testing.T) {
	ctx := context.Background()
	pool := startLedgerPostgres(t)
	schedule := integrationSchedule(t)
	ledger := NewPostgresLedger()
	occurrence := NewOccurrence(
		schedule,
		mustTime(t, "2026-07-24T00:00:00Z"),
		mustTime(t, "2026-07-24T00:00:07Z"),
	)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Claim(ctx, tx, occurrence); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Complete(ctx, tx, occurrence, OccurrenceMaterialized, 1, ""); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	rederived := occurrence
	rederived.Key = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	conflicting, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conflicting.Rollback(ctx) }()
	if _, err := ledger.Claim(ctx, conflicting, rederived); !errors.Is(err, ErrOccurrenceConflict) {
		t.Fatalf("Claim() = %v, want ErrOccurrenceConflict", err)
	}
	if count := occurrenceCount(t, pool, schedule.ID); count != 1 {
		t.Fatalf("conflicting derivation produced %d occurrences", count)
	}
}

// TestSkippedOccurrenceRecordsItsReason proves an empty producer is
// distinguishable from a working one in the durable record.
func TestSkippedOccurrenceRecordsItsReason(t *testing.T) {
	ctx := context.Background()
	pool := startLedgerPostgres(t)
	schedule := integrationSchedule(t)
	ledger := NewPostgresLedger()
	occurrence := NewOccurrence(
		schedule,
		mustTime(t, "2026-07-24T00:00:00Z"),
		mustTime(t, "2026-07-24T00:00:07Z"),
	)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Claim(ctx, tx, occurrence); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Complete(
		ctx, tx, occurrence, OccurrenceSkipped, 0, "no_active_organizations",
	); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var status, reason string
	var handoffs int
	if err := pool.QueryRow(ctx, `
SELECT status, skip_reason, handoff_count
FROM public.fixed_schedule_occurrences
WHERE occurrence_key = $1`, occurrence.Key).Scan(&status, &reason, &handoffs); err != nil {
		t.Fatal(err)
	}
	if status != OccurrenceSkipped || reason != "no_active_organizations" || handoffs != 0 {
		t.Fatalf("status=%s reason=%s handoffs=%d", status, reason, handoffs)
	}

	// A materialized record with no handoff must be rejected outright: it is
	// exactly the "zero-work result satisfying acceptance" shape the cutover
	// rules forbid.
	second, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Rollback(ctx) }()
	if err := ledger.Complete(
		ctx, second, occurrence, OccurrenceMaterialized, 0, "",
	); !errors.Is(err, ErrLedgerUnavailable) {
		t.Fatalf("Complete() = %v, want a rejection", err)
	}
}
