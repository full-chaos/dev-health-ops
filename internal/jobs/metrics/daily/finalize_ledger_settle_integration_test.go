//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A repaired metric_compatibility_executions row (state='retry_authorized',
// left there by `metrics execution-repair`/`daily-redrive`'s ledger_repair
// step) must not survive the run's own successful finalize: nothing else in
// the native path ever re-claims that row, so without an explicit settle it
// sits at 'retry_authorized' forever even after the run itself reaches
// status='succeeded'/finalization_status='succeeded' -- exactly the shape a
// later reader of the ledger cannot tell apart from "still needs a retry".
func TestCompleteFinalizeSettlesARepairedLedgerRowToSucceeded(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000004001"
		runID       = "00000000-0000-4000-8000-000000004002"
		partitionID = "00000000-0000-4000-8000-000000004003"
		ledgerID    = "00000000-0000-4000-8000-000000004004"
	)
	now := time.Date(2026, 9, 4, 16, 2, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)
	insertCompatibilityLedgerRow(t, ctx, pool, ledgerID, "finalize", runID, "", "retry_authorized", now)

	claim, err := store.ClaimFinalize(ctx, runID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimFinalize = %#v, %v", claim, err)
	}
	if err := store.CompleteFinalize(ctx, *claim); err != nil {
		t.Fatalf("CompleteFinalize: %v", err)
	}

	var state string
	var completedAt *time.Time
	var evidence []byte
	if err := pool.QueryRow(ctx,
		`SELECT state, completed_at, output_evidence FROM metric_compatibility_executions WHERE id = $1::uuid`,
		ledgerID,
	).Scan(&state, &completedAt, &evidence); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" || completedAt == nil || len(evidence) == 0 {
		t.Fatalf("ledger row after CompleteFinalize: state=%s completed_at=%v evidence=%q, want succeeded/non-nil/non-empty",
			state, completedAt, evidence)
	}
}

// The native finalize path never authorizes a further automatic retry --
// the run's own status/finalization_status carry the real terminal outcome
// ('failed'/'failed'). metric_compatibility_executions has no 'failed'
// state at all (migration 0059's own CHECK: 'executing', 'succeeded',
// 'ambiguous', 'retry_authorized' are the only values it accepts), so a
// permanent finalize failure leaves a repaired ledger row exactly where it
// was: 'retry_authorized' still accurately describes "an operator-reviewed
// retry remains valid for this row", which stays true regardless of how
// THIS attempt ended. This test pins that the failure path does not
// mutate, let alone corrupt, that row.
func TestFailFinalizePermanentlyLeavesARepairedLedgerRowUntouched(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newFinalizeRedriveTestStack(t)

	const (
		orgID    = "00000000-0000-4000-8000-000000004011"
		runID    = "00000000-0000-4000-8000-000000004012"
		ledgerID = "00000000-0000-4000-8000-000000004014"
	)
	now := time.Date(2026, 9, 4, 16, 2, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), now)
	// ClaimFinalize's partitions-ready gate needs at least one succeeded
	// partition -- this run's own success/failure is decided by the
	// finalize claim below, not by partition outcome.
	insertFinalizeTestPartition(t, ctx, pool, uuid.NewString(), runID, 0, "succeeded", now)
	insertCompatibilityLedgerRow(t, ctx, pool, ledgerID, "finalize", runID, "", "retry_authorized", now)

	claim, err := store.ClaimFinalize(ctx, runID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimFinalize = %#v, %v", claim, err)
	}
	if err := store.FailFinalizePermanently(ctx, *claim); err != nil {
		t.Fatalf("FailFinalizePermanently: %v", err)
	}

	var runStatus, finalizationStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid`, runID,
	).Scan(&runStatus, &finalizationStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "failed" || finalizationStatus != "failed" {
		t.Fatalf("run terminal state = %s/%s, want failed/failed", runStatus, finalizationStatus)
	}

	var state string
	var completedAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT state, completed_at FROM metric_compatibility_executions WHERE id = $1::uuid`, ledgerID,
	).Scan(&state, &completedAt); err != nil {
		t.Fatal(err)
	}
	if state != "retry_authorized" || completedAt != nil {
		t.Fatalf("ledger row after a permanent finalize failure = state=%s completed_at=%v, want retry_authorized/nil unchanged",
			state, completedAt)
	}
}

// The ordinary case -- a run with no compatibility-ledger row at all, which
// is every run started after the bridge that used to write one was deleted
// -- must complete exactly as before: no row is inserted, no error, nothing
// to settle.
func TestCompleteFinalizeWithNoLedgerRowCompletesNormally(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000004021"
		runID       = "00000000-0000-4000-8000-000000004022"
		partitionID = "00000000-0000-4000-8000-000000004023"
	)
	now := time.Date(2026, 9, 4, 16, 2, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	insertFinalizeTestRun(t, ctx, pool, runID, orgID, time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), now)
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "succeeded", now)

	claim, err := store.ClaimFinalize(ctx, runID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimFinalize = %#v, %v", claim, err)
	}
	if err := store.CompleteFinalize(ctx, *claim); err != nil {
		t.Fatalf("CompleteFinalize: %v", err)
	}

	var ledgerRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM metric_compatibility_executions`).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if ledgerRows != 0 {
		t.Fatalf("ledger rows after a run with none to begin with = %d, want 0", ledgerRows)
	}
}

// settleCompatibilityLedgerTx itself is idempotent: a row it already moved
// to 'succeeded' matches nothing on a second pass, so calling it again (the
// shape a duplicate River delivery of the same completion would produce)
// changes nothing and returns no error.
func TestSettleCompatibilityLedgerTxIsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool, _, _ := newFinalizeRedriveTestStack(t)

	const (
		runID    = "00000000-0000-4000-8000-000000004031"
		ledgerID = "00000000-0000-4000-8000-000000004032"
	)
	now := time.Date(2026, 9, 4, 16, 2, 0, 0, time.UTC)
	insertCompatibilityLedgerRow(t, ctx, pool, ledgerID, "finalize", runID, "", "retry_authorized", now)

	for attempt := 0; attempt < 2; attempt++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := settleCompatibilityLedgerTx(ctx, tx, now, "finalize", runID, ""); err != nil {
			t.Fatalf("settleCompatibilityLedgerTx attempt %d: %v", attempt, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	var state string
	var completedAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT state, completed_at FROM metric_compatibility_executions WHERE id = $1::uuid`, ledgerID,
	).Scan(&state, &completedAt); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" || completedAt == nil {
		t.Fatalf("ledger row after two settle passes = state=%s completed_at=%v, want succeeded/non-nil", state, completedAt)
	}
}

// The partition path has the identical gap: a redriven partition's own
// repaired ledger row (operation='partition') never gets re-claimed by the
// native path either, so CompletePartition must settle it exactly the way
// CompleteFinalize settles the finalize-scope row above.
func TestCompletePartitionSettlesARepairedLedgerRowToSucceeded(t *testing.T) {
	ctx := context.Background()
	pool, store, publisher := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000004041"
		runID       = "00000000-0000-4000-8000-000000004042"
		partitionID = "00000000-0000-4000-8000-000000004043"
		ledgerID    = "00000000-0000-4000-8000-000000004044"
	)
	now := time.Date(2026, 9, 4, 16, 2, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,'2026-09-04','daily-v1','running','pending',$3,$3)`, runID, orgID, now); err != nil {
		t.Fatal(err)
	}
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "pending", now)
	insertCompatibilityLedgerRow(t, ctx, pool, ledgerID, "partition", runID, partitionID, "retry_authorized", now)

	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimPartition = %#v, %v", claim, err)
	}
	if err := store.CompletePartition(ctx, *claim, publisher); err != nil {
		t.Fatalf("CompletePartition: %v", err)
	}

	var state string
	var completedAt *time.Time
	var evidence []byte
	if err := pool.QueryRow(ctx,
		`SELECT state, completed_at, output_evidence FROM metric_compatibility_executions WHERE id = $1::uuid`,
		ledgerID,
	).Scan(&state, &completedAt, &evidence); err != nil {
		t.Fatal(err)
	}
	if state != "succeeded" || completedAt == nil || len(evidence) == 0 {
		t.Fatalf("partition ledger row after CompletePartition: state=%s completed_at=%v evidence=%q, want succeeded/non-nil/non-empty",
			state, completedAt, evidence)
	}
}

// Mirrors TestFailFinalizePermanentlyLeavesARepairedLedgerRowUntouched for
// the partition path: FailPartitionPermanently has the identical "no
// 'failed' state exists on this ledger" constraint, so it must not touch a
// repaired row either.
func TestFailPartitionPermanentlyLeavesARepairedLedgerRowUntouched(t *testing.T) {
	ctx := context.Background()
	pool, store, _ := newFinalizeRedriveTestStack(t)

	const (
		orgID       = "00000000-0000-4000-8000-000000004051"
		runID       = "00000000-0000-4000-8000-000000004052"
		partitionID = "00000000-0000-4000-8000-000000004053"
		ledgerID    = "00000000-0000-4000-8000-000000004054"
	)
	now := time.Date(2026, 9, 4, 16, 2, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,'2026-09-04','daily-v1','running','pending',$3,$3)`, runID, orgID, now); err != nil {
		t.Fatal(err)
	}
	insertFinalizeTestPartition(t, ctx, pool, partitionID, runID, 0, "pending", now)
	insertCompatibilityLedgerRow(t, ctx, pool, ledgerID, "partition", runID, partitionID, "retry_authorized", now)

	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("ClaimPartition = %#v, %v", claim, err)
	}
	if err := store.FailPartitionPermanently(ctx, *claim, "post_bridge_family_incomplete"); err != nil {
		t.Fatalf("FailPartitionPermanently: %v", err)
	}

	var partitionStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID,
	).Scan(&partitionStatus); err != nil {
		t.Fatal(err)
	}
	if partitionStatus != "failed_permanent" {
		t.Fatalf("partition status = %s, want failed_permanent", partitionStatus)
	}

	var state string
	var completedAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT state, completed_at FROM metric_compatibility_executions WHERE id = $1::uuid`, ledgerID,
	).Scan(&state, &completedAt); err != nil {
		t.Fatal(err)
	}
	if state != "retry_authorized" || completedAt != nil {
		t.Fatalf("partition ledger row after a permanent failure = state=%s completed_at=%v, want retry_authorized/nil unchanged",
			state, completedAt)
	}
}
