//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestArmDeniedActiveFinalizeArmsAFinalizeWakeupAtNow pins the target
// mechanism: a finalize_sync_run outbox row is created (or upserted) with
// available_at == now, in the caller's own transaction -- not a Celery
// enqueue, and not a separate transaction.
func TestArmDeniedActiveFinalizeArmsAFinalizeWakeupAtNow(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := armDeniedActiveFinalize(ctx, tx, discoveryTestRun, now); err != nil {
			t.Fatalf("armDeniedActiveFinalize: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		var availableAt time.Time
		if err := pool.QueryRow(ctx, `SELECT status, available_at FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
			discoveryTestRun, outboxKindFinalizeSyncRun).Scan(&status, &availableAt); err != nil {
			t.Fatal(err)
		}
		if status != "pending" {
			t.Fatalf("status=%q want=pending", status)
		}
		if !availableAt.Equal(now) {
			t.Fatalf("available_at=%s want=%s (armed immediately, not deferred)", availableAt, now)
		}
	})
}

// TestArmDeniedActiveFinalizeRollsBackWithTheCallersTransaction pins the
// "same transaction as the caller" property empirically: if the caller's
// transaction never commits, the wakeup must not exist either.
func TestArmDeniedActiveFinalizeRollsBackWithTheCallersTransaction(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := armDeniedActiveFinalize(ctx, tx, discoveryTestRun, now); err != nil {
			t.Fatalf("armDeniedActiveFinalize: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}

		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind=$2`,
			discoveryTestRun, outboxKindFinalizeSyncRun).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("got %d rows, want 0 -- a rolled-back caller transaction must roll back the wakeup with it", count)
		}
	})
}
