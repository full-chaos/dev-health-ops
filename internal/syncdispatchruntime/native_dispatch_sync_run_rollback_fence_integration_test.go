//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This file is the codex-round-2 (CHAOS-4175) proof for the load-bearing
// mechanism claim the ruling-reversal prose makes: an uncommitted
// Publish's INSERT into worker_job_outbox holds ROW EXCLUSIVE, which
// blocks a concurrent jobroute.Controller.Rollback's own
// `LOCK TABLE public.worker_job_outbox IN SHARE ROW EXCLUSIVE MODE` until
// the producer's transaction commits or aborts. The route-hold tests in
// native_dispatch_sync_run_route_hold_integration_test.go prove the
// OUTCOME (a unit claimed under a bad route is held, not stranded); this
// file proves the MECHANISM they cited but never actually exercised --
// the same gap codex round 2 named.
//
// One refinement on team-lead's spec, found by tracing Rollback's actual
// statement order rather than assumed: Rollback's own precondition check
// (`SELECT count(*) ... WHERE status IN ('pending','claimed')`) runs
// AFTER the LOCK TABLE unblocks, in the SAME transaction, under READ
// COMMITTED -- so once the producer's now-committed PENDING row becomes
// visible, Rollback correctly REFUSES (ErrPendingOutbox), it does not
// silently roll the route back over live pending work. "Rollback proceeds
// and completes" here means the blocked call unblocks and RETURNS -- the
// fence is about serialization, not about forcing Rollback to a
// particular verdict once it can finally see what it's rolling back over.

func TestRollbackBlocksOnAnOpenProducerTransactionAndUnblocksOnCommit(t *testing.T) {
	withRouteHoldFixture(t, func(ctx context.Context, fixture routeHoldFixture) {
		producerTx, err := fixture.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// A pgx.Tx that is never committed nor rolled back holds its
		// connection (and any row locks) forever -- including through an
		// early t.Fatal unwinding this goroutine. Without this, a failure
		// anywhere below would leave the Rollback goroutine permanently
		// blocked on the still-held lock, wedging the whole test binary.
		defer func() { _ = producerTx.Rollback(ctx) }()

		unitID := "00000000-0000-4000-8000-0000000000ff"
		envelope := jobcontract.Envelope{
			ContractVersion: jobcontract.ContractVersionV1,
			OrganizationID:  ptrTo(discoveryTestOrg),
			CorrelationID:   "sync-run:" + discoveryTestRun,
			IdempotencyKey:  jobcontract.KindSyncProviderUnit + ":" + unitID,
			Domain:          jobcontract.DomainLink{Type: "sync_run_unit", ID: unitID},
			Payload:         jobcontract.ProviderUnitPayload{UnitID: unitID},
		}
		if err := fixture.service.producer.Publish(ctx, producerTx, jobcontract.KindSyncProviderUnit, envelope); err != nil {
			t.Fatalf("Publish (uncommitted): %v", err)
		}

		rollbackDone := make(chan error, 1)
		go func() {
			_, rollbackErr := fixture.controller.Rollback(ctx, jobcontract.KindSyncProviderUnit)
			rollbackDone <- rollbackErr
		}()
		waitForBlockedOutboxLock(t, ctx, fixture.pool)

		// The Rollback is provably blocked right now (waitForBlockedOutboxLock
		// only returns once Postgres itself reports a lock-waiter). Only
		// after the producer commits may it proceed.
		select {
		case err := <-rollbackDone:
			t.Fatalf("Rollback completed before the producer's transaction committed (err=%v) -- the fence did not hold", err)
		default:
		}

		if err := producerTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		select {
		case err := <-rollbackDone:
			// See the file doc comment: a real pending row is now visible
			// to Rollback's own precondition check, so ErrPendingOutbox is
			// the CORRECT unblocked verdict, not a fence failure.
			if !errors.Is(err, jobroute.ErrPendingOutbox) {
				t.Fatalf("Rollback() after commit = %v, want ErrPendingOutbox (a real pending row now exists)", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Rollback never unblocked after the producer committed")
		}
	})
}

// TestRollbackDoesNotBlockWithoutAnOpenProducerTransaction is the negative
// control: with nothing else touching worker_job_outbox, Rollback must not
// block on anything, and (with no pending work at all) must actually
// succeed.
func TestRollbackDoesNotBlockWithoutAnOpenProducerTransaction(t *testing.T) {
	withRouteHoldFixture(t, func(ctx context.Context, fixture routeHoldFixture) {
		done := make(chan error, 1)
		go func() {
			_, rollbackErr := fixture.controller.Rollback(ctx, jobcontract.KindSyncProviderUnit)
			done <- rollbackErr
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Rollback() = %v, want nil (nothing pending, nothing else holding the outbox table)", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Rollback blocked with no concurrent producer transaction open -- negative control failed")
		}

		var transport string
		if err := fixture.pool.QueryRow(ctx, `SELECT transport FROM worker_job_routes WHERE job_kind=$1`,
			jobcontract.KindSyncProviderUnit).Scan(&transport); err != nil {
			t.Fatal(err)
		}
		if transport != "celery" {
			t.Fatalf("route transport=%q after Rollback, want celery", transport)
		}
	})
}

func ptrTo(value string) *string { return &value }

// waitForBlockedOutboxLock polls pg_stat_activity, deterministically (no
// sleep-and-hope): it returns the instant Postgres itself reports a
// session waiting on Rollback's own LOCK TABLE statement, or fails the
// test after a bounded deadline if that never happens. Same technique as
// internal/jobroute/control_integration_test.go's own
// waitForBlockedRouteUpdate/waitForLockWaiter (unexported there, so not
// reusable across packages -- reimplemented here rather than exported
// solely for one cross-package test).
func waitForBlockedOutboxLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND query LIKE '%LOCK TABLE public.worker_job_outbox%'`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Rollback never blocked on the producer's worker_job_outbox lock")
}
