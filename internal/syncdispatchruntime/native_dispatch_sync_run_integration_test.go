//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func withDispatchGatePool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createReferenceDiscoveryTables(t, ctx, pool)
	seedDiscoveryRoute(t, ctx, pool)
	fn(ctx, pool)
}

// TestReferenceDiscoverySucceededIsFalseWithNoLedgerRow pins the common
// case: dispatch_sync_run runs before reference discovery has ever
// attempted this run.
func TestReferenceDiscoverySucceededIsFalseWithNoLedgerRow(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		succeeded, err := referenceDiscoverySucceeded(ctx, tx, discoveryTestRun)
		if err != nil {
			t.Fatalf("referenceDiscoverySucceeded: %v", err)
		}
		if succeeded {
			t.Fatal("succeeded=true, want false with no ledger row")
		}
	})
}

// TestReferenceDiscoverySucceededIsFalseForAnyNonSuccessStatus is the
// mismatched-value fixture: a ledger row exists but is retrying/failed/
// running/planned, never success -- must not be mistaken for success.
func TestReferenceDiscoverySucceededIsFalseForAnyNonSuccessStatus(t *testing.T) {
	for _, status := range []string{discoveryStatusPlanned, discoveryStatusRunning, discoveryStatusRetrying, discoveryStatusFailed} {
		t.Run(status, func(t *testing.T) {
			withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at)
VALUES ($1,$2,$3,$4,1,now())`,
					"00000000-0000-4000-8000-0000000000d9", discoveryTestRun, discoveryTestOrg, status); err != nil {
					t.Fatal(err)
				}
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback(ctx) }()
				succeeded, err := referenceDiscoverySucceeded(ctx, tx, discoveryTestRun)
				if err != nil {
					t.Fatalf("referenceDiscoverySucceeded: %v", err)
				}
				if succeeded {
					t.Fatalf("succeeded=true for status=%q, want false", status)
				}
			})
		})
	}
}

// TestReferenceDiscoverySucceededIsTrueForASuccessLedger pins the positive
// case dispatch_sync_run's gate actually lets through.
func TestReferenceDiscoverySucceededIsTrueForASuccessLedger(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at)
VALUES ($1,$2,$3,$4,1,now())`,
			"00000000-0000-4000-8000-0000000000da", discoveryTestRun, discoveryTestOrg, discoveryStatusSuccess); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		succeeded, err := referenceDiscoverySucceeded(ctx, tx, discoveryTestRun)
		if err != nil {
			t.Fatalf("referenceDiscoverySucceeded: %v", err)
		}
		if !succeeded {
			t.Fatal("succeeded=false for a status=success ledger, want true")
		}
	})
}

// TestEnsureReferenceDiscoveryWakeupCreatesAPlannedLedgerAndArmsItsWakeup
// pins the common case: no ledger yet -- one is created PLANNED with
// available_at=now, and the reference_discovery outbox wakeup is armed at
// that same time.
func TestEnsureReferenceDiscoveryWakeupCreatesAPlannedLedgerAndArmsItsWakeup(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		// seedDiscoveryRoute already inserts a `reference_discovery` outbox
		// row (dispatched, at seed time) as the route-currency fixture other
		// tests in this file need. It is NOT what this test is about, and
		// upsertDiscoveryOutboxWakeup's ON CONFLICT keeps the EARLIER of the
		// two available_at values -- left in place, it would silently make
		// this test pass by coincidence (or fail by timing flake) instead of
		// actually proving ensureReferenceDiscoveryWakeup's own arming.
		if _, err := pool.Exec(ctx, `DELETE FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`, discoveryTestRun); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ensureReferenceDiscoveryWakeup(ctx, tx, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatalf("ensureReferenceDiscoveryWakeup: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		var availableAt time.Time
		if err := pool.QueryRow(ctx, `SELECT status, available_at FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
			discoveryTestRun).Scan(&status, &availableAt); err != nil {
			t.Fatal(err)
		}
		if status != discoveryStatusPlanned {
			t.Fatalf("ledger status=%q want=planned", status)
		}
		if !availableAt.Equal(now) {
			t.Fatalf("ledger available_at=%s want=%s", availableAt, now)
		}

		var wakeupAvailableAt time.Time
		if err := pool.QueryRow(ctx, `SELECT available_at FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`,
			discoveryTestRun).Scan(&wakeupAvailableAt); err != nil {
			t.Fatal(err)
		}
		if !wakeupAvailableAt.Equal(now) {
			t.Fatalf("wakeup available_at=%s want=%s", wakeupAvailableAt, now)
		}
	})
}

// TestEnsureReferenceDiscoveryWakeupDoesNotPullAnExistingBackoffEarlier is
// the mismatched-value fixture: a ledger already RETRYING with a FUTURE
// available_at (mid-backoff) must keep that future time -- calling
// ensure_reference_discovery_wakeup again (e.g. because dispatch_sync_run
// ran again before the backoff elapsed) must not reset/pull the wakeup
// earlier to `now`.
func TestEnsureReferenceDiscoveryWakeupDoesNotPullAnExistingBackoffEarlier(t *testing.T) {
	withDispatchGatePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		futureAvailableAt := now.Add(10 * time.Minute)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at)
VALUES ($1,$2,$3,$4,2,$5)`,
			"00000000-0000-4000-8000-0000000000db", discoveryTestRun, discoveryTestOrg, discoveryStatusRetrying, futureAvailableAt); err != nil {
			t.Fatal(err)
		}
		// Same reasoning as the sibling test above: clear seedDiscoveryRoute's
		// pre-existing reference_discovery outbox row so the LEAST-preserving
		// upsert is only ever comparing against THIS test's futureAvailableAt.
		if _, err := pool.Exec(ctx, `DELETE FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`, discoveryTestRun); err != nil {
			t.Fatal(err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := ensureReferenceDiscoveryWakeup(ctx, tx, discoveryTestOrg, discoveryTestRun, now); err != nil {
			t.Fatalf("ensureReferenceDiscoveryWakeup: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var ledgerAvailableAt time.Time
		if err := pool.QueryRow(ctx, `SELECT available_at FROM sync_run_reference_discoveries WHERE sync_run_id=$1`,
			discoveryTestRun).Scan(&ledgerAvailableAt); err != nil {
			t.Fatal(err)
		}
		if !ledgerAvailableAt.Equal(futureAvailableAt) {
			t.Fatalf("ledger available_at=%s want=%s (must not be reset by re-ensuring)", ledgerAvailableAt, futureAvailableAt)
		}

		var wakeupAvailableAt time.Time
		if err := pool.QueryRow(ctx, `SELECT available_at FROM sync_dispatch_outbox WHERE sync_run_id=$1 AND kind='reference_discovery'`,
			discoveryTestRun).Scan(&wakeupAvailableAt); err != nil {
			t.Fatal(err)
		}
		if !wakeupAvailableAt.Equal(futureAvailableAt) {
			t.Fatalf("wakeup available_at=%s want=%s (must be armed at the ledger's own backoff time, not now)", wakeupAvailableAt, futureAvailableAt)
		}
	})
}
