//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncbudget"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// contentionRunID and friends name the runs of the contention fixture: one org,
// one provider and cost class, so every run's unit lands in ONE dispatch bucket
// and ONE budget bucket.
func contentionID(kind, index int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-0000000%02d%03d", kind, index)
}

// withSmallDispatchPool is withDispatchServicePool with the production default
// domain pool size (4): the size at which four concurrent Dispatch passes can
// hold every connection between them.
func withSmallDispatchPool(t *testing.T, maxConns int32, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createReferenceDiscoveryTables(t, ctx, pool)
	pgseed.Org(ctx, t, pool, discoveryTestOrg, "community")
	fn(ctx, pool)
}

// seedContentionRuns seeds n dispatchable runs (reference discovery done, one
// planned unit each) that all fall in the same dispatch and budget bucket, and
// returns each run's Dispatch arguments.
func seedContentionRuns(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int) []DispatchSyncRunArgs {
	t.Helper()
	if got := pgseed.SyncTransportRoute(ctx, t, pool, "dispatch_sync_run", "river", dispatchRouteGeneration, false, "celery"); got != dispatchRouteGeneration {
		t.Fatalf("dispatch_sync_run route generation = %d, want %d", got, dispatchRouteGeneration)
	}
	pgseed.EnsureSyncIntegration(ctx, t, pool, discoveryTestOrg, discoveryTestIntegration, dispatchTestSource)
	now := pgNow()
	args := make([]DispatchSyncRunArgs, n)
	for index := 0; index < n; index++ {
		runID, outboxID, unitID, discoveryID := contentionID(1, index), contentionID(2, index), contentionID(3, index), contentionID(4, index)
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration})
		pgseed.SyncDispatchOutbox(ctx, t, pool, outboxID, runID, discoveryTestOrg, "dispatch_sync_run", "dispatched", "river", dispatchRouteGeneration)
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,1,now(),now(),now())`, discoveryID, runID, discoveryTestOrg, discoveryStatusSuccess); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$5,'planned',$4,$6,'rest_core','incremental',0,now())`,
			unitID, discoveryTestOrg, runID, now, dispatchTestSource, discoveryTestIntegration); err != nil {
			t.Fatal(err)
		}
		args[index] = DispatchSyncRunArgs{TransportArgs: TransportArgs{
			Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: runID,
			DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration,
		}}
	}
	return args
}

// newContentionDispatchService builds the production wiring: the REAL in-process
// budget estimator over the same domain pool the service opens its transaction on.
func newContentionDispatchService(t *testing.T, pool *pgxpool.Pool) *NativeDispatchSyncRunService {
	t.Helper()
	registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{
		jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
	}}
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	estimator, err := newInProcessBudgetEstimator(pool, BudgetEstimatorDependencies{
		Decryptor: unusedDecryptor{}, Getenv: func(string) string { return "" },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeDispatchSyncRunService(pool, slog.New(slog.NewTextHandler(io.Discard, nil)), estimator, producer, registry)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

// runConcurrentDispatches runs one Dispatch per args on its own goroutine, each
// bounded by budget, and returns every error (nil for a successful pass).
func runConcurrentDispatches(ctx context.Context, service *NativeDispatchSyncRunService, args []DispatchSyncRunArgs, budget time.Duration) []error {
	errs := make([]error, len(args))
	var wait sync.WaitGroup
	for index := range args {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			passContext, cancel := context.WithTimeout(ctx, budget)
			defer cancel()
			errs[index] = service.Dispatch(passContext, args[index])
		}(index)
	}
	wait.Wait()
	return errs
}

// TestConcurrentDispatchOnOneBucketDoesNotStarveASmallPool is the production
// stall of CHAOS-6889, reproduced: with a 4-connection domain pool, four
// Dispatch passes on ONE bucket. Each opens a transaction (one connection). The
// first takes the bucket's advisory lock; the other three park in
// pg_advisory_xact_lock, HOLDING their connections. The first pass's budget
// estimator then reads through the POOL for a fifth connection that never
// frees: the lock holder waits for a connection, the waiters wait for its lock,
// until the pass deadline (and every other user of the pool, a lease heartbeat
// included, is starved meanwhile).
//
// RED CONTROL: on the code this replaces, at least one pass returns a deadline
// error and the pool sits at 4/4 acquired for the whole budget.
func TestConcurrentDispatchOnOneBucketDoesNotStarveASmallPool(t *testing.T) {
	withSmallDispatchPool(t, 4, func(ctx context.Context, pool *pgxpool.Pool) {
		args := seedContentionRuns(t, ctx, pool, 4)
		service := newContentionDispatchService(t, pool)

		started := time.Now()
		errs := runConcurrentDispatches(ctx, service, args, 12*time.Second)
		elapsed := time.Since(started)
		for index, err := range errs {
			if err != nil {
				t.Errorf("dispatch pass %d: %v (after %s), want nil: the pass stalled on the pool or the bucket lock", index, err, elapsed)
				if errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("dispatch pass %d hit its deadline", index)
				}
			}
		}
		var dispatching int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_runs WHERE status = $1`, syncRunStatusDispatching).Scan(&dispatching); err != nil {
			t.Fatal(err)
		}
		if dispatching != len(args) {
			t.Errorf("%d of %d runs reached dispatching", dispatching, len(args))
		}
		if elapsed > 10*time.Second {
			t.Errorf("four serialized passes took %s, want them well inside the 12s budget", elapsed)
		}
	})
}

// TestDispatchGivesItsConnectionBackWhenAnotherPassHoldsTheBucketLock is the
// bounded-wait half of CHAOS-6889: a pass that cannot take its bucket lock inside
// the wait budget returns a retryable ErrDispatchLockBusy and its transaction
// connection goes back to the pool, instead of parking the connection in a lock
// wait for as long as the holder runs.
//
// RED CONTROL: on the code this replaces the pass blocks in
// pg_advisory_xact_lock until its own deadline (12s here), holding the connection.
func TestDispatchGivesItsConnectionBackWhenAnotherPassHoldsTheBucketLock(t *testing.T) {
	previous := dispatchLockWaitBudget
	dispatchLockWaitBudget = 600 * time.Millisecond
	t.Cleanup(func() { dispatchLockWaitBudget = previous })

	withSmallDispatchPool(t, 4, func(ctx context.Context, pool *pgxpool.Pool) {
		args := seedContentionRuns(t, ctx, pool, 1)
		service := newContentionDispatchService(t, pool)

		// Another pass (a stand-in transaction) holds the bucket's advisory lock.
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`,
			bucketAdvisoryLockKey(dispatchBucket{orgID: discoveryTestOrg, provider: "github", costClass: "rest_core"})); err != nil {
			t.Fatal(err)
		}

		passContext, cancel := context.WithTimeout(ctx, 12*time.Second)
		defer cancel()
		started := time.Now()
		err = service.Dispatch(passContext, args[0])
		elapsed := time.Since(started)

		if !isDispatchLockBusy(err) {
			t.Fatalf("Dispatch = %v after %s, want ErrDispatchLockBusy (a bounded wait, retryable)", err, elapsed)
		}
		if !errors.Is(err, ErrDiscoveryTransientFailure) {
			t.Errorf("the lock-busy outcome is not in the retryable transient class: %v", err)
		}
		if elapsed > 5*time.Second {
			t.Errorf("gave up after %s, want about the 600ms wait budget", elapsed)
		}
		// Only the holder's connection is still out: the pass returned its own.
		if acquired := pool.Stat().AcquiredConns(); acquired != 1 {
			t.Errorf("%d pool connections acquired after the pass gave up, want only the holder's", acquired)
		}
		var dispatching int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_runs WHERE status = $1`, syncRunStatusDispatching).Scan(&dispatching); err != nil {
			t.Fatal(err)
		}
		if dispatching != 0 {
			t.Errorf("%d runs marked dispatching by a pass that gave up before authorizing", dispatching)
		}
	})
}

// failingTxEstimator is an estimator that reads through the caller's transaction
// and fails with a database error mid-read.
type failingTxEstimator struct {
	read func(db interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}) error
}

func (failingTxEstimator) DispatchBudgetEstimate(context.Context, string, string, []string) (map[string][]budgetEstimate, error) {
	return nil, errors.New("must read through the transaction")
}

func (estimator failingTxEstimator) DispatchBudgetEstimateOn(
	_ context.Context, db syncbudget.Querier, _, _ string, _ []string,
) (map[string][]budgetEstimate, error) {
	return nil, estimator.read(db)
}

// TestEstimateChunkKeepsThePassTransactionAliveAfterAFailedRead: the estimator now
// reads on the pass's own transaction, where a failing statement would abort it
// (25P02 on every statement after). Each chunk runs in a savepoint that is rolled
// back on failure, so the chunk fails open as it did on the estimator's own
// connection and the pass carries on.
func TestEstimateChunkKeepsThePassTransactionAliveAfterAFailedRead(t *testing.T) {
	withSmallDispatchPool(t, 4, func(ctx context.Context, pool *pgxpool.Pool) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		estimator := failingTxEstimator{read: func(db interface {
			QueryRow(context.Context, string, ...any) pgx.Row
		}) error {
			var n int
			return db.QueryRow(ctx, `SELECT 1/0`).Scan(&n)
		}}
		if _, err := estimateChunk(ctx, tx, estimator, discoveryTestOrg, discoveryTestRun, []string{"u"}); err == nil {
			t.Fatal("the failing read reported no error")
		}
		var one int
		if err := tx.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
			t.Fatalf("the pass transaction is unusable after a failed estimate read: %v (want the savepoint rolled back)", err)
		}
	})
}
