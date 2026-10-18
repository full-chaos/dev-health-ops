//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Round 456 finding: a waiter for one organization and day must not hold a pool connection while it waits. With a
// pool of two connections a holder and a same-key waiter used to exhaust it, so another organization could not even
// ask for its lock.
func TestICFinalizeDayLockWaitersDoNotExhaustASmallPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	reader := sdkmetric.NewManualReader()
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previousProvider) })
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	const orgA, orgB = "00000000-0000-4000-8000-00000000a001", "00000000-0000-4000-8000-00000000b001"

	releaseA, err := store.LockICFinalizeDay(ctx, orgA, day)
	if err != nil {
		t.Fatal(err)
	}
	// The lock must be held by an open TRANSACTION, not a session: that is what keeps it correct behind a
	// transaction-mode PgBouncer (round finding). A session lock would leave its backend idle with no transaction.
	var holderInTransaction bool
	if err := pool.QueryRow(ctx, `
SELECT COALESCE(bool_or(a.state = 'idle in transaction' AND a.xact_start IS NOT NULL), false)
FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'advisory' AND l.granted AND a.pid <> pg_backend_pid()`).Scan(&holderInTransaction); err != nil {
		t.Fatal(err)
	}
	if !holderInTransaction {
		t.Fatal("the day lock is not held by an open transaction (a session lock is not safe behind a transaction-mode pooler)")
	}
	waiterCtx, stopWaiter := context.WithCancel(ctx)
	waiterDone := make(chan error, 1)
	go func() {
		release, err := store.LockICFinalizeDay(waiterCtx, orgA, day)
		if err == nil {
			release()
		}
		waiterDone <- err
	}()
	time.Sleep(time.Second)

	otherCtx, otherCancel := context.WithTimeout(ctx, 10*time.Second)
	defer otherCancel()
	releaseB, err := store.LockICFinalizeDay(otherCtx, orgB, day)
	if err != nil {
		t.Fatalf("another organization could not take its lock while a same-key waiter waited on a pool of two: %v", err)
	}
	releaseB()

	stopWaiter()
	if err := <-waiterDone; err == nil {
		t.Fatal("the cancelled waiter must fail with the context")
	}
	releaseA()
	again, err := store.LockICFinalizeDay(ctx, orgA, day)
	if err != nil {
		t.Fatalf("the lock was not released: %v", err)
	}
	again()

	var resource metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &resource); err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]uint64{}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != ICFinalizeLockWaitHistogram {
				continue
			}
			for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
				outcome, _ := point.Attributes.Value("outcome")
				outcomes[outcome.AsString()] += point.Count
			}
		}
	}
	if outcomes["taken"] < 3 || outcomes["abandoned"] != 1 {
		t.Fatalf("wait histogram outcomes = %v, want taken>=3 and abandoned=1", outcomes)
	}
}
