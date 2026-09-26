//go:build integration

package selfprobe_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-6955, executed against a real pool: the readiness pool has ONE connection. While another
// readiness check holds it (the cold domain posture check, ~10 s under roll load at rev 191), the
// domain_transaction probe's BEGIN waits for a connection until its deadline. The registry must
// report that as TimedOut, the class the worker's preclaim-readiness retry retries; reported as a
// hard failure, go-sync exited on attempt 1.
func TestDomainTransactionProbeOnABusyReadinessPoolIsATimeoutNotAHardFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	held, err := pool.Acquire(ctx) // another check holds the only connection
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	registry := health.NewRegistry(400 * time.Millisecond)
	if err := registry.RegisterRequired("domain_transaction", func(checkCtx context.Context) error {
		return selfprobe.Once(checkCtx, selfprobe.NewPool(pool))
	}); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	readiness := registry.CheckRequired(ctx)
	if readiness.Ready || len(readiness.Checks) != 1 || !readiness.Checks[0].Failed {
		t.Fatalf("a probe with no connection available must fail: %+v", readiness)
	}
	if !readiness.Checks[0].TimedOut {
		t.Fatalf("domain_transaction on a busy readiness pool was reported as a hard failure, not a timeout: %+v "+
			"(the worker's preclaim-readiness retry runs only when every failed check timed out)", readiness.Checks[0])
	}
}
