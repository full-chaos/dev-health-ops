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

	// r1 P3 (fixed): the registry's own checkTimeout governs TWO independent clocks that fire
	// within microseconds of each other when they share one duration -- the context handed to the
	// check function, and the registry's own fallback wait for an answer at all (registry.go's
	// "wait_expired" branch). Giving both the SAME 400ms bound raced them: on the pre-fix code
	// (which dropped the deadline cause), the registry's own fallback sometimes won that race and
	// reported TimedOut=true regardless of what Once actually returned, letting the test pass on
	// broken code about 1 run in 8. The registry here gets a bound many times longer than the
	// probe's OWN internal deadline (imposed by the check function itself, not by checkTimeout), so
	// the registry's fallback can never fire first: the result is attributable ONLY to what Once
	// returns for its own, much shorter, deadline.
	const probeBudget = 300 * time.Millisecond
	registry := health.NewRegistry(5 * time.Second)
	if err := registry.RegisterRequired("domain_transaction", func(checkCtx context.Context) error {
		boundedCtx, boundedCancel := context.WithTimeout(checkCtx, probeBudget)
		defer boundedCancel()
		return selfprobe.Once(boundedCtx, selfprobe.NewPool(pool))
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
	if readiness.Checks[0].Cause != "timeout" {
		t.Fatalf("Cause = %q, want %q (not the registry's own generic wait_expired fallback)", readiness.Checks[0].Cause, "timeout")
	}
}
