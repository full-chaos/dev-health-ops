//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-6955, the same class as selfprobe.Once, on the other readiness check that shares the
// one-connection readiness pool: posture_manifest_lockstep returned a bare ErrUnavailable when its
// query failed, dropping the cause, so a query that waited for the busy pool until its deadline
// read as a hard failure (health.Registry and the worker's preclaim-readiness retry classify a
// timeout by errors.Is(err, context.DeadlineExceeded)).
func TestPostureManifestLockstepOnABusyPoolKeepsItsDeadlineClass(t *testing.T) {
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
	held, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	checkCtx, checkCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer checkCancel()
	_, err = CheckPostureManifestLockstep(checkCtx, pool, "some-digest")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckPostureManifestLockstep on a busy pool = %v, want ErrUnavailable", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CheckPostureManifestLockstep on a busy pool = %v: the deadline class was dropped, so the "+
			"registry reports a slow check as a hard failure and preclaim-readiness exits instead of retrying", err)
	}
}

// r1 P1 (fixed): a query that returns because it was CANCELED -- not because it ran into its own
// deadline -- must never be classified as the same retryable timeout a deadline is. Canceling the
// caller's own context (rather than letting a deadline expire) reproduces that class directly.
func TestPostureManifestLockstepNeverClassifiesCancellationAsARetryableTimeout(t *testing.T) {
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
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	checkCtx, checkCancel := context.WithCancel(ctx)
	checkCancel()
	_, err = CheckPostureManifestLockstep(checkCtx, pool, "some-digest")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckPostureManifestLockstep(canceled ctx) = %v, want ErrUnavailable", err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CheckPostureManifestLockstep(canceled ctx) = %v: a cancellation must not read as a retryable timeout", err)
	}
}
