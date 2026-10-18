//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
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
}
