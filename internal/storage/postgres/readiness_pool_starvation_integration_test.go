//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-6771 mechanism repro. A worker's domain pool is small (4 connections
// by default) and the same pool serves job claims, dispatch and outbox work.
// When the work holds every connection, a readiness check that needs a
// connection from that pool cannot even start its query and runs into its
// deadline: the same "context deadline exceeded" go-sync logged for
// domain_postgres, posture_manifest_lockstep and idempotency_backend while
// its sync backlog was draining.
func TestReadinessOnAnExhaustedWorkPoolTimesOutWithoutReachingTheDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(roles.domain, grantDomainPass)
	config, err := pgxpool.ParseConfig(parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 4 // the production default, config.go defaultDomainMaxConns
	workPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workPool.Close)

	// The check passes on an idle pool: the failure below is the load, not the grants.
	if err := CheckDomainAuthorization(ctx, workPool, roles.domain, grantSchema); err != nil {
		t.Fatalf("idle pool: %v", err)
	}

	// Four jobs hold every connection in a transaction.
	releaseCtx, release := context.WithCancel(ctx)
	var jobs sync.WaitGroup
	for i := 0; i < 4; i++ {
		jobs.Add(1)
		go func() {
			defer jobs.Done()
			_, _ = workPool.Exec(releaseCtx, "SELECT pg_sleep(600)")
		}()
	}
	deadline := time.Now().Add(20 * time.Second)
	for workPool.Stat().AcquiredConns() < 4 {
		if time.Now().After(deadline) {
			t.Fatal("the four jobs never took the whole pool")
		}
		time.Sleep(10 * time.Millisecond)
	}

	budget := 2 * time.Second
	checkCtx, checkCancel := context.WithTimeout(ctx, budget)
	defer checkCancel()
	started := time.Now()
	err = CheckDomainAuthorization(checkCtx, workPool, roles.domain, grantSchema)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CheckDomainAuthorization on an exhausted pool = %v after %s, want a deadline error", err, elapsed)
	}
	if elapsed < budget-200*time.Millisecond {
		t.Fatalf("returned after %s, before its %s budget: it did not wait for a connection", elapsed, budget)
	}
	tx, txErr := func() (interface{ Rollback(context.Context) error }, error) {
		txCtx, txCancel := context.WithTimeout(ctx, budget)
		defer txCancel()
		return workPool.Begin(txCtx)
	}()
	if txErr == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("a transaction opened on an exhausted pool")
	}
	if !errors.Is(txErr, context.DeadlineExceeded) {
		t.Fatalf("Begin on an exhausted pool = %v, want a deadline error", txErr)
	}

	release()
	jobs.Wait()
	// After the load the same check passes again.
	recovered, recoveredCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recoveredCancel()
	if err := CheckDomainAuthorization(recovered, workPool, roles.domain, grantSchema); err != nil {
		t.Fatalf("after the load: %v", err)
	}
}
