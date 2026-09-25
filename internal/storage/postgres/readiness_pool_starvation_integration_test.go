//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// CHAOS-6771. A worker's domain pool is small (4 connections by default) and
// serves job claims, dispatch and outbox work. While claimed jobs hold every
// connection, a readiness check that needs a connection from that pool cannot
// even start its query and runs into its deadline: the "context deadline
// exceeded" go-sync logged for domain_postgres, posture_manifest_lockstep and
// idempotency_backend while its backlog drained (53 domain-pool acquire
// timeouts of 10-60 s, prod-ops read of worker_database_pool_acquire_seconds).
//
// This drives the production wiring (NewRuntimePools) and shows both halves:
// the check on the WORK pool (what readiness did) times out; the same check on
// the readiness pool (what it does now) answers while the work pool is
// exhausted.
func TestReadinessPoolAnswersWhileTheWorkPoolIsExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri, roles := startGrantHarness(t, ctx)
	roleURI := func(role, password string) string {
		parsed, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		parsed.User = url.UserPassword(role, password)
		return parsed.String()
	}
	pools, err := NewRuntimePools(ctx, RuntimeConfig{
		DomainURI:        roleURI(roles.domain, grantDomainPass),
		QueueControlURI:  roleURI(roles.queue, grantQueuePass),
		DomainRole:       roles.domain,
		QueueRole:        roles.queue,
		RiverSchema:      grantSchema,
		QueueControlMode: config.QueueControlDirect,
		DomainMaxConns:   4, // the production default (config.go defaultDomainMaxConns)
		QueueMaxConns:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pools.Close)
	if pools.DomainProbe == nil || pools.DomainProbe.Config().MaxConns != 1 {
		t.Fatalf("NewRuntimePools did not open a one-connection readiness pool: %+v", pools.DomainProbe)
	}
	if pools.ReadinessPool() != pools.DomainProbe {
		t.Fatal("ReadinessPool is not the probe pool")
	}

	// Idle: both pools pass, so a failure below is the load, not the grants.
	if err := CheckDomainAuthorization(ctx, pools.Domain, roles.domain, grantSchema); err != nil {
		t.Fatalf("idle work pool: %v", err)
	}
	if err := CheckDomainAuthorization(ctx, pools.ReadinessPool(), roles.domain, grantSchema); err != nil {
		t.Fatalf("idle readiness pool: %v", err)
	}

	// Four claimed jobs hold every work-pool connection.
	releaseCtx, release := context.WithCancel(ctx)
	var jobs sync.WaitGroup
	for i := 0; i < 4; i++ {
		jobs.Add(1)
		go func() {
			defer jobs.Done()
			_, _ = pools.Domain.Exec(releaseCtx, "SELECT pg_sleep(600)")
		}()
	}
	t.Cleanup(func() { release(); jobs.Wait() })
	deadline := time.Now().Add(20 * time.Second)
	for pools.Domain.Stat().AcquiredConns() < 4 {
		if time.Now().After(deadline) {
			t.Fatal("the four jobs never took the whole work pool")
		}
		time.Sleep(10 * time.Millisecond)
	}

	budget := 2 * time.Second
	// What readiness did: the check queues for a work-pool connection and times out.
	workCtx, workCancel := context.WithTimeout(ctx, budget)
	defer workCancel()
	started := time.Now()
	workErr := CheckDomainAuthorization(workCtx, pools.Domain, roles.domain, grantSchema)
	if !errors.Is(workErr, context.DeadlineExceeded) {
		t.Fatalf("check on the exhausted work pool = %v after %s, want a deadline error", workErr, time.Since(started))
	}
	beginOnWorkPool := func() error {
		txCtx, txCancel := context.WithTimeout(ctx, budget)
		defer txCancel()
		tx, err := pools.Domain.Begin(txCtx)
		if err == nil {
			_ = tx.Rollback(ctx)
		}
		return err
	}
	if err := beginOnWorkPool(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Begin on the exhausted work pool = %v, want a deadline error", err)
	}

	// What it does now: the readiness pool answers inside the same budget.
	readyCtx, readyCancel := context.WithTimeout(ctx, budget)
	defer readyCancel()
	started = time.Now()
	if err := CheckDomainAuthorization(readyCtx, pools.ReadinessPool(), roles.domain, grantSchema); err != nil {
		t.Fatalf("check on the readiness pool while the work pool is exhausted = %v after %s", err, time.Since(started))
	}
	cached := NewCachedPostureCheck(pools.ReadinessPool(), roles.domain, grantSchema, DomainPosture(), PostureCheckOptions{})
	cachedCtx, cachedCancel := context.WithTimeout(ctx, budget)
	defer cachedCancel()
	if err := cached.Check(cachedCtx); err != nil {
		t.Fatalf("cached posture check on the readiness pool while the work pool is exhausted = %v", err)
	}
	// The work pool's own numbers are untouched by the readiness checks.
	if acquired := pools.Domain.Stat().AcquiredConns(); acquired != 4 {
		t.Fatalf("work pool acquired = %d, want the four jobs only", acquired)
	}
}
