//go:build integration

package reconcilerservice

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-6932: executed evidence for the probe path. The reconciler's domain readiness checks run
// on a dedicated readiness pool (CHAOS-6771), but queue_postgres, river_schema and
// coordinator_postgres run on the SHARED work pools (2 connections each in
// deploy/go-workers/deployment.json), the same pools the mutation pipeline, the relay and the
// repairs use. With a HEALTHY database and both work connections held by ordinary work, each check
// ran into its deadline without ever reaching the database, so a replica doing plenty of work read
// as not ready (the readyz 3-second timeouts on the reconciler at 15:59:36Z and on go-sync at
// 12:28/12:42/12:47Z are this window class).
//
// Each subtest holds every connection of the check's pool and asserts the check does NOT fail just
// because the pool is busy. RED on origin/main.
func TestReadinessChecksDoNotQueueBehindTheQueueAndCoordinatorWorkPools(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	newWorkPool := func() *pgxpool.Pool {
		config, err := pgxpool.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.MaxConns = 2 // the reconciler's queue_control and coordinator sizes
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	queue, coordinator := newWorkPool(), newWorkPool()
	database := &postgresReconcilerDatabase{
		pools:     &postgres.RuntimePools{QueueControl: queue, Coordinator: coordinator},
		queueRole: "postgres", coordinatorRole: "postgres", domainRole: "postgres", riverSchema: "river",
	}
	hold := func(pool *pgxpool.Pool) {
		t.Helper()
		for held := 0; held < int(pool.Config().MaxConns); held++ {
			connection, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(connection.Release)
		}
	}
	// Production warms these at construction, so each has answered long before the pools are busy.
	database.warmReadinessProbes()
	awaitAnswered(t, &database.queueProbe, &database.coordinatorProbe, &database.riverSchemaProbe)
	hold(queue)
	hold(coordinator)

	// The database itself is healthy: an independent connection answers at once.
	independent, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer independent.Close(ctx)
	var one int
	if err := independent.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("fixture: the database does not answer: %v", err)
	}

	for name, check := range map[string]func(context.Context) error{
		"QueueReady":       database.QueueReady,
		"RiverSchemaReady": func(ctx context.Context) error { return database.RiverSchemaReady(ctx, "river") },
		"CoordinatorReady": database.CoordinatorReady,
	} {
		t.Run(name, func(t *testing.T) {
			checkContext, cancelCheck := context.WithTimeout(ctx, 700*time.Millisecond)
			defer cancelCheck()
			started := time.Now()
			err := check(checkContext)
			// The check may legitimately fail for a reason of its own (this fixture has no roles or
			// River schema), but it must fail FAST from the database, not by waiting out its
			// deadline for a pooled connection another component is holding.
			if checkContext.Err() != nil {
				t.Fatalf("%s ran to its %s deadline (%v after %s) with a healthy database because every connection of "+
					"its work pool was held by other work: the readiness check queues behind the work pool it shares "+
					"with the pipeline, relay and repairs (CHAOS-6932)", name, 700*time.Millisecond, err, time.Since(started))
			}
		})
	}
}

// awaitAnswered waits until every probe has produced its first answer: in production the checks are
// warmed at construction and answer long before load arrives (CHAOS-6934).
func awaitAnswered(t *testing.T, probes ...*postgres.LazyProbeCheck) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for _, probe := range probes {
		for !probe.Answered() {
			if time.Now().After(deadline) {
				t.Fatal("a warmed readiness check never produced its first answer")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
