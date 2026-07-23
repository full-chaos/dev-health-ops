//go:build integration

package jobroute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

type integrationRegistry struct{ descriptor jobruntime.Descriptor }

func (registry integrationRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	return registry.descriptor, kind == registry.descriptor.Kind
}

func (registry integrationRegistry) Descriptors() []jobruntime.Descriptor {
	return []jobruntime.Descriptor{registry.descriptor}
}

type idleQuiescer struct{}

func (idleQuiescer) Quiesce(context.Context, string) error { return nil }

func TestRollbackWaitsForProducerRouteLockThenRejectsStagedOutbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		);
		CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		);
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('job.test', 'river_canary', FALSE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(pool, integrationRegistry{jobruntime.Descriptor{
		Kind: "job.test", Route: "river_canary", RollbackRoute: "celery",
	}}, idleQuiescer{})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = producer.Rollback(ctx) }()
	var transport string
	if err := producer.QueryRow(ctx, `
		SELECT transport FROM public.worker_job_routes
		WHERE job_kind = 'job.test' FOR SHARE`).Scan(&transport); err != nil {
		t.Fatal(err)
	}
	if transport != "river_canary" {
		t.Fatalf("producer observed %q", transport)
	}

	result := make(chan error, 1)
	go func() {
		_, rollbackErr := controller.Rollback(ctx, "job.test")
		result <- rollbackErr
	}()
	waitForBlockedRouteUpdate(t, ctx, pool)
	if _, err := producer.Exec(ctx, `
		INSERT INTO public.worker_job_outbox (id, job_kind, status)
		VALUES ('00000000-0000-4000-8000-000000000001', 'job.test', 'pending')`); err != nil {
		t.Fatal(err)
	}
	if err := producer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrPendingOutbox) {
		t.Fatalf("Rollback() error = %v", err)
	}
	state, err := controller.Inspect(ctx, "job.test")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "river_canary" || state.Generation != 1 {
		t.Fatalf("route changed despite staged work: %+v", state)
	}
}

func waitForBlockedRouteUpdate(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND query LIKE '%worker_job_routes WHERE job_kind = $1%FOR UPDATE%'`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("rollback never blocked on producer route lock")
}
