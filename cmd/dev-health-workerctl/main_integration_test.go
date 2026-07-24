//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewJobRouteControllerWiresCelerySyncProviderQuiescence(t *testing.T) {
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
		CREATE SCHEMA river;
		CREATE TABLE river.river_job (kind text NOT NULL, state text NOT NULL);
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
		CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY, provider text NOT NULL, dataset_key text NOT NULL,
			status text NOT NULL
		);
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('sync.provider_unit', 'celery', FALSE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}
	registry, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	controller, err := newJobRouteController(pool, pool, "river", registry)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.ApplyCheckedIn(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("empty Celery unit ledger activation: %v", err)
	}
	if state.Transport != "river_canary" || state.Generation != 2 {
		t.Fatalf("activated state = %+v", state)
	}
	if _, err := controller.Rollback(ctx, "sync.provider_unit"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status)
		VALUES ('00000000-0000-4000-8000-000000000002', 'launchdarkly', 'feature-flags', 'running')`); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyCheckedIn(ctx, "sync.provider_unit"); !errors.Is(err, jobroute.ErrLiveClaims) {
		t.Fatalf("nonterminal Celery unit activation error = %v, want %v", err, jobroute.ErrLiveClaims)
	}
	state, err = controller.Inspect(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "celery" || state.Generation != 3 {
		t.Fatalf("failed activation changed state = %+v", state)
	}
}
