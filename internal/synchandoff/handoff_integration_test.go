//go:build integration

package synchandoff

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// startHandoffDatabase builds the real schema (the pgmigrate baseline and
// chain) and seeds one org, integration and sync configuration.
func startHandoffDatabase(ctx context.Context, t *testing.T, maxConns int32) (*pgxpool.Pool, *Config) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
	var integrationID, configID string
	now := time.Now().UTC()
	if err := pool.QueryRow(ctx, `
INSERT INTO public.integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES (gen_random_uuid(), 'org-1', 'github', 'handoff', '{}', true, $1, $1) RETURNING id::text`, now).Scan(&integrationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO public.sync_configurations (id, org_id, name, provider, integration_id, sync_targets, sync_options, is_active,
	planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), 'org-1', 'handoff', 'github', $1, '[]', '{}', true, true, $2, $2) RETURNING id::text`,
		integrationID, now).Scan(&configID); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	loaded, err := LoadConfig(ctx, tx, configID)
	if err != nil || loaded == nil {
		t.Fatalf("load the config: %v %v", loaded, err)
	}
	return pool, loaded
}

// TestEnsureScheduledJobConcurrentFirstMintsShareOneJob races several first
// mints for one never-scheduled config (CHAOS-6699, moved here with the one
// marker-job implementation by CHAOS-6695). The insert's targetless ON
// CONFLICT DO NOTHING plus the read-back must give every caller the same job
// id and leave exactly one scheduled_jobs row -- no 23505 on either unique
// key, no second job.
func TestEnsureScheduledJobConcurrentFirstMintsShareOneJob(t *testing.T) {
	const callers = 12
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, config := startHandoffDatabase(ctx, t, callers+4)

	start := make(chan struct{})
	ids := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			errs[index] = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				var err error
				ids[index], _, err = ensureScheduledJob(ctx, tx, config, time.Now().UTC())
				return err
			})
		}(index)
	}
	// Hold a SHARE lock so every caller passes its initial SELECT (ACCESS
	// SHARE does not conflict) and then blocks on its INSERT (ROW EXCLUSIVE
	// does). Releasing the lock only once all of them are waiting forces the
	// insert race instead of hoping the scheduler produces it.
	gate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Exec(ctx, "LOCK TABLE public.scheduled_jobs IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	close(start)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%INSERT INTO public.scheduled_jobs%'`,
		).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting == callers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d callers reached the INSERT", waiting, callers)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	for index := 0; index < callers; index++ {
		if errs[index] != nil {
			t.Fatalf("caller %d: %v", index, errs[index])
		}
		if ids[index] == "" || ids[index] != ids[0] {
			t.Fatalf("caller %d got job %q, caller 0 got %q", index, ids[index], ids[0])
		}
	}
	if got := countJobs(ctx, t, pool); got != 1 {
		t.Fatalf("scheduled_jobs rows = %d, want 1", got)
	}
}

func countJobs(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.scheduled_jobs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
