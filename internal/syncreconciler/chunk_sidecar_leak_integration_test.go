//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	chunkLeakRun       = "00000000-0000-4000-8000-000000014243"
	chunkLeakFailed    = "00000000-0000-4000-8000-000000014244"
	chunkLeakRunning   = "00000000-0000-4000-8000-000000014245"
	chunkLeakChunksPer = 3
)

func seedChunkSidecars(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, unitID string, chunks int, at time.Time) {
	t.Helper()
	generation := "sync-unit:" + unitID
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_unit_chunk_checkpoints
		(org_id, sync_run_unit_id, generation, provider, dataset_key, route_version, normalized_at,
		 next_ordinal, prepared_chunks, owner, lease_expires_at, created_at, updated_at)
		VALUES ($1, $2::uuid, $3, 'github', 'cicd', 'v1', $4, $5, $5, 'worker-a', $4, $4, $4)`,
		orgID, unitID, generation, at, chunks); err != nil {
		t.Fatal(err)
	}
	for ordinal := 0; ordinal < chunks; ordinal++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO public.sync_run_unit_effect_chunks
			(org_id, sync_run_unit_id, generation, route_version, ordinal, total_chunks,
			 payload, ledger, payload_bytes, manifest_digest, status, created_at, updated_at)
			VALUES ($1, $2::uuid, $3, 'v1', $4, 0, '{"rows":[]}'::jsonb, '{}'::jsonb, 12,
			        repeat('a', 64), 'committed', $5, $5)`,
			orgID, unitID, generation, ordinal, at); err != nil {
			t.Fatal(err)
		}
	}
}

func countChunkSidecars(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) (checkpoints, chunks int) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id = $1::uuid),
		       (SELECT count(*) FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id = $1::uuid)`,
		unitID).Scan(&checkpoints, &chunks); err != nil {
		t.Fatal(err)
	}
	return checkpoints, chunks
}

// CHAOS-14243 red cell: lease repair terminalizes a chunked unit as failed
// (worker_lost) and nothing reclaims its stored chunk rows.
func TestChunkSidecarsOfLeaseRepairFailedUnitAreReclaimed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createLeaseRepairIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.November, 19, 17, 3, 0, 0, time.UTC)
	resetLeaseRepairTables(t, ctx, pool)
	seedLeaseRepairRun(t, ctx, pool, chunkLeakRun, "org-a", "running")
	seedLeaseRepairUnit(t, ctx, pool, chunkLeakFailed, chunkLeakRun, "org-a", "github", "backfill", "cicd", 0, now.Add(-time.Minute))
	seedLeaseRepairUnit(t, ctx, pool, chunkLeakRunning, chunkLeakRun, "org-a", "github", "backfill", "cicd", 0, now.Add(time.Hour))
	seedChunkSidecars(t, ctx, pool, "org-a", chunkLeakFailed, chunkLeakChunksPer, now.Add(-time.Hour))
	seedChunkSidecars(t, ctx, pool, "org-a", chunkLeakRunning, chunkLeakChunksPer, now.Add(-time.Hour))

	repair, err := NewLeaseRepair(pool)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repair.Step(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 1 {
		t.Fatalf("lease repair result = %+v, want exactly one failed unit", result)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM public.sync_run_units WHERE id = $1::uuid`, chunkLeakFailed).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("unit status = %q, want failed", status)
	}

	// The reclaim writer under test runs here once it exists; on main there is none.

	if checkpoints, chunks := countChunkSidecars(t, ctx, pool, chunkLeakRunning); checkpoints != 1 || chunks != chunkLeakChunksPer {
		t.Fatalf("running unit sidecars = %d checkpoints / %d chunks, want 1 / %d untouched", checkpoints, chunks, chunkLeakChunksPer)
	}
	if checkpoints, chunks := countChunkSidecars(t, ctx, pool, chunkLeakFailed); checkpoints != 0 || chunks != 0 {
		t.Fatalf("failed unit sidecars = %d checkpoints / %d chunks, want 0 / 0 (terminal unit's chunk rows were never reclaimed)", checkpoints, chunks)
	}
}
