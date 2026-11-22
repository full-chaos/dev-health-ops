//go:build integration

package chunkreclaimrolecell_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const (
	roleCellRun     = "00000000-0000-4000-8000-000000014246"
	roleCellFailed  = "00000000-0000-4000-8000-000000014247"
	roleCellRunning = "00000000-0000-4000-8000-000000014248"
	roleCellChunks  = 3
)

func seedRoleCellUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string, expiresAt time.Time) {
	t.Helper()
	integrationID, sourceID := pgseed.EnsureSyncIntegration(ctx, t, pool, "org-a", "", "")
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_units (
			id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts,
			expired_lease_retry_count, lease_owner, lease_expires_at, created_at, updated_at
		) VALUES ($1, 'org-a', $2, $3::uuid, $4::uuid, 'github', 'cicd', 'standard', 'backfill', 'running', 0,
			0, 'worker-a', $5, $6, $6)`,
		unitID, roleCellRun, integrationID, sourceID, expiresAt, expiresAt.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	generation := "sync-unit:" + unitID
	at := expiresAt.Add(-time.Hour)
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_unit_chunk_checkpoints
		(org_id, sync_run_unit_id, generation, provider, dataset_key, route_version, normalized_at,
		 next_ordinal, prepared_chunks, owner, lease_expires_at, created_at, updated_at)
		VALUES ('org-a', $1::uuid, $2, 'github', 'cicd', 'v1', $3, $4, $4, 'worker-a', $3, $3, $3)`,
		unitID, generation, at, roleCellChunks); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_unit_effect_chunks
		(org_id, sync_run_unit_id, generation, route_version, ordinal, total_chunks,
		 payload, ledger, payload_bytes, manifest_digest, status, created_at, updated_at)
		SELECT 'org-a', $1::uuid, $2, 'v1', ordinal, 0, '{"rows":[]}'::jsonb, '{}'::jsonb, 12,
		       repeat('a', 64), 'committed', $3, $3
		FROM generate_series(0, $4::int - 1) AS ordinal`,
		unitID, generation, at, roleCellChunks); err != nil {
		t.Fatal(err)
	}
}

func roleCellSidecars(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string) (checkpoints, chunks int) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM public.sync_run_unit_chunk_checkpoints WHERE sync_run_unit_id = $1::uuid),
		       (SELECT count(*) FROM public.sync_run_unit_effect_chunks WHERE sync_run_unit_id = $1::uuid)`,
		unitID).Scan(&checkpoints, &chunks); err != nil {
		t.Fatal(err)
	}
	return checkpoints, chunks
}

// On the migrated schema and the real go-river-migrate grants, the domain role
// runs lease repair (which fails an expired GitHub chunked unit) and one full
// reclaim tick through the production constructor: the transaction-level tick
// lock, the candidate SELECT ... FOR UPDATE OF cp, the batch and checkpoint
// DELETEs and the backlog count. The failed unit's rows are gone, the running
// unit's rows stay, no advisory lock is left and PostgreSQL refused nothing.
func TestChunkSidecarReclaimRunsAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "chunkreclaimrole")
	now := time.Date(2026, time.November, 19, 17, 3, 0, 0, time.UTC)
	pgseed.EnsureSyncRun(ctx, t, venue.Admin, pgseed.SyncRun{ID: roleCellRun, OrgID: "org-a", Status: "running"})
	seedRoleCellUnit(t, ctx, venue.Admin, roleCellFailed, now.Add(-time.Minute))
	seedRoleCellUnit(t, ctx, venue.Admin, roleCellRunning, now.Add(time.Hour))

	repair, err := syncreconciler.NewLeaseRepair(venue.Domain)
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := repair.Step(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Failed != 1 {
		t.Fatalf("lease repair as the domain role = %+v, want exactly one failed unit", repaired)
	}

	reclaim, err := syncreconciler.NewChunkSidecarReclaim(venue.Domain)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reclaim.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("reclaim tick as the domain role: %v", err)
	}
	if result.LockSkipped || result.Units != 1 || result.Rows != roleCellChunks || result.RemainingUnits != 0 || !result.Done {
		t.Fatalf("reclaim tick as the domain role = %+v, want the failed unit (%d rows), 0 remaining, done", result, roleCellChunks)
	}
	if checkpoints, chunks := roleCellSidecars(t, ctx, venue.Admin, roleCellFailed); checkpoints != 0 || chunks != 0 {
		t.Fatalf("failed unit sidecars = %d / %d, want 0 / 0", checkpoints, chunks)
	}
	if checkpoints, chunks := roleCellSidecars(t, ctx, venue.Admin, roleCellRunning); checkpoints != 1 || chunks != roleCellChunks {
		t.Fatalf("running unit sidecars = %d / %d, want 1 / %d untouched", checkpoints, chunks, roleCellChunks)
	}
	var held int
	if err := venue.Admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted`).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != 0 {
		t.Fatalf("%d advisory locks left after the tick", held)
	}
	if denied := venue.Denials.All(); len(denied) > 0 {
		t.Fatalf("domain role refused %d statement(s): %v", len(denied), denied)
	}
}
