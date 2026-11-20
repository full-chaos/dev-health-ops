//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	leaseRepairMarkRun  = "00000000-0000-4000-8000-000000014252"
	leaseRepairMarkUnit = "00000000-0000-4000-8000-000000014253"
)

func putLeaseRepairCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prepared, next int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_unit_chunk_checkpoints
(org_id, sync_run_unit_id, generation, provider, dataset_key, route_version, normalized_at,
 next_ordinal, prepared_chunks, owner, lease_expires_at, created_at, updated_at)
VALUES ('org-a', $1::uuid, $2, 'github', 'cicd', 'provider-chunk.v1', now(), $4, $3, 'owner', now(), now(), now())
ON CONFLICT (org_id, sync_run_unit_id, generation)
DO UPDATE SET next_ordinal = EXCLUDED.next_ordinal, prepared_chunks = EXCLUDED.prepared_chunks`,
		leaseRepairMarkUnit, "sync-unit:"+leaseRepairMarkUnit, prepared, next); err != nil {
		t.Fatal(err)
	}
}

// leaseRepairMarkClaim leases the unit as providersync's claim does and returns
// a claim the episode read and the deferral stamp accept.
func leaseRepairMarkClaim(t *testing.T, ctx context.Context, pool *pgxpool.Pool) providersync.Claim {
	t.Helper()
	owner := uuid.NewString()
	expires := time.Now().UTC().Add(time.Minute)
	var claim providersync.Claim
	if err := pool.QueryRow(ctx, `
UPDATE public.sync_run_units
SET status = 'running', available_at = NULL, attempts = attempts + 1, lease_owner = $2,
    lease_expires_at = $3, last_heartbeat_at = now(), updated_at = now()
WHERE id = $1::uuid AND status IN ('retrying', 'dispatching')
RETURNING id::text, sync_run_id::text, org_id, integration_id::text, source_id::text, attempts`,
		leaseRepairMarkUnit, owner, expires).Scan(&claim.ID, &claim.SyncRunID, &claim.OrgID, &claim.IntegrationID, &claim.SourceID, &claim.Attempt); err != nil {
		t.Fatal(err)
	}
	claim.SourceExternalID = claim.SourceID
	claim.Provider, claim.Dataset, claim.CostClass, claim.Mode = "github", "cicd", providersync.CostMedium, "incremental"
	claim.CredentialID = uuid.NewString()
	claim.Owner, claim.LeaseExpiresAt = owner, expires
	if err := claim.Validate(); err != nil {
		t.Fatalf("stand-in claim is not valid: %v", err)
	}
	return claim
}

// TestLeaseRepairRetryKeepsTheRateLimitProgressMark (CHAOS-14252): the
// expired-lease RETRYING stamp rebuilds result, so it must carry the worker's
// rate_limit_progress_mark; the next deferral after real checkpoint progress
// then restarts the rate-limit episode.
func TestLeaseRepairRetryKeepsTheRateLimitProgressMark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createLeaseRepairIntegrationFixture(ctx, t, pool); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	seedLeaseRepairRun(t, ctx, pool, leaseRepairMarkRun, "org-a", "running")
	// Lease repair retries only Linear backfill work-item units
	// (decideExpiredLeaseRepair); the stamp is the same for any unit, so the
	// cell seeds the mark on one directly.
	seedLeaseRepairUnit(t, ctx, pool, leaseRepairMarkUnit, leaseRepairMarkRun, "org-a", "linear", "backfill", "work_items", 0, now.Add(-time.Minute))
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_run_units
SET result = '{"error_category":"provider_rate_limited","rate_limit_progress_mark":{"next_ordinal":4,"prepared_chunks":4}}'::json
WHERE id = $1`, leaseRepairMarkUnit); err != nil {
		t.Fatal(err)
	}
	putLeaseRepairCheckpoint(t, ctx, pool, 4, 4)

	repair, err := NewLeaseRepair(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repair.Step(ctx, now, 10); err != nil {
		t.Fatal(err)
	}
	var status string
	var mark *string
	var deferrals int
	if err := pool.QueryRow(ctx, `
SELECT status, (result::jsonb -> 'rate_limit_progress_mark')::text, rate_limit_deferrals
FROM public.sync_run_units WHERE id = $1`, leaseRepairMarkUnit).Scan(&status, &mark, &deferrals); err != nil {
		t.Fatal(err)
	}
	if status != "retrying" || mark == nil || *mark != `{"next_ordinal": 4, "prepared_chunks": 4}` || deferrals != 0 {
		t.Fatalf("after lease repair: status=%q mark=%v deferrals=%d, want retrying, the mark kept, a reset pair", status, mark, deferrals)
	}
	var category string
	if err := pool.QueryRow(ctx, `SELECT result::jsonb ->> 'error_category' FROM public.sync_run_units WHERE id = $1`, leaseRepairMarkUnit).Scan(&category); err != nil || category != "worker_lost" {
		t.Fatalf("error_category=%q err=%v, want worker_lost (the carry must not keep the old category)", category, err)
	}

	repository, err := providersync.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	claim := leaseRepairMarkClaim(t, ctx, pool)
	putLeaseRepairCheckpoint(t, ctx, pool, 6, 6)
	episode, err := repository.RateLimitEpisode(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if !episode.Progressed() {
		t.Fatalf("checkpoint progress after lease repair did not read as progress: %+v mark=%v", episode, episode.Mark)
	}
	deferredAt := time.Now().UTC()
	if err := repository.DeferForRateLimit(ctx, claim, deferredAt.Add(time.Hour), deferredAt, providersync.RateLimitObservation{},
		providersync.RateLimitEpisodeStep{Restart: true, Mark: episode.Checkpoint}); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT (result::jsonb -> 'rate_limit_progress_mark')::text, rate_limit_deferrals
FROM public.sync_run_units WHERE id = $1`, leaseRepairMarkUnit).Scan(&mark, &deferrals); err != nil {
		t.Fatal(err)
	}
	if deferrals != 1 || mark == nil || *mark != `{"next_ordinal": 6, "prepared_chunks": 6}` {
		t.Fatalf("restart after lease repair stored deferrals=%d mark=%v", deferrals, mark)
	}
}
