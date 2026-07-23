//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresStoreRecoversPartitionClaimAndFinalizesExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createDailyTables(t, ctx, pool)

	const (
		runID       = "00000000-0000-4000-8000-000000000001"
		partitionID = "00000000-0000-4000-8000-000000000002"
		orgID       = "00000000-0000-4000-8000-000000000009"
	)
	now := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at) VALUES ($1,$2,'2026-07-23','daily-v1','pending','pending',$3,$3)`, runID, orgID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_partitions (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at) VALUES ($1,$2,0,'[]'::jsonb,'pending',0,$3,$3)`, partitionID, runID, now); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	if dispatched, err := store.ClaimDispatch(ctx, runID); err != nil || dispatched == nil || dispatched.Status != "running" {
		t.Fatalf("dispatch claim = %#v, %v", dispatched, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'canceled' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	if dispatched, err := store.ClaimDispatch(ctx, runID); err != nil || dispatched != nil {
		t.Fatalf("canceled dispatch = %#v, %v", dispatched, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'running' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}

	// Kill after claim: an unexpired lease suppresses duplicate execution;
	// advancing the durable clock makes the same partition reclaimable.
	first, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || first == nil {
		t.Fatalf("first claim = %#v, %v", first, err)
	}
	if duplicate, err := store.ClaimPartition(ctx, partitionID); err != nil || duplicate != nil {
		t.Fatalf("unexpired duplicate = %#v, %v", duplicate, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'canceled' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	if blocked, err := store.ClaimPartition(ctx, partitionID); err != nil || blocked != nil {
		t.Fatalf("canceled partition claim = %#v, %v", blocked, err)
	}
	if err := store.CompletePartition(ctx, *first); err == nil {
		t.Fatal("canceled run completed an in-flight partition")
	}
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'running' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease + time.Second)
	reclaimed, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || reclaimed == nil || reclaimed.Token == first.Token {
		t.Fatalf("reclaim = %#v, %v", reclaimed, err)
	}
	if err := store.CompletePartition(ctx, *first); err == nil {
		t.Fatal("stale partition token completed a reclaimed partition")
	}
	if err := store.CompletePartition(ctx, *reclaimed); err != nil {
		t.Fatal(err)
	}

	// Kill between all partition writes and finalize: finalization only claims
	// after the durable success state is visible, and its token fences a stale
	// claimant after lease recovery.
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'canceled' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	if blocked, err := store.ClaimFinalize(ctx, runID); err != nil || blocked != nil {
		t.Fatalf("canceled finalizer claim = %#v, %v", blocked, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'running' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	firstFinalize, err := store.ClaimFinalize(ctx, runID)
	if err != nil || firstFinalize == nil {
		t.Fatalf("first finalize = %#v, %v", firstFinalize, err)
	}
	now = now.Add(store.lease + time.Second)
	reclaimedFinalize, err := store.ClaimFinalize(ctx, runID)
	if err != nil || reclaimedFinalize == nil || reclaimedFinalize.Token == firstFinalize.Token {
		t.Fatalf("reclaimed finalize = %#v, %v", reclaimedFinalize, err)
	}
	if err := store.CompleteFinalize(ctx, *firstFinalize); err == nil {
		t.Fatal("stale finalizer completed a reclaimed run")
	}
	if err := store.CompleteFinalize(ctx, *reclaimedFinalize); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := store.ClaimFinalize(ctx, runID); err != nil || duplicate != nil {
		t.Fatalf("completed finalizer was reclaimed = %#v, %v", duplicate, err)
	}
}

func createDailyTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE daily_metrics_runs (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, target_day date NOT NULL, generation text NOT NULL,
 status text NOT NULL, finalization_status text NOT NULL, finalization_claim_token uuid NULL,
 finalization_lease_expires_at timestamptz NULL, finalized_at timestamptz NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE TABLE daily_metrics_partitions (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES daily_metrics_runs(id), ordinal integer NOT NULL,
 repo_ids jsonb NOT NULL, status text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL, completed_at timestamptz NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}
