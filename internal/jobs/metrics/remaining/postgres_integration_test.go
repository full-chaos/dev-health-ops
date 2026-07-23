//go:build integration

package remaining

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresStoreResumesPartitionsAndFencesCancellationAndExpiry(t *testing.T) {
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
	createRemainingTables(t, ctx, pool)

	const (
		runID    = "00000000-0000-4000-8000-000000000101"
		firstID  = "00000000-0000-4000-8000-000000000102"
		secondID = "00000000-0000-4000-8000-000000000103"
		orgID    = "00000000-0000-4000-8000-000000000109"
	)
	now := time.Date(2026, 7, 23, 20, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO remaining_metric_runs
    (id,org_id,family,generation,scope_key,generation_seed,status,created_at,updated_at)
VALUES ($1,$2,'capacity','capacity-v1','all-teams',42,'pending',$3,$3)`, runID, orgID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO remaining_metric_runs
    (id,org_id,family,generation,scope_key,generation_seed,status,created_at,updated_at)
VALUES ('00000000-0000-4000-8000-000000000110',$1,'capacity','capacity-v1','all-teams',99,'pending',$2,$2)`,
		orgID, now); err == nil {
		t.Fatal("duplicate authoritative family generation was accepted")
	}
	for ordinal, partitionID := range []string{firstID, secondID} {
		if _, err := pool.Exec(ctx, `
INSERT INTO remaining_metric_partitions
    (id,run_id,ordinal,scope,status,attempt_count,created_at,updated_at)
VALUES ($1,$2,$3,'{}'::jsonb,'pending',0,$4,$4)`, partitionID, runID, ordinal, now); err != nil {
			t.Fatal(err)
		}
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now }
	run, err := store.LoadRun(ctx, runID)
	if err != nil || run.Family != "capacity" || run.Seed == nil || *run.Seed != 42 {
		t.Fatalf("run=%#v err=%v", run, err)
	}

	first, err := store.ClaimPartition(ctx, firstID)
	if err != nil || first == nil {
		t.Fatalf("first claim=%#v err=%v", first, err)
	}
	if err := store.CompletePartition(ctx, *first, "rows=1;sha256=golden"); err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingPartitions(ctx, runID)
	if err != nil || len(pending) != 1 || pending[0].ID != secondID {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	if duplicate, err := store.ClaimPartition(ctx, firstID); err != nil || duplicate != nil {
		t.Fatalf("completed partition reclaimed=%#v err=%v", duplicate, err)
	}

	second, err := store.ClaimPartition(ctx, secondID)
	if err != nil || second == nil {
		t.Fatalf("second claim=%#v err=%v", second, err)
	}
	if err := store.CancelRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewPartition(ctx, *second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("canceled claim renewed: %v", err)
	}
	if err := store.CompletePartition(ctx, *second, "rows=1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("canceled claim completed: %v", err)
	}

	// An expired owner loses authority before any replacement claims.
	if _, err := pool.Exec(ctx, "UPDATE remaining_metric_runs SET status='running' WHERE id=$1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease + time.Second)
	if err := store.RenewPartition(ctx, *second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed claim renewed: %v", err)
	}
	if err := store.CompletePartition(ctx, *second, "rows=1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed claim completed: %v", err)
	}
	reclaimed, err := store.ClaimPartition(ctx, secondID)
	if err != nil || reclaimed == nil || reclaimed.Token == second.Token {
		t.Fatalf("reclaimed=%#v err=%v", reclaimed, err)
	}
	if err := store.CompletePartition(ctx, *second, "rows=1"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner completed after reclaim: %v", err)
	}
	if err := store.CompletePartition(ctx, *reclaimed, "rows=1;sha256=golden"); err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeRun(ctx, runID); err != nil {
		t.Fatalf("idempotent finalize: %v", err)
	}
	run, err = store.LoadRun(ctx, runID)
	if err != nil || run.Status != "succeeded" {
		t.Fatalf("final run=%#v err=%v", run, err)
	}
}

func createRemainingTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE remaining_metric_runs (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, family text NOT NULL, generation text NOT NULL,
 scope_key text NOT NULL, generation_seed bigint NULL, status text NOT NULL,
 canceled_at timestamptz NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 UNIQUE(org_id,family,generation,scope_key)
);
CREATE TABLE remaining_metric_partitions (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES remaining_metric_runs(id), ordinal integer NOT NULL,
 scope jsonb NOT NULL, status text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL, output_evidence text NULL, completed_at timestamptz NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, UNIQUE(run_id,ordinal)
)`)
	if err != nil {
		t.Fatal(err)
	}
}
