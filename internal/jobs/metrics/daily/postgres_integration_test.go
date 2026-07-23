//go:build integration

package daily

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
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
	registry, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	routes := dailyTestRegistry{production: registry}
	publisher, err := NewPostgresPublisher(pool, routes)
	if err != nil {
		t.Fatal(err)
	}

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
	now = now.Add(store.lease / 2)
	if err := store.RenewPartition(ctx, *first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease/2 + time.Second)
	if duplicate, err := store.ClaimPartition(ctx, partitionID); err != nil || duplicate != nil {
		t.Fatalf("healthy renewed partition was reclaimed = %#v, %v", duplicate, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'canceled' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	if blocked, err := store.ClaimPartition(ctx, partitionID); err != nil || blocked != nil {
		t.Fatalf("canceled partition claim = %#v, %v", blocked, err)
	}
	if err := store.CompletePartition(ctx, *first, publisher); err == nil {
		t.Fatal("canceled run completed an in-flight partition")
	}
	if _, err := pool.Exec(ctx, "UPDATE daily_metrics_runs SET status = 'running' WHERE id = $1::uuid", runID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease/2 + time.Second)
	if err := store.RenewPartition(ctx, *first); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed partition renewed: %v", err)
	}
	if err := store.CompletePartition(ctx, *first, publisher); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed partition completed: %v", err)
	}
	reclaimed, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || reclaimed == nil || reclaimed.Token == first.Token {
		t.Fatalf("reclaim = %#v, %v", reclaimed, err)
	}
	if err := store.CompletePartition(ctx, *first, publisher); err == nil {
		t.Fatal("stale partition token completed a reclaimed partition")
	}
	if err := store.CompletePartition(ctx, *reclaimed, failingFinalizePublisher{publisher}); err == nil {
		t.Fatal("injected crash after finalizer outbox insert unexpectedly committed")
	}
	var partitionStatus string
	var outboxCount int
	if err := pool.QueryRow(ctx, "SELECT status FROM daily_metrics_partitions WHERE id=$1", partitionID).Scan(&partitionStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM worker_job_outbox").Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if partitionStatus != "running" || outboxCount != 0 {
		t.Fatalf("crash window committed partial state: partition=%s outbox=%d", partitionStatus, outboxCount)
	}
	now = now.Add(store.lease + time.Second)
	recovered, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || recovered == nil || recovered.Token == reclaimed.Token {
		t.Fatalf("recovery claim = %#v, %v", recovered, err)
	}
	if err := store.CompletePartition(ctx, *recovered, publisher); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM worker_job_outbox
WHERE job_kind=$1 AND dedupe_key=$2`,
		jobcontract.KindDailyMetricsFinalize, "metrics.daily_finalize:"+runID,
	).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("finalizer outbox count = %d, want 1", outboxCount)
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
	now = now.Add(store.lease / 2)
	if err := store.RenewFinalize(ctx, *firstFinalize); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease/2 + time.Second)
	if duplicate, err := store.ClaimFinalize(ctx, runID); err != nil || duplicate != nil {
		t.Fatalf("healthy renewed finalizer was reclaimed = %#v, %v", duplicate, err)
	}
	now = now.Add(store.lease/2 + time.Second)
	if err := store.RenewFinalize(ctx, *firstFinalize); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed finalizer renewed: %v", err)
	}
	if err := store.CompleteFinalize(ctx, *firstFinalize); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired unreclaimed finalizer completed: %v", err)
	}
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
);
CREATE TABLE worker_job_outbox (
 id uuid PRIMARY KEY, dedupe_key varchar(256) NOT NULL UNIQUE,
 job_kind varchar(96) NOT NULL, contract_version integer NOT NULL,
 args json NOT NULL, payload_hash varchar(71) NOT NULL,
 queue varchar(96) NOT NULL, priority smallint NOT NULL,
 max_attempts smallint NOT NULL, scheduled_at timestamptz NOT NULL,
 status varchar(16) NOT NULL, attempt_count integer NOT NULL,
 next_attempt_at timestamptz NOT NULL, created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

type dailyTestRegistry struct {
	production *jobruntime.Registry
}

func (registry dailyTestRegistry) Descriptor(kind string) (jobruntime.Descriptor, bool) {
	descriptor, ok := registry.production.Descriptor(kind)
	if !ok {
		return descriptor, false
	}
	if kind == jobcontract.KindDailyMetricsPartition ||
		kind == jobcontract.KindDailyMetricsFinalize {
		descriptor.MigrationState = "go_default"
		descriptor.Route = "river"
	}
	return descriptor, true
}

type failingFinalizePublisher struct {
	delegate Publisher
}

func (publisher failingFinalizePublisher) PublishPartition(
	ctx context.Context,
	run Run,
	partition Partition,
) error {
	return publisher.delegate.PublishPartition(ctx, run, partition)
}

func (publisher failingFinalizePublisher) PublishFinalizeTx(
	ctx context.Context,
	tx pgx.Tx,
	run Run,
) error {
	if err := publisher.delegate.PublishFinalizeTx(ctx, tx, run); err != nil {
		return err
	}
	return errors.New("injected crash after outbox insert")
}
