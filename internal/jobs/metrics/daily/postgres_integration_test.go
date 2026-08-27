//go:build integration

package daily

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresStoreStartRunTxReplaysWholeGenerationAtomically(t *testing.T) {
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
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	request := StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000009",
		TargetDay:      time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC),
		Generation:     "post-sync:00000000-0000-4000-8000-000000000001",
		RepositoryIDs:  []RepositoryID{"00000000-0000-4000-8000-000000000002"},
	}
	var first Run
	for attempt := 0; attempt < 2; attempt++ {
		tx, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		run, startErr := store.StartRunTx(ctx, tx, request, publisher)
		if startErr != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(startErr)
		}
		if attempt == 0 {
			first = run
		} else if run.ID != first.ID {
			t.Fatalf("duplicate run id=%s want=%s", run.ID, first.ID)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			t.Fatal(commitErr)
		}
	}
	var runs, partitions, handoffs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions`).Scan(&partitions); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_dispatch'`).Scan(&handoffs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || partitions != 1 || handoffs != 1 {
		t.Fatalf("runs=%d partitions=%d handoffs=%d", runs, partitions, handoffs)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request.RepositoryIDs = []RepositoryID{"00000000-0000-4000-8000-000000000003"}
	if _, err := store.StartRunTx(ctx, tx, request, publisher); !errors.Is(err, ErrInvalidState) {
		_ = tx.Rollback(ctx)
		t.Fatalf("mutated duplicate err=%v", err)
	}
	_ = tx.Rollback(ctx)

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request.Generation = "post-sync:00000000-0000-4000-8000-000000000005"
	if _, err := store.StartRunTx(ctx, tx, request, failingRunPublisher{}); !errors.Is(err, ErrUnavailable) {
		_ = tx.Rollback(ctx)
		t.Fatalf("injected handoff failure err=%v", err)
	}
	_ = tx.Rollback(ctx)
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("failed transaction leaked run, count=%d", runs)
	}
}

func TestPostgresStoreScheduledFanoutMaterializesOnceAndRecordsNoRepositories(t *testing.T) {
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
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	request := ScheduledFanoutRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000009",
		TargetDay:      time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC),
		Generation:     "fixed-schedule:daily_metrics_fanout:2026-08-12T01:00:00Z",
	}
	for attempt := 0; attempt < 2; attempt++ {
		tx, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if _, startErr := store.StartScheduledFanoutRunTx(ctx, tx, request, publisher); startErr != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(startErr)
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			t.Fatal(commitErr)
		}
	}
	var runID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM daily_metrics_runs`).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	run, err := store.ClaimDispatch(ctx, runID)
	if err != nil || run == nil || !run.RepositoryDiscoveryRequired {
		t.Fatalf("scheduled dispatch claim=%#v err=%v", run, err)
	}
	created, err := store.MaterializeScheduledFanout(ctx, *run, []RepositoryID{
		"00000000-0000-4000-8000-000000000002",
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
	})
	if err != nil || !created {
		t.Fatalf("materialize=%t err=%v", created, err)
	}
	if duplicate, err := store.MaterializeScheduledFanout(ctx, *run, []RepositoryID{"00000000-0000-4000-8000-000000000003"}); err != nil || duplicate {
		t.Fatalf("replay materialize=%t err=%v", duplicate, err)
	}
	var partitions, handoffs int
	var ids string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions`).Scan(&partitions); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT repo_ids::text FROM daily_metrics_partitions`).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = 'metrics.daily_dispatch'`).Scan(&handoffs); err != nil {
		t.Fatal(err)
	}
	if partitions != 1 || handoffs != 1 || ids != `["00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"]` {
		t.Fatalf("partitions=%d handoffs=%d ids=%s", partitions, handoffs, ids)
	}

	request.Generation = "fixed-schedule:daily_metrics_fanout:2026-08-12T01:01:00Z"
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	emptyRun, err := store.StartScheduledFanoutRunTx(ctx, tx, request, publisher)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	emptyRunClaim, err := store.ClaimDispatch(ctx, emptyRun.ID)
	if err != nil || emptyRunClaim == nil || !emptyRunClaim.RepositoryDiscoveryRequired {
		t.Fatalf("empty dispatch claim=%#v err=%v", emptyRunClaim, err)
	}
	if created, err := store.MaterializeScheduledFanout(ctx, *emptyRunClaim, nil); err != nil || !created {
		t.Fatalf("empty materialize=%t err=%v", created, err)
	}
	var status, finalization string
	var emptyPartitions, fences int
	if err := pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id=$1::uuid`, emptyRun.ID).Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id=$1::uuid`, emptyRun.ID).Scan(&emptyPartitions); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_completion_fences WHERE completion_key=$1`, "daily_metrics_run:"+emptyRun.ID).Scan(&fences); err != nil {
		t.Fatal(err)
	}
	if status != "no_repositories" || finalization != "succeeded" || emptyPartitions != 0 || fences != 1 {
		t.Fatalf("empty status=%s finalization=%s partitions=%d fences=%d", status, finalization, emptyPartitions, fences)
	}
}

// TestMaterializeScheduledFanoutFailsLoudlyWhenRepositoryCapIsExceeded pins
// the codex adversarial-review finding (round 3, CHAOS-4263): an explicit
// StartRunRequest has always rejected more than maxDailyMetricsRepositoriesPerRun
// repositories (normalizeStartRunRequest), but live ClickHouse discovery
// (MaterializeScheduledFanout, shared by the scheduled fixed-fanout and this
// PR's deferred post-sync discovery) had no equivalent cap -- an unusually
// large tenant's discovered repository set would be silently chunked into an
// unbounded number of daily_partition jobs. This must fail loud (a durable,
// Permanent, alertable error) instead of silently truncating or bursting.
func TestMaterializeScheduledFanoutFailsLoudlyWhenRepositoryCapIsExceeded(t *testing.T) {
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
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	request := ScheduledFanoutRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000009",
		TargetDay:      time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC),
		Generation:     "fixed-schedule:daily_metrics_fanout:2026-08-12T02:00:00Z",
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartScheduledFanoutRunTx(ctx, tx, request, publisher)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDispatch(ctx, run.ID)
	if err != nil || claimed == nil || !claimed.RepositoryDiscoveryRequired {
		t.Fatalf("claim=%#v err=%v", claimed, err)
	}
	tooMany := make([]RepositoryID, maxDailyMetricsRepositoriesPerRun+1)
	for index := range tooMany {
		tooMany[index] = RepositoryID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("repository-cap-test-%d", index))).String())
	}
	created, err := store.MaterializeScheduledFanout(ctx, *claimed, tooMany)
	if created || !errors.Is(err, ErrRepositoryCapExceeded) || !errors.Is(err, ErrInvalidState) {
		t.Fatalf("materialize=%t err=%v, want created=false err=ErrRepositoryCapExceeded (wrapping ErrInvalidState)", created, err)
	}
	var partitions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id=$1::uuid`, run.ID).Scan(&partitions); err != nil {
		t.Fatal(err)
	}
	if partitions != 0 {
		t.Fatalf("partitions=%d, want 0 -- an over-cap discovery must not silently truncate and partition anyway", partitions)
	}
	var status, finalization string
	if err := pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id=$1::uuid`, run.ID).Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || finalization != "failed" {
		t.Fatalf("run status=%s finalization=%s, want failed/failed -- an over-cap run must reach a terminal state, not strand in running forever", status, finalization)
	}
	var fences int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_completion_fences WHERE completion_key=$1`, "daily_metrics_run:"+run.ID).Scan(&fences); err != nil {
		t.Fatal(err)
	}
	if fences != 1 {
		t.Fatalf("completion fences=%d, want 1 -- downstream fanout steps gated on this key must not be stranded too", fences)
	}
}

// TestPostgresStorePostSyncRunDefersToLiveDiscoveryAndSurvivesRetryAfterMaterialization
// pins the CHAOS-4263 fix at the store layer: a post-sync StartRunTx request
// no longer embeds sync_run_units.source_id (the dead Postgres
// integration_sources id space) as this run's partitions. Instead it leaves
// the run with zero partitions, so ClaimDispatch reports
// RepositoryDiscoveryRequired and the heavy worker resolves live ClickHouse
// repository identity through MaterializeScheduledFanout -- the exact same
// path the nightly fixed schedule already used correctly. It also proves a
// second post-sync StartRunTx call for the same sync (at-least-once outbox
// redelivery) after that materialization has already happened does not error
// or disturb the materialized partitions.
func TestPostgresStorePostSyncRunDefersToLiveDiscoveryAndSurvivesRetryAfterMaterialization(t *testing.T) {
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
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	request := StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000009",
		TargetDay:      time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		Generation:     "post-sync:00000000-0000-4000-8000-000000000010",
		// RepositoryIDs is deliberately empty: the post-sync writer no longer
		// derives it from sync_run_units.source_id (CHAOS-4263 root cause).
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRunTx(ctx, tx, request, publisher)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var partitionsBeforeDiscovery int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, run.ID).Scan(&partitionsBeforeDiscovery); err != nil {
		t.Fatal(err)
	}
	if partitionsBeforeDiscovery != 0 {
		t.Fatalf("post-sync run created %d partitions before discovery ran; want 0 (discovery deferred, CHAOS-4263)", partitionsBeforeDiscovery)
	}

	claimed, err := store.ClaimDispatch(ctx, run.ID)
	if err != nil || claimed == nil || !claimed.RepositoryDiscoveryRequired {
		t.Fatalf("post-sync dispatch claim=%#v err=%v, want RepositoryDiscoveryRequired=true", claimed, err)
	}

	// The heavy worker resolves this against ClickHouse repos.id in
	// production (daily.RepositoryDiscoverer); this store-layer test supplies
	// the discovered set directly, since MaterializeScheduledFanout is where
	// the CHAOS-4263 fix's store-side contract lives.
	discovered := []RepositoryID{
		"00000000-0000-4000-8000-000000000002",
		"00000000-0000-4000-8000-000000000001",
	}
	created, err := store.MaterializeScheduledFanout(ctx, *claimed, discovered)
	if err != nil || !created {
		t.Fatalf("post-sync materialize=%t err=%v", created, err)
	}
	var partitionCount int
	var ids string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, run.ID).Scan(&partitionCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT repo_ids::text FROM daily_metrics_partitions WHERE run_id = $1::uuid`, run.ID).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	if partitionCount != 1 || ids != `["00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"]` {
		t.Fatalf("post-sync materialized partitions=%d ids=%s", partitionCount, ids)
	}

	// At-least-once post-sync redelivery for the same sync, after discovery
	// has already materialized real partitions: must not error, must not
	// duplicate the run, and must not disturb the materialized partitions.
	retryTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := store.StartRunTx(ctx, retryTx, request, publisher)
	if err != nil {
		_ = retryTx.Rollback(ctx)
		t.Fatalf("post-sync retry after materialization: %v", err)
	}
	if err := retryTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if retried.ID != run.ID {
		t.Fatalf("retry run id=%s want=%s", retried.ID, run.ID)
	}
	var runs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("retry duplicated the run, count=%d", runs)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid`, run.ID).Scan(&partitionCount); err != nil {
		t.Fatal(err)
	}
	if partitionCount != 1 {
		t.Fatalf("retry disturbed materialized partitions, count=%d", partitionCount)
	}
}

// TestPostgresStorePostSyncRunWithZeroDiscoveredRepositoriesRecordsNoRepositories
// pins the second CHAOS-4263 gap for the post-sync trigger specifically: a
// post-sync run whose live discovery genuinely finds zero repositories must
// terminalize as no_repositories with its completion fence, exactly like the
// scheduled fan-out's existing empty-snapshot handling, never as a silent
// zero-row "succeeded" partition.
func TestPostgresStorePostSyncRunWithZeroDiscoveredRepositoriesRecordsNoRepositories(t *testing.T) {
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
	publisher, err := NewPostgresPublisher(pool, dailyTestRegistry{production: registry})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	request := StartRunRequest{
		OrganizationID: "00000000-0000-4000-8000-000000000009",
		TargetDay:      time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC),
		Generation:     "post-sync:00000000-0000-4000-8000-000000000011",
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.StartRunTx(ctx, tx, request, publisher)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDispatch(ctx, run.ID)
	if err != nil || claimed == nil || !claimed.RepositoryDiscoveryRequired {
		t.Fatalf("post-sync dispatch claim=%#v err=%v", claimed, err)
	}
	if created, err := store.MaterializeScheduledFanout(ctx, *claimed, nil); err != nil || !created {
		t.Fatalf("post-sync empty materialize=%t err=%v", created, err)
	}

	var status, finalization string
	var partitionCount, fences int
	if err := pool.QueryRow(ctx, `SELECT status, finalization_status FROM daily_metrics_runs WHERE id=$1::uuid`, run.ID).Scan(&status, &finalization); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM daily_metrics_partitions WHERE run_id=$1::uuid`, run.ID).Scan(&partitionCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_completion_fences WHERE completion_key=$1`, "daily_metrics_run:"+run.ID).Scan(&fences); err != nil {
		t.Fatal(err)
	}
	if status != "no_repositories" || finalization != "succeeded" || partitionCount != 0 || fences != 1 {
		t.Fatalf("post-sync empty status=%s finalization=%s partitions=%d fences=%d", status, finalization, partitionCount, fences)
	}
}

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
	// A live lease is reported, not swallowed. Reporting "nothing to claim" here
	// is what let a retry retire the only worker that could reclaim the lease.
	duplicate, duplicateErr := store.ClaimPartition(ctx, partitionID)
	assertLeaseHeld(t, "unexpired duplicate", duplicate, duplicateErr, store.lease)
	now = now.Add(store.lease / 2)
	if err := store.RenewPartition(ctx, *first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.lease/2 + time.Second)
	renewedPartition, renewedPartitionErr := store.ClaimPartition(ctx, partitionID)
	assertLeaseHeld(
		t, "healthy renewed partition", renewedPartition, renewedPartitionErr, store.lease/2-time.Second,
	)
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
	crashCtx, cancelCrash := context.WithCancel(ctx)
	if err := store.CompletePartition(
		crashCtx,
		*reclaimed,
		failingFinalizePublisher{delegate: publisher, cancel: cancelCrash},
	); err == nil {
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
	renewedFinalize, renewedFinalizeErr := store.ClaimFinalize(ctx, runID)
	assertLeaseHeld(
		t, "healthy renewed finalizer", renewedFinalize, renewedFinalizeErr, store.lease/2-time.Second,
	)
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
	var completionFences int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM worker_job_completion_fences
WHERE completion_key = $1`, "daily_metrics_run:"+runID).Scan(&completionFences); err != nil {
		t.Fatal(err)
	}
	if completionFences != 1 {
		t.Fatalf("completion fences=%d want=1", completionFences)
	}
	if duplicate, err := store.ClaimFinalize(ctx, runID); err != nil || duplicate != nil {
		t.Fatalf("completed finalizer was reclaimed = %#v, %v", duplicate, err)
	}
}

// TestPostgresStoreReleaseFailurePathsReachLiveSchema is the CHAOS-4043
// regression control. transitionFinalize (and its transitionPartition
// sibling) assigned an untyped parameter to a varchar(16) column while also
// comparing that same parameter against a text literal inside a CASE
// expression; Postgres rejects that at PARSE time against the real
// alembic-derived schema ("inconsistent types deduced for parameter $1:
// text versus character varying"), so ReleaseFinalize/ReleasePartition's
// failure paths could never reach a live database in production -- every
// failed release fell through to lease expiry instead. Prior coverage never
// caught this because createDailyTables hand-wrote those columns as `text`,
// which is not what production has, so the mismatched types never collided.
// Checked out against the pre-fix source with the hand-authored `text` DDL
// restored, this test fails with that exact parse error; it is green here
// because both the statements and the DDL were corrected.
func TestPostgresStoreReleaseFailurePathsReachLiveSchema(t *testing.T) {
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
		runID       = "00000000-0000-4000-8000-000000000101"
		partitionID = "00000000-0000-4000-8000-000000000102"
		orgID       = "00000000-0000-4000-8000-000000000109"
	)
	now := time.Date(2026, 8, 21, 3, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at) VALUES ($1,$2,'2026-08-20','daily-v1','running','pending',$3,$3)`, runID, orgID, now); err != nil {
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

	// --- ReleasePartition's failure path (identical-class sibling bug) ---
	partitionClaim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || partitionClaim == nil {
		t.Fatalf("claim partition = %#v, %v", partitionClaim, err)
	}
	if err := store.ReleasePartition(ctx, *partitionClaim); err != nil {
		t.Fatalf("ReleasePartition against live schema: %v", err)
	}
	var partitionStatus string
	var partitionClaimToken *string
	if err := pool.QueryRow(ctx,
		`SELECT status, claim_token::text FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID,
	).Scan(&partitionStatus, &partitionClaimToken); err != nil {
		t.Fatal(err)
	}
	if partitionStatus != "failed" || partitionClaimToken != nil {
		t.Fatalf("released partition state = status=%s claim_token=%v, want failed/nil",
			partitionStatus, partitionClaimToken)
	}

	// Move the partition to a state finalize can proceed from.
	if _, err := pool.Exec(ctx,
		`UPDATE daily_metrics_partitions SET status = 'succeeded' WHERE id = $1::uuid`, partitionID,
	); err != nil {
		t.Fatal(err)
	}

	// --- ReleaseFinalize's failure path (the ticket's defect) ---
	finalizeClaim, err := store.ClaimFinalize(ctx, runID)
	if err != nil || finalizeClaim == nil {
		t.Fatalf("claim finalize = %#v, %v", finalizeClaim, err)
	}
	if err := store.ReleaseFinalize(ctx, *finalizeClaim); err != nil {
		t.Fatalf("ReleaseFinalize against live schema: %v", err)
	}
	var runStatus, finalizationStatus string
	var finalizeClaimToken *string
	if err := pool.QueryRow(ctx,
		`SELECT status, finalization_status, finalization_claim_token::text FROM daily_metrics_runs WHERE id = $1::uuid`,
		runID,
	).Scan(&runStatus, &finalizationStatus, &finalizeClaimToken); err != nil {
		t.Fatal(err)
	}
	if runStatus != "running" || finalizationStatus != "failed" || finalizeClaimToken != nil {
		t.Fatalf("released finalize state = status=%s finalization_status=%s claim_token=%v, want running/failed/nil",
			runStatus, finalizationStatus, finalizeClaimToken)
	}

	// A failed finalization is re-claimable, exactly like a failed partition --
	// this is the recovery path CHAOS-3997's strand repair no longer needs to
	// cover for once ReleaseFinalize can actually run.
	reclaimed, err := store.ClaimFinalize(ctx, runID)
	if err != nil || reclaimed == nil || reclaimed.Token == finalizeClaim.Token {
		t.Fatalf("reclaim after release = %#v, %v", reclaimed, err)
	}
}

// TestPostgresStoreReclaimsAPartitionReleasedWithAFailureReason is the
// CHAOS-4316 codex-review P1 regression control: ReleasePartitionWithReason
// persists status='failed' with a non-NULL failure_reason (migration 0113's
// ck_daily_metrics_partition_failure_reason_scope permits this), but
// ClaimPartition's reclaim UPDATE moved status to 'running' WITHOUT clearing
// failure_reason -- a live schema then rejects that UPDATE outright
// ("violates check constraint"), permanently stranding the exact partition
// this ticket's whole "stays retryable" design depends on. Only a real
// Postgres with the real CHECK constraints (not a hand-authored schema
// missing them, which is exactly the CHAOS-4043 trap this file exists to
// close) can catch this -- a mock or a schema missing the constraint would
// pass regardless of whether ClaimPartition clears the column.
func TestPostgresStoreReclaimsAPartitionReleasedWithAFailureReason(t *testing.T) {
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
		runID       = "00000000-0000-4000-8000-000000000201"
		partitionID = "00000000-0000-4000-8000-000000000202"
		orgID       = "00000000-0000-4000-8000-000000000209"
	)
	now := time.Date(2026, 8, 26, 3, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `INSERT INTO daily_metrics_runs (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at) VALUES ($1,$2,'2026-08-25','daily-v1','running','pending',$3,$3)`, runID, orgID, now); err != nil {
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

	claim, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || claim == nil {
		t.Fatalf("claim partition = %#v, %v", claim, err)
	}
	if err := store.ReleasePartitionWithReason(ctx, *claim, "progress_stalled"); err != nil {
		t.Fatalf("ReleasePartitionWithReason against live schema: %v", err)
	}
	var status string
	var failureReason *string
	if err := pool.QueryRow(ctx,
		`SELECT status, failure_reason FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID,
	).Scan(&status, &failureReason); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || failureReason == nil || *failureReason != "progress_stalled" {
		t.Fatalf("released state = status=%s failure_reason=%v, want failed/progress_stalled", status, failureReason)
	}

	// The reclaim itself: before the fix, this UPDATE violates
	// ck_daily_metrics_partition_failure_reason_scope against the real
	// schema and ClaimPartition returns ErrUnavailable instead of a claim.
	reclaimed, err := store.ClaimPartition(ctx, partitionID)
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaim after a liveness-kill release = %#v, %v", reclaimed, err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT status, failure_reason FROM daily_metrics_partitions WHERE id = $1::uuid`, partitionID,
	).Scan(&status, &failureReason); err != nil {
		t.Fatal(err)
	}
	if status != "running" || failureReason != nil {
		t.Fatalf("reclaimed state = status=%s failure_reason=%v, want running/nil", status, failureReason)
	}
}

// assertLeaseHeld requires a claim to have reported a live lease and to have
// carried the exact remaining time on it. Each call site states the remainder
// it predicts, so a claim that reports the wrong deadline cannot pass.
func assertLeaseHeld[T any](t *testing.T, what string, claim *T, err error, want time.Duration) {
	t.Helper()
	if claim != nil {
		t.Fatalf("%s took a claim while the lease was live: %#v", what, claim)
	}
	var active *LeaseActiveError
	if !errors.As(err, &active) {
		t.Fatalf("%s = %v, want a live-lease report", what, err)
	}
	if active.RetryAfter != want {
		t.Fatalf("%s retry after = %v, want %v", what, active.RetryAfter, want)
	}
}

// The status/finalization_status columns below are typed varchar(16), not
// text, to match alembic 0057 (widened by 0095 for 'no_repositories')
// column for column. A prior hand-authored text-only version of this DDL let
// the gate stay green against transitionFinalize/transitionPartition
// statements that Postgres rejects at PARSE time against the real
// varchar(16) columns ("inconsistent types deduced for parameter $1: text
// versus character varying", CHAOS-4043) -- the suite never actually
// executed those statements.
func createDailyTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE daily_metrics_runs (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, target_day date NOT NULL, generation varchar(64) NOT NULL,
 status varchar(16) NOT NULL, finalization_status varchar(16) NOT NULL, finalization_claim_token uuid NULL,
 finalization_lease_expires_at timestamptz NULL, finalized_at timestamptz NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 CONSTRAINT ck_daily_metrics_run_status CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'canceled', 'no_repositories')),
 CONSTRAINT ck_daily_metrics_finalize_status CHECK (finalization_status IN ('pending', 'running', 'succeeded', 'failed'))
);
CREATE TABLE daily_metrics_partitions (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES daily_metrics_runs(id), ordinal integer NOT NULL,
 repo_ids jsonb NOT NULL, status varchar(16) NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL, completed_at timestamptz NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 failure_reason varchar(64) NULL,
 CONSTRAINT ck_daily_metrics_partition_failure_reason_scope CHECK (failure_reason IS NULL OR status IN ('failed', 'failed_permanent')),
 CONSTRAINT ck_daily_metrics_partition_failed_permanent_has_reason CHECK (status <> 'failed_permanent' OR failure_reason IS NOT NULL)
);
CREATE TABLE worker_job_outbox (
 id uuid PRIMARY KEY, dedupe_key varchar(256) NOT NULL UNIQUE,
 job_kind varchar(96) NOT NULL, contract_version integer NOT NULL,
 args json NOT NULL, payload_hash varchar(71) NOT NULL,
 queue varchar(96) NOT NULL, priority smallint NOT NULL,
 max_attempts smallint NOT NULL, scheduled_at timestamptz NOT NULL,
 status varchar(16) NOT NULL, attempt_count integer NOT NULL,
 next_attempt_at timestamptz NOT NULL, prerequisite_completion_key text NULL,
 created_at timestamptz NOT NULL,
 updated_at timestamptz NOT NULL
) ;
CREATE TABLE worker_job_completion_fences (
 completion_key text PRIMARY KEY,
 completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
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
	cancel   context.CancelFunc
}

type failingRunPublisher struct{}

func (failingRunPublisher) PublishDispatchTx(context.Context, pgx.Tx, Run, string) error {
	return ErrUnavailable
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
	publisher.cancel()
	return errors.New("injected crash after outbox insert")
}
