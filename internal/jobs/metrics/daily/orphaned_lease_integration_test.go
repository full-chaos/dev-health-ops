//go:build integration

package daily

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Production fixture for CHAOS-3991, read from fullchaosdev on 2026-08-20.
// daily_metrics_runs b49b326d had one succeeded partition and a finalize lease
// claimed at 00:57:30.127621Z that expired unreleased at 01:07:30.127621Z. River
// job 1282 (metrics.daily_finalize) then retried at 00:57:47.818849Z, inside the
// lease window, and finalized as COMPLETED after 9ms of work: the retry saw the
// orphaned live lease, took the claim==nil path, and reported success. That
// destroyed the only trigger that could have reclaimed the lease ten minutes
// later, so the run never terminalized and never wrote its completion fence.
const (
	strandedRunID       = "b49b326d-8db6-50af-8fc2-9557d0584470"
	strandedOrgID       = "c6a38355-dad6-42e4-8cc9-4c712450827d"
	strandedPartitionID = "1a1d0e2c-0c4c-4f4f-8f2a-7d0c9f5a3b21"
	strandedGeneration  = "post-sync:563fc88b-7db0-5d1a-b517-c4488b28dbcb"
	strandedFenceKey    = "daily_metrics_run:" + strandedRunID
)

var (
	prodRunCreatedAt   = time.Date(2026, 8, 20, 0, 56, 27, 91713000, time.UTC)
	prodPartitionDone  = time.Date(2026, 8, 20, 0, 57, 28, 401979000, time.UTC)
	prodFinalizeClaim  = time.Date(2026, 8, 20, 0, 57, 30, 127621000, time.UTC)
	prodFinalizeRetry  = time.Date(2026, 8, 20, 0, 57, 47, 818849000, time.UTC)
	prodLeaseExpiresAt = time.Date(2026, 8, 20, 1, 7, 30, 127621000, time.UTC)
)

// TestFinalizeRetryInsideOrphanedLeaseKeepsTheJobAlive pins the mechanism, not
// the symptom: a finalize retry that lands inside a lease orphaned by a dead
// predecessor must NOT report success to the job runtime. Reporting success is
// what deletes the job, and the job is the only thing that can reclaim the lease
// after it expires. The retry has to stay alive across the expiry instead.
func TestFinalizeRetryInsideOrphanedLeaseKeepsTheJobAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, store, leases := startStrandedRunFixture(t, ctx)

	// Attempt 1: claims the finalize lease, then dies without releasing it. This
	// is the CompleteFinalize-failure exit in FinalizeHandler.Work, the one path
	// that claims and returns retryable without a release.
	store.now = func() time.Time { return prodFinalizeClaim }
	orphaned, err := store.ClaimFinalize(ctx, strandedRunID)
	if err != nil || orphaned == nil {
		t.Fatalf("attempt 1 claim = %#v, %v", orphaned, err)
	}
	assertLeaseExpiry(t, ctx, pool, prodLeaseExpiresAt)

	// Attempt 2: fires 17s later, exactly as River job 1282 did in production.
	// The lease is orphaned but still live for another ~9m43s.
	store.now = func() time.Time { return prodFinalizeRetry }
	handler, err := NewFinalizeHandler(store, fakeCompatibility{})
	if err != nil {
		t.Fatal(err)
	}
	retryErr := handler.Work(ctx, strandedFinalizeExecution())
	// Exactly one snooze, no reclaim: the lease was live, so nothing was taken.
	leases.assert(t, "at the retry", 1, 0)
	if retryErr == nil {
		t.Fatal("finalize retry reported success while the run was still unfinalized: " +
			"River deletes the job here, and nothing can reclaim the orphaned lease afterwards")
	}
	delay, snoozed := jobruntime.SnoozeDelay(retryErr)
	if !snoozed {
		t.Fatalf("finalize retry must snooze past the live lease, got %v", retryErr)
	}
	if want := prodLeaseExpiresAt.Sub(prodFinalizeRetry); delay <= 0 || delay > want {
		t.Fatalf("snooze delay = %v, want a positive delay no later than the lease expiry %v", delay, want)
	}

	// The consequence the ticket reports: no fence row, so every gated outbox row
	// behind this run is still pending at attempt_count = 0.
	assertFenceAbsent(t, ctx, pool)
	assertOutboxPending(t, ctx, pool, 0)

	// After the lease expires the very same job must reclaim and finish the run,
	// which is what writes the completion fence that releases the fanout.
	store.now = func() time.Time { return prodLeaseExpiresAt.Add(time.Second) }
	if err := handler.Work(ctx, strandedFinalizeExecution()); err != nil {
		t.Fatalf("reclaim after lease expiry = %v", err)
	}
	// The snooze count is unchanged and exactly one expired lease was taken over.
	leases.assert(t, "after the reclaim", 1, 1)
	assertRunStatus(t, ctx, pool, "succeeded", "succeeded")
	assertFencePresent(t, ctx, pool)
}

func startStrandedRunFixture(
	t *testing.T,
	ctx context.Context,
) (*pgxpool.Pool, *PostgresStore, *recordingLeaseObserver) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createDailyTables(t, ctx, pool)
	if _, err := jobruntime.Load(filepath.Join("..", "..", "..", "..", "contracts", "jobs", "v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs
 (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,'2026-08-19',$3,'running','pending',$4,$4)`,
		strandedRunID, strandedOrgID, strandedGeneration, prodRunCreatedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_partitions
 (id,run_id,ordinal,repo_ids,status,attempt_count,completed_at,created_at,updated_at)
VALUES ($1,$2,0,'[]'::jsonb,'succeeded',1,$3,$4,$3)`,
		strandedPartitionID, strandedRunID, prodPartitionDone, prodRunCreatedAt); err != nil {
		t.Fatal(err)
	}
	// The head of the stranded chain: workgraph.build, gated on this run.
	if _, err := pool.Exec(ctx, `
INSERT INTO worker_job_outbox
 (id,dedupe_key,job_kind,contract_version,args,payload_hash,queue,priority,max_attempts,
  scheduled_at,status,attempt_count,next_attempt_at,prerequisite_completion_key,created_at,updated_at)
VALUES ('2c1f0b7e-5d3a-4a6b-9c2e-70a1b4d8e5f3','workgraph.build:'||$1,'workgraph.build',1,
  '{}'::json,'sha256:0000000000000000000000000000000000000000000000000000000000000000',
  'default',3,5,$2,'pending',0,$2,$3,$2,$2)`,
		strandedRunID, prodRunCreatedAt, strandedFenceKey); err != nil {
		t.Fatal(err)
	}
	leases := &recordingLeaseObserver{}
	store, err := NewPostgresStore(pool, leases)
	if err != nil {
		t.Fatal(err)
	}
	return pool, store, leases
}

// recordingLeaseObserver counts exactly what the production counter counts, so
// the test predicts the metric rather than merely proving the run finished.
type recordingLeaseObserver struct {
	snoozed   int
	reclaimed int
}

func (observer *recordingLeaseObserver) ObserveDailyMetricsLease(
	stage jobruntime.DailyMetricsLeaseStage,
	result jobruntime.DailyMetricsLeaseResult,
) error {
	if stage != jobruntime.DailyMetricsLeaseStageFinalize {
		return nil
	}
	switch result {
	case jobruntime.DailyMetricsLeaseResultSnoozed:
		observer.snoozed++
	case jobruntime.DailyMetricsLeaseResultReclaimed:
		observer.reclaimed++
	}
	return nil
}

func (observer *recordingLeaseObserver) assert(t *testing.T, when string, snoozed, reclaimed int) {
	t.Helper()
	if observer.snoozed != snoozed || observer.reclaimed != reclaimed {
		t.Fatalf(
			"finalize lease counters %s = snoozed %d / reclaimed %d, want %d / %d",
			when, observer.snoozed, observer.reclaimed, snoozed, reclaimed,
		)
	}
}

func strandedFinalizeExecution() *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs] {
	domain := jobcontract.DomainLink{Type: "daily_metrics_run", ID: strandedRunID}
	return &jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]{
		OrganizationID: pointer(strandedOrgID),
		Envelope:       jobcontract.Envelope{OrganizationID: pointer(strandedOrgID), Domain: domain},
		Args: jobruntime.DailyMetricsFinalizeArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.DailyMetricsFinalizePayload]{
				OrganizationID: pointer(strandedOrgID),
				Domain:         domain,
				Payload:        jobcontract.DailyMetricsFinalizePayload{RunID: strandedRunID},
			},
		},
	}
}

func assertLeaseExpiry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want time.Time) {
	t.Helper()
	var got time.Time
	if err := pool.QueryRow(ctx,
		"SELECT finalization_lease_expires_at FROM daily_metrics_runs WHERE id = $1::uuid",
		strandedRunID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.UTC().Equal(want) {
		t.Fatalf("lease expiry = %s, want %s", got.UTC(), want)
	}
}

func assertRunStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status, finalization string) {
	t.Helper()
	var gotStatus, gotFinalization string
	if err := pool.QueryRow(ctx,
		"SELECT status, finalization_status FROM daily_metrics_runs WHERE id = $1::uuid",
		strandedRunID).Scan(&gotStatus, &gotFinalization); err != nil {
		t.Fatal(err)
	}
	if gotStatus != status || gotFinalization != finalization {
		t.Fatalf("run state = %s/%s, want %s/%s", gotStatus, gotFinalization, status, finalization)
	}
}

func countFences(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM worker_job_completion_fences WHERE completion_key = $1",
		strandedFenceKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertFenceAbsent(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if count := countFences(t, ctx, pool); count != 0 {
		t.Fatalf("fence rows for an unfinalized run = %d, want 0", count)
	}
}

func assertFencePresent(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if count := countFences(t, ctx, pool); count != 1 {
		t.Fatalf("fence rows after finalize = %d, want 1", count)
	}
}

func assertOutboxPending(t *testing.T, ctx context.Context, pool *pgxpool.Pool, attempts int) {
	t.Helper()
	var status string
	var gotAttempts int
	if err := pool.QueryRow(ctx,
		"SELECT status, attempt_count FROM worker_job_outbox WHERE prerequisite_completion_key = $1",
		strandedFenceKey).Scan(&status, &gotAttempts); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || gotAttempts != attempts {
		t.Fatalf("gated outbox row = %s/%d, want pending/%d", status, gotAttempts, attempts)
	}
}

// Second production instance of the same defect, one layer down. Run
// f1cc9f0d-267a-51f0-8740-f49b5b6a03b3 lost partition
// f08e8858-6f99-570f-9154-24ed8f837641 the same way three minutes later: claimed
// at 01:00:15.128174, orphaned, and River job 1303 retried at 01:00:44.761426 --
// inside the lease -- and reported success after 12ms. That run reads as
// running/pending rather than running/running only because a partition that
// never succeeds also blocks ClaimFinalize's readiness guard, so the finalize
// layer is never even reached. One cause, two presentations.
const (
	strandedPartitionRunID  = "f1cc9f0d-267a-51f0-8740-f49b5b6a03b3"
	strandedLivePartitionID = "f08e8858-6f99-570f-9154-24ed8f837641"
)

var (
	prodPartitionClaim = time.Date(2026, 8, 20, 1, 0, 15, 128174000, time.UTC)
	prodPartitionRetry = time.Date(2026, 8, 20, 1, 0, 44, 761426000, time.UTC)
	prodPartitionLease = time.Date(2026, 8, 20, 1, 10, 15, 128174000, time.UTC)
)

func TestPartitionRetryInsideOrphanedLeaseKeepsTheJobAlive(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_runs
 (id,org_id,target_day,generation,status,finalization_status,created_at,updated_at)
VALUES ($1,$2,'2026-08-20','fixed-schedule:daily_metrics_fanout:2026-08-20T01:00:00Z','running','pending',$3,$3)`,
		strandedPartitionRunID, strandedOrgID, prodPartitionClaim); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO daily_metrics_partitions
 (id,run_id,ordinal,repo_ids,status,attempt_count,created_at,updated_at)
VALUES ($1,$2,0,'[]'::jsonb,'pending',0,$3,$3)`,
		strandedLivePartitionID, strandedPartitionRunID, prodPartitionClaim); err != nil {
		t.Fatal(err)
	}
	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	store.now = func() time.Time { return prodPartitionClaim }
	orphaned, err := store.ClaimPartition(ctx, strandedLivePartitionID)
	if err != nil || orphaned == nil {
		t.Fatalf("attempt 1 claim = %#v, %v", orphaned, err)
	}

	// Attempt 2, inside the lease, exactly as River job 1303 fired.
	store.now = func() time.Time { return prodPartitionRetry }
	retry, retryErr := store.ClaimPartition(ctx, strandedLivePartitionID)
	assertLeaseHeld(
		t, "partition retry inside an orphaned lease", retry, retryErr,
		prodPartitionLease.Sub(prodPartitionRetry),
	)

	// A partition still held cannot let the finalizer run, which is why this run
	// presents as running/pending. That must stay a plain no-op, not a snooze:
	// no finalize job exists yet, so there is nothing to keep alive.
	if claim, err := store.ClaimFinalize(ctx, strandedPartitionRunID); err != nil || claim != nil {
		t.Fatalf("finalize claim while a partition is unfinished = %#v, %v", claim, err)
	}

	// Once the lease expires the same partition job reclaims it.
	store.now = func() time.Time { return prodPartitionLease.Add(time.Second) }
	reclaimed, err := store.ClaimPartition(ctx, strandedLivePartitionID)
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaim after lease expiry = %#v, %v", reclaimed, err)
	}
	if reclaimed.Token == orphaned.Token {
		t.Fatal("reclaim reused the orphaned claim token")
	}
}
